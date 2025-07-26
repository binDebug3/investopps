import yfinance as yf
import pandas as pd
import pdb
import os
from send_email import send_table, send_email
import time
from datetime import datetime
import logging
import yaml
import traceback


"""
A class to find stock bargains based on price changes.

Attributes:
tol: float
    The percentage drop threshold to consider a stock a bargain.
period_length: int
    The number of days to look back for bargains before creating a report.
update_hour: int
    The hour of the day to check for updates (default is 15 for 3 PM).

A bunch of file paths:
    ticker_path: str - Path to the file containing stock tickers.
    prices_path: str - Path to the CSV file where stock prices will be saved.
    history_path: str - Path to the CSV file where bargain history will be saved.
    update_log: str - Path to the file where the last update time will be logged.
    log_path: str - Path to the log file for logging updates and errors.
"""
class BargainFinder(object):
    
    def __init__(self, config_path: str = "meta/config.yaml"):
        """
        Initialize the BargainFinder with configuration from a YAML file.
        
        Parameters:
        config_path: str
            Path to the YAML configuration file. Defaults to "meta/config.yaml".
            Optional. Defaults to "meta/config.yaml".
        """
        self.config_path: str = config_path
        self._read_config(self.config_path, initialize=True)
        self._set_logging()
        self.update_time: pd.Timestamp = pd.to_datetime("now").normalize() - pd.Timedelta(days=7) # arbitrary past date



    # ---- PRIVATE SET UP METHODS ----

    def _save_attributes(self) -> dict:
        """
        Save current class attributes to a dictionary.
        
        Returns:
        attributes: dict
            Dictionary containing the current class attributes.
        """
        # Dynamically return all instance attributes except private and methods
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_') and not callable(v)}


    def _read_config(self, config_path: str, 
                     initialize: bool = False) -> None:
        """
        Read configuration from a YAML file and set class attributes.
        
        Parameters:
        config_path: str
            Path to the YAML configuration file.
        
        initialize: bool
            If True, initializes the class attributes with defaults if not present.
            Optional. Defaults to False.
        """
        # Check if the config file exists and load it
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        with open(config_path, "r") as f:
            config: dict = yaml.safe_load(f)
        
        old_attrs: dict = self._save_attributes() if not initialize else {}

        # set class attributes with defaults if not present
        self.tol: float = float(config.get("tol", -20))
        self.email_rate: int = int(config.get("email_rate", 7))
        self.high_price_period: str = config.get("high_price_period", "500d")
        self.update_hour: int = int(config.get("update_hour", 15))
        self.ticker_path: str = config.get("ticker_path", "meta/tickers.txt")
        self.htick_path: str = config.get("htick_path", "meta/high_tickers.txt")
        self.prices_path: str = config.get("prices_path", "data/prices.csv")
        self.raw_prices_path: str = config.get("raw_prices_path", "data/raw_prices.csv")
        self.history_path: str = config.get("history_path", "data/bargain_history.csv")
        self.update_log_path: str = config.get("update_log_path", "meta/update_log.txt")
        self.log_path: str = config.get("log_path", "logs/bargain_finder_{}.log")
        self.bargain_subject: str = config.get("bargain_subject", "Weekly Bargain Report")
        self.sell_subject: str = config.get("sell_subject", "Sell Opportunity Found!")
        self.recipient: str = config.get("recipient", "")
        self.to_sell_tickers: list = None
        
        # Log changes in attributes
        if not initialize:
            new_attrs: dict = self._save_attributes()
            for key in new_attrs:
                if old_attrs.get(key) != new_attrs[key]:
                    self.logger.info(f"Attribute '{key}' changed from {old_attrs.get(key)} to {new_attrs[key]}")
    
    
    def _set_logging(self):
        """
        Set up logging for the BargainFinder class.
        """
        now_str: str = "persistent"
        log_file: str = self.log_path.format(now_str)
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(levelname)s in %(funcName)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            filename=log_file
        )
        self.logger: logging.Logger = logging.getLogger()

        # Only add handlers once (avoid duplicates when re-instantiating)
        if not self.logger.handlers:
            # now_str: str = datetime.now().strftime("%Y%m%d_%H%M%S")
            handler: logging.FileHandler = logging.FileHandler(log_file)
            formatter: logging.Formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        self.logger.info(f"Program initialized with PID {os.getpid()}")
        self.logger.info("BargainFinder initialized with tol=%s, period_length=%s, update_hour=%s",
                         self.tol, self.email_rate, self.update_hour)
    
    
    
    # ---- PUBLIC STATE MANAGEMENT METHODS ----
    
    def get_tickers(self, file_path: str = None) -> list:
        """
        Read tickers from a file.
        """
        if file_path is None:
            file_path = self.ticker_path
        with open(file_path, 'r') as file:
            tickers: list = [line.strip() for line in file if line.strip()]
        self.logger.info(f"Loaded {len(tickers)} tickers from {file_path}")
        return tickers
    
    
    def refresh_history(self) -> None:
        """
        Refresh the history file by removing all entries.
        """
        # Warn the user if there is an active terminal session to print to
        if os.isatty(0):
            input("Press 'Enter' to clear the files.")
        
        paths_to_clear: list = [self.history_path, self.prices_path, self.log_path, self.update_log_path]
        for path in paths_to_clear:
            if os.path.exists(path):
                os.remove(path)
                self.logger.info(f"Cleared file: '{path}'")



    # ---- FIND CURRENT BARGAINS METHODS ----

    def _need_to_redownload(self, file_path: str) -> bool:
        """
        Check if the prices file exists and what the last update date is.
        If the file does not exist or is older than 1 day, return True.
        
        Parameters:
        file_path: str
            Path to the file to check.
        
        Returns:
        redownload: bool
            True if the file needs to be redownloaded, False otherwise.
        """
        try:
            last_modified: pd.Timestamp = os.path.getmtime(file_path)
            last_update: pd.Timestamp = pd.to_datetime(last_modified, unit='s').normalize()
            return (pd.to_datetime("today").normalize() - last_update).days > 1
        except (FileNotFoundError, KeyError):
            return True


    def _update_prices(self, force_redownload: bool = False) -> pd.DataFrame:
        """
        Update prices for all tickers in the ticker file.
        
        Parameters:
        force_redownload: bool
            If True, forces redownload of prices even if the file exists.
            Optional. Defaults to False.
        
        Returns:
        price_info: pd.DataFrame
            DataFrame containing the latest prices and changes for each ticker.
        """
        force_redownload = True
        if force_redownload or self._need_to_redownload(self.prices_path):
            # Get tickers from the file
            tickers: list = self.get_tickers()
            df: pd.DataFrame = pd.DataFrame(columns=['Ticker', "3 Months", "1 Month", "Current", "3 Mo Change", "1 Mo Change"])
            df['Ticker'] = tickers
            
            # Date anchors
            today: pd.Timestamp = pd.to_datetime("today").normalize()
            d30: pd.Timestamp = today - pd.Timedelta(days=30)
            d90: pd.Timestamp = today - pd.Timedelta(days=90)
            
            # Download past ~100 days to ensure we cover the needed range
            self.logger.info(f"Downloading prices for {len(tickers)} tickers")
            data: pd.DataFrame = yf.download(tickers, period="100d", interval="1d", auto_adjust=True)["Close"]

            # Transpose the data so tickers are rows
            for label, target_date in zip(["Current", "1 Month", "3 Months"], [today, d30, d90]):
                # Get the actual market date closest to the target date
                actual_dates: pd.DatetimeIndex = data.index
                nearest_date: pd.Timestamp = actual_dates[(abs(actual_dates - target_date)).argmin()]
                
                # Extract prices for that date
                prices: pd.Series = data.loc[nearest_date]
                df[label] = df['Ticker'].map(prices)
                
            # Calculate changes
            df["3 Mo Change"] = (df["Current"] - df["3 Months"]) / df["3 Months"] * 100
            df["1 Mo Change"] = (df["Current"] - df["1 Month"]) / df["1 Month"] * 100

            # save and return
            self.logger.info(f"Saving prices to '{self.prices_path}'")
            df.to_csv(self.prices_path, index=False)
        else:
            df: pd.DataFrame = pd.read_csv(self.prices_path)
            self.logger.info(f"Loaded prices from '{self.prices_path}'")
        return df


    def find_current_bargains(self, force_redownload: bool = False) -> pd.DataFrame:
        """
        Find stocks that have dropped more than the specified tolerance.
        
        Parameters:
        tol: float
            The percentage drop threshold to consider a stock a bargain.
        
        history_path: str
            Path to the CSV file where bargain history will be saved.
        
        Returns:
        bargains: pd.DataFrame
            DataFrame containing tickers that meet the bargain criteria.
        """
        df: pd.DataFrame = self._update_prices(force_redownload)
        cols: list = ['Date'] + [col for col in df.columns if col != 'Date']
        bargains: pd.DataFrame = df[df["1 Mo Change"] < self.tol].copy()
        if bargains.empty:
            self.logger.info("No bargains found.")
            return pd.DataFrame(columns=cols)
        
        # get todays date and time
        today: pd.Timestamp = pd.to_datetime("today").normalize()
        bargains.loc[:, 'Date'] = today
        
        # initialize history DataFrame if it doesn't exist
        if os.path.exists(self.history_path):
            hdf: pd.DataFrame = pd.read_csv(self.history_path) 
        else:
            hdf: pd.DataFrame = pd.DataFrame(columns=cols)
        
        # append new bargains to history, save, return
        frames: list = [df for df in [hdf, bargains] if not df.empty and not df.isna().all().all()]
        hdf: pd.DataFrame = pd.concat(frames, ignore_index=True)
        hdf.to_csv(self.history_path, index=False)
        self.logger.info(f"Found {len(bargains)} bargains, saved to '{self.history_path}'")
        return bargains
    
    
    
    # ---- CHECK VOLATILE STOCK METHODS ----
    
    def _update_sell_tracking(self, 
                         force_redownload: bool = False) -> pd.DataFrame:
        """
        Update prices for all tickers in the ticker file.
        
        Parameters:
        force_redownload: bool
            If True, forces redownload of prices even if the file exists.
            Optional. Defaults to False.
        
        Returns:
        price_info: pd.DataFrame
            DataFrame containing the latest prices and changes for each ticker.
        """
        # read csv save file if it exists
        if os.path.exists(self.raw_prices_path):
            df: pd.DataFrame = pd.read_csv(self.raw_prices_path)
            tracked_tickers: list = df.columns.tolist()[1:]
            new_tickers: list = [ticker for ticker in self.to_sell_tickers if ticker not in tracked_tickers]
            # get the last date in last_dates
            last_date: pd.Timestamp = pd.to_datetime(df.iloc[-1]['Date'], format='%Y-%m-%d', errors='coerce')
            # find difference between last date and today
            days_diff: int = (pd.to_datetime("today").normalize() - last_date).days
            days_diff = f"{days_diff}d" if days_diff > 0 else "1d"
        else:
            tracked_tickers: list = []
            new_tickers: list = self.to_sell_tickers
            # create an empty file
            pd.DataFrame(columns=['Date']).to_csv(self.raw_prices_path, index=False)
            force_redownload = True
        
        if force_redownload or self._need_to_redownload(self.raw_prices_path):
            # Download past days to ensure we cover the needed range
            self.logger.info(f"Downloading prices for {len(self.to_sell_tickers)} tickers to find sell opportunities")
            if new_tickers:
                data: pd.DataFrame = yf.download(new_tickers, period=self.high_price_period, interval="1d", auto_adjust=True)["Close"]

            if tracked_tickers:
                updates: pd.DataFrame = yf.download(tracked_tickers, period=days_diff, interval="1d", auto_adjust=True)["Close"]
                updates = updates[updates.index > last_date]
                updates = updates.reset_index()
                
                # combine with save data
                saved_data = pd.read_csv(self.raw_prices_path)
                saved_data['Date'] = pd.to_datetime(saved_data['Date'], format='%Y-%m-%d', errors='coerce')
                saved_data = pd.concat([saved_data, updates], axis=0)
                saved_data = saved_data.reset_index(drop=True)
                
                if new_tickers:
                    data = pd.concat([data, saved_data], axis=1)
                else:
                    data = saved_data
            
            # save and return
            self.logger.info(f"Saving prices to '{self.raw_prices_path}'")
            data.to_csv(self.raw_prices_path)
        else:
            data: pd.DataFrame = pd.read_csv(self.raw_prices_path)
            self.logger.info(f"Loaded prices from '{self.raw_prices_path}'")
        return data
        
    
    def _check_for_extremes(self, 
                            force_redownload: bool = False) -> pd.DataFrame:
        """
        Check for stocks that have extreme price changes.
        
        Parameters:
        period: str
            The period for which to download prices. Defaults to "100d".
        
        tickers: list
            List of stock tickers to download prices for. If None, uses the tickers from the ticker file.
    
        force_redownload: bool
            If True, forces redownload of prices even if the file exists.
            Optional. Defaults to False.
        
        Returns:
        extremes: pd.DataFrame
            DataFrame containing tickers with extreme price changes.
        """
        
        data = self._update_sell_tracking(force_redownload)

        # check for sell signals
        ticker_stats: dict = {}
        sell_any = False
        for ticker in self.to_sell_tickers:
            analysis: dict = {}
            if ticker in data.columns:
                # get the current price
                current_price: float = data[ticker].iloc[-1]
                analysis['Current'] = current_price
                # get all prices before 1 month ago
                d30: pd.Timestamp = pd.to_datetime("today").normalize() - pd.Timedelta(days=30)
                pdb.set_trace()
                past_prices: pd.Series = data[ticker][data.index <= d30]
                
                # get the high of past prices
                if not past_prices.empty:
                    high_price: float = past_prices.max()
                    analysis['High'] = high_price
                    # calculate the percentage drop
                    analysis['dHigh'] = (current_price - high_price) / high_price * 100
                else:
                    analysis['High'] = current_price
                    analysis['dHigh'] = 0.0
                analysis["Sell"] = analysis['dHigh'] > 0
                if analysis["Sell"]:
                    sell_any = True
                self.logger.info(f"Sell signal [{analysis["Sell"]}] for {ticker}: Current={current_price}, High={high_price}, dHigh={analysis['dHigh']:.2f}%")
                ticker_stats[ticker] = analysis
            else:
                self.logger.warning(f"Ticker '{ticker}' not found in downloaded data.")
                ticker_stats[ticker] = analysis
                continue
        return sell_any, ticker_stats
    
    
    def create_sell_report(self,
                            tickers: list = None,
                            save_file: str = None,
                            force_redownload: bool = False) -> None:
        
        # set the save file path if not provided
        self.raw_prices_path: str = save_file if save_file is not None else self.raw_prices_path
        
        # set the tickers if not provided
        if tickers is None:
            self.to_sell_tickers: list = self.get_tickers(self.htick_path)
                
        sell_any, ticker_stats = self._check_for_extremes(force_redownload)
        
        if sell_any:
            self.logger.info("Sell opportunities found, creating report.")
            report: pd.DataFrame = pd.DataFrame.from_dict(ticker_stats, orient='index')
            report.reset_index(inplace=True)
            report.rename(columns={'index': 'Ticker'}, inplace=True)
            report = report[report['Sell'] == True]
            report_email: str = self.format_report(report)
            self.logger.info(f"Created sell report with {len(report)} stocks.")
            
            send_table(self.sell_subject, report_email, self.recipient)
            self.logger.info(f"Sell report sent to '{self.recipient}'")
        else:
            self.logger.info("No sell opportunities found.")
                    
    
    
    # ---- CREATE REPORT METHODS ----
        
    def _get_recent_bargains(self, period_length: int = 7) -> pd.DataFrame:
        """
        Get the bargains found in the last week.
        
        Returns:
        report: pd.DataFrame
            DataFrame containing the weekly report of bargains.
        """
        if not os.path.exists(self.history_path):
            self.logger.warning(f"History file not found: '{self.history_path}'")
            return pd.DataFrame()
        
        hdf: pd.DataFrame = pd.read_csv(self.history_path)
        if 'Date' in hdf.columns:
            hdf['Date'] = pd.to_datetime(hdf['Date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        if hdf.empty:
            self.logger.warning("No bargains found in history.")
            return pd.DataFrame()
            
        # Filter for the last week
        last_week: pd.Timestamp = pd.to_datetime("today").normalize() - pd.Timedelta(days=period_length)
        weekly_bargains: pd.DataFrame = hdf[hdf['Date'] >= last_week]
        self.logger.info(f"Found {len(weekly_bargains)} bargains in the last {period_length} days.")
        return weekly_bargains


    def format_report(self, report: pd.DataFrame) -> str:
        """
        Format the report DataFrame into HTML with links to Google Finance and Brave Search.
        
        Parameters:
        report: pd.DataFrame
            DataFrame containing the report to format.
            
        Returns:
        report_email: str
            HTML string of the formatted report with links.
        """
        google_finance_link: str = "https://www.google.com/finance/quote/{}:NYSE?window=6M"
        brave_search_link: str = "https://search.brave.com/search?q={}+stock&rh_type=st&range=ytd"
        
        # insert columns with links
        report['Google Finance'] = report['Ticker'].apply(
            lambda x: f'<a href="{google_finance_link.format(x)}" target="_blank">Open</a>'
        )
        report['Brave Search'] = report['Ticker'].apply(
            lambda x: f'<a href="{brave_search_link.format(x)}" target="_blank">Open</a>'
        )
        
        # Make Ticker column bold in HTML
        report['Ticker'] = report['Ticker'].apply(lambda x: f"<b>{x}</b>")
        report_email: str = report.to_html(escape=False, index=False, justify="center", border=1)
        return report_email

    
    def create_bargain_report(self) -> None:
        """
        Create a regular report of bargains found and save it to a file.
        """
        bargains: pd.DataFrame = self._get_recent_bargains(self.email_rate)
        if bargains.empty: return
        
        # Count the number of times each unique ticker appears
        ticker_counts: pd.DataFrame = bargains['Ticker'].value_counts().reset_index()
        ticker_counts.columns = ['Ticker', 'Count']

        # find the average change for each ticker, rounded to two decimals
        avg_changes: pd.DataFrame = bargains.groupby('Ticker').agg({'3 Mo Change': 'mean', '1 Mo Change': 'mean'}).reset_index()
        avg_changes.columns = ['Ticker', 'Avg 3 Mo Change', 'Avg 1 Mo Change']
        avg_changes['Avg 3 Mo Change'] = avg_changes['Avg 3 Mo Change'].map(lambda x: f"{x:.2f}%")
        avg_changes['Avg 1 Mo Change'] = avg_changes['Avg 1 Mo Change'].map(lambda x: f"{x:.2f}%")

        # Merge the counts and average changes
        report: pd.DataFrame = pd.merge(ticker_counts, avg_changes, on='Ticker')
        report_email: str= self.format_report(report)
        self.logger.info(f"Created bargain report with {len(report)} bargains.")
        
        send_table(self.bargain_subject, report_email, self.recipient)
        self.logger.info(f"Bargain report sent to '{self.recipient}'")



    # ---- PRIVATE TIMING METHODS ----

    def _is_market_day(self, dt: pd.Timestamp) -> bool:
        """
        Check if the given date is a market day (Monday to Friday).
        Parameters:
        dt: pd.Timestamp
            The date to check.
        """
        saturday: int = 5
        result = dt.weekday() < saturday  # Mon–Fri
        self.logger.debug(f"{dt.strftime('%Y-%m-%d')} is {'' if result else 'not '}a market day")
        return result


    def _has_updated_today(self) -> bool:
        """
        Check if the update log exists and if the last update was today.
        Returns:
        has_updated: bool
            True if the update log exists and the last update was today, False otherwise.
        """
        if not os.path.exists(self.update_log_path):
            return False
        with open(self.update_log_path, 'r') as file:
            last_update: pd.Timestamp = pd.to_datetime(file.readline().strip())
        updated_today = last_update.date() == pd.to_datetime("now").date()
        self.logger.debug(f"Last update was {'today' if updated_today else 'not today'} ({last_update})")
        return updated_today


    def _wait_until(self, target_time: datetime) -> None:
        """
        Wait until the specified target time.
        Parameters:
        target_time: datetime
            The time to wait until.
        """
        now: pd.Timestamp = pd.to_datetime("now")
        wait_seconds: int = (target_time - now).total_seconds()
        if wait_seconds > 0:
            self.logger.info(f"Sleeping until {target_time.strftime('%Y-%m-%d %H:%M:%S')} ({wait_seconds:.0f} seconds)")
            time.sleep(wait_seconds)


    def _wait_til_tmr(self, now: pd.Timestamp) -> None:
        """
        Wait until the next market day at 3 PM if the current time is past 3 PM.
        Parameters:
        now: pd.Timestamp
            The current time.
        """
        if pd.to_datetime("now") > self.update_time:
            # If it's already past 3 PM, wait until next weekday
            next_day: pd.Timestamp = now + pd.Timedelta(days=1)
            while not self._is_market_day(next_day):
                next_day += pd.Timedelta(days=1)
            self.update_time = next_day + pd.Timedelta(hours=self.update_hour)



    # ---- CONTINUOUS EXECUTION METHODS ----

    def _execute(self, day_of_week: int = 3) -> None:
        """
        Execute the BargainFinder routine to find bargains and create a report.
        Parameters:
        day_of_week: int
            The day of the week to create a report (default is 3 for Thursday).
        """
        self.logger.info("Executing BargainFinder routine")
        self.find_current_bargains()
        # self.create_sell_report()

        # Create a report if today is the specified day of the week
        if self.update_time.weekday() == day_of_week:  # Thursday
            self.logger.info("Creating weekly report")
            self.create_bargain_report()
            
        # Log update
        with open(self.update_log_path, 'w') as file:
            file.write(pd.to_datetime("now").isoformat())
        self.logger.info(f"Update logged at {pd.to_datetime('now').isoformat()}")


    def run(self, auto: bool = True):
        """
        Run the BargainFinder to find bargains every weekday at 3 PM and create a report every Thursday at 3 PM.
        """
        # intialize counting variables
        update_count = 0
        iter_count = 0
        start_date = pd.to_datetime("now").normalize()
        
        dont_end = True
        while dont_end:
            # wait until the next update time
            now = pd.to_datetime("now").normalize()
            self.update_time = now + pd.Timedelta(hours=self.update_hour)  # Today at 3 PM
            if auto:
                self._wait_til_tmr(now)
                self._wait_until(self.update_time)
            
            # check if we should update, then execute update
            self._read_config(self.config_path)
            if self._is_market_day(pd.to_datetime("now")) and not self._has_updated_today():
                self._execute()
                update_count += 1
            
            # Log the iteration
            dont_end = auto
            iter_count += 1
            days_since_start = (now - start_date).days
            self.logger.info(f"Iteration {iter_count}: Updated {update_count} times in {days_since_start} days\n\n\n")
            


# def parse_args():
#     parser = argparse.ArgumentParser(description="Run the BargainFinder script.")
#     parser.add_argument("--tol", type=float, default=-25,
#                         help="Threshold percentage drop to consider a stock a bargain (default: -25)")
#     parser.add_argument("--period", type=int, default=7,
#                         help="Number of days to look back for bargains (default: 7)")
#     parser.add_argument("--update_hour", type=int, default=15,
#                         help="Hour of the day to check for updates (default: 15 for 3 PM)")
#     return parser.parse_args()



if __name__ == "__main__":
    # args = parse_args()
    finder = BargainFinder()
    try:
        finder.run(auto=False)
        # finder.create_sell_report(force_redownload=True)
    except KeyboardInterrupt:
        print("BargainFinder stopped by user.")
    except Exception as e:
        if pd.to_datetime("now").hour == finder.update_hour:
            print(e)
            with open("logs/errors.txt", "a") as f:
                f.write(traceback.format_exc())
            send_email("ERROR", traceback.format_exc(), finder.recipient)
        else:
            raise e
    # TO EXIT
    # taskkill /PID <your_pid> /F

