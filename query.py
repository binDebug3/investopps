import yfinance as yf
import pandas as pd
import pdb
import os
from send_email import send_table
import time
from datetime import datetime
import argparse
import logging



class BargainFinder(object):
    """
    A class to find stock bargains based on price changes.
    
    Attributes:
    tol: float
        The percentage drop threshold to consider a stock a bargain.
    
    history_path: str
        Path to the CSV file where bargain history will be saved.
    """
    
    def __init__(self, tol: float, 
                 period_length: int,
                 update_hour: int,
                 ticker_path: str = "meta/tickers.txt",
                 prices_path: str = "data/prices.csv", 
                 history_path: str = "data/bargain_history.csv", 
                 update_log: str = "meta/update_log.txt"):
        self.tol = tol
        self.period_length = period_length
        self.update_hour = update_hour
        self.history_path = history_path
        self.prices_path = prices_path
        self.ticker_path = ticker_path
        self.update_log = update_log
        self.subject = "Weekly Bargain Report"
        self.recipient = self.get_personal_email()
        self.update_time = None
        self.set_logging()
        self.logger.info(f"Program initialized with PID {os.getpid()}")
        self.logger.info("BargainFinder initialized with tol=%s, period_length=%s, update_hour=%s",
                         tol, period_length, update_hour)
    
    
    def get_personal_email(self, email_path: str = "meta/personal_email.txt") -> str:
        """
        Read the personal email address from a file.
        
        Returns:
        personal_email: str
            The email address read from the file.
        """
        if not os.path.exists(email_path):
            raise FileNotFoundError(f"Email file not found: {email_path}")
        with open(email_path, 'r') as file:
            personal_email = file.readline().strip()
        if not personal_email:
            raise ValueError("Email file is empty or does not contain a valid email address.")
        return personal_email
    
    
    def set_logging(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Only add handlers once (avoid duplicates when re-instantiating)
        if not self.logger.handlers:
            now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file: str = f"logs/bargain_finder_{now_str}.log"
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    
    def get_tickers(self) -> list:
        """
        Read tickers from a file.
        """
        with open(self.ticker_path, 'r') as file:
            tickers = [line.strip() for line in file if line.strip()]
        self.logger.info(f"Loaded {len(tickers)} tickers from {self.ticker_path}")
        return tickers
    
    
    def refresh_history(self) -> None:
        """
        Refresh the history file by removing all entries.
        """
        # Warn the user if there is an active terminal session to print to
        if os.isatty(0):
            input("Press 'Enter' to clear the bargain history file.")
        
        if os.path.exists(self.history_path):
            os.remove(self.history_path)
            self.logger.info(f"Cleared history file: {self.history_path}")


    def _need_to_redownload(self) -> bool:
        """
        Check if the prices file exists and what the last update date is.
        If the file does not exist or is older than 1 day, return True.
        
        Parameters:
        prices_path: str
            Path to the CSV file containing prices.
        
        Returns:
        redownload: bool
            True if the file needs to be redownloaded, False otherwise.
        """
        try:
            last_modified = os.path.getmtime(self.prices_path)
            last_update = pd.to_datetime(last_modified, unit='s').normalize()
            return (pd.to_datetime("today").normalize() - last_update).days > 1
        except (FileNotFoundError, KeyError):
            return True


    def _update_prices(self, force_redownload: bool = False) -> pd.DataFrame:
        """
        Update prices for all tickers in the ticker file.
        
        Parameters:
        prices_path: str
            Path to the CSV file where prices will be saved.
        
        force_redownload: bool
            If True, forces redownload of prices even if the file exists.
        
        Returns:
        price_info: pd.DataFrame
            DataFrame containing the latest prices and changes for each ticker.
        """
        
        if force_redownload or self._need_to_redownload():
            # Get tickers from the file
            tickers = self.get_tickers()
            df = pd.DataFrame(columns=['Ticker', "3 Months", "1 Month", "Current", "3 Mo Change", "1 Mo Change"])
            df['Ticker'] = tickers
            
            # Date anchors
            today = pd.to_datetime("today").normalize()
            d30 = today - pd.Timedelta(days=30)
            d90 = today - pd.Timedelta(days=90)
            
            # Download past ~100 days to ensure we cover the needed range
            self.logger.info(f"Downloading prices for {len(tickers)} tickers")
            data = yf.download(tickers, period="100d", interval="1d", auto_adjust=True)["Close"]

            # Transpose the data so tickers are rows
            for label, target_date in zip(["Current", "1 Month", "3 Months"], [today, d30, d90]):
                # Get the actual market date closest to the target date
                actual_dates = data.index
                nearest_date = actual_dates[(abs(actual_dates - target_date)).argmin()]
                
                # Extract prices for that date
                prices = data.loc[nearest_date]
                df[label] = df['Ticker'].map(prices)
                
            # Calculate changes
            df["3 Mo Change"] = (df["Current"] - df["3 Months"]) / df["3 Months"] * 100
            df["1 Mo Change"] = (df["Current"] - df["1 Month"]) / df["1 Month"] * 100

            # save and return
            self.logger.info(f"Saving prices to '{self.prices_path}'")
            df.to_csv(self.prices_path, index=False)
        else:
            df = pd.read_csv(self.prices_path)
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
        df = self._update_prices(force_redownload)
        cols = ['Date'] + [col for col in df.columns if col != 'Date']
        bargains = df[df["1 Mo Change"] < self.tol].copy()
        if bargains.empty:
            self.logger.info("No bargains found.")
            return pd.DataFrame(columns=cols)
        
        # get todays date and time
        today = pd.to_datetime("today").normalize()
        bargains.loc[:, 'Date'] = today
        
        # initialize history DataFrame if it doesn't exist
        if os.path.exists(self.history_path):
            hdf = pd.read_csv(self.history_path) 
        else:
            hdf = pd.DataFrame(columns=cols)
        
        # append new bargains to history, save, return
        frames = [df for df in [hdf, bargains] if not df.empty and not df.isna().all().all()]
        hdf = pd.concat(frames, ignore_index=True)
        hdf.to_csv(self.history_path, index=False)
        self.logger.info(f"Found {len(bargains)} bargains, saved to '{self.history_path}'")
        return bargains
        
        
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
        
        hdf = pd.read_csv(self.history_path, parse_dates=['Date'])
        if hdf.empty:
            self.logger.warning("No bargains found in history.")
            return pd.DataFrame()
            
        # Filter for the last week
        last_week = pd.to_datetime("today").normalize() - pd.Timedelta(days=period_length)
        weekly_bargains = hdf[hdf['Date'] >= last_week]
        self.logger.info(f"Found {len(weekly_bargains)} bargains in the last {period_length} days.")
        return weekly_bargains


    def create_report(self) -> None:
        """
        Create a weekly report of bargains found and save it to a file.
        """
        bargains = self._get_recent_bargains(self.period_length)
        if bargains.empty: return
        
        # Count the number of times each unique ticker appears
        ticker_counts = bargains['Ticker'].value_counts().reset_index()
        ticker_counts.columns = ['Ticker', 'Count']

        # find the average change for each ticker, rounded to two decimals
        avg_changes = bargains.groupby('Ticker').agg({'3 Mo Change': 'mean', '1 Mo Change': 'mean'}).reset_index()
        avg_changes.columns = ['Ticker', 'Avg 3 Mo Change', 'Avg 1 Mo Change']
        avg_changes['Avg 3 Mo Change'] = avg_changes['Avg 3 Mo Change'].map(lambda x: f"{x:.2f}%")
        avg_changes['Avg 1 Mo Change'] = avg_changes['Avg 1 Mo Change'].map(lambda x: f"{x:.2f}%")

        # Merge the counts and average changes
        report = pd.merge(ticker_counts, avg_changes, on='Ticker')
        
        
        google_finance_link = "https://www.google.com/finance/quote/{}:NYSE?window=6M"
        brave_search_link = "https://search.brave.com/search?q={}+stock&rh_type=st&range=ytd"
        
        # insert columns with links
        report['Google Finance'] = report['Ticker'].apply(
            lambda x: f'<a href="{google_finance_link.format(x)}" target="_blank">Open</a>'
        )
        report['Brave Search'] = report['Ticker'].apply(
            lambda x: f'<a href="{brave_search_link.format(x)}" target="_blank">Open</a>'
        )
        
        # Make Ticker column bold in HTML
        report['Ticker'] = report['Ticker'].apply(lambda x: f"<b>{x}</b>")
        report_email = report.to_html(escape=False, index=False, justify="center", border=1)
        self.logger.info(f"Created report with {len(report)} bargains.")
        send_table(self.subject, report_email, self.recipient)
        self.logger.info(f"Report sent to '{self.recipient}'")


    def _is_market_day(self, dt: pd.Timestamp) -> bool:
        saturday = 5
        return dt.weekday() < saturday  # Mon–Fri


    def _has_updated_today(self) -> bool:
        if not os.path.exists(self.update_log):
            return False
        with open(self.update_log, 'r') as file:
            last_update = pd.to_datetime(file.readline().strip())
        return last_update.date() == pd.to_datetime("now").date()


    def _wait_until(self, target_time: datetime):
        now = pd.to_datetime("now")
        wait_seconds = (target_time - now).total_seconds()
        if wait_seconds > 0:
            self.logger.info(f"Sleeping until {target_time.strftime('%Y-%m-%d %H:%M:%S')} ({wait_seconds:.0f} seconds)")
            time.sleep(wait_seconds)


    def _wait_til_tmr(self, 
                      now: pd.Timestamp) -> pd.Timestamp:
        if pd.to_datetime("now") > self.update_time:
            # If it's already past 3 PM, wait until next weekday
            next_day = now + pd.Timedelta(days=1)
            while not self._is_market_day(next_day):
                next_day += pd.Timedelta(days=1)
            self.update_time = next_day + pd.Timedelta(hours=self.update_hour)


    def _execute(self, day_of_week: int = 3):
        self.logger.info("Executing BargainFinder routine")
        self._find_current_bargains()

        if self.update_time.weekday() == day_of_week:  # Thursday
            self.logger.info("Creating weekly report")
            report_html = self.create_report()
            if report_html:
                send_table(self.subject, report_html, self.recipient)
            
            # Log update
            with open(self.update_log, 'w') as file:
                file.write(pd.to_datetime("now").isoformat())
            self.logger.info(f"Update logged at {pd.to_datetime('now').isoformat()}")


    def run(self):
        """
        Run the BargainFinder to find bargains every weekday at 3 PM and create a report every Thursday at 3 PM.
        """
        update_count = 0
        iter_count = 0
        start_date = pd.to_datetime("now").normalize()
        while True:
            now = pd.to_datetime("now").normalize()
            self.update_time = now + pd.Timedelta(hours=self.update_hour)  # Today at 3 PM
            self._wait_til_tmr(now)
            self._wait_until(self.update_time)

            if self._is_market_day(self.update_time) and not self._has_updated_today():
                self._execute()
                update_count += 1
            iter_count += 1
            days_since_start = (now - start_date).days
            self.logger.info(f"Iteration {iter_count}: Updated {update_count} times in {days_since_start} days\n\n\n")
            


def parse_args():
    parser = argparse.ArgumentParser(description="Run the BargainFinder script.")
    parser.add_argument("--tol", type=float, default=-25,
                        help="Threshold percentage drop to consider a stock a bargain (default: -25)")
    parser.add_argument("--period", type=int, default=7,
                        help="Number of days to look back for bargains (default: 7)")
    parser.add_argument("--update_hour", type=int, default=15,
                        help="Hour of the day to check for updates (default: 15 for 3 PM)")
    return parser.parse_args()



if __name__ == "__main__":
    args = parse_args()
    finder = BargainFinder(tol=args.tol, 
                           period_length=args.period, 
                           update_hour=args.update_hour)
    try:
        finder.run()
    except KeyboardInterrupt:
        print("BargainFinder stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
        
    # TO EXIT
    # taskkill /PID <your_pid> /F

