from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import time
import traceback

import pandas as pd
import yfinance as yf
import yaml

from db import database_enabled
import repository
from runtime_secrets import get_missing_email_secrets, load_runtime_secrets
from send_email import send_email, send_table


PROJECT_ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = PROJECT_ROOT.parent


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path

    candidates = [Path.cwd() / path, PROJECT_ROOT / path, WORKSPACE_ROOT / path]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return PROJECT_ROOT / path


class BargainFinder:
    """Finds bargain and sell opportunities from market price data."""

    def __init__(self, config_path: str = "meta/config.yaml"):
        self.config_path = config_path
        self.runtime_secrets = load_runtime_secrets()
        self.use_database = database_enabled()
        self._read_config(self.config_path, initialize=True)
        self._set_logging()
        self._log_runtime_setup()

    def _save_attributes(self) -> dict:
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith("_") and not callable(value)
        }

    def _read_config(self, config_path: str, initialize: bool = False) -> None:
        config = {}
        resolved_config = resolve_path(os.getenv("BARGAINFINDER_CONFIG", config_path))
        if resolved_config.exists():
            with open(resolved_config, "r", encoding="utf-8") as file_handle:
                config = yaml.safe_load(file_handle) or {}

        old_attrs = self._save_attributes() if not initialize else {}

        self.tol = float(os.getenv("BARGAIN_TOL", config.get("tol", -20)))
        self.email_rate = int(
            os.getenv("BARGAIN_EMAIL_RATE", config.get("email_rate", 7))
        )
        self.high_price_period = os.getenv(
            "BARGAIN_HIGH_PRICE_PERIOD",
            config.get("high_price_period", "500d"),
        )
        self.ticker_path = config.get("ticker_path", "meta/bargain_tickers.txt")
        self.htick_path = config.get("htick_path", "meta/high_tickers.txt")
        self.prices_path = config.get("prices_path", "data/prices.csv")
        self.raw_prices_path = config.get("raw_prices_path", "data/raw_prices.csv")
        self.history_path = config.get("history_path", "data/bargain_history.csv")
        self.update_log_path = config.get("update_log_path", "logs/update_log.txt")
        self.log_path = config.get("log_path", "logs/bargain_finder_{}.log")
        self.bargain_subject = os.getenv(
            "BARGAIN_SUBJECT",
            config.get("bargain_subject", "Weekly Bargain Report"),
        )
        self.sell_subject = os.getenv(
            "SELL_SUBJECT",
            config.get("sell_subject", "Sell Opportunity Found!"),
        )
        self.recipient = os.getenv(
            "REPORT_RECIPIENT",
            config.get("recipient", ""),
        )
        raw_max = os.getenv("BARGAIN_MAX_TICKERS", config.get("max_tickers"))
        self.max_tickers = int(raw_max) if raw_max is not None else None
        self.to_sell_tickers = None

        if not initialize:
            new_attrs = self._save_attributes()
            for key, value in new_attrs.items():
                if old_attrs.get(key) != value:
                    self.logger.info(
                        "Attribute '%s' changed from %s to %s",
                        key,
                        old_attrs.get(key),
                        value,
                    )

    def _set_logging(self) -> None:
        log_file = resolve_path(self.log_path.format("persistent"))
        log_file.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger("bargain_finder")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        if not self.logger.handlers:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setFormatter(
                logging.Formatter(
                    "[%(asctime)s] %(levelname)s in %(funcName)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            self.logger.addHandler(file_handler)
            self.logger.addHandler(stream_handler)

        self.logger.info("Program initialized with PID %s", os.getpid())
        self.logger.info(
            "BargainFinder initialized with tol=%s, email_rate=%s",
            self.tol,
            self.email_rate,
        )
        self.logger.info(
            "Database persistence %s",
            "enabled" if self.use_database else "disabled",
        )

    def _log_runtime_setup(self) -> None:
        loaded_from = self.runtime_secrets.get("loaded_from")
        if loaded_from is None:
            searched = ", ".join(
                str(path) for path in self.runtime_secrets.get("searched_paths", [])
            )
            self.logger.info(
                "No runtime secrets file found. Checked: %s",
                searched,
            )
        else:
            self.logger.info(
                "Loaded runtime secrets from '%s' (%s keys).",
                loaded_from,
                len(self.runtime_secrets.get("loaded_keys", [])),
            )

        missing_email = get_missing_email_secrets(self.recipient)
        if missing_email:
            self.logger.warning(
                "Email delivery is not fully configured. Missing settings: %s",
                ", ".join(missing_email),
            )

    def _can_send_email(self, context: str) -> bool:
        missing = get_missing_email_secrets(self.recipient)
        if missing:
            self.logger.warning(
                (
                    "Skipping %s email; missing settings: %s. "
                    "Populate meta/runtime_secrets.env or set environment variables."
                ),
                context,
                ", ".join(missing),
            )
            return False
        return True

    def get_tickers(self, file_path: str = None) -> list[str]:
        target_path = file_path or self.ticker_path
        if self.use_database:
            list_type = "sell" if target_path == self.htick_path else "bargain"
            tickers = repository.load_watchlist(list_type)
            if tickers:
                if self.max_tickers is not None:
                    tickers = tickers[: self.max_tickers]
                    self.logger.info(
                        "max_tickers=%s applied; using %s tickers",
                        self.max_tickers,
                        len(tickers),
                    )
                self.logger.info(
                    "Loaded %s %s tickers from Neon", len(tickers), list_type
                )
                return tickers

        resolved_path = resolve_path(target_path)
        with open(resolved_path, "r", encoding="utf-8") as file_handle:
            tickers = [line.strip().upper() for line in file_handle if line.strip()]
        self.logger.info("Loaded %s tickers from %s", len(tickers), resolved_path)
        if self.max_tickers is not None:
            tickers = tickers[: self.max_tickers]
            self.logger.info(
                "max_tickers=%s applied; using %s tickers",
                self.max_tickers,
                len(tickers),
            )
        return tickers

    def refresh_history(self) -> None:
        if self.use_database:
            raise RuntimeError(
                "refresh_history is not implemented for database-backed runs."
            )

        if os.isatty(0):
            input("Press 'Enter' to clear the files.")

        paths_to_clear = [
            self.history_path,
            self.prices_path,
            self.raw_prices_path,
            self.update_log_path,
        ]
        for path_str in paths_to_clear:
            path = resolve_path(path_str)
            if path.exists():
                path.unlink()
                self.logger.info("Cleared file: '%s'", path)

    def _need_to_redownload(self, file_path: str) -> bool:
        try:
            last_modified = resolve_path(file_path).stat().st_mtime
            last_update = pd.to_datetime(last_modified, unit="s").normalize()
            return (pd.to_datetime("today").normalize() - last_update).days > 1
        except FileNotFoundError:
            return True

    def _period_to_days(self, period: str) -> int:
        cleaned = (period or "").strip().lower()
        if cleaned.endswith("mo"):
            return max(1, int(cleaned[:-2])) * 30
        if cleaned.endswith("wk"):
            return max(1, int(cleaned[:-2])) * 7
        if cleaned.endswith("y"):
            return max(1, int(cleaned[:-1])) * 365
        if cleaned.endswith("d"):
            return max(1, int(cleaned[:-1]))
        return 365

    def _expected_latest_price_date(self) -> pd.Timestamp:
        expected = pd.to_datetime("now").normalize()
        while expected.weekday() >= 5:
            expected -= pd.Timedelta(days=1)
        return expected

    def _normalize_history_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame()

        normalized = frame.copy()
        if "Date" in normalized.columns:
            normalized["Date"] = pd.to_datetime(normalized["Date"], errors="coerce")
            normalized = normalized.set_index("Date")

        normalized.index = pd.to_datetime(normalized.index, errors="coerce")
        normalized = normalized.loc[normalized.index.notna()]
        normalized = normalized.sort_index()
        normalized.columns = [str(column).upper() for column in normalized.columns]
        normalized = normalized.T.groupby(level=0).last().T
        return normalized

    def _load_cached_raw_history(self, tickers: list[str]) -> pd.DataFrame:
        if self.use_database:
            return self._normalize_history_frame(
                repository.get_raw_price_history(tickers)
            )

        path = resolve_path(self.raw_prices_path)
        if not path.exists():
            return pd.DataFrame(columns=tickers)

        stored = pd.read_csv(path)
        normalized = self._normalize_history_frame(stored)
        available = [ticker for ticker in tickers if ticker in normalized.columns]
        if not available:
            return pd.DataFrame(columns=tickers)
        return normalized[available]

    def _save_raw_price_history(self, history: pd.DataFrame) -> None:
        normalized = self._normalize_history_frame(history)
        if normalized.empty:
            return

        if self.use_database:
            repository.upsert_raw_price_history(normalized)
            self.logger.info("Saved raw price history to Neon")
            return

        path = resolve_path(self.raw_prices_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        normalized.reset_index().rename(columns={"index": "Date"}).to_csv(
            path, index=False
        )
        self.logger.info("Saved raw prices to '%s'", path)

    def _merge_price_history(
        self, existing: pd.DataFrame, updates: pd.DataFrame
    ) -> pd.DataFrame:
        normalized_existing = self._normalize_history_frame(existing)
        normalized_updates = self._normalize_history_frame(updates)
        if normalized_existing.empty:
            return normalized_updates
        if normalized_updates.empty:
            return normalized_existing

        combined = pd.concat([normalized_existing, normalized_updates], axis=1)
        combined = combined.loc[combined.index.notna()]
        combined = combined.sort_index()
        return combined.T.groupby(level=0).last().T

    def _plan_history_downloads(
        self,
        history: pd.DataFrame,
        tickers: list[str],
        required_start: pd.Timestamp,
        expected_latest: pd.Timestamp,
    ) -> tuple[list[str], dict[str, pd.Timestamp]]:
        normalized = self._normalize_history_frame(history)
        overlap_days = max(1, int(os.getenv("YFINANCE_REFRESH_OVERLAP_DAYS", "7")))
        full_refresh: list[str] = []
        incremental_refresh: dict[str, pd.Timestamp] = {}

        for ticker in tickers:
            if ticker not in normalized.columns:
                full_refresh.append(ticker)
                continue

            series = normalized[ticker].dropna()
            if series.empty:
                full_refresh.append(ticker)
                continue

            earliest = series.index.min().normalize()
            latest = series.index.max().normalize()
            if earliest > required_start:
                full_refresh.append(ticker)
                continue

            if latest < expected_latest:
                incremental_refresh[ticker] = max(
                    required_start,
                    latest - pd.Timedelta(days=overlap_days),
                )

        return full_refresh, incremental_refresh

    def _normalize_downloaded_close_data(
        self,
        close_data: pd.DataFrame | pd.Series,
        tickers: list[str],
    ) -> pd.DataFrame:
        if isinstance(close_data, pd.Series):
            column_name = tickers[0] if tickers else close_data.name or "Ticker"
            frame = close_data.to_frame(name=column_name)
        else:
            frame = close_data.copy()

        frame.index = pd.to_datetime(frame.index)
        frame = frame.sort_index()
        frame.columns = [str(column).upper() for column in frame.columns]
        return frame

    def _extract_close_data(
        self, downloaded: object
    ) -> pd.DataFrame | pd.Series | None:
        if downloaded is None:
            return None
        if isinstance(downloaded, dict):
            return downloaded.get("Close")
        if isinstance(downloaded, pd.Series):
            return downloaded
        if not isinstance(downloaded, pd.DataFrame):
            return None
        if isinstance(downloaded.columns, pd.MultiIndex):
            if "Close" not in downloaded.columns.get_level_values(0):
                return None
            return downloaded["Close"]
        if "Close" in downloaded.columns:
            return downloaded["Close"]
        if len(downloaded.columns) == 1:
            return downloaded
        return None

    def _get_price_snapshot_columns(self) -> list[str]:
        return [
            "Ticker",
            "3 Months",
            "1 Month",
            "Current",
            "3 Mo Change",
            "1 Mo Change",
        ]

    def _load_price_fallback(self, tickers: list[str]) -> pd.DataFrame:
        if self.use_database:
            fallback = repository.get_latest_price_snapshots()
            if not fallback.empty:
                self.logger.warning(
                    "Using latest cached Neon price snapshot after download failure."
                )
                return fallback
        else:
            path = resolve_path(self.prices_path)
            if path.exists():
                self.logger.warning(
                    "Using cached price snapshot from '%s' after download failure.",
                    path,
                )
                return pd.read_csv(path)

        return pd.DataFrame(columns=self._get_price_snapshot_columns())

    def _load_raw_price_fallback(self, tickers: list[str]) -> pd.DataFrame:
        if self.use_database:
            fallback = repository.get_raw_price_history(tickers)
            if not fallback.empty:
                self.logger.warning(
                    "Using cached Neon raw price history after download failure."
                )
                return fallback
        else:
            path = resolve_path(self.raw_prices_path)
            if path.exists():
                stored = pd.read_csv(path)
                if not stored.empty:
                    stored["Date"] = pd.to_datetime(stored["Date"], errors="coerce")
                    self.logger.warning(
                        "Using cached raw prices from '%s' after download failure.",
                        path,
                    )
                    return stored.set_index("Date")

        return pd.DataFrame(columns=tickers)

    def _download_close_history(
        self,
        tickers: list[str],
        period: str | None = None,
        start_dates: dict[str, pd.Timestamp] | None = None,
    ) -> pd.DataFrame:
        cleaned = [
            ticker.strip().upper() for ticker in tickers if ticker and ticker.strip()
        ]
        if not cleaned:
            return pd.DataFrame()

        batch_size = max(1, int(os.getenv("YFINANCE_BATCH_SIZE", "50")))
        request_pause = max(
            0.0,
            float(
                os.getenv(
                    "YFINANCE_REQUEST_PAUSE",
                    os.getenv("YFINANCE_BATCH_PAUSE", "2"),
                )
            ),
        )
        max_attempts = max(1, int(os.getenv("YFINANCE_MAX_ATTEMPTS", "4")))
        rate_limit_backoff = max(
            request_pause,
            float(os.getenv("YFINANCE_429_BACKOFF_BASE", "10")),
        )
        rate_limit_error = getattr(
            getattr(yf, "exceptions", None), "YFRateLimitError", None
        )

        frames: list[pd.DataFrame] = []
        missing_tickers: set[str] = set()
        next_request_at = 0.0

        for index, ticker in enumerate(cleaned, start=1):
            if next_request_at > 0:
                delay = next_request_at - time.monotonic()
                if delay > 0:
                    time.sleep(delay)

            downloaded_frame = pd.DataFrame()
            start_date = None if not start_dates else start_dates.get(ticker)
            request_label = (
                f"start={pd.to_datetime(start_date).date()}"
                if start_date is not None
                else f"period={period}"
            )

            for attempt in range(1, max_attempts + 1):
                try:
                    download_args = {
                        "interval": "1d",
                        "auto_adjust": True,
                        "progress": False,
                        "threads": False,
                    }
                    if start_date is not None:
                        download_args["start"] = pd.to_datetime(start_date).strftime(
                            "%Y-%m-%d"
                        )
                    elif period is not None:
                        download_args["period"] = period
                    else:
                        raise ValueError("download requires either a period or start")

                    downloaded = yf.download(ticker, **download_args)
                    next_request_at = time.monotonic() + request_pause
                    close_data = self._extract_close_data(downloaded)
                    if close_data is None:
                        raise ValueError("download returned no close data")

                    downloaded_frame = self._normalize_downloaded_close_data(
                        close_data, [ticker]
                    )
                except Exception as exc:
                    next_request_at = max(
                        next_request_at,
                        time.monotonic() + request_pause,
                    )
                    is_rate_limited = (
                        (
                            rate_limit_error is not None
                            and isinstance(exc, rate_limit_error)
                        )
                        or "Too Many Requests" in str(exc)
                        or "429" in str(exc)
                    )
                    self.logger.warning(
                        "Price download attempt %s/%s failed for %s (%s): %s",
                        attempt,
                        max_attempts,
                        ticker,
                        request_label,
                        exc,
                    )
                    if attempt < max_attempts:
                        if is_rate_limited:
                            wait_seconds = rate_limit_backoff * (2 ** (attempt - 1))
                        else:
                            wait_seconds = max(request_pause, float(attempt))
                        self.logger.info(
                            "Waiting %.1f seconds before retrying %s",
                            wait_seconds,
                            ticker,
                        )
                        time.sleep(wait_seconds)
                    continue

                if downloaded_frame.empty or downloaded_frame.index.empty:
                    self.logger.warning(
                        ("Price download attempt %s/%s returned no rows for %s (%s)."),
                        attempt,
                        max_attempts,
                        ticker,
                        request_label,
                    )
                    if attempt < max_attempts:
                        time.sleep(max(request_pause, float(attempt)))
                    continue
                break

            if downloaded_frame.empty or downloaded_frame.index.empty:
                missing_tickers.add(ticker)
            else:
                frames.append(downloaded_frame)

            if index % batch_size == 0 or index == len(cleaned):
                self.logger.info(
                    "Processed %s/%s ticker downloads",
                    index,
                    len(cleaned),
                )

        if not frames:
            self.logger.warning(
                "Unable to download close-price history for any of the requested tickers."
            )
            return pd.DataFrame()

        combined = pd.concat(frames, axis=1)
        combined = combined.loc[combined.index.notna()]
        combined = combined.sort_index()
        combined = combined.T.groupby(level=0).last().T

        if missing_tickers:
            preview = ", ".join(sorted(missing_tickers)[:10])
            suffix = "" if len(missing_tickers) <= 10 else ", ..."
            self.logger.warning(
                "Missing close-price data for %s tickers: %s%s",
                len(missing_tickers),
                preview,
                suffix,
            )

        return combined

    def _build_price_snapshot_frame(
        self, data: pd.DataFrame, tickers: list[str], run_date: pd.Timestamp
    ) -> pd.DataFrame:
        normalized = self._normalize_history_frame(data)
        frame = pd.DataFrame(columns=self._get_price_snapshot_columns())
        if normalized.empty or normalized.index.empty:
            return frame

        frame["Ticker"] = tickers
        d30 = run_date - pd.Timedelta(days=30)
        d90 = run_date - pd.Timedelta(days=90)

        for label, target_date in zip(
            ["Current", "1 Month", "3 Months"],
            [run_date, d30, d90],
        ):
            nearest_date = normalized.index[
                (abs(normalized.index - target_date)).argmin()
            ]
            prices = normalized.loc[nearest_date]
            frame[label] = frame["Ticker"].map(prices)

        missing_rows = frame[
            frame[["Current", "1 Month", "3 Months"]].isna().any(axis=1)
        ]
        if not missing_rows.empty:
            self.logger.warning(
                "Dropping %s tickers with incomplete price snapshots.",
                len(missing_rows),
            )
            frame = frame.drop(index=missing_rows.index)

        if frame.empty:
            return pd.DataFrame(columns=self._get_price_snapshot_columns())

        frame["3 Mo Change"] = (
            (frame["Current"] - frame["3 Months"]) / frame["3 Months"] * 100
        )
        frame["1 Mo Change"] = (
            (frame["Current"] - frame["1 Month"]) / frame["1 Month"] * 100
        )
        return frame

    def _update_prices(self, force_redownload: bool = False) -> pd.DataFrame:
        tickers = [ticker.upper() for ticker in self.get_tickers()]
        run_date = pd.to_datetime("today").normalize()

        if self.use_database and not force_redownload:
            existing = repository.get_latest_price_snapshots(run_date=run_date)
            if not existing.empty:
                self.logger.info("Loaded prices from Neon for %s", run_date.date())
                return existing

        use_cached_prices = not self.use_database and not force_redownload
        use_cached_prices = use_cached_prices and not self._need_to_redownload(
            self.prices_path
        )
        if use_cached_prices:
            path = resolve_path(self.prices_path)
            frame = pd.read_csv(path)
            self.logger.info("Loaded prices from '%s'", path)
            return frame

        raw_history = pd.DataFrame(columns=tickers)
        if not force_redownload:
            raw_history = self._load_cached_raw_history(tickers)

        expected_latest = self._expected_latest_price_date()
        required_start = expected_latest - pd.Timedelta(
            days=self._period_to_days("100d")
        )
        if force_redownload:
            full_refresh = tickers
            incremental_refresh = {}
        else:
            full_refresh, incremental_refresh = self._plan_history_downloads(
                raw_history,
                tickers,
                required_start.normalize(),
                expected_latest,
            )

        if full_refresh or incremental_refresh:
            self.logger.info(
                (
                    "Refreshing raw price history for %s tickers "
                    "(%s full, %s incremental)"
                ),
                len(full_refresh) + len(incremental_refresh),
                len(full_refresh),
                len(incremental_refresh),
            )
            full_history = self._download_close_history(full_refresh, period="100d")
            incremental_history = self._download_close_history(
                list(incremental_refresh),
                start_dates=incremental_refresh,
            )
            raw_history = self._merge_price_history(raw_history, full_history)
            raw_history = self._merge_price_history(raw_history, incremental_history)
            if not raw_history.empty:
                self._save_raw_price_history(raw_history)

        if raw_history.empty or raw_history.index.empty:
            fallback = self._load_price_fallback(tickers)
            if not fallback.empty:
                return fallback
            raise RuntimeError(
                "Unable to download market data from yfinance and no cached "
                "price snapshot is available."
            )

        frame = self._build_price_snapshot_frame(raw_history, tickers, run_date)
        if frame.empty:
            fallback = self._load_price_fallback(tickers)
            if not fallback.empty:
                return fallback
            raise RuntimeError(
                "Downloaded market data did not contain enough price history to build a snapshot."
            )

        if self.use_database:
            repository.record_price_snapshots(frame, run_date=run_date)
            self.logger.info("Saved %s price snapshots to Neon", len(frame))
        else:
            path = resolve_path(self.prices_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_csv(path, index=False)
            self.logger.info("Saved prices to '%s'", path)
        return frame

    def find_current_bargains(self, force_redownload: bool = False) -> pd.DataFrame:
        frame = self._update_prices(force_redownload)
        columns = ["Date"] + [column for column in frame.columns if column != "Date"]
        bargains = frame[frame["1 Mo Change"] < self.tol].copy()
        if bargains.empty:
            self.logger.info("No bargains found.")
            return pd.DataFrame(columns=columns)

        today = pd.to_datetime("today").normalize()
        bargains.loc[:, "Date"] = today

        if self.use_database:
            repository.append_bargain_history(bargains, run_date=today)
            self.logger.info("Found %s bargains, saved to Neon", len(bargains))
            return bargains

        history_path = resolve_path(self.history_path)
        if history_path.exists():
            history_frame = pd.read_csv(history_path)
        else:
            history_frame = pd.DataFrame(columns=columns)

        combined = pd.concat(
            [
                candidate
                for candidate in [history_frame, bargains]
                if not candidate.empty and not candidate.isna().all().all()
            ],
            ignore_index=True,
        )
        history_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(history_path, index=False)
        self.logger.info(
            "Found %s bargains, saved to '%s'", len(bargains), history_path
        )
        return bargains

    def _update_sell_tracking(self, force_redownload: bool = False) -> pd.DataFrame:
        if not self.to_sell_tickers:
            self.to_sell_tickers = self.get_tickers(self.htick_path)

        tickers = [ticker.upper() for ticker in self.to_sell_tickers]

        raw_history = pd.DataFrame(columns=tickers)
        if not force_redownload:
            raw_history = self._load_cached_raw_history(tickers)

        expected_latest = self._expected_latest_price_date()
        required_start = expected_latest - pd.Timedelta(
            days=self._period_to_days(self.high_price_period)
        )
        if force_redownload:
            full_refresh = tickers
            incremental_refresh = {}
        else:
            full_refresh, incremental_refresh = self._plan_history_downloads(
                raw_history,
                tickers,
                required_start.normalize(),
                expected_latest,
            )

        if full_refresh or incremental_refresh:
            self.logger.info(
                (
                    "Refreshing sell-tracking history for %s tickers "
                    "(%s full, %s incremental)"
                ),
                len(full_refresh) + len(incremental_refresh),
                len(full_refresh),
                len(incremental_refresh),
            )
            full_history = self._download_close_history(
                full_refresh,
                period=self.high_price_period,
            )
            incremental_history = self._download_close_history(
                list(incremental_refresh),
                start_dates=incremental_refresh,
            )
            raw_history = self._merge_price_history(raw_history, full_history)
            raw_history = self._merge_price_history(raw_history, incremental_history)
            if not raw_history.empty:
                self._save_raw_price_history(raw_history)

        if raw_history.empty or raw_history.index.empty:
            return self._load_raw_price_fallback(tickers)

        return raw_history

    def _check_for_extremes(self, force_redownload: bool = False) -> tuple[bool, dict]:
        data = self._update_sell_tracking(force_redownload)
        if data.empty:
            self.logger.info("No raw price history available for sell tracking.")
            return False, {}

        if "Date" in data.columns:
            data["Date"] = pd.to_datetime(data["Date"], errors="coerce")
            data = data.set_index("Date")
        data.index = pd.to_datetime(data.index)

        ticker_stats = {}
        sell_any = False
        for ticker in self.to_sell_tickers:
            analysis = {}
            if ticker not in data.columns:
                self.logger.warning("Ticker '%s' not found in downloaded data.", ticker)
                ticker_stats[ticker] = analysis
                continue

            series = data[ticker].dropna()
            if series.empty:
                self.logger.warning("Ticker '%s' has no raw price history.", ticker)
                ticker_stats[ticker] = analysis
                continue

            current_price = float(series.iloc[-1])
            analysis["Current"] = current_price
            d30 = pd.to_datetime("today").normalize() - pd.Timedelta(days=30)
            past_prices = series[series.index <= d30]

            if not past_prices.empty:
                high_price = float(past_prices.max())
                analysis["High"] = high_price
                analysis["dHigh"] = (current_price - high_price) / high_price * 100
            else:
                analysis["High"] = current_price
                analysis["dHigh"] = 0.0

            analysis["Sell"] = analysis["dHigh"] > 0
            if analysis["Sell"]:
                sell_any = True

            self.logger.info(
                "Sell signal [%s] for %s: Current=%s, High=%s, dHigh=%.2f%%",
                analysis["Sell"],
                ticker,
                current_price,
                analysis["High"],
                analysis["dHigh"],
            )
            ticker_stats[ticker] = analysis

        return sell_any, ticker_stats

    def create_sell_report(
        self,
        tickers: list[str] = None,
        save_file: str = None,
        force_redownload: bool = False,
    ) -> None:
        if save_file is not None:
            self.raw_prices_path = save_file
        if tickers is None:
            self.to_sell_tickers = self.get_tickers(self.htick_path)
        else:
            self.to_sell_tickers = tickers

        sell_any, ticker_stats = self._check_for_extremes(force_redownload)
        if not sell_any:
            self.logger.info("No sell opportunities found.")
            return

        report = pd.DataFrame.from_dict(ticker_stats, orient="index")
        report.reset_index(inplace=True)
        report.rename(columns={"index": "Ticker"}, inplace=True)
        report = report[report["Sell"]]
        report_email = self.format_report(report)
        self.logger.info("Created sell report with %s stocks.", len(report))

        if not self._can_send_email("sell report"):
            return

        sent, _ = send_table(self.sell_subject, report_email, self.recipient)
        if sent:
            self.logger.info("Sell report sent to '%s'", self.recipient)
        else:
            self.logger.warning("Sell report email send failed.")

    def _get_recent_bargains(self, period_length: int = 7) -> pd.DataFrame:
        if self.use_database:
            bargains = repository.get_recent_bargains(period_length)
            if bargains.empty:
                self.logger.warning("No bargains found in Neon history.")
            else:
                self.logger.info(
                    "Found %s bargains in the last %s days.",
                    len(bargains),
                    period_length,
                )
            return bargains

        history_path = resolve_path(self.history_path)
        if not history_path.exists():
            self.logger.warning("History file not found: '%s'", history_path)
            return pd.DataFrame()

        history_frame = pd.read_csv(history_path)
        if "Date" in history_frame.columns:
            history_frame["Date"] = pd.to_datetime(
                history_frame["Date"],
                format="%Y-%m-%d %H:%M:%S",
                errors="coerce",
            )
        if history_frame.empty:
            self.logger.warning("No bargains found in history.")
            return pd.DataFrame()

        last_week = pd.to_datetime("today").normalize() - pd.Timedelta(
            days=period_length
        )
        weekly_bargains = history_frame[history_frame["Date"] >= last_week]
        self.logger.info(
            "Found %s bargains in the last %s days.",
            len(weekly_bargains),
            period_length,
        )
        return weekly_bargains

    def format_report(self, report: pd.DataFrame) -> str:
        google_finance_link = "https://www.google.com/finance/quote/{}:NYSE?window=6M"
        brave_search_link = (
            "https://search.brave.com/search?q={}+stock&rh_type=st&range=ytd"
        )

        report = report.copy()
        report["Google Finance"] = report["Ticker"].apply(
            lambda ticker: (
                f'<a href="{google_finance_link.format(ticker)}" target="_blank">Open</a>'
            )
        )
        report["Brave Search"] = report["Ticker"].apply(
            lambda ticker: (
                f'<a href="{brave_search_link.format(ticker)}" target="_blank">Open</a>'
            )
        )
        report["Ticker"] = report["Ticker"].apply(lambda ticker: f"<b>{ticker}</b>")
        return report.to_html(escape=False, index=False, justify="center", border=1)

    def create_bargain_report(self) -> None:
        bargains = self._get_recent_bargains(self.email_rate)
        if bargains.empty:
            self.logger.info(
                "Weekly report skipped because there were no recent bargains."
            )
            return

        ticker_counts = bargains["Ticker"].value_counts().reset_index()
        ticker_counts.columns = ["Ticker", "Count"]

        avg_changes = (
            bargains.groupby("Ticker")
            .agg({"3 Mo Change": "mean", "1 Mo Change": "mean"})
            .reset_index()
        )
        avg_changes.columns = ["Ticker", "Avg 3 Mo Change", "Avg 1 Mo Change"]
        avg_changes["Avg 3 Mo Change"] = avg_changes["Avg 3 Mo Change"].map(
            lambda value: f"{value:.2f}%"
        )
        avg_changes["Avg 1 Mo Change"] = avg_changes["Avg 1 Mo Change"].map(
            lambda value: f"{value:.2f}%"
        )

        report = pd.merge(ticker_counts, avg_changes, on="Ticker")
        report_email = self.format_report(report)
        self.logger.info("Created bargain report with %s bargains.", len(report))

        if not self._can_send_email("weekly bargain report"):
            return

        sent, _ = send_table(self.bargain_subject, report_email, self.recipient)
        if sent:
            self.logger.info("Bargain report sent to '%s'", self.recipient)
        else:
            self.logger.warning("Bargain report email send failed.")

    def _execute(self) -> int:
        self.logger.info("Executing BargainFinder routine")
        bargains = self.find_current_bargains()
        self.create_bargain_report()

        if self.use_database:
            repository.set_last_update(pd.to_datetime("now"))
        else:
            update_log_path = resolve_path(self.update_log_path)
            update_log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(update_log_path, "w", encoding="utf-8") as file_handle:
                file_handle.write(pd.to_datetime("now").isoformat())

        self.logger.info("Update logged at %s", pd.to_datetime("now").isoformat())
        return len(bargains)

    def run(self) -> int:
        self.logger.info("Running BargainFinder")
        self._read_config(self.config_path)
        return self._execute()


def lambda_handler(event, context):
    del event, context

    finder = BargainFinder()
    started_at = datetime.now(timezone.utc)
    bargain_count = 0
    try:
        bargain_count = finder.run()
    except KeyboardInterrupt:
        print("BargainFinder stopped by user.")
        if finder.use_database:
            repository.record_execution(
                started_at,
                datetime.now(timezone.utc),
                "cancelled",
                bargains_found=bargain_count,
            )
        return {"statusCode": 499, "body": "Cancelled"}
    except Exception:
        error_trace = traceback.format_exc()
        if finder.use_database:
            repository.record_execution(
                started_at,
                datetime.now(timezone.utc),
                "failed",
                bargains_found=bargain_count,
                error_text=error_trace,
            )

        errors_path = resolve_path("logs/errors.txt")
        errors_path.parent.mkdir(parents=True, exist_ok=True)
        with open(errors_path, "a", encoding="utf-8") as file_handle:
            file_handle.write(error_trace)
        if finder._can_send_email("error alert"):
            send_email("ERROR", error_trace, finder.recipient)
        raise
    else:
        if finder.use_database:
            repository.record_execution(
                started_at,
                datetime.now(timezone.utc),
                "succeeded",
                bargains_found=bargain_count,
            )
        return {"statusCode": 200, "body": "Success"}


if __name__ == "__main__":
    lambda_handler({}, None)
