import logging
import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


fake_psycopg = types.ModuleType("psycopg")
fake_psycopg.Connection = object
fake_psycopg.connect = lambda *args, **kwargs: None
fake_psycopg_rows = types.ModuleType("psycopg.rows")
fake_psycopg_rows.dict_row = object()
fake_yfinance = types.ModuleType("yfinance")


class FakeRateLimitError(Exception):
    pass


fake_yfinance.download = lambda *args, **kwargs: None
fake_yfinance.exceptions = types.SimpleNamespace(YFRateLimitError=FakeRateLimitError)
sys.modules.setdefault("psycopg", fake_psycopg)
sys.modules.setdefault("psycopg.rows", fake_psycopg_rows)
sys.modules.setdefault("yfinance", fake_yfinance)

query = importlib.import_module("query")


def _fake_set_logging(self):
    logger_name = f"bargain_finder_test_{id(self)}"
    logger = logging.getLogger(logger_name)
    logger.handlers = []
    logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.INFO)
    self.logger = logger


class TestQueryModule(unittest.TestCase):
    def _new_finder(self, use_db=False):
        with patch("query.database_enabled", return_value=use_db):
            with patch.object(query.BargainFinder, "_set_logging", _fake_set_logging):
                return query.BargainFinder(config_path="missing-config.yaml")

    def test_resolve_path_returns_absolute_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            absolute = Path(tmp) / "x.txt"
            self.assertEqual(query.resolve_path(str(absolute)), absolute)

    def test_resolve_path_prefers_existing_cwd_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "meta" / "a.txt"
            path.parent.mkdir(parents=True)
            path.write_text("x", encoding="utf-8")
            old_cwd = Path.cwd()
            try:
                import os

                os.chdir(tmp)
                resolved = query.resolve_path("meta/a.txt")
            finally:
                os.chdir(old_cwd)
            self.assertEqual(resolved, path)

    def test_get_tickers_from_file(self):
        finder = self._new_finder(use_db=False)
        with tempfile.TemporaryDirectory() as tmp:
            ticker_file = Path(tmp) / "tickers.txt"
            ticker_file.write_text("aapl\nmsft\n", encoding="utf-8")
            tickers = finder.get_tickers(str(ticker_file))
        self.assertEqual(tickers, ["AAPL", "MSFT"])

    def test_can_send_email_logs_missing_settings(self):
        finder = self._new_finder(use_db=False)
        finder.recipient = ""
        with patch.dict("os.environ", {}, clear=True):
            with patch(
                "query.get_missing_email_secrets",
                return_value=["SMTP_HOST", "SMTP_PASSWORD", "REPORT_RECIPIENT"],
            ):
                with self.assertLogs(finder.logger.name, level="WARNING") as logs:
                    can_send = finder._can_send_email("weekly bargain report")

        self.assertFalse(can_send)
        self.assertIn(
            "Skipping weekly bargain report email; missing settings",
            "\n".join(logs.output),
        )

    def test_get_tickers_from_database_when_available(self):
        finder = self._new_finder(use_db=True)
        finder.ticker_path = "meta/bargain_tickers.txt"
        with patch("query.repository.load_watchlist", return_value=["AAPL", "MSFT"]):
            self.assertEqual(finder.get_tickers(), ["AAPL", "MSFT"])

    def test_need_to_redownload(self):
        finder = self._new_finder(use_db=False)
        self.assertTrue(finder._need_to_redownload("missing.file"))

    def test_normalize_downloaded_close_data_series(self):
        finder = self._new_finder(use_db=False)
        series = pd.Series(
            [1.0, 2.0], index=pd.to_datetime(["2025-01-01", "2025-01-02"])
        )
        frame = finder._normalize_downloaded_close_data(series, ["AAPL"])
        self.assertEqual(list(frame.columns), ["AAPL"])

    def test_download_close_history_retries_rate_limited_ticker(self):
        finder = self._new_finder(use_db=False)
        downloaded = pd.Series(
            [100.0, 101.0],
            index=pd.to_datetime(["2025-01-01", "2025-01-02"]),
            name="Close",
        )
        with patch.dict(
            "os.environ",
            {
                "YFINANCE_REQUEST_PAUSE": "0",
                "YFINANCE_429_BACKOFF_BASE": "1",
                "YFINANCE_MAX_ATTEMPTS": "2",
            },
            clear=False,
        ):
            with patch(
                "query.yf.download",
                side_effect=[FakeRateLimitError("Too Many Requests"), downloaded],
            ):
                with patch("query.time.sleep") as sleep_mock:
                    frame = finder._download_close_history(["AAPL"], period="5d")

        self.assertEqual(list(frame.columns), ["AAPL"])
        sleep_mock.assert_any_call(1.0)

    def test_update_prices_uses_database_snapshot(self):
        finder = self._new_finder(use_db=True)
        snapshot = pd.DataFrame(
            [
                {
                    "Ticker": "AAPL",
                    "3 Months": 1,
                    "1 Month": 2,
                    "Current": 3,
                    "3 Mo Change": 4,
                    "1 Mo Change": 5,
                }
            ]
        )
        with patch.object(finder, "get_tickers", return_value=["AAPL"]):
            with patch(
                "query.repository.get_latest_price_snapshots", return_value=snapshot
            ):
                frame = finder._update_prices(force_redownload=False)
        self.assertEqual(frame.iloc[0]["Ticker"], "AAPL")

    def test_update_prices_download_path(self):
        finder = self._new_finder(use_db=False)
        with tempfile.TemporaryDirectory() as tmp:
            prices_file = Path(tmp) / "prices.csv"
            finder.prices_path = str(prices_file)
            with patch.object(finder, "get_tickers", return_value=["AAPL"]):
                downloaded = pd.DataFrame(
                    {"AAPL": [100.0, 120.0, 90.0]},
                    index=pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01"]),
                )
                with patch("query.yf.download", return_value={"Close": downloaded}):
                    frame = finder._update_prices(force_redownload=True)
        self.assertIn("1 Mo Change", frame.columns)

    def test_update_prices_uses_cached_snapshot_when_download_is_empty(self):
        finder = self._new_finder(use_db=False)
        with tempfile.TemporaryDirectory() as tmp:
            prices_file = Path(tmp) / "prices.csv"
            pd.DataFrame(
                [
                    {
                        "Ticker": "AAPL",
                        "3 Months": 100,
                        "1 Month": 110,
                        "Current": 120,
                        "3 Mo Change": 20,
                        "1 Mo Change": 9.09,
                    }
                ]
            ).to_csv(prices_file, index=False)
            finder.prices_path = str(prices_file)
            with patch.object(finder, "get_tickers", return_value=["AAPL"]):
                with patch.dict(
                    "os.environ",
                    {"YFINANCE_REQUEST_PAUSE": "0", "YFINANCE_MAX_ATTEMPTS": "1"},
                    clear=False,
                ):
                    with patch(
                        "query.yf.download", return_value={"Close": pd.DataFrame()}
                    ):
                        frame = finder._update_prices(force_redownload=True)

        self.assertEqual(frame.iloc[0]["Ticker"], "AAPL")

    def test_update_prices_drops_tickers_without_downloaded_history(self):
        finder = self._new_finder(use_db=False)
        with tempfile.TemporaryDirectory() as tmp:
            prices_file = Path(tmp) / "prices.csv"
            finder.prices_path = str(prices_file)
            with patch.object(finder, "get_tickers", return_value=["AAPL", "MSFT"]):
                downloaded = pd.DataFrame(
                    {"AAPL": [100.0, 120.0, 90.0]},
                    index=pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01"]),
                )
                with patch("query.yf.download", return_value={"Close": downloaded}):
                    frame = finder._update_prices(force_redownload=True)

        self.assertEqual(frame["Ticker"].tolist(), ["AAPL"])

    def test_update_prices_only_downloads_missing_or_stale_tickers(self):
        finder = self._new_finder(use_db=False)
        with tempfile.TemporaryDirectory() as tmp:
            prices_file = Path(tmp) / "prices.csv"
            raw_prices_file = Path(tmp) / "raw_prices.csv"
            finder.prices_path = str(prices_file)
            finder.raw_prices_path = str(raw_prices_file)
            pd.DataFrame(
                [
                    {"Date": "2024-12-20", "AAPL": 90.0},
                    {"Date": "2025-03-01", "AAPL": 110.0},
                    {"Date": "2025-03-31", "AAPL": 120.0},
                ]
            ).to_csv(raw_prices_file, index=False)

            def fake_download(ticker, **kwargs):
                self.assertEqual(ticker, "MSFT")
                self.assertIn("period", kwargs)
                return {
                    "Close": pd.Series(
                        [200.0, 210.0, 220.0],
                        index=pd.to_datetime(
                            ["2024-12-20", "2025-03-01", "2025-03-31"]
                        ),
                        name="Close",
                    )
                }

            with patch.object(finder, "get_tickers", return_value=["AAPL", "MSFT"]):
                with patch.object(
                    finder,
                    "_expected_latest_price_date",
                    return_value=pd.Timestamp("2025-03-31"),
                ):
                    with patch(
                        "query.yf.download", side_effect=fake_download
                    ) as mock_download:
                        frame = finder._update_prices(force_redownload=False)

        self.assertEqual(mock_download.call_count, 1)
        self.assertEqual(sorted(frame["Ticker"].tolist()), ["AAPL", "MSFT"])

    def test_find_current_bargains_db_path(self):
        finder = self._new_finder(use_db=True)
        frame = pd.DataFrame(
            [
                {
                    "Ticker": "AAPL",
                    "3 Months": 100,
                    "1 Month": 110,
                    "Current": 80,
                    "3 Mo Change": -20,
                    "1 Mo Change": -27,
                }
            ]
        )
        with patch.object(finder, "_update_prices", return_value=frame):
            with patch("query.repository.append_bargain_history") as append_history:
                bargains = finder.find_current_bargains()
        append_history.assert_called_once()
        self.assertEqual(len(bargains), 1)

    def test_update_sell_tracking_database_path(self):
        finder = self._new_finder(use_db=True)
        finder.to_sell_tickers = ["AAPL"]
        history = pd.DataFrame({"AAPL": [1.0]}, index=pd.to_datetime(["today"]))
        with patch.object(finder, "_load_cached_raw_history", return_value=history):
            with patch.object(
                finder,
                "_plan_history_downloads",
                return_value=([], {}),
            ):
                data = finder._update_sell_tracking(force_redownload=False)
        self.assertFalse(data.empty)

    def test_update_sell_tracking_uses_cached_raw_prices_when_download_is_empty(self):
        finder = self._new_finder(use_db=False)
        with tempfile.TemporaryDirectory() as tmp:
            raw_prices_file = Path(tmp) / "raw_prices.csv"
            pd.DataFrame(
                [
                    {"Date": "2025-01-01", "AAPL": 100.0},
                    {"Date": "2025-01-02", "AAPL": 101.0},
                ]
            ).to_csv(raw_prices_file, index=False)
            finder.raw_prices_path = str(raw_prices_file)
            finder.to_sell_tickers = ["AAPL"]
            with patch.dict(
                "os.environ",
                {"YFINANCE_REQUEST_PAUSE": "0", "YFINANCE_MAX_ATTEMPTS": "1"},
                clear=False,
            ):
                with patch("query.yf.download", return_value={"Close": pd.DataFrame()}):
                    data = finder._update_sell_tracking(force_redownload=True)

        self.assertEqual(list(data.columns), ["AAPL"])

    def test_check_for_extremes(self):
        finder = self._new_finder(use_db=False)
        finder.to_sell_tickers = ["AAPL"]
        data = pd.DataFrame(
            {"AAPL": [100.0, 90.0, 95.0]},
            index=pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-10"]),
        )
        with patch.object(finder, "_update_sell_tracking", return_value=data):
            sell_any, stats = finder._check_for_extremes()
        self.assertIn("AAPL", stats)
        self.assertIn("Sell", stats["AAPL"])
        self.assertIsInstance(sell_any, bool)

    def test_execute_updates_state_database(self):
        finder = self._new_finder(use_db=True)
        with patch.object(
            finder,
            "find_current_bargains",
            return_value=pd.DataFrame([{"Ticker": "AAPL"}]),
        ):
            with patch.object(finder, "create_bargain_report"):
                with patch("query.repository.set_last_update") as set_last:
                    count = finder._execute()
        set_last.assert_called_once()
        self.assertEqual(count, 1)

    def test_run_executes_once(self):
        finder = self._new_finder(use_db=False)
        with patch.object(finder, "_execute", return_value=2):
            result = finder.run()
        self.assertEqual(result, 2)

    def test_lambda_handler_success(self):
        with patch("query.BargainFinder") as finder_cls:
            finder = finder_cls.return_value
            finder.use_database = False
            finder.run.return_value = 1
            response = query.lambda_handler({}, None)
        self.assertEqual(response["statusCode"], 200)

    def test_lambda_handler_failure_records_and_raises(self):
        with patch("query.BargainFinder") as finder_cls:
            finder = finder_cls.return_value
            finder.use_database = True
            finder.recipient = "to@example.com"
            finder.run.side_effect = RuntimeError("boom")
            with patch("query.repository.record_execution") as record_exec:
                with patch("query.send_email"):
                    with self.assertRaises(RuntimeError):
                        query.lambda_handler({}, None)
        record_exec.assert_called_once()


if __name__ == "__main__":
    unittest.main()
