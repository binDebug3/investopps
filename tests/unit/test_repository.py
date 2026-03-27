import contextlib
from datetime import datetime
import sys
import types
import unittest
from unittest.mock import patch
import repository

import pandas as pd


fake_psycopg = types.ModuleType("psycopg")
fake_psycopg.Connection = object
fake_psycopg.connect = lambda *args, **kwargs: None
fake_psycopg_rows = types.ModuleType("psycopg.rows")
fake_psycopg_rows.dict_row = object()
sys.modules.setdefault("psycopg", fake_psycopg)
sys.modules.setdefault("psycopg.rows", fake_psycopg_rows)


class FakeCursor:
    def __init__(self, fetchone_result=None, fetchall_result=None):
        self.fetchone_result = fetchone_result
        self.fetchall_result = fetchall_result or []
        self.executed = []
        self.executemany_calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, seq):
        self.executemany_calls.append((sql, list(seq)))

    def fetchone(self):
        return self.fetchone_result

    def fetchall(self):
        return self.fetchall_result


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def cursor(self):
        return self._cursor


def fake_connection_factory(cursor):
    @contextlib.contextmanager
    def _manager(*args, **kwargs):
        yield FakeConnection(cursor)

    return _manager


class TestRepositoryModule(unittest.TestCase):
    def test_to_date(self):
        self.assertIsNotNone(repository._to_date(None))
        self.assertEqual(str(repository._to_date("2025-01-02")), "2025-01-02")

    def test_nullable_float(self):
        self.assertIsNone(repository._nullable_float(None))
        self.assertEqual(repository._nullable_float(1), 1.0)

    def test_initialize_schema_executes_statements(self):
        cursor = FakeCursor()
        with patch("repository.get_connection", fake_connection_factory(cursor)):
            repository.initialize_schema()

        self.assertGreaterEqual(len(cursor.executed), 8)

    def test_upsert_watchlist(self):
        cursor = FakeCursor()
        with patch("repository.get_connection", fake_connection_factory(cursor)):
            count = repository.upsert_watchlist("bargain", ["aapl", "AAPL", " msft "])

        self.assertEqual(count, 2)
        sql, seq = cursor.executemany_calls[0]
        self.assertIn("INSERT INTO watchlists", sql)
        self.assertEqual(sorted(seq), [("bargain", "AAPL"), ("bargain", "MSFT")])

    def test_load_watchlist(self):
        cursor = FakeCursor(fetchall_result=[{"ticker": "AAPL"}, {"ticker": "MSFT"}])
        with patch("repository.get_connection", fake_connection_factory(cursor)):
            tickers = repository.load_watchlist("bargain")
        self.assertEqual(tickers, ["AAPL", "MSFT"])

    def test_get_latest_price_snapshot_date(self):
        cursor = FakeCursor(
            fetchone_result={"latest_date": pd.Timestamp("2025-01-02").date()}
        )
        with patch("repository.get_connection", fake_connection_factory(cursor)):
            self.assertEqual(
                str(repository.get_latest_price_snapshot_date()), "2025-01-02"
            )

    def test_record_price_snapshots(self):
        frame = pd.DataFrame(
            [
                {
                    "Ticker": "AAPL",
                    "3 Months": 100,
                    "1 Month": 110,
                    "Current": 120,
                    "3 Mo Change": 20,
                    "1 Mo Change": 9.0,
                }
            ]
        )
        cursor = FakeCursor()
        with patch("repository.get_connection", fake_connection_factory(cursor)):
            count = repository.record_price_snapshots(frame, run_date="2025-01-03")

        self.assertEqual(count, 1)
        self.assertEqual(len(cursor.executemany_calls), 1)

    def test_get_latest_price_snapshots(self):
        rows = [
            {
                "Ticker": "AAPL",
                "3 Months": 1.0,
                "1 Month": 2.0,
                "Current": 3.0,
                "3 Mo Change": 4.0,
                "1 Mo Change": 5.0,
            }
        ]
        cursor = FakeCursor(fetchall_result=rows)
        with patch("repository.get_connection", fake_connection_factory(cursor)):
            frame = repository.get_latest_price_snapshots(run_date="2025-01-03")

        self.assertEqual(list(frame.columns), repository.SNAPSHOT_COLUMNS)
        self.assertEqual(frame.iloc[0]["Ticker"], "AAPL")

    def test_append_bargain_history(self):
        frame = pd.DataFrame(
            [
                {
                    "Date": "2025-01-03",
                    "Ticker": "MSFT",
                    "3 Months": 90,
                    "1 Month": 95,
                    "Current": 80,
                    "3 Mo Change": -10,
                    "1 Mo Change": -15,
                }
            ]
        )
        cursor = FakeCursor()
        with patch("repository.get_connection", fake_connection_factory(cursor)):
            count = repository.append_bargain_history(frame)

        self.assertEqual(count, 1)
        self.assertEqual(len(cursor.executemany_calls), 1)

    def test_get_recent_bargains(self):
        rows = [
            {
                "Date": "2025-01-02",
                "Ticker": "CNC",
                "3 Months": 1.0,
                "1 Month": 2.0,
                "Current": 0.5,
                "3 Mo Change": -50,
                "1 Mo Change": -75,
            }
        ]
        cursor = FakeCursor(fetchall_result=rows)
        with patch("repository.get_connection", fake_connection_factory(cursor)):
            frame = repository.get_recent_bargains(7)

        self.assertIn("Date", frame.columns)
        self.assertEqual(frame.iloc[0]["Ticker"], "CNC")

    def test_set_and_get_last_update(self):
        set_cursor = FakeCursor()
        with patch("repository.get_connection", fake_connection_factory(set_cursor)):
            repository.set_last_update("2025-01-03T01:02:03")

        get_cursor = FakeCursor(fetchone_result={"state_value": "2025-01-03T01:02:03"})
        with patch("repository.get_connection", fake_connection_factory(get_cursor)):
            value = repository.get_last_update()

        self.assertEqual(str(value), "2025-01-03 01:02:03")

    def test_get_last_update_none(self):
        cursor = FakeCursor(fetchone_result=None)
        with patch("repository.get_connection", fake_connection_factory(cursor)):
            self.assertIsNone(repository.get_last_update())

    def test_upsert_raw_price_history(self):
        frame = pd.DataFrame(
            {
                "Date": ["2025-01-01", "2025-01-02"],
                "AAPL": [100.0, 101.0],
                "MSFT": [200.0, 201.0],
            }
        )
        cursor = FakeCursor()
        with patch("repository.get_connection", fake_connection_factory(cursor)):
            count = repository.upsert_raw_price_history(frame)

        self.assertEqual(count, 4)
        self.assertEqual(len(cursor.executemany_calls), 1)

    def test_get_raw_price_history(self):
        rows = [
            {"price_date": "2025-01-01", "ticker": "AAPL", "close_price": 100.0},
            {"price_date": "2025-01-02", "ticker": "AAPL", "close_price": 101.0},
        ]
        cursor = FakeCursor(fetchall_result=rows)
        with patch("repository.get_connection", fake_connection_factory(cursor)):
            frame = repository.get_raw_price_history(["aapl"])

        self.assertEqual(list(frame.columns), ["AAPL"])
        self.assertEqual(len(frame), 2)

    def test_get_raw_price_history_empty_input(self):
        frame = repository.get_raw_price_history([])
        self.assertTrue(frame.empty)

    def test_record_execution(self):
        cursor = FakeCursor()
        now = datetime.utcnow()
        with patch("repository.get_connection", fake_connection_factory(cursor)):
            repository.record_execution(
                now, now, "ok", bargains_found=2, error_text=None
            )

        self.assertEqual(len(cursor.executed), 1)
        sql, params = cursor.executed[0]
        self.assertIn("INSERT INTO execution_history", sql)
        self.assertEqual(params[2], "ok")


if __name__ == "__main__":
    unittest.main()
