import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import scripts.init_neon as init_neon
import scripts.seed_neon as seed_neon


class _FakeRepository:
    def __init__(self):
        self.calls = []

    def initialize_schema(self):
        self.calls.append(("initialize_schema",))

    def upsert_watchlist(self, list_type, tickers):
        self.calls.append(("upsert_watchlist", list_type, list(tickers)))
        return len(tickers)

    def record_price_snapshots(self, frame, run_date=None):
        self.calls.append(("record_price_snapshots", len(frame), run_date))

    def append_bargain_history(self, frame):
        self.calls.append(("append_bargain_history", len(frame)))
        return len(frame)

    def upsert_raw_price_history(self, frame):
        self.calls.append(("upsert_raw_price_history", len(frame)))
        return len(frame)


class TestScriptIntegration(unittest.TestCase):
    def test_init_neon_calls_initialize_schema(self):
        fake_repo = _FakeRepository()
        with patch.dict("sys.modules", {"repository": fake_repo}):
            init_neon.main()
        self.assertEqual(fake_repo.calls[0][0], "initialize_schema")

    def test_seed_neon_reads_and_seeds_expected_files(self):
        fake_repo = _FakeRepository()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "meta").mkdir()
            (root / "data").mkdir()

            (root / "meta" / "bargain_tickers.txt").write_text(
                "AAPL\nMSFT\n", encoding="utf-8"
            )
            (root / "meta" / "high_tickers.txt").write_text("TSLA\n", encoding="utf-8")

            pd.DataFrame(
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
            ).to_csv(root / "data" / "prices.csv", index=False)

            pd.DataFrame(
                [
                    {
                        "Date": "2025-01-01",
                        "Ticker": "AAPL",
                        "3 Months": 1,
                        "1 Month": 2,
                        "Current": 3,
                        "3 Mo Change": 4,
                        "1 Mo Change": 5,
                    }
                ]
            ).to_csv(root / "data" / "bargain_history.csv", index=False)

            pd.DataFrame([{"Date": "2025-01-01", "AAPL": 100.0}]).to_csv(
                root / "data" / "raw_prices.csv", index=False
            )

            with patch.object(seed_neon, "WORKSPACE_ROOT", root):
                with patch.dict("sys.modules", {"repository": fake_repo}):
                    seed_neon.main()

        call_names = [c[0] for c in fake_repo.calls]
        self.assertIn("initialize_schema", call_names)
        self.assertIn("upsert_watchlist", call_names)
        self.assertIn("record_price_snapshots", call_names)
        self.assertIn("append_bargain_history", call_names)
        self.assertIn("upsert_raw_price_history", call_names)


if __name__ == "__main__":
    unittest.main()
