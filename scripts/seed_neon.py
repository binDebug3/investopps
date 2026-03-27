from pathlib import Path
import sys

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _read_watchlist(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [
        line.strip().upper() for line in path.read_text().splitlines() if line.strip()
    ]


def main() -> None:
    import repository

    repository.initialize_schema()

    bargain_tickers = _read_watchlist(WORKSPACE_ROOT / "meta" / "bargain_tickers.txt")
    sell_tickers = _read_watchlist(WORKSPACE_ROOT / "meta" / "high_tickers.txt")

    bargain_count = repository.upsert_watchlist("bargain", bargain_tickers)
    sell_count = repository.upsert_watchlist("sell", sell_tickers)

    prices_path = WORKSPACE_ROOT / "data" / "prices.csv"
    if prices_path.exists():
        prices = pd.read_csv(prices_path)
        prices_run_date = (
            pd.to_datetime(prices_path.stat().st_mtime, unit="s").normalize().date()
        )
        repository.record_price_snapshots(prices, run_date=prices_run_date)

    bargains_path = WORKSPACE_ROOT / "data" / "bargain_history.csv"
    bargain_rows = 0
    if bargains_path.exists():
        bargain_history = pd.read_csv(bargains_path)
        if "Date" in bargain_history.columns:
            bargain_history["Date"] = pd.to_datetime(
                bargain_history["Date"], errors="coerce"
            )
        bargain_rows = repository.append_bargain_history(bargain_history)

    raw_prices_path = WORKSPACE_ROOT / "data" / "raw_prices.csv"
    raw_price_rows = 0
    if raw_prices_path.exists():
        raw_prices = pd.read_csv(raw_prices_path)
        raw_price_rows = repository.upsert_raw_price_history(raw_prices)

    print(
        "Seed complete: "
        f"{bargain_count} bargain tickers, "
        f"{sell_count} sell tickers, "
        f"{bargain_rows} bargain rows, "
        f"{raw_price_rows} raw price rows."
    )


if __name__ == "__main__":
    main()
