from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Optional

import pandas as pd

from db import get_connection


SNAPSHOT_COLUMNS = [
    "Ticker",
    "3 Months",
    "1 Month",
    "Current",
    "3 Mo Change",
    "1 Mo Change",
]


def _to_date(value: Optional[object]) -> date:
    if value is None:
        return pd.to_datetime("today").normalize().date()
    return pd.to_datetime(value).date()


def initialize_schema() -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS watchlists (
            list_type TEXT NOT NULL,
            ticker TEXT NOT NULL,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (list_type, ticker)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS price_snapshots (
            run_date DATE NOT NULL,
            ticker TEXT NOT NULL,
            price_3_months DOUBLE PRECISION,
            price_1_month DOUBLE PRECISION,
            current_price DOUBLE PRECISION,
            change_3_months DOUBLE PRECISION,
            change_1_month DOUBLE PRECISION,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (run_date, ticker)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS bargain_history (
            run_date DATE NOT NULL,
            ticker TEXT NOT NULL,
            price_3_months DOUBLE PRECISION,
            price_1_month DOUBLE PRECISION,
            current_price DOUBLE PRECISION,
            change_3_months DOUBLE PRECISION,
            change_1_month DOUBLE PRECISION,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (run_date, ticker)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS raw_price_history (
            price_date DATE NOT NULL,
            ticker TEXT NOT NULL,
            close_price DOUBLE PRECISION,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (price_date, ticker)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS run_state (
            state_key TEXT PRIMARY KEY,
            state_value TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS execution_history (
            id BIGSERIAL PRIMARY KEY,
            started_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ,
            status TEXT NOT NULL,
            bargains_found INTEGER NOT NULL DEFAULT 0,
            error_text TEXT
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_watchlists_type_active ON watchlists (list_type, active)",
        "CREATE INDEX IF NOT EXISTS idx_price_snapshots_run_date ON price_snapshots (run_date)",
        "CREATE INDEX IF NOT EXISTS idx_bargain_history_run_date ON bargain_history (run_date)",
        (
            "CREATE INDEX IF NOT EXISTS idx_raw_price_history_ticker_date "
            "ON raw_price_history (ticker, price_date)"
        ),
    ]

    with get_connection() as connection:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)


def upsert_watchlist(list_type: str, tickers: Iterable[str]) -> int:
    cleaned = sorted(
        {ticker.strip().upper() for ticker in tickers if ticker and ticker.strip()}
    )
    if not cleaned:
        return 0

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO watchlists (list_type, ticker, active, updated_at)
                VALUES (%s, %s, TRUE, NOW())
                ON CONFLICT (list_type, ticker)
                DO UPDATE SET active = EXCLUDED.active, updated_at = NOW()
                """,
                [(list_type, ticker) for ticker in cleaned],
            )
    return len(cleaned)


def load_watchlist(list_type: str) -> list[str]:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT ticker
                FROM watchlists
                WHERE list_type = %s AND active = TRUE
                ORDER BY ticker
                """,
                (list_type,),
            )
            return [row["ticker"] for row in cursor.fetchall()]


def get_latest_price_snapshot_date() -> Optional[date]:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT MAX(run_date) AS latest_date FROM price_snapshots")
            row = cursor.fetchone()
            return row["latest_date"] if row else None


def record_price_snapshots(
    frame: pd.DataFrame, run_date: Optional[object] = None
) -> int:
    if frame.empty:
        return 0

    resolved_date = _to_date(run_date)
    records = [
        (
            resolved_date,
            row["Ticker"],
            _nullable_float(row.get("3 Months")),
            _nullable_float(row.get("1 Month")),
            _nullable_float(row.get("Current")),
            _nullable_float(row.get("3 Mo Change")),
            _nullable_float(row.get("1 Mo Change")),
        )
        for row in frame.to_dict("records")
    ]

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO price_snapshots (
                    run_date,
                    ticker,
                    price_3_months,
                    price_1_month,
                    current_price,
                    change_3_months,
                    change_1_month,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (run_date, ticker)
                DO UPDATE SET
                    price_3_months = EXCLUDED.price_3_months,
                    price_1_month = EXCLUDED.price_1_month,
                    current_price = EXCLUDED.current_price,
                    change_3_months = EXCLUDED.change_3_months,
                    change_1_month = EXCLUDED.change_1_month,
                    updated_at = NOW()
                """,
                records,
            )
    return len(records)


def get_latest_price_snapshots(run_date: Optional[object] = None) -> pd.DataFrame:
    resolved_date = (
        _to_date(run_date) if run_date is not None else get_latest_price_snapshot_date()
    )
    if resolved_date is None:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    ticker AS "Ticker",
                    price_3_months AS "3 Months",
                    price_1_month AS "1 Month",
                    current_price AS "Current",
                    change_3_months AS "3 Mo Change",
                    change_1_month AS "1 Mo Change"
                FROM price_snapshots
                WHERE run_date = %s
                ORDER BY ticker
                """,
                (resolved_date,),
            )
            rows = cursor.fetchall()

    return pd.DataFrame(rows, columns=SNAPSHOT_COLUMNS)


def append_bargain_history(
    frame: pd.DataFrame, run_date: Optional[object] = None
) -> int:
    if frame.empty:
        return 0

    records = []
    for row in frame.to_dict("records"):
        row_date = _to_date(row.get("Date") or run_date)
        records.append(
            (
                row_date,
                row["Ticker"],
                _nullable_float(row.get("3 Months")),
                _nullable_float(row.get("1 Month")),
                _nullable_float(row.get("Current")),
                _nullable_float(row.get("3 Mo Change")),
                _nullable_float(row.get("1 Mo Change")),
            )
        )

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO bargain_history (
                    run_date,
                    ticker,
                    price_3_months,
                    price_1_month,
                    current_price,
                    change_3_months,
                    change_1_month,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (run_date, ticker)
                DO UPDATE SET
                    price_3_months = EXCLUDED.price_3_months,
                    price_1_month = EXCLUDED.price_1_month,
                    current_price = EXCLUDED.current_price,
                    change_3_months = EXCLUDED.change_3_months,
                    change_1_month = EXCLUDED.change_1_month,
                    updated_at = NOW()
                """,
                records,
            )
    return len(records)


def get_recent_bargains(days: int) -> pd.DataFrame:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    run_date AS "Date",
                    ticker AS "Ticker",
                    price_3_months AS "3 Months",
                    price_1_month AS "1 Month",
                    current_price AS "Current",
                    change_3_months AS "3 Mo Change",
                    change_1_month AS "1 Mo Change"
                FROM bargain_history
                WHERE run_date >= CURRENT_DATE - %s * INTERVAL '1 day'
                ORDER BY run_date, ticker
                """,
                (days,),
            )
            rows = cursor.fetchall()

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["Date", *SNAPSHOT_COLUMNS])

    frame["Date"] = pd.to_datetime(frame["Date"])
    return frame


def set_last_update(timestamp: object, state_key: str = "daily_update") -> None:
    value = pd.to_datetime(timestamp).isoformat()
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO run_state (state_key, state_value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (state_key)
                DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = NOW()
                """,
                (state_key, value),
            )


def get_last_update(state_key: str = "daily_update") -> Optional[pd.Timestamp]:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT state_value FROM run_state WHERE state_key = %s",
                (state_key,),
            )
            row = cursor.fetchone()

    if not row or not row["state_value"]:
        return None
    return pd.to_datetime(row["state_value"])


def upsert_raw_price_history(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0

    data = frame.copy()
    if "Date" in data.columns:
        data["Date"] = pd.to_datetime(data["Date"])
        data = data.set_index("Date")

    data.index = pd.to_datetime(data.index)

    records = []
    for price_date, row in data.iterrows():
        for ticker, close_price in row.items():
            if pd.isna(close_price):
                continue
            records.append((price_date.date(), ticker, float(close_price)))

    if not records:
        return 0

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO raw_price_history (price_date, ticker, close_price, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (price_date, ticker)
                DO UPDATE SET close_price = EXCLUDED.close_price, updated_at = NOW()
                """,
                records,
            )
    return len(records)


def get_raw_price_history(tickers: Iterable[str]) -> pd.DataFrame:
    cleaned = [
        ticker.strip().upper() for ticker in tickers if ticker and ticker.strip()
    ]
    if not cleaned:
        return pd.DataFrame()

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT price_date, ticker, close_price
                FROM raw_price_history
                WHERE ticker = ANY(%s)
                ORDER BY price_date, ticker
                """,
                (cleaned,),
            )
            rows = cursor.fetchall()

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=cleaned)

    pivoted = frame.pivot(index="price_date", columns="ticker", values="close_price")
    pivoted.index = pd.to_datetime(pivoted.index)
    return pivoted.sort_index()


def record_execution(
    started_at: datetime,
    completed_at: datetime,
    status: str,
    bargains_found: int = 0,
    error_text: Optional[str] = None,
) -> None:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO execution_history (
                    started_at,
                    completed_at,
                    status,
                    bargains_found,
                    error_text
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (started_at, completed_at, status, bargains_found, error_text),
            )


def _nullable_float(value: object) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    return float(value)
