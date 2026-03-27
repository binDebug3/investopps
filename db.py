import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

from psycopg import Connection, connect
from psycopg.rows import dict_row


PROJECT_ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
DEFAULT_DATABASE_URL_FILE = PROJECT_ROOT / "meta" / "neon_db_url.txt"
LEGACY_DATABASE_URL_FILE = WORKSPACE_ROOT / "meta" / "neon_db_url.txt"


def _read_database_url_file(file_path: Path = DEFAULT_DATABASE_URL_FILE) -> str:
    if file_path.exists():
        return file_path.read_text(encoding="utf-8").strip()
    if LEGACY_DATABASE_URL_FILE.exists():
        return LEGACY_DATABASE_URL_FILE.read_text(encoding="utf-8").strip()
    return ""


def get_database_url(database_url: Optional[str] = None) -> str:
    url = database_url or os.getenv("DATABASE_URL") or _read_database_url_file()
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set and meta/neon_db_url.txt was not found."
        )
    return url


def database_enabled() -> bool:
    return bool(os.getenv("DATABASE_URL") or _read_database_url_file())


@contextmanager
def get_connection(
    database_url: Optional[str] = None,
    autocommit: bool = False,
) -> Iterator[Connection]:
    connection = connect(
        get_database_url(database_url),
        autocommit=autocommit,
        row_factory=dict_row,
    )
    try:
        yield connection
        if not autocommit:
            connection.commit()
    except Exception:
        if not autocommit:
            connection.rollback()
        raise
    finally:
        connection.close()
