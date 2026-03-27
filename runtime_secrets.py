from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
DEFAULT_SECRETS_FILE = PROJECT_ROOT / "meta" / "runtime_secrets.env"
LEGACY_SECRETS_FILE = WORKSPACE_ROOT / "meta" / "runtime_secrets.env"
DATABASE_URL_FILES = (
    PROJECT_ROOT / "meta" / "neon_db_url.txt",
    WORKSPACE_ROOT / "meta" / "neon_db_url.txt",
)
SMTP_PASSWORD_FILES = (
    PROJECT_ROOT / "meta" / "google_app_password.txt",
    WORKSPACE_ROOT / "meta" / "google_app_password.txt",
    WORKSPACE_ROOT / "meta" / "goole_app_password.txt",
)
SMTP_REQUIRED_KEYS = ("SMTP_HOST", "SMTP_USERNAME", "SMTP_PASSWORD")


def get_secret_file_candidates() -> tuple[Path, ...]:
    configured = os.getenv("BARGAINFINDER_SECRETS_FILE")
    candidates: list[Path] = []
    if configured:
        candidates.append(Path(configured).expanduser())
    candidates.extend([DEFAULT_SECRETS_FILE, LEGACY_SECRETS_FILE])

    unique_candidates: list[Path] = []
    for candidate in candidates:
        resolved = candidate.resolve(strict=False)
        if resolved not in unique_candidates:
            unique_candidates.append(resolved)
    return tuple(unique_candidates)


def _parse_secret_line(raw_line: str) -> tuple[str, str] | None:
    line = raw_line.strip()
    if not line or line.startswith("#") or "=" not in line:
        return None

    key, value = line.split("=", 1)
    key = key.strip()
    value = value.strip().strip('"').strip("'")
    if not key:
        return None
    return key, value


def load_runtime_secrets() -> dict[str, object]:
    loaded_from: Path | None = None
    loaded_keys: list[str] = []
    searched_paths = get_secret_file_candidates()

    for candidate in searched_paths:
        if not candidate.exists():
            continue

        loaded_from = candidate
        for raw_line in candidate.read_text(encoding="utf-8").splitlines():
            parsed = _parse_secret_line(raw_line)
            if parsed is None:
                continue

            key, value = parsed
            if key not in os.environ and value:
                os.environ[key] = value
                loaded_keys.append(key)
        break

    _load_secret_from_file("DATABASE_URL", DATABASE_URL_FILES, loaded_keys)
    _load_secret_from_file("SMTP_PASSWORD", SMTP_PASSWORD_FILES, loaded_keys)

    return {
        "loaded_from": loaded_from,
        "loaded_keys": tuple(sorted(loaded_keys)),
        "searched_paths": searched_paths,
    }


def _load_secret_from_file(
    env_key: str,
    candidate_paths: tuple[Path, ...],
    loaded_keys: list[str],
) -> None:
    if os.getenv(env_key):
        return

    for path in candidate_paths:
        if not path.exists():
            continue
        value = path.read_text(encoding="utf-8").strip()
        if not value:
            continue
        os.environ[env_key] = value
        loaded_keys.append(env_key)
        return


def get_missing_secrets(keys: Iterable[str]) -> list[str]:
    load_runtime_secrets()
    return [key for key in keys if not os.getenv(key)]


def get_missing_email_secrets(recipient: str | None = None) -> list[str]:
    missing = get_missing_secrets(SMTP_REQUIRED_KEYS)
    resolved_recipient = recipient or os.getenv("REPORT_RECIPIENT", "")
    if not resolved_recipient:
        missing.append("REPORT_RECIPIENT")
    return missing
