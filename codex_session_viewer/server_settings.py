from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import Settings


MIN_PAGE_SIZE = 6
MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 24


@dataclass(slots=True)
class ServerSettingsSnapshot:
    page_size: int
    expected_agent_version: str
    sync_on_start: bool


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def parse_bool_value(raw: object, default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off", ""}:
        return False
    return default


def normalize_page_size(raw: object) -> int:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("Home page repo limit must be a whole number.") from exc
    if value < MIN_PAGE_SIZE or value > MAX_PAGE_SIZE:
        raise ValueError(
            f"Home page repo limit must be between {MIN_PAGE_SIZE} and {MAX_PAGE_SIZE}."
        )
    return value


def normalize_expected_agent_version(raw: object, default_version: str) -> str:
    value = str(raw or "").strip() or default_version
    if len(value) > 64:
        raise ValueError("Expected agent version must be 64 characters or fewer.")
    return value


def fetch_server_settings(
    connection: sqlite3.Connection,
    defaults: "Settings",
) -> ServerSettingsSnapshot:
    rows = connection.execute(
        """
        SELECT key, value
        FROM server_settings
        WHERE key IN ('page_size', 'expected_agent_version', 'sync_on_start')
        """
    ).fetchall()
    values = {str(row["key"]): str(row["value"]) for row in rows}

    page_size = defaults.page_size
    if "page_size" in values:
        try:
            page_size = normalize_page_size(values["page_size"])
        except ValueError:
            page_size = defaults.page_size

    expected_agent_version = defaults.expected_agent_version
    if "expected_agent_version" in values:
        try:
            expected_agent_version = normalize_expected_agent_version(
                values["expected_agent_version"],
                defaults.app_version,
            )
        except ValueError:
            expected_agent_version = defaults.expected_agent_version

    sync_on_start = defaults.sync_on_start
    if "sync_on_start" in values:
        sync_on_start = parse_bool_value(values["sync_on_start"], defaults.sync_on_start)

    return ServerSettingsSnapshot(
        page_size=page_size,
        expected_agent_version=expected_agent_version,
        sync_on_start=sync_on_start,
    )


def apply_server_settings(
    connection: sqlite3.Connection,
    settings: "Settings",
    *,
    preserve_sync_on_start: bool = False,
) -> ServerSettingsSnapshot:
    snapshot = fetch_server_settings(connection, settings)
    settings.page_size = snapshot.page_size
    settings.expected_agent_version = snapshot.expected_agent_version
    if not preserve_sync_on_start:
        settings.sync_on_start = snapshot.sync_on_start
    return snapshot


def update_server_settings(
    connection: sqlite3.Connection,
    *,
    page_size: int,
    expected_agent_version: str,
    sync_on_start: bool,
) -> ServerSettingsSnapshot:
    now = utc_now_iso()
    values = {
        "page_size": str(page_size),
        "expected_agent_version": expected_agent_version,
        "sync_on_start": "1" if sync_on_start else "0",
    }
    for key, value in values.items():
        connection.execute(
            """
            INSERT INTO server_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, value, now),
        )
    return ServerSettingsSnapshot(
        page_size=page_size,
        expected_agent_version=expected_agent_version,
        sync_on_start=sync_on_start,
    )
