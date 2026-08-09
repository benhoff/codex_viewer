from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any


MAX_MACHINE_ALIAS_LENGTH = 80


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def normalize_machine_display_alias(value: object) -> str | None:
    alias = trimmed(value)
    if alias is None:
        return None
    normalized = " ".join(alias.split())
    if len(normalized) > MAX_MACHINE_ALIAS_LENGTH:
        raise ValueError(f"Machine alias must be {MAX_MACHINE_ALIAS_LENGTH} characters or fewer.")
    return normalized


def list_machine_display_aliases(connection: sqlite3.Connection) -> dict[str, str]:
    rows = connection.execute(
        """
        SELECT source_host, display_alias
        FROM machine_aliases
        ORDER BY source_host ASC
        """
    ).fetchall()
    return {
        str(row["source_host"]): str(row["display_alias"])
        for row in rows
        if trimmed(row["source_host"]) and trimmed(row["display_alias"])
    }


def fetch_machine_display_alias(connection: sqlite3.Connection, source_host: str) -> str | None:
    row = connection.execute(
        """
        SELECT display_alias
        FROM machine_aliases
        WHERE source_host = ?
        """,
        (source_host,),
    ).fetchone()
    if row is None:
        return None
    return trimmed(row["display_alias"])


def machine_display_name(source_host: str, display_alias: str | None) -> str:
    return trimmed(display_alias) or source_host


def set_machine_display_alias(
    connection: sqlite3.Connection,
    *,
    source_host: str,
    display_alias: object,
) -> dict[str, Any] | None:
    clean_source_host = trimmed(source_host)
    if clean_source_host is None:
        raise ValueError("Missing machine source host.")

    clean_alias = normalize_machine_display_alias(display_alias)
    if clean_alias is None or clean_alias == clean_source_host:
        connection.execute(
            "DELETE FROM machine_aliases WHERE source_host = ?",
            (clean_source_host,),
        )
        return None

    now = utc_now_iso()
    connection.execute(
        """
        INSERT INTO machine_aliases (
            source_host,
            display_alias,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?)
        ON CONFLICT(source_host) DO UPDATE SET
            display_alias = excluded.display_alias,
            updated_at = excluded.updated_at
        """,
        (clean_source_host, clean_alias, now, now),
    )
    return {
        "source_host": clean_source_host,
        "display_alias": clean_alias,
        "display_name": clean_alias,
        "updated_at": now,
    }
