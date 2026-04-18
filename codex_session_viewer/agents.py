from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from .config import Settings


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def upsert_remote_agent_status(
    connection: sqlite3.Connection,
    *,
    source_host: str,
    agent_version: str,
    sync_api_version: str,
    sync_mode: str,
    update_state: str,
    update_message: str | None,
    server_version_seen: str | None,
    server_api_version_seen: str | None,
    last_seen_at: str | None = None,
    last_sync_at: str | None = None,
    last_upload_count: int = 0,
    last_skip_count: int = 0,
    last_fail_count: int = 0,
    last_error: str | None = None,
) -> None:
    seen_at = last_seen_at or utc_now_iso()
    row = connection.execute(
        "SELECT source_host FROM remote_agents WHERE source_host = ?",
        (source_host,),
    ).fetchone()
    values = (
        agent_version,
        sync_api_version,
        sync_mode,
        update_state,
        trimmed(update_message),
        trimmed(server_version_seen),
        trimmed(server_api_version_seen),
        seen_at,
        last_sync_at,
        last_upload_count,
        last_skip_count,
        last_fail_count,
        trimmed(last_error),
    )
    if row is None:
        connection.execute(
            """
            INSERT INTO remote_agents (
                source_host,
                agent_version,
                sync_api_version,
                sync_mode,
                update_state,
                update_message,
                server_version_seen,
                server_api_version_seen,
                last_seen_at,
                last_sync_at,
                last_upload_count,
                last_skip_count,
                last_fail_count,
                last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (source_host, *values),
        )
        return

    connection.execute(
        """
        UPDATE remote_agents
        SET
            agent_version = ?,
            sync_api_version = ?,
            sync_mode = ?,
            update_state = ?,
            update_message = ?,
            server_version_seen = ?,
            server_api_version_seen = ?,
            last_seen_at = ?,
            last_sync_at = ?,
            last_upload_count = ?,
            last_skip_count = ?,
            last_fail_count = ?,
            last_error = ?
        WHERE source_host = ?
        """,
        (*values, source_host),
    )


def build_remote_agent_health(rows: list[sqlite3.Row], settings: Settings) -> list[dict[str, Any]]:
    health_rows: list[dict[str, Any]] = []
    for row in rows:
        agent_version = trimmed(row["agent_version"]) or "unknown"
        sync_api_version = trimmed(row["sync_api_version"]) or "unknown"
        server_version_seen = trimmed(row["server_version_seen"]) or settings.expected_agent_version
        server_api_version_seen = trimmed(row["server_api_version_seen"]) or settings.sync_api_version

        api_mismatch = sync_api_version != settings.sync_api_version
        version_mismatch = agent_version != settings.expected_agent_version
        stale = False
        last_seen_at = trimmed(row["last_seen_at"])
        if last_seen_at:
            seen = datetime.fromisoformat(last_seen_at.replace("Z", "+00:00"))
            stale = (datetime.now(UTC) - seen.astimezone(UTC)).total_seconds() > max(120, settings.sync_interval_seconds * 4)

        health_rows.append(
            {
                "source_host": row["source_host"],
                "agent_version": agent_version,
                "sync_api_version": sync_api_version,
                "sync_mode": trimmed(row["sync_mode"]) or "unknown",
                "update_state": trimmed(row["update_state"]) or "unknown",
                "update_message": trimmed(row["update_message"]),
                "server_version_seen": server_version_seen,
                "server_api_version_seen": server_api_version_seen,
                "last_seen_at": last_seen_at,
                "last_sync_at": trimmed(row["last_sync_at"]),
                "last_upload_count": int(row["last_upload_count"] or 0),
                "last_skip_count": int(row["last_skip_count"] or 0),
                "last_fail_count": int(row["last_fail_count"] or 0),
                "last_error": trimmed(row["last_error"]),
                "api_mismatch": api_mismatch,
                "version_mismatch": version_mismatch,
                "stale": stale,
            }
        )
    health_rows.sort(key=lambda item: item["last_seen_at"] or "", reverse=True)
    return health_rows


def fetch_remote_agent_health(
    connection: sqlite3.Connection,
    settings: Settings,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM remote_agents
        ORDER BY last_seen_at DESC, source_host ASC
        """
    ).fetchall()
    return build_remote_agent_health(rows, settings)
