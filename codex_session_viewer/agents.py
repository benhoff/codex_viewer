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
    acknowledged_raw_resend_token: str | None = None,
    last_raw_resend_at: str | None = None,
) -> None:
    seen_at = last_seen_at or utc_now_iso()
    row = connection.execute(
        """
        SELECT
            source_host,
            requested_raw_resend_token,
            acknowledged_raw_resend_token
        FROM remote_agents
        WHERE source_host = ?
        """,
        (source_host,),
    ).fetchone()
    existing_ack_token = trimmed(row["acknowledged_raw_resend_token"]) if row is not None else None
    ack_token = trimmed(acknowledged_raw_resend_token) or existing_ack_token
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
        ack_token,
        trimmed(last_raw_resend_at),
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
                last_error,
                acknowledged_raw_resend_token,
                last_raw_resend_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            last_error = ?,
            acknowledged_raw_resend_token = ?,
            last_raw_resend_at = ?
        WHERE source_host = ?
        """,
        (*values, source_host),
    )


def request_remote_raw_resend(
    connection: sqlite3.Connection,
    source_host: str,
    note: str | None = None,
) -> str:
    token = utc_now_iso()
    requested_at = token
    row = connection.execute(
        "SELECT source_host FROM remote_agents WHERE source_host = ?",
        (source_host,),
    ).fetchone()
    if row is None:
        connection.execute(
            """
            INSERT INTO remote_agents (
                source_host,
                agent_version,
                sync_api_version,
                sync_mode,
                update_state,
                last_seen_at,
                last_upload_count,
                last_skip_count,
                last_fail_count,
                requested_raw_resend_token,
                requested_raw_resend_at,
                requested_raw_resend_note
            ) VALUES (?, '', '', '', 'awaiting_contact', ?, 0, 0, 0, ?, ?, ?)
            """,
            (
                source_host,
                requested_at,
                token,
                requested_at,
                trimmed(note),
            ),
        )
        return token

    connection.execute(
        """
        UPDATE remote_agents
        SET
            requested_raw_resend_token = ?,
            requested_raw_resend_at = ?,
            requested_raw_resend_note = ?
        WHERE source_host = ?
        """,
        (
            token,
            requested_at,
            trimmed(note),
            source_host,
        ),
    )
    return token


def fetch_pending_remote_actions(
    connection: sqlite3.Connection,
    source_host: str,
) -> dict[str, dict[str, str]]:
    row = connection.execute(
        """
        SELECT
            requested_raw_resend_token,
            requested_raw_resend_at,
            requested_raw_resend_note,
            acknowledged_raw_resend_token
        FROM remote_agents
        WHERE source_host = ?
        """,
        (source_host,),
    ).fetchone()
    if row is None:
        return {}

    requested_token = trimmed(row["requested_raw_resend_token"])
    acknowledged_token = trimmed(row["acknowledged_raw_resend_token"])
    if not requested_token or requested_token == acknowledged_token:
        return {}

    action: dict[str, str] = {
        "token": requested_token,
        "requested_at": trimmed(row["requested_raw_resend_at"]) or "",
    }
    note = trimmed(row["requested_raw_resend_note"])
    if note:
        action["note"] = note
    return {"resend_raw": action}


def build_remote_agent_health(rows: list[sqlite3.Row], settings: Settings) -> list[dict[str, Any]]:
    health_rows: list[dict[str, Any]] = []
    for row in rows:
        agent_version = trimmed(row["agent_version"]) or "unknown"
        sync_api_version = trimmed(row["sync_api_version"]) or "unknown"
        server_version_seen = trimmed(row["server_version_seen"]) or settings.expected_agent_version
        server_api_version_seen = trimmed(row["server_api_version_seen"]) or settings.sync_api_version

        api_mismatch = sync_api_version != settings.sync_api_version
        version_mismatch = agent_version != settings.expected_agent_version
        requested_raw_resend_token = trimmed(row["requested_raw_resend_token"])
        acknowledged_raw_resend_token = trimmed(row["acknowledged_raw_resend_token"])
        pending_raw_resend = bool(
            requested_raw_resend_token
            and requested_raw_resend_token != acknowledged_raw_resend_token
        )
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
                "pending_raw_resend": pending_raw_resend,
                "requested_raw_resend_at": trimmed(row["requested_raw_resend_at"]),
                "requested_raw_resend_note": trimmed(row["requested_raw_resend_note"]),
                "last_raw_resend_at": trimmed(row["last_raw_resend_at"]),
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
