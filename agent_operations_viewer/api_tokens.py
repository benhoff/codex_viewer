from __future__ import annotations

import hashlib
import secrets
import sqlite3
from datetime import UTC, datetime
from typing import Any


TOKEN_USAGE_WRITE_INTERVAL_SECONDS = 60


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def hash_api_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def active_api_token_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM api_tokens WHERE revoked_at IS NULL"
    ).fetchone()
    return int(row["count"] or 0) if row is not None else 0


def list_api_tokens(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            id,
            label,
            token_prefix,
            created_at,
            last_used_at,
            last_used_source_host,
            revoked_at
        FROM api_tokens
        ORDER BY
            CASE WHEN revoked_at IS NULL THEN 0 ELSE 1 END ASC,
            created_at DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def create_api_token(connection: sqlite3.Connection, label: str | None) -> dict[str, str]:
    normalized_label = trimmed(label) or "Sync API token"
    raw_token = f"csvr_{secrets.token_urlsafe(24)}"
    token_id = secrets.token_hex(12)
    now = utc_now_iso()
    connection.execute(
        """
        INSERT INTO api_tokens (
            id,
            label,
            token_prefix,
            token_hash,
            created_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            token_id,
            normalized_label,
            raw_token[:14],
            hash_api_token(raw_token),
            now,
        ),
    )
    return {
        "id": token_id,
        "label": normalized_label,
        "token": raw_token,
        "token_prefix": raw_token[:14],
        "created_at": now,
    }


def find_active_api_token(
    connection: sqlite3.Connection,
    raw_token: str,
) -> sqlite3.Row | None:
    candidate = trimmed(raw_token)
    if not candidate:
        return None
    return connection.execute(
        """
        SELECT
            id,
            label,
            token_prefix,
            created_at,
            last_used_at,
            last_used_source_host,
            revoked_at
        FROM api_tokens
        WHERE token_hash = ? AND revoked_at IS NULL
        """,
        (hash_api_token(candidate),),
    ).fetchone()


def touch_api_token_usage(
    connection: sqlite3.Connection,
    token_id: str,
    source_host: str | None,
) -> None:
    row = connection.execute(
        """
        SELECT
            last_used_at,
            last_used_source_host
        FROM api_tokens
        WHERE id = ?
        """,
        (token_id,),
    ).fetchone()
    if row is None:
        return

    normalized_source_host = trimmed(source_host)
    last_used_at = parse_timestamp(trimmed(row["last_used_at"]))
    if (
        last_used_at is not None
        and normalized_source_host == trimmed(row["last_used_source_host"])
        and (datetime.now(tz=UTC) - last_used_at.astimezone(UTC)).total_seconds()
        < TOKEN_USAGE_WRITE_INTERVAL_SECONDS
    ):
        return

    connection.execute(
        """
        UPDATE api_tokens
        SET
            last_used_at = ?,
            last_used_source_host = ?
        WHERE id = ?
        """,
        (
            utc_now_iso(),
            normalized_source_host,
            token_id,
        ),
    )


def revoke_api_token(connection: sqlite3.Connection, token_id: str) -> None:
    connection.execute(
        """
        UPDATE api_tokens
        SET revoked_at = COALESCE(revoked_at, ?)
        WHERE id = ?
        """,
        (utc_now_iso(), token_id),
    )


def delete_api_token(connection: sqlite3.Connection, token_id: str) -> None:
    connection.execute("DELETE FROM api_tokens WHERE id = ?", (token_id,))
