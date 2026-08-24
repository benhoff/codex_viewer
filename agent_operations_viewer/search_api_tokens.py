from __future__ import annotations

import json
import secrets
import sqlite3
from datetime import UTC, datetime
from typing import Any, Iterable

from .api_tokens import hash_api_token, parse_timestamp, utc_now_iso


SEARCH_READ_SCOPE = "search:read"
TOKEN_USAGE_WRITE_INTERVAL_SECONDS = 60


def _trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _normalize_scopes(scopes: Iterable[str]) -> list[str]:
    normalized = sorted({_trimmed(scope) for scope in scopes if _trimmed(scope)})
    if SEARCH_READ_SCOPE not in normalized:
        raise ValueError(f"Search API tokens require the {SEARCH_READ_SCOPE} scope.")
    return normalized


def _parse_scopes(value: object) -> list[str]:
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return sorted({_trimmed(item) for item in parsed if _trimmed(item)})


def create_search_api_token(
    connection: sqlite3.Connection,
    *,
    owner_user_id: str,
    label: str | None,
    scopes: Iterable[str] = (SEARCH_READ_SCOPE,),
) -> dict[str, Any]:
    normalized_owner = _trimmed(owner_user_id)
    if not normalized_owner:
        raise ValueError("A token owner is required.")
    owner = connection.execute(
        "SELECT id, disabled_at FROM users WHERE id = ?",
        (normalized_owner,),
    ).fetchone()
    if owner is None or _trimmed(owner["disabled_at"]):
        raise ValueError("Token owner is not an active user.")

    normalized_label = _trimmed(label) or "Search API token"
    if len(normalized_label) > 120:
        raise ValueError("Token label must be 120 characters or fewer.")
    normalized_scopes = _normalize_scopes(scopes)
    raw_token = f"csvr_read_{secrets.token_urlsafe(24)}"
    token_id = secrets.token_hex(12)
    now = utc_now_iso()
    token_prefix = raw_token[:18]
    connection.execute(
        """
        INSERT INTO search_api_tokens (
            id,
            owner_user_id,
            label,
            token_prefix,
            token_hash,
            scopes_json,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            token_id,
            normalized_owner,
            normalized_label,
            token_prefix,
            hash_api_token(raw_token),
            json.dumps(normalized_scopes, separators=(",", ":")),
            now,
        ),
    )
    return {
        "id": token_id,
        "owner_user_id": normalized_owner,
        "label": normalized_label,
        "token": raw_token,
        "token_prefix": token_prefix,
        "scopes": normalized_scopes,
        "created_at": now,
    }


def list_search_api_tokens(
    connection: sqlite3.Connection,
    owner_user_id: str,
) -> list[dict[str, Any]]:
    normalized_owner = _trimmed(owner_user_id)
    if not normalized_owner:
        return []
    rows = connection.execute(
        """
        SELECT
            id,
            owner_user_id,
            label,
            token_prefix,
            scopes_json,
            created_at,
            last_used_at,
            revoked_at
        FROM search_api_tokens
        WHERE owner_user_id = ?
        ORDER BY
            CASE WHEN revoked_at IS NULL THEN 0 ELSE 1 END ASC,
            created_at DESC
        """,
        (normalized_owner,),
    ).fetchall()
    return [
        {
            **dict(row),
            "scopes": _parse_scopes(row["scopes_json"]),
        }
        for row in rows
    ]


def find_active_search_api_token(
    connection: sqlite3.Connection,
    raw_token: str,
    *,
    required_scope: str = SEARCH_READ_SCOPE,
) -> sqlite3.Row | None:
    candidate = _trimmed(raw_token)
    if not candidate:
        return None
    row = connection.execute(
        """
        SELECT
            t.id,
            t.owner_user_id,
            t.label,
            t.token_prefix,
            t.scopes_json,
            t.created_at,
            t.last_used_at,
            t.revoked_at
        FROM search_api_tokens AS t
        JOIN users AS u
            ON u.id = t.owner_user_id
        WHERE t.token_hash = ?
          AND t.revoked_at IS NULL
          AND u.disabled_at IS NULL
        """,
        (hash_api_token(candidate),),
    ).fetchone()
    if row is None:
        return None
    return row if required_scope in _parse_scopes(row["scopes_json"]) else None


def touch_search_api_token_usage(
    connection: sqlite3.Connection,
    token_id: str,
) -> None:
    row = connection.execute(
        "SELECT last_used_at FROM search_api_tokens WHERE id = ?",
        (token_id,),
    ).fetchone()
    if row is None:
        return
    last_used_at = parse_timestamp(_trimmed(row["last_used_at"]))
    if (
        last_used_at is not None
        and (datetime.now(tz=UTC) - last_used_at.astimezone(UTC)).total_seconds()
        < TOKEN_USAGE_WRITE_INTERVAL_SECONDS
    ):
        return
    connection.execute(
        "UPDATE search_api_tokens SET last_used_at = ? WHERE id = ?",
        (utc_now_iso(), token_id),
    )


def revoke_search_api_token(
    connection: sqlite3.Connection,
    *,
    owner_user_id: str,
    token_id: str,
) -> bool:
    cursor = connection.execute(
        """
        UPDATE search_api_tokens
        SET revoked_at = COALESCE(revoked_at, ?)
        WHERE id = ? AND owner_user_id = ?
        """,
        (utc_now_iso(), token_id, owner_user_id),
    )
    return cursor.rowcount > 0


def delete_search_api_token(
    connection: sqlite3.Connection,
    *,
    owner_user_id: str,
    token_id: str,
) -> bool:
    cursor = connection.execute(
        "DELETE FROM search_api_tokens WHERE id = ? AND owner_user_id = ?",
        (token_id, owner_user_id),
    )
    return cursor.rowcount > 0
