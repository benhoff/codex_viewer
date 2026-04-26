from __future__ import annotations

import hashlib
import secrets
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any


PAIRING_EXPIRY_SECONDS = 900
MACHINE_USAGE_WRITE_INTERVAL_SECONDS = 60
NONCE_RETENTION_SECONDS = 900


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def parse_timestamp(value: str | None) -> datetime | None:
    candidate = trimmed(value)
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
    return parsed.astimezone(UTC)


def hash_pairing_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def generate_pairing_session_id() -> str:
    return f"pair_{secrets.token_hex(12)}"


def list_machine_credentials(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            id,
            label,
            source_host,
            public_key,
            created_by_user_id,
            created_at,
            last_seen_at,
            revoked_at
        FROM machine_credentials
        ORDER BY
            CASE WHEN revoked_at IS NULL THEN 0 ELSE 1 END ASC,
            created_at DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def create_machine_credential(
    connection: sqlite3.Connection,
    *,
    label: str | None,
    source_host: str | None,
    public_key: str,
    created_by_user_id: str | None,
) -> dict[str, Any]:
    now = utc_now_iso()
    machine_id = f"mcred_{secrets.token_hex(12)}"
    normalized_label = trimmed(label) or trimmed(source_host) or "Paired machine"
    normalized_source_host = trimmed(source_host) or normalized_label
    connection.execute(
        """
        INSERT INTO machine_credentials (
            id,
            label,
            source_host,
            public_key,
            created_by_user_id,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            machine_id,
            normalized_label,
            normalized_source_host,
            public_key,
            trimmed(created_by_user_id),
            now,
        ),
    )
    return {
        "id": machine_id,
        "label": normalized_label,
        "source_host": normalized_source_host,
        "public_key": public_key,
        "created_by_user_id": trimmed(created_by_user_id),
        "created_at": now,
        "last_seen_at": None,
        "revoked_at": None,
    }


def fetch_machine_credential(
    connection: sqlite3.Connection,
    machine_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT
            id,
            label,
            source_host,
            public_key,
            created_by_user_id,
            created_at,
            last_seen_at,
            revoked_at
        FROM machine_credentials
        WHERE id = ?
        """,
        (machine_id,),
    ).fetchone()


def fetch_active_machine_credential(
    connection: sqlite3.Connection,
    machine_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT
            id,
            label,
            source_host,
            public_key,
            created_by_user_id,
            created_at,
            last_seen_at,
            revoked_at
        FROM machine_credentials
        WHERE id = ? AND revoked_at IS NULL
        """,
        (machine_id,),
    ).fetchone()


def touch_machine_credential_usage(connection: sqlite3.Connection, machine_id: str) -> None:
    row = connection.execute(
        """
        SELECT last_seen_at
        FROM machine_credentials
        WHERE id = ?
        """,
        (machine_id,),
    ).fetchone()
    if row is None:
        return
    last_seen_at = parse_timestamp(trimmed(row["last_seen_at"]))
    if (
        last_seen_at is not None
        and (datetime.now(tz=UTC) - last_seen_at).total_seconds()
        < MACHINE_USAGE_WRITE_INTERVAL_SECONDS
    ):
        return
    connection.execute(
        """
        UPDATE machine_credentials
        SET last_seen_at = ?
        WHERE id = ?
        """,
        (utc_now_iso(), machine_id),
    )


def revoke_machine_credential(connection: sqlite3.Connection, machine_id: str) -> None:
    connection.execute(
        """
        UPDATE machine_credentials
        SET revoked_at = COALESCE(revoked_at, ?)
        WHERE id = ?
        """,
        (utc_now_iso(), machine_id),
    )


def purge_expired_pairing_sessions(connection: sqlite3.Connection) -> None:
    now = utc_now_iso()
    connection.execute(
        """
        UPDATE pairing_sessions
        SET
            status = 'expired',
            updated_at = ?
        WHERE status = 'pending' AND expires_at <= ?
        """,
        (now, now),
    )


def create_pairing_session(
    connection: sqlite3.Connection,
    *,
    label: str | None,
    source_host: str | None,
    public_key: str,
    secret_hash: str,
    session_id: str | None = None,
    expires_in_seconds: int = PAIRING_EXPIRY_SECONDS,
) -> dict[str, Any]:
    purge_expired_pairing_sessions(connection)
    now = datetime.now(tz=UTC).replace(microsecond=0)
    effective_session_id = trimmed(session_id) or generate_pairing_session_id()
    normalized_label = trimmed(label) or trimmed(source_host) or "Paired machine"
    normalized_source_host = trimmed(source_host) or normalized_label
    expires_at = (now + timedelta(seconds=expires_in_seconds)).isoformat()
    connection.execute(
        """
        INSERT INTO pairing_sessions (
            id,
            label,
            source_host,
            public_key,
            secret_hash,
            status,
            expires_at,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
        """,
        (
            effective_session_id,
            normalized_label,
            normalized_source_host,
            public_key,
            secret_hash,
            expires_at,
            now.isoformat(),
            now.isoformat(),
        ),
    )
    return {
        "id": effective_session_id,
        "label": normalized_label,
        "source_host": normalized_source_host,
        "public_key": public_key,
        "status": "pending",
        "expires_at": expires_at,
        "created_at": now.isoformat(),
    }


def fetch_pairing_session(
    connection: sqlite3.Connection,
    session_id: str,
) -> sqlite3.Row | None:
    purge_expired_pairing_sessions(connection)
    return connection.execute(
        """
        SELECT
            id,
            label,
            source_host,
            public_key,
            secret_hash,
            status,
            machine_credential_id,
            expires_at,
            completed_at,
            used_at,
            created_at,
            updated_at
        FROM pairing_sessions
        WHERE id = ?
        """,
        (session_id,),
    ).fetchone()


def ensure_pairing_session(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    label: str | None,
    source_host: str | None,
    public_key: str,
    secret_hash: str,
    expires_in_seconds: int = PAIRING_EXPIRY_SECONDS,
) -> dict[str, Any]:
    existing = fetch_pairing_session(connection, session_id)
    if existing is None:
        return create_pairing_session(
            connection,
            session_id=session_id,
            label=label,
            source_host=source_host,
            public_key=public_key,
            secret_hash=secret_hash,
            expires_in_seconds=expires_in_seconds,
        )
    normalized_label = trimmed(label) or trimmed(source_host) or "Paired machine"
    normalized_source_host = trimmed(source_host) or normalized_label
    if (
        trimmed(existing["secret_hash"]) != trimmed(secret_hash)
        or trimmed(existing["public_key"]) != trimmed(public_key)
        or trimmed(existing["source_host"]) != normalized_source_host
        or trimmed(existing["label"]) != normalized_label
    ):
        raise ValueError("This pairing session id already exists with different parameters.")
    return _pairing_result_payload(connection, existing)


def fetch_pairing_session_for_secret(
    connection: sqlite3.Connection,
    session_id: str,
    secret: str,
) -> sqlite3.Row | None:
    row = fetch_pairing_session(connection, session_id)
    if row is None:
        return None
    if hash_pairing_secret(secret) != str(row["secret_hash"] or ""):
        return None
    return row


def _pairing_result_payload(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
) -> dict[str, Any]:
    payload = {
        "id": str(row["id"]),
        "label": str(row["label"] or ""),
        "source_host": str(row["source_host"] or ""),
        "public_key": str(row["public_key"] or ""),
        "status": str(row["status"] or "pending"),
        "expires_at": str(row["expires_at"] or ""),
        "completed_at": trimmed(row["completed_at"]),
        "used_at": trimmed(row["used_at"]),
        "machine_credential_id": trimmed(row["machine_credential_id"]),
    }
    machine_id = payload["machine_credential_id"]
    if machine_id:
        machine_row = fetch_machine_credential(connection, machine_id)
        if machine_row is not None:
            payload["machine"] = dict(machine_row)
    return payload


def approve_pairing_session(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    secret: str,
    approver_user_id: str | None,
) -> dict[str, Any]:
    row = fetch_pairing_session_for_secret(connection, session_id, secret)
    if row is None:
        raise ValueError("Pairing request not found.")
    status = str(row["status"] or "pending")
    if status == "declined":
        raise ValueError("This pairing request was declined.")
    if status == "expired":
        raise ValueError("This pairing request expired.")
    machine_id = trimmed(row["machine_credential_id"])
    if machine_id:
        machine_row = fetch_machine_credential(connection, machine_id)
        if machine_row is None:
            raise ValueError("This pairing request is invalid.")
    else:
        machine = create_machine_credential(
            connection,
            label=str(row["label"] or ""),
            source_host=str(row["source_host"] or ""),
            public_key=str(row["public_key"] or ""),
            created_by_user_id=approver_user_id,
        )
        machine_id = str(machine["id"])
    now = utc_now_iso()
    connection.execute(
        """
        UPDATE pairing_sessions
        SET
            status = 'approved',
            machine_credential_id = ?,
            completed_at = COALESCE(completed_at, ?),
            updated_at = ?
        WHERE id = ?
        """,
        (machine_id, now, now, session_id),
    )
    refreshed = fetch_pairing_session(connection, session_id)
    if refreshed is None:
        raise ValueError("Pairing request not found.")
    return _pairing_result_payload(connection, refreshed)


def decline_pairing_session(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    secret: str,
) -> dict[str, Any]:
    row = fetch_pairing_session_for_secret(connection, session_id, secret)
    if row is None:
        raise ValueError("Pairing request not found.")
    now = utc_now_iso()
    connection.execute(
        """
        UPDATE pairing_sessions
        SET
            status = 'declined',
            completed_at = COALESCE(completed_at, ?),
            updated_at = ?
        WHERE id = ?
        """,
        (now, now, session_id),
    )
    refreshed = fetch_pairing_session(connection, session_id)
    if refreshed is None:
        raise ValueError("Pairing request not found.")
    return _pairing_result_payload(connection, refreshed)


def finalize_pairing_session(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    secret: str,
) -> dict[str, Any]:
    row = fetch_pairing_session_for_secret(connection, session_id, secret)
    if row is None:
        raise ValueError("Pairing request not found.")
    status = str(row["status"] or "pending")
    if status == "pending":
        raise ValueError("Pairing approval is still pending.")
    if status == "declined":
        raise ValueError("Pairing request was declined.")
    if status == "expired":
        raise ValueError("Pairing request expired.")
    now = utc_now_iso()
    connection.execute(
        """
        UPDATE pairing_sessions
        SET
            used_at = COALESCE(used_at, ?),
            updated_at = ?
        WHERE id = ?
        """,
        (now, now, session_id),
    )
    refreshed = fetch_pairing_session(connection, session_id)
    if refreshed is None:
        raise ValueError("Pairing request not found.")
    return _pairing_result_payload(connection, refreshed)


def purge_machine_auth_nonces(connection: sqlite3.Connection) -> None:
    cutoff = (
        datetime.now(tz=UTC).replace(microsecond=0)
        - timedelta(seconds=NONCE_RETENTION_SECONDS)
    ).isoformat()
    connection.execute(
        "DELETE FROM machine_auth_nonces WHERE created_at < ?",
        (cutoff,),
    )


def record_machine_auth_nonce(
    connection: sqlite3.Connection,
    *,
    machine_id: str,
    nonce: str,
    created_at: str | None = None,
) -> bool:
    purge_machine_auth_nonces(connection)
    try:
        connection.execute(
            """
            INSERT INTO machine_auth_nonces (
                machine_id,
                nonce,
                created_at
            ) VALUES (?, ?, ?)
            """,
            (
                machine_id,
                nonce,
                trimmed(created_at) or utc_now_iso(),
            ),
        )
    except sqlite3.IntegrityError:
        return False
    return True
