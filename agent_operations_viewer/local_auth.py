from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4


PASSWORD_MIN_LENGTH = 8
PBKDF2_ITERATIONS = 310_000
USER_ROLES = {"admin", "viewer"}


@dataclass(slots=True)
class AuthStatus:
    bootstrap_required: bool
    bootstrap_completed_at: str | None
    admin_user: dict[str, object] | None
    local_admin: dict[str, object] | None


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _trimmed(value: object) -> str:
    return str(value or "").strip()


def normalize_user_role(role: str | None) -> str:
    candidate = _trimmed(role).lower()
    return candidate if candidate in USER_ROLES else "viewer"


USER_SELECT_SQL = """
    SELECT
        id,
        username,
        password_hash,
        created_at,
        updated_at,
        last_login_at,
        is_admin,
        role,
        auth_source,
        external_subject,
        display_name,
        email,
        disabled_at,
        last_seen_at
    FROM users
"""


def _user_from_row(row: sqlite3.Row | None) -> dict[str, object] | None:
    if row is None:
        return None
    role = normalize_user_role(row["role"])
    return {
        "id": row["id"],
        "username": row["username"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "last_login_at": row["last_login_at"],
        "last_seen_at": row["last_seen_at"],
        "role": role,
        "is_admin": role == "admin" or bool(row["is_admin"]),
        "auth_source": _trimmed(row["auth_source"]) or "password",
        "external_subject": _trimmed(row["external_subject"]) or None,
        "display_name": _trimmed(row["display_name"]) or _trimmed(row["username"]),
        "email": _trimmed(row["email"]) or "",
        "disabled_at": _trimmed(row["disabled_at"]) or None,
    }


def validate_username(username: str) -> str:
    candidate = username.strip()
    if not candidate:
        raise ValueError("Username is required.")
    if len(candidate) > 120:
        raise ValueError("Username is too long.")
    return candidate


def validate_new_password(password: str) -> str:
    if len(password) < PASSWORD_MIN_LENGTH:
        raise ValueError(f"Password must be at least {PASSWORD_MIN_LENGTH} characters.")
    return password


def hash_password(password: str) -> str:
    validate_new_password(password)
    salt = secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
    )
    salt_b64 = base64.b64encode(salt).decode("ascii")
    hash_b64 = base64.b64encode(derived).decode("ascii")
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt_b64}${hash_b64}"


def verify_password_hash(password_hash: str, password: str) -> bool:
    try:
        algorithm, iterations_raw, salt_b64, expected_b64 = password_hash.split("$", 3)
    except ValueError:
        return False
    if algorithm != "pbkdf2_sha256":
        return False
    try:
        iterations = int(iterations_raw)
        salt = base64.b64decode(salt_b64.encode("ascii"))
        expected = base64.b64decode(expected_b64.encode("ascii"))
    except Exception:
        return False
    actual = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(actual, expected)


def fetch_user_by_id(connection: sqlite3.Connection, user_id: str) -> sqlite3.Row | None:
    return connection.execute(
        USER_SELECT_SQL + "\nWHERE id = ?",
        (user_id,),
    ).fetchone()


def fetch_user_by_username(connection: sqlite3.Connection, username: str) -> sqlite3.Row | None:
    return connection.execute(
        USER_SELECT_SQL + "\nWHERE username = ?",
        (validate_username(username),),
    ).fetchone()


def fetch_user_by_external_subject(connection: sqlite3.Connection, external_subject: str) -> sqlite3.Row | None:
    candidate = _trimmed(external_subject)
    if not candidate:
        return None
    return connection.execute(
        USER_SELECT_SQL + "\nWHERE external_subject = ?",
        (candidate,),
    ).fetchone()


def fetch_admin_user(connection: sqlite3.Connection) -> sqlite3.Row | None:
    return connection.execute(
        USER_SELECT_SQL
        + """
        WHERE role = 'admin' AND disabled_at IS NULL
        ORDER BY created_at ASC
        LIMIT 1
        """
    ).fetchone()


def fetch_local_admin(connection: sqlite3.Connection) -> sqlite3.Row | None:
    return connection.execute(
        USER_SELECT_SQL
        + """
        WHERE role = 'admin'
          AND auth_source = 'password'
          AND disabled_at IS NULL
        ORDER BY created_at ASC
        LIMIT 1
        """
    ).fetchone()


def touch_bootstrap_completed(connection: sqlite3.Connection, completed_at: str) -> None:
    now = _now_iso()
    connection.execute(
        """
        INSERT INTO auth_state (singleton, bootstrap_completed_at, created_at, updated_at)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(singleton) DO UPDATE SET
            bootstrap_completed_at = excluded.bootstrap_completed_at,
            updated_at = excluded.updated_at
        """,
        (completed_at, now, now),
    )


def fetch_auth_status(connection: sqlite3.Connection) -> AuthStatus:
    row = connection.execute(
        """
        SELECT bootstrap_completed_at
        FROM auth_state
        WHERE singleton = 1
        """
    ).fetchone()
    admin_row = fetch_admin_user(connection)
    local_admin_row = fetch_local_admin(connection)
    return AuthStatus(
        bootstrap_required=admin_row is None,
        bootstrap_completed_at=_trimmed(row["bootstrap_completed_at"]) or None if row else None,
        admin_user=_user_from_row(admin_row),
        local_admin=_user_from_row(local_admin_row),
    )


def create_initial_admin(
    connection: sqlite3.Connection,
    *,
    username: str,
    password: str,
) -> dict[str, object]:
    status = fetch_auth_status(connection)
    if not status.bootstrap_required:
        raise ValueError("Initial setup has already been completed.")

    clean_username = validate_username(username)
    password_hash = hash_password(password)
    now = _now_iso()
    user_id = str(uuid4())

    connection.execute(
        """
        INSERT INTO users (
            id,
            username,
            password_hash,
            created_at,
            updated_at,
            last_login_at,
            is_admin,
            role,
            auth_source,
            external_subject,
            display_name,
            email,
            disabled_at,
            last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, 1, 'admin', 'password', NULL, ?, '', NULL, ?)
        """,
        (user_id, clean_username, password_hash, now, now, now, clean_username, now),
    )
    touch_bootstrap_completed(connection, now)
    return {
        "id": user_id,
        "username": clean_username,
        "created_at": now,
        "updated_at": now,
        "last_login_at": now,
        "last_seen_at": now,
        "role": "admin",
        "auth_source": "password",
        "display_name": clean_username,
        "email": "",
        "disabled_at": None,
        "is_admin": True,
    }


def verify_local_password_login(
    connection: sqlite3.Connection,
    username: str,
    password: str,
) -> dict[str, object] | None:
    try:
        user = fetch_user_by_username(connection, username)
    except ValueError:
        return None
    if user is None:
        return None
    if _trimmed(user["disabled_at"]):
        return None
    if not verify_password_hash(_trimmed(user["password_hash"]), password):
        return None
    return _user_from_row(user)


def verify_local_password_for_user(
    connection: sqlite3.Connection,
    user_id: str,
    password: str,
) -> bool:
    user = fetch_user_by_id(connection, user_id)
    if user is None:
        return False
    if _trimmed(user["disabled_at"]):
        return False
    return verify_password_hash(_trimmed(user["password_hash"]), password)


def touch_user_login(connection: sqlite3.Connection, user_id: str) -> None:
    connection.execute(
        "UPDATE users SET last_login_at = ?, last_seen_at = ?, updated_at = ? WHERE id = ?",
        (_now_iso(), _now_iso(), _now_iso(), user_id),
    )


def update_user_password(connection: sqlite3.Connection, user_id: str, password: str) -> None:
    password_hash = hash_password(password)
    now = _now_iso()
    connection.execute(
        "UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?",
        (password_hash, now, user_id),
    )


def touch_user_seen(connection: sqlite3.Connection, user_id: str) -> None:
    now = _now_iso()
    connection.execute(
        "UPDATE users SET last_seen_at = ?, updated_at = ? WHERE id = ?",
        (now, now, user_id),
    )


def create_local_user(
    connection: sqlite3.Connection,
    *,
    username: str,
    password: str,
    role: str = "viewer",
) -> dict[str, object]:
    clean_username = validate_username(username)
    normalized_role = normalize_user_role(role)
    password_hash = hash_password(password)
    now = _now_iso()
    user_id = str(uuid4())
    connection.execute(
        """
        INSERT INTO users (
            id,
            username,
            password_hash,
            created_at,
            updated_at,
            last_login_at,
            is_admin,
            role,
            auth_source,
            external_subject,
            display_name,
            email,
            disabled_at,
            last_seen_at
        ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, 'password', NULL, ?, '', NULL, NULL)
        """,
        (
            user_id,
            clean_username,
            password_hash,
            now,
            now,
            1 if normalized_role == "admin" else 0,
            normalized_role,
            clean_username,
        ),
    )
    row = fetch_user_by_id(connection, user_id)
    return _user_from_row(row) or {
        "id": user_id,
        "username": clean_username,
        "role": normalized_role,
        "is_admin": normalized_role == "admin",
    }


def upsert_proxy_user(
    connection: sqlite3.Connection,
    *,
    external_subject: str,
    username: str,
    display_name: str | None = None,
    email: str | None = None,
) -> dict[str, object]:
    subject = _trimmed(external_subject)
    if not subject:
        raise ValueError("Proxy identity is missing a stable subject.")
    clean_username = validate_username(username)
    clean_display_name = _trimmed(display_name) or clean_username
    clean_email = _trimmed(email) or ""
    now = _now_iso()

    row = fetch_user_by_external_subject(connection, subject)
    if row is None:
        row = fetch_user_by_username(connection, clean_username)
        if row is not None:
            existing_subject = _trimmed(row["external_subject"])
            existing_source = _trimmed(row["auth_source"]) or "password"
            if existing_source != "proxy" or (existing_subject and existing_subject != subject):
                raise ValueError("This proxy identity conflicts with an existing local account.")

    if row is None:
        user_id = str(uuid4())
        connection.execute(
            """
            INSERT INTO users (
                id,
                username,
                password_hash,
                created_at,
                updated_at,
                last_login_at,
                is_admin,
                role,
                auth_source,
                external_subject,
                display_name,
                email,
                disabled_at,
                last_seen_at
            ) VALUES (?, ?, '', ?, ?, NULL, 0, 'viewer', 'proxy', ?, ?, ?, NULL, ?)
            """,
            (user_id, clean_username, now, now, subject, clean_display_name, clean_email, now),
        )
        row = fetch_user_by_id(connection, user_id)
    else:
        connection.execute(
            """
            UPDATE users
            SET
                username = ?,
                auth_source = 'proxy',
                external_subject = ?,
                display_name = ?,
                email = ?,
                last_seen_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (clean_username, subject, clean_display_name, clean_email, now, now, row["id"]),
        )
        row = fetch_user_by_id(connection, str(row["id"]))

    user = _user_from_row(row)
    if user is None:
        raise ValueError("Proxy user could not be persisted.")
    return user


def list_users(connection: sqlite3.Connection) -> list[dict[str, object]]:
    rows = connection.execute(
        USER_SELECT_SQL
        + """
        ORDER BY
            CASE WHEN role = 'admin' THEN 0 ELSE 1 END ASC,
            CASE WHEN disabled_at IS NULL THEN 0 ELSE 1 END ASC,
            created_at ASC
        """
    ).fetchall()
    return [user for row in rows if (user := _user_from_row(row)) is not None]


def _active_admin_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM users
        WHERE role = 'admin' AND disabled_at IS NULL
        """
    ).fetchone()
    return int(row["count"] or 0) if row is not None else 0


def update_user_role(connection: sqlite3.Connection, user_id: str, role: str) -> dict[str, object]:
    row = fetch_user_by_id(connection, user_id)
    if row is None:
        raise ValueError("User not found.")
    next_role = normalize_user_role(role)
    current_role = normalize_user_role(row["role"])
    if current_role == "admin" and next_role != "admin" and _active_admin_count(connection) <= 1:
        raise ValueError("At least one active admin must remain.")
    now = _now_iso()
    connection.execute(
        """
        UPDATE users
        SET role = ?, is_admin = ?, updated_at = ?
        WHERE id = ?
        """,
        (next_role, 1 if next_role == "admin" else 0, now, user_id),
    )
    updated = fetch_user_by_id(connection, user_id)
    return _user_from_row(updated) or {"id": user_id, "role": next_role}


def set_user_disabled(connection: sqlite3.Connection, user_id: str, disabled: bool) -> dict[str, object]:
    row = fetch_user_by_id(connection, user_id)
    if row is None:
        raise ValueError("User not found.")
    current_role = normalize_user_role(row["role"])
    if disabled and current_role == "admin" and _active_admin_count(connection) <= 1:
        raise ValueError("At least one active admin must remain.")
    now = _now_iso()
    connection.execute(
        """
        UPDATE users
        SET disabled_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (now if disabled else None, now, user_id),
    )
    updated = fetch_user_by_id(connection, user_id)
    return _user_from_row(updated) or {"id": user_id}


def claim_initial_admin(connection: sqlite3.Connection, user_id: str) -> dict[str, object]:
    status = fetch_auth_status(connection)
    if not status.bootstrap_required:
        raise ValueError("An admin has already been created.")
    user = fetch_user_by_id(connection, user_id)
    if user is None:
        raise ValueError("User not found.")
    if _trimmed(user["disabled_at"]):
        raise ValueError("Disabled users cannot claim admin access.")
    now = _now_iso()
    connection.execute(
        """
        UPDATE users
        SET role = 'admin', is_admin = 1, updated_at = ?, last_seen_at = COALESCE(last_seen_at, ?)
        WHERE id = ?
        """,
        (now, now, user_id),
    )
    touch_bootstrap_completed(connection, now)
    updated = fetch_user_by_id(connection, user_id)
    return _user_from_row(updated) or {"id": user_id, "role": "admin", "is_admin": True}
