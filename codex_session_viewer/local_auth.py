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


@dataclass(slots=True)
class AuthStatus:
    bootstrap_required: bool
    bootstrap_completed_at: str | None
    local_admin: dict[str, object] | None


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _trimmed(value: object) -> str:
    return str(value or "").strip()


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
        """
        SELECT id, username, password_hash, created_at, updated_at, last_login_at, is_admin
        FROM users
        WHERE id = ?
        """,
        (user_id,),
    ).fetchone()


def fetch_user_by_username(connection: sqlite3.Connection, username: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT id, username, password_hash, created_at, updated_at, last_login_at, is_admin
        FROM users
        WHERE username = ?
        """,
        (validate_username(username),),
    ).fetchone()


def fetch_local_admin(connection: sqlite3.Connection) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT id, username, password_hash, created_at, updated_at, last_login_at, is_admin
        FROM users
        WHERE is_admin = 1
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
    admin_row = fetch_local_admin(connection)
    admin = None
    if admin_row is not None:
        admin = {
            "id": admin_row["id"],
            "username": admin_row["username"],
            "created_at": admin_row["created_at"],
            "updated_at": admin_row["updated_at"],
            "last_login_at": admin_row["last_login_at"],
            "is_admin": bool(admin_row["is_admin"]),
        }
    return AuthStatus(
        bootstrap_required=admin_row is None,
        bootstrap_completed_at=_trimmed(row["bootstrap_completed_at"]) or None if row else None,
        local_admin=admin,
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
            is_admin
        ) VALUES (?, ?, ?, ?, ?, ?, 1)
        """,
        (user_id, clean_username, password_hash, now, now, now),
    )
    touch_bootstrap_completed(connection, now)
    return {
        "id": user_id,
        "username": clean_username,
        "created_at": now,
        "updated_at": now,
        "last_login_at": now,
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
    if not verify_password_hash(_trimmed(user["password_hash"]), password):
        return None
    return {
        "id": user["id"],
        "username": user["username"],
        "created_at": user["created_at"],
        "updated_at": user["updated_at"],
        "last_login_at": user["last_login_at"],
        "is_admin": bool(user["is_admin"]),
    }


def verify_local_password_for_user(
    connection: sqlite3.Connection,
    user_id: str,
    password: str,
) -> bool:
    user = fetch_user_by_id(connection, user_id)
    if user is None:
        return False
    return verify_password_hash(_trimmed(user["password_hash"]), password)


def touch_user_login(connection: sqlite3.Connection, user_id: str) -> None:
    connection.execute(
        "UPDATE users SET last_login_at = ?, updated_at = ? WHERE id = ?",
        (_now_iso(), _now_iso(), user_id),
    )


def update_user_password(connection: sqlite3.Connection, user_id: str, password: str) -> None:
    password_hash = hash_password(password)
    now = _now_iso()
    connection.execute(
        "UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?",
        (password_hash, now, user_id),
    )
