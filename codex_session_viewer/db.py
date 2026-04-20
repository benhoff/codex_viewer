from __future__ import annotations

from contextlib import contextmanager
import sqlite3
import threading
from pathlib import Path

from .session_rollups import (
    backfill_session_rollups,
    backfill_session_turn_activity_daily,
)
from .onboarding import ensure_onboarding_state_row
from .turn_index import backfill_session_turns


SESSION_COLUMN_DEFS = {
    "id": "TEXT PRIMARY KEY",
    "source_path": "TEXT NOT NULL",
    "source_root": "TEXT NOT NULL",
    "file_size": "INTEGER NOT NULL",
    "file_mtime_ns": "INTEGER NOT NULL",
    "content_sha256": "TEXT NOT NULL DEFAULT ''",
    "session_timestamp": "TEXT",
    "started_at": "TEXT",
    "ended_at": "TEXT",
    "cwd": "TEXT",
    "cwd_name": "TEXT NOT NULL DEFAULT ''",
    "source_host": "TEXT NOT NULL DEFAULT ''",
    "originator": "TEXT",
    "cli_version": "TEXT",
    "source": "TEXT",
    "model_provider": "TEXT",
    "git_branch": "TEXT",
    "git_commit_hash": "TEXT",
    "git_repository_url": "TEXT",
    "github_remote_url": "TEXT",
    "github_org": "TEXT",
    "github_repo": "TEXT",
    "github_slug": "TEXT",
    "inferred_project_kind": "TEXT NOT NULL DEFAULT 'directory'",
    "inferred_project_key": "TEXT NOT NULL DEFAULT ''",
    "inferred_project_label": "TEXT NOT NULL DEFAULT ''",
    "summary": "TEXT NOT NULL",
    "event_count": "INTEGER NOT NULL DEFAULT 0",
    "user_message_count": "INTEGER NOT NULL DEFAULT 0",
    "assistant_message_count": "INTEGER NOT NULL DEFAULT 0",
    "tool_call_count": "INTEGER NOT NULL DEFAULT 0",
    "rollup_version": "INTEGER NOT NULL DEFAULT 0",
    "turn_activity_rollup_version": "INTEGER NOT NULL DEFAULT 0",
    "turn_index_version": "INTEGER NOT NULL DEFAULT 0",
    "turn_count": "INTEGER NOT NULL DEFAULT 0",
    "last_user_message": "TEXT NOT NULL DEFAULT ''",
    "last_turn_timestamp": "TEXT",
    "latest_turn_summary": "TEXT",
    "command_failure_count": "INTEGER NOT NULL DEFAULT 0",
    "aborted_turn_count": "INTEGER NOT NULL DEFAULT 0",
    "import_warning": "TEXT",
    "search_text": "TEXT NOT NULL DEFAULT ''",
    "raw_meta_json": "TEXT NOT NULL",
    "imported_at": "TEXT NOT NULL",
    "updated_at": "TEXT NOT NULL",
}

REMOTE_AGENT_COLUMN_DEFS = {
    "source_host": "TEXT PRIMARY KEY",
    "agent_version": "TEXT NOT NULL DEFAULT ''",
    "sync_api_version": "TEXT NOT NULL DEFAULT ''",
    "sync_mode": "TEXT NOT NULL DEFAULT ''",
    "update_state": "TEXT NOT NULL DEFAULT ''",
    "update_message": "TEXT",
    "server_version_seen": "TEXT",
    "server_api_version_seen": "TEXT",
    "last_seen_at": "TEXT NOT NULL",
    "last_sync_at": "TEXT",
    "last_upload_count": "INTEGER NOT NULL DEFAULT 0",
    "last_skip_count": "INTEGER NOT NULL DEFAULT 0",
    "last_fail_count": "INTEGER NOT NULL DEFAULT 0",
    "last_error": "TEXT",
    "last_failed_source_path": "TEXT",
    "last_failure_detail": "TEXT",
    "requested_raw_resend_token": "TEXT",
    "requested_raw_resend_at": "TEXT",
    "requested_raw_resend_note": "TEXT",
    "acknowledged_raw_resend_token": "TEXT",
    "last_raw_resend_at": "TEXT",
}

USER_COLUMN_DEFS = {
    "id": "TEXT PRIMARY KEY",
    "username": "TEXT NOT NULL UNIQUE",
    "password_hash": "TEXT NOT NULL",
    "created_at": "TEXT NOT NULL",
    "updated_at": "TEXT NOT NULL",
    "last_login_at": "TEXT",
    "is_admin": "INTEGER NOT NULL DEFAULT 0",
    "role": "TEXT NOT NULL DEFAULT 'viewer'",
    "auth_source": "TEXT NOT NULL DEFAULT 'password'",
    "external_subject": "TEXT",
    "display_name": "TEXT NOT NULL DEFAULT ''",
    "email": "TEXT NOT NULL DEFAULT ''",
    "disabled_at": "TEXT",
    "last_seen_at": "TEXT",
}

PROJECT_COLUMN_DEFS = {
    "id": "TEXT PRIMARY KEY",
    "current_group_key": "TEXT NOT NULL UNIQUE",
    "display_label": "TEXT NOT NULL DEFAULT ''",
    "visibility": "TEXT NOT NULL DEFAULT 'authenticated'",
    "created_at": "TEXT NOT NULL",
    "updated_at": "TEXT NOT NULL",
}

PROJECT_SOURCE_COLUMN_DEFS = {
    "match_project_key": "TEXT PRIMARY KEY",
    "project_id": "TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE",
    "created_at": "TEXT NOT NULL",
    "updated_at": "TEXT NOT NULL",
}

PROJECT_ACL_COLUMN_DEFS = {
    "project_id": "TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE",
    "user_id": "TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE",
    "role": "TEXT NOT NULL DEFAULT 'viewer'",
    "granted_by_user_id": "TEXT REFERENCES users(id) ON DELETE SET NULL",
    "created_at": "TEXT NOT NULL",
    "updated_at": "TEXT NOT NULL",
}

AUTH_STATE_COLUMN_DEFS = {
    "singleton": "INTEGER PRIMARY KEY CHECK(singleton = 1)",
    "bootstrap_completed_at": "TEXT",
    "created_at": "TEXT NOT NULL",
    "updated_at": "TEXT NOT NULL",
}

ONBOARDING_STATE_COLUMN_DEFS = {
    "singleton": "INTEGER PRIMARY KEY CHECK(singleton = 1)",
    "completed_at": "TEXT",
    "first_heartbeat_at": "TEXT",
    "first_heartbeat_source_host": "TEXT",
    "first_session_ingested_at": "TEXT",
    "first_session_source_host": "TEXT",
    "last_failure_reason": "TEXT",
    "created_at": "TEXT NOT NULL",
    "updated_at": "TEXT NOT NULL",
}

ALERT_INCIDENT_COLUMN_DEFS = {
    "alert_key": "TEXT PRIMARY KEY",
    "source_host": "TEXT NOT NULL",
    "issue_kind": "TEXT NOT NULL",
    "status": "TEXT NOT NULL DEFAULT 'open'",
    "severity": "TEXT NOT NULL DEFAULT 'warning'",
    "title": "TEXT NOT NULL DEFAULT ''",
    "detail": "TEXT NOT NULL DEFAULT ''",
    "fingerprint": "TEXT NOT NULL DEFAULT ''",
    "opened_at": "TEXT NOT NULL",
    "last_seen_at": "TEXT NOT NULL",
    "resolved_at": "TEXT",
    "last_notified_at": "TEXT",
    "last_notification_kind": "TEXT",
    "last_notification_fingerprint": "TEXT",
    "detail_json": "TEXT NOT NULL DEFAULT '{}'",
    "updated_at": "TEXT NOT NULL",
}

ALERT_DELIVERY_COLUMN_DEFS = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "alert_key": "TEXT NOT NULL",
    "source_host": "TEXT NOT NULL DEFAULT ''",
    "issue_kind": "TEXT NOT NULL DEFAULT ''",
    "notification_kind": "TEXT NOT NULL DEFAULT 'open'",
    "provider": "TEXT NOT NULL DEFAULT 'webhook'",
    "payload_json": "TEXT NOT NULL",
    "status": "TEXT NOT NULL DEFAULT 'pending'",
    "attempt_count": "INTEGER NOT NULL DEFAULT 0",
    "next_attempt_at": "TEXT NOT NULL",
    "created_at": "TEXT NOT NULL",
    "claimed_at": "TEXT",
    "sent_at": "TEXT",
    "last_error": "TEXT",
}

SAVED_TURN_COLUMN_DEFS = {
    "owner_scope": "TEXT NOT NULL",
    "session_id": "TEXT NOT NULL",
    "turn_number": "INTEGER NOT NULL",
    "prompt_excerpt": "TEXT NOT NULL DEFAULT ''",
    "response_excerpt": "TEXT NOT NULL DEFAULT ''",
    "prompt_timestamp": "TEXT",
    "response_timestamp": "TEXT",
    "status": "TEXT NOT NULL DEFAULT 'open'",
    "created_at": "TEXT NOT NULL",
    "resolved_at": "TEXT",
    "updated_at": "TEXT NOT NULL",
}

SESSION_TURN_COLUMN_DEFS = {
    "session_id": "TEXT NOT NULL",
    "turn_number": "INTEGER NOT NULL",
    "start_event_index": "INTEGER NOT NULL DEFAULT 0",
    "end_event_index": "INTEGER NOT NULL DEFAULT 0",
    "prompt_excerpt": "TEXT NOT NULL DEFAULT ''",
    "prompt_timestamp": "TEXT",
    "response_excerpt": "TEXT NOT NULL DEFAULT ''",
    "response_timestamp": "TEXT",
    "response_state": "TEXT NOT NULL DEFAULT 'missing'",
    "latest_timestamp": "TEXT",
    "command_count": "INTEGER NOT NULL DEFAULT 0",
    "patch_count": "INTEGER NOT NULL DEFAULT 0",
    "failure_count": "INTEGER NOT NULL DEFAULT 0",
    "files_touched_count": "INTEGER NOT NULL DEFAULT 0",
}

SESSION_COLUMNS = list(SESSION_COLUMN_DEFS.keys())
EVENT_COLUMNS = [
    "id",
    "session_id",
    "event_index",
    "timestamp",
    "record_type",
    "payload_type",
    "kind",
    "role",
    "title",
    "display_text",
    "detail_text",
    "tool_name",
    "call_id",
    "command_text",
    "exit_code",
    "record_json",
]
LEGACY_SESSION_TABLES = ("sessions_legacy", "__sessions_rebuild__")
LEGACY_EVENT_TABLES = ("events_legacy", "__events_rebuild__")

SESSION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    source_path TEXT NOT NULL,
    source_root TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    file_mtime_ns INTEGER NOT NULL,
    content_sha256 TEXT NOT NULL DEFAULT '',
    session_timestamp TEXT,
    started_at TEXT,
    ended_at TEXT,
    cwd TEXT,
    cwd_name TEXT NOT NULL DEFAULT '',
    source_host TEXT NOT NULL DEFAULT '',
    originator TEXT,
    cli_version TEXT,
    source TEXT,
    model_provider TEXT,
    git_branch TEXT,
    git_commit_hash TEXT,
    git_repository_url TEXT,
    github_remote_url TEXT,
    github_org TEXT,
    github_repo TEXT,
    github_slug TEXT,
    inferred_project_kind TEXT NOT NULL DEFAULT 'directory',
    inferred_project_key TEXT NOT NULL DEFAULT '',
    inferred_project_label TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 0,
    user_message_count INTEGER NOT NULL DEFAULT 0,
    assistant_message_count INTEGER NOT NULL DEFAULT 0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    rollup_version INTEGER NOT NULL DEFAULT 0,
    turn_activity_rollup_version INTEGER NOT NULL DEFAULT 0,
    turn_index_version INTEGER NOT NULL DEFAULT 0,
    turn_count INTEGER NOT NULL DEFAULT 0,
    last_user_message TEXT NOT NULL DEFAULT '',
    last_turn_timestamp TEXT,
    latest_turn_summary TEXT,
    command_failure_count INTEGER NOT NULL DEFAULT 0,
    aborted_turn_count INTEGER NOT NULL DEFAULT 0,
    import_warning TEXT,
    search_text TEXT NOT NULL DEFAULT '',
    raw_meta_json TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

EVENT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    event_index INTEGER NOT NULL,
    timestamp TEXT,
    record_type TEXT NOT NULL,
    payload_type TEXT,
    kind TEXT NOT NULL,
    role TEXT,
    title TEXT NOT NULL,
    display_text TEXT NOT NULL DEFAULT '',
    detail_text TEXT NOT NULL DEFAULT '',
    tool_name TEXT,
    call_id TEXT,
    command_text TEXT,
    exit_code INTEGER,
    record_json TEXT NOT NULL
);
"""

OTHER_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS project_overrides (
    match_project_key TEXT PRIMARY KEY,
    override_group_key TEXT,
    override_organization TEXT,
    override_repository TEXT,
    override_remote_url TEXT,
    override_display_label TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ignored_project_sources (
    match_project_key TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS remote_agents (
    source_host TEXT PRIMARY KEY,
    agent_version TEXT NOT NULL DEFAULT '',
    sync_api_version TEXT NOT NULL DEFAULT '',
    sync_mode TEXT NOT NULL DEFAULT '',
    update_state TEXT NOT NULL DEFAULT '',
    update_message TEXT,
    server_version_seen TEXT,
    server_api_version_seen TEXT,
    last_seen_at TEXT NOT NULL,
    last_sync_at TEXT,
    last_upload_count INTEGER NOT NULL DEFAULT 0,
    last_skip_count INTEGER NOT NULL DEFAULT 0,
    last_fail_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_failed_source_path TEXT,
    last_failure_detail TEXT,
    requested_raw_resend_token TEXT,
    requested_raw_resend_at TEXT,
    requested_raw_resend_note TEXT,
    acknowledged_raw_resend_token TEXT,
    last_raw_resend_at TEXT
);

CREATE TABLE IF NOT EXISTS api_tokens (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    token_prefix TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    last_used_at TEXT,
    last_used_source_host TEXT,
    revoked_at TEXT
);

CREATE TABLE IF NOT EXISTS server_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_login_at TEXT,
    is_admin INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    current_group_key TEXT NOT NULL UNIQUE,
    display_label TEXT NOT NULL DEFAULT '',
    visibility TEXT NOT NULL DEFAULT 'authenticated',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_sources (
    match_project_key TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_acl (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role TEXT NOT NULL DEFAULT 'viewer',
    granted_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (project_id, user_id)
);

CREATE TABLE IF NOT EXISTS auth_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    bootstrap_completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS onboarding_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    completed_at TEXT,
    first_heartbeat_at TEXT,
    first_heartbeat_source_host TEXT,
    first_session_ingested_at TEXT,
    first_session_source_host TEXT,
    last_failure_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_incidents (
    alert_key TEXT PRIMARY KEY,
    source_host TEXT NOT NULL,
    issue_kind TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    severity TEXT NOT NULL DEFAULT 'warning',
    title TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT '',
    fingerprint TEXT NOT NULL DEFAULT '',
    opened_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    resolved_at TEXT,
    last_notified_at TEXT,
    last_notification_kind TEXT,
    last_notification_fingerprint TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_key TEXT NOT NULL,
    source_host TEXT NOT NULL DEFAULT '',
    issue_kind TEXT NOT NULL DEFAULT '',
    notification_kind TEXT NOT NULL DEFAULT 'open',
    provider TEXT NOT NULL DEFAULT 'webhook',
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    claimed_at TEXT,
    sent_at TEXT,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS saved_turns (
    owner_scope TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    turn_number INTEGER NOT NULL,
    prompt_excerpt TEXT NOT NULL DEFAULT '',
    response_excerpt TEXT NOT NULL DEFAULT '',
    prompt_timestamp TEXT,
    response_timestamp TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (owner_scope, session_id, turn_number)
);

CREATE TABLE IF NOT EXISTS session_turn_activity_daily (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    activity_date TEXT NOT NULL,
    turn_count INTEGER NOT NULL DEFAULT 0,
    latest_timestamp TEXT,
    PRIMARY KEY (session_id, activity_date)
);

CREATE TABLE IF NOT EXISTS session_turns (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    turn_number INTEGER NOT NULL,
    start_event_index INTEGER NOT NULL DEFAULT 0,
    end_event_index INTEGER NOT NULL DEFAULT 0,
    prompt_excerpt TEXT NOT NULL DEFAULT '',
    prompt_timestamp TEXT,
    response_excerpt TEXT NOT NULL DEFAULT '',
    response_timestamp TEXT,
    response_state TEXT NOT NULL DEFAULT 'missing',
    latest_timestamp TEXT,
    command_count INTEGER NOT NULL DEFAULT 0,
    patch_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    files_touched_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, turn_number)
);
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_sessions_timestamp ON sessions(session_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_cwd ON sessions(cwd);
CREATE INDEX IF NOT EXISTS idx_sessions_model_provider ON sessions(model_provider);
CREATE INDEX IF NOT EXISTS idx_sessions_source_host ON sessions(source_host);
CREATE INDEX IF NOT EXISTS idx_sessions_github_slug ON sessions(github_slug);
CREATE INDEX IF NOT EXISTS idx_sessions_project_key ON sessions(inferred_project_key);
CREATE INDEX IF NOT EXISTS idx_sessions_last_turn_timestamp ON sessions(last_turn_timestamp DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_host_path_unique
ON sessions(source_host, source_path);

CREATE INDEX IF NOT EXISTS idx_session_turn_activity_date
ON session_turn_activity_daily(activity_date DESC);

CREATE INDEX IF NOT EXISTS idx_session_turns_latest
ON session_turns(latest_timestamp DESC, session_id DESC, turn_number DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_session_index
ON events(session_id, event_index);
CREATE INDEX IF NOT EXISTS idx_events_session_kind
ON events(session_id, kind);

CREATE INDEX IF NOT EXISTS idx_remote_agents_last_seen
ON remote_agents(last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_incidents_source_status
ON alert_incidents(source_host, status, issue_kind);

CREATE INDEX IF NOT EXISTS idx_alert_incidents_status_seen
ON alert_incidents(status, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_deliveries_status_next_attempt
ON alert_deliveries(status, next_attempt_at ASC, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_alert_deliveries_alert_key_status
ON alert_deliveries(alert_key, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_api_tokens_created_at
ON api_tokens(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_api_tokens_last_used_at
ON api_tokens(last_used_at DESC);

CREATE INDEX IF NOT EXISTS idx_project_overrides_override_group_key
ON project_overrides(override_group_key);

CREATE INDEX IF NOT EXISTS idx_projects_visibility
ON projects(visibility, current_group_key);

CREATE INDEX IF NOT EXISTS idx_project_sources_project_id
ON project_sources(project_id);

CREATE INDEX IF NOT EXISTS idx_project_acl_user_id
ON project_acl(user_id, project_id);

CREATE INDEX IF NOT EXISTS idx_project_acl_project_role
ON project_acl(project_id, role);

CREATE INDEX IF NOT EXISTS idx_saved_turns_owner_status_updated
ON saved_turns(owner_scope, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_saved_turns_session
ON saved_turns(session_id, turn_number);

CREATE INDEX IF NOT EXISTS idx_users_username
ON users(username);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_external_subject
ON users(external_subject)
WHERE external_subject IS NOT NULL AND TRIM(external_subject) <> '';
"""

WRITE_LOCK = threading.RLock()


def connect(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    return connection


@contextmanager
def write_transaction(connection: sqlite3.Connection):
    with WRITE_LOCK:
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield connection
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()


def table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row["name"] for row in rows}


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def sessions_need_rebuild(connection: sqlite3.Connection) -> bool:
    if not table_exists(connection, "sessions"):
        return False
    for index_row in connection.execute("PRAGMA index_list(sessions)").fetchall():
        if not index_row["unique"]:
            continue
        index_name = index_row["name"]
        columns = [
            item["name"]
            for item in connection.execute(f"PRAGMA index_info({index_name})").fetchall()
        ]
        if columns == ["source_path"]:
            return True
    return False


def rebuild_sessions_table(connection: sqlite3.Connection) -> None:
    existing_columns = table_columns(connection, "sessions")
    connection.execute("DROP TABLE IF EXISTS __sessions_rebuild__")
    connection.execute("ALTER TABLE sessions RENAME TO __sessions_rebuild__")
    connection.execute(SESSION_TABLE_SQL)

    insert_columns = ", ".join(SESSION_COLUMNS)
    select_columns = ", ".join(
        column if column in existing_columns else f"{default_select(column)} AS {column}"
        for column in SESSION_COLUMNS
    )
    connection.execute(
        f"INSERT INTO sessions ({insert_columns}) SELECT {select_columns} FROM __sessions_rebuild__"
    )
    connection.execute("DROP TABLE __sessions_rebuild__")


def events_need_rebuild(connection: sqlite3.Connection) -> bool:
    if not table_exists(connection, "events"):
        return False
    foreign_keys = connection.execute("PRAGMA foreign_key_list(events)").fetchall()
    if not foreign_keys:
        return True
    session_foreign_keys = [row for row in foreign_keys if row["from"] == "session_id"]
    if not session_foreign_keys:
        return True
    return any(row["table"] != "sessions" or row["to"] != "id" for row in session_foreign_keys)


def rebuild_events_table(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "events"):
        connection.execute(EVENT_TABLE_SQL)
        return

    existing_columns = table_columns(connection, "events")
    connection.execute("DROP TABLE IF EXISTS __events_rebuild__")
    connection.execute("ALTER TABLE events RENAME TO __events_rebuild__")
    connection.execute(EVENT_TABLE_SQL)

    insert_columns = ", ".join(EVENT_COLUMNS)
    select_columns = ", ".join(
        column if column in existing_columns else f"{default_event_select(column)} AS {column}"
        for column in EVENT_COLUMNS
    )
    connection.execute(
        f"INSERT INTO events ({insert_columns}) SELECT {select_columns} FROM __events_rebuild__"
    )
    connection.execute("DROP TABLE __events_rebuild__")


def recover_legacy_sessions(connection: sqlite3.Connection) -> None:
    insert_columns = ", ".join(SESSION_COLUMNS)
    for table_name in LEGACY_SESSION_TABLES:
        if not table_exists(connection, table_name):
            continue
        existing_columns = table_columns(connection, table_name)
        select_columns = ", ".join(
            column if column in existing_columns else f"{default_select(column)} AS {column}"
            for column in SESSION_COLUMNS
        )
        connection.execute(
            f"INSERT OR IGNORE INTO sessions ({insert_columns}) SELECT {select_columns} FROM {table_name}"
        )
        connection.execute(f"DROP TABLE {table_name}")


def recover_legacy_events(connection: sqlite3.Connection) -> None:
    insert_columns = ", ".join(EVENT_COLUMNS)
    for table_name in LEGACY_EVENT_TABLES:
        if not table_exists(connection, table_name):
            continue
        existing_columns = table_columns(connection, table_name)
        select_columns = ", ".join(
            column if column in existing_columns else f"{default_event_select(column)} AS {column}"
            for column in EVENT_COLUMNS
        )
        connection.execute(
            f"INSERT OR IGNORE INTO events ({insert_columns}) SELECT {select_columns} FROM {table_name}"
        )
        connection.execute(f"DROP TABLE {table_name}")


def default_select(column_name: str) -> str:
    if column_name in {
        "source_path",
        "source_root",
        "content_sha256",
        "cwd_name",
        "source_host",
        "inferred_project_key",
        "inferred_project_label",
        "search_text",
    }:
        return "''"
    if column_name == "inferred_project_kind":
        return "'directory'"
    if column_name in {
        "file_size",
        "file_mtime_ns",
        "event_count",
        "user_message_count",
        "assistant_message_count",
        "tool_call_count",
        "rollup_version",
        "turn_activity_rollup_version",
        "turn_index_version",
        "turn_count",
        "command_failure_count",
        "aborted_turn_count",
    }:
        return "0"
    if column_name == "last_user_message":
        return "''"
    return "NULL"


def default_event_select(column_name: str) -> str:
    if column_name in {
        "session_id",
        "record_type",
        "kind",
        "title",
        "display_text",
        "detail_text",
        "record_json",
    }:
        return "''"
    if column_name == "event_index":
        return "0"
    return "NULL"


def ensure_session_columns(connection: sqlite3.Connection) -> None:
    session_columns = table_columns(connection, "sessions")
    for column_name, column_def in SESSION_COLUMN_DEFS.items():
        if column_name not in session_columns and column_name != "id":
            connection.execute(
                f"ALTER TABLE sessions ADD COLUMN {column_name} {column_def}"
            )


def ensure_remote_agent_columns(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "remote_agents"):
        return
    remote_agent_columns = table_columns(connection, "remote_agents")
    for column_name, column_def in REMOTE_AGENT_COLUMN_DEFS.items():
        if column_name not in remote_agent_columns and column_name != "source_host":
            connection.execute(
                f"ALTER TABLE remote_agents ADD COLUMN {column_name} {column_def}"
            )


def ensure_user_columns(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "users"):
        return
    user_columns = table_columns(connection, "users")
    for column_name, column_def in USER_COLUMN_DEFS.items():
        if column_name not in user_columns and column_name != "id":
            connection.execute(
                f"ALTER TABLE users ADD COLUMN {column_name} {column_def}"
            )


def ensure_project_columns(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "projects"):
        return
    project_columns = table_columns(connection, "projects")
    for column_name, column_def in PROJECT_COLUMN_DEFS.items():
        if column_name not in project_columns and column_name != "id":
            connection.execute(
                f"ALTER TABLE projects ADD COLUMN {column_name} {column_def}"
            )


def ensure_project_source_columns(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "project_sources"):
        return
    project_source_columns = table_columns(connection, "project_sources")
    for column_name, column_def in PROJECT_SOURCE_COLUMN_DEFS.items():
        if column_name not in project_source_columns and column_name != "match_project_key":
            connection.execute(
                f"ALTER TABLE project_sources ADD COLUMN {column_name} {column_def}"
            )


def ensure_project_acl_columns(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "project_acl"):
        return
    project_acl_columns = table_columns(connection, "project_acl")
    for column_name, column_def in PROJECT_ACL_COLUMN_DEFS.items():
        if column_name not in project_acl_columns:
            connection.execute(
                f"ALTER TABLE project_acl ADD COLUMN {column_name} {column_def}"
            )


def backfill_user_access_columns(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "users"):
        return
    connection.execute(
        """
        UPDATE users
        SET role = CASE WHEN is_admin = 1 THEN 'admin' ELSE 'viewer' END
        WHERE COALESCE(TRIM(role), '') = ''
           OR (role = 'viewer' AND is_admin = 1)
        """
    )
    connection.execute(
        """
        UPDATE users
        SET auth_source = 'password'
        WHERE COALESCE(TRIM(auth_source), '') = ''
        """
    )
    connection.execute(
        """
        UPDATE users
        SET display_name = username
        WHERE COALESCE(TRIM(display_name), '') = ''
        """
    )
    connection.execute(
        """
        UPDATE users
        SET email = ''
        WHERE email IS NULL
        """
    )
    connection.execute(
        """
        UPDATE users
        SET last_seen_at = COALESCE(last_seen_at, last_login_at)
        WHERE last_seen_at IS NULL AND last_login_at IS NOT NULL
        """
    )


def ensure_auth_state_columns(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "auth_state"):
        return
    auth_state_columns = table_columns(connection, "auth_state")
    for column_name, column_def in AUTH_STATE_COLUMN_DEFS.items():
        if column_name not in auth_state_columns and column_name != "singleton":
            connection.execute(
                f"ALTER TABLE auth_state ADD COLUMN {column_name} {column_def}"
            )


def ensure_onboarding_state_columns(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "onboarding_state"):
        return
    onboarding_state_columns = table_columns(connection, "onboarding_state")
    for column_name, column_def in ONBOARDING_STATE_COLUMN_DEFS.items():
        if column_name not in onboarding_state_columns and column_name != "singleton":
            connection.execute(
                f"ALTER TABLE onboarding_state ADD COLUMN {column_name} {column_def}"
            )


def ensure_alert_incident_columns(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "alert_incidents"):
        return
    alert_incident_columns = table_columns(connection, "alert_incidents")
    for column_name, column_def in ALERT_INCIDENT_COLUMN_DEFS.items():
        if column_name not in alert_incident_columns and column_name != "alert_key":
            connection.execute(
                f"ALTER TABLE alert_incidents ADD COLUMN {column_name} {column_def}"
            )


def ensure_alert_delivery_columns(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "alert_deliveries"):
        return
    alert_delivery_columns = table_columns(connection, "alert_deliveries")
    for column_name, column_def in ALERT_DELIVERY_COLUMN_DEFS.items():
        if column_name not in alert_delivery_columns and column_name != "id":
            connection.execute(
                f"ALTER TABLE alert_deliveries ADD COLUMN {column_name} {column_def}"
            )


def ensure_saved_turn_columns(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "saved_turns"):
        return
    saved_turn_columns = table_columns(connection, "saved_turns")
    for column_name, column_def in SAVED_TURN_COLUMN_DEFS.items():
        if column_name not in saved_turn_columns:
            connection.execute(
                f"ALTER TABLE saved_turns ADD COLUMN {column_name} {column_def}"
            )


def ensure_session_turn_columns(connection: sqlite3.Connection) -> None:
    if not table_exists(connection, "session_turns"):
        return
    session_turn_columns = table_columns(connection, "session_turns")
    for column_name, column_def in SESSION_TURN_COLUMN_DEFS.items():
        if column_name not in session_turn_columns:
            connection.execute(
                f"ALTER TABLE session_turns ADD COLUMN {column_name} {column_def}"
            )


def ensure_auth_state_row(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        "SELECT 1 FROM auth_state WHERE singleton = 1"
    ).fetchone()
    if row is None:
        connection.execute(
            """
            INSERT INTO auth_state (singleton, bootstrap_completed_at, created_at, updated_at)
            VALUES (1, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
        )


def init_db(database_path: Path) -> None:
    with connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        with write_transaction(connection):
            connection.execute(SESSION_TABLE_SQL)
            connection.execute(EVENT_TABLE_SQL)
            connection.executescript(OTHER_TABLES_SQL)
            ensure_session_columns(connection)
            ensure_remote_agent_columns(connection)
            ensure_user_columns(connection)
            backfill_user_access_columns(connection)
            ensure_project_columns(connection)
            ensure_project_source_columns(connection)
            ensure_project_acl_columns(connection)
            ensure_auth_state_columns(connection)
            ensure_onboarding_state_columns(connection)
            ensure_alert_incident_columns(connection)
            ensure_alert_delivery_columns(connection)
            ensure_saved_turn_columns(connection)
            ensure_session_turn_columns(connection)
            recover_legacy_sessions(connection)
            recover_legacy_events(connection)
            rebuilt_sessions = False
            if sessions_need_rebuild(connection):
                rebuild_sessions_table(connection)
                rebuilt_sessions = True
            if rebuilt_sessions or events_need_rebuild(connection):
                rebuild_events_table(connection)
            ensure_auth_state_row(connection)
            ensure_onboarding_state_row(connection)
            connection.executescript(INDEX_SQL)
            backfill_session_rollups(connection)
            backfill_session_turn_activity_daily(connection)
            backfill_session_turns(connection)
            from .projects import sync_project_registry

            sync_project_registry(connection)
        connection.execute("PRAGMA foreign_keys = ON")
