from __future__ import annotations

import sqlite3
from pathlib import Path


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
    "requested_raw_resend_token": "TEXT",
    "requested_raw_resend_at": "TEXT",
    "requested_raw_resend_note": "TEXT",
    "acknowledged_raw_resend_token": "TEXT",
    "last_raw_resend_at": "TEXT",
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
    requested_raw_resend_token TEXT,
    requested_raw_resend_at TEXT,
    requested_raw_resend_note TEXT,
    acknowledged_raw_resend_token TEXT,
    last_raw_resend_at TEXT
);
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_sessions_timestamp ON sessions(session_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_cwd ON sessions(cwd);
CREATE INDEX IF NOT EXISTS idx_sessions_model_provider ON sessions(model_provider);
CREATE INDEX IF NOT EXISTS idx_sessions_source_host ON sessions(source_host);
CREATE INDEX IF NOT EXISTS idx_sessions_github_slug ON sessions(github_slug);
CREATE INDEX IF NOT EXISTS idx_sessions_project_key ON sessions(inferred_project_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_host_path_unique
ON sessions(source_host, source_path);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_session_index
ON events(session_id, event_index);
CREATE INDEX IF NOT EXISTS idx_events_session_kind
ON events(session_id, kind);

CREATE INDEX IF NOT EXISTS idx_remote_agents_last_seen
ON remote_agents(last_seen_at DESC);
"""


def connect(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


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
    }:
        return "0"
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


def init_db(database_path: Path) -> None:
    with connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute(SESSION_TABLE_SQL)
        connection.execute(EVENT_TABLE_SQL)
        connection.executescript(OTHER_TABLES_SQL)
        ensure_session_columns(connection)
        ensure_remote_agent_columns(connection)
        recover_legacy_sessions(connection)
        recover_legacy_events(connection)
        rebuilt_sessions = False
        if sessions_need_rebuild(connection):
            rebuild_sessions_table(connection)
            rebuilt_sessions = True
        if rebuilt_sessions or events_need_rebuild(connection):
            rebuild_events_table(connection)
        connection.executescript(INDEX_SQL)
        connection.execute("PRAGMA foreign_keys = ON")
