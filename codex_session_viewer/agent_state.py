from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from pathlib import Path


def connect_agent_state(database_path: Path) -> sqlite3.Connection:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_session_files (
            source_path TEXT PRIMARY KEY,
            source_root TEXT NOT NULL,
            file_size INTEGER NOT NULL DEFAULT 0,
            file_mtime_ns INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL DEFAULT 'seen',
            session_format TEXT,
            session_id TEXT,
            inferred_project_key TEXT,
            inferred_project_label TEXT,
            invalid_reason TEXT,
            last_seen_at TEXT NOT NULL,
            last_uploaded_at TEXT,
            deleted_at TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_agent_session_files_root_deleted
        ON agent_session_files(source_root, deleted_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_agent_session_files_project_state
        ON agent_session_files(inferred_project_key, state, deleted_at)
        """
    )
    return connection


def fetch_agent_file_states(
    connection: sqlite3.Connection,
    *,
    roots: Iterable[Path] | None = None,
) -> dict[str, sqlite3.Row]:
    parameters: list[object] = []
    query = """
        SELECT *
        FROM agent_session_files
        WHERE deleted_at IS NULL
    """
    root_values = [str(root.expanduser()) for root in roots or []]
    if root_values:
        query += f" AND source_root IN ({','.join('?' for _ in root_values)})"
        parameters.extend(root_values)
    rows = connection.execute(query, parameters).fetchall()
    return {str(row["source_path"]): row for row in rows}


def upsert_agent_file_state(
    connection: sqlite3.Connection,
    *,
    source_root: Path,
    source_path: Path,
    file_size: int,
    file_mtime_ns: int,
    last_seen_at: str,
    state: str = "seen",
    session_format: str | None = None,
    session_id: str | None = None,
    inferred_project_key: str | None = None,
    inferred_project_label: str | None = None,
    invalid_reason: str | None = None,
    last_uploaded_at: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO agent_session_files (
            source_path,
            source_root,
            file_size,
            file_mtime_ns,
            state,
            session_format,
            session_id,
            inferred_project_key,
            inferred_project_label,
            invalid_reason,
            last_seen_at,
            last_uploaded_at,
            deleted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT(source_path) DO UPDATE SET
            source_root = excluded.source_root,
            file_size = excluded.file_size,
            file_mtime_ns = excluded.file_mtime_ns,
            state = excluded.state,
            session_format = COALESCE(excluded.session_format, agent_session_files.session_format),
            session_id = COALESCE(excluded.session_id, agent_session_files.session_id),
            inferred_project_key = COALESCE(excluded.inferred_project_key, agent_session_files.inferred_project_key),
            inferred_project_label = COALESCE(excluded.inferred_project_label, agent_session_files.inferred_project_label),
            invalid_reason = excluded.invalid_reason,
            last_seen_at = excluded.last_seen_at,
            last_uploaded_at = COALESCE(excluded.last_uploaded_at, agent_session_files.last_uploaded_at),
            deleted_at = NULL
        """,
        (
            str(source_path),
            str(source_root.expanduser()),
            file_size,
            file_mtime_ns,
            state,
            session_format,
            session_id,
            inferred_project_key,
            inferred_project_label,
            invalid_reason,
            last_seen_at,
            last_uploaded_at,
        ),
    )


def mark_agent_file_uploaded(
    connection: sqlite3.Connection,
    *,
    source_path: Path,
    uploaded_at: str,
) -> None:
    connection.execute(
        """
        UPDATE agent_session_files
        SET
            state = CASE
                WHEN state = 'invalid' THEN state
                ELSE 'uploaded'
            END,
            last_uploaded_at = ?,
            deleted_at = NULL
        WHERE source_path = ?
        """,
        (uploaded_at, str(source_path)),
    )


def mark_agent_file_deleted(
    connection: sqlite3.Connection,
    *,
    source_path: Path,
    deleted_at: str,
) -> None:
    connection.execute(
        """
        UPDATE agent_session_files
        SET
            state = 'deleted',
            deleted_at = ?
        WHERE source_path = ?
        """,
        (deleted_at, str(source_path)),
    )


def mark_missing_agent_files_deleted(
    connection: sqlite3.Connection,
    *,
    roots: Iterable[Path],
    seen_paths: set[str],
    deleted_at: str,
) -> list[str]:
    root_values = [str(root.expanduser()) for root in roots]
    if not root_values:
        return []

    query = f"""
        SELECT source_path
        FROM agent_session_files
        WHERE deleted_at IS NULL
          AND source_root IN ({','.join('?' for _ in root_values)})
    """
    rows = connection.execute(query, root_values).fetchall()
    deleted_paths = [str(row["source_path"]) for row in rows if str(row["source_path"]) not in seen_paths]
    if not deleted_paths:
        return []

    connection.executemany(
        """
        UPDATE agent_session_files
        SET
            state = 'deleted',
            deleted_at = ?
        WHERE source_path = ?
        """,
        [(deleted_at, path) for path in deleted_paths],
    )
    return deleted_paths
