from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import sqlite3

from .config import Settings
from .projects import sync_project_registry
from .session_artifacts import ARTIFACT_ROOT, absolute_artifact_path


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


@dataclass(slots=True)
class ResetSetupResult:
    users_removed: int = 0
    tokens_removed: int = 0
    remote_agents_removed: int = 0
    alert_incidents_removed: int = 0
    alert_deliveries_removed: int = 0
    sessions_removed: int = 0
    session_artifacts_removed: int = 0
    artifact_paths: tuple[Path, ...] = ()


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"] or 0) if row is not None else 0


def _reset_onboarding_state(connection: sqlite3.Connection, *, now: str) -> None:
    connection.execute(
        """
        INSERT INTO onboarding_state (
            singleton,
            completed_at,
            first_heartbeat_at,
            first_heartbeat_source_host,
            first_session_ingested_at,
            first_session_source_host,
            last_failure_reason,
            created_at,
            updated_at
        ) VALUES (1, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)
        ON CONFLICT(singleton) DO UPDATE SET
            completed_at = NULL,
            first_heartbeat_at = NULL,
            first_heartbeat_source_host = NULL,
            first_session_ingested_at = NULL,
            first_session_source_host = NULL,
            last_failure_reason = NULL,
            updated_at = excluded.updated_at
        """,
        (now, now),
    )


def _reset_auth_state(connection: sqlite3.Connection, *, now: str) -> None:
    connection.execute(
        """
        INSERT INTO auth_state (singleton, bootstrap_completed_at, created_at, updated_at)
        VALUES (1, NULL, ?, ?)
        ON CONFLICT(singleton) DO UPDATE SET
            bootstrap_completed_at = NULL,
            updated_at = excluded.updated_at
        """,
        (now, now),
    )


def _orphaned_artifact_rows(
    connection: sqlite3.Connection,
) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT sha256, storage_path
        FROM session_artifacts
        WHERE NOT EXISTS (
            SELECT 1
            FROM sessions
            WHERE sessions.raw_artifact_sha256 = session_artifacts.sha256
        )
        """
    ).fetchall()


def reset_setup_state(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    reset_tokens: bool = True,
    reset_remote_agents: bool = True,
    reset_sessions: bool = True,
    full_bootstrap: bool = False,
) -> ResetSetupResult:
    result = ResetSetupResult()
    now = utc_now_iso()

    if full_bootstrap:
        result.users_removed = _count_rows(connection, "users")
        connection.execute("DELETE FROM users")
        _reset_auth_state(connection, now=now)

    if reset_tokens:
        result.tokens_removed = _count_rows(connection, "api_tokens")
        connection.execute("DELETE FROM api_tokens")

    if reset_remote_agents:
        result.alert_deliveries_removed = _count_rows(connection, "alert_deliveries")
        result.alert_incidents_removed = _count_rows(connection, "alert_incidents")
        result.remote_agents_removed = _count_rows(connection, "remote_agents")
        connection.execute("DELETE FROM alert_deliveries")
        connection.execute("DELETE FROM alert_incidents")
        connection.execute("DELETE FROM remote_agents")

    if reset_sessions:
        result.sessions_removed = _count_rows(connection, "sessions")
        connection.execute("DELETE FROM sessions")
        sync_project_registry(connection)

        orphan_rows = _orphaned_artifact_rows(connection)
        result.session_artifacts_removed = len(orphan_rows)
        result.artifact_paths = tuple(
            absolute_artifact_path(settings, str(row["storage_path"]))
            for row in orphan_rows
            if str(row["storage_path"] or "").strip()
        )
        if orphan_rows:
            connection.executemany(
                "DELETE FROM session_artifacts WHERE sha256 = ?",
                [(str(row["sha256"]),) for row in orphan_rows],
            )

    _reset_onboarding_state(connection, now=now)
    return result


def remove_artifact_files(paths: tuple[Path, ...]) -> int:
    removed = 0
    for path in paths:
        existed = path.exists()
        try:
            path.unlink(missing_ok=True)
        except OSError:
            continue
        if existed and not path.exists():
            removed += 1
    return removed


def prune_empty_artifact_dirs(settings: Settings) -> None:
    artifact_root = settings.data_dir / ARTIFACT_ROOT
    if not artifact_root.exists():
        return
    for directory in sorted(
        (path for path in artifact_root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    ):
        try:
            directory.rmdir()
        except OSError:
            continue
