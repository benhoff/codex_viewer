from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from .config import Settings
from .db import connect, write_transaction
from .projects import project_is_ignored
from .session_artifacts import load_session_artifact_text, read_session_source_text, store_session_artifact
from .session_parsing import (
    NormalizedEvent,
    ParsedSession,
    SessionParseError,
    SessionPreScan,
    SessionSkipError,
    first_text_from_content,
    format_command,
    friendly_tool_title,
    iter_session_files,
    normalize_event,
    normalize_jsonl_line,
    normalized_event_to_dict,
    parse_jsonish,
    parse_session_file,
    parse_session_text,
    parsed_session_from_payload,
    parsed_session_to_payload,
    prescan_session_source,
    safe_json,
    summarize_token_count,
    summarize_tool_call_input,
    summarize_web_search_action,
)
from .session_rollups import replace_session_turn_activity_daily
from .turn_index import replace_session_turn_search, replace_session_turns

logger = logging.getLogger("agent_operations_viewer.importer")

def upsert_parsed_session(connection: sqlite3.Connection, parsed: ParsedSession) -> None:
    existing_by_path = connection.execute(
        """
        SELECT id
        FROM sessions
        WHERE source_host = ? AND source_path = ?
        """,
        (parsed.source_host, str(parsed.source_path)),
    ).fetchone()
    if existing_by_path is not None and str(existing_by_path["id"] or "") != parsed.session_id:
        connection.execute(
            "DELETE FROM events WHERE session_id = ?",
            (str(existing_by_path["id"]),),
        )
        connection.execute(
            "DELETE FROM sessions WHERE id = ?",
            (str(existing_by_path["id"]),),
        )

    connection.execute("DELETE FROM events WHERE session_id = ?", (parsed.session_id,))
    existing_by_id = connection.execute(
        "SELECT raw_artifact_sha256 FROM sessions WHERE id = ?",
        (parsed.session_id,),
    ).fetchone()
    raw_artifact_sha256 = parsed.raw_artifact_sha256
    if raw_artifact_sha256 is None and existing_by_id is not None:
        raw_artifact_sha256 = str(existing_by_id["raw_artifact_sha256"] or "").strip() or None
    session_columns = (
        "id",
        "source_path",
        "source_root",
        "file_size",
        "file_mtime_ns",
        "content_sha256",
        "raw_artifact_sha256",
        "session_timestamp",
        "started_at",
        "ended_at",
        "cwd",
        "cwd_name",
        "source_host",
        "originator",
        "cli_version",
        "source",
        "model_provider",
        "git_branch",
        "git_commit_hash",
        "git_repository_url",
        "github_remote_url",
        "github_org",
        "github_repo",
        "github_slug",
        "forked_from_id",
        "agent_nickname",
        "agent_role",
        "agent_path",
        "memory_mode",
        "inferred_project_kind",
        "inferred_project_key",
        "inferred_project_label",
        "summary",
        "event_count",
        "user_message_count",
        "assistant_message_count",
        "tool_call_count",
        "rollup_version",
        "turn_count",
        "last_user_message",
        "last_turn_timestamp",
        "latest_turn_summary",
        "command_failure_count",
        "aborted_turn_count",
        "latest_usage_timestamp",
        "latest_input_tokens",
        "latest_cached_input_tokens",
        "latest_output_tokens",
        "latest_reasoning_output_tokens",
        "latest_total_tokens",
        "latest_context_window",
        "latest_context_remaining_percent",
        "latest_primary_limit_used_percent",
        "latest_primary_limit_resets_at",
        "latest_secondary_limit_used_percent",
        "latest_secondary_limit_resets_at",
        "latest_rate_limit_name",
        "latest_rate_limit_reached_type",
        "import_warning",
        "search_text",
        "raw_meta_json",
        "imported_at",
        "updated_at",
    )
    session_values = (
        parsed.session_id,
        str(parsed.source_path),
        str(parsed.source_root),
        parsed.file_size,
        parsed.file_mtime_ns,
        parsed.content_sha256,
        raw_artifact_sha256,
        parsed.session_timestamp,
        parsed.started_at,
        parsed.ended_at,
        parsed.cwd,
        parsed.cwd_name,
        parsed.source_host,
        parsed.originator,
        parsed.cli_version,
        parsed.source,
        parsed.model_provider,
        parsed.git_branch,
        parsed.git_commit_hash,
        parsed.git_repository_url,
        parsed.github_remote_url,
        parsed.github_org,
        parsed.github_repo,
        parsed.github_slug,
        parsed.forked_from_id,
        parsed.agent_nickname,
        parsed.agent_role,
        parsed.agent_path,
        parsed.memory_mode,
        parsed.inferred_project_kind,
        parsed.inferred_project_key,
        parsed.inferred_project_label,
        parsed.summary,
        parsed.event_count,
        parsed.user_message_count,
        parsed.assistant_message_count,
        parsed.tool_call_count,
        parsed.rollup_version,
        parsed.turn_count,
        parsed.last_user_message,
        parsed.last_turn_timestamp,
        parsed.latest_turn_summary,
        parsed.command_failure_count,
        parsed.aborted_turn_count,
        parsed.latest_usage_timestamp,
        parsed.latest_input_tokens,
        parsed.latest_cached_input_tokens,
        parsed.latest_output_tokens,
        parsed.latest_reasoning_output_tokens,
        parsed.latest_total_tokens,
        parsed.latest_context_window,
        parsed.latest_context_remaining_percent,
        parsed.latest_primary_limit_used_percent,
        parsed.latest_primary_limit_resets_at,
        parsed.latest_secondary_limit_used_percent,
        parsed.latest_secondary_limit_resets_at,
        parsed.latest_rate_limit_name,
        parsed.latest_rate_limit_reached_type,
        parsed.import_warning,
        parsed.search_text,
        parsed.raw_meta_json,
        parsed.imported_at,
        parsed.updated_at,
    )
    if existing_by_id is None:
        insert_columns_sql = ", ".join(session_columns)
        insert_placeholders_sql = ", ".join("?" for _ in session_columns)
        connection.execute(
            f"INSERT INTO sessions ({insert_columns_sql}) VALUES ({insert_placeholders_sql})",
            session_values,
        )
    else:
        update_columns = session_columns[1:]
        update_assignments_sql = ", ".join(f"{column} = ?" for column in update_columns)
        connection.execute(
            f"UPDATE sessions SET {update_assignments_sql} WHERE id = ?",
            session_values[1:] + (parsed.session_id,),
        )
    connection.executemany(
        """
        INSERT INTO events (
            session_id, event_index, timestamp, record_type, payload_type,
            kind, role, title, display_text, detail_text, tool_name,
            call_id, command_text, exit_code, record_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                parsed.session_id,
                event.event_index,
                event.timestamp,
                event.record_type,
                event.payload_type,
                event.kind,
                event.role,
                event.title,
                event.display_text,
                event.detail_text,
                event.tool_name,
                event.call_id,
                event.command_text,
                event.exit_code,
                event.record_json,
            )
            for event in parsed.events
        ],
    )
    replace_session_turn_activity_daily(connection, parsed.session_id, parsed.events)
    replace_session_turns(connection, parsed.session_id, parsed.events)
    replace_session_turn_search(connection, parsed.session_id, parsed.events)
    from .action_queue import replace_session_action_queue_rollups
    from .environment_audit import replace_session_environment_rollups

    replace_session_action_queue_rollups(connection, parsed.session_id, parsed.events)
    replace_session_environment_rollups(connection, parsed.session_id, parsed.events)


def fetch_host_sync_manifest(connection: sqlite3.Connection, source_host: str) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT
            s.id,
            s.source_host,
            s.source_path,
            s.source_root,
            s.file_size,
            s.file_mtime_ns,
            s.content_sha256,
            MAX(CASE WHEN s.raw_artifact_sha256 IS NOT NULL AND sa.sha256 IS NOT NULL THEN 1 ELSE 0 END) AS has_raw_artifact,
            s.event_count,
            COUNT(e.id) AS stored_event_count,
            s.updated_at
        FROM sessions AS s
        LEFT JOIN events AS e
            ON e.session_id = s.id
        LEFT JOIN session_artifacts AS sa
            ON sa.sha256 = s.raw_artifact_sha256
        WHERE s.source_host = ?
        GROUP BY
            s.id,
            s.source_host,
            s.source_path,
            s.source_root,
            s.file_size,
            s.file_mtime_ns,
            s.content_sha256,
            s.event_count,
            s.updated_at
        ORDER BY s.source_path ASC
        """,
        (source_host,),
    ).fetchall()
    return [dict(row) for row in rows]


def sync_sessions(settings: Settings, force: bool = False) -> dict[str, int]:
    imported = 0
    updated = 0
    skipped = 0
    project_registry_changed = False

    with connect(settings.database_path) as connection:
        with write_transaction(connection):
            from .projects import sync_project_registry

            existing_rows = connection.execute(
                """
                SELECT
                    id,
                    source_path,
                    source_root,
                    file_size,
                    file_mtime_ns,
                    content_sha256,
                    source_host,
                    raw_artifact_sha256
                FROM sessions
                """
            ).fetchall()
            existing_by_source = {
                (row["source_host"], row["source_path"]): {
                    "id": row["id"],
                    "source_root": row["source_root"],
                    "file_size": row["file_size"],
                    "file_mtime_ns": row["file_mtime_ns"],
                    "content_sha256": row["content_sha256"],
                    "source_host": row["source_host"],
                    "raw_artifact_sha256": row["raw_artifact_sha256"],
                }
                for row in existing_rows
            }
            if force:
                connection.execute("DELETE FROM events")
                connection.execute("DELETE FROM sessions")
                connection.execute("DELETE FROM environment_command_observations")
                connection.execute("DELETE FROM environment_host_capabilities")
                project_registry_changed = True

            restored_source_keys: set[tuple[str, str]] = set()

            for source_root, path in iter_session_files(settings.session_roots):
                stat = path.stat()
                record = existing_by_source.get((settings.source_host, str(path)))
                if (
                    not force
                    and record
                    and record["file_size"] == stat.st_size
                    and record["file_mtime_ns"] == stat.st_mtime_ns
                    and record["source_host"] == settings.source_host
                    and str(record["raw_artifact_sha256"] or "").strip()
                ):
                    skipped += 1
                    continue

                try:
                    raw_jsonl = read_session_source_text(path)
                    parsed = parse_session_text(
                        raw_jsonl,
                        path,
                        source_root,
                        settings.source_host,
                        file_size=stat.st_size,
                        file_mtime_ns=stat.st_mtime_ns,
                    )
                except SessionSkipError as exc:
                    logger.info("Skipping session file %s", exc)
                    skipped += 1
                    continue
                except SessionParseError as exc:
                    logger.warning("Skipping malformed session file %s", exc)
                    skipped += 1
                    continue
                if project_is_ignored(connection, parsed.inferred_project_key):
                    skipped += 1
                    continue
                parsed.raw_artifact_sha256 = store_session_artifact(connection, settings, raw_jsonl)
                upsert_parsed_session(connection, parsed)
                restored_source_keys.add((parsed.source_host, str(parsed.source_path)))
                project_registry_changed = True

                if record:
                    updated += 1
                else:
                    imported += 1

            if force:
                for row in existing_rows:
                    source_host = str(row["source_host"] or "").strip()
                    source_path = str(row["source_path"] or "").strip()
                    if not source_host or not source_path:
                        continue
                    source_key = (source_host, source_path)
                    if source_key in restored_source_keys:
                        continue
                    artifact_sha256 = str(row["raw_artifact_sha256"] or "").strip()
                    if not artifact_sha256:
                        continue

                    raw_jsonl = load_session_artifact_text(connection, settings, artifact_sha256)
                    if raw_jsonl is None:
                        skipped += 1
                        continue

                    try:
                        parsed = parse_session_text(
                            raw_jsonl,
                            Path(source_path),
                            Path(str(row["source_root"] or "").strip() or Path(source_path).parent),
                            source_host,
                            file_size=int(row["file_size"] or len(raw_jsonl.encode("utf-8"))),
                            file_mtime_ns=int(row["file_mtime_ns"] or 0),
                        )
                    except SessionSkipError as exc:
                        logger.info("Skipping stored session artifact %s", exc)
                        skipped += 1
                        continue
                    except SessionParseError as exc:
                        logger.warning("Skipping malformed stored session artifact %s", exc)
                        skipped += 1
                        continue

                    if project_is_ignored(connection, parsed.inferred_project_key):
                        skipped += 1
                        continue

                    parsed.raw_artifact_sha256 = artifact_sha256
                    upsert_parsed_session(connection, parsed)
                    restored_source_keys.add(source_key)
                    project_registry_changed = True
                    updated += 1

            if project_registry_changed:
                sync_project_registry(connection)

    return {"imported": imported, "updated": updated, "skipped": skipped}
