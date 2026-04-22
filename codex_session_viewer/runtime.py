from __future__ import annotations

from dataclasses import replace
import json
import logging
from pathlib import Path
import signal
import sqlite3
import threading
import time

from .config import Settings
from .file_watch import SessionFileWatcher
from .remote_sync import RemoteSyncError, RestartRequired, sync_sessions_remote
from .session_exports import build_execution_context_export


def get_events(connection: sqlite3.Connection, session_id: str) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM events
        WHERE session_id = ?
        ORDER BY event_index ASC
        """,
        (session_id,),
    ).fetchall()


def export_markdown(session: sqlite3.Row, events: list[sqlite3.Row]) -> str:
    execution_context = build_execution_context_export(session, events)
    lines = [
        f"# {session['summary']}",
        "",
        f"- Session ID: `{session['id']}`",
        f"- Timestamp: `{session['session_timestamp'] or session['started_at'] or 'unknown'}`",
        f"- CWD: `{session['cwd'] or 'unknown'}`",
        f"- Host: `{session['source_host'] or 'unknown'}`",
        f"- Model Provider: `{session['model_provider'] or 'unknown'}`",
        "",
    ]

    if any(value for value in execution_context.values()):
        lines.extend(
            [
                "## Execution Context",
                "",
                "~~~json",
                json.dumps(execution_context, indent=2, ensure_ascii=False, sort_keys=True),
                "~~~",
                "",
            ]
        )

    lines.extend(
        [
        "## Timeline",
        "",
        ]
    )

    for event in events:
        lines.append(f"### {event['title']}")
        lines.append("")
        lines.append(f"- Kind: `{event['kind']}`")
        if event["role"]:
            lines.append(f"- Role: `{event['role']}`")
        if event["timestamp"]:
            lines.append(f"- Timestamp: `{event['timestamp']}`")
        if event["tool_name"]:
            lines.append(f"- Tool: `{event['tool_name']}`")
        if event["command_text"]:
            lines.append(f"- Command: `{event['command_text']}`")
        if event["call_id"]:
            lines.append(f"- Call ID: `{event['call_id']}`")
        lines.append("")
        lines.append("~~~text")
        lines.append(event["detail_text"] or event["display_text"] or "")
        lines.append("~~~")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def run_sync_daemon(settings: Settings, interval_seconds: int, rebuild_on_start: bool = False) -> int:
    logger = logging.getLogger("codex_session_viewer.daemon")
    stop_event = threading.Event()
    interval_seconds = max(1, interval_seconds)
    daemon_settings = replace(settings, sync_mode="remote")
    daemon_settings.ensure_directories()

    def _request_shutdown(signum: int, _frame: object) -> None:
        logger.info("Received signal %s, shutting down agent daemon", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    logger.info(
        "Starting agent daemon with interval=%ss roots=%s target=%s mode=remote",
        interval_seconds,
        ",".join(str(path) for path in daemon_settings.session_roots),
        daemon_settings.server_base_url or "unconfigured",
    )

    watcher: SessionFileWatcher | None = None
    if daemon_settings.remote_watch_mode != "off":
        watcher = SessionFileWatcher(
            daemon_settings.session_roots,
            mode=daemon_settings.remote_watch_mode,
            debounce_seconds=daemon_settings.remote_watch_debounce_seconds,
            poll_interval_seconds=daemon_settings.remote_watch_poll_seconds,
        )
        watcher.start()
        logger.info(
            "Agent file watcher enabled mode=%s backend=%s debounce=%.2fs poll=%.2fs",
            daemon_settings.remote_watch_mode,
            watcher.backend,
            daemon_settings.remote_watch_debounce_seconds,
            daemon_settings.remote_watch_poll_seconds,
        )

    first_run = True
    next_sync_deadline = time.monotonic()
    try:
        while not stop_event.is_set():
            force = rebuild_on_start and first_run
            candidate_paths: list[Path] | None = None

            if not first_run and watcher is not None and not force:
                timeout_seconds = max(0.0, next_sync_deadline - time.monotonic())
                candidate_paths = watcher.wait_for_changes(
                    stop_event,
                    timeout_seconds=timeout_seconds,
                )
                if stop_event.is_set():
                    break
            elif not first_run and watcher is None:
                if stop_event.wait(max(0.0, next_sync_deadline - time.monotonic())):
                    break

            try:
                stats = sync_sessions_remote(
                    daemon_settings,
                    force=force,
                    candidate_paths=candidate_paths,
                )
            except RestartRequired as exc:
                logger.info("Agent update completed, restarting daemon: %s", exc)
                return 75
            except RemoteSyncError as exc:
                first_run = False
                logger.warning("Remote sync unavailable, will retry in %ss: %s", interval_seconds, exc)
                next_sync_deadline = time.monotonic() + interval_seconds
                continue
            except Exception:
                first_run = False
                logger.exception("Daemon sync pass crashed unexpectedly, retrying in %ss", interval_seconds)
                next_sync_deadline = time.monotonic() + interval_seconds
                continue

            logger.info("Sync pass finished: %s", json.dumps(stats, sort_keys=True))
            first_run = False
            next_sync_deadline = time.monotonic() + interval_seconds
    finally:
        if watcher is not None:
            watcher.close()

    logger.info("Agent daemon stopped")
    return 0
