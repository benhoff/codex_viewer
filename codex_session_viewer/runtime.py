from __future__ import annotations

import json
import logging
import signal
import sqlite3
import threading

from .config import Settings
from .db import init_db
from .importer import sync_sessions
from .remote_sync import RestartRequired, sync_sessions_remote


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
    lines = [
        f"# {session['summary']}",
        "",
        f"- Session ID: `{session['id']}`",
        f"- Timestamp: `{session['session_timestamp'] or session['started_at'] or 'unknown'}`",
        f"- CWD: `{session['cwd'] or 'unknown'}`",
        f"- Host: `{session['source_host'] or 'unknown'}`",
        f"- Model Provider: `{session['model_provider'] or 'unknown'}`",
        "",
        "## Timeline",
        "",
    ]

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

    def _request_shutdown(signum: int, _frame: object) -> None:
        logger.info("Received signal %s, shutting down agent daemon", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    if settings.sync_mode == "remote":
        logger.info(
            "Starting agent daemon with interval=%ss roots=%s target=%s mode=remote",
            interval_seconds,
            ",".join(str(path) for path in settings.session_roots),
            settings.server_base_url or "unconfigured",
        )
    else:
        init_db(settings.database_path)
        logger.info(
            "Starting agent daemon with interval=%ss roots=%s db=%s mode=local",
            interval_seconds,
            ",".join(str(path) for path in settings.session_roots),
            settings.database_path,
        )

    first_run = True
    while not stop_event.is_set():
        force = rebuild_on_start and first_run
        try:
            if settings.sync_mode == "remote":
                stats = sync_sessions_remote(settings, force=force)
            else:
                stats = sync_sessions(settings, force=force)
        except RestartRequired as exc:
            logger.info("Agent update completed, restarting daemon: %s", exc)
            return 75
        logger.info("Sync pass finished: %s", json.dumps(stats, sort_keys=True))
        first_run = False
        if stop_event.wait(interval_seconds):
            break

    logger.info("Agent daemon stopped")
    return 0
