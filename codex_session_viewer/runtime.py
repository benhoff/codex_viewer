from __future__ import annotations

from dataclasses import replace
import json
import logging
import signal
import sqlite3
import threading

from .config import Settings
from .remote_sync import RemoteSyncError, RestartRequired, sync_sessions_remote


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
    daemon_settings = replace(settings, sync_mode="remote")

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

    first_run = True
    while not stop_event.is_set():
        force = rebuild_on_start and first_run
        try:
            stats = sync_sessions_remote(daemon_settings, force=force)
        except RestartRequired as exc:
            logger.info("Agent update completed, restarting daemon: %s", exc)
            return 75
        except RemoteSyncError as exc:
            first_run = False
            logger.warning("Remote sync unavailable, will retry in %ss: %s", interval_seconds, exc)
            if stop_event.wait(interval_seconds):
                break
            continue
        except Exception:
            first_run = False
            logger.exception("Daemon sync pass crashed unexpectedly, retrying in %ss", interval_seconds)
            if stop_event.wait(interval_seconds):
                break
            continue
        logger.info("Sync pass finished: %s", json.dumps(stats, sort_keys=True))
        first_run = False
        if stop_event.wait(interval_seconds):
            break

    logger.info("Agent daemon stopped")
    return 0
