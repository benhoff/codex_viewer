from __future__ import annotations

from dataclasses import replace
import json
import logging
import random
from pathlib import Path
import signal
import threading
import time

from agent_operations_viewer.config import Settings

from .file_watch import SessionFileWatcher
from .remote_sync import RemoteSyncBusyError, RemoteSyncError, RestartRequired, sync_sessions_remote


def _busy_retry_delay(attempt: int, retry_after: float, interval: float) -> float:
    delay = max(retry_after, min(300, max(5, interval) * 2 ** min(attempt - 1, 6)))
    return delay + random.uniform(0, min(5, delay * 0.2))


def run_sync_daemon(settings: Settings, interval_seconds: int, rebuild_on_start: bool = False) -> int:
    logger = logging.getLogger("agent_operations_viewer.daemon")
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
    busy_attempts = 0
    cooldown_deadline = 0.0
    rescan_pending = False
    next_sync_deadline = time.monotonic()
    try:
        while not stop_event.is_set():
            if cooldown_deadline:
                if stop_event.wait(max(0, cooldown_deadline - time.monotonic())):
                    break
                cooldown_deadline = 0.0
                rescan_pending = True
            force = rebuild_on_start and first_run
            candidate_paths: list[Path] | None = None

            if not first_run and watcher is not None and not force and not rescan_pending:
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

            rescan_pending = False
            try:
                stats = sync_sessions_remote(
                    daemon_settings,
                    force=force,
                    candidate_paths=candidate_paths,
                )
            except RestartRequired as exc:
                logger.info("Agent update completed, restarting daemon: %s", exc)
                return 75
            except RemoteSyncBusyError as exc:
                first_run = False
                busy_attempts += 1
                delay = _busy_retry_delay(busy_attempts, exc.retry_after_seconds, interval_seconds)
                logger.warning("Viewer busy, pausing uploads for %.1fs: %s", delay, exc)
                cooldown_deadline = next_sync_deadline = time.monotonic() + delay
                continue
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
            retry_after = stats.get("retry_after_seconds")
            if retry_after is not None:
                busy_attempts += 1
                delay = _busy_retry_delay(busy_attempts, float(retry_after), interval_seconds)
                cooldown_deadline = next_sync_deadline = time.monotonic() + delay
                logger.warning("Viewer busy, pausing uploads for %.1fs", delay)
            else:
                busy_attempts = 0
                next_sync_deadline = time.monotonic() + interval_seconds
    finally:
        if watcher is not None:
            watcher.close()

    logger.info("Agent daemon stopped")
    return 0
