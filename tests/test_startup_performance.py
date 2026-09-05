from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from fastapi import Request
from fastapi.responses import JSONResponse

from agent_operations_viewer.api_tokens import create_api_token
from agent_operations_viewer.db import (
    SESSION_BACKFILL_BATCH_SIZE,
    WRITE_LOCK,
    backfill_session_agent_metadata,
    connect,
    init_db,
    run_db_backfills,
    try_write_transaction,
    write_transaction,
)
from agent_operations_viewer.config import Settings
from agent_operations_viewer.importer import fetch_host_sync_manifest
from agent_operations_viewer.session_insights import AGENT_METADATA_VERSION
from agent_operations_viewer.turn_index import (
    TURN_INDEX_VERSION,
    TURN_SEARCH_VERSION,
    backfill_session_turn_search,
    backfill_session_turns,
)
from agent_operations_viewer.web.app import _start_post_startup_maintenance, create_app
from agent_operations_viewer.web.auth import AuthMiddleware, _require_sync_api_auth
from agent_operations_viewer.web.concurrency import (
    WorkQueueFull,
    _BoundedWorkExecutor,
    run_in_history_threadpool,
)


def insert_legacy_session(
    connection: object,
    *,
    session_id: str,
    inferred_project_key: str,
    raw_meta_json: str = "{}",
) -> None:
    connection.execute(
        """
        INSERT INTO sessions (
            id,
            source_path,
            source_root,
            file_size,
            file_mtime_ns,
            inferred_project_key,
            inferred_project_label,
            summary,
            raw_meta_json,
            imported_at,
            updated_at
        ) VALUES (?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            f"/tmp/{session_id}.jsonl",
            "/tmp",
            inferred_project_key,
            inferred_project_key,
            session_id,
            raw_meta_json,
            "2026-08-16T00:00:00Z",
            "2026-08-16T00:00:00Z",
        ),
    )


class StartupPerformanceTests(unittest.TestCase):
    def test_turn_backfills_honor_batch_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(database_path)
            with connect(database_path) as connection:
                with write_transaction(connection):
                    for index in range(5):
                        insert_legacy_session(
                            connection,
                            session_id=f"batched-session-{index}",
                            inferred_project_key=f"directory:test:/workspace/{index}",
                        )

                with write_transaction(connection):
                    turn_count = backfill_session_turns(connection, batch_size=2)
                current_turns = connection.execute(
                    "SELECT COUNT(*) FROM sessions WHERE turn_index_version = ?",
                    (TURN_INDEX_VERSION,),
                ).fetchone()[0]

                with write_transaction(connection):
                    search_count = backfill_session_turn_search(connection, batch_size=2)
                current_search = connection.execute(
                    "SELECT COUNT(*) FROM sessions WHERE turn_search_version = ?",
                    (TURN_SEARCH_VERSION,),
                ).fetchone()[0]

            self.assertEqual(turn_count, 2)
            self.assertEqual(current_turns, 2)
            self.assertEqual(search_count, 2)
            self.assertEqual(current_search, 2)

    def test_run_db_backfills_processes_every_bounded_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(database_path)
            session_count = SESSION_BACKFILL_BATCH_SIZE + 3
            with connect(database_path) as connection:
                with write_transaction(connection):
                    for index in range(session_count):
                        insert_legacy_session(
                            connection,
                            session_id=f"complete-session-{index:03d}",
                            inferred_project_key=f"directory:test:/complete/{index}",
                        )

            run_db_backfills(database_path)

            with connect(database_path) as connection:
                current = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM sessions
                    WHERE turn_index_version = ? AND turn_search_version = ?
                    """,
                    (TURN_INDEX_VERSION, TURN_SEARCH_VERSION),
                ).fetchone()[0]
            self.assertEqual(current, session_count)

    def test_sync_token_auth_skips_usage_write_while_writer_is_busy(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(database_path)
            with connect(database_path) as connection:
                with write_transaction(connection):
                    token = create_api_token(connection, "Busy-writer token")

            writer_started = threading.Event()
            release_writer = threading.Event()

            def hold_writer_slot() -> None:
                with WRITE_LOCK:
                    writer_started.set()
                    release_writer.wait(timeout=2)

            holder = threading.Thread(target=hold_writer_slot)
            holder.start()
            self.assertTrue(writer_started.wait(timeout=1))
            try:
                started_at = time.monotonic()
                result = _require_sync_api_auth(
                    SimpleNamespace(database_path=database_path),
                    bearer_token=token["token"],
                    source_host="busy-host",
                    raw_body=b"",
                    method="GET",
                    path="/api/sync/manifest-v2",
                    machine_id="",
                    machine_timestamp="",
                    machine_nonce="",
                    machine_signature="",
                    machine_body_sha256="",
                )
                elapsed = time.monotonic() - started_at
            finally:
                release_writer.set()
                holder.join(timeout=1)

            self.assertEqual(result["auth_type"], "api_token")
            self.assertLess(elapsed, 0.1)

    def test_incidental_write_skips_immediately_while_upload_writer_is_busy(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(database_path)
            writer_started = threading.Event()
            release_writer = threading.Event()

            def hold_writer_slot() -> None:
                with WRITE_LOCK:
                    writer_started.set()
                    release_writer.wait(timeout=2)

            holder = threading.Thread(target=hold_writer_slot)
            holder.start()
            self.assertTrue(writer_started.wait(timeout=1))
            try:
                with connect(database_path) as connection:
                    started_at = time.monotonic()
                    with try_write_transaction(connection) as writable:
                        self.assertFalse(writable)
                    elapsed = time.monotonic() - started_at
                    self.assertLess(elapsed, 0.1)
            finally:
                release_writer.set()
                holder.join(timeout=1)

    def test_health_check_bypasses_database_backed_auth_resolution(self) -> None:
        settings = SimpleNamespace(
            auth_enabled=lambda: True,
            auth_mode="password",
        )
        middleware = AuthMiddleware(mock.Mock(), settings=settings)
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/api/health",
                "headers": [],
                "query_string": b"",
                "scheme": "http",
                "server": ("testserver", 80),
                "client": ("testclient", 123),
            }
        )

        async def call_next(_request: Request) -> JSONResponse:
            return JSONResponse({"status": "ok"})

        with mock.patch(
            "agent_operations_viewer.web.auth.run_in_threadpool",
            side_effect=AssertionError("health check touched the database"),
        ):
            response = asyncio.run(middleware.dispatch(request, call_next))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.body), {"status": "ok"})

    def test_queue_saturation_is_returned_as_retryable_503(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            environment = {
                "CODEX_VIEWER_AUTH_MODE": "none",
                "CODEX_VIEWER_DATA_DIR": str(project_root / "data"),
                "CODEX_VIEWER_SYNC_MODE": "remote",
            }
            with mock.patch.dict(os.environ, environment, clear=True):
                settings = Settings.from_env(project_root)
                app = create_app(settings)

            handler = app.exception_handlers[WorkQueueFull]
            response = asyncio.run(
                handler(mock.Mock(), WorkQueueFull("session-upload", duplicate=True))
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers["retry-after"], "5")
        self.assertEqual(json.loads(response.body)["retryable"], True)

    def test_bounded_executor_rejects_duplicate_and_excess_work(self) -> None:
        started = threading.Event()
        release = threading.Event()
        executor = _BoundedWorkExecutor(
            name="test-upload",
            worker_count=1,
            max_inflight=1,
            poll_interval_seconds=0.01,
        )

        def blocking_work() -> str:
            started.set()
            release.wait(timeout=2)
            return "finished"

        async def exercise() -> None:
            task = asyncio.create_task(
                executor.run(blocking_work, dedupe_key=("session", "one"))
            )
            while not started.is_set():
                await asyncio.sleep(0.001)

            with self.assertRaises(WorkQueueFull) as duplicate:
                await executor.run(blocking_work, dedupe_key=("session", "one"))
            self.assertTrue(duplicate.exception.duplicate)

            with self.assertRaises(WorkQueueFull) as saturated:
                await executor.run(blocking_work, dedupe_key=("session", "two"))
            self.assertFalse(saturated.exception.duplicate)
            self.assertEqual(executor.inflight_count(), 1)

            release.set()
            self.assertEqual(await asyncio.wait_for(task, timeout=1), "finished")
            self.assertEqual(executor.inflight_count(), 0)

        try:
            asyncio.run(exercise())
        finally:
            release.set()

    def test_single_worker_executor_serializes_accepted_uploads(self) -> None:
        first_started = threading.Event()
        first_release = threading.Event()
        order: list[str] = []
        executor = _BoundedWorkExecutor(
            name="test-serialized-upload",
            worker_count=1,
            max_inflight=2,
            poll_interval_seconds=0.01,
        )

        def work(label: str) -> str:
            order.append(f"start:{label}")
            if label == "first":
                first_started.set()
                first_release.wait(timeout=2)
            order.append(f"end:{label}")
            return label

        async def exercise() -> None:
            first = asyncio.create_task(
                executor.run(work, "first", dedupe_key=("session", "first"))
            )
            while not first_started.is_set():
                await asyncio.sleep(0.001)
            second = asyncio.create_task(
                executor.run(work, "second", dedupe_key=("session", "second"))
            )
            await asyncio.sleep(0.03)
            self.assertEqual(order, ["start:first"])
            self.assertEqual(executor.inflight_count(), 2)

            first_release.set()
            self.assertEqual(await asyncio.gather(first, second), ["first", "second"])
            self.assertEqual(
                order,
                ["start:first", "end:first", "start:second", "end:second"],
            )

        try:
            asyncio.run(exercise())
        finally:
            first_release.set()

    def test_history_worker_does_not_block_event_loop_progress(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def blocking_history_work() -> str:
            started.set()
            release.wait(timeout=2)
            return "finished"

        async def exercise() -> None:
            task = asyncio.create_task(run_in_history_threadpool(blocking_history_work))
            while not started.is_set():
                await asyncio.sleep(0.001)
            await asyncio.sleep(0.02)
            self.assertFalse(task.done())
            release.set()
            self.assertEqual(await asyncio.wait_for(task, timeout=1), "finished")

        try:
            asyncio.run(exercise())
        finally:
            release.set()

    def test_init_db_can_defer_derived_session_backfills(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(database_path)
            with connect(database_path) as connection:
                with write_transaction(connection):
                    insert_legacy_session(
                        connection,
                        session_id="deferred-session",
                        inferred_project_key="directory:test:/workspace/deferred",
                    )

            init_db(database_path, defer_backfills=True)
            with connect(database_path) as connection:
                version_before = connection.execute(
                    "SELECT agent_metadata_version FROM sessions WHERE id = ?",
                    ("deferred-session",),
                ).fetchone()[0]
                source_count_before = connection.execute(
                    "SELECT COUNT(*) FROM project_sources"
                ).fetchone()[0]

            run_db_backfills(database_path)
            with connect(database_path) as connection:
                version_after = connection.execute(
                    "SELECT agent_metadata_version FROM sessions WHERE id = ?",
                    ("deferred-session",),
                ).fetchone()[0]
                source_count_after = connection.execute(
                    "SELECT COUNT(*) FROM project_sources"
                ).fetchone()[0]

            self.assertEqual(version_before, 0)
            self.assertEqual(source_count_before, 0)
            self.assertEqual(version_after, AGENT_METADATA_VERSION)
            self.assertEqual(source_count_after, 1)

    def test_post_startup_backfills_run_off_the_event_loop_thread(self) -> None:
        started = threading.Event()
        release = threading.Event()
        worker_thread_ids: list[int] = []

        def blocking_backfill(_database_path: Path) -> None:
            worker_thread_ids.append(threading.get_ident())
            started.set()
            release.wait(timeout=2)

        settings = SimpleNamespace(
            database_path=Path("/tmp/deferred-viewer.sqlite3"),
            sync_on_start=False,
            sync_mode="remote",
        )
        try:
            with mock.patch(
                "agent_operations_viewer.web.app.run_db_backfills",
                new=blocking_backfill,
            ):
                worker = _start_post_startup_maintenance(settings)
                self.assertTrue(started.wait(timeout=1))
                self.assertEqual(len(worker_thread_ids), 1)
                self.assertNotEqual(worker_thread_ids[0], threading.get_ident())
                self.assertTrue(worker.daemon)
                release.set()
                worker.join(timeout=1)
                self.assertFalse(worker.is_alive())
        finally:
            release.set()

    def test_init_db_repairs_registry_once_then_skips_full_resync(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(database_path)
            with connect(database_path) as connection:
                with write_transaction(connection):
                    insert_legacy_session(
                        connection,
                        session_id="legacy-session",
                        inferred_project_key="directory:test:/workspace/repo",
                    )

            init_db(database_path)
            with connect(database_path) as connection:
                source_count = connection.execute(
                    "SELECT COUNT(*) FROM project_sources"
                ).fetchone()[0]
            self.assertEqual(source_count, 1)

            with mock.patch(
                "agent_operations_viewer.projects.sync_project_registry",
                side_effect=AssertionError("current registry should not be rebuilt"),
            ):
                init_db(database_path)

    def test_agent_metadata_backfill_marks_empty_legacy_metadata_current(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(database_path)
            with connect(database_path) as connection:
                with write_transaction(connection):
                    insert_legacy_session(
                        connection,
                        session_id="metadata-session",
                        inferred_project_key="directory:test:/workspace/metadata",
                    )
                    updated = backfill_session_agent_metadata(connection)
                version = connection.execute(
                    "SELECT agent_metadata_version FROM sessions WHERE id = ?",
                    ("metadata-session",),
                ).fetchone()[0]

                with mock.patch(
                    "agent_operations_viewer.db.parse_raw_meta_json",
                    side_effect=AssertionError("current metadata should not be reparsed"),
                ):
                    with write_transaction(connection):
                        repeated = backfill_session_agent_metadata(connection)

            self.assertEqual(updated, 1)
            self.assertEqual(version, AGENT_METADATA_VERSION)
            self.assertEqual(repeated, 0)

    def test_sync_manifest_does_not_recount_the_event_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(database_path)
            with connect(database_path) as connection:
                with write_transaction(connection):
                    insert_legacy_session(
                        connection,
                        session_id="manifest-session",
                        inferred_project_key="directory:test:/workspace/manifest",
                    )
                    connection.execute(
                        "UPDATE sessions SET source_host = ?, event_count = ? WHERE id = ?",
                        ("manifest-host", 27, "manifest-session"),
                    )

                def deny_event_reads(
                    action: int,
                    table: str | None,
                    _column: str | None,
                    _database: str | None,
                    _trigger: str | None,
                ) -> int:
                    if action == sqlite3.SQLITE_READ and table == "events":
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                connection.set_authorizer(deny_event_reads)
                manifest = fetch_host_sync_manifest(connection, "manifest-host")

            self.assertEqual(len(manifest), 1)
            self.assertEqual(manifest[0]["event_count"], 27)
            self.assertEqual(manifest[0]["stored_event_count"], 27)


if __name__ == "__main__":
    unittest.main()
