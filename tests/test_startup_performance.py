from __future__ import annotations

import asyncio
from pathlib import Path
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

from agent_operations_viewer.db import (
    backfill_session_agent_metadata,
    connect,
    init_db,
    run_db_backfills,
    write_transaction,
)
from agent_operations_viewer.importer import fetch_host_sync_manifest
from agent_operations_viewer.session_insights import AGENT_METADATA_VERSION
from agent_operations_viewer.web.app import _start_post_startup_maintenance
from agent_operations_viewer.web.concurrency import run_in_history_threadpool


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
