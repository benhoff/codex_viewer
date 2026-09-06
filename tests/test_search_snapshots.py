from contextlib import closing
from datetime import datetime
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
import threading
import fcntl
import sqlite3
from unittest.mock import patch

from fastapi import HTTPException

from agent_operations_viewer.db import connect, init_db
from agent_operations_viewer.search import search_turn_hits_raw
from agent_operations_viewer.search_snapshots import evidence_snapshot, _BUILDS
from agent_operations_viewer.search import _search_coverage, prepare_coverage_inventory
from agent_operations_viewer.projects import project_access_condition_sql
from tests.test_search import insert_search_turn


class SearchSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.settings = SimpleNamespace(
            database_path=Path(self.directory.name) / "viewer.sqlite3"
        )
        init_db(self.settings.database_path)
        with closing(connect(self.settings.database_path)) as connection, connection:
            insert_search_turn(
                connection,
                session_id="public",
                project_id="public",
                project_key="public",
                project_label="Public",
                prompt="needle",
            )
            insert_search_turn(
                connection,
                session_id="private",
                project_id="private",
                project_key="private",
                project_label="Private",
                prompt="needle",
                visibility="private",
            )
        self.user = {"user_id": "reader", "role": "viewer"}

    def snapshot(self, snapshot_id=None, user=None):
        return evidence_snapshot(
            self.settings,
            auth_user=user or self.user,
            auth_enabled=True,
            snapshot_id=snapshot_id,
        )

    def test_reopen_does_not_require_process_cache_and_enforces_owner(self):
        with self.snapshot() as (connection, access, metadata, _, _):
            snapshot_id = metadata["snapshot_id"]
            expected = search_turn_hits_raw(connection, "needle", project_access=access)
        with self.snapshot(snapshot_id) as (connection, access, metadata, _, _):
            self.assertEqual(
                search_turn_hits_raw(connection, "needle", project_access=access),
                expected,
            )
        with self.assertRaises(HTTPException) as caught:
            with self.snapshot(snapshot_id, {"user_id": "other", "role": "admin"}):
                pass
        self.assertEqual(caught.exception.status_code, 403)
        for path in (self.settings.database_path.parent / "search-snapshots").glob(
            "*.sqlite3"
        ):
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_new_access_does_not_broaden_existing_snapshot(self):
        with self.snapshot() as (_, _, metadata, _, _):
            snapshot_id = metadata["snapshot_id"]
        with closing(connect(self.settings.database_path)) as connection, connection:
            connection.execute(
                "UPDATE projects SET visibility = 'authenticated' WHERE id = 'private'"
            )
        with self.snapshot(snapshot_id) as (connection, access, _, _, _):
            self.assertEqual(
                search_turn_hits_raw(connection, "needle", project_access=access)[
                    "session_count"
                ],
                1,
            )
        with self.snapshot() as (connection, access, _, _, _):
            self.assertEqual(
                search_turn_hits_raw(connection, "needle", project_access=access)[
                    "session_count"
                ],
                2,
            )

    def test_capacity_reuse_expiration_and_cleanup(self):
        with patch("agent_operations_viewer.search_snapshots.MAX_SNAPSHOTS", 1):
            with self.snapshot() as (_, _, metadata, _, _):
                snapshot_id = metadata["snapshot_id"]
            with self.snapshot(snapshot_id):
                pass
            with self.assertRaises(HTTPException) as caught:
                with self.snapshot():
                    pass
            self.assertEqual(caught.exception.status_code, 503)
            with patch(
                "agent_operations_viewer.search_snapshots.time.time",
                return_value=time.time() + 1000,
            ):
                with self.snapshot() as (_, _, newer, _, _):
                    self.assertNotEqual(newer["snapshot_id"], snapshot_id)
                with self.assertRaises(HTTPException) as caught:
                    with self.snapshot(snapshot_id):
                        pass
                self.assertEqual(caught.exception.status_code, 410)
        self.assertEqual(
            len(
                list(
                    (self.settings.database_path.parent / "search-snapshots").glob(
                        "*.sqlite3"
                    )
                )
            ),
            1,
        )

    def test_normalization_upgrade_fails_explicitly(self):
        with self.snapshot() as (_, _, metadata, _, _):
            snapshot_id = metadata["snapshot_id"]
        with patch(
            "agent_operations_viewer.search_snapshots.NORMALIZATION_VERSION", "next"
        ):
            with self.assertRaises(HTTPException) as caught:
                with self.snapshot(snapshot_id):
                    pass
        self.assertEqual(caught.exception.status_code, 409)
        self.assertEqual(caught.exception.detail["code"], "snapshot_version_mismatch")

    def test_invalid_tokens_never_wait_for_creation_or_open_database(self):
        with self.snapshot() as (_, _, metadata, _, _):
            snapshot_id = metadata["snapshot_id"]
        with patch(
            "agent_operations_viewer.search_snapshots.fcntl.flock",
            side_effect=AssertionError("Unexpected file lock"),
        ):
            with self.snapshot(snapshot_id):
                pass
            with patch(
                "agent_operations_viewer.search_snapshots.connect",
                side_effect=AssertionError("Unexpected database access"),
            ):
                with self.assertRaises(HTTPException) as caught:
                    with self.snapshot("invalid"):
                        pass
                self.assertEqual(caught.exception.status_code, 409)

    def test_snapshot_coverage_never_rescans_fts_or_events(self):
        with self.snapshot() as (connection, _, _, _, _):

            def authorize(action, table, column, database, trigger):
                if action == sqlite3.SQLITE_READ and (
                    table == "events" or table.startswith("session_turn_search")
                ):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            connection.set_authorizer(authorize)
            coverage = _search_coverage(
                connection, base_conditions=["s.id = ?"], base_params=["public"]
            )
            self.assertEqual(coverage["sessions_total"], 1)
            self.assertEqual(coverage["turns_missing_evidence"], 1)

    def test_unscoped_coverage_reuses_snapshot_metadata_without_scanning_corpus(self):
        with self.snapshot() as (connection, access, metadata, _, _):
            condition, params = project_access_condition_sql(access)

            def authorize(action, table, column, database, trigger):
                if action == sqlite3.SQLITE_READ and table not in {
                    "sqlite_master",
                    "evidence_snapshot_metadata",
                }:
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            connection.set_authorizer(authorize)
            coverage = _search_coverage(
                connection, base_conditions=[condition], base_params=params
            )
            self.assertEqual(coverage, metadata["coverage"])
            # A different scope must not get the cached authorization totals.
            with self.assertRaises(sqlite3.DatabaseError):
                _search_coverage(connection, base_conditions=[], base_params=[])

    def test_filtered_coverage_does_not_reuse_unscoped_totals(self):
        with self.snapshot() as (connection, access, metadata, _, _):
            condition, params = project_access_condition_sql(access)
            self.assertEqual(metadata["coverage"]["sessions_total"], 1)
            for extra, value in (("s.id != ?", "public"), ("s.id = ?", "private")):
                coverage = _search_coverage(
                    connection,
                    base_conditions=[extra, condition],
                    base_params=[value, *params],
                )
                self.assertEqual(coverage["sessions_total"], 0)

    def test_filtered_coverage_uses_compact_catalog_not_wide_corpus_tables(self):
        with self.snapshot() as (connection, access, _, _, _):
            condition, params = project_access_condition_sql(access)

            def authorize(action, table, column, database, trigger):
                if action == sqlite3.SQLITE_READ and table in {
                    "sessions",
                    "session_turns",
                    "events",
                    "session_turn_search",
                }:
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            connection.set_authorizer(authorize)
            coverage = _search_coverage(
                connection,
                base_conditions=["s.id = ?", condition],
                base_params=["public", *params],
            )
            self.assertEqual(coverage["sessions_total"], 1)
            self.assertEqual(coverage["turns_total"], 1)

    def test_ready_lifetime_and_cleanup_do_not_charge_preparation_time(self):
        start = time.time()
        wall_clock = [start]

        def slow_inventory(connection, **kwargs):
            wall_clock[0] += 500
            prepare_coverage_inventory(connection, **kwargs)

        with patch(
            "agent_operations_viewer.search_snapshots.time.time",
            side_effect=lambda: wall_clock[0],
        ), patch("agent_operations_viewer.search_snapshots.MAX_SNAPSHOTS", 1):
            with patch(
                "agent_operations_viewer.search_snapshots.prepare_coverage_inventory",
                side_effect=slow_inventory,
            ):
                with self.snapshot() as (_, _, metadata, _, _):
                    snapshot_id = metadata["snapshot_id"]
                    ready = datetime.fromisoformat(metadata["ready_at"]).timestamp()
                    expires = datetime.fromisoformat(metadata["expires_at"]).timestamp()
                    self.assertAlmostEqual(expires - ready, 900, places=3)
            wall_clock[0] = start + 950
            with self.snapshot(snapshot_id):
                pass
            with self.assertRaises(HTTPException) as caught:
                with self.snapshot():
                    pass
            self.assertEqual(caught.exception.detail["code"], "snapshot_capacity")
            wall_clock[0] = expires + 1
            # The signed upper bound has not expired yet: ready metadata must
            # enforce the earlier actual expiration, without renewing content.
            with self.assertRaises(HTTPException) as caught:
                with self.snapshot(snapshot_id):
                    pass
            self.assertEqual(caught.exception.detail["code"], "snapshot_expired")
            with self.snapshot():
                pass

    def test_inventory_uses_explicit_keys_and_tracks_missing_search_rows(self):
        with closing(connect(self.settings.database_path)) as connection:
            connection.execute("PRAGMA automatic_index=OFF")
            connection.execute(
                "DELETE FROM session_turn_search WHERE session_id='private'"
            )
            prepare_coverage_inventory(connection)
            self.assertEqual(
                [
                    tuple(row)
                    for row in connection.execute(
                        "SELECT session_id, has_search, has_evidence FROM evidence_turn_integrity ORDER BY session_id"
                    )
                ],
                [("private", 0, 0), ("public", 1, 0)],
            )
            self.assertIsNone(
                connection.execute(
                    "SELECT name FROM sqlite_temp_master WHERE name='evidence_indexed_turns'"
                ).fetchone()
            )

    def test_sqlite_deadline_interrupt_is_reported_as_timeout(self):
        def exceed_budget(connection, **kwargs):
            connection.execute("""
                WITH RECURSIVE counter(n) AS (
                    VALUES(1) UNION ALL SELECT n+1 FROM counter WHERE n<1000000000
                ) SELECT sum(n) FROM counter
            """).fetchone()

        with patch(
            "agent_operations_viewer.search_snapshots.prepare_coverage_inventory",
            side_effect=exceed_budget,
        ), patch(
            "agent_operations_viewer.search_snapshots.SNAPSHOT_BUILD_TIMEOUT_SECONDS",
            0.1,
        ), self.assertLogs("agent_operations_viewer.search_snapshots", level="ERROR"):
            with self.assertRaises(HTTPException) as caught:
                with self.snapshot():
                    pass
        self.assertEqual(caught.exception.detail["stage"], "coverage_inventory")
        self.assertEqual(caught.exception.detail["reason"], "deadline_exceeded")
        self.assertEqual(
            caught.exception.detail["sqlite_errorname"], "SQLITE_INTERRUPT"
        )

    def test_inventory_can_finish_after_five_minutes_of_build_work(self):
        clock = [time.monotonic()]

        def production_sized_inventory(connection, **kwargs):
            clock[0] += 310
            return prepare_coverage_inventory(connection, **kwargs)

        with patch(
            "agent_operations_viewer.search_snapshots.time.monotonic",
            side_effect=lambda: clock[0],
        ), patch(
            "agent_operations_viewer.search_snapshots.prepare_coverage_inventory",
            side_effect=production_sized_inventory,
        ):
            with self.snapshot() as (_, _, metadata, _, _):
                self.assertIn("snapshot_id", metadata)

    def test_creation_contention_is_retryable_without_blocking_readers(self):
        with self.snapshot() as (_, _, metadata, _, _):
            snapshot_id = metadata["snapshot_id"]
        directory = self.settings.database_path.parent / "search-snapshots"
        with (directory / "lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(HTTPException) as caught:
                with self.snapshot():
                    pass
            self.assertEqual(caught.exception.detail["code"], "snapshot_building")
            self.assertEqual(caught.exception.headers["Retry-After"], "2")
            with self.snapshot(snapshot_id):
                pass

    def test_failed_build_reports_stage_and_sanitized_reason(self):
        for error, reason in (
            (TimeoutError("private /path SQL token"), "deadline_exceeded"),
            (sqlite3.OperationalError("private /path SQL token"), "sqlite_error"),
            (RuntimeError("private /path SQL token"), "internal_error"),
        ):
            with self.subTest(reason=reason), patch(
                "agent_operations_viewer.search_snapshots.prepare_coverage_inventory",
                side_effect=error,
            ), self.assertLogs(
                "agent_operations_viewer.search_snapshots", level="ERROR"
            ):
                with self.assertRaises(HTTPException) as caught:
                    with self.snapshot():
                        pass
                detail = caught.exception.detail
                self.assertEqual(detail["code"], "snapshot_build_failed")
                self.assertEqual(detail["stage"], "coverage_inventory")
                self.assertEqual(detail["reason"], reason)
                self.assertIn("backup", detail["stage_timings_seconds"])
                self.assertGreaterEqual(detail["elapsed_seconds"], 0)
                self.assertEqual(
                    detail["backup_pages_copied"], detail["backup_pages_total"]
                )
                self.assertNotIn("private", str(detail))
        directory = self.settings.database_path.parent / "search-snapshots"
        self.assertFalse(list(directory.glob("*.creating")))
        self.assertFalse(list(directory.glob("*.pending")))
        # A failed job does not prevent a later build from succeeding.
        with self.snapshot():
            pass

    def test_slow_build_returns_identifier_and_finishes_independently(self):
        release = threading.Event()
        entered = threading.Event()

        def slow_coverage(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("Test did not release builder")
            return _search_coverage(*args, **kwargs)

        with patch(
            "agent_operations_viewer.search_snapshots._search_coverage",
            side_effect=slow_coverage,
        ), patch(
            "agent_operations_viewer.search_snapshots.SNAPSHOT_REQUEST_WAIT_SECONDS",
            0.01,
        ):
            try:
                with self.assertRaises(HTTPException) as caught:
                    with self.snapshot():
                        pass
                self.assertEqual(caught.exception.detail["code"], "snapshot_building")
                snapshot_id = caught.exception.detail["snapshot_id"]
                self.assertTrue(entered.wait(2))
                with self.assertRaises(HTTPException) as invalid:
                    with self.snapshot("invalid"):
                        pass
                self.assertEqual(invalid.exception.status_code, 409)
                with self.assertRaises(HTTPException) as pending:
                    with self.snapshot(snapshot_id):
                        pass
                self.assertEqual(pending.exception.detail["snapshot_id"], snapshot_id)
                self.assertEqual(pending.exception.detail["stage"], "coverage")
                self.assertIn(
                    "backup", pending.exception.detail["stage_timings_seconds"]
                )
                job = _BUILDS[(str(self.settings.database_path.resolve()), "reader")]
            finally:
                release.set()
            self.assertTrue(job.done.wait(3))
        with self.snapshot(snapshot_id) as (_, _, metadata, _, _):
            self.assertEqual(metadata["snapshot_id"], snapshot_id)
