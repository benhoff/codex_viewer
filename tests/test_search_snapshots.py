from contextlib import closing
from datetime import datetime
from pathlib import Path
import json
import os
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

    def snapshot(self, snapshot_id=None, user=None, *, fresh_snapshot=False):
        return evidence_snapshot(
            self.settings,
            auth_user=user or self.user,
            auth_enabled=True,
            snapshot_id=snapshot_id,
            fresh_snapshot=fresh_snapshot,
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
            with self.snapshot() as (_, _, reused, _, _):
                self.assertEqual(reused["index_generation"], metadata["index_generation"])
            with self.assertRaises(HTTPException) as caught:
                with self.snapshot(fresh_snapshot=True):
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
                    "sqlite_temp_master",
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
                with self.snapshot(fresh_snapshot=True):
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

    def test_poll_elapsed_advances_while_inventory_has_no_progress_callbacks(self):
        clock = [time.monotonic()]
        entered, release = threading.Event(), threading.Event()

        def stalled_inventory(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("Test did not release inventory")
            return prepare_coverage_inventory(*args, **kwargs)

        with patch("agent_operations_viewer.search_snapshots.time.monotonic", side_effect=lambda: clock[0]), \
             patch("agent_operations_viewer.search_snapshots.prepare_coverage_inventory", side_effect=stalled_inventory), \
             patch("agent_operations_viewer.search_snapshots.SNAPSHOT_REQUEST_WAIT_SECONDS", 0.01):
            try:
                with self.assertRaises(HTTPException) as first:
                    with self.snapshot():
                        pass
                snapshot_id = first.exception.detail["snapshot_id"]
                job = _BUILDS[(str(self.settings.database_path.resolve()), "reader")]
                self.assertTrue(entered.wait(2))
                for elapsed in (17, 83):
                    clock[0] = job.started + elapsed
                    with self.assertRaises(HTTPException) as poll:
                        with self.snapshot(snapshot_id):
                            pass
                    detail = poll.exception.detail
                    self.assertEqual(detail["stage"], "coverage_inventory")
                    self.assertEqual(detail["elapsed_seconds"], elapsed)
                    self.assertEqual(detail["stage_elapsed_seconds"], elapsed)
                    self.assertEqual(detail["progress_age_seconds"], elapsed)
                    self.assertEqual(detail["backup_pages_copied"], detail["backup_pages_total"])
                    self.assertEqual(detail["backup_progress_scope"], "database_copy_only")
                    self.assertNotIn("_recorded_monotonic", detail)
            finally:
                release.set()
            self.assertTrue(job.done.wait(3))
        with self.snapshot(snapshot_id) as (_, _, metadata, _, _):
            self.assertEqual(metadata["preparation"]["elapsed_seconds"], 83)

    def test_final_timings_include_commit_close_and_publication_and_remain_frozen(self):
        from agent_operations_viewer.search_snapshots import _build_snapshot
        clock, wall = [time.monotonic()], [time.time()]
        start = wall[0]
        original_connect, original_replace = sqlite3.connect, os.replace

        def advance(seconds):
            clock[0] += seconds
            wall[0] += seconds

        class SlowFinalization(sqlite3.Connection):
            def commit(self):
                super().commit()
                advance(5)

            def close(self):
                super().close()
                advance(3)

        def connect_with_slow_finalization(database, *args, **kwargs):
            if str(database).endswith(".creating"):
                kwargs["factory"] = SlowFinalization
            return original_connect(database, *args, **kwargs)

        def slow_publication(source, destination):
            if str(destination).endswith(".sqlite3"):
                advance(7)
            return original_replace(source, destination)

        def delayed_worker(*args, **kwargs):
            advance(4)
            return _build_snapshot(*args, **kwargs)

        with patch("agent_operations_viewer.search_snapshots.time.monotonic", side_effect=lambda: clock[0]), \
             patch("agent_operations_viewer.search_snapshots.time.time", side_effect=lambda: wall[0]), \
             patch("agent_operations_viewer.search_snapshots.sqlite3.connect", side_effect=connect_with_slow_finalization), \
             patch("agent_operations_viewer.search_snapshots._build_snapshot", side_effect=delayed_worker), \
             patch("agent_operations_viewer.search_snapshots.os.replace", side_effect=slow_publication):
            with self.snapshot() as (_, _, metadata, _, _):
                snapshot_id = metadata["snapshot_id"]
                preparation = metadata["preparation"]
                self.assertEqual(preparation["elapsed_seconds"], 19)
                self.assertEqual(preparation["stage_timings_seconds"]["queued"], 4)
                self.assertEqual(preparation["stage_timings_seconds"]["metadata"], 8)
                self.assertEqual(preparation["stage_timings_seconds"]["publish"], 7)
                self.assertEqual(sum(preparation["stage_timings_seconds"].values()), 19)
                self.assertAlmostEqual(datetime.fromisoformat(metadata["ready_at"]).timestamp(), start + 19, places=5)
                self.assertAlmostEqual(datetime.fromisoformat(metadata["expires_at"]).timestamp(), start + 919, places=5)
            advance(60)
            with self.snapshot(snapshot_id) as (_, _, reused, _, _):
                self.assertEqual(reused["preparation"], preparation)
                self.assertEqual(reused["ready_at"], metadata["ready_at"])

    def test_partial_publication_is_not_ready_without_final_manifest(self):
        from agent_operations_viewer.search_snapshots import _write_status
        entered, release = threading.Event(), threading.Event()

        def hold_manifest(path, detail):
            if path.suffix == ".ready":
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("Test did not release publication")
            _write_status(path, detail)

        with patch("agent_operations_viewer.search_snapshots._write_status", side_effect=hold_manifest), \
             patch("agent_operations_viewer.search_snapshots.SNAPSHOT_REQUEST_WAIT_SECONDS", 0.01):
            try:
                with self.assertRaises(HTTPException) as first:
                    with self.snapshot():
                        pass
                snapshot_id = first.exception.detail["snapshot_id"]
                job = _BUILDS[(str(self.settings.database_path.resolve()), "reader")]
                self.assertTrue(entered.wait(2))
                with self.assertRaises(HTTPException) as poll:
                    with self.snapshot(snapshot_id):
                        pass
                self.assertEqual(poll.exception.detail["code"], "snapshot_building")
                self.assertEqual(poll.exception.detail["stage"], "publish")
            finally:
                release.set()
            self.assertTrue(job.done.wait(3))
        with self.snapshot(snapshot_id):
            pass
        ready = self.settings.database_path.parent / "search-snapshots" / f"{job.generation}.ready"
        ready.unlink()
        with self.assertRaises(HTTPException) as missing:
            with self.snapshot(snapshot_id):
                pass
        self.assertEqual(missing.exception.detail["code"], "snapshot_build_interrupted")

    def test_legacy_snapshots_without_timing_manifest_remain_readable(self):
        with self.snapshot() as (_, _, metadata, signer, _):
            token = signer.loads(metadata["snapshot_id"])
        directory = self.settings.database_path.parent / "search-snapshots"
        generation = metadata["index_generation"]
        stored = json.loads((directory / f"{generation}.{token['handle']}.handle").read_text())
        token.pop("handle")
        snapshot_id = signer.dumps(token)
        stored["metadata"].pop("preparation_status_version")
        stored["metadata"].pop("preparation")
        # Build a genuine legacy owner-bound metadata record in the physical DB.
        with closing(sqlite3.connect(directory / f"{generation}.sqlite3")) as frozen, frozen:
            stored["access"] = {"auth_enabled": True, "bypass": False, "user_id": "reader", "project_roles": {}}
            frozen.execute("CREATE TABLE evidence_snapshot_metadata (payload TEXT NOT NULL)")
            frozen.execute("INSERT INTO evidence_snapshot_metadata VALUES (?)", (json.dumps(stored),))
        (directory / f"{generation}.ready").unlink()
        with self.snapshot(snapshot_id) as (_, _, legacy, _, _):
            self.assertNotIn("preparation", legacy)
            self.assertEqual(legacy["expires_at"], metadata["expires_at"])

    def test_failure_after_database_publication_never_returns_partial_snapshot(self):
        from agent_operations_viewer.search_snapshots import _write_status

        def fail_manifest(path, detail):
            if path.suffix == ".ready":
                raise OSError("private filesystem details")
            _write_status(path, detail)

        with patch("agent_operations_viewer.search_snapshots._write_status", side_effect=fail_manifest), \
             self.assertLogs("agent_operations_viewer.search_snapshots", level="ERROR"):
            with self.assertRaises(HTTPException) as failed:
                with self.snapshot():
                    pass
        self.assertEqual(failed.exception.detail["code"], "snapshot_build_failed")
        self.assertEqual(failed.exception.detail["stage"], "publish")
        self.assertIn("publish", failed.exception.detail["stage_timings_seconds"])
        self.assertNotIn("private", json.dumps(failed.exception.detail))
        self.assertNotIn("_recorded_monotonic", failed.exception.detail)

    def test_poll_reports_timeout_while_builder_is_still_blocked(self):
        clock = [time.monotonic()]
        entered, release = threading.Event(), threading.Event()

        def blocked_inventory(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("Test did not release inventory")
            return prepare_coverage_inventory(*args, **kwargs)

        with patch("agent_operations_viewer.search_snapshots.time.monotonic", side_effect=lambda: clock[0]), \
             patch("agent_operations_viewer.search_snapshots.prepare_coverage_inventory", side_effect=blocked_inventory), \
             patch("agent_operations_viewer.search_snapshots.SNAPSHOT_REQUEST_WAIT_SECONDS", 0.01), \
             self.assertLogs("agent_operations_viewer.search_snapshots", level="ERROR"):
            try:
                with self.assertRaises(HTTPException) as first:
                    with self.snapshot():
                        pass
                snapshot_id = first.exception.detail["snapshot_id"]
                job = _BUILDS[(str(self.settings.database_path.resolve()), "reader")]
                self.assertTrue(entered.wait(2))
                clock[0] = job.started + 605
                with self.assertRaises(HTTPException) as overdue:
                    with self.snapshot(snapshot_id):
                        pass
                self.assertEqual(overdue.exception.detail["code"], "snapshot_build_failed")
                self.assertEqual(overdue.exception.detail["reason"], "deadline_exceeded")
                self.assertEqual(overdue.exception.detail["elapsed_seconds"], 605)
                self.assertEqual(overdue.exception.detail["stage_timings_seconds"]["coverage_inventory"], 605)
                self.assertTrue(overdue.exception.detail["worker_stopping"])
                self.assertFalse(job.done.is_set())
                clock[0] += 10
                with self.assertRaises(HTTPException) as repeat:
                    with self.snapshot(snapshot_id):
                        pass
                self.assertEqual(repeat.exception.detail, overdue.exception.detail)
            finally:
                release.set()
            self.assertTrue(job.done.wait(3))
        with self.assertRaises(HTTPException) as finished:
            with self.snapshot(snapshot_id):
                pass
        self.assertEqual(finished.exception.detail["code"], "snapshot_build_failed")
        self.assertEqual(finished.exception.detail["reason"], "deadline_exceeded")

    def test_faster_snapshot_directory_preserves_old_ids_and_signing_key(self):
        with self.snapshot() as (_, _, old, _, _):
            old_id = old["snapshot_id"]
        legacy = self.settings.database_path.parent / "search-snapshots"
        key = (legacy / "signing-key").read_bytes()
        self.settings.search_snapshot_dir = Path(self.directory.name) / "fast-snapshots"
        with self.snapshot(old_id) as (_, _, reused, _, _):
            self.assertEqual(reused, old)
        with self.snapshot() as (_, _, new, _, _):
            self.assertNotEqual(new["snapshot_id"], old_id)
        generation = new["index_generation"]
        self.assertTrue((self.settings.search_snapshot_dir / f"{generation}.sqlite3").exists())
        self.assertFalse((legacy / f"{generation}.sqlite3").exists())
        self.assertEqual((legacy / "signing-key").read_bytes(), key)
        self.assertFalse((self.settings.search_snapshot_dir / "signing-key").exists())
        with patch("agent_operations_viewer.search_snapshots.MAX_SNAPSHOTS", 2):
            with self.assertRaises(HTTPException) as full:
                with self.snapshot(fresh_snapshot=True):
                    pass
            self.assertEqual(full.exception.detail["code"], "snapshot_capacity")
        with patch("agent_operations_viewer.search_snapshots.time.time", return_value=time.time() + 1000):
            with self.snapshot():
                pass
        self.assertFalse(list(legacy.glob("*.sqlite3")))

    def test_relocated_snapshot_builder_still_uses_shared_creation_lock(self):
        from agent_operations_viewer.search_snapshots import _signer
        _, _ = _signer(self.settings)
        legacy = self.settings.database_path.parent / "search-snapshots"
        self.settings.search_snapshot_dir = Path(self.directory.name) / "fast-snapshots"
        with (legacy / "lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(HTTPException) as busy:
                with self.snapshot():
                    pass
            self.assertEqual(busy.exception.detail["code"], "snapshot_building")
        with self.snapshot():
            pass

    def test_shared_generation_has_private_handles_and_no_owner_in_physical_file(self):
        with patch('agent_operations_viewer.search_snapshots.prepare_coverage_inventory', wraps=prepare_coverage_inventory) as inventory:
            with self.snapshot() as (_, _, first, _, _):
                pass
            with self.snapshot(user={'user_id': 'administrator', 'role': 'admin'}) as (connection, access, admin, _, _):
                self.assertEqual(search_turn_hits_raw(connection, 'needle', project_access=access)['session_count'], 2)
                self.assertEqual(admin['coverage']['sessions_total'], 2)
            with self.snapshot(user={'user_id': 'another-reader', 'role': 'viewer'}) as (connection, access, reader, _, _):
                self.assertEqual(search_turn_hits_raw(connection, 'needle', project_access=access)['session_count'], 1)
                self.assertEqual(reader['coverage']['sessions_total'], 1)
                self.assertEqual(connection.execute('SELECT COUNT(*) FROM sessions').fetchone()[0], 1)
            self.assertEqual(inventory.call_count, 1)
        self.assertEqual(first['index_generation'], admin['index_generation'])
        self.assertEqual(first['index_generation'], reader['index_generation'])
        self.assertEqual(len({first['snapshot_id'], admin['snapshot_id'], reader['snapshot_id']}), 3)
        self.assertEqual(reader['generation_reuse'], 'ready')
        self.assertEqual(first['preparation'], reader['preparation'])
        directory = self.settings.database_path.parent / 'search-snapshots'
        self.assertEqual(len(list(directory.glob('*.sqlite3'))), 1)
        with closing(sqlite3.connect(next(directory.glob('*.sqlite3')))) as connection:
            stored = json.loads(connection.execute('SELECT payload FROM evidence_generation_metadata').fetchone()[0])
            self.assertNotIn('access', stored)
            self.assertNotIn('owner', stored)
            self.assertNotIn('coverage', stored['metadata'])
            self.assertIsNone(connection.execute("SELECT name FROM sqlite_master WHERE name='evidence_snapshot_metadata'").fetchone())
        for path in directory.glob('*.handle'):
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_fresh_request_refreshes_content_without_changing_existing_handles(self):
        with self.snapshot() as (_, _, original, _, _):
            pass
        with closing(connect(self.settings.database_path)) as connection, connection:
            connection.execute("UPDATE session_turn_search SET prompt_text='newcapture' WHERE session_id='public'")
        with self.snapshot() as (connection, access, reused, _, _):
            self.assertEqual(reused['index_generation'], original['index_generation'])
            self.assertEqual(search_turn_hits_raw(connection, 'newcapture', fields=('prompt',), project_access=access)['total_count'], 0)
        with self.snapshot(fresh_snapshot=True) as (connection, access, fresh, _, _):
            self.assertNotEqual(fresh['index_generation'], original['index_generation'])
            self.assertEqual(fresh['generation_reuse'], 'created')
            self.assertEqual(search_turn_hits_raw(connection, 'newcapture', fields=('prompt',), project_access=access)['total_count'], 1)
        with self.snapshot(original['snapshot_id']) as (_, _, reopened, _, _):
            self.assertEqual(reopened, original)

    def test_revocation_does_not_poison_other_handles_and_new_scope_excludes_removed_sessions(self):
        with self.snapshot(user={'user_id': 'reader', 'role': 'admin'}) as (_, _, admin, _, _):
            pass
        with self.snapshot() as (_, _, reader, _, _):
            pass
        with self.assertRaises(HTTPException) as denied:
            with self.snapshot(admin['snapshot_id']):
                pass
        self.assertEqual(denied.exception.detail['code'], 'snapshot_access_revoked')
        with self.snapshot(reader['snapshot_id']):
            pass
        with closing(connect(self.settings.database_path)) as connection, connection:
            connection.execute("UPDATE project_sources SET project_id='private' WHERE match_project_key='public'")
        with self.assertRaises(HTTPException) as remapped:
            with self.snapshot(reader['snapshot_id']):
                pass
        self.assertEqual(remapped.exception.detail['code'], 'snapshot_access_revoked')
        with self.snapshot() as (connection, access, restricted, _, _):
            self.assertEqual(restricted['index_generation'], reader['index_generation'])
            self.assertEqual(restricted['coverage']['sessions_total'], 0)
            self.assertEqual(search_turn_hits_raw(connection, 'needle', project_access=access)['total_count'], 0)
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM sessions').fetchone()[0], 0)

    def test_join_in_progress_generation_uses_separate_handle(self):
        entered, release = threading.Event(), threading.Event()
        def slow_inventory(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise RuntimeError('Test did not release inventory')
            return prepare_coverage_inventory(*args, **kwargs)
        with patch('agent_operations_viewer.search_snapshots.prepare_coverage_inventory', side_effect=slow_inventory) as inventory, \
             patch('agent_operations_viewer.search_snapshots.SNAPSHOT_REQUEST_WAIT_SECONDS', 0.01):
            try:
                with self.assertRaises(HTTPException) as first:
                    with self.snapshot():
                        pass
                first_id = first.exception.detail['snapshot_id']
                job = _BUILDS[(str(self.settings.database_path.resolve()), 'reader')]
                self.assertTrue(entered.wait(2))
                with self.assertRaises(HTTPException) as joined:
                    with self.snapshot(user={'user_id': 'other', 'role': 'admin'}, fresh_snapshot=True):
                        pass
                other_id = joined.exception.detail['snapshot_id']
                self.assertNotEqual(first_id, other_id)
                self.assertEqual(joined.exception.detail['build_id'], first.exception.detail['build_id'])
            finally:
                release.set()
            self.assertTrue(job.done.wait(3))
            self.assertEqual(inventory.call_count, 1)
        with self.snapshot(first_id) as (_, _, first, _, _):
            pass
        with self.snapshot(other_id, user={'user_id': 'other', 'role': 'admin'}) as (_, _, joined, _, _):
            self.assertEqual(joined['index_generation'], first['index_generation'])
            self.assertEqual(joined['generation_reuse'], 'building')
            self.assertEqual(joined['coverage']['sessions_total'], 2)
            self.assertEqual(first['coverage']['sessions_total'], 1)

    def test_reuse_window_and_generation_expiry_never_extend(self):
        with self.snapshot() as (_, _, original, _, _):
            pass
        ready = datetime.fromisoformat(original['ready_at']).timestamp()
        with patch('agent_operations_viewer.search_snapshots.time.time', return_value=ready + 299):
            with self.snapshot() as (_, _, last_reuse, _, _):
                self.assertEqual(last_reuse['index_generation'], original['index_generation'])
                self.assertEqual(last_reuse['expires_at'], original['expires_at'])
        with patch('agent_operations_viewer.search_snapshots.time.time', return_value=ready + 301):
            with self.snapshot() as (_, _, fresh, _, _):
                self.assertNotEqual(fresh['index_generation'], original['index_generation'])
            with self.snapshot(last_reuse['snapshot_id']):
                pass
        with patch('agent_operations_viewer.search_snapshots.time.time', return_value=ready + 901):
            with self.assertRaises(HTTPException) as expired:
                with self.snapshot(last_reuse['snapshot_id']):
                    pass
            self.assertEqual(expired.exception.status_code, 410)

    def test_another_process_reuses_ready_generation(self):
        import subprocess
        import sys
        with self.snapshot() as (_, _, original, _, _):
            pass
        code = '''
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from agent_operations_viewer.search_snapshots import evidence_snapshot
settings = SimpleNamespace(database_path=Path(sys.argv[1]))
with patch('agent_operations_viewer.search_snapshots._build_snapshot', side_effect=AssertionError('Unexpected backup')):
    with evidence_snapshot(settings, auth_user={'user_id':'other-process', 'role':'viewer'}, auth_enabled=True) as (_, _, metadata, _, _):
        assert metadata['index_generation'] == sys.argv[2]
        assert metadata['coverage']['sessions_total'] == 1
        assert metadata['generation_reuse'] == 'ready'
'''
        result = subprocess.run([sys.executable, '-c', code, str(self.settings.database_path), original['index_generation']], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_visible_reassignment_requires_fresh_capture_instead_of_incomplete_results(self):
        admin = {'user_id': 'administrator', 'role': 'admin'}
        with self.snapshot(user=admin):
            pass
        with closing(connect(self.settings.database_path)) as connection, connection:
            connection.execute("UPDATE project_sources SET project_id='private' WHERE match_project_key='public'")
        with self.assertRaises(HTTPException) as changed:
            with self.snapshot(user=admin):
                pass
        self.assertEqual(changed.exception.detail['code'], 'snapshot_scope_changed')
        with self.snapshot(user=admin, fresh_snapshot=True) as (connection, access, metadata, _, _):
            self.assertEqual(metadata['coverage']['sessions_total'], 2)
            self.assertEqual(search_turn_hits_raw(connection, 'needle', project_access=access)['session_count'], 2)
