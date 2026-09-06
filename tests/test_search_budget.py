from contextlib import closing, contextmanager
from types import SimpleNamespace
import sqlite3
import unittest
from unittest.mock import patch

from fastapi import HTTPException

from agent_operations_viewer.search_budget import (
    SearchWorkTimeout,
    mark_search_stage,
    search_work_budget,
)
from agent_operations_viewer.web.routes.search_api import _snapshot, _hit_identity
from agent_operations_viewer.turn_index import compute_session_turn_index


class SearchBudgetTests(unittest.TestCase):
    def test_sqlite_work_is_interrupted_and_connection_remains_usable(self):
        with closing(sqlite3.connect(":memory:")) as connection:
            with patch(
                "agent_operations_viewer.search_budget.SEARCH_REQUEST_TIMEOUT_SECONDS",
                0.02,
            ):
                with self.assertRaises(SearchWorkTimeout) as caught:
                    with search_work_budget(connection):
                        mark_search_stage("facets")
                        connection.execute("""
                            WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000000)
                            SELECT sum(x) FROM n
                        """).fetchone()
            self.assertEqual(caught.exception.stage, "facets")
            self.assertEqual(connection.execute("SELECT 1").fetchone()[0], 1)

    def test_work_budget_is_shared_across_query_stages(self):
        clock = [0.0]
        with closing(sqlite3.connect(":memory:")) as connection, patch(
            "agent_operations_viewer.search_budget.time.monotonic",
            side_effect=lambda: clock[0],
        ):
            with self.assertRaises(SearchWorkTimeout):
                with search_work_budget(connection):
                    mark_search_stage("coverage")
                    clock[0] = 12
                    mark_search_stage("page_retrieval")
                    clock[0] = 21
                    mark_search_stage("coverage")
            # The request-local context must be restored after failure.
            mark_search_stage("outside_request")

    def test_api_returns_a_structured_timeout(self):
        @contextmanager
        def slow_snapshot(*args, **kwargs):
            with closing(sqlite3.connect(":memory:")) as connection:
                with search_work_budget(connection):
                    yield (connection,)

        request = SimpleNamespace(state=SimpleNamespace(auth_enabled=False))
        with patch(
            "agent_operations_viewer.web.routes.search_api.get_app_context",
            return_value=SimpleNamespace(settings=None),
        ), patch(
            "agent_operations_viewer.web.routes.search_api.evidence_snapshot",
            slow_snapshot,
        ), patch(
            "agent_operations_viewer.search_budget.SEARCH_REQUEST_TIMEOUT_SECONDS", -1
        ):
            with self.assertRaises(HTTPException) as caught:
                with _snapshot(request):
                    mark_search_stage("coverage")
        self.assertEqual(caught.exception.status_code, 503)
        self.assertEqual(caught.exception.detail["code"], "query_timeout")
        self.assertIn("stage", caught.exception.detail)

    def test_patch_evidence_uses_integer_lookup_and_does_not_reread_identity(self):
        with closing(sqlite3.connect(":memory:")) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute(
                "CREATE TABLE session_search_chunks(chunk_id TEXT PRIMARY KEY)"
            )
            connection.execute(
                "CREATE VIRTUAL TABLE session_search_chunk_fts USING fts5(content, chunk_id UNINDEXED)"
            )
            connection.execute(
                "INSERT INTO session_search_chunks(rowid,chunk_id) VALUES(7,'patch')"
            )
            connection.execute(
                "INSERT INTO session_search_chunk_fts(rowid,content,chunk_id) VALUES(7,'@@\n-old\n+new','patch')"
            )

            def authorize(action, table, column, database, trigger):
                if (
                    action == sqlite3.SQLITE_READ
                    and table == "session_search_chunk_fts"
                    and column == "chunk_id"
                ):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            connection.set_authorizer(authorize)
            identity = dict.fromkeys(
                (
                    "content_digest",
                    "normalization_version",
                    "content_version",
                    "activity_digest",
                ),
                "test",
            )
            hit = {
                "session_id": "s",
                "turn_number": 1,
                "matched_field": "patches",
                "chunk": {"id": "patch", "start_offset": 0},
            }
            with patch(
                "agent_operations_viewer.web.routes.search_api._load_turn",
                side_effect=AssertionError("cached identity reconstructed"),
            ):
                _hit_identity(connection, hit, {("s", 1): identity})
            self.assertEqual(
                [line["kind"] for line in hit["chunk"]["lines"]],
                ["header", "deletion", "addition"],
            )

    def test_hit_identity_loads_only_required_metadata_and_caches_only_digests(self):
        with closing(sqlite3.connect(":memory:")) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute(
                "CREATE TABLE sessions(id TEXT PRIMARY KEY, cwd TEXT, raw_text TEXT)"
            )
            connection.execute(
                "CREATE TABLE session_turns(session_id TEXT, turn_number INTEGER, start_event_index INTEGER, end_event_index INTEGER)"
            )
            connection.execute(
                "INSERT INTO sessions VALUES('s','/example','unused body')"
            )
            connection.execute("INSERT INTO session_turns VALUES('s',1,0,2)")

            def authorize(action, table, column, database, trigger):
                if (
                    action == sqlite3.SQLITE_READ
                    and table == "sessions"
                    and column == "raw_text"
                ):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            connection.set_authorizer(authorize)
            identity = dict.fromkeys(
                (
                    "content_digest",
                    "normalization_version",
                    "content_version",
                    "activity_digest",
                ),
                "test",
            )
            cache = {}
            with patch(
                "agent_operations_viewer.web.routes.search_api._load_turn",
                return_value={**identity, "activity": ["large body"]},
            ) as load:
                for _ in range(2):
                    hit = {"session_id": "s", "turn_number": 1}
                    _hit_identity(connection, hit, cache)
                    self.assertEqual(hit["content_digest"], "test")
                load.assert_called_once()
            self.assertEqual(cache, {("s", 1): identity})

    def test_environment_only_capture_does_not_invent_a_turn(self):
        events = [
            {
                "event_index": 0,
                "record_type": "response_item",
                "payload_type": "message",
                "kind": "message",
                "role": "user",
                "display_text": "<environment_context>\n<cwd>/example</cwd>\n</environment_context>",
            }
        ]
        self.assertEqual(compute_session_turn_index(events), [])
