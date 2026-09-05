from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from agent_operations_viewer.db import connect, init_db, write_transaction
from agent_operations_viewer.importer import parse_session_text, upsert_parsed_session
from agent_operations_viewer.projects import ProjectAccessContext, query_group_rows
from agent_operations_viewer.search import search_turn_hits_raw
from agent_operations_viewer.turn_index import (
    SEARCH_CHUNK_VERSION,
    TURN_SEARCH_VERSION,
    backfill_session_search_chunks,
    replace_session_search_chunks,
    split_search_text_chunks,
    _patch_search_text_full,
)
from tests.test_search import insert_search_turn


def search_event(
    *,
    event_index: int,
    record_type: str,
    payload_type: str,
    kind: str,
    role: str | None,
    display_text: str,
    detail_text: str | None = None,
    phase: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {"type": payload_type}
    if phase:
        payload["phase"] = phase
    return {
        "event_index": event_index,
        "timestamp": f"2026-08-23T12:00:0{event_index}+00:00",
        "record_type": record_type,
        "payload_type": payload_type,
        "kind": kind,
        "role": role,
        "display_text": display_text,
        "detail_text": detail_text if detail_text is not None else display_text,
        "tool_name": None,
        "command_text": None,
        "exit_code": None,
        "record_json": json.dumps({"payload": payload}),
    }


class SearchChunkIndexTests(unittest.TestCase):
    def test_patch_normalization_extracts_bodies_and_excludes_status(self) -> None:
        submitted = "*** Begin Patch\n*** Update File: app.py\n@@\n-old\n+new\n*** End Patch"
        self.assertEqual(_patch_search_text_full({"kind": "tool_call", "tool_name": "apply_patch", "display_text": submitted}), submitted)
        self.assertEqual(_patch_search_text_full({"kind": "tool_call", "tool_name": "exec_command", "display_text": "Run command", "command_text": "apply_patch <<'PATCH'\n" + submitted + "\nPATCH\necho ignored"}), submitted)
        self.assertEqual(_patch_search_text_full({"kind": "tool_call", "tool_name": "functions.apply_patch", "display_text": json.dumps({"patch": submitted})}), submitted)
        applied = {"payload_type": "patch_apply_end", "display_text": "Status: completed", "detail_text": json.dumps({"app.py": {"unified_diff": "@@\n-old\n+new"}})}
        self.assertEqual(_patch_search_text_full(applied), "@@\n-old\n+new")
        self.assertEqual(_patch_search_text_full({"kind": "tool_result", "tool_name": "apply_patch", "display_text": "Status: completed"}), "")

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "viewer.sqlite3"
        init_db(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_chunk_boundaries_are_stable_overlapping_and_content_addressed(self) -> None:
        text = " ".join(f"token-{index:04d}" for index in range(800))
        first = split_search_text_chunks(text, target_chars=512, overlap_chars=64)
        second = split_search_text_chunks(text, target_chars=512, overlap_chars=64)

        self.assertEqual(first, second)
        self.assertGreater(len(first), 2)
        for index, chunk in enumerate(first):
            self.assertEqual(
                chunk["content"],
                text[chunk["start_offset"] : chunk["end_offset"]],
            )
            if index:
                self.assertLess(chunk["start_offset"], first[index - 1]["end_offset"])

    def test_legacy_search_tables_are_rebuilt_as_derived_data(self) -> None:
        with connect(self.db_path) as connection:
            insert_search_turn(
                connection,
                session_id="legacy-index-session",
                project_id="legacy-index-project",
                project_key="acme/legacy-index",
                project_label="acme/legacy-index",
                prompt="legacy indexed prompt",
            )
            connection.execute(
                """
                UPDATE sessions
                SET turn_search_version = ?, search_chunk_version = ?, search_indexed_at = ?
                WHERE id = 'legacy-index-session'
                """,
                (
                    TURN_SEARCH_VERSION,
                    SEARCH_CHUNK_VERSION,
                    "2026-08-23T12:05:00+00:00",
                ),
            )
            connection.execute("DROP TABLE session_turn_search")
            connection.execute(
                """
                CREATE VIRTUAL TABLE session_turn_search USING fts5(
                    project_text,
                    prompt_text,
                    response_text,
                    event_text,
                    session_id UNINDEXED,
                    turn_number UNINDEXED
                )
                """
            )
            connection.execute("DROP TRIGGER session_search_chunks_delete_fts")
            connection.execute("DROP TABLE session_search_chunk_fts")
            connection.execute("DROP TABLE session_search_chunks")
            connection.execute(
                """
                CREATE TABLE session_search_chunks (
                    chunk_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    turn_number INTEGER NOT NULL,
                    field TEXT NOT NULL CHECK(field IN ('prompt', 'response', 'activity')),
                    chunk_index INTEGER NOT NULL,
                    start_offset INTEGER NOT NULL,
                    end_offset INTEGER NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    index_version INTEGER NOT NULL,
                    UNIQUE(session_id, turn_number, field, chunk_index)
                )
                """
            )
            connection.execute(
                """
                CREATE VIRTUAL TABLE session_search_chunk_fts USING fts5(
                    content,
                    project_text,
                    chunk_id UNINDEXED,
                    session_id UNINDEXED,
                    turn_number UNINDEXED,
                    field UNINDEXED
                )
                """
            )

        init_db(self.db_path, defer_backfills=True)

        with connect(self.db_path) as connection:
            turn_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(session_turn_search)"
                ).fetchall()
            }
            chunk_schema = str(
                connection.execute(
                    "SELECT sql FROM sqlite_master WHERE name = 'session_search_chunks'"
                ).fetchone()["sql"]
            )
            session = connection.execute(
                """
                SELECT turn_search_version, search_chunk_version, search_indexed_at
                FROM sessions
                WHERE id = 'legacy-index-session'
                """
            ).fetchone()

        self.assertTrue(
            {"command_text", "path_text", "commit_id_text", "tool_output_text"}
            <= turn_columns
        )
        self.assertIn("'commands'", chunk_schema)
        self.assertIn("'tool_output'", chunk_schema)
        self.assertEqual(int(session["turn_search_version"]), 0)
        self.assertEqual(int(session["search_chunk_version"]), 0)
        self.assertIsNone(session["search_indexed_at"])

    def test_hybrid_search_finds_text_beyond_all_legacy_turn_caps(self) -> None:
        prompt_marker = "prompt-tail-marker-neptune"
        response_marker = "response-tail-marker-saturn"
        activity_marker = "activity-tail-marker-jupiter"
        full_prompt = ("prompt filler " * 800) + prompt_marker
        full_response = ("response filler " * 1_100) + response_marker
        full_activity = ("activity filler " * 1_800) + activity_marker
        self.assertGreater(len(full_prompt), 8_000)
        self.assertGreater(len(full_response), 12_000)
        self.assertGreater(len(full_activity), 24_000)

        events = [
            search_event(
                event_index=1,
                record_type="event_msg",
                payload_type="user_message",
                kind="message",
                role="user",
                display_text=full_prompt,
            ),
            search_event(
                event_index=2,
                record_type="response_item",
                payload_type="custom_tool_call_output",
                kind="command",
                role=None,
                display_text=full_activity,
            ),
            search_event(
                event_index=3,
                record_type="response_item",
                payload_type="message",
                kind="message",
                role="assistant",
                display_text=full_response,
                phase="final_answer",
            ),
        ]

        with connect(self.db_path) as connection:
            insert_search_turn(
                connection,
                session_id="long-chunk-session",
                project_id="long-project",
                project_key="acme/long-project",
                project_label="acme/long-project",
                prompt="legacy prompt excerpt",
                response="legacy response excerpt",
                activity="legacy activity excerpt",
            )
            replace_session_search_chunks(connection, "long-chunk-session", events)
            first_ids = {
                str(row["chunk_id"])
                for row in connection.execute(
                    "SELECT chunk_id FROM session_search_chunks WHERE session_id = ?",
                    ("long-chunk-session",),
                ).fetchall()
            }
            replace_session_search_chunks(connection, "long-chunk-session", events)
            second_ids = {
                str(row["chunk_id"])
                for row in connection.execute(
                    "SELECT chunk_id FROM session_search_chunks WHERE session_id = ?",
                    ("long-chunk-session",),
                ).fetchall()
            }
            pages = {
                "prompt": search_turn_hits_raw(connection, prompt_marker),
                "response": search_turn_hits_raw(connection, response_marker),
                "activity": search_turn_hits_raw(connection, activity_marker),
            }
            insert_search_turn(
                connection,
                session_id="private-long-chunk-session",
                project_id="private-long-project",
                project_key="acme/private-long-project",
                project_label="acme/private-long-project",
                visibility="private",
                prompt="private legacy excerpt",
            )
            replace_session_search_chunks(connection, "private-long-chunk-session", events)
            restricted_page = search_turn_hits_raw(
                connection,
                response_marker,
                project_access=ProjectAccessContext(
                    auth_enabled=True,
                    bypass=False,
                    user_id="chunk-viewer",
                    project_roles={},
                ),
            )

        self.assertEqual(first_ids, second_ids)
        self.assertGreater(len(first_ids), 3)
        for field, page in pages.items():
            with self.subTest(field=field):
                self.assertEqual(page["total_count"], 1)
                self.assertEqual(page["items"][0]["session_id"], "long-chunk-session")
                self.assertEqual(page["items"][0]["match_source"], "chunk")
                self.assertEqual(page["items"][0]["matched_field"], field)
                self.assertEqual(page["items"][0]["chunk"]["field"], field)
                self.assertIn("marker", page["items"][0]["snippet"])
        self.assertEqual(
            {item["session_id"] for item in restricted_page["items"]},
            {"long-chunk-session"},
        )

    def test_versioned_backfill_is_bounded_and_resumable(self) -> None:
        with connect(self.db_path) as connection:
            for session_id in ("chunk-backfill-a", "chunk-backfill-b"):
                insert_search_turn(
                    connection,
                    session_id=session_id,
                    project_id="backfill-project",
                    project_key="acme/backfill",
                    project_label="acme/backfill",
                    prompt="backfill prompt",
                )

            first_count = backfill_session_search_chunks(connection, batch_size=1)
            versions_after_first = [
                int(row["search_chunk_version"] or 0)
                for row in connection.execute(
                    """
                    SELECT search_chunk_version
                    FROM sessions
                    WHERE id LIKE 'chunk-backfill-%'
                    ORDER BY id
                    """
                ).fetchall()
            ]
            second_count = backfill_session_search_chunks(connection, batch_size=1)
            final_versions = [
                int(row["search_chunk_version"] or 0)
                for row in connection.execute(
                    """
                    SELECT search_chunk_version
                    FROM sessions
                    WHERE id LIKE 'chunk-backfill-%'
                    ORDER BY id
                    """
                ).fetchall()
            ]
            final_count = backfill_session_search_chunks(connection, batch_size=1)

        self.assertEqual(first_count, 1)
        self.assertEqual(versions_after_first, [SEARCH_CHUNK_VERSION, 0])
        self.assertEqual(second_count, 1)
        self.assertEqual(final_versions, [SEARCH_CHUNK_VERSION, SEARCH_CHUNK_VERSION])
        self.assertEqual(final_count, 0)

    def test_backfill_keeps_truncation_warning_when_no_chunks_can_be_built(self) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO sessions (
                    id,
                    source_path,
                    source_root,
                    file_size,
                    file_mtime_ns,
                    summary,
                    import_warning,
                    raw_meta_json,
                    imported_at,
                    updated_at
                ) VALUES (?, ?, ?, 0, 0, ?, ?, '{}', ?, ?)
                """,
                (
                    "chunkless-warning-session",
                    "/tmp/chunkless-warning-session.jsonl",
                    "/tmp",
                    "Chunkless warning session",
                    "Search text truncated during import",
                    "2026-08-23T12:00:00+00:00",
                    "2026-08-23T12:00:00+00:00",
                ),
            )
            indexed_count = backfill_session_search_chunks(connection, batch_size=1)
            row = connection.execute(
                """
                SELECT import_warning, search_chunk_version
                FROM sessions
                WHERE id = ?
                """,
                ("chunkless-warning-session",),
            ).fetchone()

        self.assertEqual(indexed_count, 1)
        self.assertEqual(row["import_warning"], "Search text truncated during import")
        self.assertEqual(int(row["search_chunk_version"]), SEARCH_CHUNK_VERSION)

    def test_import_indexes_full_completion_message_immediately(self) -> None:
        marker = "completion-tail-marker-venus"
        full_response = ("completion response filler " * 700) + marker
        self.assertGreater(len(full_response), 12_000)
        raw_jsonl = "\n".join(
            json.dumps(record)
            for record in (
                {
                    "type": "session_meta",
                    "payload": {
                        "id": "imported-chunk-session",
                        "timestamp": "2026-08-23T12:00:00Z",
                        "cwd": "/workspace/imported-chunk",
                        "originator": "tester",
                        "cli_version": "1.0.0",
                        "source": "cli",
                        "model_provider": "openai",
                    },
                },
                {
                    "type": "event_msg",
                    "timestamp": "2026-08-23T12:00:01Z",
                    "payload": {
                        "type": "user_message",
                        "message": "Inspect the complete response.",
                    },
                },
                {
                    "type": "event_msg",
                    "timestamp": "2026-08-23T12:00:02Z",
                    "payload": {
                        "type": "turn_complete",
                        "turn_id": "turn-1",
                        "last_agent_message": full_response,
                    },
                },
            )
        )
        parsed = parse_session_text(
            raw_jsonl,
            Path("/tmp/imported-chunk-session.jsonl"),
            Path("/tmp"),
            "import-host",
        )

        with connect(self.db_path) as connection:
            with write_transaction(connection):
                upsert_parsed_session(connection, parsed)
            page = search_turn_hits_raw(connection, marker)
            version_row = connection.execute(
                "SELECT search_chunk_version, search_indexed_at FROM sessions WHERE id = ?",
                ("imported-chunk-session",),
            ).fetchone()

        self.assertEqual(int(version_row["search_chunk_version"]), SEARCH_CHUNK_VERSION)
        self.assertIsNotNone(version_row["search_indexed_at"])
        self.assertEqual(page["total_count"], 1)
        self.assertEqual(page["items"][0]["match_source"], "chunk")
        self.assertEqual(page["items"][0]["matched_field"], "response")
        self.assertIn("marker", page["items"][0]["snippet"])

    def test_full_chunk_index_retires_legacy_warning_and_covers_dashboard_search(self) -> None:
        marker = "session-tail-marker-uranus"
        full_prompt = ("oversized session search filler " * 7_500) + marker
        self.assertGreater(len(full_prompt), 200_000)
        raw_jsonl = "\n".join(
            json.dumps(record)
            for record in (
                {
                    "type": "session_meta",
                    "payload": {
                        "id": "legacy-warning-chunk-session",
                        "timestamp": "2026-08-23T12:00:00Z",
                        "cwd": "/workspace/legacy-warning",
                    },
                },
                {
                    "type": "event_msg",
                    "timestamp": "2026-08-23T12:00:01Z",
                    "payload": {
                        "type": "user_message",
                        "message": full_prompt,
                    },
                },
            )
        )
        parsed = parse_session_text(
            raw_jsonl,
            Path("/tmp/legacy-warning-chunk-session.jsonl"),
            Path("/tmp"),
            "import-host",
        )
        self.assertEqual(parsed.import_warning, "Search text truncated during import")

        with connect(self.db_path) as connection:
            with write_transaction(connection):
                upsert_parsed_session(connection, parsed)
            stored = connection.execute(
                "SELECT import_warning FROM sessions WHERE id = ?",
                ("legacy-warning-chunk-session",),
            ).fetchone()
            dashboard_rows = query_group_rows(connection, q=marker)
            api_page = search_turn_hits_raw(connection, marker)

        self.assertIsNone(stored["import_warning"])
        self.assertEqual(
            [str(row["id"]) for row in dashboard_rows],
            ["legacy-warning-chunk-session"],
        )
        self.assertEqual(api_page["total_count"], 1)
        self.assertEqual(api_page["items"][0]["match_source"], "chunk")


if __name__ == "__main__":
    unittest.main()
