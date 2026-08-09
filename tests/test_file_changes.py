from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from agent_operations_viewer.db import connect, init_db
from agent_operations_viewer.projects import fetch_project_changed_files, fetch_project_file_activity
from agent_operations_viewer.turn_index import compute_session_turn_index, replace_session_turns


def patch_event(detail: dict[str, object], *, event_index: int = 2) -> dict[str, object]:
    return {
        "event_index": event_index,
        "timestamp": "2026-04-27T10:01:00+00:00",
        "record_type": "event_msg",
        "payload_type": "patch_apply_end",
        "kind": "system",
        "role": None,
        "display_text": "Patch applied",
        "detail_text": json.dumps(detail),
        "tool_name": None,
        "command_text": None,
        "exit_code": None,
        "record_json": None,
    }


def insert_indexed_session(
    connection: object,
    session_id: str,
    *,
    project_key: str,
    branch: str,
    turn_number: int = 1,
    failure_count: int = 0,
) -> None:
    connection.execute(
        """
        INSERT INTO sessions (
            id,
            source_path,
            source_root,
            file_size,
            file_mtime_ns,
            content_sha256,
            source_host,
            inferred_project_key,
            inferred_project_label,
            git_branch,
            summary,
            raw_meta_json,
            imported_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            f"/tmp/{session_id}.jsonl",
            "/tmp",
            1,
            1,
            session_id,
            "builder",
            project_key,
            "Project",
            branch,
            "Session",
            "{}",
            "2026-04-27T10:00:00+00:00",
            "2026-04-27T10:00:00+00:00",
        ),
    )
    connection.execute(
        """
        INSERT INTO session_turns (
            session_id,
            turn_number,
            prompt_excerpt,
            prompt_timestamp,
            response_excerpt,
            response_timestamp,
            response_state,
            latest_timestamp,
            patch_count,
            failure_count,
            files_touched_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            turn_number,
            "Change a file",
            "2026-04-27T10:00:00+00:00",
            "Changed a file.",
            "2026-04-27T10:02:00+00:00",
            "final",
            "2026-04-27T10:02:00+00:00",
            1,
            failure_count,
            1,
        ),
    )


class FileChangesTests(unittest.TestCase):
    def test_compute_session_turn_index_extracts_patch_file_changes(self) -> None:
        events = [
            {
                "event_index": 1,
                "timestamp": "2026-04-27T10:00:00+00:00",
                "record_type": "event_msg",
                "payload_type": "user_message",
                "kind": "message",
                "role": "user",
                "display_text": "Change the daemon sync path",
                "detail_text": "",
                "tool_name": None,
                "command_text": None,
                "exit_code": None,
                "record_json": None,
            },
            patch_event(
                {
                    "agent_daemon/remote_sync.py": {
                        "type": "update",
                        "unified_diff": "@@ -1 +1 @@\n-old\n+new\n+extra\n",
                    },
                    "agent_daemon/commands.py": {
                        "type": "add",
                        "unified_diff": "@@ -0,0 +1 @@\n+created\n",
                    },
                }
            ),
            {
                "event_index": 3,
                "timestamp": "2026-04-27T10:02:00+00:00",
                "record_type": "response_item",
                "payload_type": "message",
                "kind": "message",
                "role": "assistant",
                "display_text": "Updated sync.",
                "detail_text": "Updated sync.",
                "tool_name": None,
                "command_text": None,
                "exit_code": None,
                "record_json": json.dumps({"payload": {"phase": "final"}}),
            },
        ]

        turns = compute_session_turn_index(events)

        self.assertEqual(len(turns), 1)
        self.assertEqual(turns[0]["files_touched_count"], 2)
        self.assertEqual(
            [
                (item["path"], item["operation"], item["additions"], item["deletions"], item["hunks"])
                for item in turns[0]["file_changes"]
            ],
            [
                ("agent_daemon/commands.py", "add", 1, 0, 1),
                ("agent_daemon/remote_sync.py", "update", 2, 1, 1),
            ],
        )

    def test_compute_session_turn_index_counts_exec_command_apply_patch(self) -> None:
        events = [
            {
                "event_index": 1,
                "timestamp": "2026-04-27T10:00:00+00:00",
                "record_type": "event_msg",
                "payload_type": "user_message",
                "kind": "message",
                "role": "user",
                "display_text": "Patch a file",
                "detail_text": "",
                "tool_name": None,
                "command_text": None,
                "exit_code": None,
                "record_json": None,
            },
            {
                "event_index": 2,
                "timestamp": "2026-04-27T10:00:30+00:00",
                "record_type": "response_item",
                "payload_type": "function_call",
                "kind": "tool_call",
                "role": None,
                "tool_name": "exec_command",
                "display_text": "apply_patch <<'PATCH'\n*** Begin Patch\n*** Update File: src/app.py\n@@\n-old\n+new\n*** End Patch",
                "detail_text": "",
                "command_text": "apply_patch <<'PATCH'",
                "exit_code": None,
                "record_json": None,
            },
            patch_event(
                {
                    "src/app.py": {
                        "type": "update",
                        "unified_diff": "@@ -1 +1 @@\n-old\n+new\n",
                    }
                },
                event_index=3,
            ),
        ]

        turns = compute_session_turn_index(events)

        self.assertEqual(len(turns), 1)
        self.assertEqual(turns[0]["patch_count"], 1)
        self.assertEqual(turns[0]["files_touched_count"], 1)

    def test_replace_session_turns_writes_session_file_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "viewer.sqlite3"
            init_db(db_path)
            with connect(db_path) as connection:
                with connection:
                    connection.execute(
                        """
                        INSERT INTO sessions (
                            id,
                            source_path,
                            source_root,
                            file_size,
                            file_mtime_ns,
                            content_sha256,
                            source_host,
                            inferred_project_key,
                            inferred_project_label,
                            summary,
                            raw_meta_json,
                            imported_at,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "file-change-session",
                            "/tmp/file-change-session.jsonl",
                            "/tmp",
                            1,
                            1,
                            "abc",
                            "builder",
                            "directory:builder:/tmp/project",
                            "Project",
                            "Session",
                            "{}",
                            "2026-04-27T10:00:00+00:00",
                            "2026-04-27T10:00:00+00:00",
                        ),
                    )
                    replace_session_turns(
                        connection,
                        "file-change-session",
                        [
                            {
                                "event_index": 1,
                                "timestamp": "2026-04-27T10:00:00+00:00",
                                "record_type": "event_msg",
                                "payload_type": "user_message",
                                "kind": "message",
                                "role": "user",
                                "display_text": "Change files",
                                "detail_text": "",
                                "tool_name": None,
                                "command_text": None,
                                "exit_code": None,
                                "record_json": None,
                            },
                            patch_event(
                                {
                                    "src/app.py": {
                                        "type": "update",
                                        "unified_diff": "@@ -1 +1 @@\n-before\n+after\n",
                                    }
                                }
                            ),
                        ],
                    )

                row = connection.execute(
                    """
                    SELECT session_id, turn_number, event_index, path, operation, additions, deletions, hunks
                    FROM session_file_changes
                    WHERE session_id = ?
                    """,
                    ("file-change-session",),
                ).fetchone()

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["turn_number"], 1)
        self.assertEqual(row["event_index"], 2)
        self.assertEqual(row["path"], "src/app.py")
        self.assertEqual(row["operation"], "update")
        self.assertEqual(row["additions"], 1)
        self.assertEqual(row["deletions"], 1)
        self.assertEqual(row["hunks"], 1)

    def test_fetch_project_changed_files_filters_and_keeps_branch_facets(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "viewer.sqlite3"
            init_db(db_path)
            project_key = "directory:builder:/tmp/project"
            with connect(db_path) as connection:
                with connection:
                    insert_indexed_session(
                        connection,
                        "main-session",
                        project_key=project_key,
                        branch="main",
                        failure_count=1,
                    )
                    insert_indexed_session(
                        connection,
                        "feature-session",
                        project_key=project_key,
                        branch="feature/file-browser",
                    )
                    connection.executemany(
                        """
                        INSERT INTO session_file_changes (
                            session_id,
                            turn_number,
                            event_index,
                            path,
                            operation,
                            additions,
                            deletions,
                            hunks,
                            timestamp
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            ("main-session", 1, 2, "src/app.py", "update", 2, 1, 1, "2026-04-27T10:03:00+00:00"),
                            ("main-session", 1, 3, "src/app.py", "update", 1, 0, 1, "2026-04-27T10:04:00+00:00"),
                            ("feature-session", 1, 2, "src/app.py", "update", 5, 2, 2, "2026-04-27T10:05:00+00:00"),
                            ("feature-session", 1, 3, "docs/readme.md", "update", 1, 0, 1, "2026-04-27T10:06:00+00:00"),
                        ],
                    )

                data = fetch_project_changed_files(
                    connection,
                    group_key=project_key,
                    q="src",
                    branch="main",
                    sort="touches",
                    detail_href_override="/builder/project",
                )

        self.assertEqual(data["total_count"], 1)
        self.assertEqual(data["summary"]["failed_turn_count"], 1)
        self.assertEqual(data["items"][0]["path"], "src/app.py")
        self.assertEqual(data["items"][0]["touch_count"], 2)
        self.assertEqual(data["items"][0]["failed_turn_count"], 1)
        self.assertEqual(
            {item["branch"] for item in data["branches"]},
            {"main", "feature/file-browser"},
        )
        self.assertEqual(data["clear_branch_href"], "/builder/project/files?q=src&sort=touches")

    def test_fetch_project_file_activity_returns_turn_timeline(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "viewer.sqlite3"
            init_db(db_path)
            project_key = "directory:builder:/tmp/project"
            with connect(db_path) as connection:
                with connection:
                    insert_indexed_session(
                        connection,
                        "timeline-session",
                        project_key=project_key,
                        branch="file-view",
                    )
                    connection.execute(
                        """
                        INSERT INTO session_file_changes (
                            session_id,
                            turn_number,
                            event_index,
                            path,
                            operation,
                            additions,
                            deletions,
                            hunks,
                            timestamp
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "timeline-session",
                            1,
                            2,
                            "agent_daemon/sync.py",
                            "update",
                            7,
                            3,
                            2,
                            "2026-04-27T11:00:00+00:00",
                        ),
                    )

                activity = fetch_project_file_activity(
                    connection,
                    group_key=project_key,
                    path="agent_daemon/sync.py",
                    detail_href_override="/builder/project",
                )

        self.assertIsNotNone(activity)
        assert activity is not None
        self.assertEqual(activity["summary"]["turn_count"], 1)
        self.assertEqual(activity["summary"]["additions"], 7)
        self.assertEqual(activity["items"][0]["git_branch"], "file-view")
        self.assertEqual(
            activity["items"][0]["audit_href"],
            "/sessions/timeline-session?view=audit&turn=1&focus=1#turn-1-files",
        )


if __name__ == "__main__":
    unittest.main()
