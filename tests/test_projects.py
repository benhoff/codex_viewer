from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from agent_operations_viewer.db import connect, init_db
from agent_operations_viewer.projects import (
    build_session_signal_badges,
    count_session_turn_prompts_since,
    fetch_recent_session_turn_activity_windows,
    summarize_attention_status,
)


def insert_session(connection: object, session_id: str, *, source_path: str) -> None:
    connection.execute(
        """
        INSERT INTO sessions (
            id,
            source_path,
            source_root,
            file_size,
            file_mtime_ns,
            summary,
            raw_meta_json,
            imported_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            source_path,
            "/tmp",
            0,
            0,
            session_id,
            "{}",
            "2026-04-24T00:00:00Z",
            "2026-04-24T00:00:00Z",
        ),
    )


class ProjectTurnActivityWindowTests(unittest.TestCase):
    def test_secondary_window_counts_only_today(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(db_path)

            with connect(db_path) as connection:
                insert_session(connection, "session-a", source_path="/tmp/session-a.jsonl")
                insert_session(connection, "session-b", source_path="/tmp/session-b.jsonl")
                connection.executemany(
                    """
                    INSERT INTO session_turn_activity_daily (
                        session_id,
                        activity_date,
                        turn_count,
                        latest_timestamp
                    ) VALUES (?, ?, ?, ?)
                    """,
                    [
                        ("session-a", "2026-04-18", 4, "2026-04-18T18:00:00Z"),
                        ("session-b", "2026-04-21", 3, "2026-04-21T21:00:00Z"),
                        ("session-b", "2026-04-24", 2, "2026-04-24T09:00:00Z"),
                    ],
                )

                activity = fetch_recent_session_turn_activity_windows(
                    connection,
                    ["session-a", "session-b"],
                    "2026-04-17T00:00:00Z",
                    secondary_since_timestamp="2026-04-24T00:00:00Z",
                )

        self.assertEqual(set(activity), {"session-a", "session-b"})
        self.assertEqual(int(activity["session-a"]["turn_count"]), 4)
        self.assertEqual(int(activity["session-a"]["secondary_turn_count"]), 0)
        self.assertEqual(int(activity["session-b"]["turn_count"]), 5)
        self.assertEqual(int(activity["session-b"]["secondary_turn_count"]), 2)
        self.assertEqual(
            sum(int(item["secondary_turn_count"]) for item in activity.values()),
            2,
        )

    def test_count_session_turn_prompts_since_uses_real_prompt_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(db_path)

            with connect(db_path) as connection:
                insert_session(connection, "session-a", source_path="/tmp/session-a.jsonl")
                insert_session(connection, "session-b", source_path="/tmp/session-b.jsonl")
                connection.executemany(
                    """
                    INSERT INTO session_turns (
                        session_id,
                        turn_number,
                        prompt_timestamp
                    ) VALUES (?, ?, ?)
                    """,
                    [
                        ("session-a", 1, "2026-04-24T06:59:59Z"),
                        ("session-a", 2, "2026-04-24T07:00:00Z"),
                        ("session-b", 1, "2026-04-24T14:30:00Z"),
                    ],
                )

                count = count_session_turn_prompts_since(
                    connection,
                    ["session-a", "session-b"],
                    "2026-04-24T00:00:00-07:00",
                )

        self.assertEqual(count, 2)

    def test_build_session_signal_badges_uses_exact_viewer_warning_text(self) -> None:
        badges = build_session_signal_badges(
            {},
            viewer_warning="Search text truncated during import",
        )

        self.assertEqual(len(badges), 1)
        self.assertEqual(badges[0]["tone"], "amber")
        self.assertEqual(badges[0]["label"], "Search text truncated during import")

    def test_summarize_attention_status_uses_exact_viewer_warning_text(self) -> None:
        summary = summarize_attention_status(
            viewer_warning="Search text truncated during import",
        )

        self.assertTrue(bool(summary["has_attention"]))
        self.assertEqual(summary["status_label"], "Attention")
        self.assertEqual(summary["status_title"], "Search text truncated during import")


if __name__ == "__main__":
    unittest.main()
