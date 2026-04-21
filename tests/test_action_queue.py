from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from codex_session_viewer import SYNC_API_VERSION, __version__
from codex_session_viewer.action_queue import build_homepage_action_queue
from codex_session_viewer.config import Settings
from codex_session_viewer.db import connect, init_db, write_transaction
from codex_session_viewer.importer import parse_session_text, upsert_parsed_session
from codex_session_viewer.projects import build_grouped_projects, query_group_rows


def make_test_settings(*, data_dir: Path, session_roots: list[Path]) -> Settings:
    return Settings(
        project_root=Path.cwd(),
        environment_name="test",
        data_dir=data_dir,
        database_path=data_dir / "viewer.sqlite3",
        session_roots=session_roots,
        sync_mode="remote",
        app_version=__version__,
        sync_api_version=SYNC_API_VERSION,
        expected_agent_version=__version__,
        agent_update_command=None,
        daemon_rebuild_on_start=False,
        sync_on_start=False,
        page_size=24,
        alerts_enabled=False,
        alerts_provider="webhook",
        alerts_webhook_url=None,
        alerts_realert_minutes=60,
        alerts_send_resolutions=True,
        server_host="127.0.0.1",
        server_port=8000,
        server_base_url="http://127.0.0.1:8000",
        sync_api_token="test-token",
        sync_interval_seconds=30,
        remote_timeout_seconds=15,
        remote_batch_size=25,
        log_level="info",
        source_host="test-host",
        auth_mode="none",
        session_secret=None,
        auth_proxy_user_header="X-Forwarded-User",
        auth_proxy_name_header="X-Forwarded-Name",
        auth_proxy_email_header="X-Forwarded-Email",
        auth_proxy_login_url=None,
        auth_proxy_logout_url=None,
        auth_cookie_secure=False,
    )


def response_item(payload: dict[str, object], *, timestamp: str) -> dict[str, object]:
    return {
        "type": "response_item",
        "timestamp": timestamp,
        "payload": payload,
    }


def event_msg(payload: dict[str, object], *, timestamp: str) -> dict[str, object]:
    return {
        "type": "event_msg",
        "timestamp": timestamp,
        "payload": payload,
    }


def raw_session_jsonl(
    session_id: str,
    *,
    cwd: str,
    records: list[dict[str, object]],
    session_timestamp: str = "2026-04-20T03:19:14Z",
) -> str:
    session_meta = {
        "type": "session_meta",
        "payload": {
            "id": session_id,
            "timestamp": session_timestamp,
            "cwd": cwd,
            "originator": "tester",
            "cli_version": "1.0.0",
            "source": "cli",
            "model_provider": "openai",
        },
    }
    return "\n".join(json.dumps(record) for record in [session_meta, *records])


class ActionQueueTests(unittest.TestCase):
    def _build_action_queue(self, raw_jsonl: str, *, source_host: str = "queue-host") -> list[dict[str, object]]:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            settings = make_test_settings(
                data_dir=data_dir,
                session_roots=[Path(tmpdir) / "sessions"],
            )
            init_db(settings.database_path)

            parsed = parse_session_text(
                raw_jsonl,
                Path("/tmp/session.jsonl"),
                Path("/tmp"),
                source_host,
                file_size=len(raw_jsonl.encode("utf-8")),
                file_mtime_ns=0,
            )

            with connect(settings.database_path) as connection:
                with write_transaction(connection):
                    upsert_parsed_session(connection, parsed)
                rows = query_group_rows(connection)
                repo_groups = build_grouped_projects(rows, route_rows=rows)
                return build_homepage_action_queue(connection, rows, repo_groups)

    def test_action_queue_surfaces_failed_verification_after_patch(self) -> None:
        raw_jsonl = raw_session_jsonl(
            "verification-failed-session",
            cwd="/workspace/codex",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Update the TUI styling and make sure tests still pass.",
                    },
                    timestamp="2026-04-20T03:19:15Z",
                ),
                response_item(
                    {
                        "type": "custom_tool_call",
                        "call_id": "patch-1",
                        "name": "apply_patch",
                        "input": "*** Begin Patch\n*** Update File: codex-rs/tui/src/lib.rs\n@@\n-old\n+new\n*** End Patch\n",
                    },
                    timestamp="2026-04-20T03:19:16Z",
                ),
                event_msg(
                    {
                        "type": "patch_apply_end",
                        "call_id": "patch-1",
                        "turn_id": "turn-1",
                        "stdout": "",
                        "stderr": "",
                        "success": True,
                        "status": "completed",
                        "changes": {
                            "codex-rs/tui/src/lib.rs": {
                                "type": "update",
                                "unified_diff": "@@ -1 +1 @@\n-old\n+new\n",
                            }
                        },
                    },
                    timestamp="2026-04-20T03:19:17Z",
                ),
                response_item(
                    {
                        "type": "custom_tool_call",
                        "call_id": "cmd-1",
                        "name": "exec_command",
                        "input": {
                            "cmd": "cargo test -p codex-tui",
                            "workdir": "/workspace/codex",
                        },
                    },
                    timestamp="2026-04-20T03:19:18Z",
                ),
                event_msg(
                    {
                        "type": "exec_command_end",
                        "call_id": "cmd-1",
                        "turn_id": "turn-1",
                        "command": ["cargo", "test", "-p", "codex-tui"],
                        "cwd": "/workspace/codex",
                        "parsed_cmd": [{"type": "unknown", "cmd": "cargo test -p codex-tui"}],
                        "stdout": "",
                        "stderr": "test failure",
                        "aggregated_output": "test suite failed",
                        "exit_code": 1,
                        "duration": {"secs": 2, "nanos": 0},
                        "formatted_output": "test suite failed",
                        "status": "failed",
                    },
                    timestamp="2026-04-20T03:19:19Z",
                ),
                event_msg(
                    {
                        "type": "turn_complete",
                        "turn_id": "turn-1",
                        "last_agent_message": "Tests failed after the patch.",
                    },
                    timestamp="2026-04-20T03:19:20Z",
                ),
            ],
        )

        action_queue = self._build_action_queue(raw_jsonl)

        self.assertEqual(len(action_queue), 1)
        self.assertEqual(action_queue[0]["issue_kind"], "verification_failed")
        self.assertEqual(action_queue[0]["status_label"], "Verification failed")
        self.assertIn("cargo test -p codex-tui", action_queue[0]["status_title"])
        self.assertIn("rerun cargo test -p codex-tui", action_queue[0]["next_action"].lower())

    def test_action_queue_suppresses_exploratory_search_failures(self) -> None:
        raw_jsonl = raw_session_jsonl(
            "search-noise-session",
            cwd="/workspace/codex",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Check whether TODO markers are still present.",
                    },
                    timestamp="2026-04-20T04:00:15Z",
                ),
                response_item(
                    {
                        "type": "custom_tool_call",
                        "call_id": "cmd-1",
                        "name": "exec_command",
                        "input": {
                            "cmd": "rg TODO codex-rs/tui/src",
                            "workdir": "/workspace/codex",
                        },
                    },
                    timestamp="2026-04-20T04:00:16Z",
                ),
                event_msg(
                    {
                        "type": "exec_command_end",
                        "call_id": "cmd-1",
                        "turn_id": "turn-1",
                        "command": ["rg", "TODO", "codex-rs/tui/src"],
                        "cwd": "/workspace/codex",
                        "parsed_cmd": [
                            {
                                "type": "search",
                                "cmd": "rg TODO codex-rs/tui/src",
                                "query": "TODO",
                                "path": "codex-rs/tui/src",
                            }
                        ],
                        "stdout": "",
                        "stderr": "",
                        "aggregated_output": "",
                        "exit_code": 1,
                        "duration": {"secs": 1, "nanos": 0},
                        "formatted_output": "",
                        "status": "failed",
                    },
                    timestamp="2026-04-20T04:00:17Z",
                ),
                event_msg(
                    {
                        "type": "turn_complete",
                        "turn_id": "turn-1",
                        "last_agent_message": "No TODO markers were found.",
                    },
                    timestamp="2026-04-20T04:00:18Z",
                ),
            ],
        )

        action_queue = self._build_action_queue(raw_jsonl)

        self.assertEqual(action_queue, [])


if __name__ == "__main__":
    unittest.main()
