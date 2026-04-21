from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from codex_session_viewer import SYNC_API_VERSION, __version__
from codex_session_viewer.action_queue import (
    build_homepage_action_queue,
    build_project_action_groups,
    build_repo_action_signal_map,
)
from codex_session_viewer.action_queue_state import set_action_queue_state
from codex_session_viewer.config import Settings
from codex_session_viewer.db import connect, init_db, write_transaction
from codex_session_viewer.importer import parse_session_text, upsert_parsed_session
from codex_session_viewer.projects import build_grouped_projects, fetch_group_detail, query_group_rows
from codex_session_viewer.session_view import build_turns


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


def patch_records(
    file_path: str,
    *,
    turn_id: str = "turn-1",
    call_id: str = "patch-1",
    timestamp_call: str,
    timestamp_result: str,
) -> list[dict[str, object]]:
    return [
        response_item(
            {
                "type": "custom_tool_call",
                "call_id": call_id,
                "name": "apply_patch",
                "input": (
                    "*** Begin Patch\n"
                    f"*** Update File: {file_path}\n"
                    "@@\n-old\n+new\n"
                    "*** End Patch\n"
                ),
            },
            timestamp=timestamp_call,
        ),
        event_msg(
            {
                "type": "patch_apply_end",
                "call_id": call_id,
                "turn_id": turn_id,
                "stdout": "",
                "stderr": "",
                "success": True,
                "status": "completed",
                "changes": {
                    file_path: {
                        "type": "update",
                        "unified_diff": "@@ -1 +1 @@\n-old\n+new\n",
                    }
                },
            },
            timestamp=timestamp_result,
        ),
    ]


def command_records(
    command: list[str],
    *,
    timestamp_call: str,
    timestamp_result: str,
    turn_id: str = "turn-1",
    call_id: str = "cmd-1",
    parsed_cmd: list[dict[str, object]] | None = None,
    workdir: str = "/workspace/codex",
    exit_code: int = 0,
    status: str = "completed",
    stdout: str = "",
    stderr: str = "",
    aggregated_output: str = "",
    formatted_output: str = "",
) -> list[dict[str, object]]:
    command_text = " ".join(command)
    if parsed_cmd is None:
        parsed_cmd = [{"type": "unknown", "cmd": command_text}]
    output = aggregated_output or formatted_output or stderr or stdout
    return [
        response_item(
            {
                "type": "custom_tool_call",
                "call_id": call_id,
                "name": "exec_command",
                "input": {
                    "cmd": command_text,
                    "workdir": workdir,
                },
            },
            timestamp=timestamp_call,
        ),
        event_msg(
            {
                "type": "exec_command_end",
                "call_id": call_id,
                "turn_id": turn_id,
                "command": command,
                "cwd": workdir,
                "parsed_cmd": parsed_cmd,
                "stdout": stdout,
                "stderr": stderr,
                "aggregated_output": output,
                "exit_code": exit_code,
                "duration": {"secs": 1, "nanos": 0},
                "formatted_output": formatted_output or output,
                "status": status,
            },
            timestamp=timestamp_result,
        ),
    ]


def turn_complete_record(message: str, *, timestamp: str, turn_id: str = "turn-1") -> dict[str, object]:
    return event_msg(
        {
            "type": "turn_complete",
            "turn_id": turn_id,
            "last_agent_message": message,
        },
        timestamp=timestamp,
    )


def turn_aborted_record(reason: str, *, timestamp: str, turn_id: str = "turn-1") -> dict[str, object]:
    return event_msg(
        {
            "type": "turn_aborted",
            "turn_id": turn_id,
            "reason": reason,
        },
        timestamp=timestamp,
    )


def mcp_tool_call_end_record(
    *,
    timestamp: str,
    call_id: str = "mcp-1",
    server: str = "codex_apps",
    tool: str = "github_fetch",
    arguments: dict[str, object] | None = None,
    result: dict[str, object] | None = None,
) -> dict[str, object]:
    return event_msg(
        {
            "type": "mcp_tool_call_end",
            "call_id": call_id,
            "invocation": {
                "server": server,
                "tool": tool,
                "arguments": arguments or {"url": "https://github.com/openai/codex"},
            },
            "duration": {"secs": 1, "nanos": 0},
            "result": result
            or {
                "Ok": {
                    "content": [
                        {
                            "type": "text",
                            "text": 'Error code: NOT_FOUNDError: GitHub API error 404: {"message":"Not Found"}',
                        }
                    ],
                    "isError": True,
                }
            },
        },
        timestamp=timestamp,
    )


def context_compacted_record(*, timestamp: str) -> dict[str, object]:
    return event_msg(
        {
            "type": "context_compacted",
        },
        timestamp=timestamp,
    )


def thread_rolled_back_record(*, timestamp: str, num_turns: int) -> dict[str, object]:
    return event_msg(
        {
            "type": "thread_rolled_back",
            "num_turns": num_turns,
        },
        timestamp=timestamp,
    )


class ActionQueueTests(unittest.TestCase):
    def _create_settings(self) -> Settings:
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        tmpdir_path = Path(tmpdir.name)
        data_dir = tmpdir_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        settings = make_test_settings(
            data_dir=data_dir,
            session_roots=[tmpdir_path / "sessions"],
        )
        init_db(settings.database_path)
        return settings

    def _ingest_sessions(
        self,
        raw_sessions: list[tuple[str, str]],
    ) -> Settings:
        settings = self._create_settings()
        with connect(settings.database_path) as connection:
            with write_transaction(connection):
                for index, (raw_jsonl, source_host) in enumerate(raw_sessions):
                    parsed = parse_session_text(
                        raw_jsonl,
                        Path(f"/tmp/session-{index}.jsonl"),
                        Path("/tmp"),
                        source_host,
                        file_size=len(raw_jsonl.encode("utf-8")),
                        file_mtime_ns=0,
                    )
                    upsert_parsed_session(connection, parsed)
        return settings

    def _load_action_queue(
        self,
        settings: Settings,
        *,
        owner_scope: str | None = None,
    ) -> list[dict[str, object]]:
        with connect(settings.database_path) as connection:
            rows = query_group_rows(connection)
            repo_groups = build_grouped_projects(rows, route_rows=rows)
            return build_homepage_action_queue(
                connection,
                rows,
                repo_groups,
                owner_scope=owner_scope,
            )

    def _load_repo_action_signals(
        self,
        settings: Settings,
        *,
        owner_scope: str | None = None,
    ) -> dict[str, dict[str, object]]:
        with connect(settings.database_path) as connection:
            rows = query_group_rows(connection)
            return build_repo_action_signal_map(
                connection,
                rows,
                owner_scope=owner_scope,
            )

    def _load_turns_for_session(
        self,
        settings: Settings,
        session_id: str,
    ) -> list[dict[str, object]]:
        with connect(settings.database_path) as connection:
            session_row = connection.execute(
                "SELECT cwd FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
            self.assertIsNotNone(session_row)
            events = connection.execute(
                "SELECT * FROM events WHERE session_id = ? ORDER BY event_index ASC",
                (session_id,),
            ).fetchall()
        return build_turns(events, cwd=str(session_row["cwd"] or "").strip() or None)

    def _load_group_detail(
        self,
        settings: Settings,
        *,
        owner_scope: str | None = None,
    ) -> dict[str, object]:
        with connect(settings.database_path) as connection:
            rows = query_group_rows(connection)
            groups = build_grouped_projects(rows, route_rows=rows)
            self.assertEqual(len(groups), 1)
            return fetch_group_detail(
                connection,
                groups[0].key,
                owner_scope=owner_scope,
                detail_href=groups[0].detail_href,
            )

    def _build_action_queue(self, raw_jsonl: str, *, source_host: str = "queue-host") -> list[dict[str, object]]:
        settings = self._ingest_sessions([(raw_jsonl, source_host)])
        return self._load_action_queue(settings)

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
                *patch_records(
                    "codex-rs/tui/src/lib.rs",
                    timestamp_call="2026-04-20T03:19:16Z",
                    timestamp_result="2026-04-20T03:19:17Z",
                ),
                *command_records(
                    ["cargo", "test", "-p", "codex-tui"],
                    timestamp_call="2026-04-20T03:19:18Z",
                    timestamp_result="2026-04-20T03:19:19Z",
                    exit_code=1,
                    status="failed",
                    stderr="test failure",
                    aggregated_output="test suite failed",
                    formatted_output="test suite failed",
                ),
                turn_complete_record(
                    "Tests failed after the patch.",
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
                *command_records(
                    ["rg", "TODO", "codex-rs/tui/src"],
                    timestamp_call="2026-04-20T04:00:16Z",
                    timestamp_result="2026-04-20T04:00:17Z",
                    parsed_cmd=[
                        {
                            "type": "search",
                            "cmd": "rg TODO codex-rs/tui/src",
                            "query": "TODO",
                            "path": "codex-rs/tui/src",
                        }
                    ],
                    exit_code=1,
                    status="failed",
                ),
                turn_complete_record(
                    "No TODO markers were found.",
                    timestamp="2026-04-20T04:00:18Z",
                ),
            ],
        )

        action_queue = self._build_action_queue(raw_jsonl)

        self.assertEqual(action_queue, [])

    def test_action_queue_honors_owner_scoped_ignore_state(self) -> None:
        raw_jsonl = raw_session_jsonl(
            "ignored-queue-session",
            cwd="/workspace/codex",
            session_timestamp="2026-04-21T05:00:14Z",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Start the dev server.",
                    },
                    timestamp="2026-04-21T05:00:15Z",
                ),
                *command_records(
                    ["uv", "run", "uvicorn", "app.main:app"],
                    timestamp_call="2026-04-21T05:00:16Z",
                    timestamp_result="2026-04-21T05:00:17Z",
                    exit_code=127,
                    status="failed",
                    stderr="uv: command not found",
                    aggregated_output="uv: command not found",
                    formatted_output="uv: command not found",
                ),
                turn_complete_record(
                    "The environment is missing uv.",
                    timestamp="2026-04-21T05:00:18Z",
                ),
            ],
        )
        settings = self._ingest_sessions([(raw_jsonl, "queue-host")])
        action_queue = self._load_action_queue(settings)

        self.assertEqual(len(action_queue), 1)
        item = action_queue[0]

        with connect(settings.database_path) as connection:
            with write_transaction(connection):
                set_action_queue_state(
                    connection,
                    owner_scope="viewer:a",
                    fingerprint=str(item["fingerprint"]),
                    project_key=str(item["project_key"]),
                    issue_kind=str(item["issue_kind"]),
                    status="ignored",
                )

        self.assertEqual(self._load_action_queue(settings, owner_scope="viewer:a"), [])
        self.assertEqual(len(self._load_action_queue(settings, owner_scope="viewer:b")), 1)

    def test_repo_action_signal_uses_actionable_label_and_next_step(self) -> None:
        raw_jsonl = raw_session_jsonl(
            "repo-verification-failed-session",
            cwd="/workspace/codex",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Update the TUI styling and make sure tests still pass.",
                    },
                    timestamp="2026-04-20T03:19:15Z",
                ),
                *patch_records(
                    "codex-rs/tui/src/lib.rs",
                    timestamp_call="2026-04-20T03:19:16Z",
                    timestamp_result="2026-04-20T03:19:17Z",
                ),
                *command_records(
                    ["cargo", "test", "-p", "codex-tui"],
                    timestamp_call="2026-04-20T03:19:18Z",
                    timestamp_result="2026-04-20T03:19:19Z",
                    exit_code=1,
                    status="failed",
                    stderr="test failure",
                    aggregated_output="test suite failed",
                    formatted_output="test suite failed",
                ),
                turn_complete_record(
                    "Tests failed after the patch.",
                    timestamp="2026-04-20T03:19:20Z",
                ),
            ],
        )

        settings = self._ingest_sessions([(raw_jsonl, "queue-host")])
        repo_signals = self._load_repo_action_signals(settings)

        self.assertEqual(len(repo_signals), 1)
        signal = next(iter(repo_signals.values()))
        self.assertEqual(signal["status_label"], "Verification failed")
        self.assertIn("exited non-zero", str(signal["status_title"]))
        self.assertIn("rerun cargo test -p codex-tui", str(signal["action_next_action"]).lower())

    def test_repo_action_signal_demotes_mismatch_only_repos_to_review_needed(self) -> None:
        raw_jsonl = raw_session_jsonl(
            "repo-mismatch-only-session",
            cwd="/workspace/codex",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Update the TUI styling and tell me whether tests passed.",
                    },
                    timestamp="2026-04-20T03:19:15Z",
                ),
                turn_complete_record(
                    "Updated the styling and tests passed.",
                    timestamp="2026-04-20T03:19:18Z",
                ),
            ],
        )

        settings = self._ingest_sessions([(raw_jsonl, "queue-host")])
        repo_signals = self._load_repo_action_signals(settings)

        self.assertEqual(len(repo_signals), 1)
        signal = next(iter(repo_signals.values()))
        self.assertEqual(signal["status_label"], "Review needed")
        self.assertEqual(signal["status_tone"], "amber")
        self.assertFalse(bool(signal["has_attention"]))
        self.assertEqual(int(signal["attention_count"]), 0)
        self.assertIn("Response claims do not match", str(signal["status_title"]))

    def test_repo_action_signal_prefers_setup_blocker_over_mismatch(self) -> None:
        mismatch_session = raw_session_jsonl(
            "repo-mismatch-session",
            cwd="/workspace/codex",
            session_timestamp="2026-04-20T03:19:14Z",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Update the TUI styling and tell me whether tests passed.",
                    },
                    timestamp="2026-04-20T03:19:15Z",
                ),
                turn_complete_record(
                    "Updated the styling and tests passed.",
                    timestamp="2026-04-20T03:19:18Z",
                ),
            ],
        )
        setup_session = raw_session_jsonl(
            "repo-setup-blocker-session",
            cwd="/workspace/codex",
            session_timestamp="2026-04-21T03:19:14Z",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Start the dev server.",
                    },
                    timestamp="2026-04-21T03:19:15Z",
                ),
                *command_records(
                    ["uv", "run", "uvicorn", "app.main:app"],
                    timestamp_call="2026-04-21T03:19:16Z",
                    timestamp_result="2026-04-21T03:19:17Z",
                    exit_code=127,
                    status="failed",
                    stderr="uv: command not found",
                    aggregated_output="uv: command not found",
                    formatted_output="uv: command not found",
                ),
                turn_complete_record(
                    "The environment is missing uv.",
                    timestamp="2026-04-21T03:19:18Z",
                ),
            ],
        )

        settings = self._ingest_sessions(
            [
                (mismatch_session, "queue-host"),
                (setup_session, "queue-host"),
            ]
        )
        repo_signals = self._load_repo_action_signals(settings)

        self.assertEqual(len(repo_signals), 1)
        signal = next(iter(repo_signals.values()))
        self.assertEqual(signal["status_label"], "Setup blocker")
        self.assertEqual(signal["status_tone"], "rose")
        self.assertTrue(bool(signal["has_attention"]))
        self.assertIn("Environment setup blocked", str(signal["status_title"]))

    def test_repo_action_signal_honors_owner_scoped_ignore_state(self) -> None:
        raw_jsonl = raw_session_jsonl(
            "repo-ignored-queue-session",
            cwd="/workspace/codex",
            session_timestamp="2026-04-21T05:00:14Z",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Start the dev server.",
                    },
                    timestamp="2026-04-21T05:00:15Z",
                ),
                *command_records(
                    ["uv", "run", "uvicorn", "app.main:app"],
                    timestamp_call="2026-04-21T05:00:16Z",
                    timestamp_result="2026-04-21T05:00:17Z",
                    exit_code=127,
                    status="failed",
                    stderr="uv: command not found",
                    aggregated_output="uv: command not found",
                    formatted_output="uv: command not found",
                ),
                turn_complete_record(
                    "The environment is missing uv.",
                    timestamp="2026-04-21T05:00:18Z",
                ),
            ],
        )
        settings = self._ingest_sessions([(raw_jsonl, "queue-host")])
        action_queue = self._load_action_queue(settings)
        self.assertEqual(len(action_queue), 1)
        item = action_queue[0]

        with connect(settings.database_path) as connection:
            with write_transaction(connection):
                set_action_queue_state(
                    connection,
                    owner_scope="viewer:a",
                    fingerprint=str(item["fingerprint"]),
                    project_key=str(item["project_key"]),
                    issue_kind=str(item["issue_kind"]),
                    status="ignored",
                )

        self.assertEqual(self._load_repo_action_signals(settings, owner_scope="viewer:a"), {})
        self.assertEqual(len(self._load_repo_action_signals(settings, owner_scope="viewer:b")), 1)

    def test_repo_action_signal_suppresses_exploratory_search_failures(self) -> None:
        raw_jsonl = raw_session_jsonl(
            "repo-search-noise-session",
            cwd="/workspace/codex",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Check whether TODO markers are still present.",
                    },
                    timestamp="2026-04-20T04:00:15Z",
                ),
                *command_records(
                    ["rg", "TODO", "codex-rs/tui/src"],
                    timestamp_call="2026-04-20T04:00:16Z",
                    timestamp_result="2026-04-20T04:00:17Z",
                    parsed_cmd=[
                        {
                            "type": "search",
                            "cmd": "rg TODO codex-rs/tui/src",
                            "query": "TODO",
                            "path": "codex-rs/tui/src",
                        }
                    ],
                    exit_code=1,
                    status="failed",
                ),
                turn_complete_record(
                    "No TODO markers were found.",
                    timestamp="2026-04-20T04:00:18Z",
                ),
            ],
        )

        settings = self._ingest_sessions([(raw_jsonl, "queue-host")])
        self.assertEqual(self._load_repo_action_signals(settings), {})

    def test_group_detail_surfaces_repo_blockers_and_health_status(self) -> None:
        raw_jsonl = raw_session_jsonl(
            "group-verification-failed-session",
            cwd="/workspace/codex",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Update the TUI styling and make sure tests still pass.",
                    },
                    timestamp="2026-04-20T03:19:15Z",
                ),
                *patch_records(
                    "codex-rs/tui/src/lib.rs",
                    timestamp_call="2026-04-20T03:19:16Z",
                    timestamp_result="2026-04-20T03:19:17Z",
                ),
                *command_records(
                    ["cargo", "test", "-p", "codex-tui"],
                    timestamp_call="2026-04-20T03:19:18Z",
                    timestamp_result="2026-04-20T03:19:19Z",
                    exit_code=1,
                    status="failed",
                    stderr="test failure",
                    aggregated_output="test suite failed",
                    formatted_output="test suite failed",
                ),
                turn_complete_record(
                    "Tests failed after the patch.",
                    timestamp="2026-04-20T03:19:20Z",
                ),
            ],
        )

        settings = self._ingest_sessions([(raw_jsonl, "queue-host")])
        detail = self._load_group_detail(settings)

        repo_blockers = detail["project_action_queue"]
        self.assertEqual(len(repo_blockers), 1)
        self.assertEqual(repo_blockers[0]["status_label"], "Verification failed")
        self.assertIn("rerun cargo test -p codex-tui", str(repo_blockers[0]["next_action"]).lower())
        self.assertEqual(detail["status_strip"]["health_label"], "Verification failed")
        self.assertIn("Verification failed after code changes", str(detail["status_strip"]["health_title"]))
        self.assertEqual(len(detail["project_action_groups"]), 1)
        self.assertEqual(detail["project_action_groups"][0]["count"], 1)

    def test_group_detail_consolidates_repeated_repo_blockers_and_uses_repo_priority_health(self) -> None:
        setup_fastapi = raw_session_jsonl(
            "group-setup-blocker-1",
            cwd="/workspace/codex-viewer",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Start the web app.",
                    },
                    timestamp="2026-04-21T05:10:15Z",
                ),
                *command_records(
                    ["python3", "-c", "import fastapi"],
                    timestamp_call="2026-04-21T05:10:16Z",
                    timestamp_result="2026-04-21T05:10:17Z",
                    exit_code=1,
                    status="failed",
                    stderr="ModuleNotFoundError: No module named 'fastapi'",
                    aggregated_output="ModuleNotFoundError: No module named 'fastapi'",
                    formatted_output="ModuleNotFoundError: No module named 'fastapi'",
                ),
                turn_complete_record(
                    "The environment is missing fastapi.",
                    timestamp="2026-04-21T05:10:18Z",
                ),
            ],
        )
        setup_httpx = raw_session_jsonl(
            "group-setup-blocker-2",
            cwd="/workspace/codex-viewer",
            session_timestamp="2026-04-21T05:12:14Z",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Start the API server.",
                    },
                    timestamp="2026-04-21T05:12:15Z",
                ),
                *command_records(
                    ["python3", "-c", "import httpx"],
                    timestamp_call="2026-04-21T05:12:16Z",
                    timestamp_result="2026-04-21T05:12:17Z",
                    exit_code=1,
                    status="failed",
                    stderr="ModuleNotFoundError: No module named 'httpx'",
                    aggregated_output="ModuleNotFoundError: No module named 'httpx'",
                    formatted_output="ModuleNotFoundError: No module named 'httpx'",
                ),
                turn_complete_record(
                    "The environment is missing httpx.",
                    timestamp="2026-04-21T05:12:18Z",
                ),
            ],
        )
        mismatch_session = raw_session_jsonl(
            "group-mismatch-session",
            cwd="/workspace/codex-viewer",
            session_timestamp="2026-04-21T05:14:14Z",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Update the app and confirm what changed.",
                    },
                    timestamp="2026-04-21T05:14:15Z",
                ),
                turn_complete_record(
                    "I updated the app entrypoint and verified the change.",
                    timestamp="2026-04-21T05:14:16Z",
                ),
            ],
        )

        settings = self._ingest_sessions(
            [
                (setup_fastapi, "queue-host"),
                (setup_httpx, "queue-host"),
                (mismatch_session, "queue-host"),
            ]
        )
        detail = self._load_group_detail(settings)

        repo_blockers = detail["project_action_queue"]
        self.assertEqual(len(repo_blockers), 3)
        self.assertEqual(detail["status_strip"]["health_label"], "Setup blocker")
        self.assertIn("unresolved repo blockers", str(detail["status_strip"]["health_title"]))

        action_groups = detail["project_action_groups"]
        self.assertEqual(len(action_groups), 2)
        self.assertEqual(action_groups[0]["issue_kind"], "setup_blocker")
        self.assertEqual(action_groups[0]["count"], 2)
        self.assertEqual(action_groups[0]["status_label"], "Setup blocker")
        self.assertIn("2 occurrences", str(action_groups[0]["summary"]))
        self.assertEqual(action_groups[1]["issue_kind"], "claim_evidence_mismatch")
        self.assertEqual(action_groups[1]["status_label"], "Review needed")

        grouped_again = build_project_action_groups(repo_blockers)
        self.assertEqual([item["issue_kind"] for item in grouped_again], [item["issue_kind"] for item in action_groups])

    def test_group_detail_limits_attention_sessions_preview(self) -> None:
        raw_sessions = []
        for index in range(4):
            raw_sessions.append(
                (
                    raw_session_jsonl(
                        f"group-attention-session-{index}",
                        cwd="/workspace/codex-viewer",
                        session_timestamp=f"2026-04-21T05:1{index}:14Z",
                        records=[
                            event_msg(
                                {
                                    "type": "user_message",
                                    "message": f"Run failing setup check {index}.",
                                },
                                timestamp=f"2026-04-21T05:1{index}:15Z",
                            ),
                            *command_records(
                                ["uv", "--version"],
                                timestamp_call=f"2026-04-21T05:1{index}:16Z",
                                timestamp_result=f"2026-04-21T05:1{index}:17Z",
                                exit_code=127,
                                status="failed",
                                stderr="uv: command not found",
                                aggregated_output="uv: command not found",
                                formatted_output="uv: command not found",
                            ),
                            turn_complete_record(
                                f"uv is missing for run {index}.",
                                timestamp=f"2026-04-21T05:1{index}:18Z",
                            ),
                        ],
                    ),
                    "queue-host",
                )
            )

        settings = self._ingest_sessions(raw_sessions)
        detail = self._load_group_detail(settings)

        self.assertEqual(len(detail["attention_sessions"]), 1)
        self.assertEqual(len(detail["attention_sessions_preview"]), 1)
        self.assertEqual(len(detail["attention_sessions_remaining"]), 0)

    def test_group_detail_repo_blockers_honor_owner_scoped_ignore_state(self) -> None:
        raw_jsonl = raw_session_jsonl(
            "group-ignored-queue-session",
            cwd="/workspace/codex",
            session_timestamp="2026-04-21T05:00:14Z",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Start the dev server.",
                    },
                    timestamp="2026-04-21T05:00:15Z",
                ),
                *command_records(
                    ["uv", "run", "uvicorn", "app.main:app"],
                    timestamp_call="2026-04-21T05:00:16Z",
                    timestamp_result="2026-04-21T05:00:17Z",
                    exit_code=127,
                    status="failed",
                    stderr="uv: command not found",
                    aggregated_output="uv: command not found",
                    formatted_output="uv: command not found",
                ),
                turn_complete_record(
                    "The environment is missing uv.",
                    timestamp="2026-04-21T05:00:18Z",
                ),
            ],
        )

        settings = self._ingest_sessions([(raw_jsonl, "queue-host")])
        detail = self._load_group_detail(settings)
        repo_blockers = detail["project_action_queue"]
        self.assertEqual(len(repo_blockers), 1)

        item = repo_blockers[0]
        with connect(settings.database_path) as connection:
            with write_transaction(connection):
                set_action_queue_state(
                    connection,
                    owner_scope="viewer:a",
                    fingerprint=str(item["fingerprint"]),
                    project_key=str(item["project_key"]),
                    issue_kind=str(item["issue_kind"]),
                    status="ignored",
                )

        self.assertEqual(self._load_group_detail(settings, owner_scope="viewer:a")["project_action_queue"], [])
        self.assertEqual(len(self._load_group_detail(settings, owner_scope="viewer:b")["project_action_queue"]), 1)

    def test_action_queue_auto_clears_verification_failure_after_later_success(self) -> None:
        failing_session = raw_session_jsonl(
            "verification-failed-session",
            cwd="/workspace/codex",
            session_timestamp="2026-04-20T03:19:14Z",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Update the TUI styling and make sure tests still pass.",
                    },
                    timestamp="2026-04-20T03:19:15Z",
                ),
                *patch_records(
                    "codex-rs/tui/src/lib.rs",
                    timestamp_call="2026-04-20T03:19:16Z",
                    timestamp_result="2026-04-20T03:19:17Z",
                ),
                *command_records(
                    ["cargo", "test", "-p", "codex-tui"],
                    timestamp_call="2026-04-20T03:19:18Z",
                    timestamp_result="2026-04-20T03:19:19Z",
                    exit_code=1,
                    status="failed",
                    stderr="test failure",
                    aggregated_output="test suite failed",
                    formatted_output="test suite failed",
                ),
                turn_complete_record(
                    "Tests failed after the patch.",
                    timestamp="2026-04-20T03:19:20Z",
                ),
            ],
        )
        successful_session = raw_session_jsonl(
            "verification-passed-session",
            cwd="/workspace/codex",
            session_timestamp="2026-04-20T04:19:14Z",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Rerun the targeted tests.",
                    },
                    timestamp="2026-04-20T04:19:15Z",
                ),
                *command_records(
                    ["cargo", "test", "-p", "codex-tui"],
                    timestamp_call="2026-04-20T04:19:16Z",
                    timestamp_result="2026-04-20T04:19:17Z",
                    exit_code=0,
                    status="completed",
                    stdout="ok",
                    aggregated_output="ok",
                    formatted_output="ok",
                ),
                turn_complete_record(
                    "The targeted tests passed.",
                    timestamp="2026-04-20T04:19:18Z",
                ),
            ],
        )

        settings = self._ingest_sessions(
            [
                (failing_session, "queue-host"),
                (successful_session, "queue-host"),
            ]
        )
        action_queue = self._load_action_queue(settings)

        self.assertEqual(action_queue, [])

    def test_action_queue_auto_clears_unverified_risky_changes_after_later_success(self) -> None:
        risky_session = raw_session_jsonl(
            "risky-session",
            cwd="/workspace/codex",
            session_timestamp="2026-04-20T06:00:14Z",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Update the CI workflow cache settings.",
                    },
                    timestamp="2026-04-20T06:00:15Z",
                ),
                *patch_records(
                    ".github/workflows/ci.yml",
                    timestamp_call="2026-04-20T06:00:16Z",
                    timestamp_result="2026-04-20T06:00:17Z",
                ),
                turn_complete_record(
                    "Updated the workflow cache settings.",
                    timestamp="2026-04-20T06:00:18Z",
                ),
            ],
        )
        successful_session = raw_session_jsonl(
            "verification-passed-session",
            cwd="/workspace/codex",
            session_timestamp="2026-04-20T07:00:14Z",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Run the repo tests.",
                    },
                    timestamp="2026-04-20T07:00:15Z",
                ),
                *command_records(
                    ["cargo", "test", "-p", "codex-tui"],
                    timestamp_call="2026-04-20T07:00:16Z",
                    timestamp_result="2026-04-20T07:00:17Z",
                    exit_code=0,
                    status="completed",
                    stdout="ok",
                    aggregated_output="ok",
                    formatted_output="ok",
                ),
                turn_complete_record(
                    "The repo tests passed.",
                    timestamp="2026-04-20T07:00:18Z",
                ),
            ],
        )

        settings = self._ingest_sessions(
            [
                (risky_session, "queue-host"),
                (successful_session, "queue-host"),
            ]
        )
        action_queue = self._load_action_queue(settings)

        self.assertEqual(action_queue, [])

    def test_action_queue_reads_materialized_rollups_without_events(self) -> None:
        raw_jsonl = raw_session_jsonl(
            "materialized-rollup-session",
            cwd="/workspace/codex",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Update the TUI styling and make sure tests still pass.",
                    },
                    timestamp="2026-04-20T07:19:15Z",
                ),
                *patch_records(
                    "codex-rs/tui/src/lib.rs",
                    timestamp_call="2026-04-20T07:19:16Z",
                    timestamp_result="2026-04-20T07:19:17Z",
                ),
                *command_records(
                    ["cargo", "test", "-p", "codex-tui"],
                    timestamp_call="2026-04-20T07:19:18Z",
                    timestamp_result="2026-04-20T07:19:19Z",
                    exit_code=1,
                    status="failed",
                    stderr="test failure",
                    aggregated_output="test suite failed",
                    formatted_output="test suite failed",
                ),
                turn_complete_record(
                    "Tests failed after the patch.",
                    timestamp="2026-04-20T07:19:20Z",
                ),
            ],
        )

        settings = self._ingest_sessions([(raw_jsonl, "queue-host")])
        with connect(settings.database_path) as connection:
            signal_rows = connection.execute(
                """
                SELECT issue_kind
                FROM action_queue_signals
                WHERE session_id = ?
                """,
                ("materialized-rollup-session",),
            ).fetchall()
            self.assertEqual([str(row["issue_kind"]) for row in signal_rows], ["verification_failed"])
            with write_transaction(connection):
                connection.execute(
                    "DELETE FROM events WHERE session_id = ?",
                    ("materialized-rollup-session",),
                )
                connection.execute(
                    "DELETE FROM session_turns WHERE session_id = ?",
                    ("materialized-rollup-session",),
                )

        action_queue = self._load_action_queue(settings)

        self.assertEqual(len(action_queue), 1)
        self.assertEqual(action_queue[0]["issue_kind"], "verification_failed")

    def test_build_turns_surfaces_failed_mcp_call_in_audit_view(self) -> None:
        raw_jsonl = raw_session_jsonl(
            "audit-mcp-call-session",
            cwd="/workspace/codex",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Fetch the repo file through MCP.",
                    },
                    timestamp="2026-04-20T07:20:15Z",
                ),
                mcp_tool_call_end_record(
                    timestamp="2026-04-20T07:20:16Z",
                    tool="github_fetch",
                    arguments={"url": "https://github.com/openai/codex/blob/main/README.md"},
                ),
                turn_complete_record(
                    "The MCP fetch failed with a 404.",
                    timestamp="2026-04-20T07:20:17Z",
                ),
            ],
        )

        settings = self._ingest_sessions([(raw_jsonl, "queue-host")])
        turns = self._load_turns_for_session(settings, "audit-mcp-call-session")

        self.assertEqual(len(turns), 1)
        turn = turns[0]
        self.assertEqual(len(turn["audit_mcp_events"]), 1)
        mcp_event = turn["audit_mcp_events"][0]
        self.assertEqual(mcp_event["server"], "codex_apps")
        self.assertEqual(mcp_event["tool"], "github_fetch")
        self.assertEqual(mcp_event["status"], "failed")
        self.assertIn("404", str(mcp_event["error_text"]))
        audit_labels = {
            str(signal.get("label") or "")
            for signal in turn.get("audit_trust_signals", [])
        }
        self.assertIn("MCP call failed", audit_labels)

    def test_build_turns_surfaces_context_compaction_and_rollback_markers(self) -> None:
        raw_jsonl = raw_session_jsonl(
            "audit-context-shift-session",
            cwd="/workspace/codex",
            records=[
                event_msg(
                    {
                        "type": "user_message",
                        "message": "Summarize what happened and continue.",
                    },
                    timestamp="2026-04-20T07:30:15Z",
                ),
                context_compacted_record(timestamp="2026-04-20T07:30:16Z"),
                thread_rolled_back_record(timestamp="2026-04-20T07:30:17Z", num_turns=2),
                turn_complete_record(
                    "Context shifted before the summary completed.",
                    timestamp="2026-04-20T07:30:18Z",
                ),
            ],
        )

        settings = self._ingest_sessions([(raw_jsonl, "queue-host")])
        turns = self._load_turns_for_session(settings, "audit-context-shift-session")

        self.assertEqual(len(turns), 1)
        turn = turns[0]
        shift_titles = [str(item.get("title") or "") for item in turn["audit_context_shift_events"]]
        self.assertEqual(
            shift_titles,
            ["Context compacted", "Thread rolled back"],
        )
        audit_labels = {
            str(signal.get("label") or "")
            for signal in turn.get("audit_trust_signals", [])
        }
        self.assertIn("Thread rolled back", audit_labels)

    def test_action_queue_labels_are_visible_in_the_audit_turn(self) -> None:
        cases = [
            (
                "Verification failed",
                raw_session_jsonl(
                    "queue-case-verification-failed",
                    cwd="/workspace/codex",
                    records=[
                        event_msg(
                            {
                                "type": "user_message",
                                "message": "Update the TUI styling and make sure tests still pass.",
                            },
                            timestamp="2026-04-20T08:00:15Z",
                        ),
                        *patch_records(
                            "codex-rs/tui/src/lib.rs",
                            timestamp_call="2026-04-20T08:00:16Z",
                            timestamp_result="2026-04-20T08:00:17Z",
                        ),
                        *command_records(
                            ["cargo", "test", "-p", "codex-tui"],
                            timestamp_call="2026-04-20T08:00:18Z",
                            timestamp_result="2026-04-20T08:00:19Z",
                            exit_code=1,
                            status="failed",
                            stderr="test failure",
                            aggregated_output="test suite failed",
                            formatted_output="test suite failed",
                        ),
                        turn_complete_record(
                            "Tests failed after the patch.",
                            timestamp="2026-04-20T08:00:20Z",
                        ),
                    ],
                ),
            ),
            (
                "Evidence mismatch",
                raw_session_jsonl(
                    "queue-case-evidence-mismatch",
                    cwd="/workspace/codex",
                    records=[
                        event_msg(
                            {
                                "type": "user_message",
                                "message": "Update the TUI styling and tell me whether tests passed.",
                            },
                            timestamp="2026-04-20T08:10:15Z",
                        ),
                        *patch_records(
                            "codex-rs/tui/src/lib.rs",
                            timestamp_call="2026-04-20T08:10:16Z",
                            timestamp_result="2026-04-20T08:10:17Z",
                        ),
                        turn_complete_record(
                            "Updated the styling and tests passed.",
                            timestamp="2026-04-20T08:10:18Z",
                        ),
                    ],
                ),
            ),
            (
                "Needs verification",
                raw_session_jsonl(
                    "queue-case-needs-verification",
                    cwd="/workspace/codex",
                    records=[
                        event_msg(
                            {
                                "type": "user_message",
                                "message": "Update the CI workflow cache settings.",
                            },
                            timestamp="2026-04-20T08:20:15Z",
                        ),
                        *patch_records(
                            ".github/workflows/ci.yml",
                            timestamp_call="2026-04-20T08:20:16Z",
                            timestamp_result="2026-04-20T08:20:17Z",
                        ),
                        turn_complete_record(
                            "Updated the workflow cache settings.",
                            timestamp="2026-04-20T08:20:18Z",
                        ),
                    ],
                ),
            ),
            (
                "Setup blocker",
                raw_session_jsonl(
                    "queue-case-setup-blocker",
                    cwd="/workspace/codex",
                    session_timestamp="2026-04-21T08:30:14Z",
                    records=[
                        event_msg(
                            {
                                "type": "user_message",
                                "message": "Start the dev server.",
                            },
                            timestamp="2026-04-21T08:30:15Z",
                        ),
                        *command_records(
                            ["uv", "run", "uvicorn", "app.main:app"],
                            timestamp_call="2026-04-21T08:30:16Z",
                            timestamp_result="2026-04-21T08:30:17Z",
                            exit_code=127,
                            status="failed",
                            stderr="uv: command not found",
                            aggregated_output="uv: command not found",
                            formatted_output="uv: command not found",
                        ),
                        turn_complete_record(
                            "The environment is missing uv.",
                            timestamp="2026-04-21T08:30:18Z",
                        ),
                    ],
                ),
            ),
            (
                "Approval blocked",
                raw_session_jsonl(
                    "queue-case-approval-blocked",
                    cwd="/workspace/codex",
                    records=[
                        event_msg(
                            {
                                "type": "user_message",
                                "message": "Edit a protected file.",
                            },
                            timestamp="2026-04-20T08:40:15Z",
                        ),
                        event_msg(
                            {
                                "type": "request_permissions",
                                "reason": "Need broader filesystem access",
                                "permissions": {"file_system": {"mode": "write"}},
                            },
                            timestamp="2026-04-20T08:40:16Z",
                        ),
                    ],
                ),
            ),
            (
                "Guardian denied",
                raw_session_jsonl(
                    "queue-case-guardian-denied",
                    cwd="/workspace/codex",
                    records=[
                        event_msg(
                            {
                                "type": "user_message",
                                "message": "Remove the temp database directory.",
                            },
                            timestamp="2026-04-20T08:50:15Z",
                        ),
                        event_msg(
                            {
                                "type": "guardian_assessment",
                                "id": "review-1",
                                "target_item_id": "item-1",
                                "turn_id": "turn-1",
                                "status": "denied",
                                "risk_level": "high",
                                "user_authorization": "low",
                                "rationale": "too risky",
                                "decision_source": "agent",
                                "action": {
                                    "type": "command",
                                    "source": "shell",
                                    "command": "rm -rf /tmp/example.sqlite",
                                    "cwd": "/tmp",
                                },
                            },
                            timestamp="2026-04-20T08:50:16Z",
                        ),
                    ],
                ),
            ),
            (
                "MCP failed",
                raw_session_jsonl(
                    "queue-case-mcp-failed",
                    cwd="/workspace/codex",
                    records=[
                        event_msg(
                            {
                                "type": "user_message",
                                "message": "List repo pull requests.",
                            },
                            timestamp="2026-04-20T09:00:15Z",
                        ),
                        event_msg(
                            {
                                "type": "mcp_startup_complete",
                                "ready": [],
                                "failed": [{"server": "github", "error": "missing token"}],
                                "cancelled": [],
                            },
                            timestamp="2026-04-20T09:00:16Z",
                        ),
                    ],
                ),
            ),
            (
                "Interrupted",
                raw_session_jsonl(
                    "queue-case-interrupted",
                    cwd="/workspace/codex",
                    records=[
                        event_msg(
                            {
                                "type": "user_message",
                                "message": "Update the CI workflow and keep going.",
                            },
                            timestamp="2026-04-20T09:10:15Z",
                        ),
                        *patch_records(
                            ".github/workflows/ci.yml",
                            timestamp_call="2026-04-20T09:10:16Z",
                            timestamp_result="2026-04-20T09:10:17Z",
                        ),
                        turn_aborted_record(
                            "interrupted",
                            timestamp="2026-04-20T09:10:18Z",
                        ),
                    ],
                ),
            ),
        ]

        for expected_label, raw_jsonl in cases:
            with self.subTest(label=expected_label):
                settings = self._ingest_sessions([(raw_jsonl, "queue-host")])
                action_queue = self._load_action_queue(settings)
                matching_items = [item for item in action_queue if item["status_label"] == expected_label]

                self.assertTrue(matching_items, f"Expected {expected_label!r} in action queue")
                item = matching_items[0]

                turns = self._load_turns_for_session(settings, str(item["session_id"]))
                matching_turn = next(
                    turn for turn in turns if int(turn["number"]) == int(item["turn_number"])
                )
                audit_labels = {
                    str(signal.get("label") or "")
                    for signal in matching_turn.get("audit_trust_signals", [])
                }
                self.assertIn(expected_label, audit_labels)


if __name__ == "__main__":
    unittest.main()
