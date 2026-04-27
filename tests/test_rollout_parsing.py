from __future__ import annotations

import asyncio
import gzip
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import unittest
from unittest import mock

from agent_operations_viewer import SYNC_API_VERSION, __version__
from agent_operations_viewer.git_utils import infer_project_identity
from agent_operations_viewer.config import Settings
from agent_operations_viewer.db import connect, init_db, write_transaction
from agent_operations_viewer.importer import (
    normalize_event,
    normalized_event_to_dict,
    parse_session_text,
    parsed_session_from_payload,
    parsed_session_to_payload,
    prescan_session_source,
    sync_sessions,
    upsert_parsed_session,
)
from agent_operations_viewer.file_watch import SessionFileWatcher
from agent_operations_viewer.remote_sync import RemoteSyncError, json_request, sync_sessions_remote
from agent_operations_viewer.projects import (
    build_project_access_context,
    effective_project_fields,
    fetch_turn_stream,
    fetch_group_detail,
)
from agent_operations_viewer.session_status import is_task_complete, terminal_turn_summary
from agent_operations_viewer.session_artifacts import store_session_artifact
from agent_operations_viewer.session_view import build_turns
from agent_operations_viewer.web.routes.sync_api import (
    _read_json_request_payload,
    store_raw_sync_session_tail,
    store_raw_sync_sessions_batch,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
CLAUDE_FIXTURES_DIR = FIXTURES_DIR / "claude"


def parse_claude_fixture(relative_path: str) -> object:
    fixture_path = CLAUDE_FIXTURES_DIR / "projects" / relative_path
    raw_jsonl = fixture_path.read_text(encoding="utf-8")
    return parse_session_text(
        raw_jsonl,
        fixture_path,
        CLAUDE_FIXTURES_DIR / "projects",
        "test-host",
        file_size=len(raw_jsonl.encode("utf-8")),
        file_mtime_ns=0,
    )


def concise_event_view(event: object) -> dict[str, object]:
    event_dict = normalized_event_to_dict(event)
    return {
        "kind": event_dict["kind"],
        "role": event_dict["role"],
        "tool_name": event_dict["tool_name"],
        "call_id": event_dict["call_id"],
        "command_text": event_dict["command_text"],
        "exit_code": event_dict["exit_code"],
        "display_text": event_dict["display_text"],
        "detail_text": event_dict["detail_text"],
    }


def make_record_json(payload: dict[str, object], *, record_type: str = "response_item") -> str:
    return json.dumps({"type": record_type, "payload": payload})


def message_event(
    *,
    event_index: int,
    role: str,
    text: str,
    phase: str | None = None,
    timestamp: str = "2026-04-20T03:19:14Z",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "type": "message",
        "role": role,
        "content": [{"type": "output_text", "text": text}],
    }
    if phase is not None:
        payload["phase"] = phase
    return {
        "event_index": event_index,
        "timestamp": timestamp,
        "record_type": "response_item",
        "payload_type": "message",
        "kind": "message",
        "role": role,
        "title": "Assistant Message" if role == "assistant" else "User Message",
        "display_text": text,
        "detail_text": text,
        "tool_name": None,
        "call_id": None,
        "command_text": None,
        "exit_code": None,
        "record_json": make_record_json(payload),
    }


def make_raw_session_jsonl(
    session_id: str,
    *,
    cwd: str = "/workspace/repo",
    user_message: str | None = None,
) -> str:
    return "\n".join(
        [
            json.dumps(
                {
                    "type": "session_meta",
                    "payload": {
                        "id": session_id,
                        "timestamp": "2026-04-20T03:19:14Z",
                        "cwd": cwd,
                        "originator": "tester",
                        "cli_version": "1.0.0",
                        "source": "cli",
                        "model_provider": "openai",
                    },
                }
            ),
            json.dumps(
                {
                    "type": "event_msg",
                    "timestamp": "2026-04-20T03:19:15Z",
                    "payload": {
                        "type": "user_message",
                        "message": user_message or f"Investigate {session_id}.",
                    },
                }
            ),
            json.dumps(
                {
                    "type": "event_msg",
                    "timestamp": "2026-04-20T03:19:16Z",
                    "payload": {
                        "type": "token_count",
                        "info": {
                            "model_context_window": 128000,
                            "total_token_usage": {
                                "input_tokens": 1000,
                                "cached_input_tokens": 0,
                                "output_tokens": 100,
                                "reasoning_output_tokens": 0,
                                "total_tokens": 1100,
                            },
                            "last_token_usage": {
                                "input_tokens": 1000,
                                "cached_input_tokens": 0,
                                "output_tokens": 100,
                                "reasoning_output_tokens": 0,
                                "total_tokens": 1100,
                            },
                        },
                    },
                }
            ),
        ]
    )


def make_claude_transcript_jsonl(
    session_id: str,
    *,
    cwd: str = "/workspace/repo",
    command: str = "pytest -q",
    include_custom_title: bool = False,
    persisted_output_path: str | None = None,
) -> str:
    lines: list[str] = []
    if include_custom_title:
        lines.append(
            json.dumps(
                {
                    "type": "custom-title",
                    "sessionId": session_id,
                    "customTitle": "Mapped Claude Session",
                }
            )
        )

    lines.extend(
        [
            json.dumps(
                {
                    "type": "user",
                    "uuid": "user-1",
                    "timestamp": "2026-04-20T03:19:14Z",
                    "message": {
                        "role": "user",
                        "content": "Investigate the failing test run.",
                    },
                    "cwd": cwd,
                    "userType": "human",
                    "sessionId": session_id,
                    "version": "1.2.3",
                    "parentUuid": None,
                    "isSidechain": False,
                }
            ),
            json.dumps(
                {
                    "type": "assistant",
                    "uuid": "assistant-1",
                    "timestamp": "2026-04-20T03:19:15Z",
                    "message": {
                        "id": "message-1",
                        "role": "assistant",
                        "model": "claude-sonnet-4-5",
                        "type": "message",
                        "stop_reason": "tool_use",
                        "stop_sequence": None,
                        "container": None,
                        "context_management": None,
                        "usage": {
                            "input_tokens": 100,
                            "output_tokens": 20,
                            "cache_creation_input_tokens": 0,
                            "cache_read_input_tokens": 0,
                        },
                        "content": [
                            {"type": "text", "text": "I ran the test suite."},
                            {
                                "type": "tool_use",
                                "id": "tool-1",
                                "name": "Bash",
                                "input": {
                                    "command": command,
                                    "description": "Run the failing test suite",
                                },
                            },
                        ],
                    },
                    "cwd": cwd,
                    "userType": "human",
                    "sessionId": session_id,
                    "version": "1.2.3",
                    "parentUuid": "user-1",
                    "isSidechain": False,
                }
            ),
            json.dumps(
                {
                    "type": "user",
                    "uuid": "user-2",
                    "timestamp": "2026-04-20T03:19:16Z",
                    "message": {
                        "role": "user",
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": "tool-1",
                                "content": (
                                    (
                                        "<persisted-output>\n"
                                        f"Output too large (4 KB). Full output saved to: {persisted_output_path}\n\n"
                                        "Preview (first 2 KB):\nFAILED\n...\n"
                                        "</persisted-output>"
                                    )
                                    if persisted_output_path
                                    else "<bash-stdout>FAILED</bash-stdout><bash-stderr>boom</bash-stderr>"
                                ),
                                "is_error": True,
                            }
                        ],
                    },
                    "toolUseResult": (
                        {
                            "persistedOutputPath": persisted_output_path,
                            "interrupted": False,
                        }
                        if persisted_output_path
                        else {
                            "stdout": "FAILED",
                            "stderr": "boom",
                            "interrupted": False,
                        }
                    ),
                    "isMeta": True,
                    "cwd": cwd,
                    "userType": "human",
                    "sessionId": session_id,
                    "version": "1.2.3",
                    "parentUuid": "assistant-1",
                    "isSidechain": False,
                }
            ),
            json.dumps(
                {
                    "type": "assistant",
                    "uuid": "assistant-2",
                    "timestamp": "2026-04-20T03:19:17Z",
                    "message": {
                        "id": "message-2",
                        "role": "assistant",
                        "model": "claude-sonnet-4-5",
                        "type": "message",
                        "stop_reason": "end_turn",
                        "stop_sequence": None,
                        "container": None,
                        "context_management": None,
                        "usage": {
                            "input_tokens": 140,
                            "output_tokens": 30,
                            "cache_creation_input_tokens": 0,
                            "cache_read_input_tokens": 10,
                        },
                        "content": [
                            {"type": "text", "text": "The suite fails in test_login_timeout."}
                        ],
                    },
                    "cwd": cwd,
                    "userType": "human",
                    "sessionId": session_id,
                    "version": "1.2.3",
                    "parentUuid": "user-2",
                    "isSidechain": False,
                }
            ),
        ]
    )
    return "\n".join(lines)


def make_claude_streaming_transcript_jsonl(session_id: str, *, cwd: str = "/workspace/repo") -> str:
    return "\n".join(
        [
            json.dumps(
                {
                    "type": "user",
                    "uuid": "user-1",
                    "timestamp": "2026-04-20T03:19:14Z",
                    "message": {
                        "role": "user",
                        "content": "Summarize the current blocker.",
                    },
                    "cwd": cwd,
                    "userType": "human",
                    "sessionId": session_id,
                    "version": "1.2.3",
                    "parentUuid": None,
                    "isSidechain": False,
                }
            ),
            json.dumps(
                {
                    "type": "assistant",
                    "uuid": "assistant-1",
                    "timestamp": "2026-04-20T03:19:15Z",
                    "message": {
                        "id": "message-1",
                        "role": "assistant",
                        "model": "claude-sonnet-4-5",
                        "type": "message",
                        "stop_reason": None,
                        "usage": {
                            "input_tokens": 100,
                            "output_tokens": 10,
                            "cache_creation_input_tokens": 0,
                            "cache_read_input_tokens": 0,
                        },
                        "content": [
                            {"type": "text", "text": "Scanning the recent failures..."},
                        ],
                    },
                    "cwd": cwd,
                    "userType": "human",
                    "sessionId": session_id,
                    "version": "1.2.3",
                    "parentUuid": "user-1",
                    "isSidechain": False,
                }
            ),
            json.dumps(
                {
                    "type": "assistant",
                    "uuid": "assistant-1",
                    "timestamp": "2026-04-20T03:19:16Z",
                    "message": {
                        "id": "message-1",
                        "role": "assistant",
                        "model": "claude-sonnet-4-5",
                        "type": "message",
                        "stop_reason": "end_turn",
                        "usage": {
                            "input_tokens": 140,
                            "output_tokens": 22,
                            "cache_creation_input_tokens": 0,
                            "cache_read_input_tokens": 5,
                        },
                        "content": [
                            {"type": "text", "text": "The blocker is a missing environment dependency."},
                        ],
                    },
                    "cwd": cwd,
                    "userType": "human",
                    "sessionId": session_id,
                    "version": "1.2.3",
                    "parentUuid": "user-1",
                    "isSidechain": False,
                }
            ),
        ]
    )


def make_claude_sidechain_jsonl(
    session_id: str,
    agent_id: str,
    *,
    cwd: str = "/workspace/repo",
    first_prompt: str = "Investigate the flaky test.",
    assistant_text: str | None = "The issue is a missing retry.",
) -> str:
    lines = [
        json.dumps(
            {
                "type": "user",
                "uuid": "user-1",
                "timestamp": "2026-04-20T03:19:14Z",
                "message": {
                    "role": "user",
                    "content": first_prompt,
                },
                "cwd": cwd,
                "userType": "human",
                "sessionId": session_id,
                "version": "1.2.3",
                "parentUuid": None,
                "isSidechain": True,
                "agentId": agent_id,
            }
        )
    ]
    if assistant_text is not None:
        lines.append(
            json.dumps(
                {
                    "type": "assistant",
                    "uuid": "assistant-1",
                    "timestamp": "2026-04-20T03:19:15Z",
                    "message": {
                        "id": "message-1",
                        "role": "assistant",
                        "model": "claude-sonnet-4-5",
                        "type": "message",
                        "stop_reason": "end_turn",
                        "usage": {
                            "input_tokens": 60,
                            "output_tokens": 20,
                            "cache_creation_input_tokens": 0,
                            "cache_read_input_tokens": 0,
                        },
                        "content": [
                            {"type": "text", "text": assistant_text},
                        ],
                    },
                    "cwd": cwd,
                    "userType": "human",
                    "sessionId": session_id,
                    "version": "1.2.3",
                    "parentUuid": "user-1",
                    "isSidechain": True,
                    "agentId": agent_id,
                }
            )
        )
    return "\n".join(lines)


def make_test_settings(
    *,
    data_dir: Path,
    session_roots: list[Path],
    source_host: str = "test-host",
    remote_batch_size: int = 25,
) -> Settings:
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
        remote_batch_size=remote_batch_size,
        log_level="info",
        source_host=source_host,
        auth_mode="none",
        session_secret=None,
        auth_proxy_user_header="X-Forwarded-User",
        auth_proxy_name_header="X-Forwarded-Name",
        auth_proxy_email_header="X-Forwarded-Email",
        auth_proxy_login_url=None,
        auth_proxy_logout_url=None,
        auth_cookie_secure=False,
    )


class RolloutParsingTests(unittest.TestCase):
    def test_parse_session_text_supports_claude_transcripts(self) -> None:
        raw_jsonl = make_claude_transcript_jsonl("claude-session")

        parsed = parse_session_text(
            raw_jsonl,
            Path("/tmp/claude-session.jsonl"),
            Path("/tmp"),
            "test-host",
            file_size=len(raw_jsonl.encode("utf-8")),
            file_mtime_ns=0,
        )

        self.assertEqual(parsed.session_id, "claude-session")
        self.assertEqual(parsed.source, "claude-code")
        self.assertEqual(parsed.model_provider, "anthropic")
        self.assertEqual(parsed.turn_count, 1)
        self.assertEqual(parsed.command_failure_count, 1)
        self.assertEqual(parsed.latest_input_tokens, 140)
        self.assertEqual(parsed.latest_cached_input_tokens, 10)
        self.assertEqual(parsed.latest_output_tokens, 30)
        self.assertEqual(parsed.latest_total_tokens, 170)

        tool_call = next((event for event in parsed.events if event.kind == "tool_call"), None)
        self.assertIsNotNone(tool_call)
        self.assertEqual(tool_call.tool_name, "exec_command")

        command_event = next((event for event in parsed.events if event.kind == "command"), None)
        self.assertIsNotNone(command_event)
        self.assertEqual(command_event.command_text, "pytest -q")
        self.assertEqual(command_event.exit_code, 1)
        self.assertIn("FAILED", command_event.detail_text)

    def test_parse_session_text_collapses_claude_assistant_fragments(self) -> None:
        raw_jsonl = make_claude_streaming_transcript_jsonl("claude-stream")

        parsed = parse_session_text(
            raw_jsonl,
            Path("/tmp/claude-stream.jsonl"),
            Path("/tmp"),
            "test-host",
            file_size=len(raw_jsonl.encode("utf-8")),
            file_mtime_ns=0,
        )

        assistant_messages = [event for event in parsed.events if event.kind == "message" and event.role == "assistant"]
        self.assertEqual(len(assistant_messages), 1)
        self.assertEqual(assistant_messages[0].display_text, "The blocker is a missing environment dependency.")
        self.assertEqual(parsed.latest_input_tokens, 140)
        self.assertEqual(parsed.latest_cached_input_tokens, 5)
        self.assertEqual(parsed.latest_output_tokens, 22)

    def test_parse_session_text_resolves_claude_persisted_command_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            transcript_path = Path(tmp) / "claude-session.jsonl"
            persisted_output_path = Path(tmp) / "tool-output.txt"
            persisted_output_path.write_text("full persisted output\nwith context", encoding="utf-8")
            raw_jsonl = make_claude_transcript_jsonl(
                "claude-session",
                persisted_output_path=str(persisted_output_path),
            )

            parsed = parse_session_text(
                raw_jsonl,
                transcript_path,
                Path(tmp),
                "test-host",
                file_size=len(raw_jsonl.encode("utf-8")),
                file_mtime_ns=0,
            )

        command_event = next((event for event in parsed.events if event.kind == "command"), None)
        self.assertIsNotNone(command_event)
        self.assertIn("full persisted output", command_event.detail_text)
        self.assertIn("with context", command_event.detail_text)

    def test_parse_session_text_assigns_unique_ids_to_claude_sidechains(self) -> None:
        raw_jsonl = make_claude_sidechain_jsonl("claude-parent", "researcher@ops")

        parsed = parse_session_text(
            raw_jsonl,
            Path("/tmp/claude-projects/sanitized-project/claude-parent/subagents/agent-researcher@ops.jsonl"),
            Path("/tmp/claude-projects"),
            "test-host",
            file_size=len(raw_jsonl.encode("utf-8")),
            file_mtime_ns=0,
        )

        self.assertEqual(parsed.session_id, "claude-parent:agent:researcher@ops")
        self.assertEqual(parsed.forked_from_id, "claude-parent")
        self.assertEqual(parsed.agent_nickname, "researcher")
        self.assertEqual(parsed.source, "claude-code")

    def test_sync_sessions_imports_claude_transcript_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            session_root = root / "claude-projects"
            project_dir = session_root / "sanitized-project"
            project_dir.mkdir(parents=True)
            transcript_path = project_dir / "claude-session.jsonl"
            transcript_path.write_text(
                make_claude_transcript_jsonl("claude-session", include_custom_title=True),
                encoding="utf-8",
            )

            data_dir = root / "data"
            data_dir.mkdir()
            settings = make_test_settings(
                data_dir=data_dir,
                session_roots=[session_root],
                source_host="claude-host",
            )
            init_db(settings.database_path)

            stats = sync_sessions(settings)

            self.assertEqual(stats["imported"], 1)
            self.assertEqual(stats["updated"], 0)
            self.assertEqual(stats["skipped"], 0)

            with connect(settings.database_path) as connection:
                row = connection.execute(
                    "SELECT id, source, summary, raw_artifact_sha256 FROM sessions WHERE id = ?",
                    ("claude-session",),
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["source"], "claude-code")
                self.assertIn("Investigate the failing test run.", row["summary"])
                self.assertTrue(str(row["raw_artifact_sha256"] or "").strip())

    def test_sync_sessions_skips_claude_warmup_sidechains(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            session_root = root / "claude-projects"
            project_dir = session_root / "sanitized-project"
            warmup_dir = project_dir / "claude-session" / "subagents"
            warmup_dir.mkdir(parents=True)
            (project_dir / "claude-session.jsonl").write_text(
                make_claude_transcript_jsonl("claude-session"),
                encoding="utf-8",
            )
            (warmup_dir / "agent-warmup@ops.jsonl").write_text(
                make_claude_sidechain_jsonl(
                    "claude-session",
                    "warmup@ops",
                    first_prompt="Warmup",
                    assistant_text=None,
                ),
                encoding="utf-8",
            )

            data_dir = root / "data"
            data_dir.mkdir()
            settings = make_test_settings(
                data_dir=data_dir,
                session_roots=[session_root],
                source_host="claude-host",
            )
            init_db(settings.database_path)

            stats = sync_sessions(settings)

            self.assertEqual(stats["imported"], 1)
            self.assertEqual(stats["skipped"], 1)
            with connect(settings.database_path) as connection:
                count = connection.execute("SELECT COUNT(*) AS count FROM sessions").fetchone()
                self.assertEqual(int(count["count"]), 1)

    def test_sync_sessions_imports_claude_fixture_projects(self) -> None:
        fixture_projects = CLAUDE_FIXTURES_DIR / "projects"
        self.assertTrue(fixture_projects.exists(), fixture_projects)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            session_root = root / "claude-projects"
            shutil.copytree(fixture_projects, session_root)

            data_dir = root / "data"
            data_dir.mkdir()
            settings = make_test_settings(
                data_dir=data_dir,
                session_roots=[session_root],
                source_host="claude-host",
            )
            init_db(settings.database_path)

            stats = sync_sessions(settings)

            self.assertEqual(stats["imported"], 6)
            self.assertEqual(stats["updated"], 0)
            self.assertEqual(stats["skipped"], 1)

            with connect(settings.database_path) as connection:
                rows = connection.execute(
                    "SELECT id, source, forked_from_id, source_path FROM sessions ORDER BY id ASC"
                ).fetchall()
                ids = [str(row["id"]) for row in rows]
                self.assertEqual(
                    ids,
                    [
                        "persist-session",
                        "public-stream",
                        "test-session",
                        "test-session:agent:child01",
                        "test-session:agent:orch001",
                        "warmup-parent",
                    ],
                )
                child_row = next(row for row in rows if str(row["id"]) == "test-session:agent:child01")
                orch_row = next(row for row in rows if str(row["id"]) == "test-session:agent:orch001")
                self.assertEqual(str(child_row["forked_from_id"] or ""), "test-session")
                self.assertEqual(str(orch_row["forked_from_id"] or ""), "test-session")
                self.assertIn("nested-skill/test-session/subagents/agent-child01.jsonl", str(child_row["source_path"]))

    def test_parse_session_text_reads_claude_persisted_output_fixture(self) -> None:
        fixture_path = (
            CLAUDE_FIXTURES_DIR
            / "projects"
            / "persisted-output"
            / "persist-session.jsonl"
        )
        raw_jsonl = fixture_path.read_text(encoding="utf-8")

        parsed = parse_session_text(
            raw_jsonl,
            fixture_path,
            CLAUDE_FIXTURES_DIR / "projects",
            "test-host",
            file_size=len(raw_jsonl.encode("utf-8")),
            file_mtime_ns=0,
        )

        command_event = next((event for event in parsed.events if event.kind == "command"), None)
        self.assertIsNotNone(command_event)
        self.assertIn("full fixture output", command_event.detail_text)
        self.assertIn("line two", command_event.detail_text)

    def test_parse_session_text_reads_public_claude_streaming_fixture(self) -> None:
        fixture_path = (
            CLAUDE_FIXTURES_DIR
            / "projects"
            / "public-streaming"
            / "public-stream.jsonl"
        )
        raw_jsonl = fixture_path.read_text(encoding="utf-8")

        parsed = parse_session_text(
            raw_jsonl,
            fixture_path,
            CLAUDE_FIXTURES_DIR / "projects",
            "test-host",
            file_size=len(raw_jsonl.encode("utf-8")),
            file_mtime_ns=0,
        )

        self.assertEqual(parsed.session_id, "public-stream")
        self.assertEqual(parsed.user_message_count, 1)
        self.assertEqual(parsed.latest_output_tokens, 402)

        reasoning_event = next((event for event in parsed.events if event.kind == "reasoning"), None)
        self.assertIsNotNone(reasoning_event)
        self.assertIn("uv-tdd skill", reasoning_event.detail_text)

        tool_call = next((event for event in parsed.events if event.kind == "tool_call"), None)
        self.assertIsNotNone(tool_call)
        self.assertEqual(tool_call.tool_name, "Skill")

    def test_sync_sessions_skips_online_warmup_fixture(self) -> None:
        fixture_projects = CLAUDE_FIXTURES_DIR / "projects"

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            session_root = root / "claude-projects"
            shutil.copytree(fixture_projects / "warmup-issue", session_root / "warmup-issue")

            data_dir = root / "data"
            data_dir.mkdir()
            settings = make_test_settings(
                data_dir=data_dir,
                session_roots=[session_root],
                source_host="claude-host",
            )
            init_db(settings.database_path)

            stats = sync_sessions(settings)

            self.assertEqual(stats["imported"], 1)
            self.assertEqual(stats["skipped"], 1)
            with connect(settings.database_path) as connection:
                rows = connection.execute(
                    "SELECT id FROM sessions ORDER BY id ASC"
                ).fetchall()
                self.assertEqual([str(row["id"]) for row in rows], ["warmup-parent"])

    def test_parse_session_text_matches_claude_fixture_golden_events(self) -> None:
        fixtures = [
            {
                "relative_path": "persisted-output/persist-session.jsonl",
                "session_id": "persist-session",
                "forked_from_id": None,
                "agent_nickname": None,
                "latest_output_tokens": 18,
                "events": [
                    {
                        "kind": "message",
                        "role": "user",
                        "display_text": "Show me the installer output.",
                    },
                    {
                        "kind": "tool_call",
                        "tool_name": "exec_command",
                        "call_id": "toolu_bash1",
                        "command_text": "npm install",
                        "display_text": "npm install\nin install",
                    },
                    {
                        "kind": "telemetry",
                        "display_text": "Token usage: 145 total, 120 in, 25 out",
                    },
                    {
                        "kind": "command",
                        "call_id": "toolu_bash1",
                        "command_text": "npm install",
                        "exit_code": 0,
                        "display_text": "npm install",
                        "detail_text": "full fixture output\nline two\nline three",
                    },
                    {
                        "kind": "message",
                        "role": "assistant",
                        "display_text": "The installer completed without errors.",
                    },
                    {
                        "kind": "telemetry",
                        "display_text": "Token usage: 198 total, 180 in, 18 out",
                    },
                ],
            },
            {
                "relative_path": "public-streaming/public-stream.jsonl",
                "session_id": "public-stream",
                "forked_from_id": None,
                "agent_nickname": None,
                "latest_output_tokens": 402,
                "events": [
                    {
                        "kind": "message",
                        "role": "user",
                        "display_text": "Use the uv-tdd skill to build a small HTTP proxy that adds response-time headers.",
                    },
                    {
                        "kind": "reasoning",
                        "display_text": "The uv-tdd skill is the right tool for this proxy task.",
                    },
                    {
                        "kind": "tool_call",
                        "tool_name": "Skill",
                        "call_id": "toolu_public_skill",
                        "display_text": "{\n  \"args\": \"Build a small HTTP proxy that adds x-proxied-by and x-response-time-microseconds headers.\",\n  \"skill\": \"uv-tdd\"\n}",
                    },
                    {
                        "kind": "telemetry",
                        "display_text": "Token usage: 5,754 total, 9 in, 402 out",
                    },
                ],
            },
            {
                "relative_path": "nested-skill/test-session.jsonl",
                "session_id": "test-session",
                "forked_from_id": None,
                "agent_nickname": None,
                "latest_output_tokens": 50,
                "events": [
                    {
                        "kind": "message",
                        "role": "user",
                        "display_text": "run pir",
                    },
                    {
                        "kind": "message",
                        "role": "assistant",
                        "display_text": "Running PIR...",
                    },
                    {
                        "kind": "telemetry",
                        "display_text": "Token usage: 150 total, 100 in, 50 out",
                    },
                ],
            },
            {
                "relative_path": "nested-skill/test-session/subagents/agent-orch001.jsonl",
                "session_id": "test-session:agent:orch001",
                "forked_from_id": "test-session",
                "agent_nickname": "orch001",
                "latest_output_tokens": 30,
                "events": [
                    {
                        "kind": "message",
                        "role": "user",
                        "display_text": "Base directory for this skill: /skills/pir\n\n# Post Incident Record",
                    },
                    {
                        "kind": "tool_call",
                        "tool_name": "Skill",
                        "call_id": "toolu_skill_pg",
                        "display_text": "{\n  \"args\": \"last 24h\",\n  \"skill\": \"pagerduty-oncall\"\n}",
                    },
                    {
                        "kind": "telemetry",
                        "display_text": "Token usage: 230 total, 200 in, 30 out",
                    },
                    {
                        "kind": "tool_result",
                        "call_id": "toolu_skill_pg",
                        "display_text": "Skill completed.",
                    },
                ],
            },
            {
                "relative_path": "nested-skill/test-session/subagents/agent-child01.jsonl",
                "session_id": "test-session:agent:child01",
                "forked_from_id": "test-session",
                "agent_nickname": "child01",
                "latest_output_tokens": 10,
                "events": [
                    {
                        "kind": "message",
                        "role": "user",
                        "display_text": "Investigate PagerDuty incidents for the past 24 hours",
                    },
                    {
                        "kind": "tool_call",
                        "tool_name": "exec_command",
                        "call_id": "toolu_bash1",
                        "command_text": "pd incident list",
                        "display_text": "pd incident list\nin pir",
                    },
                    {
                        "kind": "telemetry",
                        "display_text": "Token usage: 120 total, 100 in, 20 out",
                    },
                    {
                        "kind": "command",
                        "call_id": "toolu_bash1",
                        "command_text": "pd incident list",
                        "exit_code": 0,
                        "display_text": "pd incident list",
                        "detail_text": "No incidents found.",
                    },
                    {
                        "kind": "message",
                        "role": "assistant",
                        "display_text": "No PagerDuty incidents.",
                    },
                    {
                        "kind": "telemetry",
                        "display_text": "Token usage: 160 total, 150 in, 10 out",
                    },
                ],
            },
        ]

        for fixture in fixtures:
            with self.subTest(fixture=fixture["relative_path"]):
                parsed = parse_claude_fixture(str(fixture["relative_path"]))
                self.assertEqual(parsed.source, "claude-code")
                self.assertEqual(parsed.session_id, fixture["session_id"])
                self.assertEqual(parsed.forked_from_id, fixture["forked_from_id"])
                self.assertEqual(parsed.agent_nickname, fixture["agent_nickname"])
                self.assertEqual(parsed.latest_output_tokens, fixture["latest_output_tokens"])

                actual_events = [concise_event_view(event) for event in parsed.events]
                self.assertEqual(len(actual_events), len(fixture["events"]))
                for index, expected_event in enumerate(fixture["events"]):
                    with self.subTest(fixture=fixture["relative_path"], event_index=index):
                        for key, expected_value in expected_event.items():
                            self.assertEqual(actual_events[index][key], expected_value)

    def test_build_turns_treats_commentary_as_update_not_final(self) -> None:
        commentary = "Comparing current assumptions against upstream."
        final_answer = "The phase-aware fix should land in session_status.py."
        events = [
            {
                "event_index": 1,
                "timestamp": "2026-04-20T03:19:14Z",
                "record_type": "event_msg",
                "payload_type": "user_message",
                "kind": "message",
                "role": "user",
                "title": "User Message",
                "display_text": "Find the parser drift.",
                "detail_text": "Find the parser drift.",
                "tool_name": None,
                "call_id": None,
                "command_text": None,
                "exit_code": None,
                "record_json": make_record_json(
                    {"type": "user_message", "message": "Find the parser drift."},
                    record_type="event_msg",
                ),
            },
            message_event(event_index=2, role="assistant", text=commentary, phase="commentary"),
            message_event(event_index=3, role="assistant", text=final_answer, phase="final_answer"),
            {
                "event_index": 4,
                "timestamp": "2026-04-20T03:19:18Z",
                "record_type": "event_msg",
                "payload_type": "task_complete",
                "kind": "system",
                "role": None,
                "title": "Task Complete",
                "display_text": final_answer,
                "detail_text": final_answer,
                "tool_name": None,
                "call_id": None,
                "command_text": None,
                "exit_code": None,
                "record_json": make_record_json(
                    {"type": "task_complete", "last_agent_message": final_answer},
                    record_type="event_msg",
                ),
            },
        ]

        turns = build_turns(events)

        self.assertEqual(len(turns), 1)
        self.assertEqual(turns[0]["response_state"], "final")
        self.assertEqual(turns[0]["response_text"], final_answer)
        self.assertEqual(len(turns[0]["audit_assistant_updates"]), 1)
        self.assertEqual(turns[0]["audit_assistant_updates"][0]["display_text"], commentary)

    def test_terminal_turn_summary_uses_commentary_when_turn_is_still_open(self) -> None:
        commentary = "Checking the upstream event schema before changing anything."
        events = [
            {
                "event_index": 1,
                "timestamp": "2026-04-20T03:19:14Z",
                "record_type": "event_msg",
                "payload_type": "user_message",
                "kind": "message",
                "role": "user",
                "display_text": "Inspect the latest rollout format.",
                "record_json": make_record_json(
                    {"type": "user_message", "message": "Inspect the latest rollout format."},
                    record_type="event_msg",
                ),
            },
            message_event(event_index=2, role="assistant", text=commentary, phase="commentary"),
        ]

        self.assertEqual(terminal_turn_summary(events), commentary)
        turns = build_turns(events)
        self.assertEqual(len(turns), 1)
        self.assertEqual(turns[0]["response_state"], "update")
        self.assertEqual(turns[0]["response_text"], commentary)

    def test_normalize_event_supports_local_shell_call_and_turn_complete_alias(self) -> None:
        shell_event = normalize_event(
            {
                "timestamp": "2026-04-20T03:19:15Z",
                "type": "response_item",
                "payload": {
                    "type": "local_shell_call",
                    "call_id": "call_123",
                    "status": "completed",
                    "action": {
                        "type": "exec",
                        "command": ["rg", "phase", "agent_operations_viewer"],
                        "working_directory": "/home/wulfuser/agent_operations_viewer",
                    },
                },
            },
            2,
        )
        self.assertIsNotNone(shell_event)
        assert shell_event is not None
        self.assertEqual(shell_event.kind, "tool_call")
        self.assertEqual(shell_event.tool_name, "exec_command")
        self.assertEqual(shell_event.call_id, "call_123")
        self.assertIn("rg phase agent_operations_viewer", shell_event.command_text or "")

        completion_event = normalize_event(
            {
                "timestamp": "2026-04-20T03:19:18Z",
                "type": "event_msg",
                "payload": {
                    "type": "turn_complete",
                    "last_agent_message": "Done",
                },
            },
            3,
        )
        self.assertIsNotNone(completion_event)
        assert completion_event is not None
        self.assertEqual(completion_event.title, "Task Complete")
        self.assertTrue(is_task_complete(normalized_event_to_dict(completion_event)))

    def test_build_turns_surfaces_execution_context_research_and_context_events(self) -> None:
        user_event = {
            "event_index": 1,
            "timestamp": "2026-04-20T07:20:26Z",
            "record_type": "event_msg",
            "payload_type": "user_message",
            "kind": "message",
            "role": "user",
            "title": "User Message",
            "display_text": "Audit the session.",
            "detail_text": "Audit the session.",
            "tool_name": None,
            "call_id": None,
            "command_text": None,
            "exit_code": None,
            "record_json": make_record_json(
                {"type": "user_message", "message": "Audit the session."},
                record_type="event_msg",
            ),
        }
        turn_context = normalize_event(
            {
                "timestamp": "2026-04-20T07:20:26.744Z",
                "type": "turn_context",
                "payload": {
                    "cwd": "/home/wulfuser/agent_operations_viewer",
                    "approval_policy": "on-request",
                    "sandbox_policy": {"type": "workspace-write", "network_access": False},
                    "model": "gpt-5.4",
                    "effort": "xhigh",
                    "collaboration_mode": {"mode": "default"},
                    "truncation_policy": {"mode": "tokens", "limit": 10000},
                },
            },
            2,
        )
        research_call = normalize_event(
            {
                "timestamp": "2026-04-20T07:20:27Z",
                "type": "response_item",
                "payload": {
                    "type": "web_search_call",
                    "status": "completed",
                    "action": {
                        "type": "open_page",
                        "url": "https://www.rfc-editor.org/rfc/rfc9293.html",
                    },
                },
            },
            3,
        )
        research_result = normalize_event(
            {
                "timestamp": "2026-04-20T07:20:28Z",
                "type": "event_msg",
                "payload": {
                    "type": "web_search_end",
                    "call_id": "ws_123",
                    "query": "https://www.rfc-editor.org/rfc/rfc9293.html",
                    "action": {
                        "type": "open_page",
                        "url": "https://www.rfc-editor.org/rfc/rfc9293.html",
                    },
                },
            },
            4,
        )
        context_event = normalize_event(
            {
                "timestamp": "2026-04-20T07:20:29Z",
                "type": "event_msg",
                "payload": {"type": "context_compacted"},
            },
            5,
        )
        final_message = message_event(
            event_index=6,
            role="assistant",
            text="Reviewed the rollout and surfaced the evidence.",
            phase="final_answer",
            timestamp="2026-04-20T07:20:30Z",
        )
        completion_event = {
            "event_index": 7,
            "timestamp": "2026-04-20T07:20:31Z",
            "record_type": "event_msg",
            "payload_type": "task_complete",
            "kind": "system",
            "role": None,
            "title": "Task Complete",
            "display_text": "Reviewed the rollout and surfaced the evidence.",
            "detail_text": "Reviewed the rollout and surfaced the evidence.",
            "tool_name": None,
            "call_id": None,
            "command_text": None,
            "exit_code": None,
            "record_json": make_record_json(
                {"type": "task_complete", "last_agent_message": "Reviewed the rollout and surfaced the evidence."},
                record_type="event_msg",
            ),
        }

        events = [
            user_event,
            normalized_event_to_dict(turn_context),
            normalized_event_to_dict(research_call),
            normalized_event_to_dict(research_result),
            normalized_event_to_dict(context_event),
            final_message,
            completion_event,
        ]

        turns = build_turns(events, cwd="/home/wulfuser/agent_operations_viewer")

        self.assertEqual(len(turns), 1)
        turn = turns[0]
        self.assertEqual(turn["audit_execution_context"]["approval_policy"], "on request")
        self.assertEqual(turn["audit_execution_context"]["sandbox_policy"], "workspace write")
        self.assertEqual(turn["audit_execution_context"]["network_access"], "disabled")
        self.assertEqual(len(turn["audit_research_events"]), 2)
        self.assertEqual(len(turn["audit_context_events"]), 1)
        self.assertTrue(any(phase["key"] == "research" for phase in turn["audit_phase_strip"]))
        self.assertTrue(any(link["label"] == "2 research events" for link in turn["audit_response_evidence"]["links"]))

    def test_infer_project_identity_uses_generic_git_remote(self) -> None:
        identity = infer_project_identity(
            source_host="builder",
            cwd="/workspace/repo",
            github_org=None,
            github_repo=None,
            github_slug=None,
            git_repository_url="git@bitbucket.org:finderscope/book-match-service.git",
        )

        self.assertEqual(identity["kind"], "git")
        self.assertEqual(identity["key"], "git:bitbucket.org/finderscope/book-match-service")
        self.assertEqual(identity["label"], "bitbucket.org/finderscope/book-match-service")

    def test_infer_project_identity_scopes_local_git_remote_to_source_host(self) -> None:
        first = infer_project_identity(
            source_host="nanopct6-lts",
            cwd="/home/wulfuser/wulf-linux-config",
            github_org=None,
            github_repo=None,
            github_slug=None,
            git_repository_url="../1_git_bare_repo/wulf_linux_config_bare/",
        )
        second = infer_project_identity(
            source_host="nanopct6-lts",
            cwd="/home/wulfuser/wulf-linux-config/subdir",
            github_org=None,
            github_repo=None,
            github_slug=None,
            git_repository_url="../1_git_bare_repo/wulf_linux_config_bare/",
        )
        other_host = infer_project_identity(
            source_host="other-host",
            cwd="/home/wulfuser/wulf-linux-config",
            github_org=None,
            github_repo=None,
            github_slug=None,
            git_repository_url="../1_git_bare_repo/wulf_linux_config_bare/",
        )

        self.assertEqual(first["kind"], "git")
        self.assertEqual(
            first["key"],
            "git:nanopct6-lts:file:../1_git_bare_repo/wulf_linux_config_bare",
        )
        self.assertEqual(first["key"], second["key"])
        self.assertNotEqual(first["key"], other_host["key"])
        self.assertEqual(first["label"], "nanopct6-lts/wulf_linux_config_bare")

    def test_effective_project_fields_use_generic_git_identity(self) -> None:
        project = effective_project_fields(
            {
                "id": "session-1",
                "project_id": None,
                "project_visibility": "authenticated",
                "source_host": "builder",
                "cwd": "/workspace/repo",
                "cwd_name": "repo",
                "github_org": None,
                "github_repo": None,
                "github_slug": None,
                "github_remote_url": None,
                "git_repository_url": "git@bitbucket.org:finderscope/book-match-service.git",
                "inferred_project_kind": "git",
                "inferred_project_key": "git:bitbucket.org/finderscope/book-match-service",
                "inferred_project_label": "bitbucket.org/finderscope/book-match-service",
                "override_group_key": None,
                "override_organization": None,
                "override_repository": None,
                "override_remote_url": None,
                "override_display_label": None,
            }
        )

        self.assertEqual(project["effective_project_kind"], "git")
        self.assertEqual(project["effective_group_key"], "git:bitbucket.org/finderscope/book-match-service")
        self.assertEqual(project["organization"], "bitbucket.org")
        self.assertEqual(project["repository"], "finderscope/book-match-service")
        self.assertEqual(project["display_label"], "bitbucket.org/finderscope/book-match-service")

    def test_effective_project_fields_use_project_root_identity(self) -> None:
        project = effective_project_fields(
            {
                "id": "session-2",
                "project_id": None,
                "project_visibility": "authenticated",
                "source_host": "builder",
                "cwd": "/workspace/repo/subdir",
                "cwd_name": "subdir",
                "github_org": None,
                "github_repo": None,
                "github_slug": None,
                "github_remote_url": None,
                "git_repository_url": None,
                "inferred_project_kind": "project",
                "inferred_project_key": "project:builder:/workspace/repo",
                "inferred_project_label": "builder/repo",
                "override_group_key": None,
                "override_organization": None,
                "override_repository": None,
                "override_remote_url": None,
                "override_display_label": None,
            }
        )

        self.assertEqual(project["effective_project_kind"], "project")
        self.assertEqual(project["organization"], "builder")
        self.assertEqual(project["repository"], "repo")
        self.assertEqual(project["display_label"], "builder/repo")

    def test_parse_session_text_skips_git_probe_for_remote_host(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_path = Path(temp_dir) / "repo"
            repo_path.mkdir()
            subprocess.run(["git", "init"], cwd=repo_path, check=True, capture_output=True, text=True)
            subprocess.run(
                ["git", "remote", "add", "origin", "git@bitbucket.org:finderscope/book-match-service.git"],
                cwd=repo_path,
                check=True,
                capture_output=True,
                text=True,
            )

            raw_jsonl = "\n".join(
                [
                    json.dumps(
                        {
                            "type": "session_meta",
                            "payload": {
                                "id": "session-remote-probe",
                                "timestamp": "2026-04-20T03:19:14Z",
                                "cwd": str(repo_path),
                                "originator": "tester",
                                "cli_version": "1.0.0",
                                "source": "cli",
                                "model_provider": "openai",
                            },
                        }
                    ),
                    json.dumps(
                        {
                            "type": "event_msg",
                            "payload": {
                                "type": "user_message",
                                "message": "Inspect repo grouping.",
                            },
                            "timestamp": "2026-04-20T03:19:15Z",
                        }
                    ),
                ]
            )

            with mock.patch.dict(os.environ, {"CODEX_VIEWER_SOURCE_HOST": "local-host"}, clear=False):
                parsed = parse_session_text(
                    raw_jsonl,
                    Path("/tmp/rollout-remote-probe.jsonl"),
                    Path("/tmp"),
                    "remote-host",
                    file_size=len(raw_jsonl.encode("utf-8")),
                    file_mtime_ns=0,
                )

            self.assertIsNone(parsed.git_repository_url)
            self.assertEqual(parsed.inferred_project_kind, "directory")

    def test_parse_session_text_uses_git_project_root_for_nested_local_session(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir) / "repo"
            nested = project_root / "src" / "feature"
            (project_root / ".git").mkdir(parents=True)
            nested.mkdir(parents=True)
            raw_jsonl = "\n".join(
                [
                    json.dumps(
                        {
                            "type": "session_meta",
                            "payload": {
                                "id": "session-project-root",
                                "timestamp": "2026-04-20T03:19:14Z",
                                "cwd": str(nested),
                                "originator": "tester",
                                "cli_version": "1.0.0",
                                "source": "cli",
                                "model_provider": "openai",
                            },
                        }
                    ),
                    json.dumps(
                        {
                            "type": "event_msg",
                            "payload": {
                                "type": "user_message",
                                "message": "Use the project root instead of the nested cwd.",
                            },
                            "timestamp": "2026-04-20T03:19:15Z",
                        }
                    ),
                ]
            )

            with mock.patch.dict(os.environ, {"CODEX_VIEWER_SOURCE_HOST": "local-host"}, clear=False):
                parsed = parse_session_text(
                    raw_jsonl,
                    Path("/tmp/rollout-project-root.jsonl"),
                    Path("/tmp"),
                    "local-host",
                    file_size=len(raw_jsonl.encode("utf-8")),
                    file_mtime_ns=0,
                )

            self.assertEqual(parsed.inferred_project_kind, "project")
            self.assertEqual(parsed.inferred_project_key, f"project:local-host:{project_root}")
            self.assertEqual(parsed.inferred_project_label, "local-host/repo")

    def test_parse_session_text_honors_alternate_project_root_markers(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            codex_home = Path(temp_dir) / "codex-home"
            codex_home.mkdir()
            (codex_home / "config.toml").write_text(
                "project_root_markers = [\".hg\"]\n",
                encoding="utf-8",
            )
            project_root = Path(temp_dir) / "repo"
            nested = project_root / "child"
            project_root.mkdir()
            (project_root / ".hg").write_text("hg", encoding="utf-8")
            nested.mkdir(parents=True)
            raw_jsonl = json.dumps(
                {
                    "type": "session_meta",
                    "payload": {
                        "id": "session-alt-root-marker",
                        "timestamp": "2026-04-20T03:19:14Z",
                        "cwd": str(nested),
                        "originator": "tester",
                        "cli_version": "1.0.0",
                        "source": "cli",
                        "model_provider": "openai",
                    },
                }
            )

            with mock.patch.dict(
                os.environ,
                {
                    "CODEX_HOME": str(codex_home),
                    "CODEX_VIEWER_SOURCE_HOST": "local-host",
                },
                clear=False,
            ):
                parsed = parse_session_text(
                    raw_jsonl,
                    Path("/tmp/rollout-alt-root.jsonl"),
                    Path("/tmp"),
                    "local-host",
                    file_size=len(raw_jsonl.encode("utf-8")),
                    file_mtime_ns=0,
                )

            self.assertEqual(parsed.inferred_project_kind, "project")
            self.assertEqual(parsed.inferred_project_key, f"project:local-host:{project_root}")
            self.assertEqual(parsed.inferred_project_label, "local-host/repo")

    def test_parse_session_text_uses_generic_git_probe_for_local_host(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_path = Path(temp_dir) / "repo"
            repo_path.mkdir()
            subprocess.run(["git", "init"], cwd=repo_path, check=True, capture_output=True, text=True)
            subprocess.run(
                ["git", "remote", "add", "origin", "git@bitbucket.org:finderscope/book-match-service.git"],
                cwd=repo_path,
                check=True,
                capture_output=True,
                text=True,
            )

            raw_jsonl = json.dumps(
                {
                    "type": "session_meta",
                    "payload": {
                        "id": "session-local-probe",
                        "timestamp": "2026-04-20T03:19:14Z",
                        "cwd": str(repo_path),
                        "originator": "tester",
                        "cli_version": "1.0.0",
                        "source": "cli",
                        "model_provider": "openai",
                    },
                }
            )

            with mock.patch.dict(os.environ, {"CODEX_VIEWER_SOURCE_HOST": "local-host"}, clear=False):
                parsed = parse_session_text(
                    raw_jsonl,
                    Path("/tmp/rollout-local-probe.jsonl"),
                    Path("/tmp"),
                    "local-host",
                    file_size=len(raw_jsonl.encode("utf-8")),
                    file_mtime_ns=0,
                )

            self.assertEqual(parsed.git_repository_url, "git@bitbucket.org:finderscope/book-match-service.git")
            self.assertEqual(parsed.inferred_project_kind, "git")
            self.assertEqual(
                parsed.inferred_project_key,
                "git:bitbucket.org/finderscope/book-match-service",
            )

    def test_sync_sessions_force_rebuild_preserves_artifact_backed_remote_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            session_root = project_root / "sessions"
            session_root.mkdir()
            data_dir = project_root / "data"
            db_path = data_dir / "agent_operations_viewer_sessions.sqlite3"
            raw_jsonl = "\n".join(
                [
                    json.dumps(
                        {
                            "type": "session_meta",
                            "payload": {
                                "id": "session-artifact-rebuild",
                                "timestamp": "2026-04-20T03:19:14Z",
                                "cwd": "/remote/workspace/repo",
                                "originator": "tester",
                                "cli_version": "1.0.0",
                                "source": "cli",
                                "model_provider": "openai",
                                "git": {
                                    "repository_url": "git@bitbucket.org:finderscope/book-match-service.git",
                                },
                            },
                        }
                    ),
                    json.dumps(
                        {
                            "type": "event_msg",
                            "payload": {
                                "type": "user_message",
                                "message": "Keep remote artifact sessions during rebuild.",
                            },
                            "timestamp": "2026-04-20T03:19:15Z",
                        }
                    ),
                ]
            )

            with mock.patch.dict(
                os.environ,
                {
                    "CODEX_VIEWER_SOURCE_HOST": "local-host",
                    "CODEX_VIEWER_DATA_DIR": str(data_dir),
                    "CODEX_VIEWER_DB": str(db_path),
                    "CODEX_SESSION_ROOTS": str(session_root),
                },
                clear=False,
            ):
                settings = Settings.from_env(project_root=project_root)
                settings.ensure_directories()
                init_db(settings.database_path)

                parsed = parse_session_text(
                    raw_jsonl,
                    Path("/remote/root/rollout-artifact.jsonl"),
                    Path("/remote/root"),
                    "remote-host",
                    file_size=len(raw_jsonl.encode("utf-8")),
                    file_mtime_ns=0,
                )

                with connect(settings.database_path) as connection:
                    with write_transaction(connection):
                        parsed.raw_artifact_sha256 = store_session_artifact(connection, settings, raw_jsonl)
                        upsert_parsed_session(connection, parsed)

                stats = sync_sessions(settings, force=True)

                self.assertEqual(stats["updated"], 1)
                with connect(settings.database_path) as connection:
                    row = connection.execute(
                        "SELECT inferred_project_kind, inferred_project_key, source_host FROM sessions WHERE id = ?",
                        ("session-artifact-rebuild",),
                    ).fetchone()

                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row["source_host"], "remote-host")
                self.assertEqual(row["inferred_project_kind"], "git")
                self.assertEqual(
                    row["inferred_project_key"],
                    "git:bitbucket.org/finderscope/book-match-service",
                )

    def test_store_raw_sync_sessions_batch_persists_sessions_and_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            settings = make_test_settings(data_dir=data_dir, session_roots=[Path(tmpdir) / "sessions"])
            init_db(settings.database_path)

            raw_items: list[tuple[object, str]] = []
            for session_id in ("batch-session-1", "batch-session-2"):
                raw_jsonl = make_raw_session_jsonl(session_id)
                parsed = parse_session_text(
                    raw_jsonl,
                    Path(f"/tmp/{session_id}.jsonl"),
                    Path("/tmp"),
                    "remote-host",
                    file_size=len(raw_jsonl.encode("utf-8")),
                    file_mtime_ns=0,
                )
                raw_items.append((parsed, raw_jsonl))

            with connect(settings.database_path) as connection:
                with write_transaction(connection):
                    results = store_raw_sync_sessions_batch(connection, settings, raw_items)
                session_count = connection.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
                artifact_count = connection.execute("SELECT COUNT(*) FROM session_artifacts").fetchone()[0]

        self.assertEqual([result["status"] for result in results], ["ok", "ok"])
        self.assertEqual(session_count, 2)
        self.assertEqual(artifact_count, 2)

    def test_store_raw_sync_session_tail_appends_existing_raw_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            settings = make_test_settings(data_dir=data_dir, session_roots=[Path(tmpdir) / "sessions"])
            init_db(settings.database_path)

            source_path = Path("/tmp/tail-session.jsonl")
            source_root = Path("/tmp")
            base_raw_jsonl = make_raw_session_jsonl("tail-session")
            parsed = parse_session_text(
                base_raw_jsonl,
                source_path,
                source_root,
                "remote-host",
                file_size=len(base_raw_jsonl.encode("utf-8")),
                file_mtime_ns=1,
            )
            tail_jsonl = "\n" + json.dumps(
                {
                    "type": "event_msg",
                    "timestamp": "2026-04-20T03:19:17Z",
                    "payload": {
                        "type": "agent_message",
                        "message": "Tail update",
                    },
                }
            )

            with connect(settings.database_path) as connection:
                with write_transaction(connection):
                    store_raw_sync_sessions_batch(connection, settings, [(parsed, base_raw_jsonl)])
                    result = store_raw_sync_session_tail(
                        connection,
                        settings,
                        {
                            "source_host": "remote-host",
                            "source_root": str(source_root),
                            "source_path": str(source_path),
                            "base_file_size": len(base_raw_jsonl.encode("utf-8")),
                            "base_content_sha256": parsed.content_sha256,
                            "file_size": len((base_raw_jsonl + tail_jsonl).encode("utf-8")),
                            "file_mtime_ns": 2,
                            "tail_jsonl": tail_jsonl,
                        },
                    )
                row = connection.execute(
                    "SELECT event_count, file_size FROM sessions WHERE id = ?",
                    ("tail-session",),
                ).fetchone()

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["event_count"], 3)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["event_count"], 3)
        self.assertEqual(row["file_size"], len((base_raw_jsonl + tail_jsonl).encode("utf-8")))

    def test_store_raw_sync_session_tail_reports_base_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            settings = make_test_settings(data_dir=data_dir, session_roots=[Path(tmpdir) / "sessions"])
            init_db(settings.database_path)

            with connect(settings.database_path) as connection:
                with write_transaction(connection):
                    result = store_raw_sync_session_tail(
                        connection,
                        settings,
                        {
                            "source_host": "remote-host",
                            "source_root": "/tmp",
                            "source_path": "/tmp/missing.jsonl",
                            "base_file_size": 123,
                            "base_content_sha256": "missing",
                            "file_size": 456,
                            "file_mtime_ns": 2,
                            "tail_jsonl": "\n{}",
                        },
                    )

        self.assertEqual(result["status"], "base_mismatch")
        self.assertEqual(result["reason"], "missing-session")

    def test_sync_sessions_remote_batches_raw_uploads(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            session_root = Path(tmpdir) / "sessions"
            session_root.mkdir(parents=True, exist_ok=True)
            for session_id in ("remote-batch-1", "remote-batch-2", "remote-batch-3"):
                (session_root / f"{session_id}.jsonl").write_text(
                    make_raw_session_jsonl(session_id),
                    encoding="utf-8",
                )

            settings = make_test_settings(
                data_dir=Path(tmpdir) / "data",
                session_roots=[session_root],
                remote_batch_size=2,
            )
            calls: list[tuple[str, str, object | None]] = []

            def fake_json_request(
                _settings: Settings,
                method: str,
                path: str,
                payload: dict[str, object] | None = None,
            ) -> dict[str, object]:
                calls.append((method, path, payload))
                if path == "/api/sync/sessions-raw":
                    sessions = payload.get("sessions") if isinstance(payload, dict) else None
                    assert isinstance(sessions, list)
                    return {"status": "ok", "results": [{"status": "ok"} for _ in sessions]}
                if path == "/api/sync/session-raw":
                    return {"status": "ok"}
                if path == "/api/sync/heartbeat":
                    return {"status": "ok"}
                raise AssertionError(f"Unexpected request path: {path}")

            with mock.patch("agent_operations_viewer.remote_sync.json_request", side_effect=fake_json_request):
                stats = sync_sessions_remote(settings, force=True)

        batch_calls = [call for call in calls if call[1] == "/api/sync/sessions-raw"]
        single_calls = [call for call in calls if call[1] == "/api/sync/session-raw"]
        heartbeat_calls = [call for call in calls if call[1] == "/api/sync/heartbeat"]

        self.assertEqual(stats, {"uploaded": 3, "skipped": 0, "failed": 0})
        self.assertEqual(len(batch_calls), 1)
        self.assertEqual(len(single_calls), 1)
        self.assertEqual(len(heartbeat_calls), 1)
        self.assertEqual(len(batch_calls[0][2]["sessions"]), 2)

    def test_sync_sessions_remote_falls_back_to_single_uploads_when_batch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            session_root = Path(tmpdir) / "sessions"
            session_root.mkdir(parents=True, exist_ok=True)
            for session_id in ("remote-fallback-1", "remote-fallback-2", "remote-fallback-3"):
                (session_root / f"{session_id}.jsonl").write_text(
                    make_raw_session_jsonl(session_id),
                    encoding="utf-8",
                )

            settings = make_test_settings(
                data_dir=Path(tmpdir) / "data",
                session_roots=[session_root],
                remote_batch_size=2,
            )
            calls: list[tuple[str, str, object | None]] = []
            batch_attempted = False

            def fake_json_request(
                _settings: Settings,
                method: str,
                path: str,
                payload: dict[str, object] | None = None,
            ) -> dict[str, object]:
                nonlocal batch_attempted
                calls.append((method, path, payload))
                if path == "/api/sync/sessions-raw":
                    batch_attempted = True
                    raise RemoteSyncError("404 Not Found")
                if path == "/api/sync/session-raw":
                    return {"status": "ok"}
                if path == "/api/sync/heartbeat":
                    return {"status": "ok"}
                raise AssertionError(f"Unexpected request path: {path}")

            with mock.patch("agent_operations_viewer.remote_sync.json_request", side_effect=fake_json_request):
                stats = sync_sessions_remote(settings, force=True)

        batch_calls = [call for call in calls if call[1] == "/api/sync/sessions-raw"]
        single_calls = [call for call in calls if call[1] == "/api/sync/session-raw"]

        self.assertTrue(batch_attempted)
        self.assertEqual(stats, {"uploaded": 3, "skipped": 0, "failed": 0})
        self.assertEqual(len(batch_calls), 1)
        self.assertEqual(len(single_calls), 3)

    def test_json_request_gzips_large_payloads(self) -> None:
        settings = make_test_settings(
            data_dir=Path(tempfile.mkdtemp()) / "data",
            session_roots=[Path("/tmp/sessions")],
        )
        settings.remote_gzip_min_bytes = 1
        captured: dict[str, object] = {}

        class _FakeResponse:
            def __enter__(self) -> "_FakeResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def read(self) -> bytes:
                return b'{"status":"ok"}'

        def fake_urlopen(request: object, timeout: int) -> _FakeResponse:
            del timeout
            captured["data"] = request.data
            captured["headers"] = dict(request.header_items())
            return _FakeResponse()

        with mock.patch("agent_operations_viewer.remote_sync.urlopen", side_effect=fake_urlopen):
            response = json_request(
                settings,
                "POST",
                "/api/sync/heartbeat",
                {"message": "x" * 2048},
            )

        headers = {str(key).lower(): value for key, value in dict(captured["headers"]).items()}
        self.assertEqual(response, {"status": "ok"})
        self.assertEqual(headers["content-encoding"], "gzip")
        self.assertEqual(
            json.loads(gzip.decompress(captured["data"]).decode("utf-8")),
            {"message": "x" * 2048},
        )

    def test_read_json_request_payload_accepts_gzip_bodies(self) -> None:
        payload = {"status": "ok", "count": 2}

        class _FakeRequest:
            headers = {"content-encoding": "gzip"}

            def __init__(self, body: bytes) -> None:
                self._body = body

            async def body(self) -> bytes:
                return self._body

        parsed = asyncio.run(
            _read_json_request_payload(
                _FakeRequest(gzip.compress(json.dumps(payload).encode("utf-8")))
            )
        )
        self.assertEqual(parsed, payload)

    def test_prescan_session_source_infers_project_without_full_parse(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sessions"
            root.mkdir(parents=True, exist_ok=True)
            codex_path = root / "codex.jsonl"
            claude_path = root / "claude.jsonl"
            codex_raw = make_raw_session_jsonl("pre-scan-codex", cwd="/workspace/pre-scan")
            claude_raw = make_claude_transcript_jsonl("pre-scan-claude", cwd="/workspace/pre-scan")
            codex_path.write_text(codex_raw, encoding="utf-8")
            claude_path.write_text(claude_raw, encoding="utf-8")

            codex_prescan = prescan_session_source(codex_path, root, "test-host")
            claude_prescan = prescan_session_source(claude_path, root, "test-host")

        self.assertIsNotNone(codex_prescan)
        self.assertIsNotNone(claude_prescan)
        assert codex_prescan is not None
        assert claude_prescan is not None
        self.assertEqual(codex_prescan.session_format, "codex")
        self.assertEqual(codex_prescan.session_id, "pre-scan-codex")
        self.assertEqual(codex_prescan.inferred_project_key, "directory:test-host:/workspace/pre-scan")
        self.assertEqual(claude_prescan.session_format, "claude")
        self.assertEqual(claude_prescan.session_id, "pre-scan-claude")
        self.assertEqual(claude_prescan.inferred_project_key, "directory:test-host:/workspace/pre-scan")

    def test_sync_sessions_remote_reuses_persistent_ignored_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            session_root = Path(tmpdir) / "sessions"
            session_root.mkdir(parents=True, exist_ok=True)
            session_path = session_root / "ignored-session.jsonl"
            raw_jsonl = make_raw_session_jsonl("ignored-session", cwd="/workspace/ignored")
            session_path.write_text(raw_jsonl, encoding="utf-8")
            inferred_key = "directory:test-host:/workspace/ignored"
            settings = make_test_settings(
                data_dir=Path(tmpdir) / "data",
                session_roots=[session_root],
            )

            def fake_json_request(
                _settings: Settings,
                method: str,
                path: str,
                payload: dict[str, object] | None = None,
            ) -> dict[str, object]:
                del payload
                if path.startswith("/api/sync/manifest"):
                    return {
                        "sessions": [],
                        "ignored_project_keys": [inferred_key],
                        "actions": {},
                        "server": {
                            "app_version": settings.app_version,
                            "sync_api_version": settings.sync_api_version,
                            "expected_agent_version": settings.expected_agent_version,
                        },
                    }
                if path == "/api/sync/heartbeat":
                    return {"status": "ok"}
                raise AssertionError(f"Unexpected request path: {method} {path}")

            with mock.patch("agent_operations_viewer.remote_sync.json_request", side_effect=fake_json_request):
                first_stats = sync_sessions_remote(settings)

            with (
                mock.patch("agent_operations_viewer.remote_sync.json_request", side_effect=fake_json_request),
                mock.patch("agent_operations_viewer.remote_sync.prescan_session_source", side_effect=AssertionError("prescan should not rerun")),
                mock.patch("agent_operations_viewer.remote_sync.parse_session_text", side_effect=AssertionError("parse should not rerun")),
                mock.patch("agent_operations_viewer.remote_sync.read_session_source_text", side_effect=AssertionError("read should not rerun")),
            ):
                second_stats = sync_sessions_remote(settings)

        self.assertEqual(first_stats, {"uploaded": 0, "skipped": 1, "failed": 0})
        self.assertEqual(second_stats, {"uploaded": 0, "skipped": 1, "failed": 0})

    def test_sync_sessions_remote_accepts_changed_path_list_without_full_tree_scan(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            session_root = Path(tmpdir) / "sessions"
            session_root.mkdir(parents=True, exist_ok=True)
            first_path = session_root / "changed.jsonl"
            second_path = session_root / "unchanged.jsonl"
            first_path.write_text(make_raw_session_jsonl("changed"), encoding="utf-8")
            second_path.write_text(make_raw_session_jsonl("unchanged"), encoding="utf-8")
            settings = make_test_settings(
                data_dir=Path(tmpdir) / "data",
                session_roots=[session_root],
                remote_batch_size=1,
            )
            calls: list[tuple[str, str, object | None]] = []

            def fake_json_request(
                _settings: Settings,
                method: str,
                path: str,
                payload: dict[str, object] | None = None,
            ) -> dict[str, object]:
                calls.append((method, path, payload))
                if path == "/api/sync/session-raw":
                    return {"status": "ok"}
                if path == "/api/sync/heartbeat":
                    return {"status": "ok"}
                raise AssertionError(f"Unexpected request path: {path}")

            with (
                mock.patch("agent_operations_viewer.remote_sync.json_request", side_effect=fake_json_request),
                mock.patch("agent_operations_viewer.remote_sync.iter_session_files", side_effect=AssertionError("full scan should not run")),
            ):
                stats = sync_sessions_remote(
                    settings,
                    force=True,
                    candidate_paths=[first_path],
                )

        upload_calls = [call for call in calls if call[1] == "/api/sync/session-raw"]
        self.assertEqual(stats, {"uploaded": 1, "skipped": 0, "failed": 0})
        self.assertEqual(len(upload_calls), 1)
        self.assertEqual(upload_calls[0][2]["source_path"], str(first_path))

    def test_sync_sessions_remote_uploads_append_tail_when_base_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            session_root = Path(tmpdir) / "sessions"
            session_root.mkdir(parents=True, exist_ok=True)
            session_path = session_root / "tail-remote.jsonl"
            base_raw_jsonl = make_raw_session_jsonl("tail-remote")
            session_path.write_text(base_raw_jsonl, encoding="utf-8")
            settings = make_test_settings(
                data_dir=Path(tmpdir) / "data",
                session_roots=[session_root],
                remote_batch_size=1,
            )
            base_parsed = parse_session_text(
                base_raw_jsonl,
                session_path,
                session_root,
                settings.source_host,
                file_size=len(base_raw_jsonl.encode("utf-8")),
                file_mtime_ns=session_path.stat().st_mtime_ns,
            )
            appended_tail = "\n" + json.dumps(
                {
                    "type": "event_msg",
                    "timestamp": "2026-04-20T03:19:17Z",
                    "payload": {
                        "type": "agent_message",
                        "message": "Tail update",
                    },
                }
            )
            appended_raw_jsonl = base_raw_jsonl + appended_tail
            appended_content_sha256 = parse_session_text(
                appended_raw_jsonl,
                session_path,
                session_root,
                settings.source_host,
                file_size=len(appended_raw_jsonl.encode("utf-8")),
                file_mtime_ns=2,
            ).content_sha256
            calls: list[tuple[str, str, object | None]] = []

            def fake_json_request(
                _settings: Settings,
                method: str,
                path: str,
                payload: dict[str, object] | None = None,
            ) -> dict[str, object]:
                calls.append((method, path, payload))
                if path == "/api/sync/session-raw":
                    return {
                        "status": "ok",
                        "content_sha256": base_parsed.content_sha256,
                        "event_count": base_parsed.event_count,
                    }
                if path.startswith("/api/sync/manifest"):
                    return {
                        "sessions": [
                            {
                                "id": "tail-remote",
                                "source_path": str(session_path),
                                "file_size": len(base_raw_jsonl.encode("utf-8")),
                                "file_mtime_ns": 1,
                                "content_sha256": base_parsed.content_sha256,
                                "has_raw_artifact": True,
                                "event_count": base_parsed.event_count,
                                "stored_event_count": base_parsed.event_count,
                            }
                        ],
                        "ignored_project_keys": [],
                        "actions": {},
                        "server": {
                            "app_version": settings.app_version,
                            "sync_api_version": settings.sync_api_version,
                            "expected_agent_version": settings.expected_agent_version,
                        },
                    }
                if path == "/api/sync/session-tail":
                    return {
                        "status": "ok",
                        "content_sha256": appended_content_sha256,
                        "event_count": 3,
                    }
                if path == "/api/sync/heartbeat":
                    return {"status": "ok"}
                raise AssertionError(f"Unexpected request path: {path}")

            with mock.patch("agent_operations_viewer.remote_sync.json_request", side_effect=fake_json_request):
                first_stats = sync_sessions_remote(settings, force=True)
                session_path.write_text(appended_raw_jsonl, encoding="utf-8")
                second_stats = sync_sessions_remote(settings)

        tail_calls = [call for call in calls if call[1] == "/api/sync/session-tail"]
        self.assertEqual(first_stats, {"uploaded": 1, "skipped": 0, "failed": 0})
        self.assertEqual(second_stats, {"uploaded": 1, "skipped": 0, "failed": 0})
        self.assertEqual(len(tail_calls), 1)
        self.assertEqual(tail_calls[0][2]["base_file_size"], len(base_raw_jsonl.encode("utf-8")))
        self.assertEqual(tail_calls[0][2]["tail_jsonl"], appended_tail)

    def test_session_file_watcher_poll_detects_changes_with_debounce(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sessions"
            root.mkdir(parents=True, exist_ok=True)
            watcher = SessionFileWatcher(
                [root],
                mode="poll",
                debounce_seconds=0.05,
                poll_interval_seconds=0.02,
            )
            stop_event = threading.Event()
            target = root / "watched.jsonl"
            try:
                self.assertEqual(watcher.wait_for_changes(stop_event, timeout_seconds=0.05), [])

                target.write_text(make_raw_session_jsonl("watched"), encoding="utf-8")
                self.assertEqual(watcher.wait_for_changes(stop_event, timeout_seconds=0.02), [])
                created = watcher.wait_for_changes(stop_event, timeout_seconds=0.3)
                self.assertEqual(created, [target])

                target.unlink()
                deleted = watcher.wait_for_changes(stop_event, timeout_seconds=0.3)
                self.assertEqual(deleted, [target])
            finally:
                watcher.close()

    def test_parse_session_text_extracts_agent_lineage_and_usage_pressure(self) -> None:
        raw_jsonl = "\n".join(
            [
                json.dumps(
                    {
                        "type": "session_meta",
                        "payload": {
                            "id": "agent-usage-session",
                            "timestamp": "2026-04-20T03:19:14Z",
                            "cwd": "/workspace/repo",
                            "originator": "tester",
                            "cli_version": "1.0.0",
                            "source": "cli",
                            "model_provider": "openai",
                            "forked_from_id": "parent-session",
                            "agent_nickname": "Worker 7",
                            "agent_role": "worker",
                            "agent_path": "root/worker-7",
                            "memory_mode": "full",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:15Z",
                        "payload": {
                            "type": "user_message",
                            "message": "Check the new lineage telemetry.",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:16Z",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "model_context_window": 128000,
                                "total_token_usage": {
                                    "input_tokens": 40000,
                                    "cached_input_tokens": 10000,
                                    "output_tokens": 2000,
                                    "reasoning_output_tokens": 500,
                                    "total_tokens": 42000,
                                },
                            },
                            "rate_limits": {
                                "limit_name": "requests",
                                "primary": {
                                    "used_percent": 82.4,
                                    "resets_at": 1770000000,
                                },
                                "secondary": {
                                    "used_percent": 31.1,
                                    "resets_at": 1770003600,
                                },
                            },
                        },
                    }
                ),
            ]
        )

        parsed = parse_session_text(
            raw_jsonl,
            Path("/tmp/agent-usage-session.jsonl"),
            Path("/tmp"),
            "local-host",
            file_size=len(raw_jsonl.encode("utf-8")),
            file_mtime_ns=0,
        )

        self.assertEqual(parsed.forked_from_id, "parent-session")
        self.assertEqual(parsed.agent_nickname, "Worker 7")
        self.assertEqual(parsed.agent_role, "worker")
        self.assertEqual(parsed.agent_path, "root/worker-7")
        self.assertEqual(parsed.memory_mode, "full")
        self.assertEqual(parsed.latest_total_tokens, 42000)
        self.assertEqual(parsed.latest_input_tokens, 40000)
        self.assertEqual(parsed.latest_cached_input_tokens, 10000)
        self.assertEqual(parsed.latest_output_tokens, 2000)
        self.assertEqual(parsed.latest_context_window, 128000)
        self.assertEqual(parsed.latest_context_remaining_percent, 74)
        self.assertAlmostEqual(parsed.latest_primary_limit_used_percent or 0.0, 82.4)
        self.assertEqual(parsed.latest_rate_limit_name, "requests")

    def test_parse_session_text_uses_last_token_usage_for_context_pressure(self) -> None:
        raw_jsonl = "\n".join(
            [
                json.dumps(
                    {
                        "type": "session_meta",
                        "payload": {
                            "id": "agent-usage-last-token-session",
                            "timestamp": "2026-04-20T03:19:14Z",
                            "cwd": "/workspace/repo",
                            "originator": "tester",
                            "cli_version": "1.0.0",
                            "source": "cli",
                            "model_provider": "openai",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:15Z",
                        "payload": {
                            "type": "user_message",
                            "message": "Compute context pressure from the latest request.",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:16Z",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "model_context_window": 258400,
                                "total_token_usage": {
                                    "input_tokens": 626588,
                                    "cached_input_tokens": 575488,
                                    "output_tokens": 4709,
                                    "reasoning_output_tokens": 1770,
                                    "total_tokens": 631297,
                                },
                                "last_token_usage": {
                                    "input_tokens": 73245,
                                    "cached_input_tokens": 72960,
                                    "output_tokens": 143,
                                    "reasoning_output_tokens": 21,
                                    "total_tokens": 73388,
                                },
                            },
                        },
                    }
                ),
            ]
        )

        parsed = parse_session_text(
            raw_jsonl,
            Path("/tmp/agent-usage-last-token-session.jsonl"),
            Path("/tmp"),
            "local-host",
            file_size=len(raw_jsonl.encode("utf-8")),
            file_mtime_ns=0,
        )

        self.assertEqual(parsed.latest_total_tokens, 631297)
        self.assertEqual(parsed.latest_context_window, 258400)
        self.assertEqual(parsed.latest_context_remaining_percent, 75)

    def test_parsed_session_from_payload_backfills_lineage_and_usage_from_older_payloads(self) -> None:
        raw_jsonl = "\n".join(
            [
                json.dumps(
                    {
                        "type": "session_meta",
                        "payload": {
                            "id": "legacy-payload-session",
                            "timestamp": "2026-04-20T03:19:14Z",
                            "cwd": "/workspace/repo",
                            "originator": "tester",
                            "cli_version": "1.0.0",
                            "source": "cli",
                            "model_provider": "openai",
                            "forked_from_id": "parent-session",
                            "agent_role": "explorer",
                            "agent_path": "root/explorer-1",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:15Z",
                        "payload": {
                            "type": "user_message",
                            "message": "Recover fields from an older payload.",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:16Z",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "model_context_window": 64000,
                                "total_token_usage": {
                                    "input_tokens": 18000,
                                    "cached_input_tokens": 2000,
                                    "output_tokens": 1500,
                                    "reasoning_output_tokens": 250,
                                    "total_tokens": 19500,
                                },
                            },
                            "rate_limits": {
                                "primary": {"used_percent": 61.0, "resets_at": 1770000000},
                            },
                        },
                    }
                ),
            ]
        )

        parsed = parse_session_text(
            raw_jsonl,
            Path("/tmp/legacy-payload-session.jsonl"),
            Path("/tmp"),
            "local-host",
            file_size=len(raw_jsonl.encode("utf-8")),
            file_mtime_ns=0,
        )
        payload = parsed_session_to_payload(parsed)
        session_payload = payload["session"]
        assert isinstance(session_payload, dict)
        session_payload["rollup_version"] = 2
        for key in (
            "forked_from_id",
            "agent_nickname",
            "agent_role",
            "agent_path",
            "memory_mode",
            "latest_usage_timestamp",
            "latest_input_tokens",
            "latest_cached_input_tokens",
            "latest_output_tokens",
            "latest_reasoning_output_tokens",
            "latest_total_tokens",
            "latest_context_window",
            "latest_context_remaining_percent",
            "latest_primary_limit_used_percent",
            "latest_primary_limit_resets_at",
            "latest_secondary_limit_used_percent",
            "latest_secondary_limit_resets_at",
            "latest_rate_limit_name",
            "latest_rate_limit_reached_type",
        ):
            session_payload.pop(key, None)

        restored = parsed_session_from_payload(payload)

        self.assertEqual(restored.rollup_version, 4)
        self.assertEqual(restored.forked_from_id, "parent-session")
        self.assertEqual(restored.agent_role, "explorer")
        self.assertEqual(restored.agent_path, "root/explorer-1")
        self.assertEqual(restored.latest_input_tokens, 18000)
        self.assertEqual(restored.latest_total_tokens, 19500)
        self.assertEqual(restored.latest_context_window, 64000)
        self.assertAlmostEqual(restored.latest_primary_limit_used_percent or 0.0, 61.0)

    def test_upsert_parsed_session_persists_agent_and_usage_columns(self) -> None:
        raw_jsonl = "\n".join(
            [
                json.dumps(
                    {
                        "type": "session_meta",
                        "payload": {
                            "id": "persisted-agent-session",
                            "timestamp": "2026-04-20T03:19:14Z",
                            "cwd": "/workspace/repo",
                            "originator": "tester",
                            "cli_version": "1.0.0",
                            "source": "cli",
                            "model_provider": "openai",
                            "forked_from_id": "parent-session",
                            "agent_nickname": "Research worker",
                            "agent_role": "worker",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:15Z",
                        "payload": {
                            "type": "user_message",
                            "message": "Persist the new session insight columns.",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:16Z",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "model_context_window": 100000,
                                "total_token_usage": {
                                    "input_tokens": 30000,
                                    "cached_input_tokens": 5000,
                                    "output_tokens": 2500,
                                    "reasoning_output_tokens": 300,
                                    "total_tokens": 32500,
                                },
                            },
                            "rate_limits": {
                                "limit_name": "tokens",
                                "primary": {"used_percent": 95.0, "resets_at": 1770000000},
                            },
                        },
                    }
                ),
            ]
        )

        parsed = parse_session_text(
            raw_jsonl,
            Path("/tmp/persisted-agent-session.jsonl"),
            Path("/tmp"),
            "local-host",
            file_size=len(raw_jsonl.encode("utf-8")),
            file_mtime_ns=0,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(db_path)
            with connect(db_path) as connection:
                with write_transaction(connection):
                    upsert_parsed_session(connection, parsed)
                row = connection.execute(
                    """
                    SELECT
                        forked_from_id,
                        agent_nickname,
                        agent_role,
                        latest_usage_timestamp,
                        latest_total_tokens,
                        latest_context_remaining_percent,
                        latest_primary_limit_used_percent,
                        latest_rate_limit_name
                    FROM sessions
                    WHERE id = ?
                    """,
                    ("persisted-agent-session",),
                ).fetchone()

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["forked_from_id"], "parent-session")
        self.assertEqual(row["agent_nickname"], "Research worker")
        self.assertEqual(row["agent_role"], "worker")
        self.assertEqual(row["latest_total_tokens"], 32500)
        self.assertEqual(row["latest_context_remaining_percent"], 77)
        self.assertAlmostEqual(float(row["latest_primary_limit_used_percent"] or 0.0), 95.0)
        self.assertEqual(row["latest_rate_limit_name"], "tokens")

    def test_fetch_turn_stream_uses_turn_level_usage_pressure(self) -> None:
        raw_jsonl = "\n".join(
            [
                json.dumps(
                    {
                        "type": "session_meta",
                        "payload": {
                            "id": "turn-usage-session",
                            "timestamp": "2026-04-20T03:19:14Z",
                            "cwd": "/workspace/repo",
                            "originator": "tester",
                            "cli_version": "1.0.0",
                            "source": "cli",
                            "model_provider": "openai",
                            "git": {
                                "repository_url": "https://github.com/openai/codex-viewer.git",
                            },
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:15Z",
                        "payload": {
                            "type": "user_message",
                            "message": "Handle the first turn.",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:16Z",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "model_context_window": 128000,
                                "total_token_usage": {
                                    "input_tokens": 17000,
                                    "cached_input_tokens": 2000,
                                    "output_tokens": 3000,
                                    "reasoning_output_tokens": 500,
                                    "total_tokens": 20000,
                                },
                            },
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:17Z",
                        "payload": {
                            "type": "turn_complete",
                            "turn_id": "turn-1",
                            "last_agent_message": "First turn complete.",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:18Z",
                        "payload": {
                            "type": "user_message",
                            "message": "Handle the second turn.",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:19Z",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "model_context_window": 128000,
                                "total_token_usage": {
                                    "input_tokens": 85000,
                                    "cached_input_tokens": 5000,
                                    "output_tokens": 15000,
                                    "reasoning_output_tokens": 1200,
                                    "total_tokens": 100000,
                                },
                            },
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:20Z",
                        "payload": {
                            "type": "turn_complete",
                            "turn_id": "turn-2",
                            "last_agent_message": "Second turn complete.",
                        },
                    }
                ),
            ]
        )

        parsed = parse_session_text(
            raw_jsonl,
            Path("/tmp/turn-usage-session.jsonl"),
            Path("/tmp"),
            "local-host",
            file_size=len(raw_jsonl.encode("utf-8")),
            file_mtime_ns=0,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(db_path)
            with connect(db_path) as connection:
                with write_transaction(connection):
                    upsert_parsed_session(connection, parsed)

                rows = connection.execute(
                    """
                    SELECT turn_number, latest_context_remaining_percent
                    FROM session_turns
                    WHERE session_id = ?
                    ORDER BY turn_number ASC
                    """,
                    ("turn-usage-session",),
                ).fetchall()
                stream = fetch_turn_stream(connection, page_size=10)

        self.assertEqual(
            [(int(row["turn_number"]), row["latest_context_remaining_percent"]) for row in rows],
            [(1, 93), (2, 24)],
        )

        items_by_turn = {int(item["turn_number"]): item for item in stream["items"]}
        self.assertNotIn(
            "Context 24%",
            [str(badge["label"]) for badge in items_by_turn[1]["signal_badges"]],
        )
        self.assertIn(
            "Context 24%",
            [str(badge["label"]) for badge in items_by_turn[2]["signal_badges"]],
        )

    def test_fetch_group_detail_handles_attention_sessions_with_usage_pressure(self) -> None:
        raw_jsonl = "\n".join(
            [
                json.dumps(
                    {
                        "type": "session_meta",
                        "payload": {
                            "id": "project-detail-session",
                            "timestamp": "2026-04-20T03:19:14Z",
                            "cwd": "/workspace/repo",
                            "originator": "tester",
                            "cli_version": "1.0.0",
                            "source": "cli",
                            "model_provider": "openai",
                            "forked_from_id": "parent-session",
                            "agent_role": "worker",
                            "git": {
                                "repository_url": "https://github.com/openai/codex-viewer.git",
                            },
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:15Z",
                        "payload": {
                            "type": "user_message",
                            "message": "Open the project detail page.",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-04-20T03:19:16Z",
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "model_context_window": 128000,
                                "total_token_usage": {
                                    "input_tokens": 40000,
                                    "cached_input_tokens": 10000,
                                    "output_tokens": 2000,
                                    "reasoning_output_tokens": 500,
                                    "total_tokens": 42000,
                                },
                            },
                            "rate_limits": {
                                "primary": {"used_percent": 82.4, "resets_at": 1770000000},
                            },
                        },
                    }
                ),
            ]
        )

        parsed = parse_session_text(
            raw_jsonl,
            Path("/tmp/project-detail-session.jsonl"),
            Path("/tmp"),
            "local-host",
            file_size=len(raw_jsonl.encode("utf-8")),
            file_mtime_ns=0,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "viewer.sqlite3"
            init_db(db_path)
            with connect(db_path) as connection:
                with write_transaction(connection):
                    upsert_parsed_session(connection, parsed)
                project_access = build_project_access_context(connection, auth_user=None, auth_enabled=False)
                detail = fetch_group_detail(
                    connection,
                    "github:openai/codex-viewer",
                    project_access=project_access,
                )

        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertEqual(detail["status_strip"]["health_label"], "High Usage")
        self.assertEqual(len(detail["attention_sessions"]), 1)
        self.assertEqual(detail["attention_sessions"][0]["status_label"], "High Usage")
        self.assertIn("Rate limit usage is at 83%", detail["attention_sessions"][0]["detail"])
        self.assertEqual(detail["attention_sessions"][0]["attention_score"], 68)

if __name__ == "__main__":
    unittest.main()
