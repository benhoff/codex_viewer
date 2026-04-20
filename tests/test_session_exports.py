from __future__ import annotations

from io import BytesIO
import json
import unittest
import zipfile

from codex_session_viewer.runtime import export_markdown
from codex_session_viewer.session_exports import (
    build_execution_context_export,
    build_session_bundle,
    export_json_payload,
)


def make_record_json(payload: dict[str, object], *, record_type: str) -> str:
    return json.dumps({"type": record_type, "payload": payload})


class SessionExportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.session = {
            "id": "session-export-1",
            "summary": "Export the environment context.",
            "session_timestamp": "2026-04-20T07:20:26Z",
            "started_at": "2026-04-20T07:20:26Z",
            "cwd": "/home/wulfuser/codex",
            "source_host": "builder",
            "model_provider": "openai",
            "raw_meta_json": json.dumps(
                {
                    "cwd": "/home/wulfuser/codex",
                    "originator": "tester",
                    "cli_version": "1.0.0",
                    "source": "cli",
                    "model_provider": "openai",
                    "base_instructions": {"kind": "repo"},
                    "dynamic_tools": [{"name": "mcp__github"}],
                    "memory_mode": "disabled",
                    "agent_role": "explorer",
                }
            ),
        }
        self.events = [
            {
                "event_index": 1,
                "timestamp": "2026-04-20T07:20:26Z",
                "record_type": "turn_context",
                "payload_type": None,
                "kind": "context",
                "role": None,
                "title": "Turn Context",
                "display_text": "gpt-5.4 | /home/wulfuser/codex",
                "detail_text": "Turn context updated",
                "tool_name": None,
                "call_id": None,
                "command_text": None,
                "record_json": make_record_json(
                    {
                        "cwd": "/home/wulfuser/codex",
                        "approval_policy": "on-request",
                        "sandbox_policy": {"type": "workspace-write", "network_access": False},
                        "network": {"allowed_domains": ["api.github.com"]},
                        "file_system_sandbox_policy": {"mode": "workspace-write"},
                        "model": "gpt-5.4",
                        "effort": "high",
                        "collaboration_mode": {"mode": "default"},
                        "truncation_policy": {"mode": "tokens", "limit": 12000},
                    },
                    record_type="turn_context",
                ),
            },
            {
                "event_index": 2,
                "timestamp": "2026-04-20T07:20:27Z",
                "record_type": "event_msg",
                "payload_type": "session_configured",
                "kind": "system",
                "role": None,
                "title": "Session Configured",
                "display_text": "Session configured",
                "detail_text": "Session configured",
                "tool_name": None,
                "call_id": None,
                "command_text": None,
                "record_json": make_record_json(
                    {
                        "type": "session_configured",
                        "model": "gpt-5.4",
                        "model_provider_id": "openai",
                        "approval_policy": "on-request",
                        "approvals_reviewer": "user",
                        "sandbox_policy": {"type": "workspace-write", "network_access": False},
                        "cwd": "/home/wulfuser/codex",
                        "reasoning_effort": "high",
                        "network_proxy": {"http": "http://127.0.0.1:8080"},
                    },
                    record_type="event_msg",
                ),
            },
            {
                "event_index": 3,
                "timestamp": "2026-04-20T07:20:28Z",
                "record_type": "event_msg",
                "payload_type": "request_permissions",
                "kind": "system",
                "role": None,
                "title": "Request Permissions",
                "display_text": "Need broader filesystem access",
                "detail_text": "Need broader filesystem access",
                "tool_name": None,
                "call_id": "perm_123",
                "command_text": None,
                "record_json": make_record_json(
                    {
                        "type": "request_permissions",
                        "reason": "Need broader filesystem access",
                        "permissions": {"file_system": {"mode": "write"}},
                    },
                    record_type="event_msg",
                ),
            },
            {
                "event_index": 4,
                "timestamp": "2026-04-20T07:20:29Z",
                "record_type": "event_msg",
                "payload_type": "mcp_startup_complete",
                "kind": "system",
                "role": None,
                "title": "MCP Startup Complete",
                "display_text": "github ready",
                "detail_text": "github ready",
                "tool_name": None,
                "call_id": None,
                "command_text": None,
                "record_json": make_record_json(
                    {
                        "type": "mcp_startup_complete",
                        "ready": ["github"],
                        "failed": [],
                        "cancelled": [],
                    },
                    record_type="event_msg",
                ),
            },
            {
                "event_index": 5,
                "timestamp": "2026-04-20T07:20:30Z",
                "record_type": "event_msg",
                "payload_type": "model_reroute",
                "kind": "system",
                "role": None,
                "title": "Model Reroute",
                "display_text": "gpt-5.4 -> gpt-5.4-mini",
                "detail_text": "gpt-5.4 -> gpt-5.4-mini",
                "tool_name": None,
                "call_id": None,
                "command_text": None,
                "record_json": make_record_json(
                    {
                        "type": "model_reroute",
                        "from_model": "gpt-5.4",
                        "to_model": "gpt-5.4-mini",
                        "reason": "capacity",
                    },
                    record_type="event_msg",
                ),
            },
        ]

    def test_execution_context_export_collects_session_and_policy_data(self) -> None:
        payload = build_execution_context_export(self.session, self.events)

        self.assertEqual(payload["session_meta"]["model_provider"], "openai")
        self.assertEqual(payload["session_meta"]["memory_mode"], "disabled")
        self.assertEqual(payload["latest_turn_context"]["approval_policy"], "on-request")
        self.assertEqual(payload["latest_session_configured"]["model_provider_id"], "openai")
        self.assertEqual(payload["counts"]["permission_requests"], 1)
        self.assertEqual(
            payload["permission_requests"][0]["payload"]["reason"],
            "Need broader filesystem access",
        )
        self.assertEqual(payload["mcp_startup_events"][0]["payload"]["ready"], ["github"])
        self.assertEqual(payload["model_reroutes"][0]["payload"]["to_model"], "gpt-5.4-mini")

    def test_export_json_payload_includes_execution_context(self) -> None:
        payload = export_json_payload(self.session, self.events)

        self.assertIn("execution_context", payload)
        self.assertEqual(
            payload["execution_context"]["latest_turn_context"]["collaboration_mode"]["mode"],
            "default",
        )

    def test_build_session_bundle_writes_execution_context_file(self) -> None:
        bundle_bytes = build_session_bundle(
            self.session,
            self.events,
            raw_jsonl="{}\n",
            raw_export_info={"source": "artifact"},
        )

        with zipfile.ZipFile(BytesIO(bundle_bytes), mode="r") as bundle:
            self.assertIn("execution_context.json", bundle.namelist())
            metadata = json.loads(bundle.read("metadata.json"))
            execution_context = json.loads(bundle.read("execution_context.json"))

        self.assertEqual(metadata["bundle_version"], 2)
        self.assertEqual(execution_context["counts"]["session_configured_events"], 1)

    def test_export_markdown_embeds_execution_context_block(self) -> None:
        markdown = export_markdown(self.session, self.events)

        self.assertIn("## Execution Context", markdown)
        self.assertIn('"approval_policy": "on-request"', markdown)
        self.assertIn('"model_provider_id": "openai"', markdown)


if __name__ == "__main__":
    unittest.main()
