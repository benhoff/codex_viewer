from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from codex_session_viewer import SYNC_API_VERSION, __version__
from codex_session_viewer.config import Settings
from codex_session_viewer.db import connect, init_db, write_transaction
from codex_session_viewer.environment_audit import (
    ENVIRONMENT_ROLLUP_VERSION,
    fetch_host_environment_audit,
    fetch_project_environment_audit,
)
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


def command_records(
    command: list[str],
    *,
    timestamp_call: str,
    timestamp_result: str,
    turn_id: str = "turn-1",
    call_id: str,
    exit_code: int,
    stderr: str = "",
    workdir: str,
) -> list[dict[str, object]]:
    command_text = " ".join(command)
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
                "parsed_cmd": [{"type": "unknown", "cmd": command_text}],
                "stdout": "",
                "stderr": stderr,
                "aggregated_output": stderr,
                "exit_code": exit_code,
                "duration": {"secs": 1, "nanos": 0},
                "formatted_output": stderr,
                "status": "failed" if exit_code else "completed",
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


def raw_session_jsonl(
    session_id: str,
    *,
    cwd: str,
    records: list[dict[str, object]],
    session_timestamp: str = "2026-04-21T02:00:00Z",
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


class EnvironmentAuditRollupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        data_dir = Path(self.temp_dir.name)
        self.session_root = data_dir / "sessions"
        self.session_root.mkdir(parents=True, exist_ok=True)
        self.settings = make_test_settings(data_dir=data_dir, session_roots=[self.session_root])
        init_db(self.settings.database_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _upsert_session(self, *, session_id: str, source_host: str, cwd: str) -> None:
        records = [
            *command_records(
                ["python3", "-m", "demo_app"],
                timestamp_call="2026-04-21T02:00:01Z",
                timestamp_result="2026-04-21T02:00:02Z",
                call_id=f"{session_id}-cmd-1",
                exit_code=127,
                stderr="python3: command not found",
                workdir=cwd,
            ),
            *command_records(
                ["python3", "-m", "demo_app"],
                timestamp_call="2026-04-21T02:00:03Z",
                timestamp_result="2026-04-21T02:00:04Z",
                call_id=f"{session_id}-cmd-2",
                exit_code=127,
                stderr="python3: command not found",
                workdir=cwd,
            ),
            turn_complete_record(
                "Tried to run the demo app.",
                timestamp="2026-04-21T02:00:05Z",
            ),
        ]
        raw = raw_session_jsonl(
            session_id,
            cwd=cwd,
            records=records,
        )
        parsed = parse_session_text(
            raw,
            self.session_root / f"{session_id}.jsonl",
            self.session_root,
            source_host,
        )
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                upsert_parsed_session(connection, parsed)

    def test_project_environment_audit_uses_materialized_rollups(self) -> None:
        self._upsert_session(
            session_id="session-project-env",
            source_host="alpha-host",
            cwd="/workspace/demo",
        )

        with connect(self.settings.database_path) as connection:
            session_row = connection.execute(
                "SELECT environment_rollup_version FROM sessions WHERE id = ?",
                ("session-project-env",),
            ).fetchone()
            self.assertIsNotNone(session_row)
            self.assertEqual(int(session_row["environment_rollup_version"] or 0), ENVIRONMENT_ROLLUP_VERSION)

            observation_count = connection.execute(
                "SELECT COUNT(*) AS count FROM environment_command_observations WHERE session_id = ?",
                ("session-project-env",),
            ).fetchone()
            self.assertGreaterEqual(int(observation_count["count"] or 0), 2)

            capability_count = connection.execute(
                "SELECT COUNT(*) AS count FROM environment_host_capabilities WHERE source_host = ?",
                ("alpha-host",),
            ).fetchone()
            self.assertGreater(int(capability_count["count"] or 0), 0)

            group_key = build_grouped_projects(query_group_rows(connection))[0].key
            baseline = fetch_project_environment_audit(connection, group_key)
            self.assertIsNotNone(baseline)
            self.assertEqual(baseline["failure_groups"][0]["class_label"], "Command not found")
            self.assertIn("python3", [item["label"] for item in baseline["requirements"]])
            baseline_fit = baseline["host_fit"][0]

            connection.execute("DELETE FROM events")
            materialized = fetch_project_environment_audit(connection, group_key)

        self.assertIsNotNone(materialized)
        self.assertEqual(materialized["failure_groups"][0]["class_label"], "Command not found")
        self.assertIn("python3", [item["label"] for item in materialized["requirements"]])
        self.assertEqual(materialized["host_fit"][0]["status"], baseline_fit["status"])
        self.assertEqual(materialized["host_fit"][0]["required_total"], baseline_fit["required_total"])
        self.assertEqual(materialized["host_fit"][0]["met_count"], baseline_fit["met_count"])

    def test_host_environment_audit_uses_materialized_rollups(self) -> None:
        self._upsert_session(
            session_id="session-host-env",
            source_host="beta-host",
            cwd="/workspace/demo",
        )

        with connect(self.settings.database_path) as connection:
            connection.execute("DELETE FROM events")
            audit = fetch_host_environment_audit(
                connection,
                "beta-host",
                self.settings,
            )

        self.assertEqual(audit["failure_groups"][0]["class_label"], "Command not found")
        self.assertEqual(audit["binaries"][0]["binary"], "python3")
        self.assertEqual(audit["signals"][0]["subject_label"], "python3")


if __name__ == "__main__":
    unittest.main()
