from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import tempfile
import unittest

from codex_session_viewer import SYNC_API_VERSION, __version__
from codex_session_viewer.agents import (
    fetch_agents_dashboard,
    remote_needs_attention,
    request_remote_raw_resend,
    upsert_remote_agent_status,
)
from codex_session_viewer.config import Settings
from codex_session_viewer.db import connect, init_db, write_transaction
from codex_session_viewer.importer import parse_session_text, upsert_parsed_session
from codex_session_viewer.projects import sync_project_registry


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


class AgentsDashboardTests(unittest.TestCase):
    def test_remote_needs_attention_matches_remotes_attention_model(self) -> None:
        self.assertTrue(
            remote_needs_attention(
                {
                    "last_fail_count": 1,
                    "last_upload_count": 0,
                    "last_skip_count": 0,
                    "last_error": "Upload failures occurred",
                }
            )
        )
        self.assertFalse(
            remote_needs_attention(
                {
                    "stale": True,
                    "last_seen_at": "2026-04-20T00:00:00+00:00",
                    "sync_api_version": "2",
                    "server_api_version_seen": "3",
                    "api_mismatch": True,
                    "update_state": "current",
                }
            )
        )

    def test_fresh_heartbeat_hosts_stay_active_when_idle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            settings = make_test_settings(
                data_dir=data_dir,
                session_roots=[Path(tmpdir) / "sessions"],
            )
            init_db(settings.database_path)

            now_iso = datetime.now(tz=UTC).replace(microsecond=0).isoformat()
            raw_jsonl = json.dumps(
                {
                    "type": "session_meta",
                    "payload": {
                        "id": "idle-online-session",
                        "timestamp": now_iso,
                        "cwd": "/workspace/idle-online-repo",
                        "originator": "tester",
                        "cli_version": "1.0.0",
                        "source": "cli",
                        "model_provider": "openai",
                    },
                }
            )
            parsed = parse_session_text(
                raw_jsonl,
                Path("/tmp/idle-online-session.jsonl"),
                Path("/tmp"),
                "idle-online-host",
                file_size=len(raw_jsonl.encode("utf-8")),
                file_mtime_ns=0,
            )

            with connect(settings.database_path) as connection:
                with write_transaction(connection):
                    upsert_parsed_session(connection, parsed)
                    sync_project_registry(connection)
                    upsert_remote_agent_status(
                        connection,
                        source_host="idle-online-host",
                        agent_version=__version__,
                        sync_api_version=SYNC_API_VERSION,
                        sync_mode="remote",
                        update_state="current",
                        update_message="Agent is current",
                        server_version_seen=__version__,
                        server_api_version_seen=SYNC_API_VERSION,
                        last_seen_at=now_iso,
                        last_sync_at=now_iso,
                    )
                dashboard = fetch_agents_dashboard(connection, settings)

            active_hosts = {item["source_host"]: item for item in dashboard["active"]}
            dormant_hosts = {item["source_host"] for item in dashboard["dormant"]}

            self.assertIn("idle-online-host", active_hosts)
            self.assertNotIn("idle-online-host", dormant_hosts)
            self.assertEqual(active_hosts["idle-online-host"]["recent_turn_count"], 0)
            self.assertEqual(
                active_hosts["idle-online-host"]["summary"],
                "Heartbeat healthy · no recent turns in the last 24h",
            )

    def test_awaiting_contact_hosts_remain_dormant(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            settings = make_test_settings(
                data_dir=data_dir,
                session_roots=[Path(tmpdir) / "sessions"],
            )
            init_db(settings.database_path)

            with connect(settings.database_path) as connection:
                with write_transaction(connection):
                    request_remote_raw_resend(connection, "awaiting-contact-host")
                dashboard = fetch_agents_dashboard(connection, settings)

            active_hosts = {item["source_host"] for item in dashboard["active"]}
            dormant_hosts = {item["source_host"] for item in dashboard["dormant"]}

            self.assertNotIn("awaiting-contact-host", active_hosts)
            self.assertIn("awaiting-contact-host", dormant_hosts)


if __name__ == "__main__":
    unittest.main()
