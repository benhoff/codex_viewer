from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from agent_operations_viewer import SYNC_API_VERSION, __version__
from agent_operations_viewer.agents import fetch_agents_dashboard, upsert_remote_agent_status
from agent_operations_viewer.config import Settings
from agent_operations_viewer.db import connect, init_db, write_transaction
from agent_operations_viewer.machine_aliases import (
    fetch_machine_display_alias,
    set_machine_display_alias,
)


def make_test_settings(*, data_dir: Path) -> Settings:
    return Settings(
        project_root=Path.cwd(),
        environment_name="test",
        data_dir=data_dir,
        database_path=data_dir / "viewer.sqlite3",
        session_roots=[data_dir / "sessions"],
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
        server_base_url="http://viewer.test:8000",
        sync_api_token=None,
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


class MachineAliasTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name)
        (self.data_dir / "sessions").mkdir(parents=True, exist_ok=True)
        self.settings = make_test_settings(data_dir=self.data_dir)
        init_db(self.settings.database_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_dashboard_uses_display_alias_without_changing_source_host(self) -> None:
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                upsert_remote_agent_status(
                    connection,
                    source_host="builder-1",
                    agent_version=self.settings.expected_agent_version,
                    sync_api_version=self.settings.sync_api_version,
                    sync_mode=self.settings.sync_mode,
                    update_state="current",
                    update_message=None,
                    server_version_seen=self.settings.expected_agent_version,
                    server_api_version_seen=self.settings.sync_api_version,
                )
                set_machine_display_alias(
                    connection,
                    source_host="builder-1",
                    display_alias="GPU Workstation",
                )

            dashboard = fetch_agents_dashboard(connection, self.settings)

        [machine] = dashboard["active"]
        self.assertEqual(machine["source_host"], "builder-1")
        self.assertEqual(machine["display_alias"], "GPU Workstation")
        self.assertEqual(machine["display_name"], "GPU Workstation")

    def test_blank_alias_clears_display_alias(self) -> None:
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                set_machine_display_alias(
                    connection,
                    source_host="builder-1",
                    display_alias="GPU Workstation",
                )
                set_machine_display_alias(
                    connection,
                    source_host="builder-1",
                    display_alias="  ",
                )

            alias = fetch_machine_display_alias(connection, "builder-1")

        self.assertIsNone(alias)


if __name__ == "__main__":
    unittest.main()
