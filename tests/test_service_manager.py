from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from codex_session_viewer import SYNC_API_VERSION, __version__
from codex_session_viewer.config import Settings
from codex_session_viewer.service_manager import (
    SYSTEMD_USER_UNIT_NAME,
    ServiceCommandResult,
    install_service,
    service_status,
)


def make_test_settings(*, project_root: Path, data_dir: Path) -> Settings:
    return Settings(
        project_root=project_root,
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
        auth_mode="password",
        session_secret="test-session-secret",
        auth_proxy_user_header="X-Forwarded-User",
        auth_proxy_name_header="X-Forwarded-Name",
        auth_proxy_email_header="X-Forwarded-Email",
        auth_proxy_login_url=None,
        auth_proxy_logout_url=None,
        auth_cookie_secure=False,
    )


class ServiceManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.data_dir = self.root / "data"
        (self.data_dir / "sessions").mkdir(parents=True, exist_ok=True)
        self.settings = make_test_settings(project_root=self.root, data_dir=self.data_dir)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_install_service_serializes_command_results_for_systemd_user(self) -> None:
        unit_path = self.root / ".config" / "systemd" / "user" / SYSTEMD_USER_UNIT_NAME

        def fake_run_command(command: list[str], *, check: bool = False) -> ServiceCommandResult:
            self.assertFalse(check)
            return ServiceCommandResult(
                ok=True,
                command=command,
                returncode=0,
                stdout="ok",
                stderr="",
            )

        with patch("codex_session_viewer.service_manager._linux_unit_path", return_value=unit_path):
            with patch("codex_session_viewer.service_manager._run_command", side_effect=fake_run_command):
                result = install_service(self.settings, target="systemd-user")

        self.assertEqual(result["target"], "systemd-user")
        self.assertEqual(result["unit_path"], str(unit_path))
        self.assertEqual(
            result["daemon_reload"]["command"],
            ["systemctl", "--user", "daemon-reload"],
        )
        self.assertEqual(
            result["enable"]["command"],
            ["systemctl", "--user", "enable", SYSTEMD_USER_UNIT_NAME],
        )
        self.assertTrue(unit_path.exists())
        self.assertIn(
            str(self.settings.project_root / "scripts" / "start-agent-daemon.sh"),
            unit_path.read_text(encoding="utf-8"),
        )

    def test_service_status_serializes_nested_command_payloads(self) -> None:
        unit_path = self.root / ".config" / "systemd" / "user" / SYSTEMD_USER_UNIT_NAME

        def fake_run_command(command: list[str], *, check: bool = False) -> ServiceCommandResult:
            self.assertFalse(check)
            if command[-2:] == ["is-active", SYSTEMD_USER_UNIT_NAME]:
                return ServiceCommandResult(
                    ok=True,
                    command=command,
                    returncode=0,
                    stdout="active",
                    stderr="",
                )
            if command[-2:] == ["is-enabled", SYSTEMD_USER_UNIT_NAME]:
                return ServiceCommandResult(
                    ok=True,
                    command=command,
                    returncode=0,
                    stdout="enabled",
                    stderr="",
                )
            raise AssertionError(f"unexpected command: {command}")

        with patch("codex_session_viewer.service_manager._linux_unit_path", return_value=unit_path):
            with patch("codex_session_viewer.service_manager._run_command", side_effect=fake_run_command):
                result = service_status(target="systemd-user")

        self.assertEqual(result["target"], "systemd-user")
        self.assertTrue(result["installed"])
        self.assertTrue(result["running"])
        self.assertEqual(result["active"]["stdout"], "active")
        self.assertEqual(result["enabled"]["stdout"], "enabled")
        self.assertEqual(result["unit_path"], str(unit_path))


if __name__ == "__main__":
    unittest.main()
