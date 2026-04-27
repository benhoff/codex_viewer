from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from contextlib import redirect_stderr
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from agent_operations_viewer import SYNC_API_VERSION, __version__
from agent_daemon.commands import cli
from agent_operations_viewer.config import Settings
from agent_operations_viewer.commands import parse_args as viewer_parse_args


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


class ServiceCommandFeedbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.data_dir = self.root / "data"
        (self.data_dir / "sessions").mkdir(parents=True, exist_ok=True)
        self.settings = make_test_settings(project_root=self.root, data_dir=self.data_dir)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_service_install_cli_prints_summary_without_raw_json(self) -> None:
        install_result = {
            "target": "systemd-user",
            "unit_path": "/tmp/agent-operations-viewer-agent.service",
            "daemon_reload": {"ok": True},
            "enable": {"ok": True},
        }
        stdout = io.StringIO()
        args = argparse.Namespace(command="service", service_command="install")

        with patch("agent_daemon.commands.parse_args", return_value=args):
            with patch("agent_daemon.commands.Settings.from_env", return_value=self.settings):
                with patch("agent_daemon.commands.install_service", return_value=install_result):
                    with redirect_stdout(stdout):
                        exit_code = cli()

        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("Installed the background daemon service for your user via systemd-user.", output)
        self.assertIn("Definition file: /tmp/agent-operations-viewer-agent.service", output)
        self.assertIn("It is not started yet.", output)
        self.assertNotIn("Details:", output)
        self.assertNotIn('"target": "systemd-user"', output)

    def test_service_status_cli_prints_summary_without_raw_json(self) -> None:
        status_result = {
            "target": "systemd-user",
            "installed": True,
            "running": False,
            "unit_path": "/tmp/agent-operations-viewer-agent.service",
            "active": {"ok": False},
            "enabled": {"ok": True},
        }
        stdout = io.StringIO()
        args = argparse.Namespace(command="service", service_command="status")

        with patch("agent_daemon.commands.parse_args", return_value=args):
            with patch("agent_daemon.commands.Settings.from_env", return_value=self.settings):
                with patch("agent_daemon.commands.service_status", return_value=status_result):
                    with redirect_stdout(stdout):
                        exit_code = cli()

        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("Background daemon status via systemd-user: installed=yes, running=no.", output)
        self.assertIn("Definition: /tmp/agent-operations-viewer-agent.service", output)
        self.assertNotIn("Details:", output)
        self.assertNotIn('"running": false', output)

    def test_flat_status_cli_prints_agent_status(self) -> None:
        stdout = io.StringIO()
        args = argparse.Namespace(command="status", json=False)
        status_result = {"paired": False, "service": {"installed": False, "running": False}}

        with patch("agent_daemon.commands.parse_args", return_value=args):
            with patch("agent_daemon.commands.Settings.from_env", return_value=self.settings):
                with patch("agent_daemon.commands.collect_agent_status", return_value=status_result):
                    with patch("agent_daemon.commands.format_agent_status", return_value="agent status text"):
                        with redirect_stdout(stdout):
                            exit_code = cli()

        self.assertEqual(exit_code, 0)
        self.assertEqual(stdout.getvalue().strip(), "agent status text")

    def test_flat_doctor_cli_returns_nonzero_when_checks_fail(self) -> None:
        stdout = io.StringIO()
        args = argparse.Namespace(command="doctor", json=False)
        doctor_result = {"ok": False, "checks": [], "next_actions": ["Run setup."]}

        with patch("agent_daemon.commands.parse_args", return_value=args):
            with patch("agent_daemon.commands.Settings.from_env", return_value=self.settings):
                with patch("agent_daemon.commands.doctor_agent", return_value=doctor_result):
                    with patch("agent_daemon.commands.format_agent_doctor", return_value="doctor text"):
                        with redirect_stdout(stdout):
                            exit_code = cli()

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue().strip(), "doctor text")

    def test_flat_setup_cli_delegates_to_daemon_machine_setup(self) -> None:
        stdout = io.StringIO()
        args = argparse.Namespace(
            command="setup",
            server="https://viewer.example.com",
            label="build-box",
            no_browser=True,
            timeout=60,
            force=True,
            reinstall_service=True,
            json=True,
        )
        setup_result = {"actions": ["paired_machine"], "status": {"paired": True}}

        with patch("agent_daemon.commands.parse_args", return_value=args):
            with patch("agent_daemon.commands.Settings.from_env", return_value=self.settings):
                with patch("agent_daemon.commands.machine_setup", return_value=setup_result) as setup_mock:
                    with redirect_stdout(stdout):
                        exit_code = cli()

        self.assertEqual(exit_code, 0)
        setup_mock.assert_called_once_with(
            self.settings,
            label="build-box",
            open_browser=False,
            timeout_seconds=60,
            force=True,
            reinstall_service=True,
        )
        self.assertEqual(self.settings.server_base_url, "https://viewer.example.com")
        self.assertIn('"paired_machine"', stdout.getvalue())

    def test_main_cli_rejects_service_commands(self) -> None:
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["agent_operations_viewer", "service", "status"]):
            with redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as exc_info:
                    viewer_parse_args()

        self.assertNotEqual(exc_info.exception.code, 0)
        self.assertIn("invalid choice", stderr.getvalue())

    def test_main_cli_rejects_daemon_command(self) -> None:
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["agent_operations_viewer", "daemon"]):
            with redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as exc_info:
                    viewer_parse_args()

        self.assertNotEqual(exc_info.exception.code, 0)
        self.assertIn("invalid choice", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
