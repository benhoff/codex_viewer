from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from starlette.requests import Request

from codex_session_viewer.api_tokens import create_api_token
from codex_session_viewer import SYNC_API_VERSION, __version__
from codex_session_viewer.config import Settings
from codex_session_viewer.db import connect, write_transaction
from codex_session_viewer.local_auth import create_initial_admin
from codex_session_viewer.web.app import create_app
from codex_session_viewer.web.routes.pages import index, setup_page, setup_status


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
        auth_mode="password",
        session_secret="test-session-secret",
        auth_proxy_user_header="X-Forwarded-User",
        auth_proxy_name_header="X-Forwarded-Name",
        auth_proxy_email_header="X-Forwarded-Email",
        auth_proxy_login_url=None,
        auth_proxy_logout_url=None,
        auth_cookie_secure=False,
    )


class SetupAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        data_dir = Path(self.temp_dir.name)
        (data_dir / "sessions").mkdir(parents=True, exist_ok=True)
        self.settings = make_test_settings(data_dir=data_dir)
        self.app = create_app(self.settings, preserve_sync_on_start=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _make_request(
        self,
        path: str,
        *,
        bootstrap_required: bool,
        auth_user: dict[str, object] | None = None,
    ) -> Request:
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode("ascii"),
            "query_string": b"",
            "headers": [
                (b"host", b"viewer.test"),
                (b"accept", b"text/html"),
            ],
            "client": ("127.0.0.1", 12345),
            "server": ("viewer.test", 80),
            "root_path": "",
            "app": self.app,
        }
        request = Request(scope)
        request.state.auth_enabled = self.settings.auth_enabled()
        request.state.auth_mode = self.settings.auth_mode
        request.state.auth_user = auth_user
        request.state.bootstrap_required = bootstrap_required
        request.state.bootstrap_completed_at = None
        request.state.local_admin = None
        return request

    def _create_password_admin(self) -> None:
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                create_initial_admin(
                    connection,
                    username="admin",
                    password="Password123!",
                )

    def _create_setup_token(self) -> None:
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                create_api_token(connection, "First machine token")

    def _admin_auth_user(self) -> dict[str, object]:
        return {
            "user_id": "1",
            "username": "admin",
            "display_name": "admin",
            "auth_source": "password",
            "role": "admin",
            "is_admin": True,
        }

    def test_setup_page_stays_public_during_password_bootstrap(self) -> None:
        response = setup_page(self._make_request("/setup", bootstrap_required=True))

        self.assertEqual(response.status_code, 200)
        self.assertIn("Create the first admin", response.body.decode("utf-8"))

    def test_setup_page_redirects_to_login_after_password_admin_creation(self) -> None:
        self._create_password_admin()

        response = setup_page(self._make_request("/setup", bootstrap_required=False))

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers.get("location"), "/login?next=/setup")

    def test_setup_status_redirects_to_login_after_password_admin_creation(self) -> None:
        self._create_password_admin()

        response = setup_status(self._make_request("/setup/status", bootstrap_required=False))

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers.get("location"), "/login?next=/setup/status")

    def test_authenticated_setup_page_redirects_home_after_token_creation(self) -> None:
        self._create_password_admin()
        self._create_setup_token()

        response = setup_page(
            self._make_request(
                "/setup",
                bootstrap_required=False,
                auth_user=self._admin_auth_user(),
            )
        )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers.get("location"), "/")

    def test_dashboard_opens_after_token_creation_before_first_machine_sync(self) -> None:
        self._create_password_admin()
        self._create_setup_token()

        response = index(
            self._make_request(
                "/",
                bootstrap_required=False,
                auth_user=self._admin_auth_user(),
            ),
            q=None,
            host=None,
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("First machine still pending", response.body.decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
