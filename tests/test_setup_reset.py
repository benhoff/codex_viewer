from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import tempfile
import unittest

from codex_session_viewer import SYNC_API_VERSION, __version__
from codex_session_viewer.agents import upsert_remote_agent_status
from codex_session_viewer.api_tokens import create_api_token
from codex_session_viewer.config import Settings
from codex_session_viewer.db import connect, init_db, write_transaction
from codex_session_viewer.local_auth import create_initial_admin, fetch_auth_status
from codex_session_viewer.onboarding import reconcile_onboarding_state
from codex_session_viewer.session_artifacts import store_session_artifact
from codex_session_viewer.setup_reset import (
    prune_empty_artifact_dirs,
    remove_artifact_files,
    reset_setup_state,
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
        auth_mode="password",
        session_secret="test-session-secret",
        auth_proxy_user_header="X-Forwarded-User",
        auth_proxy_name_header="X-Forwarded-Name",
        auth_proxy_email_header="X-Forwarded-Email",
        auth_proxy_login_url=None,
        auth_proxy_logout_url=None,
        auth_cookie_secure=False,
    )


class ResetSetupStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name)
        (self.data_dir / "sessions").mkdir(parents=True, exist_ok=True)
        self.settings = make_test_settings(data_dir=self.data_dir)
        init_db(self.settings.database_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _seed_remote_onboarding_complete(self) -> Path:
        now = datetime.now(tz=UTC).replace(microsecond=0).isoformat()
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                create_initial_admin(
                    connection,
                    username="admin",
                    password="Password123!",
                )
                create_api_token(connection, "Seed token")
                upsert_remote_agent_status(
                    connection,
                    source_host="seed-host",
                    agent_version=self.settings.expected_agent_version,
                    sync_api_version=self.settings.sync_api_version,
                    sync_mode=self.settings.sync_mode,
                    update_state="current",
                    update_message=None,
                    server_version_seen=self.settings.app_version,
                    server_api_version_seen=self.settings.sync_api_version,
                    last_seen_at=now,
                    last_sync_at=now,
                    last_upload_count=1,
                    last_skip_count=0,
                    last_fail_count=0,
                    last_error=None,
                )
                artifact_sha256 = store_session_artifact(connection, self.settings, '{"event":"seed"}\n')
                artifact_row = connection.execute(
                    "SELECT storage_path FROM session_artifacts WHERE sha256 = ?",
                    (artifact_sha256,),
                ).fetchone()
                connection.execute(
                    """
                    INSERT INTO sessions (
                        id,
                        source_path,
                        source_root,
                        file_size,
                        file_mtime_ns,
                        content_sha256,
                        raw_artifact_sha256,
                        source_host,
                        summary,
                        raw_meta_json,
                        imported_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "session-1",
                        "/tmp/seed-host/session-1.jsonl",
                        "/tmp/seed-host",
                        17,
                        1,
                        "content-sha",
                        artifact_sha256,
                        "seed-host",
                        "seeded session",
                        "{}",
                        now,
                        now,
                    ),
                )
                onboarding = reconcile_onboarding_state(connection, self.settings)
                self.assertFalse(onboarding["onboarding_required"])
                self.assertTrue(bool(onboarding["completed_at"]))
                return self.settings.data_dir / str(artifact_row["storage_path"])

    def test_reset_setup_state_rewinds_onboarding_but_keeps_admin(self) -> None:
        artifact_path = self._seed_remote_onboarding_complete()
        self.assertTrue(artifact_path.exists())

        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                result = reset_setup_state(connection, self.settings)

        removed_files = remove_artifact_files(result.artifact_paths)
        prune_empty_artifact_dirs(self.settings)

        self.assertEqual(result.users_removed, 0)
        self.assertEqual(result.tokens_removed, 1)
        self.assertEqual(result.remote_agents_removed, 1)
        self.assertEqual(result.sessions_removed, 1)
        self.assertEqual(result.session_artifacts_removed, 1)
        self.assertEqual(removed_files, 1)
        self.assertFalse(artifact_path.exists())

        with connect(self.settings.database_path) as connection:
            auth_status = fetch_auth_status(connection)
            onboarding = reconcile_onboarding_state(connection, self.settings)
            token_count = connection.execute("SELECT COUNT(*) FROM api_tokens").fetchone()[0]
            remote_agent_count = connection.execute("SELECT COUNT(*) FROM remote_agents").fetchone()[0]
            session_count = connection.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            artifact_count = connection.execute("SELECT COUNT(*) FROM session_artifacts").fetchone()[0]

        self.assertFalse(auth_status.bootstrap_required)
        self.assertIsNotNone(auth_status.admin_user)
        self.assertTrue(onboarding["onboarding_required"])
        self.assertFalse(onboarding["bootstrap_required"])
        self.assertEqual(token_count, 0)
        self.assertEqual(remote_agent_count, 0)
        self.assertEqual(session_count, 0)
        self.assertEqual(artifact_count, 0)
        self.assertIsNone(onboarding["completed_at"])

    def test_full_bootstrap_removes_users_and_clears_auth_state(self) -> None:
        self._seed_remote_onboarding_complete()

        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                result = reset_setup_state(
                    connection,
                    self.settings,
                    reset_tokens=False,
                    reset_remote_agents=False,
                    reset_sessions=False,
                    full_bootstrap=True,
                )

            auth_status = fetch_auth_status(connection)
            onboarding = reconcile_onboarding_state(connection, self.settings)
            user_count = connection.execute("SELECT COUNT(*) FROM users").fetchone()[0]

        self.assertEqual(result.users_removed, 1)
        self.assertEqual(user_count, 0)
        self.assertTrue(auth_status.bootstrap_required)
        self.assertIsNone(auth_status.bootstrap_completed_at)
        self.assertIsNone(auth_status.admin_user)
        self.assertTrue(onboarding["onboarding_required"])
        self.assertTrue(onboarding["bootstrap_required"])


if __name__ == "__main__":
    unittest.main()
