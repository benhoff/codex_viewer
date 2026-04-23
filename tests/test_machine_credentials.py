from __future__ import annotations

import asyncio
from pathlib import Path
import tempfile
import unittest
from urllib.parse import urlsplit

from fastapi import HTTPException
from starlette.requests import Request

from codex_session_viewer import SYNC_API_VERSION, __version__
from codex_session_viewer.config import Settings
from codex_session_viewer.db import connect, write_transaction
from codex_session_viewer.local_machine import LocalMachineIdentity, store_machine_identity
from codex_session_viewer.machine_auth import build_machine_auth_headers, generate_machine_keypair
from codex_session_viewer.machine_credentials import (
    approve_pairing_session,
    create_machine_credential,
    create_pairing_session,
    finalize_pairing_session,
    hash_pairing_secret,
)
from codex_session_viewer.remote_sync import build_headers
from codex_session_viewer.web.app import create_app
from codex_session_viewer.web.auth import require_sync_api_auth


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


class MachineCredentialTests(unittest.TestCase):
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
        raw_path: str,
        *,
        method: str = "GET",
        body: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> Request:
        split = urlsplit(raw_path)
        encoded_body = body
        delivered = False

        async def receive() -> dict[str, object]:
            nonlocal delivered
            if delivered:
                return {"type": "http.request", "body": b"", "more_body": False}
            delivered = True
            return {"type": "http.request", "body": encoded_body, "more_body": False}

        raw_headers = [
            (b"host", b"viewer.test"),
            (b"accept", b"application/json"),
        ]
        for key, value in (headers or {}).items():
            raw_headers.append((key.lower().encode("utf-8"), value.encode("utf-8")))
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": method.upper(),
            "scheme": "http",
            "path": split.path,
            "raw_path": split.path.encode("utf-8"),
            "query_string": split.query.encode("utf-8"),
            "headers": raw_headers,
            "client": ("127.0.0.1", 12345),
            "server": ("viewer.test", 80),
            "root_path": "",
            "app": self.app,
        }
        request = Request(scope, receive)
        request.state.auth_enabled = self.settings.auth_enabled()
        request.state.auth_mode = self.settings.auth_mode
        request.state.auth_user = None
        request.state.bootstrap_required = False
        request.state.bootstrap_completed_at = None
        request.state.local_admin = None
        return request

    def test_pairing_session_approval_and_finalize_creates_machine_credential(self) -> None:
        keypair = generate_machine_keypair()
        secret = "pairing-secret"
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                pairing = create_pairing_session(
                    connection,
                    label="Builder laptop",
                    source_host="builder-laptop",
                    public_key=keypair.public_key,
                    secret_hash=hash_pairing_secret(secret),
                )
                approved = approve_pairing_session(
                    connection,
                    session_id=str(pairing["id"]),
                    secret=secret,
                    approver_user_id=None,
                )
                finalized = finalize_pairing_session(
                    connection,
                    session_id=str(pairing["id"]),
                    secret=secret,
                )

        self.assertEqual(approved["status"], "approved")
        self.assertEqual(finalized["status"], "approved")
        self.assertIsNotNone(finalized["machine_credential_id"])
        self.assertEqual(finalized["machine"]["label"], "Builder laptop")
        self.assertEqual(finalized["machine"]["source_host"], "builder-laptop")

    def test_require_sync_api_auth_accepts_machine_signature_and_rejects_replay(self) -> None:
        keypair = generate_machine_keypair()
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                machine = create_machine_credential(
                    connection,
                    label="Replay test",
                    source_host=self.settings.source_host,
                    public_key=keypair.public_key,
                    created_by_user_id=None,
                )

        signed_headers = {
            "x-codex-viewer-host": self.settings.source_host,
            **build_machine_auth_headers(
                private_key=keypair.private_key,
                machine_id=str(machine["id"]),
                method="GET",
                path="/api/sync/manifest?host=test-host",
                raw_body=None,
                source_host=self.settings.source_host,
            ),
        }

        first_request = self._make_request(
            "/api/sync/manifest?host=test-host",
            headers=signed_headers,
        )
        result = asyncio.run(require_sync_api_auth(first_request))
        self.assertEqual(result["auth_type"], "machine_credential")
        self.assertEqual(result["machine_id"], machine["id"])

        second_request = self._make_request(
            "/api/sync/manifest?host=test-host",
            headers=signed_headers,
        )
        with self.assertRaises(HTTPException) as exc_info:
            asyncio.run(require_sync_api_auth(second_request))
        self.assertEqual(exc_info.exception.status_code, 401)

    def test_remote_sync_build_headers_prefers_machine_identity(self) -> None:
        keypair = generate_machine_keypair()
        store_machine_identity(
            self.settings,
            LocalMachineIdentity(
                machine_id="mcred_test",
                label="Builder laptop",
                source_host=self.settings.source_host,
                server_base_url=str(self.settings.server_base_url),
                public_key=keypair.public_key,
                private_key=keypair.private_key,
                paired_at="2026-04-23T00:00:00+00:00",
                created_by_user_id=None,
            ),
        )
        self.settings.sync_api_token = "legacy-token"

        headers = build_headers(
            self.settings,
            method="GET",
            path="/api/sync/manifest?host=test-host",
            raw_body=None,
        )

        self.assertNotIn("Authorization", headers)
        self.assertEqual(headers["X-Codex-Viewer-Machine-Id"], "mcred_test")
        self.assertEqual(headers["X-Codex-Viewer-Host"], self.settings.source_host)


if __name__ == "__main__":
    unittest.main()
