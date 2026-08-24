from __future__ import annotations

import socket
from pathlib import Path
import re
import tempfile
import threading
import time
import unittest

import requests
import uvicorn

from agent_operations_viewer import SYNC_API_VERSION, __version__
from agent_operations_viewer.api_tokens import create_api_token
from agent_operations_viewer.config import Settings
from agent_operations_viewer.db import connect, write_transaction
from agent_operations_viewer.local_auth import create_initial_admin, create_local_user
from agent_operations_viewer.projects import upsert_project_acl_member
from agent_operations_viewer.search_api_tokens import create_search_api_token
from agent_operations_viewer.turn_index import replace_session_search_chunks
from agent_operations_viewer.web.app import create_app
from tests.test_search import insert_search_turn
from tests.test_search_chunks import search_event


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def make_test_settings(*, data_dir: Path, port: int) -> Settings:
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
        server_port=port,
        server_base_url=f"http://127.0.0.1:{port}",
        sync_api_token=None,
        sync_interval_seconds=30,
        remote_timeout_seconds=15,
        remote_batch_size=25,
        log_level="warning",
        source_host="search-api-host",
        auth_mode="password",
        session_secret="test-session-secret",
        auth_proxy_user_header="X-Forwarded-User",
        auth_proxy_name_header="X-Forwarded-Name",
        auth_proxy_email_header="X-Forwarded-Email",
        auth_proxy_login_url=None,
        auth_proxy_logout_url=None,
        auth_cookie_secure=False,
    )


class SearchApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.port = find_free_port()
        data_dir = Path(self.temp_dir.name)
        (data_dir / "sessions").mkdir(parents=True, exist_ok=True)
        self.settings = make_test_settings(data_dir=data_dir, port=self.port)
        self.app = create_app(self.settings, preserve_sync_on_start=True)

        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                self.admin = create_initial_admin(
                    connection,
                    username="admin",
                    password="Password123!",
                )
                self.viewer = create_local_user(
                    connection,
                    username="viewer",
                    password="Password123!",
                    role="viewer",
                )
                viewer_token = create_search_api_token(
                    connection,
                    owner_user_id=str(self.viewer["id"]),
                    label="Viewer search",
                )
                sync_token = create_api_token(connection, "Sync only")
                insert_search_turn(
                    connection,
                    session_id="public-one",
                    project_id="public-project",
                    project_key="acme/public-hws",
                    project_label="acme/public-hws",
                    prompt="shared api needle",
                    response="public response one",
                )
                insert_search_turn(
                    connection,
                    session_id="public-two",
                    project_id="public-project",
                    project_key="acme/public-hws",
                    project_label="acme/public-hws",
                    prompt="shared api needle",
                    response="public response two",
                    timestamp="2026-08-23T13:00:00+00:00",
                )
                insert_search_turn(
                    connection,
                    session_id="private-one",
                    project_id="private-project",
                    project_key="acme/private-hws",
                    project_label="acme/private-hws",
                    visibility="private",
                    prompt="shared api needle",
                    response="classified private evidence",
                )
                self.long_marker = "api-full-content-marker-mercury"
                insert_search_turn(
                    connection,
                    session_id="chunk-api-session",
                    project_id="chunk-api-project",
                    project_key="acme/chunk-api",
                    project_label="acme/chunk-api",
                    prompt="legacy chunk prompt",
                    response="legacy chunk response",
                )
                replace_session_search_chunks(
                    connection,
                    "chunk-api-session",
                    [
                        search_event(
                            event_index=1,
                            record_type="event_msg",
                            payload_type="user_message",
                            kind="message",
                            role="user",
                            display_text="Inspect the complete API response.",
                        ),
                        search_event(
                            event_index=2,
                            record_type="response_item",
                            payload_type="message",
                            kind="message",
                            role="assistant",
                            display_text=("full response filler " * 900) + self.long_marker,
                            phase="final_answer",
                        ),
                    ],
                )
        self.viewer_token = str(viewer_token["token"])
        self.sync_token = str(sync_token["token"])
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.server = uvicorn.Server(
            uvicorn.Config(self.app, host="127.0.0.1", port=self.port, log_level="warning")
        )
        self.server_thread = threading.Thread(target=self.server.run, daemon=True)
        self.server_thread.start()
        self._wait_for_server()

    def tearDown(self) -> None:
        self.server.should_exit = True
        self.server_thread.join(timeout=5)
        self.temp_dir.cleanup()

    def _wait_for_server(self) -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                response = requests.get(f"{self.base_url}/api/health", timeout=0.5)
                if response.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.05)
        raise RuntimeError("Timed out waiting for search API test server")

    def _search(self, *, token: str | None = None, **params: object) -> requests.Response:
        headers = {"accept": "application/json"}
        if token:
            headers["authorization"] = f"Bearer {token}"
        return requests.get(
            f"{self.base_url}/api/v1/search",
            params={"q": "shared api needle", **params},
            headers=headers,
            timeout=2,
        )

    def test_read_token_is_required_and_sync_token_is_not_accepted(self) -> None:
        self.assertEqual(self._search().status_code, 401)
        self.assertEqual(self._search(token=self.sync_token).status_code, 401)

    def test_signed_in_user_can_create_a_personal_search_token(self) -> None:
        session = requests.Session()
        login_response = session.post(
            f"{self.base_url}/login",
            data={"username": "viewer", "password": "Password123!", "next": "/settings"},
            allow_redirects=False,
            timeout=2,
        )
        self.assertEqual(login_response.status_code, 303, login_response.text)
        create_response = session.post(
            f"{self.base_url}/settings/search-api-tokens",
            data={"label": "Created through settings"},
            timeout=2,
        )
        self.assertEqual(create_response.status_code, 200, create_response.text)
        self.assertIn("Created through settings", create_response.text)
        match = re.search(r"csvr_read_[A-Za-z0-9_-]+", create_response.text)
        self.assertIsNotNone(match)

        search_response = self._search(token=str(match.group(0)))
        self.assertEqual(search_response.status_code, 200, search_response.text)
        self.assertEqual(search_response.json()["total_count"], 2)

    def test_search_token_inherits_project_acl_and_returns_plain_json(self) -> None:
        response = self._search(token=self.viewer_token)

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["total_count"], 2)
        self.assertEqual(
            {hit["session_id"] for hit in payload["hits"]},
            {"public-one", "public-two"},
        )
        self.assertNotIn("classified private evidence", response.text)
        self.assertNotIn("<mark>", response.text)
        self.assertNotIn("[[", response.text)
        self.assertEqual(response.headers["cache-control"], "private, no-store")

        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                upsert_project_acl_member(
                    connection,
                    project_id="private-project",
                    user_id=str(self.viewer["id"]),
                    role="viewer",
                    granted_by_user_id=str(self.admin["id"]),
                )

        granted_response = self._search(token=self.viewer_token, project_id="private-project")
        self.assertEqual(granted_response.status_code, 200, granted_response.text)
        self.assertEqual(granted_response.json()["total_count"], 1)
        self.assertEqual(granted_response.json()["hits"][0]["session_id"], "private-one")

    def test_abstract_query_uses_acl_safe_project_history_fallback(self) -> None:
        response = self._search(
            token=self.viewer_token,
            q="What was the last thing we were going to do on the public-hws project?",
        )

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["retrieval"]["intent"], "latest_next_step")
        self.assertEqual(payload["retrieval"]["strategy"], "project_history")
        self.assertEqual(payload["retrieval"]["project"]["id"], "public-project")
        self.assertEqual(payload["hits"][0]["session_id"], "public-two")

        hidden_response = self._search(
            token=self.viewer_token,
            q="What issues remain on the private-hws project?",
        )
        self.assertEqual(hidden_response.status_code, 200, hidden_response.text)
        self.assertEqual(hidden_response.json()["total_count"], 0)
        self.assertEqual(
            hidden_response.json()["retrieval"]["project"]["resolution"],
            "unmatched",
        )

    def test_api_returns_full_content_chunk_provenance(self) -> None:
        response = self._search(token=self.viewer_token, q=self.long_marker)

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["total_count"], 1)
        hit = payload["hits"][0]
        self.assertEqual(hit["session_id"], "chunk-api-session")
        self.assertEqual(hit["match_source"], "chunk")
        self.assertEqual(hit["chunk"]["field"], "response")
        self.assertGreater(hit["chunk"]["start_offset"], 12_000)
        self.assertIn("marker", hit["snippet"])

    def test_cursor_is_bound_to_query_and_advances_results(self) -> None:
        first_response = self._search(token=self.viewer_token, limit=1)
        self.assertEqual(first_response.status_code, 200, first_response.text)
        first_payload = first_response.json()
        self.assertIsNotNone(first_payload["next_cursor"])
        self.assertEqual(len(first_payload["hits"]), 1)

        second_response = self._search(
            token=self.viewer_token,
            limit=1,
            cursor=first_payload["next_cursor"],
        )
        self.assertEqual(second_response.status_code, 200, second_response.text)
        second_payload = second_response.json()
        self.assertEqual(len(second_payload["hits"]), 1)
        self.assertNotEqual(
            first_payload["hits"][0]["session_id"],
            second_payload["hits"][0]["session_id"],
        )
        self.assertIsNone(second_payload["next_cursor"])

        mismatched_response = requests.get(
            f"{self.base_url}/api/v1/search",
            params={
                "q": "different query",
                "limit": 1,
                "cursor": first_payload["next_cursor"],
            },
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {self.viewer_token}",
            },
            timeout=2,
        )
        self.assertEqual(mismatched_response.status_code, 400)


if __name__ == "__main__":
    unittest.main()
