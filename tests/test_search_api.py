from __future__ import annotations

import socket
import json
from pathlib import Path
import re
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import requests
import uvicorn

from agent_operations_viewer import SYNC_API_VERSION, __version__
from agent_operations_viewer.api_tokens import create_api_token
from agent_operations_viewer.config import Settings
from agent_operations_viewer.db import connect, write_transaction
from agent_operations_viewer.local_auth import create_initial_admin, create_local_user
from agent_operations_viewer.projects import upsert_project_acl_member
from agent_operations_viewer.repositories import sync_repository_registry
from agent_operations_viewer.search_api_tokens import create_search_api_token
from agent_operations_viewer.turn_index import replace_session_search_chunks
from agent_operations_viewer.search_snapshots import digest, NORMALIZATION_VERSION
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
                    prompt="shared api needle grouping",
                    response="public response one",
                    commands="pytest -q",
                    paths="app.py",
                    commit_ids="2d48e17abc123",
                    tool_output="1 passed",
                )
                connection.execute(
                    """
                    UPDATE sessions
                    SET
                        git_repository_url = ?,
                        git_branch = ?,
                        git_commit_hash = ?
                    WHERE id = ?
                    """,
                    (
                        "https://github.com/acme/public-hws.git",
                        "feature/search-api",
                        "2d48e17abc123",
                        "public-one",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO session_turns (
                        session_id,
                        turn_number,
                        start_event_index,
                        end_event_index,
                        prompt_excerpt,
                        prompt_timestamp,
                        response_excerpt,
                        response_timestamp,
                        response_state,
                        latest_timestamp
                    ) VALUES (?, 2, 2, 9, ?, ?, ?, ?, 'final', ?)
                    """,
                    (
                        "public-one",
                        "neighboring prompt",
                        "2026-08-23T12:02:00+00:00",
                        "neighboring response",
                        "2026-08-23T12:03:00+00:00",
                        "2026-08-23T12:03:00+00:00",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO session_turn_search (
                        project_text,
                        prompt_text,
                        response_text,
                        event_text,
                        session_id,
                        turn_number
                    ) VALUES (?, ?, ?, '', ?, ?)
                    """,
                    (
                        "acme/public-hws",
                        "grouping api needle neighboring prompt",
                        "grouping api needle neighboring response",
                        "public-one",
                        2,
                    ),
                )
                connection.executemany(
                    """
                    INSERT INTO events (
                        session_id,
                        event_index,
                        timestamp,
                        record_type,
                        payload_type,
                        kind,
                        role,
                        title,
                        display_text,
                        detail_text,
                        tool_name,
                        call_id,
                        command_text,
                        exit_code,
                        record_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            "public-one",
                            0,
                            "2026-08-23T12:00:00+00:00",
                            "event_msg",
                            "user_message",
                            "message",
                            "user",
                            "User",
                            "shared api needle",
                            "",
                            None,
                            None,
                            None,
                            None,
                            '{"payload":{"type":"user_message"}}',
                        ),
                        (
                            "public-one",
                            1,
                            "2026-08-23T12:01:00+00:00",
                            "response_item",
                            "message",
                            "message",
                            "assistant",
                            "Assistant",
                            "public response one with complete turn detail",
                            "",
                            None,
                            None,
                            None,
                            None,
                            '{"payload":{"type":"message","role":"assistant","phase":"final_answer"}}',
                        ),
                        (
                            "public-one",
                            2,
                            "2026-08-23T12:02:00+00:00",
                            "event_msg",
                            "user_message",
                            "message",
                            "user",
                            "User",
                            "neighboring prompt",
                            "",
                            None,
                            None,
                            None,
                            None,
                            '{"payload":{"type":"user_message"}}',
                        ),
                        (
                            "public-one",
                            3,
                            "2026-08-23T12:02:10+00:00",
                            "response_item",
                            "function_call",
                            "tool_call",
                            None,
                            "Shell",
                            '{"cmd":"pytest -q","workdir":"/workspace/acme/public-hws"}',
                            '{"cmd":"pytest -q","workdir":"/workspace/acme/public-hws"}',
                            "exec_command",
                            "call-command",
                            "pytest -q",
                            None,
                            '{"payload":{"type":"function_call"}}',
                        ),
                        (
                            "public-one",
                            4,
                            "2026-08-23T12:02:20+00:00",
                            "event_msg",
                            "exec_command_end",
                            "command",
                            None,
                            "Command",
                            "pytest -q",
                            "1 passed",
                            "exec_command",
                            "call-command",
                            "pytest -q",
                            0,
                            (
                                '{"payload":{"cwd":"/workspace/acme/public-hws",'
                                '"status":"completed","duration":"1.2s",'
                                '"parsed_cmd":[{"type":"test"}]}}'
                            ),
                        ),
                        (
                            "public-one",
                            5,
                            "2026-08-23T12:02:20+00:00",
                            "response_item",
                            "function_call_output",
                            "tool_result",
                            None,
                            "Tool result",
                            "1 passed",
                            "1 passed",
                            "exec_command",
                            "call-command",
                            None,
                            0,
                            '{"payload":{"type":"function_call_output"}}',
                        ),
                        (
                            "public-one",
                            6,
                            "2026-08-23T12:02:30+00:00",
                            "response_item",
                            "function_call",
                            "tool_call",
                            None,
                            "Patch",
                            "*** Begin Patch\n*** Update File: app.py\n@@\n-old\n+new\n*** End Patch",
                            "",
                            "apply_patch",
                            "call-patch",
                            None,
                            None,
                            '{"payload":{"type":"function_call"}}',
                        ),
                        (
                            "public-one",
                            7,
                            "2026-08-23T12:02:40+00:00",
                            "event_msg",
                            "patch_apply_end",
                            "system",
                            None,
                            "Patch applied",
                            "Status: completed",
                            '{"app.py":{"type":"update","unified_diff":"@@ -1 +1 @@\\n-old\\n+new\\n"}}',
                            "apply_patch",
                            "call-patch",
                            None,
                            None,
                            '{"payload":{"success":true,"status":"completed"}}',
                        ),
                        (
                            "public-one",
                            8,
                            "2026-08-23T12:02:40+00:00",
                            "response_item",
                            "function_call_output",
                            "tool_result",
                            None,
                            "Tool result",
                            "Updated app.py",
                            "Updated app.py",
                            "apply_patch",
                            "call-patch",
                            None,
                            None,
                            '{"payload":{"type":"function_call_output"}}',
                        ),
                        (
                            "public-one",
                            9,
                            "2026-08-23T12:03:00+00:00",
                            "response_item",
                            "message",
                            "message",
                            "assistant",
                            "Assistant",
                            "neighboring response",
                            "",
                            None,
                            None,
                            None,
                            None,
                            '{"payload":{"type":"message","role":"assistant","phase":"final_answer"}}',
                        ),
                    ],
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
                    session_id="grouping-newest",
                    project_id="grouping-project",
                    project_key="acme/grouping",
                    project_label="acme/grouping",
                    prompt="grouping api needle",
                    response="newest grouping evidence",
                    timestamp="2026-08-23T14:00:00+00:00",
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
                # Canonical repository identity must not weaken the private
                # project's independent ACL even when it shares a remote.
                connection.execute(
                    "UPDATE sessions SET git_repository_url = ? WHERE id = ?",
                    ("git@github.com:acme/public-hws.git", "private-one"),
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
                chunk_events = [
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
                ]
                replace_session_search_chunks(
                    connection, "chunk-api-session", chunk_events
                )
                # Search hits now identify complete evidence, so indexed fixtures
                # must retain their source events as a real import does.
                for event in chunk_events:
                    connection.execute(
                        "INSERT INTO events (session_id, event_index, timestamp, record_type, payload_type, kind, role, display_text, record_json, title, detail_text) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', '')",
                        (
                            "chunk-api-session",
                            event["event_index"],
                            event["timestamp"],
                            event["record_type"],
                            event["payload_type"],
                            event["kind"],
                            event["role"],
                            event["display_text"],
                            event["record_json"],
                        ),
                    )
                connection.execute(
                    "UPDATE session_turns SET start_event_index = 1, end_event_index = 2 WHERE session_id = 'chunk-api-session'"
                )
                for sid in ("public-two", "grouping-newest", "private-one"):
                    indexed = connection.execute(
                        "SELECT * FROM session_turn_search WHERE session_id = ?", (sid,)
                    ).fetchone()
                    for index, role, field in (
                        (0, "user", "prompt_text"),
                        (1, "assistant", "response_text"),
                    ):
                        connection.execute(
                            "INSERT INTO events (session_id, event_index, timestamp, record_type, payload_type, kind, role, display_text, record_json, title, detail_text) VALUES (?, ?, ?, 'response_item', 'message', 'message', ?, ?, ?, '', '')",
                            (
                                sid,
                                index,
                                "2026-08-23T12:00:00+00:00",
                                role,
                                indexed[field],
                                json.dumps(
                                    {
                                        "payload": {
                                            "type": "message",
                                            "role": role,
                                            "phase": "final_answer"
                                            if role == "assistant"
                                            else None,
                                        }
                                    }
                                ),
                            ),
                        )
                sync_repository_registry(connection)
        self.viewer_token = str(viewer_token["token"])
        self.sync_token = str(sync_token["token"])
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.server = uvicorn.Server(
            uvicorn.Config(
                self.app, host="127.0.0.1", port=self.port, log_level="warning"
            )
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

    def _search(
        self, *, token: str | None = None, **params: object
    ) -> requests.Response:
        headers = {"accept": "application/json"}
        if token:
            headers["authorization"] = f"Bearer {token}"
        return requests.get(
            f"{self.base_url}/api/v1/search",
            params={"q": "shared api needle", **params},
            headers=headers,
            timeout=2,
        )

    def _turn(self, *, token: str | None = None, **params: object) -> requests.Response:
        headers = {"accept": "application/json"}
        if token:
            headers["authorization"] = f"Bearer {token}"
        return requests.get(
            f"{self.base_url}/api/v1/sessions/public-one/turns/1",
            params=params,
            headers=headers,
            timeout=2,
        )

    def _batch(self, *, token: str | None = None, **body: object) -> requests.Response:
        headers = {"accept": "application/json"}
        if token:
            headers["authorization"] = f"Bearer {token}"
        return requests.post(
            f"{self.base_url}/api/v1/search/batch",
            json=body,
            headers=headers,
            timeout=2,
        )

    def _projects(
        self, *, token: str | None = None, **params: object
    ) -> requests.Response:
        headers = {"accept": "application/json"}
        if token:
            headers["authorization"] = f"Bearer {token}"
        return requests.get(
            f"{self.base_url}/api/v1/projects",
            params=params,
            headers=headers,
            timeout=2,
        )

    def test_read_token_is_required_and_sync_token_is_not_accepted(self) -> None:
        self.assertEqual(self._search().status_code, 401)
        self.assertEqual(self._search(token=self.sync_token).status_code, 401)
        self.assertEqual(self._projects().status_code, 401)
        self.assertEqual(self._projects(token=self.sync_token).status_code, 401)

    def test_projects_api_and_repository_filters_preserve_project_acls(self) -> None:
        response = self._projects(token=self.viewer_token)

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["total_count"], 3)
        self.assertNotIn("private-project", response.text)
        public_project = next(
            project
            for project in payload["projects"]
            if project["id"] == "public-project"
        )
        repository_id = public_project["repository_id"]
        self.assertIsNotNone(repository_id)
        self.assertEqual(public_project["repository"]["host"], "github.com")
        self.assertEqual(public_project["repository"]["path"], "acme/public-hws")
        self.assertEqual(public_project["session_count"], 2)
        self.assertEqual(len(public_project["sources"]), 2)

        remote_response = self._projects(
            token=self.viewer_token,
            remote="git@github.com:acme/public-hws.git",
        )
        self.assertEqual(remote_response.status_code, 200, remote_response.text)
        self.assertEqual(
            [project["id"] for project in remote_response.json()["projects"]],
            ["public-project"],
        )

        remote_search = self._search(
            token=self.viewer_token,
            remote="https://github.com/acme/public-hws.git",
        )
        self.assertEqual(remote_search.status_code, 200, remote_search.text)
        self.assertEqual(remote_search.json()["total_count"], 2)
        self.assertEqual(
            remote_search.json()["filters"]["remote"],
            "github.com/acme/public-hws",
        )

        repository_search = self._search(
            token=self.viewer_token,
            repository_id=repository_id,
        )
        self.assertEqual(repository_search.status_code, 200, repository_search.text)
        self.assertEqual(repository_search.json()["total_count"], 2)
        self.assertEqual(
            {hit["repository"]["id"] for hit in repository_search.json()["hits"]},
            {repository_id},
        )

        root_search = self._search(
            token=self.viewer_token,
            root="/workspace/acme/public-hws/",
        )
        self.assertEqual(root_search.status_code, 200, root_search.text)
        self.assertEqual(root_search.json()["total_count"], 2)

        first_page = self._projects(token=self.viewer_token, limit=1)
        self.assertEqual(first_page.status_code, 200, first_page.text)
        first_payload = first_page.json()
        self.assertEqual(first_payload["total_count"], 3)
        self.assertIsNotNone(first_payload["next_cursor"])
        second_page = self._projects(
            token=self.viewer_token,
            limit=1,
            cursor=first_payload["next_cursor"],
        )
        self.assertEqual(second_page.status_code, 200, second_page.text)
        self.assertNotEqual(
            first_payload["projects"][0]["id"],
            second_page.json()["projects"][0]["id"],
        )
        mismatched_cursor = self._projects(
            token=self.viewer_token,
            limit=1,
            remote="https://github.com/acme/public-hws.git",
            cursor=first_payload["next_cursor"],
        )
        self.assertEqual(mismatched_cursor.status_code, 400)

        invalid_remote = self._projects(
            token=self.viewer_token,
            remote="not-a-remote",
        )
        self.assertEqual(invalid_remote.status_code, 422)

    def test_signed_in_user_can_create_a_personal_search_token(self) -> None:
        session = requests.Session()
        login_response = session.post(
            f"{self.base_url}/login",
            data={
                "username": "viewer",
                "password": "Password123!",
                "next": "/settings",
            },
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
        coverage = payload["coverage"]
        self.assertEqual(coverage["sessions_total"], 4)
        self.assertEqual(coverage["sessions_indexed"], 4)
        self.assertEqual(coverage["turns_total"], 5)
        self.assertEqual(coverage["turns_indexed"], 5)
        self.assertEqual(coverage["pending_reindex_sessions"], 0)
        self.assertIsNotNone(coverage["last_indexed_at"])
        self.assertEqual(coverage["freshness"]["state"], "current")
        self.assertEqual(
            {project["id"] for project in coverage["projects_searched"]},
            {"public-project", "grouping-project", "chunk-api-project"},
        )
        self.assertNotIn("private-project", response.text)
        repository = next(
            hit["repository"]
            for hit in payload["hits"]
            if hit["session_id"] == "public-one"
        )
        self.assertEqual(repository["remote"], "https://github.com/acme/public-hws.git")
        self.assertEqual(repository["branch"], "feature/search-api")
        self.assertEqual(repository["head"], "2d48e17abc123")
        self.assertIsNone(repository["dirty"])
        public_hit = next(
            hit for hit in payload["hits"] if hit["session_id"] == "public-one"
        )
        self.assertEqual(
            public_hit["links"]["turn"],
            "/api/v1/sessions/public-one/turns/1",
        )

        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                upsert_project_acl_member(
                    connection,
                    project_id="private-project",
                    user_id=str(self.viewer["id"]),
                    role="viewer",
                    granted_by_user_id=str(self.admin["id"]),
                )

        granted_response = self._search(
            token=self.viewer_token, project_id="private-project"
        )
        self.assertEqual(granted_response.status_code, 200, granted_response.text)
        self.assertEqual(granted_response.json()["total_count"], 1)
        self.assertEqual(
            granted_response.json()["hits"][0]["session_id"], "private-one"
        )
        granted_coverage = granted_response.json()["coverage"]
        self.assertEqual(granted_coverage["sessions_total"], 1)
        self.assertEqual(granted_coverage["pending_reindex_sessions"], 0)
        self.assertEqual(
            [project["id"] for project in granted_coverage["projects_searched"]],
            ["private-project"],
        )

    def test_turn_context_api_returns_complete_turn_and_repository(self) -> None:
        self.assertEqual(self._turn().status_code, 401)
        self.assertEqual(self._turn(token=self.sync_token).status_code, 401)

        response = self._turn(token=self.viewer_token, context=2, include="activity")

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["requested_turn"], 1)
        self.assertEqual(payload["context"]["first_turn"], 1)
        self.assertEqual(payload["context"]["last_turn"], 2)
        self.assertEqual(
            payload["repository"]["remote"],
            "https://github.com/acme/public-hws.git",
        )
        self.assertEqual(payload["repository"]["branch"], "feature/search-api")
        self.assertEqual(payload["repository"]["head"], "2d48e17abc123")
        self.assertEqual(len(payload["turns"]), 2)
        turn = payload["turns"][0]
        self.assertTrue(turn["is_target"])
        self.assertEqual(turn["prompt"]["text"], "shared api needle")
        self.assertEqual(
            turn["response"]["text"],
            "public response one with complete turn detail",
        )
        self.assertIn("activity", turn)
        self.assertFalse(payload["turns"][1]["is_target"])
        self.assertEqual(payload["turns"][1]["prompt"]["text"], "neighboring prompt")
        self.assertEqual(payload["turns"][1]["commands"][0]["command"], "pytest -q")
        self.assertEqual(payload["turns"][1]["commands"][0]["output"], "1 passed")
        self.assertEqual(payload["turns"][1]["patches"][0]["status"], "completed")
        self.assertIn("*** Begin Patch", payload["turns"][1]["patches"][0]["patch"])
        self.assertEqual(response.headers["cache-control"], "private, no-store")

        invalid_include = self._turn(token=self.viewer_token, include="raw")
        self.assertEqual(invalid_include.status_code, 422)

    def test_turn_context_api_hides_inaccessible_sessions(self) -> None:
        response = requests.get(
            f"{self.base_url}/api/v1/sessions/private-one/turns/1",
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {self.viewer_token}",
            },
            timeout=2,
        )

        self.assertEqual(response.status_code, 404)

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
        self.assertEqual(
            hidden_response.json()["coverage"]["freshness"]["state"],
            "unresolved_scope",
        )
        self.assertEqual(hidden_response.json()["coverage"]["projects_searched"], [])

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

    def test_search_modes_fields_and_facets_are_exposed(self) -> None:
        response = self._search(
            token=self.viewer_token,
            q="shared missing-term",
            mode="any",
            fields="prompt",
            facets="project,branch,matched_field",
            limit=1,
        )

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["mode"], "any")
        self.assertEqual(payload["filters"]["fields"], ["prompt"])
        self.assertEqual(payload["total_count"], 2)
        self.assertEqual(payload["facets"]["project"][0]["count"], 2)
        self.assertEqual(payload["facets"]["branch"][0]["value"], "feature/search-api")
        self.assertEqual(
            payload["facets"]["matched_field"][0]["value"],
            "prompt",
        )

        command_response = self._search(
            token=self.viewer_token,
            q="pytest",
            fields="commands",
        )
        self.assertEqual(command_response.status_code, 200, command_response.text)
        self.assertEqual(command_response.json()["total_count"], 2)
        self.assertEqual(
            command_response.json()["hits"][0]["matched_field"],
            "commands",
        )

        exact_response = self._search(
            token=self.viewer_token,
            q="shared api need",
            mode="exact",
            fields="prompt",
        )
        self.assertEqual(exact_response.status_code, 200, exact_response.text)
        self.assertEqual(exact_response.json()["total_count"], 0)

    def test_batch_search_is_bounded_and_uses_search_token_acl(self) -> None:
        body = {
            "queries": [
                {
                    "id": "command",
                    "q": "pytest",
                    "fields": ["commands"],
                    "limit": 2,
                },
                {
                    "id": "responses",
                    "q": "public response",
                    "mode": "phrase",
                    "fields": ["response"],
                    "facets": ["project"],
                    "remote": "git@github.com:acme/public-hws.git",
                    "limit": 2,
                },
            ],
            "max_total_hits": 4,
        }
        self.assertEqual(self._batch(**body).status_code, 401)
        self.assertEqual(self._batch(token=self.sync_token, **body).status_code, 401)

        response = self._batch(token=self.viewer_token, **body)

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["query_count"], 2)
        self.assertEqual(
            [result["id"] for result in payload["results"]], ["command", "responses"]
        )
        self.assertEqual(payload["results"][0]["hits"][0]["matched_field"], "commands")
        self.assertEqual(
            payload["results"][1]["filters"]["remote"],
            "github.com/acme/public-hws",
        )
        self.assertNotIn("classified private evidence", response.text)
        self.assertEqual(response.headers["cache-control"], "private, no-store")

        over_budget = self._batch(
            token=self.viewer_token,
            queries=[{"q": "shared", "limit": 3}],
            max_total_hits=2,
        )
        self.assertEqual(over_budget.status_code, 422)

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

        mismatched_options = self._search(
            token=self.viewer_token,
            limit=1,
            cursor=first_payload["next_cursor"],
            mode="any",
            fields="prompt",
            facets="project",
        )
        self.assertEqual(mismatched_options.status_code, 400)

    def test_chronological_sorting_is_explicit_and_stable(self) -> None:
        ascending_response = self._search(
            token=self.viewer_token,
            sort="time_asc",
        )
        descending_response = self._search(
            token=self.viewer_token,
            sort="time_desc",
        )

        self.assertEqual(ascending_response.status_code, 200, ascending_response.text)
        self.assertEqual(descending_response.status_code, 200, descending_response.text)
        ascending = ascending_response.json()
        descending = descending_response.json()
        self.assertEqual(
            [hit["session_id"] for hit in ascending["hits"]],
            ["public-one", "public-two"],
        )
        self.assertEqual(
            [hit["session_id"] for hit in descending["hits"]],
            ["public-two", "public-one"],
        )
        self.assertEqual(ascending["sort"], "time_asc")
        self.assertEqual(ascending["group_by"], "none")
        self.assertEqual(ascending["session_count"], 2)
        self.assertEqual(ascending["pagination"]["unit"], "hit")
        self.assertEqual(ascending["groups"], [])

    def test_session_grouping_caps_hits_and_paginates_by_session(self) -> None:
        first_response = self._search(
            token=self.viewer_token,
            q="grouping api needle",
            sort="time_desc",
            group_by="session",
            max_hits_per_session=1,
            limit=1,
        )

        self.assertEqual(first_response.status_code, 200, first_response.text)
        first = first_response.json()
        self.assertEqual(first["hits"], [])
        self.assertEqual(first["total_count"], 3)
        self.assertEqual(first["session_count"], 2)
        self.assertEqual(first["pagination"]["unit"], "session")
        self.assertEqual(first["pagination"]["total_count"], 2)
        self.assertEqual(len(first["groups"]), 1)
        self.assertEqual(first["groups"][0]["session_id"], "grouping-newest")
        self.assertEqual(first["groups"][0]["match_count"], 1)
        self.assertEqual(first["groups"][0]["returned_hit_count"], 1)
        self.assertIsNotNone(first["next_cursor"])

        second_response = self._search(
            token=self.viewer_token,
            q="grouping api needle",
            sort="time_desc",
            group_by="session",
            max_hits_per_session=1,
            limit=1,
            cursor=first["next_cursor"],
        )
        self.assertEqual(second_response.status_code, 200, second_response.text)
        second = second_response.json()
        self.assertEqual(second["groups"][0]["session_id"], "public-one")
        self.assertEqual(second["groups"][0]["match_count"], 2)
        self.assertEqual(second["groups"][0]["returned_hit_count"], 1)
        self.assertEqual(second["groups"][0]["hits"][0]["turn_number"], 2)
        self.assertIsNone(second["next_cursor"])

        mismatched_response = self._search(
            token=self.viewer_token,
            q="grouping api needle",
            sort="time_asc",
            group_by="session",
            max_hits_per_session=1,
            limit=1,
            cursor=first["next_cursor"],
        )
        self.assertEqual(mismatched_response.status_code, 400)

        mismatched_cap_response = self._search(
            token=self.viewer_token,
            q="grouping api needle",
            sort="time_desc",
            group_by="session",
            max_hits_per_session=2,
            limit=1,
            cursor=first["next_cursor"],
        )
        self.assertEqual(mismatched_cap_response.status_code, 400)

    def test_followup_strict_parameters(self) -> None:
        for response, name in (
            (self._search(token=self.viewer_token, typo="x"), "typo"),
            (self._projects(token=self.viewer_token, project_id="x"), "project_id"),
            (self._turn(token=self.viewer_token, activity_limit=1), "activity_limit"),
            (
                self._batch(
                    token=self.viewer_token, queries=[{"q": "needle", "typo": 1}]
                ),
                "typo",
            ),
            (
                self._batch(token=self.viewer_token, queries=[{"q": "needle"}], typo=1),
                "typo",
            ),
        ):
            self.assertEqual(response.status_code, 422, response.text)
            error = response.json()["detail"][0]
            self.assertEqual(error["loc"][-1], name)
            self.assertIn("allowed", error)
            if "queries" in error["loc"]:
                self.assertEqual(error["loc"], ["body", "queries", 0, "typo"])
        self.assertEqual(
            self._search(token=self.viewer_token, max_hits_per_session=1).status_code,
            422,
        )

    def test_followup_exclusion_facets_coverage_batch_and_cursors(self) -> None:
        base = self._search(
            token=self.viewer_token,
            q="shared api needle",
            fields="prompt",
            facets="session",
            limit=1,
        ).json()
        snapshot = base["snapshot_id"]
        excluded = self._search(
            token=self.viewer_token,
            q="shared api needle",
            fields="prompt",
            facets="session",
            limit=1,
            exclude_session_id=["public-one", "public-one"],
            snapshot_id=snapshot,
        )
        self.assertEqual(excluded.status_code, 200, excluded.text)
        result = excluded.json()
        self.assertEqual([hit["session_id"] for hit in result["hits"]], ["public-two"])
        self.assertEqual(
            result["coverage"]["sessions_total"], base["coverage"]["sessions_total"] - 1
        )
        self.assertEqual(result["excluded_session_ids"], ["public-one"])
        self.assertEqual(
            result["normalized_query"]["exclude_session_id"], ["public-one"]
        )
        self.assertNotIn("public-one", str(result["facets"]))
        mismatch = self._search(
            token=self.viewer_token,
            q="shared api needle",
            fields="prompt",
            facets="session",
            limit=1,
            exclude_session_id="public-one",
            cursor=base["next_cursor"],
        )
        self.assertEqual(mismatch.status_code, 400)
        batch = self._batch(
            token=self.viewer_token,
            snapshot_id=snapshot,
            queries=[
                {
                    "q": "shared api needle",
                    "fields": ["prompt"],
                    "facets": ["session"],
                    "limit": 1,
                    "exclude_session_id": ["public-one"],
                }
            ],
        ).json()
        for key in ("hits", "facets", "coverage", "normalized_query", "next_cursor"):
            self.assertEqual(batch["results"][0][key], result[key], key)
        self.assertEqual(batch["results"][0]["snapshot_id"], snapshot)

    def test_followup_snapshot_pins_content_and_discovery(self) -> None:
        initial = self._search(
            token=self.viewer_token,
            q="shared api needle",
            fields="prompt",
            sort="time_asc",
        ).json()
        snapshot = initial["snapshot_id"]
        old_turn = self._turn(token=self.viewer_token, snapshot_id=snapshot).json()
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                connection.execute(
                    "UPDATE events SET display_text = 'Corrected response' WHERE session_id = 'public-one' AND event_index = 1"
                )
                connection.execute(
                    "UPDATE session_turn_search SET prompt_text = 'replacement token' WHERE session_id = 'public-two'"
                )
                connection.execute(
                    "DELETE FROM session_search_chunks WHERE session_id = 'public-two'"
                )
                connection.execute(
                    "UPDATE sessions SET search_chunk_version = 0 WHERE id = 'public-two'"
                )
        pinned = self._search(
            token=self.viewer_token,
            q="shared api needle",
            fields="prompt",
            sort="time_asc",
            snapshot_id=snapshot,
        ).json()
        self.assertEqual(pinned, initial)
        self.assertEqual(
            self._turn(token=self.viewer_token, snapshot_id=snapshot).json(), old_turn
        )
        live = self._search(
            token=self.viewer_token,
            q="shared api needle",
            fields="prompt",
            sort="time_asc",
        ).json()
        self.assertNotEqual(live["snapshot_id"], snapshot)
        self.assertLess(live["total_count"], initial["total_count"])
        self.assertNotEqual(
            live["hits"][0]["content_digest"], initial["hits"][0]["content_digest"]
        )
        self.assertFalse(live["coverage"]["exhaustive_ready"])
        self.assertEqual(
            self._projects(token=self.viewer_token, snapshot_id=snapshot).json()[
                "snapshot_id"
            ],
            snapshot,
        )
        batch = self._batch(
            token=self.viewer_token,
            snapshot_id=snapshot,
            queries=[{"q": "needle"}, {"q": "response"}],
        ).json()
        self.assertEqual(
            {result["snapshot_id"] for result in batch["results"]}, {snapshot}
        )

    def test_followup_snapshot_expiration_tampering_and_revocation(self) -> None:
        initial = self._search(token=self.viewer_token, limit=1).json()
        snapshot = initial["snapshot_id"]
        self.assertEqual(
            self._search(token=self.viewer_token, snapshot_id="bad-token").status_code,
            409,
        )
        cursor = initial["next_cursor"]
        self.assertEqual(
            self._search(
                token=self.viewer_token, limit=1, cursor=cursor[:-8] + "tampered"
            ).status_code,
            400,
        )
        with patch(
            "agent_operations_viewer.search_snapshots.time.time",
            return_value=time.time() + 1000,
        ):
            response = self._search(token=self.viewer_token, snapshot_id=snapshot)
        self.assertEqual(response.status_code, 410, response.text)
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                connection.execute(
                    "UPDATE projects SET visibility = 'private' WHERE id = 'public-project'"
                )
        response = self._search(token=self.viewer_token, snapshot_id=snapshot)
        self.assertEqual(response.status_code, 403, response.text)
        self.assertEqual(response.json()["detail"]["code"], "snapshot_access_revoked")
        self.assertEqual(
            self._turn(token=self.viewer_token, snapshot_id=snapshot).status_code, 403
        )

    def test_followup_digest_scope_and_conditional_retrieval(self) -> None:
        search = self._search(
            token=self.viewer_token,
            q="shared api needle",
            fields="prompt",
            project_id="public-project",
        ).json()
        hit = next(hit for hit in search["hits"] if hit["session_id"] == "public-one")
        response = self._turn(
            token=self.viewer_token,
            include="activity",
            snapshot_id=search["snapshot_id"],
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        turn = payload["turns"][0]
        canonical = {
            key: value
            for key, value in turn.items()
            if key
            not in {
                "is_target",
                "content_digest",
                "content_version",
                "normalization_version",
                "activity_digest",
            }
        }
        self.assertEqual(
            digest({"normalization_version": NORMALIZATION_VERSION, "turn": canonical}),
            hit["content_digest"],
        )
        self.assertEqual(payload["content_digest"], hit["content_digest"])
        self.assertEqual(
            self._turn(token=self.viewer_token, context=1).json()["content_digest"],
            hit["content_digest"],
        )
        conditional = requests.get(
            f"{self.base_url}/api/v1/sessions/public-one/turns/1",
            params={"include": "activity", "snapshot_id": search["snapshot_id"]},
            headers={
                "Authorization": f"Bearer {self.viewer_token}",
                "If-None-Match": response.headers["ETag"],
            },
            timeout=2,
        )
        self.assertEqual(conditional.status_code, 304)
        different_projection = requests.get(
            f"{self.base_url}/api/v1/sessions/public-one/turns/1",
            params={"snapshot_id": search["snapshot_id"]},
            headers={
                "Authorization": f"Bearer {self.viewer_token}",
                "If-None-Match": response.headers["ETag"],
            },
            timeout=2,
        )
        self.assertEqual(different_projection.status_code, 200)
        self.assertNotEqual(
            different_projection.headers["ETag"], response.headers["ETag"]
        )

    def _activity(self, turn=2, **params):
        return requests.get(
            f"{self.base_url}/api/v1/sessions/public-one/turns/{turn}/activity",
            params=params,
            headers={"Authorization": f"Bearer {self.viewer_token}"},
            timeout=2,
        )

    def test_followup_activity_filters_and_cursor_binding(self) -> None:
        first_response = self._activity(limit=1)
        self.assertEqual(first_response.status_code, 200, first_response.text)
        first = first_response.json()
        self.assertEqual(first["returned_count"], 1)
        self.assertIsNotNone(first["next_cursor"])
        self.assertEqual(
            self._activity(
                limit=1, cursor=first["next_cursor"], tool_name="exec_command"
            ).status_code,
            400,
        )
        another = self._activity(limit=1).json()
        self.assertEqual(
            self._activity(
                limit=1, cursor=first["next_cursor"], snapshot_id=another["snapshot_id"]
            ).status_code,
            400,
        )
        self.assertEqual(self._activity(unsupported=1).status_code, 422)
        filtered = self._activity(
            tool_name="apply_patch",
            kind="tool_call",
            event_type="function_call",
            snapshot_id=first["snapshot_id"],
        ).json()
        self.assertGreater(filtered["total_count"], 0)
        self.assertTrue(
            all(
                event["tool_name"] == "apply_patch"
                and event["kind"] == "tool_call"
                and event["payload_type"] == "function_call"
                for event in filtered["activity"]
            )
        )
        bounded = self._activity(
            from_event_index=6, to_event_index=6, snapshot_id=first["snapshot_id"]
        ).json()
        self.assertTrue(all(event["event_index"] == 6 for event in bounded["activity"]))

    def test_followup_activity_110_events_no_gaps(self) -> None:
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                connection.execute(
                    "INSERT INTO session_turns (session_id, turn_number, start_event_index, end_event_index) VALUES ('public-one', 3, 10, 121)"
                )
                for index in range(10, 122):
                    role = (
                        "user" if index == 10 else "assistant" if index == 121 else None
                    )
                    kind = "message" if role else "tool_call"
                    payload_type = "message" if role else "function_call"
                    connection.execute(
                        "INSERT INTO events (session_id, event_index, timestamp, record_type, payload_type, kind, role, title, display_text, tool_name, call_id, record_json) VALUES ('public-one', ?, ?, 'response_item', ?, ?, ?, 'Activity', ?, ?, ?, ?)",
                        (
                            index,
                            "2026-08-23T15:00:00Z",
                            payload_type,
                            kind,
                            role,
                            "hello" if role else "tool argument",
                            None if role else "test_tool",
                            f"event-{index}",
                            json.dumps(
                                {
                                    "payload": {
                                        "type": payload_type,
                                        "role": role,
                                        "phase": "final_answer"
                                        if role == "assistant"
                                        else None,
                                    }
                                }
                            ),
                        ),
                    )
        first = self._activity(turn=3, limit=13).json()
        self.assertEqual(first["total_count"], 110)
        events = first["activity"]
        page = first
        while page["next_cursor"]:
            response = self._activity(turn=3, limit=13, cursor=page["next_cursor"])
            self.assertEqual(response.status_code, 200, response.text)
            page = response.json()
            self.assertEqual(page["snapshot_id"], first["snapshot_id"])
            events.extend(page["activity"])
        self.assertEqual(
            [event["event_index"] for event in events], list(range(11, 121))
        )
        self.assertEqual(len({event["activity_id"] for event in events}), 110)

    def test_followup_patch_search_modes_and_batch(self) -> None:
        # Reindex a submitted patch with a token absent from paths and status output.
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                connection.execute(
                    "UPDATE events SET display_text = ? WHERE session_id = 'public-one' AND event_index = 6",
                    (
                        "*** Begin Patch\n*** Update File: app.py\n@@\n-oldsentinel\n+patchsentinel alpha\n context\n*** End Patch",
                    ),
                )
                events = connection.execute(
                    "SELECT * FROM events WHERE session_id = 'public-one' ORDER BY event_index"
                ).fetchall()
                replace_session_search_chunks(connection, "public-one", events)
        snapshot = None
        for mode in ("exact", "phrase", "any", "all"):
            response = self._search(
                token=self.viewer_token,
                q="patchsentinel alpha",
                fields="patches",
                mode=mode,
                snapshot_id=snapshot,
            )
            self.assertEqual(response.status_code, 200, response.text)
            payload = response.json()
            snapshot = payload["snapshot_id"]
            self.assertEqual(payload["total_count"], 1)
            hit = payload["hits"][0]
            self.assertEqual(hit["matched_field"], "patches")
            self.assertEqual(
                {line["kind"] for line in hit["chunk"]["lines"]},
                {"header", "addition", "deletion", "context"},
            )
            batch = self._batch(
                token=self.viewer_token,
                snapshot_id=snapshot,
                queries=[
                    {"q": "patchsentinel alpha", "fields": ["patches"], "mode": mode}
                ],
            ).json()
            self.assertEqual(batch["results"][0]["hits"], payload["hits"])
        self.assertEqual(
            self._search(
                token=self.viewer_token,
                q="patchsentinel",
                fields="paths",
                snapshot_id=snapshot,
            ).json()["total_count"],
            0,
        )

    def test_followup_patch_pagination_keeps_acl_and_batch_semantics(self) -> None:
        patch_text = (
            "*** Begin Patch\n*** Update File: app.py\n@@\n+patchpager\n*** End Patch"
        )
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                connection.execute(
                    "UPDATE events SET display_text = ? WHERE session_id = 'public-one' AND event_index = 6",
                    (patch_text,),
                )
                for sid in ("public-two", "private-one"):
                    connection.execute(
                        "UPDATE events SET event_index = 2 WHERE session_id = ? AND event_index = 1",
                        (sid,),
                    )
                    connection.execute(
                        "UPDATE session_turns SET end_event_index = 2 WHERE session_id = ?",
                        (sid,),
                    )
                    connection.execute(
                        "INSERT INTO events (session_id, event_index, timestamp, record_type, payload_type, kind, title, display_text, tool_name, record_json) VALUES (?, 1, '2026-08-23T12:00:00Z', 'response_item', 'function_call', 'tool_call', 'Patch', ?, 'apply_patch', '{}')",
                        (sid, patch_text),
                    )
                for sid in ("public-one", "public-two", "private-one"):
                    replace_session_search_chunks(connection, sid)
        query = {
            "q": "patchpager",
            "fields": "patches",
            "facets": "session",
            "sort": "time_asc",
            "limit": 1,
        }
        first = self._search(token=self.viewer_token, **query).json()
        self.assertEqual(first["total_count"], 2)
        self.assertNotIn("private-one", str(first))
        second = self._search(
            token=self.viewer_token, **query, cursor=first["next_cursor"]
        ).json()
        self.assertEqual(
            {first["hits"][0]["session_id"], second["hits"][0]["session_id"]},
            {"public-one", "public-two"},
        )
        self.assertIsNone(second["next_cursor"])
        batch = self._batch(
            token=self.viewer_token,
            snapshot_id=first["snapshot_id"],
            queries=[{**query, "fields": ["patches"], "facets": ["session"]}],
        ).json()["results"][0]
        for key in ("hits", "next_cursor", "facets", "coverage"):
            self.assertEqual(first[key], batch[key], key)

    def test_followup_pending_session_counts_reconcile(self) -> None:
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                connection.execute(
                    "DELETE FROM session_turns WHERE session_id = 'public-two'"
                )
                connection.execute(
                    "DELETE FROM session_turn_search WHERE session_id = 'public-two'"
                )
                connection.execute(
                    "UPDATE sessions SET turn_index_version = 0, turn_count = 4 WHERE id = 'public-two'"
                )
        projects = self._projects(token=self.viewer_token).json()
        project = next(
            project
            for project in projects["projects"]
            if project["id"] == "public-project"
        )
        coverage = self._search(
            token=self.viewer_token,
            project_id=project["id"],
            snapshot_id=projects["snapshot_id"],
        ).json()["coverage"]
        self.assertEqual(project["session_count"], 2)
        self.assertEqual(project["session_count"], coverage["sessions_total"])
        self.assertEqual(
            sum(source["session_count"] for source in project["sources"]),
            project["session_count"],
        )
        self.assertEqual(project["first_session_at"], coverage["first_session_at"])
        self.assertEqual(coverage["sessions_pending"], 1)
        self.assertEqual(coverage["turns_pending"], 4)
        self.assertFalse(coverage["exhaustive_ready"])

    def test_search_sort_and_group_parameters_are_validated(self) -> None:
        invalid_sort = self._search(token=self.viewer_token, sort="newest")
        invalid_group = self._search(token=self.viewer_token, group_by="project")
        invalid_cap = self._search(
            token=self.viewer_token,
            group_by="session",
            max_hits_per_session=0,
        )
        invalid_mode = self._search(token=self.viewer_token, mode="semantic")
        invalid_fields = self._search(token=self.viewer_token, fields="prompt,secrets")
        invalid_facets = self._search(token=self.viewer_token, facets="project,owner")

        self.assertEqual(invalid_sort.status_code, 422)
        self.assertEqual(invalid_group.status_code, 422)
        self.assertEqual(invalid_cap.status_code, 422)
        self.assertEqual(invalid_mode.status_code, 422)
        self.assertEqual(invalid_fields.status_code, 422)
        self.assertEqual(invalid_facets.status_code, 422)


if __name__ == "__main__":
    unittest.main()
