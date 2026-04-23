from __future__ import annotations

import json
from pathlib import Path
import re
import socket
import tempfile
import threading
import time
import unittest
from urllib.parse import urlencode, urlsplit

import requests
import uvicorn

from codex_session_viewer import SYNC_API_VERSION, __version__
from codex_session_viewer.config import Settings
from codex_session_viewer.db import connect, write_transaction
from codex_session_viewer.local_auth import create_initial_admin
from codex_session_viewer.machine_credentials import hash_pairing_secret
from codex_session_viewer.web.app import create_app


PLACEHOLDER_PATTERN = re.compile(r"\{([a-zA-Z0-9_]+)(?::[^}]+)?\}")


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
        source_host="audit-host",
        auth_mode="password",
        session_secret="test-session-secret",
        auth_proxy_user_header="X-Forwarded-User",
        auth_proxy_name_header="X-Forwarded-Name",
        auth_proxy_email_header="X-Forwarded-Email",
        auth_proxy_login_url=None,
        auth_proxy_logout_url=None,
        auth_cookie_secure=False,
    )


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class RouteAuthAuditTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.port = find_free_port()
        data_dir = Path(self.temp_dir.name)
        (data_dir / "sessions").mkdir(parents=True, exist_ok=True)
        self.settings = make_test_settings(data_dir=data_dir, port=self.port)
        self.app = create_app(self.settings, preserve_sync_on_start=True)
        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                create_initial_admin(
                    connection,
                    username="admin",
                    password="Password123!",
                )
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.server = uvicorn.Server(
            uvicorn.Config(
                self.app,
                host="127.0.0.1",
                port=self.port,
                log_level="warning",
            )
        )
        self.server_thread = threading.Thread(target=self.server.run, daemon=True)
        self.server_thread.start()
        self._wait_for_server()
        self.pairing_secret = "audit-pairing-secret"
        self.pairing_session_id: str | None = None

    def tearDown(self) -> None:
        self.server.should_exit = True
        self.server_thread.join(timeout=5)
        self.temp_dir.cleanup()

    def _wait_for_server(self) -> None:
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            try:
                response = requests.get(
                    f"{self.base_url}/api/health",
                    timeout=0.5,
                )
                if response.status_code < 500:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.1)
        raise RuntimeError("Timed out waiting for the test server to start")

    def _request(
        self,
        method: str,
        raw_path: str,
        *,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
    ) -> dict[str, object]:
        request_headers = dict(headers or {})
        split = urlsplit(raw_path)
        if "accept" not in {key.lower() for key in request_headers}:
            request_headers["accept"] = (
                "application/json"
                if split.path.startswith("/api/") or split.path == "/openapi.json"
                else "text/html"
            )
        try:
            response = requests.request(
                method.upper(),
                f"{self.base_url}{raw_path}",
                headers=request_headers,
                data=body or None,
                allow_redirects=False,
                timeout=1.5,
            )
        except requests.Timeout:
            return {
                "status": -1,
                "headers": {},
                "body": "Timed out",
            }
        except requests.RequestException as exc:
            return {
                "status": -2,
                "headers": {},
                "body": f"{exc.__class__.__name__}: {exc}",
            }
        return {
            "status": int(response.status_code),
            "headers": dict(response.headers),
            "body": response.text,
        }

    def _bootstrap_pairing_session_via_authenticated_start(self) -> None:
        session = requests.Session()
        login_response = session.post(
            f"{self.base_url}/login",
            data={
                "username": "admin",
                "password": "Password123!",
                "next": "/",
            },
            allow_redirects=False,
            timeout=1.5,
        )
        self.assertEqual(
            login_response.status_code,
            303,
            f"Expected authenticated login to redirect, got {login_response.status_code}: {login_response.text}",
        )
        response = session.get(
            f"{self.base_url}/machine-pairing/start",
            params={
                "session_id": "pair_audit",
                "secret": self.pairing_secret,
                "public_key": "audit-public-key",
                "source_host": "audit-host",
                "label": "Audit pairing",
            },
            allow_redirects=False,
            timeout=1.5,
        )
        self.assertEqual(
            response.status_code,
            303,
            f"Expected authenticated pairing bootstrap to redirect, got {response.status_code}: {response.text}",
        )
        self.assertEqual(
            response.headers.get("location"),
            f"/machine-pairing/pair_audit?secret={self.pairing_secret}",
        )
        self.pairing_session_id = "pair_audit"

    def _route_specs(self) -> list[tuple[str, str]]:
        specs: list[tuple[str, str]] = []
        for route in self.app.router.routes:
            path = getattr(route, "path", None)
            methods = getattr(route, "methods", None)
            if not path or not methods:
                continue
            if "GET" in methods:
                specs.append(("GET", path))
            elif "POST" in methods:
                specs.append(("POST", path))
        specs.append(("GET", "/static/app.css"))
        return specs

    def _materialize_path(self, route_path: str) -> str:
        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name == "source_host":
                return "audit-host"
            if name == "owner_slug":
                return "openai"
            if name == "project_slug":
                return "codex-viewer"
            if name == "org":
                return "openai"
            if name == "repo":
                return "codex-viewer"
            if name == "directory":
                return "workspace/repo"
            if name == "key":
                return "openai/codex-viewer"
            if name == "session_id":
                if route_path.startswith("/api/machine-pairing/") or route_path.startswith("/machine-pairing/"):
                    return self.pairing_session_id or "pair_audit"
                return "session-audit"
            return f"{name}-sample"

        path = PLACEHOLDER_PATTERN.sub(replace, route_path)
        query_items: list[tuple[str, str]] = []
        if path == "/api/sync/manifest":
            query_items.append(("host", "audit-host"))
        elif path.startswith("/api/machine-pairing/sessions/") or path.startswith("/machine-pairing/"):
            query_items.append(("secret", self.pairing_secret))
        elif path == "/search":
            query_items.append(("q", "audit"))
        if query_items:
            return f"{path}?{urlencode(query_items)}"
        return path

    def _request_body(self, method: str, route_path: str) -> bytes:
        if method != "POST":
            return b""
        if route_path == "/api/machine-pairing/sessions":
            return json.dumps(
                {
                    "label": "Audit pairing",
                    "source_host": "audit-host",
                    "public_key": "audit-public-key",
                    "secret_hash": hash_pairing_secret(self.pairing_secret),
                }
            ).encode("utf-8")
        if route_path.startswith("/api/machine-pairing/sessions/") and route_path.endswith("/finalize"):
            return json.dumps({"secret": self.pairing_secret}).encode("utf-8")
        return b""

    def _capture_route_matrix(self) -> list[dict[str, object]]:
        matrix: list[dict[str, object]] = []
        for method, route_path in self._route_specs():
            request_path = self._materialize_path(route_path)
            body = self._request_body(method, route_path)
            response = self._request(
                method,
                request_path,
                headers={"content-type": "application/json"} if body else None,
                body=body,
            )
            status = int(response["status"])
            location = str(response["headers"].get("location", ""))
            matrix.append(
                {
                    "method": method,
                    "route_path": route_path,
                    "request_path": request_path,
                    "status": status,
                    "location": location,
                    "body": str(response["body"]),
                }
            )
            if route_path == "/api/machine-pairing/sessions" and 200 <= status < 400:
                try:
                    payload = json.loads(str(response["body"]))
                except json.JSONDecodeError:
                    payload = {}
                self.pairing_session_id = str(payload.get("id") or "") or self.pairing_session_id
        return matrix

    def test_anonymous_route_audit_contextualizes_public_vs_secret_gated_routes(self) -> None:
        self._bootstrap_pairing_session_via_authenticated_start()
        matrix = self._capture_route_matrix()
        by_route = {(entry["method"], entry["route_path"]): entry for entry in matrix}

        expected_public = {
            ("GET", "/api/health"),
            ("GET", "/login"),
            ("POST", "/login"),
            ("GET", "/static/app.css"),
        }
        secret_gated = {
            ("GET", "/api/machine-pairing/sessions/{session_id}"),
            ("POST", "/api/machine-pairing/sessions/{session_id}/finalize"),
        }

        unexpected_public: list[str] = []
        secret_gated_reachable: list[str] = []
        gated_ok: list[str] = []

        for entry in matrix:
            key = (str(entry["method"]), str(entry["route_path"]))
            status = int(entry["status"])
            request_path = str(entry["request_path"])

            if key in expected_public:
                if not (200 <= status < 500):
                    unexpected_public.append(f"expected public {key} -> {status}")
                continue

            if key in secret_gated:
                if status not in {200, 400}:
                    unexpected_public.append(f"expected secret-gated {request_path} -> {status}")
                else:
                    secret_gated_reachable.append(f"{entry['method']} {request_path} -> {status}")
                continue

            if status in {303, 401, 403}:
                gated_ok.append(f"{entry['method']} {request_path} -> {status}")
                continue

            if 200 <= status < 500:
                unexpected_public.append(f"{entry['method']} {request_path} -> {status}")
                continue

            unexpected_public.append(f"{entry['method']} {request_path} -> {status}")

        self.assertTrue(secret_gated_reachable, "Expected to prove secret-gated pairing routes are reachable anonymously.")
        if unexpected_public:
            audit_dump = "\n".join(
                f"{entry['method']:4} {entry['request_path']:50} -> {entry['status']} {entry['location']}".rstrip()
                for entry in matrix
            )
            self.fail(
                "Anonymous route audit found routes outside the expected public and secret-gated buckets:\n"
                + "\n".join(unexpected_public)
                + "\n\nFull audit:\n"
                + audit_dump
            )


if __name__ == "__main__":
    unittest.main()
