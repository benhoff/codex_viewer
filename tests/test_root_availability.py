from contextlib import closing
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from agent_operations_viewer.api_tokens import create_api_token
from agent_operations_viewer.db import connect, write_transaction
from agent_operations_viewer.local_auth import create_initial_admin
from agent_operations_viewer.web.app import create_app
from agent_operations_viewer.web import concurrency
from agent_operations_viewer.web.routes.sync_api import _process_sync_heartbeat
from tests.test_route_auth_audit import make_test_settings


class RootAvailabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.settings = make_test_settings(data_dir=Path(self.tmp.name), port=8765)
        self.app = create_app(self.settings)
        with closing(connect(self.settings.database_path)) as connection, write_transaction(connection):
            create_initial_admin(connection, username="admin", password="Password123!")
            create_api_token(connection, "Test machine")
        self.client = TestClient(self.app)
        self.addCleanup(self.client.close)
        login = self.client.post("/login", data={"username": "admin", "password": "Password123!"}, follow_redirects=False)
        self.assertEqual(login.status_code, 303)

    def test_authenticated_root_finishes_with_all_history_workers_waiting_for_writer(self):
        ready, release, finished = threading.Event(), threading.Event(), threading.Event()
        errors, responses, items = [], [], []

        def upload():
            try:
                with closing(connect(self.settings.database_path)) as connection, write_transaction(connection):
                    connection.execute("UPDATE onboarding_state SET last_failure_reason = 'uncommitted upload'")
                    ready.set()
                    release.wait(10)
            except BaseException as exc:
                errors.append(exc)
                ready.set()

        def heartbeat(started, host):
            started.set()
            return _process_sync_heartbeat(self.settings, {}, host)

        def browse():
            try:
                responses.append(self.client.get("/", follow_redirects=False))
            except BaseException as exc:
                errors.append(exc)
            finally:
                finished.set()

        holder = threading.Thread(target=upload)
        browser = threading.Thread(target=browse)
        holder.start()
        try:
            self.assertTrue(ready.wait(3))
            for number in range(4):
                started = threading.Event()
                items.append(concurrency._HISTORY_EXECUTOR._submit(
                    heartbeat, started, f"test-host-{number}", dedupe_key=None,
                ))
                self.assertTrue(started.wait(3))
            browser.start()
            self.assertTrue(finished.wait(3), "Root authentication waited for sync heartbeat workers")
            self.assertFalse(release.is_set())
            self.assertTrue(all(not item.done.is_set() for item in items))
            self.assertFalse(errors, errors)
            self.assertEqual(responses[0].status_code, 200)
            self.assertIn("Active Repos", responses[0].text)
        finally:
            release.set()
            holder.join(5)
            if browser.ident is not None:
                browser.join(5)
            for item in items:
                self.assertTrue(item.done.wait(5))
                self.assertIsNone(item.exception)

    def test_auth_queue_overload_returns_retryable_response(self):
        with patch("agent_operations_viewer.web.auth.run_in_auth_threadpool",
                   side_effect=concurrency.WorkQueueFull("browser-auth")):
            response = self.client.get("/", follow_redirects=False)
        self.assertEqual(response.status_code, 503)
        self.assertTrue(response.json()["retryable"])
        self.assertEqual(response.headers["retry-after"], "5")
