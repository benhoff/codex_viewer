from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import sqlite3
import threading
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient

from agent_operations_viewer.db import connect, init_db, write_transaction
from agent_operations_viewer.onboarding import read_onboarding_status, reconcile_onboarding_state
from agent_operations_viewer.projects import (
    ProjectAccessContext, _project_route_groups, build_grouped_projects, query_group_rows,
)
from agent_operations_viewer.web.context import AppContext, set_app_context
from agent_operations_viewer.web.routes import pages
from tests.test_projects import insert_session
from tests.test_route_auth_audit import make_test_settings
from tests.test_search import insert_search_turn
from tests.test_action_queue import raw_session_jsonl, event_msg, patch_records, command_records, turn_complete_record
from agent_operations_viewer.importer import parse_session_text, upsert_parsed_session
from agent_operations_viewer.web.app import create_app


class PageLoadPerformanceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.settings = make_test_settings(data_dir=Path(self.tmp.name), port=8765)
        self.settings.auth_mode = 'none'
        self.settings.sync_mode = 'local'
        init_db(self.settings.database_path)

    def test_onboarding_status_is_read_only_even_before_state_row_exists(self):
        with closing(connect(self.settings.database_path)) as c:
            with c:
                c.execute('DELETE FROM onboarding_state')
                insert_session(c, 'session', source_path='/tmp/session.jsonl')
            c.execute('PRAGMA query_only=ON')
            with patch('agent_operations_viewer.onboarding.utc_now_iso', return_value='2026-09-12T00:00:00+00:00'):
                status = read_onboarding_status(c, self.settings)
                self.assertEqual(c.execute('SELECT COUNT(*) FROM onboarding_state').fetchone()[0], 0)
                self.assertTrue(status['data_verified'])
                self.assertEqual(status['overall_state'], 'complete')
                c.execute('PRAGMA query_only=OFF')
                with write_transaction(c):
                    saved = reconcile_onboarding_state(c, self.settings)
            self.assertEqual(status, saved)
            self.assertTrue(c.execute('SELECT completed_at FROM onboarding_state').fetchone()[0])

    def test_read_status_reflects_remote_changes_without_persisting_failure(self):
        self.settings.sync_mode = 'remote'
        with closing(connect(self.settings.database_path)) as c:
            with write_transaction(c):
                reconcile_onboarding_state(c, self.settings)
            before = tuple(c.execute('SELECT * FROM onboarding_state').fetchone())
            c.execute('PRAGMA query_only=ON')
            status = read_onboarding_status(c, self.settings)
            self.assertFalse(status['machine_access_ready'])
            self.assertEqual(tuple(c.execute('SELECT * FROM onboarding_state').fetchone()), before)

    def test_homepage_browses_activity_without_reading_task_failure_evidence(self):
        timestamp = datetime.now(timezone.utc).isoformat()
        raw = raw_session_jsonl('failed-check', cwd='/workspace/example', records=[
            event_msg({'type': 'user_message', 'message': 'Update the styling.'}, timestamp=timestamp),
            *patch_records('src/main.rs', timestamp_call=timestamp, timestamp_result=timestamp),
            *command_records(['cargo', 'test'], timestamp_call=timestamp, timestamp_result=timestamp,
                             exit_code=1, status='failed', stderr='test failure',
                             aggregated_output='test suite failed', formatted_output='test suite failed'),
            turn_complete_record('Tests failed after the patch.', timestamp=timestamp),
        ])
        parsed = parse_session_text(raw, Path('/tmp/failed-check.jsonl'), Path('/tmp'), 'builder',
                                    file_size=len(raw.encode()), file_mtime_ns=0)
        with closing(connect(self.settings.database_path)) as connection, write_transaction(connection):
            upsert_parsed_session(connection, parsed)
        app = create_app(self.settings, preserve_sync_on_start=True)

        def activity_connection(path):
            connection = connect(path)
            def authorize(action, table, column, database, trigger):
                return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ and table == 'events' else sqlite3.SQLITE_OK
            connection.set_authorizer(authorize)
            return connection

        with TestClient(app) as client:
            with patch('agent_operations_viewer.db.connect', side_effect=activity_connection):
                response = client.get('/')
            self.assertEqual(response.status_code, 200, response.text)
            self.assertIn('data-active-repo', response.text)
            self.assertIn('builder', response.text)
            self.assertNotIn('Verification failed', response.text)
            self.assertNotIn('Needs Attention', response.text)
            self.assertNotIn('Machines Needing Attention', response.text)
            audit = client.get('/sessions/failed-check?view=audit')
            self.assertEqual(audit.status_code, 200, audit.text)
            self.assertIn('Verification failed', audit.text)

    def test_page_reads_finish_while_an_upload_holds_both_write_locks(self):
        ready, release = threading.Event(), threading.Event()
        failures = []
        def upload():
            try:
                with closing(connect(self.settings.database_path)) as c, write_transaction(c):
                    c.execute("UPDATE onboarding_state SET last_failure_reason = 'uncommitted'")
                    ready.set()
                    release.wait(8)
            except BaseException as exc:
                failures.append(exc)
                ready.set()
        holder = threading.Thread(target=upload)
        holder.start()
        self.assertTrue(ready.wait(2))
        captured = []
        class Templates:
            def TemplateResponse(self, request, *, name, context, **kwargs):
                captured.append(name)
                return HTMLResponse('ok')
        app = SimpleNamespace(state=SimpleNamespace())
        set_app_context(app, AppContext(settings=self.settings, templates=Templates()))
        def request(path):
            r = Request({'type': 'http', 'method': 'GET', 'path': path, 'headers': [],
                         'query_string': b'', 'scheme': 'http', 'server': ('testserver', 80), 'app': app})
            r.state.auth_enabled = False
            r.state.auth_user = None
            r.state.bootstrap_required = False
            return r
        done = threading.Event()
        def read_pages():
            try:
                for path, handler in [('/', lambda r: pages.index(r, q=None, host=None)),
                                      ('/settings', pages.settings_page), ('/machines', pages.machines_health),
                                      ('/setup/status', pages.render_onboarding_status_fragment)]:
                    self.assertEqual(handler(request(path)).status_code, 200)
            except BaseException as exc:
                failures.append(exc)
            finally:
                done.set()
        reader = threading.Thread(target=read_pages)
        reader.start()
        try:
            self.assertTrue(done.wait(3), 'Page reads waited for the upload writer')
            self.assertFalse(release.is_set())
        finally:
            release.set()
            holder.join(3)
            reader.join(3)
        if failures:
            raise failures[0]
        self.assertEqual(len(captured), 4)

    def test_lean_route_lookup_preserves_collisions_hosts_overrides_and_acls(self):
        with closing(connect(self.settings.database_path)) as c:
            with c:
                for index, label in enumerate(['Same Name', 'Same-Name', 'Private']):
                    insert_search_turn(c, session_id=f's{index}', project_id=f'p{index}',
                                       project_key=f'project:p{index}', project_label=label,
                                       host='host', visibility='private' if index == 2 else 'authenticated')
                c.execute("UPDATE sessions SET summary = ?, last_user_message = ?", ('large ' * 10000,) * 2)
                insert_search_turn(c, session_id='s3', project_id='p0', project_key='project:p0',
                                   project_label='Same Name', host='z-host')
                c.execute("INSERT INTO project_overrides(match_project_key, override_organization, override_repository, created_at, updated_at) VALUES ('project:p1', 'host', 'Same Name', 'now', 'now')")
            for access in [None, ProjectAccessContext(auth_enabled=True, bypass=False, user_id='viewer', project_roles={})]:
                # Reading large preview columns is forbidden, not merely slow.
                expected = {g.key: g.detail_href for g in build_grouped_projects(query_group_rows(c, project_access=access))}
                self.assertIn('--', expected['project:p0'])
                self.assertIn('--', expected['project:p1'])
                def authorize(action, table, column, database, trigger):
                    if action == sqlite3.SQLITE_READ and table == 'sessions' and column in {'summary', 'last_user_message', 'latest_turn_summary'}:
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK
                c.set_authorizer(authorize)
                actual = {g.key: g.detail_href for g in _project_route_groups(c, project_access=access)}
                c.set_authorizer(None)
                self.assertEqual(actual, expected)
                if access is not None:
                    self.assertNotIn('project:p2', actual)
