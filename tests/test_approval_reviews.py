from contextlib import closing
import json
from pathlib import Path
import tempfile
import unittest

from fastapi.testclient import TestClient

from agent_operations_viewer.db import connect, init_db
from agent_operations_viewer.project_browse import ensure_browse_schema, project_catalog, browse_project_detail
from agent_operations_viewer.projects import fetch_turn_stream, search_turn_hits, ProjectAccessContext
from agent_operations_viewer.web.app import create_app
from tests.test_route_auth_audit import make_test_settings
from tests.test_search import insert_search_turn


class ApprovalReviewTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.settings = make_test_settings(data_dir=Path(self.tmp.name), port=8765)
        self.settings.auth_mode = 'none'
        self.settings.sync_mode = 'local'
        init_db(self.settings.database_path)
        self.c = connect(self.settings.database_path)
        self.addCleanup(self.c.close)

    def insert(self, sid, metadata, *, pid='p1', label='One', visibility='authenticated'):
        insert_search_turn(self.c, session_id=sid, project_id=pid, project_key=f'project:{pid}',
                           project_label=label, host='host', visibility=visibility,
                           prompt=f'Needle {sid}', response='Done')
        self.c.execute('UPDATE sessions SET raw_meta_json=? WHERE id=?',
                       (json.dumps(metadata), sid))
        self.c.commit()

    def test_only_structured_markers_classify_reviews_and_updates_invalidate_cache(self):
        cases = [
            ('guardian', {'source': {'subagent': {'other': 'guardian'}}}, True),
            ('thread', {'thread_source': 'guardian_review'}, True),
            ('cli', {'source': 'cli'}, False),
            ('worker', {'source': {'subagent': {'other': 'worker'}}}, False),
            ('lookalike', {'thread_source': 'guardian_review_extra'}, False),
            ('quoted', {'instructions': '"thread_source": "guardian_review"'}, False),
            ('unknown', {}, False),
        ]
        for sid, meta, expected in cases:
            self.insert(sid, meta)
            actual = self.c.execute('SELECT is_approval_review FROM session_browse WHERE id=?', (sid,)).fetchone()[0]
            self.assertEqual(bool(actual), expected, sid)
        self.c.execute("UPDATE sessions SET raw_meta_json='invalid JSON' WHERE id='unknown'")
        self.c.execute("UPDATE sessions SET summary='The following is the Codex agent history added since your last approval assessment.' WHERE id='cli'")
        self.c.commit()
        hidden = project_catalog(self.c, show_approval_reviews=False)
        shown = project_catalog(self.c, show_approval_reviews=True)
        self.assertEqual(len(hidden.rows), 5)
        self.assertEqual(len(shown.rows), 7)
        self.assertIs(hidden, project_catalog(self.c, show_approval_reviews=False))
        self.assertIs(shown, project_catalog(self.c, show_approval_reviews=True))
        self.c.execute("UPDATE sessions SET raw_meta_json='{}' WHERE id='guardian'")
        self.c.commit()
        self.assertEqual(len(project_catalog(self.c, show_approval_reviews=False).rows), 6)

    def test_filter_applies_before_pagination_counts_and_search(self):
        self.insert('normal', {})
        for i in range(12):
            self.insert(f'review-{i}', {'thread_source': 'guardian_review'})
        stream = fetch_turn_stream(self.c, group_key='project:p1', page_size=10)
        self.assertEqual(stream['total_count'], 1)
        self.assertEqual([r['session_id'] for r in stream['items']], ['normal'])
        self.assertFalse(stream['has_next'])
        shown = fetch_turn_stream(self.c, group_key='project:p1', show_approval_reviews=True, page_size=10)
        self.assertEqual(shown['total_count'], 13)
        self.assertEqual(len(shown['items']), 10)
        self.assertTrue(shown['has_next'])
        detail = browse_project_detail(self.c, 'project:p1', view='sessions', sessions_page_size=3)
        self.assertEqual(detail['session_ids'], ['normal'])
        self.assertEqual(len(detail['all_sessions_page']['items']), 1)
        self.assertFalse(detail['all_sessions_page']['has_next'])
        self.assertEqual(search_turn_hits(self.c, 'Needle')['total_count'], 1)
        self.assertEqual(search_turn_hits(self.c, 'Needle', show_approval_reviews=True)['total_count'], 13)

    def test_review_only_projects_keep_routes_and_access_checks(self):
        self.insert('normal', {})
        self.insert('review', {'thread_source': 'guardian_review'}, pid='p2')
        all_groups = project_catalog(self.c).groups
        visible_groups = project_catalog(self.c, show_approval_reviews=False).groups
        expected_href = next(g.detail_href for g in all_groups if g.key == 'project:p1')
        self.assertEqual(visible_groups[0].detail_href, expected_href)
        empty = browse_project_detail(self.c, 'project:p2', view='sessions')
        self.assertIsNotNone(empty)
        self.assertEqual(empty['session_ids'], [])
        self.assertEqual(empty['all_sessions_page']['items'], [])
        self.c.execute("UPDATE projects SET visibility='private' WHERE id='p2'")
        self.c.commit()
        viewer = ProjectAccessContext(auth_enabled=True, bypass=False, user_id='viewer', project_roles={})
        for show in (False, True):
            self.assertIsNone(browse_project_detail(self.c, 'project:p2', project_access=viewer, show_approval_reviews=show))
            self.assertEqual(len(project_catalog(self.c, viewer, show_approval_reviews=show).rows), 1)

    def test_existing_projection_migration_backfills_and_replaces_old_triggers(self):
        self.insert('review', {'source': {'subagent': {'other': 'guardian'}}})
        for suffix in ('insert', 'update', 'delete'):
            self.c.execute(f'DROP TRIGGER browse_session_{suffix}')
        self.c.execute('ALTER TABLE session_browse DROP COLUMN is_approval_review')
        self.c.execute('CREATE TRIGGER browse_session_update AFTER UPDATE ON sessions BEGIN SELECT 1; END')
        self.c.commit()
        ensure_browse_schema(self.c)
        self.c.commit()
        self.assertEqual(project_catalog(self.c, show_approval_reviews=False).rows, [])
        self.c.execute("UPDATE sessions SET raw_meta_json='{}' WHERE id='review'")
        self.c.commit()
        self.assertEqual(len(project_catalog(self.c, show_approval_reviews=False).rows), 1)
        revision = self.c.execute('SELECT revision FROM project_browse_revision').fetchone()[0]
        ensure_browse_schema(self.c)
        self.assertEqual(self.c.execute('SELECT revision FROM project_browse_revision').fetchone()[0], revision)

    def test_browser_toggle_persists_across_views_and_resets_pagination(self):
        self.insert('normal', {})
        self.insert('review', {'thread_source': 'guardian_review'})
        app = create_app(self.settings, preserve_sync_on_start=True)
        with TestClient(app) as client:
            for path in ('/', '/host/one', '/host/one?view=sessions', '/host/one/stream', '/search?q=Needle'):
                response = client.get(path)
                self.assertEqual(response.status_code, 200, path)
                self.assertIn('Show approval reviews', response.text)
                self.assertIn('aria-checked="false"', response.text)
            self.assertNotIn('Needle review', client.get('/host/one').text)
            page = client.get('/host/one?view=sessions&sessions_page=4')
            self.assertIn('name="return_to" value="/host/one?view=sessions"', page.text)
            toggled = client.post('/preferences/approval-reviews', data={'show': '1', 'return_to': '/host/one'}, follow_redirects=False)
            self.assertEqual(toggled.status_code, 303)
            self.assertEqual(toggled.headers['location'], '/host/one')
            self.assertEqual(client.cookies.get('aov_show_approval_reviews'), '1')
            for path in ('/host/one', '/host/one?view=sessions', '/host/one/stream', '/search?q=Needle'):
                response = client.get(path)
                self.assertIn('Needle review', response.text, path)
                self.assertIn('aria-checked="true"', response.text)
            toggled = client.post('/preferences/approval-reviews', data={'show': '0', 'return_to': 'https://example.com'}, follow_redirects=False)
            self.assertEqual(toggled.headers['location'], '/')
            self.assertNotIn('Needle review', client.get('/host/one').text)
        # Filtering is presentation-only: source sessions and turn IDs survive.
        with closing(connect(self.settings.database_path)) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM sessions').fetchone()[0], 2)
            self.assertEqual(c.execute('SELECT count(*) FROM session_turns').fetchone()[0], 2)
