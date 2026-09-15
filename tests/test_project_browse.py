from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace

from fastapi import Request
from unittest.mock import patch

from agent_operations_viewer.db import connect, init_db
from agent_operations_viewer.project_browse import (browse_project_detail, ensure_browse_schema,
                                                  project_catalog)
from agent_operations_viewer.projects import (ProjectAccessContext, build_grouped_projects,
    apply_project_session_preview, count_session_turn_prompts_since, fetch_turn_stream, query_group_rows,
    resolve_group_key_from_detail_path)
from tests.test_search import insert_search_turn


class ProjectBrowseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'browse.sqlite3'
        init_db(self.path)
        self.c = connect(self.path)
        self.addCleanup(self.c.close)

    def insert(self, sid='s1', key='project:one', pid='p1', label='One', visibility='authenticated'):
        insert_search_turn(self.c, session_id=sid, project_id=pid, project_key=key,
                           project_label=label, host='host', visibility=visibility,
                           prompt='A task', response='Done')
        self.c.commit()

    def test_projection_and_catalog_follow_updates_deletes_and_rollback(self):
        self.insert()
        original = project_catalog(self.c)
        self.c.execute("UPDATE sessions SET summary=?, turn_count=12 WHERE id='s1'", ('x'*5000,))
        pending = project_catalog(self.c)
        self.assertEqual(pending.groups[0].turn_count, 12)
        self.assertEqual(len(pending.groups[0].latest_summary), 1024)
        self.c.rollback()
        self.assertEqual(project_catalog(self.c).groups[0].turn_count, original.groups[0].turn_count)
        self.c.execute("UPDATE sessions SET summary='Updated', turn_count=7 WHERE id='s1'")
        self.c.commit()
        self.assertEqual(project_catalog(self.c).groups[0].turn_count, 7)
        self.assertEqual(project_catalog(self.c).groups[0].latest_summary, 'Updated')
        self.c.execute("DELETE FROM sessions WHERE id='s1'")
        self.c.commit()
        self.assertEqual(project_catalog(self.c).groups, [])
        self.assertEqual(self.c.execute('SELECT COUNT(*) FROM session_browse').fetchone()[0], 0)

    def test_warm_catalog_observes_other_connection_commits(self):
        self.insert()
        project_catalog(self.c)
        with closing(connect(self.path)) as other:
            other.execute("UPDATE sessions SET summary='From other worker' WHERE id='s1'")
            other.commit()
        self.assertEqual(project_catalog(self.c).groups[0].latest_summary, 'From other worker')

    def test_warm_routes_follow_overrides_ignores_and_visibility(self):
        self.insert(label='Same Name')
        self.insert('s2', 'project:two', 'p2', 'Same-Name')
        viewer = ProjectAccessContext(auth_enabled=True, bypass=False, user_id='viewer', project_roles={})
        def compare():
            expected = {g.key:g.detail_href for g in build_grouped_projects(query_group_rows(self.c, project_access=viewer))}
            actual = {g.key:g.detail_href for g in project_catalog(self.c, viewer).groups}
            self.assertEqual(actual, expected)
            return actual
        self.assertTrue(all('--' in path for path in compare().values()))
        self.c.execute("UPDATE projects SET visibility='private' WHERE id='p2'")
        self.c.commit()
        self.assertEqual(compare(), {'project:one':'/host/same-name'})
        granted = ProjectAccessContext(auth_enabled=True, bypass=False, user_id='viewer', project_roles={'p2':'viewer'})
        self.assertEqual(len(project_catalog(self.c, granted).groups), 2)
        self.assertEqual(len(project_catalog(self.c, viewer).groups), 1)
        self.c.execute("INSERT INTO project_overrides(match_project_key, override_repository, created_at, updated_at) VALUES ('project:one', 'Renamed', 'now', 'now')")
        self.c.commit()
        self.assertEqual(compare(), {'project:one':'/host/renamed'})
        self.c.execute("INSERT INTO ignored_project_sources(match_project_key, created_at) VALUES ('project:one','now')")
        self.c.commit()
        self.assertEqual(compare(), {})

    def test_browse_does_not_read_transcripts_or_rebuild_hidden_action_signals(self):
        self.insert()
        def authorize(action, table, column, database, trigger):
            if action == sqlite3.SQLITE_READ and table in {'sessions', 'events', 'action_queue_signals'}:
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        self.c.set_authorizer(authorize)
        self.assertEqual(resolve_group_key_from_detail_path(self.c, 'host', 'one'), 'project:one')
        detail = browse_project_detail(self.c, 'project:one')
        self.assertEqual(detail['all_sessions_page']['items'], [])
        stream = fetch_turn_stream(self.c, group_key='project:one', session_ids=detail['session_ids'],
                                   page_size=10, detail_href_override='/host/one')
        self.assertEqual(len(stream['items']), 1)

    def test_sessions_are_built_only_for_selected_page(self):
        for i in range(8):
            self.insert(f's{i}')
        with patch('agent_operations_viewer.projects.apply_project_session_preview', wraps=apply_project_session_preview) as preview:
            first = browse_project_detail(self.c, 'project:one', view='sessions', sessions_page_size=3)
            self.assertEqual(preview.call_count, 3)
        second = browse_project_detail(self.c, 'project:one', view='sessions', sessions_page=2, sessions_page_size=3)
        self.assertFalse({r['id'] for r in first['all_sessions_page']['items']} & {r['id'] for r in second['all_sessions_page']['items']})
        self.assertTrue(second['all_sessions_page']['has_prev'])
        self.assertTrue(second['all_sessions_page']['has_next'])
        last = browse_project_detail(self.c, 'project:one', view='sessions', sessions_page=3, sessions_page_size=3)
        self.assertEqual(len(last['all_sessions_page']['items']), 2)
        self.assertFalse(last['all_sessions_page']['has_next'])

    def test_backfill_existing_sessions_is_idempotent(self):
        self.insert()
        self.c.execute('DELETE FROM session_browse')
        ensure_browse_schema(self.c, rebuild=True)
        self.c.commit()
        self.assertEqual(len(project_catalog(self.c).rows), 1)
        revision = self.c.execute('SELECT revision FROM project_browse_revision').fetchone()[0]
        ensure_browse_schema(self.c)
        self.assertEqual(self.c.execute('SELECT revision FROM project_browse_revision').fetchone()[0], revision)
        self.assertEqual(self.c.execute('SELECT COUNT(*) FROM session_browse').fetchone()[0], 1)

    def test_initial_migration_backfills_existing_database_and_installs_triggers(self):
        self.insert()
        triggers = self.c.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'browse_%'").fetchall()
        for trigger in triggers:
            self.c.execute(f'DROP TRIGGER "{trigger[0]}"')
        self.c.execute('DROP TABLE session_browse')
        self.c.execute('DROP TABLE project_browse_revision')
        self.c.commit()
        init_db(self.path, defer_backfills=True)
        self.assertEqual(len(project_catalog(self.c).rows), 1)
        self.c.execute("UPDATE sessions SET summary='After migration' WHERE id='s1'")
        self.c.commit()
        self.assertEqual(project_catalog(self.c).groups[0].latest_summary, 'After migration')

    def test_sessions_tab_does_not_fetch_timeline(self):
        from agent_operations_viewer.web.context import AppContext, set_app_context
        from agent_operations_viewer.web.routes.projects import render_group_detail
        self.insert()
        app = SimpleNamespace(state=SimpleNamespace())
        templates = SimpleNamespace(TemplateResponse=lambda request, **kwargs: kwargs['context'])
        settings = SimpleNamespace(database_path=self.path, page_size=24)
        set_app_context(app, AppContext(settings=settings, templates=templates))
        request = Request({'type': 'http', 'method': 'GET', 'path': '/host/one',
                           'headers': [], 'query_string': b'', 'app': app})
        with patch('agent_operations_viewer.web.routes.projects.fetch_turn_stream',
                   side_effect=AssertionError('Inactive timeline must not load')):
            context = render_group_detail(request, 'project:one', view='sessions')
        self.assertIsNone(context['stream_preview'])
        self.assertEqual(len(context['all_sessions_page']['items']), 1)

    def test_today_count_uses_index_and_keeps_timezone_boundaries(self):
        self.insert()
        self.c.execute("UPDATE session_turns SET prompt_timestamp='2026-09-12T00:30:00+02:00'")
        self.assertEqual(count_session_turn_prompts_since(self.c, ['s1'], '2026-09-12T00:00:00Z'), 0)
        self.assertEqual(count_session_turn_prompts_since(self.c, ['s1'], '2026-09-11T22:00:00Z'), 1)
        plan = self.c.execute("EXPLAIN QUERY PLAN SELECT count(*) FROM session_turns WHERE session_id=? AND CAST(strftime('%s', prompt_timestamp) AS INTEGER)>=CAST(strftime('%s', ?) AS INTEGER)", ('s1','2026-09-12T00:00:00Z')).fetchall()
        self.assertIn('idx_session_turns_prompt_epoch', str([tuple(r) for r in plan]))


if __name__ == '__main__':
    unittest.main()
