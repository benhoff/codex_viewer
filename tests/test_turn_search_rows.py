import json
from pathlib import Path
import tempfile
import unittest

from agent_operations_viewer.db import connection_scope, init_db
from agent_operations_viewer.importer import parse_session_text, upsert_parsed_session
from agent_operations_viewer.turn_index import (
    backfill_session_turn_search,
    reindex_session_turn_search_for_project_keys,
    replace_session_turn_suffix,
)


class TurnSearchRowsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'viewer.sqlite3'
        init_db(self.path)

    def parsed(self, session_id, prompt='originalmarker'):
        records = [
            {'type': 'session_meta', 'payload': {'id': session_id,
             'timestamp': '2026-10-04T09:00:00Z', 'cwd': '/workspace/project'}},
            {'type': 'event_msg', 'timestamp': '2026-10-04T09:00:01Z',
             'payload': {'type': 'user_message', 'message': prompt}},
        ]
        return parse_session_text('\n'.join(json.dumps(r) for r in records),
                                  Path('/tmp/' + session_id + '.jsonl'), Path('/tmp'), 'host')

    def assert_mapping(self, connection):
        fts = [tuple(r) for r in connection.execute(
            'SELECT rowid, session_id, CAST(turn_number AS INTEGER) FROM session_turn_search ORDER BY rowid')]
        mapping = [tuple(r) for r in connection.execute(
            'SELECT search_rowid, session_id, turn_number FROM session_turn_search_rows ORDER BY search_rowid')]
        self.assertEqual(fts, mapping)

    def test_reimport_and_session_delete_preserve_other_session(self):
        with connection_scope(self.path) as c:
            upsert_parsed_session(c, self.parsed('target'))
            upsert_parsed_session(c, self.parsed('other', 'othermarker'))
            other_before = tuple(c.execute(
                "SELECT rowid, prompt_text FROM session_turn_search WHERE session_id='other'").fetchone())
            upsert_parsed_session(c, self.parsed('target', 'replacementmarker'))
            self.assert_mapping(c)
            self.assertEqual(c.execute("SELECT count(*) FROM session_turn_search WHERE session_turn_search MATCH 'originalmarker'").fetchone()[0], 0)
            self.assertEqual(c.execute("SELECT count(*) FROM session_turn_search WHERE session_turn_search MATCH 'replacementmarker'").fetchone()[0], 1)
            c.execute("DELETE FROM sessions WHERE id='target'")
            self.assert_mapping(c)
            self.assertEqual(tuple(c.execute('SELECT rowid, prompt_text FROM session_turn_search').fetchone()), other_before)

    def test_suffix_replacement_preserves_closed_turn_and_other_session(self):
        with connection_scope(self.path) as c:
            upsert_parsed_session(c, self.parsed('target'))
            upsert_parsed_session(c, self.parsed('other'))
            closed = tuple(c.execute("SELECT rowid, prompt_text FROM session_turn_search WHERE session_id='target'").fetchone())
            for prompt in ['obsoleteopenmarker', 'currentopenmarker']:
                replace_session_turn_suffix(c, 'target', self.parsed('target', prompt).events, start_turn_number=2)
                self.assert_mapping(c)
            self.assertEqual(tuple(c.execute("SELECT rowid, prompt_text FROM session_turn_search WHERE session_id='target' AND turn_number=1").fetchone()), closed)
            self.assertEqual(c.execute("SELECT count(*) FROM session_turn_search WHERE session_turn_search MATCH 'obsoleteopenmarker'").fetchone()[0], 0)
            self.assertEqual(c.execute("SELECT count(*) FROM session_turn_search WHERE session_turn_search MATCH 'currentopenmarker'").fetchone()[0], 1)

    def test_backfill_and_project_reindex_keep_mapping_in_sync(self):
        with connection_scope(self.path) as c:
            parsed = self.parsed('target')
            upsert_parsed_session(c, parsed)
            c.execute("UPDATE sessions SET turn_search_version=0 WHERE id='target'")
            self.assertEqual(backfill_session_turn_search(c, batch_size=1), 1)
            self.assert_mapping(c)
            self.assertEqual(reindex_session_turn_search_for_project_keys(c, [parsed.inferred_project_key]), 1)
            self.assert_mapping(c)

    def test_existing_database_migration_preserves_search_rows_and_is_idempotent(self):
        with connection_scope(self.path) as c:
            upsert_parsed_session(c, self.parsed('target'))
            before = tuple(c.execute('SELECT rowid, prompt_text FROM session_turn_search').fetchone())
            c.execute('DROP TABLE session_turn_search_rows')
            c.execute('DROP TRIGGER session_turn_search_delete_session')
            c.execute('CREATE TRIGGER session_turn_search_delete_session AFTER DELETE ON sessions BEGIN DELETE FROM session_turn_search WHERE session_id=OLD.id; END')
        for _ in range(2):
            init_db(self.path, defer_backfills=True)
            with connection_scope(self.path) as c:
                self.assert_mapping(c)
                self.assertEqual(tuple(c.execute('SELECT rowid, prompt_text FROM session_turn_search').fetchone()), before)
        with connection_scope(self.path) as c:
            c.execute("DELETE FROM sessions WHERE id='target'")
            self.assert_mapping(c)
            self.assertEqual(c.execute('SELECT count(*) FROM session_turn_search').fetchone()[0], 0)

    def test_deletion_plans_use_mapping_index_and_fts_rowid(self):
        with connection_scope(self.path) as c:
            plan = ' '.join(r[3] for r in c.execute(
                'EXPLAIN QUERY PLAN DELETE FROM session_turn_search_rows WHERE session_id=? AND turn_number>=?', ('target', 2)))
            self.assertIn('idx_turn_search_rows_session_turn', plan)
            fts_plan = ' '.join(r[3] for r in c.execute(
                'EXPLAIN QUERY PLAN DELETE FROM session_turn_search WHERE rowid=?', (1,)))
            self.assertIn('session_turn_search VIRTUAL TABLE INDEX 0:=', fts_plan)
            trigger = c.execute(
                "SELECT sql FROM sqlite_master WHERE name='session_turn_search_rows_delete_fts'").fetchone()[0]
            self.assertIn('WHERE rowid = OLD.search_rowid', trigger)
