import asyncio
from dataclasses import replace
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

from agent_daemon.remote_sync import RemoteSyncBusyError, _retry_after_seconds, sync_sessions_remote
from agent_daemon.runtime import _busy_retry_delay, run_sync_daemon
from agent_daemon.remote_sync import RestartRequired
from agent_operations_viewer.backup_restore import create_instance_backup, restore_instance_backup
from agent_operations_viewer.db import connection_scope, init_db
from agent_operations_viewer.importer import parse_session_text
from agent_operations_viewer.session_parsing import session_content_sha256
from agent_operations_viewer.session_artifacts import (
    load_session_artifact_text, prune_orphaned_session_artifacts,
)
from agent_operations_viewer.web import concurrency
from agent_operations_viewer.web.app import _run_artifact_maintenance
from agent_operations_viewer.web.routes import sync_api
from tests.test_rollout_parsing import make_raw_session_jsonl, make_test_settings


class UploadAdmissionTests(unittest.TestCase):
    def test_capacity_is_checked_before_reading_body(self):
        executor = concurrency._BoundedWorkExecutor(name='test', worker_count=1, max_inflight=1)
        executor._inflight[object()] = None
        messages = []
        async def should_not_run(*args):
            self.fail('Busy request read its body or reached authentication')
        async def send(message):
            messages.append(message)
        async def exercise():
            with mock.patch.object(concurrency, '_UPLOAD_EXECUTOR', executor):
                await concurrency.UploadAdmissionMiddleware(should_not_run)(
                    {'type': 'http', 'method': 'POST', 'path': '/api/sync/session-raw'}, should_not_run, send)
        asyncio.run(exercise())
        self.assertEqual(messages[0]['status'], 503)
        self.assertIn((b'retry-after', b'5'), messages[0]['headers'])

    def test_real_app_transfers_reservation_into_upload_worker(self):
        import httpx
        from agent_operations_viewer.web.app import create_app
        with tempfile.TemporaryDirectory() as tmp:
            settings = make_test_settings(data_dir=Path(tmp), session_roots=[])
            app = create_app(settings)
            executor = concurrency._BoundedWorkExecutor(name='test', worker_count=1, max_inflight=1)
            raw = make_raw_session_jsonl('http-upload')
            payload = dict(source_host='host', source_root='/tmp', source_path='/tmp/http-upload.jsonl',
                           raw_jsonl=raw, file_size=len(raw.encode()), file_mtime_ns=1)
            async def exercise():
                with mock.patch.object(concurrency, '_UPLOAD_EXECUTOR', executor), \
                     mock.patch.object(sync_api, 'require_sync_api_auth', new=mock.AsyncMock()):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                        response = await client.post('/api/sync/session-raw', json=payload)
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(executor.inflight_count(), 0)
            asyncio.run(exercise())
            with connection_scope(settings.database_path) as c:
                self.assertIsNotNone(c.execute("SELECT id FROM sessions WHERE id='http-upload'").fetchone())

    def test_worker_keeps_slot_when_request_is_cancelled(self):
        executor = concurrency._BoundedWorkExecutor(name='test', worker_count=1, max_inflight=1)
        started, release = threading.Event(), threading.Event()
        def work():
            started.set()
            release.wait(5)
        async def app(*args):
            await concurrency.run_in_upload_threadpool(work, dedupe_key=('session', 'one'))
        async def noop(*args):
            pass
        async def exercise():
            with mock.patch.object(concurrency, '_UPLOAD_EXECUTOR', executor):
                request = asyncio.create_task(concurrency.UploadAdmissionMiddleware(app)(
                    {'type': 'http', 'method': 'POST', 'path': '/api/sync/session-tail'}, noop, noop))
                try:
                    for _ in range(100):
                        if started.is_set():
                            break
                        await asyncio.sleep(.01)
                    self.assertTrue(started.is_set())
                    request.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await request
                    self.assertEqual(executor.inflight_count(), 1)
                finally:
                    release.set()
                for _ in range(100):
                    if executor.inflight_count() == 0:
                        break
                    await asyncio.sleep(.01)
                self.assertEqual(executor.inflight_count(), 0)
        asyncio.run(exercise())

    def test_failed_request_releases_reservation(self):
        executor = concurrency._BoundedWorkExecutor(name='test', worker_count=1, max_inflight=1)
        async def fail(*args):
            raise ValueError('invalid request')
        async def exercise():
            with mock.patch.object(concurrency, '_UPLOAD_EXECUTOR', executor):
                with self.assertRaises(ValueError):
                    await concurrency.UploadAdmissionMiddleware(fail)(
                        {'type': 'http', 'method': 'POST', 'path': '/api/sync/session'}, fail, fail)
                self.assertEqual(executor.inflight_count(), 0)
        asyncio.run(exercise())


class ArtifactAppendTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.settings = make_test_settings(data_dir=self.root / 'data', session_roots=[])
        self.settings.ensure_directories()
        init_db(self.settings.database_path)
        self.raw = make_raw_session_jsonl('append', user_message='base ' * 20000)
        parsed = parse_session_text(self.raw, Path('/tmp/append.jsonl'), Path('/tmp'), 'host',
                                    file_size=len(self.raw.encode()), file_mtime_ns=1)
        with connection_scope(self.settings.database_path) as c:
            sync_api.store_raw_sync_sessions_batch(c, self.settings, [(parsed, self.raw)])

    def append(self, marker):
        tail = '\n' + json.dumps({'type': 'event_msg', 'timestamp': '2026-10-05T00:00:00Z',
                                  'payload': {'type': 'user_message', 'message': marker}})
        with connection_scope(self.settings.database_path) as c:
            row = c.execute("SELECT * FROM sessions WHERE id='append'").fetchone()
        payload = dict(source_host='host', source_root='/tmp', source_path='/tmp/append.jsonl',
                       base_file_size=row['file_size'], base_content_sha256=row['content_sha256'],
                       tail_jsonl=tail, file_size=len((self.raw + tail).encode()), file_mtime_ns=2)
        result = sync_api._process_raw_sync_session_tail(self.settings, payload, header_host='host')
        self.assertTrue(result['incremental'])
        self.raw += tail
        return tail

    def test_only_append_is_compressed_and_base_read_is_outside_writer(self):
        compressed_inputs = []
        real_compress = gzip.compress
        real_load = sync_api.iter_session_artifact_bytes
        def compress(data, **kwargs):
            compressed_inputs.append(data)
            return real_compress(data, **kwargs)
        def load(c, *args):
            self.assertFalse(c.in_transaction, 'historical read held SQLite writer')
            return real_load(c, *args)
        with mock.patch('agent_operations_viewer.session_artifacts.gzip.compress', side_effect=compress), \
             mock.patch.object(sync_api, 'iter_session_artifact_bytes', side_effect=load), \
             mock.patch('agent_operations_viewer.session_artifacts.prune_orphaned_session_artifacts') as prune:
            tail = self.append('smallmarker')
        self.assertEqual(compressed_inputs, [tail.encode()])
        prune.assert_not_called()
        with connection_scope(self.settings.database_path) as c:
            sha = c.execute("SELECT raw_artifact_sha256 FROM sessions WHERE id='append'").fetchone()[0]
            self.assertEqual(load_session_artifact_text(c, self.settings, sha), self.raw)

    def test_streamed_checksums_match_full_text_across_utf8_and_line_boundaries(self):
        tail = '\n' + json.dumps({'type': 'event_msg', 'payload': {'type': 'user_message', 'message': 'new'}})
        # Split every byte to exercise CR/LF and multibyte decoder boundaries.
        base = make_raw_session_jsonl('append', user_message='café').replace('caf\\u00e9', 'café').replace('\n', '\r\n') + '\r\n'
        payload = dict(source_host='host', source_path='/tmp/append.jsonl', tail_jsonl=tail,
                       base_file_size=len(base.encode()), base_content_sha256='basehash')
        with connection_scope(self.settings.database_path) as c:
            c.execute("UPDATE sessions SET file_size=?, content_sha256='basehash' WHERE id='append'", (len(base.encode()),))
            with mock.patch.object(sync_api, 'iter_session_artifact_bytes',
                                   return_value=(bytes([b]) for b in base.encode())):
                prepared = sync_api._prepare_tail_base(c, self.settings, payload)
        self.assertEqual(prepared.content_sha256, session_content_sha256(base + tail))
        self.assertEqual(prepared.combined_sha256, hashlib.sha256((base + tail).encode()).hexdigest())
        self.assertEqual(prepared.base_line_count, len(base.splitlines(keepends=True)))
        self.assertTrue(prepared.base_ends_newline)

    def test_cleanup_preserves_ancestors_and_removes_deleted_chain(self):
        self.append('firstmarker')
        self.append('secondmarker')
        self.assertEqual(prune_orphaned_session_artifacts(self.settings), 0)
        with connection_scope(self.settings.database_path) as c:
            sha = c.execute("SELECT raw_artifact_sha256 FROM sessions WHERE id='append'").fetchone()[0]
            self.assertEqual(load_session_artifact_text(c, self.settings, sha), self.raw)
            self.assertEqual(c.execute('SELECT count(*) FROM session_artifacts').fetchone()[0], 3)
            c.execute("DELETE FROM sessions WHERE id='append'")
        self.assertEqual(prune_orphaned_session_artifacts(self.settings), 3)
        self.assertEqual(list((self.settings.data_dir / 'session_artifacts').glob('*/*.gz')), [])

    def test_segmented_artifact_survives_backup_restore(self):
        self.append('backupmarker')
        archive = self.root / 'backup.zip'
        create_instance_backup(self.settings, output_path=archive)
        restored_dir = self.root / 'restored'
        restore_instance_backup(archive, target_data_dir=restored_dir)
        restored = replace(self.settings, data_dir=restored_dir, database_path=restored_dir / 'viewer.sqlite3')
        with connection_scope(restored.database_path) as c:
            sha = c.execute("SELECT raw_artifact_sha256 FROM sessions WHERE id='append'").fetchone()[0]
            self.assertEqual(load_session_artifact_text(c, restored, sha), self.raw)

    def test_rollback_leaves_recoverable_untracked_piece(self):
        old_raw = self.raw
        with mock.patch.object(sync_api, 'append_parsed_session_tail', side_effect=ValueError('failed write')):
            with self.assertRaises(ValueError):
                self.append('rollbackmarker')
        with connection_scope(self.settings.database_path) as c:
            sha = c.execute("SELECT raw_artifact_sha256 FROM sessions WHERE id='append'").fetchone()[0]
            self.assertEqual(load_session_artifact_text(c, self.settings, sha), old_raw)
        self.assertEqual(prune_orphaned_session_artifacts(self.settings), 1)
        self.append('rollbackmarker')


class UploadBackoffTests(unittest.TestCase):
    def test_single_busy_response_stops_pass_and_preserves_completed_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'sessions'
            root.mkdir()
            for name in ['a', 'b', 'c']:
                (root / (name + '.jsonl')).write_text(make_raw_session_jsonl(name))
            settings = make_test_settings(data_dir=Path(tmp) / 'data', session_roots=[root], remote_batch_size=1)
            uploads = []
            def request(settings, method, path, payload=None):
                if path == '/api/sync/heartbeat':
                    return {'status': 'ok'}
                if path == '/api/sync/session-raw':
                    uploads.append(payload['source_path'])
                    if len(uploads) == 2:
                        raise RemoteSyncBusyError('busy', retry_after_seconds=45)
                    return {'status': 'ok'}
                self.fail(path)
            with mock.patch('agent_daemon.remote_sync.json_request', side_effect=request):
                stats = sync_sessions_remote(settings, force=True)
            self.assertEqual(len(uploads), 2)
            self.assertEqual(stats, {'uploaded': 1, 'skipped': 0, 'failed': 2, 'retry_after_seconds': 45})

    def test_parallel_uploads_stop_scheduling_when_busy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'sessions'
            root.mkdir()
            for i in range(8):
                (root / f'{i}.jsonl').write_text(make_raw_session_jsonl(str(i)))
            settings = replace(make_test_settings(data_dir=Path(tmp) / 'data', session_roots=[root], remote_batch_size=1), remote_upload_workers=2)
            uploads = []
            barrier = threading.Barrier(2)
            def request(settings, method, path, payload=None):
                if path == '/api/sync/heartbeat':
                    return {'status': 'ok'}
                if path == '/api/sync/session-raw':
                    uploads.append(payload['source_path'])
                    barrier.wait(timeout=5)
                    raise RemoteSyncBusyError('busy', 10)
                self.fail(path)
            with mock.patch('agent_daemon.remote_sync.json_request', side_effect=request):
                stats = sync_sessions_remote(settings, force=True)
            self.assertEqual(len(uploads), 2)
            self.assertEqual(stats['failed'], 8)
            self.assertEqual(stats['retry_after_seconds'], 10)

    def test_watcher_cannot_bypass_busy_cooldown_and_retry_rescans(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = make_test_settings(data_dir=Path(tmp), session_roots=[])
            stop = mock.Mock()
            stop.is_set.return_value = False
            stop.wait.return_value = False
            with mock.patch('agent_daemon.runtime.threading.Event', return_value=stop), \
                 mock.patch('agent_daemon.runtime.signal.signal'), \
                 mock.patch('agent_daemon.runtime.time.monotonic', return_value=100), \
                 mock.patch('agent_daemon.runtime.random.uniform', return_value=0), \
                 mock.patch('agent_daemon.runtime.SessionFileWatcher') as watcher, \
                 mock.patch('agent_daemon.runtime.sync_sessions_remote', side_effect=[
                     {'uploaded': 0, 'failed': 1, 'skipped': 0, 'retry_after_seconds': 45},
                     RestartRequired('test stop'),
                 ]) as sync:
                self.assertEqual(run_sync_daemon(settings, 30), 75)
                stop.wait.assert_called_once_with(45)
                watcher.return_value.wait_for_changes.assert_not_called()
                self.assertIsNone(sync.call_args.kwargs['candidate_paths'])

    def test_retry_delay_honors_header_and_increases_to_bounded_backoff(self):
        self.assertEqual(_retry_after_seconds('45'), 45)
        self.assertEqual(_retry_after_seconds('invalid'), 5)
        with mock.patch('agent_daemon.runtime.random.uniform', return_value=0):
            self.assertEqual(_busy_retry_delay(1, 45, 30), 45)
            self.assertEqual(_busy_retry_delay(2, 5, 30), 60)
            self.assertEqual(_busy_retry_delay(20, 5, 30), 300)

    def test_artifact_cleanup_waits_for_maintenance_interval(self):
        stop = mock.Mock()
        stop.wait.side_effect = [False, True]
        with mock.patch('agent_operations_viewer.web.app.prune_orphaned_session_artifacts') as prune:
            _run_artifact_maintenance(mock.sentinel.settings, stop)
        self.assertEqual(stop.wait.call_args_list, [mock.call(3600), mock.call(3600)])
        prune.assert_called_once_with(mock.sentinel.settings)
