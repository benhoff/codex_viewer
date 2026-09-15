import io
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from agent_operations_viewer.search_client import SearchClient, SearchClientError, content_digest, verify_turn


def response(value):
    return io.BytesIO(json.dumps(value).encode())


def error(code, detail, headers=None):
    return HTTPError('http://localhost/api/v1/search', code, 'error', headers or {}, response({'detail': detail}))


class SearchClientTests(unittest.TestCase):
    def test_pending_retries_pin_id_and_preserve_repeated_filters(self):
        client = SearchClient('http://localhost', 'secret')
        with patch.object(client._opener, 'open', side_effect=[
            error(503, {'code': 'snapshot_building', 'snapshot_id': 'pinned'}, {'Retry-After': '3'}),
            response({'snapshot_id': 'pinned', 'next_cursor': 'page2'}),
            response({'snapshot_id': 'pinned', 'next_cursor': None}),
        ]) as transport, patch('agent_operations_viewer.search_client.time.sleep') as sleep:
            pages = list(client.search('a & b', exclude_session_id=['s1', 's2']))
        self.assertEqual(len(pages), 2)
        sleep.assert_called_once_with(3)
        requests = [call.args[0] for call in transport.call_args_list]
        self.assertNotIn('snapshot_id', requests[0].full_url)
        self.assertIn('snapshot_id=pinned', requests[1].full_url)
        self.assertIn('cursor=page2', requests[2].full_url)
        self.assertIn('exclude_session_id=s1&exclude_session_id=s2', requests[2].full_url)
        self.assertNotIn('secret', requests[2].full_url)

    def test_terminal_errors_do_not_replace_snapshot(self):
        for status, code in [(410, 'snapshot_expired'), (503, 'snapshot_build_failed'),
                             (503, 'snapshot_build_interrupted'), (403, 'snapshot_access_revoked'),
                             (503, 'query_timeout')]:
            client = SearchClient('http://localhost', 'secret')
            client.snapshot_id = 'pinned'
            with patch.object(client._opener, 'open', side_effect=error(status, {'code': code})) as transport:
                with self.assertRaises(SearchClientError) as exc:
                    client.get('/api/v1/search', {'q': 'x'})
            self.assertEqual(exc.exception.code, code)
            self.assertEqual(transport.call_count, 1)
            self.assertEqual(client.snapshot_id, 'pinned')

    def test_wait_budget_and_snapshot_changes(self):
        client = SearchClient('http://localhost', 'secret', preparation_timeout=1)
        with patch.object(client._opener, 'open', side_effect=error(503, {'code': 'snapshot_building'}, {'Retry-After': '2'})):
            with self.assertRaisesRegex(SearchClientError, 'client_preparation_timeout'):
                client.get('/api/v1/search')

    def test_explicit_snapshot_is_verified_on_first_request(self):
        client = SearchClient('http://localhost', 'secret')
        with patch.object(client._opener, 'open', return_value=response({'snapshot_id': 'wrong'})):
            with self.assertRaisesRegex(SearchClientError, 'snapshot_changed'):
                client.get('/api/v1/search', {'snapshot_id': 'requested'})
        client.snapshot_id = 'original'
        with patch.object(client._opener, 'open', return_value=response({'snapshot_id': 'changed'})):
            with self.assertRaisesRegex(SearchClientError, 'snapshot_changed'):
                client.get('/api/v1/search')

    def test_digest_detects_modified_and_wrong_hit_evidence(self):
        turn = {'turn_number': 1, 'prompt': {'text': 'λ\n'}, 'activity': [], 'source_events': []}
        identity = content_digest({'normalization_version': 'evidence-2', 'turn': turn})
        turn.update(content_digest=identity, content_version=identity[7:], normalization_version='evidence-2',
                    activity_digest=content_digest({'normalization_version': 'evidence-2', 'activity': []}), is_target=True)
        verify_turn(turn, identity)
        with self.assertRaisesRegex(SearchClientError, 'search_hit_digest_mismatch'):
            verify_turn(turn, 'sha256:wrong')
        turn['prompt']['text'] = 'changed'
        with self.assertRaisesRegex(SearchClientError, 'digest_mismatch'):
            verify_turn(turn)

    def test_activity_reorders_ordinals_and_detects_missing_events(self):
        events = [{'event_index': 2}, {'event_index': 1}]
        digest = content_digest({'normalization_version': 'evidence-2', 'activity': events})
        def page(ordinal):
            return {'normalization_version': 'evidence-2', 'activity_digest': digest, 'total_count': 2,
                    'activity': [{**events[ordinal], 'activity_ordinal': ordinal,
                                  'activity_id': content_digest({'turn': digest, 'ordinal': ordinal})}]}
        client = SearchClient('http://localhost', 'secret')
        with patch.object(client, 'pages', return_value=iter([page(1), page(0)])):
            self.assertEqual(client.activity('s', 1), events)
        with patch.object(client, 'pages', return_value=iter([page(1)])):
            with self.assertRaisesRegex(SearchClientError, 'incomplete_activity'):
                client.activity('s', 1)

    def test_repeated_cursor_does_not_loop(self):
        client = SearchClient('http://localhost', 'secret')
        with patch.object(client, 'get', return_value={'next_cursor': 'same'}):
            with self.assertRaisesRegex(SearchClientError, 'repeated_cursor'):
                list(client.search('x'))
