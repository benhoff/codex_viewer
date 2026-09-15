import copy
import unittest

from agent_operations_viewer import evidence_consolidation as c
from agent_operations_viewer import llm_grader as g


class ConsolidationTests(unittest.TestCase):
    def event(self, text, index=1, **extra):
        return {'event_index': index, 'kind': 'tool_result', 'text': text, **extra}

    def test_choices_are_exact_and_do_not_rewrite_nonconsecutive_or_formatted_values(self):
        values = ','.join(map(str, range(1, 36001)))
        text = f'usage: probe\n    [--frames {{{values}}}]\noptions:\n  --frames {{{values}}}\n'
        result, stats = c.consolidate_events([self.event(text)])
        self.assertEqual(result[0]['text'].count('integers 1 through 36000 inclusive'), 2)
        self.assertGreater(stats['bytes_removed'], 400000)
        self.assertEqual(stats['events'][0]['sha256'], c.digest(text))
        for invalid in (values.replace(',100,', ',102,'), values.replace(',100,', ',0100,')):
            data = f'usage: probe\n --frames {{{invalid}}}\n'
            self.assertEqual(c.compact_options(data), data)
        self.assertEqual(c.compact_options(text.replace('usage:', 'example:')), text.replace('usage:', 'example:'))

    def test_known_artifacts_have_provenance_and_unfamiliar_long_lines_stay(self):
        index = '/usr/src/Documentation/searchindex.js:1:Search.setIndex(' + '"foo":1,' * 1500
        source_map = '/home/u/.local/share/pnpm/store/v10/files/aa/hash:1:{"version":3,"mappings":"' + 'AAAA;' * 2000
        unknown = 'const applicationState = ' + 'x' * 12000
        text = index + '\n' + source_map + '\n' + 'AAAA;' * 2000 + '\n' + unknown + '\nFailure: missing driver\n'
        result, stats = c.consolidate_events([self.event(text)])
        self.assertIn('generated documentation search index', result[0]['text'])
        self.assertIn('origin=/usr/src/Documentation/searchindex.js:1:', result[0]['text'])
        self.assertEqual(result[0]['text'].count('generated source map'), 2)
        self.assertIn(unknown, result[0]['text'])
        self.assertIn('Failure: missing driver', result[0]['text'])
        self.assertEqual(stats['rules']['generated_artifact'], 1)

    def test_artifact_detection_does_not_follow_into_another_record(self):
        a = '/usr/src/Documentation/searchindex.js:1:Search.setIndex(' + 'x' * 5000
        tail = 'abc":123,' + 'x' * 5000
        result, _ = c.consolidate_events([self.event(a), self.event(tail, 2)])
        self.assertEqual(result[1]['text'], tail)
        unfamiliar = 'const message = "/usr/src/searchindex.js:1:Search.setIndex(' + 'x' * 5000
        self.assertEqual(c.compact_artifacts(unfamiliar), unfamiliar)

    def test_truncated_source_map_and_explicit_artifact_review(self):
        text = '/home/u/.local/share/pnpm/store/v10/files/aa/hash:1:{"version":3,"sources":["a.js"],"sourcesContent":["' + 'x' * 5000
        text += '\n' + 'AAAA;' * 2000 + '"}\nActual failure: missing import\n'
        result, _ = c.consolidate_events([self.event(text)])
        self.assertEqual(result[0]['text'].count('generated source map'), 2)
        self.assertIn('Actual failure: missing import', result[0]['text'])
        for criteria in ('Review the source map contents.', 'Inspect the pnpm store.'):
            result, _ = c.consolidate_events([self.event(text)], criteria=criteria)
            self.assertEqual(result[0]['text'], text)
        events = [self.event('Fix the source maps.', 0, kind='message', role='user'), self.event(text)]
        result, _ = c.consolidate_events(events)
        self.assertEqual(result, events)
        unknown = text.replace('"sourcesContent"', '"customData"')
        self.assertEqual(c.compact_artifacts(unknown), unknown)

    def test_log_groups_keep_failures_recovery_order_values_and_samples(self):
        def lines(message, start, count=20):
            return ''.join(f'homebox-1 | 2026/09/12 08:00:{i:02d} ERROR file.go:17 {message}\n' for i in range(start, start + count))
        first = lines('connection refused', 0)
        recovery = 'homebox-1 | 2026/09/12 08:00:20 INFO connection restored\n'
        last = lines('connection refused', 21)
        changed = lines('connection refused: retries=2', 41, 5)
        text = first + recovery + last + changed + 'Exit code: 1\n'
        result, _ = c.consolidate_events([self.event(text, exit_code=1)])
        new = result[0]['text']
        self.assertEqual(new.count('20 consecutive occurrences'), 2)
        self.assertLess(new.index('08:00:19'), new.index('connection restored'))
        self.assertLess(new.index('connection restored'), new.index('08:00:21'))
        self.assertIn('retries=2', new)
        self.assertIn('Exit code: 1', new)
        self.assertEqual(result[0]['exit_code'], 1)
        varied = ''.join(f'2026-09-12T08:00:00Z ERROR measured={i}\n' for i in range(200))
        self.assertEqual(c.compact_logs(varied), varied)
        levels = ''.join(f'<{level}>2026-09-12 08:00:{i:02d} same message\n'
                         for i, level in enumerate(['W', 'E'] * 20))
        self.assertEqual(c.compact_logs(levels), levels)

    def test_user_instructions_assistant_answers_and_patches_are_unchanged(self):
        bulk = '/usr/src/Documentation/searchindex.js:1:Search.setIndex(' + 'x' * 6000
        events = [self.event(bulk, 1, kind='message', role='user'),
                  self.event(bulk, 2, kind='message', role='assistant'),
                  self.event(bulk, 3, kind='system', role='developer'),
                  self.event('diff --git a/file b/file\n@@ -1 +1 @@\n' + bulk, 4)]
        before = copy.deepcopy(events)
        after, stats = c.consolidate_events(events)
        self.assertEqual(after, before)
        self.assertEqual(events, before)
        self.assertEqual(stats['changed_events'], 0)

    def test_lint_retains_each_location_rule_file_and_severity(self):
        first = ''.join(f' {i}:2  warning  Avoid using an untyped value in this expression  @typescript-eslint/no-explicit-any\n' for i in range(1, 21))
        text = '/src/first.ts\n' + first + ' 21:1  error  This import is missing  import/no-unresolved\n/src/second.ts\n' + first
        result, stats = c.consolidate_events([self.event(text, exit_code=1)])
        new = result[0]['text']
        self.assertEqual(new.count('20 lint warning diagnostics'), 2)
        self.assertIn(','.join(f'{i}:2' for i in range(1, 21)), new)
        self.assertIn('21:1  error  This import is missing', new)
        self.assertIn('/src/first.ts', new)
        self.assertIn('/src/second.ts', new)
        self.assertIn('@typescript-eslint/no-explicit-any', new)
        self.assertEqual(stats['rules']['lint_diagnostics'], 1)

    def test_exact_repeats_keep_occurrences_and_distinct_status(self):
        text = 'Observed complete output\n' * 200
        events = [self.event(text, 1, exit_code=0), self.event(text, 2, exit_code=0), self.event(text, 3, exit_code=1)]
        before = copy.deepcopy(events)
        after, stats = c.consolidate_events(events)
        self.assertIn('identical to event 1', after[1]['text'])
        self.assertEqual(after[2]['text'], text)
        self.assertEqual([e['event_index'] for e in after], [1, 2, 3])
        self.assertEqual(events, before)
        self.assertEqual(stats['rules']['exact_repeat'], 1)

    def test_known_legacy_header_retains_command_and_exit_status(self):
        text = 'Command: pytest\nChunk ID: abc\nWall time: 0.01 seconds\nProcess exited with code 1\nOriginal token count: 40\nOutput:\n1 failed\n'
        self.assertEqual(c.unwrap_header(text), 'Command: pytest\n1 failed\n\nExit code: 1')
        self.assertEqual(c.unwrap_header('Some text\n' + text), 'Some text\n' + text)

    def test_request_context_keeps_prose_before_inside_and_after_pasted_diagnostics(self):
        rows = ''.join(f'03:55:02 0 {i} 0.00 0.00 0.00 0 worker\n' for i in range(10))
        text = 'Find the latency cause.\n' + rows + 'Do not restart the server.\n' + rows + 'Explain the fix first.\n'
        event = self.event(text, kind='message', role='user')
        context = c.request_view(event)
        self.assertIn('Find the latency cause.', context['text'])
        self.assertIn('Do not restart the server.', context['text'])
        self.assertIn('Explain the fix first.', context['text'])
        self.assertEqual(context['diagnostic_lines_in_evidence'], 20)
        self.assertEqual(event['text'], text)
        self.assertIn('retained in primary evidence', context['text'])
        prose = self.event('Please review 12 rows and 9 columns.\n' * 100, kind='message', role='user')
        self.assertEqual(c.request_view(prose), prose)

    def test_batch_plans_preserve_primary_diagnostics_and_share_compact_requests(self):
        rows = ''.join(f'03:55:02 0 {i} 0.00 0.00 0.00 0.00 0 0 worker\n' for i in range(600))
        report = {'evidence': [{'event_index': 1, 'kind': 'message', 'role': 'user', 'display_text': 'Find the bottleneck.\n' + rows + 'Keep the server running.'},
                               {'event_index': 2, 'kind': 'message', 'role': 'assistant', 'display_text': 'The database work is the bottleneck.'}],
                  'turns': [{'turn_number': 1, 'start_event_index': 1}], 'metrics': {'configurations': []}}
        original = copy.deepcopy(report)
        plan = g.grader_input(report, '', g.DEFAULT_CONFIG)
        self.assertEqual(plan['consolidation']['diagnostic_lines_separated'], 600)
        self.assertEqual(report, original)
        for batch in plan['demand_batches']:
            self.assertNotIn('task_overview', batch)
            context = batch['task_context']['requests'][0]['text']
            self.assertIn('Keep the server running.', context)
            self.assertNotIn('03:55:02 0 599', context)
            self.assertLessEqual(g.json_size(batch), g.evidence_budget(g.DEFAULT_CONFIG, g.EXTRACTION_PROMPT, g.EvidenceNotes))
        fragments = [e for b in plan['demand_batches'] for e in b['events'] if e['event_index'] == 1]
        import json
        restored = json.loads(''.join(e['event_json_fragment'] for e in fragments))
        self.assertEqual(restored['text'], report['evidence'][0]['display_text'])

    def test_adjacent_small_turns_pack_together(self):
        events = [{'event_index': i, 'kind': 'message', 'role': 'assistant', 'text': 'Answer ' + str(i)} for i in range(1, 5)]
        turns = [{'turn_number': i, 'start_event_index': i} for i in range(1, 5)]
        batches = g.contextual_batches({'events': events, 'acceptance_criteria': ''}, turns, 3000)
        self.assertEqual(len(batches), 1)
        self.assertEqual([e['turn_number'] for e in batches[0]['events']], [1, 2, 3, 4])

    def test_small_action_and_result_stay_together_at_a_batch_boundary(self):
        events = [{'event_index': 1, 'kind': 'message', 'text': 'x' * 600},
                  {'event_index': 2, 'kind': 'tool_call', 'text': 'pytest ' + 'y' * 100},
                  {'event_index': 3, 'kind': 'tool_result', 'text': '1 failed ' + 'z' * 100}]
        batches = g.batch_evidence({'events': events}, [{'turn_number': 1, 'start_event_index': 1}], 1000)
        self.assertEqual([[e['event_index'] for e in b['events']] for b in batches], [[1], [2, 3]])


if __name__ == '__main__':
    unittest.main()
