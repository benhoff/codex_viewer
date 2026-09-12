"""Read-only sample of real grading plans; never invokes a provider.

Run: PYTHONPATH=.deps:. python3 scripts/audit-grader-batches.py --output /tmp/batch-audit.json
"""
import argparse
from collections import Counter
from datetime import datetime, UTC
import json
from pathlib import Path
import random
import re
import sqlite3
import time

from agent_operations_viewer import llm_grader as g
from agent_operations_viewer.task_assessment import task_source, report_for_source, DEFAULT_POLICY


def plan(report, config):
    try:
        inputs = g.grader_input(report, '', config)
        batches = inputs.get('demand_batches', [])
        return {'batches': len(batches) or 1,
                'payload_bytes': sum(g.json_size(b) for b in batches) if batches else g.json_size(inputs['demand']),
                'context_bytes': sum(g.json_size({k: b[k] for k in ('task_context', 'task_overview') if k in b}) for b in batches),
                'dialogue_excerpted_at_synthesis': bool(batches) and g.json_size(inputs['synthesis']['events']) > min(
                    g.evidence_budget({**config, 'max_output_tokens': config['synthesis_output_tokens']}, g.SYNTHESIS_PROMPT, g.DemandGrade),
                    g.evidence_budget(config, g.EXTRACTION_PROMPT, g.EvidenceNotes)) // 2}
    except ValueError as exc:
        return {'error': str(exc)}


def diagnose(report, config):
    cleaned = g.clean_events(report['evidence'])
    details = []
    counts = Counter(e['text'] for e in cleaned if e.get('kind') in {'command', 'tool_result'} and len(e['text']) > 200)
    duplicate_bytes = sum((n - 1) * len(t.encode()) for t, n in counts.items() if n > 1)
    for e in sorted(cleaned, key=g.json_size, reverse=True)[:6]:
        t = e['text']
        details.append({'event_index': e['event_index'], 'kind': e.get('kind'), 'role': e.get('role'),
                        'bytes': g.json_size(e), 'same_output_count': counts.get(t, 0),
                        'transport_markers': [s for s in ('aggregated_output', 'formatted_output', 'wall_time_seconds', 'original_token_count', 'input_text') if s in t],
                        'media_omitted': 'Media omitted' in t})
    size_by_kind = Counter()
    for e in cleaned:
        size_by_kind[e.get('role') if e.get('role') in {'system', 'developer'} else e['kind']] += g.json_size(e)
    # Diagnostic alternatives only: same planner, modified in-memory evidence.
    unique, seen = [], {}
    for e in cleaned:
        t = e['text']
        if e.get('kind') in {'command', 'tool_result'} and len(t) > 200:
            if t in seen:
                e = {**e, 'text': f"[Identical output already recorded at event {seen[t]}; this repeat occurred here.]"}
            else:
                seen[t] = e['event_index']
        unique.append(e)
    converted = [{**e, 'display_text': e['text'], 'detail_text': '', 'command_text': e.get('command')} for e in unique]
    dedup = plan({**report, 'evidence': converted}, config)
    normalized_control = plan({**report, 'evidence': [{**e, 'display_text': e['text'], 'detail_text': '',
                                                      'command_text': e.get('command')} for e in cleaned]}, config)
    # Lower-context-overhead comparison: shared requests, pack adjacent turns.
    overview = g.bounded_events([e for e in cleaned if e.get('kind') == 'message' and e.get('role') == 'user'], config['max_input_chars'] // 3)
    shared = {'events': cleaned, 'acceptance_criteria': '', 'task_overview': overview,
              'limitations': 'Text only; images unavailable. Diagnostic shared-context packing.'}
    try:
        packed = len(g.batch_evidence(shared, report['turns'], min(g.evidence_budget(config, g.DEMAND_PROMPT, g.DemandGrade),
                                                                       g.evidence_budget(config, g.EXTRACTION_PROMPT, g.EvidenceNotes))))
    except ValueError as exc:
        packed = str(exc)
    return {'clean_events': len(cleaned), 'clean_bytes': sum(g.json_size(e) for e in cleaned),
            'bytes_by_kind': dict(size_by_kind), 'exact_repeat_bytes': duplicate_bytes,
            'remaining_image_data_urls': sum(bool(re.search(r'data:image/[A-Za-z0-9.+-]+;base64,[A-Za-z0-9+/=]{256,}', g.canonical_json(e))) for e in cleaned),
            'top_events': details, 'dedup_diagnostic': dedup, 'normalization_control': normalized_control,
            'shared_context_diagnostic_batches': packed}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', default='data/codex_sessions.sqlite3')
    parser.add_argument('--output', default='/tmp/aov-batch-hotspots.json')
    args = parser.parse_args()
    started = time.monotonic()
    connection = sqlite3.connect(Path(args.database).resolve().as_uri() + '?mode=ro', uri=True)
    connection.row_factory = sqlite3.Row
    with connection:
        connection.execute('PRAGMA query_only=ON')
        connection.execute('BEGIN')
        config = g.load_config(connection)
        sessions = [dict(r) for r in connection.execute('select id,turn_count,event_count,summary,inferred_project_label,updated_at,model_provider,source,forked_from_id,cli_version,cwd,source_host,git_commit_hash,git_repository_url from sessions where turn_count > 0 order by updated_at desc,id')]
        chosen = {s['id']: s for s in sessions[:24]}
        for s in sorted(sessions, key=lambda s: s['event_count'], reverse=True)[:8] + random.Random(42).sample(sessions, min(16, len(sessions))):
            chosen[s['id']] = s
        projects = Counter(s['inferred_project_label'] for s in sessions)
        for project, _ in projects.most_common(8):
            s = next(s for s in sessions if s['inferred_project_label'] == project)
            chosen[s['id']] = s
        results = []
        for n, s in enumerate(chosen.values(), 1):
            turns = [dict(r) for r in connection.execute('select turn_number,start_event_index,end_event_index from session_turns where session_id=? order by turn_number', (s['id'],))]
            if not turns:
                continue
            last = turns[-1]['turn_number']
            heavy = max(turns, key=lambda t: t['end_event_index'] - t['start_event_index'])['turn_number']
            windows = list(dict.fromkeys([(1, min(5, last)), (max(1, last - 4), last), (heavy, heavy)]))
            for a, b in windows:
                row = {'session_id': s['id'], 'project': s['inferred_project_label'], 'start': a, 'end': b}
                try:
                    source = task_source(connection, s, a, b)
                    report = report_for_source(source, DEFAULT_POLICY)
                    row.update(plan(report, config))
                    row.update(diagnose(report, config))
                except (ValueError, LookupError) as exc:
                    row['error'] = str(exc)
                results.append(row)
            if n % 10 == 0:
                print(f'Planned {n}/{len(chosen)} sampled sessions ({len(results)} ranges)', flush=True)
    connection.close()
    document = {'at': datetime.now(UTC).isoformat(), 'seconds': round(time.monotonic() - started, 2),
                'prompt_version': g.PROMPT_VERSION, 'settings': {k:config[k] for k in ('max_input_chars', 'max_output_tokens', 'synthesis_output_tokens')},
                'indexed_sessions': len(sessions), 'sampled_sessions': len(chosen), 'ranges': results}
    Path(args.output).write_text(json.dumps(document, indent=2))
    print(json.dumps({'output': args.output, 'seconds': document['seconds'], 'sessions':len(chosen), 'ranges':len(results),
                      'rejected':sum('error' in r for r in results),
                      'top': [{k:r.get(k) for k in ('session_id','project','start','end','batches','error','clean_bytes','exact_repeat_bytes','shared_context_diagnostic_batches')} for r in sorted(results, key=lambda r:r.get('batches',101), reverse=True)[:12]]}), flush=True)


if __name__ == '__main__':
    main()
