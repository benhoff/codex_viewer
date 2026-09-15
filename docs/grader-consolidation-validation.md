# Deterministic grading consolidation — 2026-09-12

Prompt version `capability-grader-v6-deterministic-consolidation` prepares smaller
grading views without another LLM call. Originals remain in frozen assessment
snapshots and exports; exact model inputs and consolidation metadata are auditable.

## Rules and boundaries

- Exact repeated tool outputs become references with samples and hashes. Every
  occurrence, event index, command and exit status remains.
- Consecutive identical log messages retain counts and first/last timestamps.
  Severity, process/service identity and measured values are not normalized.
  Distinct failures and recovery messages remain in order.
- Consecutive identical lint diagnostics retain every location, rule and severity.
- Exhaustive integer option lists become inclusive ranges only after checking
  every value. Nonconsecutive or specially formatted values remain unchanged.
- Recognized generated documentation indexes and source maps become explicit
  omission markers with source paths, line numbers, sizes and hashes. Unknown
  package-cache content stays intact. Requests or criteria naming these artifacts
  disable this rule, since their contents may be the subject of review.
- Recognizable pasted diagnostics become attachments in repeated task context;
  complete user messages remain primary evidence. Prose before, between and after
  diagnostic runs is retained.
- Adjacent turns share batches with one bounded request context instead of two
  overlapping copies. Small actions/results stay together when that does not add
  calls; split results retain preceding-action context. Requests after a batch's
  last turn are excluded from its context.

Patches, user instructions and assistant deliverables are excluded from tool-output
consolidation. Unfamiliar formats are preserved. Context limits, schemas, output
allowances, endpoint, model, cancellation and retry policy are unchanged.
The preflight UI reports savings and links to the largest remaining records.

## Comparison on real captures

The audit reuses 117 ranges across 54 sessions. Both planners read the same SQLite
snapshot with the saved 20,000-character / 512-output-token settings and 1,024
synthesis output tokens. Baseline: local Git revision
`2699dd5e42a4f9ad8b58c740851d3678dcb8f45a` (v5).

| Measure | Before | After |
| --- | ---: | ---: |
| Ranges exceeding 100 evidence batches | 8 | 2 |
| Batches across the 109 ranges accepted by both planners | 4,111 | 3,624 |
| Evidence payload bytes across those same ranges | 65,125,693 | 56,630,915 |
| Accepted ranges with increased batch counts | — | 0 |

That is 11.8% fewer extraction batches and 13.0% less extraction payload on the
comparable ranges. Ranges overlap, so totals are comparison metrics, not a proposed
workload. Six formerly rejected ranges now fit. Active captures grew after the
original audit, which had seven rejections; the eight above come from rerunning
the old planner on the same snapshot as the new one.

| Range | Before | After |
| --- | ---: | ---: |
| HWS documentation search, turn 83 | >100 | 57 |
| HWS timing/options, turns 1–5 | >100 | 89 |
| PipeWire/package-cache search, turns 1–5 | >100 | 89 |
| PipeWire/package-cache search, turn 2 | 80 | 37 |
| Viewer latency diagnostics, turns 9–13 | >100 | 60 |
| Audio/logging, turns 1–5 | 87 | 53 |
| Audio/logging, turns 5–9 | 68 | 39 |

Background-recorder turns 6–10 and Homebox turns 48–52 still exceed the cap. Their
remaining source/log evidence does not meet the conservative rules and is not
silently dropped. Large ranges can still require many calls; long dialogue can
still be excerpted for synthesis. Exact server token counting remains separate.

## Validation

85 Python regression tests passed, covering original preservation, fragment
reconstruction, diagnostic/prose separation, log severity and recovery order,
artifact opt-outs, output validation, cancellation and assessment permissions.
The Chromium grader workflow passed with a local mock provider: configuration,
preflight, export metadata, adjacent-turn batching, failure, cancellation,
immediate resubmission and explicit retry without repeating completed batches.
No live LLM requests were made; this does not establish grading accuracy or
inference-time improvements.

```bash
PYTHONPATH=.deps:. python3 scripts/audit-grader-batches.py \
  --ranges-from docs/performance/grader-batch-hotspots-2026-09-12.json \
  --compare-revision 2699dd5e42a4f9ad8b58c740851d3678dcb8f45a \
  --plans-only --output /tmp/aov-consolidation-comparison.json
```

[Range-level results](performance/grader-consolidation-2026-09-12.json) contain
metadata only, without conversation excerpts. Existing runs remain reviewable;
submit a new run after the viewer loads v6 to use the new preparation and boundaries.
