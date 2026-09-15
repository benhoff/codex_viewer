# Batch-planner hotspot audit — 2026-09-12

## Scope and method

Ran the actual v5 planner against 117 selected ranges from 54 sessions across
22 projects. The database contained 1,212 indexed sessions. Sampling included the
24 most recently updated sessions, eight with the largest event counts, a seeded
16-session sample, and recent representatives of eight common projects, with
duplicates removed. Each session contributed its first/last five-turn ranges and
the individual turn with the largest event span, where distinct.

This deliberately emphasizes potential hotspots; the distribution is **not** an
estimate of typical usage across the entire database. The main scan took 127 seconds.
Reads used a consistent SQLite snapshot in read-only mode. No model requests,
production data changes, settings changes, or batching implementation changes were made.

Settings: 20,000 input characters, 512 extraction output tokens, 1,024 synthesis
output tokens, and the existing conservative 32K context guard. Counts below are
evidence batches only; final synthesis, reductions and configuration calls are additional.

## Results

| Evidence batches | Selected ranges |
| --- | ---: |
| 1–10 | 18 |
| 11–25 | 22 |
| 26–50 | 38 |
| 51–100 | 32 |
| Rejected: more than 100 | 7 |

Among the 110 accepted plans, the median was 31.5 batches and the 90th-percentile
order statistic was 74. Of those plans, 49 exceeded the final synthesis's direct
conversation allocation and would use marked conversation excerpts. Pasted logs
count as conversation under the current implementation, so this does not always
mean the meaningful dialogue itself is too long.

No remaining long base64 image data URLs were found in the sampled cleaned
evidence. An initial keyword check also matched source code discussing image
handling; a follow-up checked actual long data-URL payloads and found zero.

## Concrete hotspots

| Case | Range | Current batches | Main driver |
| --- | --- | ---: | --- |
| HWS kernel investigation | Session `01a04e77-c545-7102-b0a2-ac996f088911`, turn 83 | >100 | One command captures about 1 MB of generated kernel documentation search-index data (event 9328). |
| HWS timing investigation | Session `01a07785-cf3e-7ab0-97af-49c987026b4a`, turns 1–5 | >100 | Help output enumerates frame choices 1–36,000 twice, consuming about 411 KB (event 444). |
| PipeWire investigation | Session `01a069ee-bdde-7fa0-a971-e81db48837a2`, turns 1–5 | >100 | A broad search pulls package-cache/source-map content into a roughly 941 KB command record (event 32). |
| Viewer latency investigation | Session `01a0946a-e4cc-7a41-9883-5abfa6ce04ae`, turns 9–13 | 85 | User messages include a roughly 116 KB process listing and 54 KB diagnostic dump. Repeated context accounts for about 57% of the batch payload. |
| Audio/logging investigation | Session `01a0915b-2da9-7701-87e6-b672b34dae20`, turns 1–5 | 87 | Source dumps plus a log-bearing request repeated as context; context accounts for about 53% of the payload. |
| Homebox debugging | Session `01a00aea-8f59-7f81-8f92-62dde0fb4bee`, turns 48–52 | >100 | Large Docker logs (~547 KB serialized), lint output (~454 KB), and search output (~239 KB). |

There are also legitimately large workflows: the sampled speech-segmentation
range required 98 batches, and an older viewer investigation required 93 batches
for a single busy turn. Selecting one turn therefore does not guarantee a small plan.

## Diagnostic consolidation experiments

These are in-memory comparisons, not changes to the production planner:

* Replacing the generated search-index bulk lines with explicit omission markers,
  while retaining short lines and the command, reduced the HWS turn from **>100
  to 64** batches. The event shrank from 1,048,841 to 2,837 UTF-8 bytes. This shows
  the opportunity, not that blind long-line removal is an acceptable policy.
* Expressing the two exhaustive `--frames` lists as their exact contiguous integer
  range reduced the help record from **411,078 to 1,360 bytes**, and its task from
  **>100 to 96 batches**. The range check confirmed all integers 1 through 36,000.
* Replacing the package-cache bulk lines reduced that record from **941,276 to
  50,027 bytes**, but its task still exceeded 100 batches: it has multiple hotspots.
* Packing adjacent turns with one shared request overview, without per-batch
  request/action context, reduced the latency example from **85 to 57 batches**
  and the audio/logging example from **87 to 63**. This combines reduced context
  overhead and different packing; it is not an isolated test of turn boundaries.
  A production change must retain action/result relationships and corrections.
* Exact whole-text repeat removal changed only two sampled range counts, each by
  one batch. Older textual tool headers and patch mirrors still exist, but simple
  hash-based deduplication is not the main opportunity demonstrated by this scan.

## Recommended next changes

These deterministic improvements are now implemented in v6. See the
[consolidation validation](grader-consolidation-validation.md) for preservation
rules, same-snapshot comparisons, tests and remaining hotspots.

1. **Separate requests from attached diagnostics.** Keep the user's actual goal,
   corrections and final responses in shared context. Carry pasted logs/process
   dumps once as cited supporting evidence rather than repeating them as requests.
2. **Recognize generated artifacts and compact verbose structured output.** Handle
   documentation indexes, source maps, package caches and exhaustive option lists
   explicitly. Preserve provenance, the original record, relevant matches and any
   failures. Avoid a generic truncation rule for long code, patches or test output.
3. **Group repetitive diagnostics by meaning.** For logs and linter output, preserve
   distinct errors, counts, affected files, time spans, representative samples and
   exit status. Keep new failures and recovery evidence visible.
4. **Reduce repeated context and pack related evidence across turn boundaries.**
   Preserve the full task dialogue for synthesis and pair actions with results.
5. **Add a preflight explanation.** Show the largest contributing records, context
   overhead and expected conversation excerpts before the user submits a large run.

Exact server token counting remains a separate opportunity for using more of the
32K window. This audit used the current conservative guard and does not establish
grading accuracy or predict inference duration.

Reproduce with:

```bash
PYTHONPATH=.deps:. python3 scripts/audit-grader-batches.py --output /tmp/aov-batch-hotspots.json
```

The script emits metadata and size diagnostics, without conversation excerpts.
Archived range-level results and the targeted experiments are in
[the JSON report](performance/grader-batch-hotspots-2026-09-12.json).
