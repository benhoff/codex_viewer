# Task Cost and Configuration Assessment

Status: first release implemented. Automated grading and comparison runs remain
in the later phases below.

## Purpose

Help a reviewer answer whether a Codex task used an appropriate model, reasoning
effort, workflow, and amount of generated content. Keep outcome quality separate
from resource use. A trace can support a hypothesis that a cheaper configuration
is worth testing; it cannot establish that the alternative would succeed.

“Model generation” covers both the exact model family/version and the text/code
it generates. A newer generation is neither automatically better suited nor more
expensive. Never derive a capability or cost ranking from model-name ordering.

## First release

1. Open **Assess task** from a session turn.
2. Select a contiguous range of turns in that session, including corrections and
   recovery work belonging to the request. A single turn is the default candidate
   task, not an assertion that every turn is an independent task.
3. Inspect recorded token cost, configuration history, observable activity, and
   linked source events. Review the full prompt/response in the session audit.
4. Record acceptance criteria, task demand, independent verification notes, fit
   judgments, findings, and a recommended experiment.
5. Save a personal, immutable review revision and export the assessment as JSON.

The first release uses deterministic accounting and human grading. It does not
send traces to an external model, launch reruns, or change model routing. LLM
grading and controlled comparisons are later phases described below.

## Evaluation boundary and provenance

- Identity: session ID plus inclusive start/end turn numbers, scoped to reviewer.
- Use indexed turn event boundaries, not timestamps or truncated search snippets.
  Align them to producer `task_started` events where present: the search index's
  prompt-based boundaries can attach the next turn's leading context to the
  preceding turn. Include that context with the task it starts.
- Keep original event indexes, source JSON, normalization version, rubric version,
  policy contents/hash, and an evidence digest with every saved revision.
- Include the preceding usage checkpoint and configuration context needed for
  accounting in the digest. Changing a selected event, boundary, or relevant
  checkpoint makes a saved review stale. Appending unrelated later turns does not.
- Reconstruct metrics on demand from indexed ranges; do not require a full reimport
  or an expensive startup backfill. Bound ranges to 50 turns and 20,000 events;
  fail explicitly when exceeded instead of silently truncating evidence.
- Cross-session task membership and child-agent cost aggregation are deferred.
  Label the first release as **selected session only; child work excluded**.
- Ranges may overlap. Do not sum overlapping assessments into a portfolio cost.

## Accounting contract

### Raw dimensions

Preserve nullable input, cached-input, output, and reasoning-output token counts;
observed tool calls and shell invocations; final response and commentary character
counts; and elapsed time between recorded timestamps. Character counts are not
token estimates. Tool-call counts are not model request counts. Elapsed time is
not model latency and can include tools, approvals, and user waits.

Normalize Codex cumulative `total_token_usage` into increments. Do not sum
cumulative turn rollups. Identical repeated checkpoints contribute no additional
cost. A decrease is a counter discontinuity, not negative consumption or a free
restart. Preserve unknown fields instead of inheriting the existing rollups' zero
defaults. Reject booleans, negative values, and inconsistent cache/output subsets.

A previous-turn checkpoint supplies a starting baseline. Without one, the first
checkpoint can establish an origin only when its input/output/cache values equal
the corresponding `last_token_usage` values and the task starts at turn one of an
unforked session. Otherwise the initial interval is unknown; later valid deltas
remain a partial observed subtotal. A gap across an unselected turn cannot be
charged to the selected task as though it were a known interval.

If required fields are missing or counters reset, show partial/unknown coverage
and a subtotal for measurable intervals. No missing field or unsupported provider
becomes zero. Cost describes recorded usage, not proof of a complete billing log.
An open or interrupted task may have unrecorded trailing usage.

The first release supports native Codex accounting only. Claude normalization
currently has different usage semantics; report unsupported accounting rather
than apply Codex cumulative deltas to those records.

Each token dimension has its own coverage label. For example, missing reasoning
detail can leave reasoning counts partial while total output and resource WU are
fully measurable. Count command lifecycle records by call ID alongside tool calls
so a call and its completion contribute only one observed invocation.

Reasoning is a subset of output, so total output already pays for reasoning.
`output - reasoning` is non-reasoning output, not necessarily final-answer text.
Never add reasoning tokens to output a second time.

### Abstract cost policy

For policy `work-units-v1`:

```text
U = input_tokens - cached_input_tokens
C = cached_input_tokens
O = output_tokens (including reasoning)
resource_WU = (1.0 * U + 0.1 * C + 4.0 * O) / 1000
```

These are illustrative accounting weights, not dollars or measured physical
compute. Keep precision for calculation; round only presentation. A saved policy
contains a name/version, three nonnegative finite resource weights, and an
optional mapping of exact model IDs to three model-specific weights plus their
documented basis and optional family/version/tier labels. At least one weight in
each set must be positive. Users may edit the policy for their own assessment;
this does not change install-wide settings or another reviewer's results.

Compute model-weighted WU per attributable usage interval. Unknown model IDs or
ambiguous intervals remain unpriced. Show priced-interval coverage separately
from resource coverage. Model cost mappings have no defaults beyond an empty
mapping: a model's spelling is not evidence of its price or capability.

If the model changes between usage checkpoints, the interval cannot be reliably
allocated to either model. Keep resource WU, mark model-weighted cost partial,
and preserve every configuration observation. Keep requested/recorded context
distinct from actual execution: turn context identifies recorded configuration,
not proof of the serving model. Selector identity remains unknown unless explicit.

A recorded model reroute clears model attribution until a subsequent supported
configuration observation. It must not continue pricing against the old model.

Tool/time/human-intervention dimensions remain separate from token WU. Keep cost
for failures and interruptions; do not reward a low-cost incomplete result.

## Review contract

All judgments begin unknown/unestablished and are explicitly human assessments.

| Field | Allowed values |
| --- | --- |
| Outcome | unknown, pass, partial, fail |
| Verification fit | unknown, adequate, inadequate |
| Execution fit | uncertain, insufficient, appropriate, excessive |
| Generated-content fit | uncertain, insufficient, appropriate, excessive |
| Reasoning-effort fit | unestablished, appropriate, candidate_for_reduction, candidate_for_increase |
| Model family/version fit | unestablished, appropriate, candidate_for_comparison |

“Appropriate” means reviewer judgment, not a validated least-cost configuration.
Evidence level is fixed to `human_trace_review` in this release. There is no UI or
API value that labels a trace-only recommendation as experimentally validated.

Task demand is optional 1–5 ratings for complexity, ambiguity, environment risk,
change risk, and verification need. These ordinal ratings guide review; they do
not define a numeric demand budget or an efficiency percentage.

Store acceptance criteria, verification notes, findings, recommended experiment,
and event references. Non-default judgments require findings and at least one
in-range event reference. A passing outcome additionally requires acceptance
criteria and verification notes describing the evidence used. The agent's final
claim alone does not demonstrate success. Free text remains escaped in HTML.

Rubric:

- Judge investigation against what was known when the action occurred.
- Repeated tests can be justified by a change, flakiness, or a new hypothesis.
- Search misses and nonzero exit codes are not automatic workflow failures.
- Assess completeness before concision. More text/code is not automatically waste.
- Consider requirements, risk, environment blockers, and mandatory instructions.
- Recommend instruction/tool/environment improvements when they explain waste.
- Never infer that a cheaper model succeeds from high WU or a narrow prompt alone.

## Storage and access

Add `task_assessment_revisions` in the existing SQLite schema: an increasing
revision ID, owner scope, session foreign key, turn range, created timestamp,
evidence digest, review JSON, policy JSON, and measured snapshot JSON. Index by
owner/session/range/revision. Session deletion cascades; reindexing does not delete
review history. Saved revisions are immutable. Reads show the newest revision for
that reviewer/range, a history list, and stale status against current evidence.

Reuse existing browser authentication, personal owner scopes, and project ACLs.
Every page, save, revision read, and JSON export rechecks access; inaccessible
sessions/revisions return 404. Never accept owner identity from the request body.
Search/sync tokens do not gain new write permissions. Writes require a same-origin
browser request when an Origin header is supplied. Responses are private/no-store.
No trace contents are sent to an external evaluator by this feature.

## Routes and interface

- `GET /sessions/{id}/assessment?start_turn=N&end_turn=M`: current metrics, latest
  personal review, and range selector. Optional `revision=R` opens a saved snapshot.
- `POST /sessions/{id}/assessment`: validated form save; redirects to the range.
  Require the rendered evidence digest; concurrent evidence changes return 409.
- `GET /sessions/{id}/assessment.json` with the same range/revision parameters:
  structured report including raw metrics, policy, review, evidence, and freshness.

The page shows resource and model-weighted WU with coverage labels; raw usage;
model/effort history and model catalog labels; activity and generated character
counts; review fields; linked evidence; and revision history. Explain partial
coverage adjacent to cost. Give trace evidence stable links within this page and
links back to each selected turn's full audit. The session view provides an
**Assess task** link on every turn in conversation and audit modes.

## Later phases

1. Optional rubric-based LLM grader: blind outcome grading to model/cost; require
   event-linked findings; record grader/prompt versions; treat trace content as
   untrusted evidence; calibrate against human reviews. Explicitly configure local
   or external processing and track evaluator usage separately from task usage.
2. Controlled reruns: preserve initial repository/environment, task input,
   instructions, tools, permissions, and independent acceptance checks. Change
   effort or model one at a time. Retain failed/interrupted/recovery attempts.
3. Comparable cohorts: report acceptance rate, sample size, uncertainty, latency,
   and total WU across attempts divided by accepted completions. Compare policies
   only after recalculating onto one policy. Zero accepted completions yields no
   finite cost-per-accepted-task. Use thresholds set before comparisons.
4. Multi-session tasks, deduplicated parent/child accounting, shared calibration,
   aggregate dashboards, and validated recommendations by task category.

No release should divide ordinal task demand by arbitrary cost to display an
“efficiency percentage.” Generation overhead estimates, if introduced later,
must state how sufficient output was estimated and their uncertainty.

## Acceptance criteria

1. Checkpoints 100 then 160 charge 60 for the second interval; duplicate 160 adds
   nothing. A non-first selected turn uses the preceding turn's checkpoint.
2. Missing baseline/cache/output, reset counters, forked starts, unsupported
   provider, and unpriced/mixed models produce explicit coverage limitations.
3. Reasoning output is not counted twice. Policy changes recompute costs while
   raw usage remains unchanged; invalid/NaN/infinite/negative weights are rejected.
4. Multiple context observations survive; unknown family/version/tier and selector
   identity remain unknown. Character counts never masquerade as tokens.
5. Saves create new personal revisions. Source changes mark old reviews stale;
   unrelated later turns do not. Stale submitted evidence is rejected.
6. Unauthorized users cannot read/export another project or another user's review;
   malformed ranges, judgments, policies, or out-of-range citations are rejected.
7. UI renders metrics, unknown states, review fields, event links, saved values,
   and history. JSON export is consistent with the page and saved snapshot.
8. Focused accounting/storage/route tests and existing parser/session/auth tests
   pass. Build CSS for the new template; no external evaluator is required.

## Existing integration points and sources

Implemented in [task_assessment.py](../agent_operations_viewer/task_assessment.py),
[assessment routes](../agent_operations_viewer/web/routes/assessments.py), and
[the assessment template](../agent_operations_viewer/templates/assessment.html).
Python checks live in [test_task_assessment.py](../tests/test_task_assessment.py);
the browser workflow is in
[task-assessment.spec.js](../tests/e2e/specs/task-assessment.spec.js).

Validation: 74 focused Python tests passed across assessment, parsing, session
view, and route authentication. The assessment browser test passed using installed
Chrome, including save/export, snapshot viewing, and mobile overflow checks. CSS
compiled successfully. A broader Python run reported eight existing action-queue
failures, and the existing session browser tests reported two ambiguous-selector
failures; both sets reproduced on an untouched HEAD checkout.

- [Usage rollups](../agent_operations_viewer/session_insights.py) retain cumulative
  values and zero defaults; the assessment must use source events instead.
- [Turn index](../agent_operations_viewer/turn_index.py) supplies event boundaries.
- [Session views](../agent_operations_viewer/web/routes/sessions.py) supply access
  checks and turn links; [personal state](../agent_operations_viewer/saved_turns.py)
  supplies owner scoping.
- [OpenAI reasoning documentation](https://developers.openai.com/api/docs/guides/reasoning)
  describes reasoning as output usage.
- [OpenAI trace grading](https://developers.openai.com/api/docs/guides/trace-grading)
  describes structured judgments over agent traces. The WU convention and this
  viewer's human-review workflow are project-specific designs.
