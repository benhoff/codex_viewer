# Local grader verification

Verified on 2026-09-11 against the existing configured LAN endpoint and
`qwen3.8-27b-ud-q4_k_m`. The endpoint reported the model loaded with a
32,768-token context. No endpoint URL, model selection or server configuration
was changed. Live samples used the viewer's request transport and grade validators,
with 20,000 input characters, 512 output tokens and a 600-second deadline.

| Check | Observed result |
| --- | --- |
| Known arithmetic fix with patch and passing test | Valid demand grade in 17.63 seconds; correctly reported pass. 992 prompt tokens, 156 output tokens. |
| Unfamiliar model configuration | Valid null/unknown rating in 7.60 seconds. |
| Actual Settings-page excerpt: request, patch, tool results and final response | Cleaned input was 9,252 characters, versus roughly 20,953 in the earlier probe. Valid demand grade in 63.99 seconds; 3,569 prompt tokens and 308 output tokens. |
| Real excerpt's mixed configuration | Initially rejected a null rating paired with high confidence. After clarifying the prompt and schema field description, returned a valid null/unknown rating in 8.09 seconds. |
| Oversized multilingual input | Rejected locally before opening an HTTP connection. |
| Cancel during a live grading request | Request aborted at 2.00 seconds. |
| Immediate submission after cancellation | Valid grade in 12.99 seconds, without waiting or automatically retrying. |

The demand and configuration stages were tested separately against the live server.
Local integration and browser tests cover the complete submission, persistence,
progress, cancellation and retry workflow. These probes establish functionality
and one simple expected outcome; they do not measure grading accuracy across tasks.

On 2026-09-12, a real viewer run still used saved limits of 120 seconds,
100,000 input characters and 2,048 output tokens. It timed out on the first
of 93 batches. The saved limits were updated to 600 / 20,000 / 512 without
changing the endpoint or model. Retesting the first turn's first batch under
those limits completed in 92.85 seconds (5,223 prompt tokens, 199 output tokens).
It correctly returned an unknown outcome because that batch contained only an
instruction fragment. Turn 1 requires 8 batches; the original turns 1–9 selection
exceeds the 100-batch cap at the smaller input limit and needs a narrower range.
The failed-run UI now displays original limits and explains when changed settings
require a new submission instead of a retry.

A subsequent run over turns 1–5 completed four of 58 batches, then failed output
validation on batch 5 after 62.15 seconds (263 output tokens). The original rejected
JSON was not retained, so its exact failed rule cannot be recovered. Replaying that
same frozen batch reproduced an inconsistent rating: required level 2, range 1–2,
but confidence unknown. A clarified request schema explains that confidence refers
to the capability rating, separately from outcome uncertainty. The next replay
passed in 59.09 seconds with all three levels null and confidence unknown.
The batch contained only the tail of search results, without task context.

Calls now retain their exact request schema, request-contract version, finish reason
and specific validation diagnostics. Private provider values and arbitrary extra
field names are excluded from schema-error diagnostics. Accepted-grade rules and
the frozen inputs are unchanged, allowing explicit retry to retain the four valid
batch results. All 32 grader regression tests passed after this change. Future
batching improvements should carry task context into individual fragments so that
valid JSON also yields a useful assessment.

Task-context batching was subsequently implemented under prompt version v4. The
previously failing tail of event 27 was replanned with the recorded user request
and preceding tool call, within the existing 20,000-character/32K context budget.
The live call completed in 81.56 seconds (4,912 prompt tokens, 247 output tokens)
with valid JSON. Its criteria correctly identified the user's actual goal: locate
the LLM provider setting and explain web UI navigation. It kept outcome unknown
because the fragment did not contain the final answer. The five-turn example was
planned as 68 batches rather than 58 before context was added.
Regression checks cover original requests and corrections, no future-turn context,
environment-wrapper filtering, explicit Unicode truncation, lossless primary
fragments, citations into supplied context, input budgets and old-run handling.
The complete browser grading/cancellation/retry workflow passed with context in
each batch. Existing v3 grades remain visible; v4 requires a new submission.

Regression coverage includes:

- Strict JSON schema and explicit thinking disablement on outgoing requests.
- Malformed JSON, invalid fields, invalid citations and `finish_reason: "length"`.
- Combined instruction/schema/evidence/output budgeting, including Unicode.
- Duplicate evidence cleanup, preserving unique patch results, instructions,
  commands, test output, exit status and standalone completion records.
- Socket closure before response headers and during body reads, absolute timeout,
  no automatic retry, cancellation ownership/origin checks and lock release.
- Browser cancellation, immediate resubmission, batch failure and explicit retry
  that skips completed batches.

### Evidence extraction and whole-task synthesis (v5, 2026-09-12)

The run-4 UI example now plans 17 extraction batches rather than 93, with
247,803 serialized payload bytes rather than 1,662,527. Encoded images are removed
from text requests and explicitly marked unavailable; the original capture is
unchanged. The final assessment retains the complete meaningful user/assistant
conversation (7,101 bytes including event metadata), supporting notes, and cited
original evidence where space permits. Intermediate batches extract observations
and limitations rather than rating task difficulty. Bounded reductions and final
synthesis checkpoint separately and preserve original event citations.

Automated validation: 72 Python tests across the grader, task assessment and
dashboard modules, plus the full browser grading/cancellation/retry flow. New
coverage includes mixed media and nested transports, actual final synthesis,
preserved dialogue/corrections, bounded reductions, invalid summary fields/citations,
and retry after incomplete synthesis without repeating completed extractions.

Local-model validation used the saved endpoint and model, without changing their
configuration or creating production grader runs:

* A known arithmetic example returned a valid pass, level 1, in 14.53 seconds.
* A targeted real browser-measurement excerpt returned valid evidence notes in
  18.37 seconds, separating the assistant's claims from observed dimensions and
  preserving the missing-screenshot limitation.
* Final synthesis with the complete saved UI dialogue and those notes returned
  valid whole-task assessments in 55.64 and 46.70 seconds, using approximately
  2,800 prompt tokens and 444/316 output tokens. Both judged the advice as passing
  and addressed the measured mobile defect and timeline workflow.

This was a targeted evidence/synthesis validation, not a full 17-batch inference
run or a calibrated accuracy benchmark. One final response misattributed a finding
about incorporating a correction to an earlier event; numerical citation validation
cannot establish semantic support. Citation chronology instructions were tightened,
and a separate coverage panel now retains source and extraction limitations even
when the final model prose omits them. Human review remains necessary.

The first extraction schemas were rejected by the local server with a grammar
parse error. A simplified strict object schema with inlined fields and without
transport-level length/repetition bounds succeeded. The viewer still enforces all
Pydantic size, nonempty-field, type and citation constraints before accepting notes.
No fallback accepts unstructured or incomplete output, and no automatic inference
retry was added. Exact request schemas, prompts, inputs and limits are exported.

Both `/tokenize` and `/v1/tokenize` returned HTTP 404 at the configured proxy.
The existing conservative 32K context guard remains in place. The fix does not
claim exact token packing or visual evaluation; those require a verified endpoint
integration. A separate synthesis output setting defaults to 1,024 tokens while
evidence requests retain the existing 512-token setting.

Run the automated checks with the repository's dependency environment:

```bash
PYTHONWARNINGS=ignore PYTHONPATH=.deps python3 -m unittest tests.test_llm_grader tests.test_task_assessment tests.test_assessment_dashboard tests.test_route_auth_audit
npm run test:e2e -- tests/e2e/specs/llm-grader.spec.js --project=chromium
```
