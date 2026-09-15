# Assessment workflow review — 2026-09-12

Implementation update: following the user's preference for AI grading over manual
assessment, `/assessments` now lists submitted task ranges and run status. The
assessment page leads with outcome/progress and actions, has readable source panels
and saved-run pages, and collapses manual review, accounting and batch details.
The manual citation-entry redesign below was intentionally deferred. Original
findings and baseline measurements follow for comparison.

Validation of the implemented flow: 77 Python tests and five Chromium workflows
passed, covering owner/project access, latest attempts per range, status filters,
pagination, frozen results after evidence changes/removal, optional manual reviews,
submission, cancellation, immediate resubmission and explicit retry. A 60-batch
failure fixture verifies that recovery actions precede collapsed diagnostics and
remain within 1,400 desktop / 1,800 mobile viewport pixels even before scrolling.
Source-panel checks include readable text, raw-record disclosure, Escape dismissal,
keyboard focus returning to the citation, and direct links into saved evidence.
No real LLM calls or production grading/settings changes were used for these checks.

The assessment flow makes the reviewer move between a session inventory, numeric
turn selection, grader internals, accounting, a manual form and raw evidence.
The information is present, but its order does not support the main decisions:
what work am I reviewing, did it succeed, what evidence supports that, and what
should I do next?

## Inspection method

Reviewed current templates, routes and dashboard queries. Walked through the current
application in Chromium at 1440×1000 and 390×844 using an isolated fixture database:
a five-turn session, a sixty-turn session, and a simulated failed run with 58
batches. Checked evidence-link behavior. Read production run metadata separately;
the live browser required authentication, so browser measurements below are from
fixtures, not a logged-in production session. No production grades or settings
were changed and no LLM requests were made.

Screenshots and measurements are in `/tmp/aov-assessment-ux/`; reproduction script:
`/tmp/aov-assessment-ux.cjs`. This review did not change application code.

## Findings, ordered by impact

### 1. Chunk-level grading work disappears from the main overview

`assessment_dashboard.py` calls `read_run` for exactly turns 1 through the session's
current turn count. The dashboard presents only completed whole-session estimates;
it has no list/filter for chunk runs that are running, failed, cancelled or awaiting
human review. Human review counts are a separate concept. A user who follows the
recommendation to grade smaller chunks cannot reliably return through this page.
Run history on the assessment page offers JSON exports rather than readable prior
results.

Change: make an assessment list show task ranges, project/session, AI status,
completed/total batches, outcome, human-review status and a direct Open/Resume
action. Keep browsing unassessed sessions available as a separate view. Show prior
attempts as readable pages with export as a secondary action. Preserve owner scope
and project ACL checks for every status, filter, history view and count.

### 2. Batch details push recovery controls out of reach

`_assessment_grader.html` renders all batch rows and per-call usage before the form
containing Submit, Retry, Cancel and live progress. In the 58-batch failure fixture,
Submit was at document y=4,443 on desktop and y=4,809 on mobile. The same container
holds retry/cancel when available. A completed run also retains this long list.

Change: put status, progress and the relevant action at the top. Lead a failed run
with the failure, retained progress and a clear resume action when retry is valid;
otherwise explain why a new run is required. Collapse batch details and usage under
one diagnostic section; let the user reveal the failed batch directly. Keep the
current explicit retry and cancellation behavior.

### 3. Evidence is distant and cumbersome to inspect

`assessment.html` places evidence after accounting, configuration, the review form
and revision history. In the failed fixture, evidence began at y=6,920 on desktop
and y=8,292 on mobile. Clicking a citation changed the fragment to `#event-2` but
left its `<details>` closed. Opening it exposes `record_json`, not a readable
request, response, patch or test result. Manual citation entry requires typing
comma-separated event indexes.

Change: show readable conversation and evidence next to findings on desktop and in
a directly accessible panel on mobile. Citation links should open and focus the
referenced source, preserve context, and offer a return to the finding. Add an
“Add to review” action instead of requiring index transcription. Retain raw records
under an advanced disclosure and in exports.

### 4. The result hierarchy emphasizes capability before task success

`_assessment_grader.html` puts the capability chart before the estimated outcome.
Findings, acceptance criteria and the suggested experiment are collapsed together.
Important coverage limits are in another disclosure. This makes it difficult to
judge whether the result is useful before interpreting its capability estimate.

Change: lead with task outcome, a short explanation, the strongest cited findings
and material coverage gaps. Place capability/cost analysis below that conclusion.
Keep uncertainty and stale-result indicators visible; never turn an incomplete
run into an apparent whole-task grade.

### 5. Selection and review require unnecessary bookkeeping

The user chooses numbered turns, then edits another numeric range on the assessment
page. “Choose task range” for a large session opens a one-turn assessment, not a
dedicated selector. The human form presents outcome, content/model/effort fit,
multiple text fields, event indexes and an optional cost policy. The task goal is
entered separately for AI grading and human review.

Change: use a compact turn list with request/response previews, clear selected
boundaries and a grading-size preview. Default the human workflow to outcome,
review notes and selected evidence. Put capability ratings and policy editing in
advanced sections. Explicitly copying AI suggestions into an editable human draft
may help; never silently approve or save the model's judgments.

### 6. The dashboard delays reaching actionable work

At desktop size, the first session title was near y=993; on mobile, y=1,709.
Filters, three metric cards, explanatory text and an expanded machine-coverage
table come first. The two-session mobile fixture was already 2,860 pixels tall.
There was no horizontal overflow, so the central problem is hierarchy and page
length rather than a broken responsive width.

Change: put actionable assessments first, collapse machine/accounting summaries,
and use compact rows with task previews and plain status labels.

## Suggested implementation sequence

1. Restore a short path through a single assessment: top-level result/progress and
   actions, collapsed batch/accounting details, readable source panels, working
   citation navigation and click-to-add evidence.
2. Add the range-based assessment list and readable run history so completed and
   interrupted chunk work is easy to find again.
3. Improve task-range preview and simplify the human review form while retaining
   advanced capability/accounting controls.

Target workflow: **Choose work → Review outcome and sources → Save your judgment**.
Starting AI grading is optional; batch execution is represented by progress, with
diagnostic detail available when needed.

Acceptance checks should include a 60–100-batch failed run, an old run that cannot
be resumed, a completed grade with coverage gaps, citation navigation and return,
manual evidence selection, finding a chunk run from the dashboard, keyboard/mobile
use, and unchanged authorization/cancellation/validation behavior.
