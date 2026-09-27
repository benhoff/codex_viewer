# Agent Operations Viewer

FastAPI viewer and sync tooling for Codex sessions and related agent operations.

The repo and Python package are now named `agent_operations_viewer`, but several runtime identifiers still keep the legacy `CODEX_VIEWER_*` / `.codex` naming for compatibility. That is intentional.

It is optimized for the shortest path to useful output:

- local import is the default
- `~/.codex/sessions` is the default source
- no `.env` file is required for first run
- no token or agent daemon is required for first run

Project browsing, session lists, turn timelines, and web search hide Codex approval
review sessions by default. **Show approval reviews** restores them and remembers
the choice in this browser. Detection uses the exact session metadata markers
`thread_source: "guardian_review"` or `source.subagent.other: "guardian"`, not prompt
text. Existing imports are classified on the next server startup; new imports and
metadata updates are classified automatically. Raw history, direct session links,
saved assessments, exports, and the search API remain available and unchanged.

Design notes:

- [Task cost and configuration assessment](docs/task-assessment-spec.md)
- [Action queue scoring](docs/action-queue-scoring.md)
- [Agent daemon on macOS and Windows](docs/agent-daemon-windows-macos.md)
- [Search API guide](docs/search-api.md)
- [Search API implementation roadmap](docs/search-api-roadmap.md)
- [Self-hosted launch priorities](docs/selfhosted-launch-priorities.md)

## Fastest Path To Value

If you want to see your own sessions in the UI as quickly as possible, start here.

```bash
./scripts/bootstrap-local.sh
./scripts/start-server.sh
```

Then open `http://127.0.0.1:8000`.

`bootstrap-local.sh` installs Python dependencies into `.deps` and only invokes `npm` if the built CSS asset is missing.

What this does by default:

- runs in `local` sync mode
- imports from `~/.codex/sessions`
- stores SQLite data in `./data`
- starts the web UI on `127.0.0.1:8000`
- enables password auth by default
- routes the first browser visit through `/setup` so you can create the initial admin

If the dashboard is empty:

1. Make sure Codex has written at least one session on this machine.
2. If your sessions live somewhere else, set `CODEX_SESSION_ROOTS`.
3. Run a manual import:

```bash
PYTHONPATH=.deps python3 -m agent_operations_viewer sync
```

Do not start by copying `.env.server.example` unless you are intentionally setting up a central server for other machines.

## Remote Server Mode

Use `remote` mode only when you want one server to collect uploads from other machines.

Server setup:

```bash
cp .env.server.example .env
docker compose up --build -d
```

`compose.yml` starts in `remote` sync mode, serves the UI on port `8000`, and defaults browser auth to `password`.
If you want to hand auth off to a trusted proxy instead, set `CODEX_VIEWER_AUTH_MODE=proxy`.
Concrete reverse-proxy examples live in [deploy/proxy](deploy/proxy).

Then open the viewer, finish `/setup`, create a sync token, and connect an agent host.

Agent host setup:

Token-based daemon host:

```bash
cp .env.agent.example .env
./scripts/bootstrap-local.sh --skip-css
PYTHONPATH=.deps python3 -m agent_daemon install
PYTHONPATH=.deps python3 -m agent_daemon start
```

Set these values in the agent `.env` file:

- `CODEX_VIEWER_SERVER_URL`
- `CODEX_VIEWER_SYNC_API_TOKEN`

Browser-paired machine flow:

```bash
./scripts/bootstrap-local.sh --skip-css
PYTHONPATH=.deps python3 -m agent_daemon setup --server http://viewer.example.com:8000
```

That flow pairs the machine through the browser, installs the per-user daemon service, and starts it.

For foreground/manual daemon runs, `./scripts/start-agent-daemon.sh` still starts the sync loop directly.
The daemon wrapper forces `CODEX_VIEWER_SYNC_MODE=remote`.
For native macOS and Windows launch examples, including `launchd` and Task Scheduler, see [docs/agent-daemon-windows-macos.md](docs/agent-daemon-windows-macos.md).

## Agent Daemon Layout

The daemon implementation now lives in [agent_daemon](agent_daemon):

- [agent_daemon/runtime.py](agent_daemon/runtime.py)
- [agent_daemon/remote_sync.py](agent_daemon/remote_sync.py)
- [agent_daemon/file_watch.py](agent_daemon/file_watch.py)
- [agent_daemon/service_manager.py](agent_daemon/service_manager.py)
- [agent_daemon/start.sh](agent_daemon/start.sh)
- [agent_daemon/start.ps1](agent_daemon/start.ps1)

The existing wrapper scripts under [scripts](scripts) remain the stable entrypoints and delegate into the root-level daemon directory:

- [scripts/start-agent-daemon.sh](scripts/start-agent-daemon.sh)
- [scripts/start-agent-daemon.ps1](scripts/start-agent-daemon.ps1)

Daemon-specific third-party Python dependencies are listed in [agent_daemon/requirements.txt](agent_daemon/requirements.txt).
Today the only required external package is `cryptography` for machine-credential signing.
`watchdog` is optional; if it is not installed, the daemon falls back to polling for `.jsonl` session changes.

## Commands

Start the web app:

```bash
./scripts/start-server.sh
```

Override the bind target without editing `.env`:

```bash
./scripts/start-server.sh --host 127.0.0.1 --port 8001
```

Run a one-shot local import:

```bash
PYTHONPATH=.deps python3 -m agent_operations_viewer sync
```

Force a full rebuild:

```bash
PYTHONPATH=.deps python3 -m agent_operations_viewer sync --rebuild
```

Run the remote sync daemon in the foreground:

```bash
./scripts/start-agent-daemon.sh
```

Install the background daemon service for the current user:

```bash
PYTHONPATH=.deps python3 -m agent_daemon install
PYTHONPATH=.deps python3 -m agent_daemon start
PYTHONPATH=.deps python3 -m agent_daemon status
```

Pair this machine through the browser, install the service, and start it:

```bash
PYTHONPATH=.deps python3 -m agent_daemon setup --server http://viewer.example.com:8000
```

Diagnose agent-host issues:

```bash
PYTHONPATH=.deps python3 -m agent_daemon doctor
PYTHONPATH=.deps python3 -m agent_daemon logs --lines 200
PYTHONPATH=.deps python3 -m agent_daemon sync --once
```

Run the remote sync daemon from Windows PowerShell:

```powershell
.\scripts\bootstrap-local.ps1 -SkipCss
.\scripts\start-agent-daemon.ps1
```

Export one session:

```bash
PYTHONPATH=.deps python3 -m agent_operations_viewer export SESSION_ID --format markdown
```

Create a whole-instance backup archive:

```bash
PYTHONPATH=.deps python3 -m agent_operations_viewer backup create --output ./agent-operations-viewer-backup.zip
```

Verify a backup archive:

```bash
PYTHONPATH=.deps python3 -m agent_operations_viewer backup verify ./agent-operations-viewer-backup.zip
```

Restore a backup archive into a fresh data directory:

```bash
PYTHONPATH=.deps python3 -m agent_operations_viewer backup restore ./agent-operations-viewer-backup.zip --data-dir ./restore-data
```

## Configuration

You usually only need to care about these variables:

- `CODEX_VIEWER_SYNC_MODE`: `local` by default, `remote` for central-server deployments
- `CODEX_SESSION_ROOTS`: comma-separated local import roots, default `~/.codex/sessions`
- `CODEX_VIEWER_SERVER_URL`: required for remote agents uploading to a server
- `CODEX_VIEWER_SYNC_API_TOKEN`: required for remote agents
- `CODEX_VIEWER_REMOTE_TIMEOUT`: remote request timeout in seconds, default `120`
- `CODEX_VIEWER_REMOTE_BATCH_SIZE`: remote daemon upload batch size, default `1`
- `CODEX_VIEWER_REMOTE_UPLOAD_WORKERS`: concurrent upload requests, default `1`
- `CODEX_VIEWER_AUTH_MODE`: `none`, `password`, `proxy`, or `password_or_proxy`

Env files are loaded in this order:

- `.env`
- `.env.<CODEX_VIEWER_ENV>`
- `.env.local`
- `.env.<CODEX_VIEWER_ENV>.local`

Example env files:

- `.env.server.example`: central server using remote uploads
- `.env.agent.example`: remote daemon host
- `.env.development.example`: repo development with reload enabled

## Auth

`password` is the default auth mode.
Set `CODEX_VIEWER_AUTH_MODE=none` only for a trusted single-user localhost install, or switch to `proxy` / `password_or_proxy` when you have a trusted auth proxy in front of the app.

Built-in password auth:

```env
CODEX_VIEWER_AUTH_MODE=password
```

Reverse-proxy header auth:

```env
CODEX_VIEWER_AUTH_MODE=proxy
CODEX_VIEWER_AUTH_PROXY_USER_HEADER=X-Forwarded-User
CODEX_VIEWER_AUTH_PROXY_NAME_HEADER=X-Forwarded-Name
CODEX_VIEWER_AUTH_PROXY_EMAIL_HEADER=X-Forwarded-Email
```

Concrete proxy examples:

- [Caddy + Authentik](deploy/proxy/Caddyfile.authentik.example)
- [Traefik + Authelia middleware](deploy/proxy/traefik.authelia.dynamic.yml.example)
- [Traefik viewer labels](deploy/proxy/traefik.authelia.viewer-compose.example.yml)
- [Reverse-proxy setup notes](deploy/proxy/README.md)

If auth is enabled and no admin exists yet, the first visit will route through `/setup` so the initial admin can be created or claimed.

## Search API

Signed-in users can create personal, read-scoped search tokens from **Settings → Search API**. These tokens inherit the user's project ACLs and cannot upload sessions or perform administrative actions. Daemon sync tokens are intentionally not accepted by the search API.

See the [Search API guide](docs/search-api.md) for the hosted endpoint, token setup, complete request and response reference, pagination examples, and troubleshooting.

Search imported turns:

```bash
curl --get http://127.0.0.1:8000/api/v1/search \
  --header "Accept: application/json" \
  --header "Authorization: Bearer csvr_read_REPLACE_ME" \
  --data-urlencode "q=authentication failure" \
  --data-urlencode "limit=20"
```

Optional filters are `project_id`, canonical `repository_id`, normalized Git `remote`, working-directory `root`, `host`, `from`, and `to`. `GET /api/v1/projects` discovers ACL-visible project IDs, canonical repository IDs, aliases, sources, time ranges, and session counts. Deterministic lexical options include `mode=all|any|phrase|exact`, field filters for prompts, responses, activity, commands, paths, commit IDs, and tool output, plus pre-pagination facets. Related bounded queries can be sent to `POST /api/v1/search/batch`. Timestamps use ISO 8601. When more results exist, pass the returned `next_cursor` value as the `cursor` query parameter. Search responses contain plain-text snippets and relative links to the matching turn.

Every hit also includes repository provenance stored with the session and a `links.turn` URL for retrieving the complete normalized prompt, response, commands, patches, and optional activity context through the API.

Natural project-history questions are also accepted, for example `q=what was the last thing we were going to do on the hws project` or `q=what issues remain on the hws project`. The response's `retrieval` object reports the detected intent, resolved project, and whether results came from strict matching, relaxed matching, or the recent project-history fallback. This stage returns evidence candidates; it does not yet reconcile them into a synthesized answer.

Searchable prompt, response, and activity text is also stored in deterministic overlapping chunks. Chunk indexing uses the complete normalized event text rather than the legacy per-turn 8k/12k/24k search fields, and stale sessions are reindexed in committed versioned background batches after server startup. Hits found only in full-content chunks report `match_source: "chunk"` plus chunk field and offset metadata. The dashboard search also consults this full-content index, and a successful chunk build retires the obsolete `Search text truncated during import` warning. No external embedding provider is required; current ranking combines the compact turn FTS index with the full-content chunk FTS index.

Remote sync authentication, decompression, parsing, and database work run in a bounded history worker pool so ingestion cannot monopolize the async HTTP event loop. Append-only Codex session tails preserve historical event rows and rebuild only the open/new turn suffix, including search, action-queue, activity, and environment indexes. Full raw uploads remain the compatibility and recovery path; Claude transcript tails currently use that full-import fallback.

When browser authentication is disabled for a trusted local install, the endpoint follows the rest of the app and does not require a token.

## Docker

`docker compose up --build -d` is intentionally configured for remote-server mode.

Defaults in [compose.yml](compose.yml):

- `CODEX_VIEWER_SYNC_MODE=remote`
- SQLite persisted in the `viewer-data` Docker volume
- viewer served on port `8000`
- browser auth defaults to `password`
- Docker health is exposed through an explicit Compose `healthcheck:` hitting `/api/health`

The Docker build now compiles Tailwind inside the image, so the host does not need Node for the container path.

If you want host-visible SQLite files instead of a named volume, replace:

```yaml
volumes:
  - viewer-data:/app/data
```

with:

```yaml
volumes:
  - ./data:/app/data
```

If you want local import inside Docker instead of remote uploads, override the sync mode and mount your session directory:

```yaml
services:
  viewer:
    environment:
      CODEX_VIEWER_SYNC_MODE: local
      CODEX_SESSION_ROOTS: /sessions
    volumes:
      - viewer-data:/app/data
      - /home/you/.codex/sessions:/sessions:ro
```

## Systemd

Systemd examples live in [deploy/systemd](deploy/systemd).

Wrapper scripts:

- [scripts/start-server.sh](scripts/start-server.sh)
- [scripts/start-agent-daemon.sh](scripts/start-agent-daemon.sh)
- [scripts/bootstrap-local.sh](scripts/bootstrap-local.sh)
- [scripts/start-agent-daemon.ps1](scripts/start-agent-daemon.ps1)
- [scripts/bootstrap-local.ps1](scripts/bootstrap-local.ps1)

Daemon source:

- [agent_daemon/start.sh](agent_daemon/start.sh)
- [agent_daemon/start.ps1](agent_daemon/start.ps1)
- [agent_daemon/runtime.py](agent_daemon/runtime.py)
- [agent_daemon/remote_sync.py](agent_daemon/remote_sync.py)
- [agent_daemon/file_watch.py](agent_daemon/file_watch.py)
- [agent_daemon/service_manager.py](agent_daemon/service_manager.py)
- [agent_daemon/requirements.txt](agent_daemon/requirements.txt)

## Task Assessment

Select **Assessments** in the navigation for a dashboard across all machines
syncing to this viewer. No machine-agent update, feature flag, or LLM is required.
Filter sessions by machine, project, activity window, and whether you have saved a
review. Machine counts cover all matching sessions; source-backed metrics are
calculated for the current page of ten sessions. **Assess full session** includes
all indexed turns, so corrections and recovery are visible before you select a
task range. Sessions exceeding 50 turns or 20,000 events require a smaller range.
Claude sessions appear with unsupported usage accounting.

The dashboard shows your latest review and its exact turn range, flags reviews
that need rechecking, and exports the displayed page as JSON. All dashboard costs
use the default resource policy for comparison; personal policies remain in the
individual assessments. Costs are not summed across sessions or overlapping
reviews. New uploads appear on the next page load. Project access and personal
review ownership apply to filters, counts, metrics, and exports.

Open a session and use **Grade a chunk**: select turns, enter a first/last range,
or choose **Use this page**, then click **Review chunk & grade**. All turns between
the selected endpoints are included, even across pages. **Grade turn** opens a
single turn. Include corrections and recovery belonging to the same request. The assessment
shows recorded token Work Units, optional model-weighted cost, model/effort
history, and observable tool and generated-content volume.

Save personal review revisions for outcome, verification, execution, model,
reasoning effort, and generated-content fit. Judgments require findings and source
event references; a passing outcome also requires acceptance criteria and
verification notes. Review history and JSON exports retain the evidence and cost
policy used at save time. Changed evidence marks prior reviews stale.

Work Units are a versioned accounting convention, not dollars. Model weights can
be supplied in the assessment's policy editor with an explicit basis. Unknown or
incomplete telemetry stays unknown/partial. This release measures native Codex
usage within the selected session; child costs and controlled reruns are future work.

### Optional LLM grader

Admins can open **Settings → LLM Configuration** (`/settings#settings-llm`) to
choose an OpenAI-compatible Chat Completions base URL, exact model ID, local or
external processing, JSON output mode, evidence-size limit, output-token limit,
and per-call timeout. Grading is disabled by default. The model must support
Chat Completions, `max_completion_tokens`, and the selected JSON output mode.
Enter the provider API key on the same settings page. Leave the password field
blank to keep the saved key, enter a new key to replace it, or select **Remove saved
API key** to clear it. Changes apply to the next run without restarting. No grader
environment variable is used. Credentials are encrypted in the server database
using a generated, owner-only `data/.grader-encryption-key` file, and are never
displayed or included in review exports. Back up that file with the database.
External providers require a key. Local mode
supports localhost or a private IP endpoint and can run without a key.

Open **Assessments** (`/assessments`) to track the latest AI grading attempt for
each task range you submitted. Filter by status, project, machine or submission
date. Running rows refresh as progress changes; interrupted workers are labeled
**Interrupted**. **Open result** shows the run's frozen evidence and findings.
**Review and resume** opens the current chunk, where eligible runs offer
**Retry unfinished batches**. Older attempts have readable pages in run history.

Use **Choose work to grade** (`/assessments?view=sessions`) to browse synced
sessions, preview a full session, or select turns from its conversation. The
assessment shows included requests and an estimated batch count before submission.
Results lead with outcome, supporting findings, coverage limitations and a suggested
next step. Click an event citation to inspect readable source evidence in a panel;
close it or press Escape to return. Batch diagnostics, capability comparisons,
cost details and optional manual review are collapsed by default. No manual review
is required to submit work or inspect an AI result.

The JSON dashboard uses the same views: `/assessments.json` now lists grading
runs; `/assessments.json?view=sessions` retains the session metrics/review export.
Both enforce the current user's owner scope and project access before pagination.

Once enabled, **Submit chunk for AI grading** at the top of an assessment explicitly sends that selected
range's text evidence to the displayed endpoint. Evidence that fits uses two calls:
demand and outcome first, with structured model/effort/cost metadata withheld, then
configured capability using model/effort observations only. Larger selections are
automatically packed into sequential evidence batches across adjacent turns, keeping a
whole turn together where possible. An oversized turn is split at event boundaries; an oversized event
is split into exact fragments with source indexes and character offsets. The viewer
removes mirrored bookkeeping records, repeated instructions, duplicate display/detail
text and tool transport wrappers. Requests, patches, command output, exit status and
final responses remain; the export retains the original evidence snapshot.
Deterministic rules compact exact repeated tool outputs, consecutive identical log
messages, repeated lint diagnostics (retaining every location), exhaustive integer
option ranges, and recognized generated search indexes/source maps. Markers retain
provenance and identify omitted contents. Unknown formats and patches stay intact;
generated artifacts explicitly named in requests or reviewer criteria are retained.
Recognizable pasted diagnostics stay in primary evidence but are represented as
attachments in repeated request context. No additional model calls perform this cleanup.
Each evidence-extraction batch also carries quoted task requests from the selected range, including
the initial goal and current request/corrections, plus the preceding tool call when
it fits. Turns after the batch are excluded from that context. Environment wrappers are not
treated as requests. When reviewer criteria are blank, the grader uses the recorded
requests to identify the task. Context is prepared locally without extra model calls.
Long context is marked as truncated, omitted requests are counted, and complete
source records remain in evidence batches. Context shares the existing input budget,
so the added context can increase the number of batches. Context explains intent;
it does not establish a passing outcome for unseen work.
The initial limits are 20,000 input characters, 512 output tokens for evidence
extraction, 1,024 output tokens for whole-task synthesis, and 600 seconds per call.
The synthesis output allowance is independently adjustable in LLM Configuration.
An additional 32,768-token budget includes instructions, schemas, evidence, output
and a 1,024-token chat-template reserve. UTF-8 bytes conservatively bound input
tokens for byte-level tokenizers such as Qwen; multilingual inputs can batch sooner.
Up to 100 evidence
batches run per submission, followed by whole-task synthesis and one independent
configuration request. If extracted notes do not fit, bounded summary reductions
combine them before synthesis. Successful reductions are checkpointed for explicit retry.
Incidental model
mentions can remain in trace text. Evidence is treated as untrusted; the grader
has no tools. Images and audio are explicitly marked unavailable; encoded media
is never fragmented into text grading batches. Mixed text/media tool results retain
their text. Original media remains in the frozen evidence export.

The browser shows an estimated batch count, consolidation savings, and links to the
largest prepared records before submission, and progress while
the explicitly submitted job runs in the background. You can leave the page and
return. **Cancel grading** aborts the active HTTP socket, including while waiting
for headers. The absolute per-call timeout also aborts the socket. The worker releases
its grading lock in `finally`. Completed results and usage are saved after each call.
Every request disables thinking with `chat_template_kwargs.enable_thinking=false`
and requests concise JSON. Schema/field violations, malformed JSON and
`finish_reason: "length"` fail the stage; incomplete grades are never accepted.
**Retry unfinished batches** continues a failed, cancelled or interrupted run without repeating
completed batches, provided evidence, criteria, configuration, and prompt version are unchanged.
Runs created before v6 deterministic consolidation remain available for review/export; submit
a new run to use the new batch boundaries and context.
A request interrupted before its result was saved may be sent again on an explicit
retry. There are no automatic retries. Jobs run in the current viewer process;
server restarts interrupt them, leaving saved results available for explicit retry.
Each batch extracts cited observations and limitations without assigning a slice
capability rating. The final assessment receives the meaningful user/assistant
conversation, including corrections and final responses, plus extracted notes and
original cited evidence when it fits. Oversized conversation excerpts are marked.
It evaluates the whole selected task and can return pass, partial, fail or unknown;
ratings are never averaged. Advice/design requests are evaluated as deliverables,
without demanding unrequested implementation. Every stage's exact input, prompt,
schema, output allowance and usage are retained in the run export. A failed final
assessment is not accepted as a grade; retry reuses completed extractions/reductions.

The UTF-8 context bound remains intentionally conservative. The configured local
proxy returned 404 for both `/tokenize` and `/v1/tokenize` during validation; this
release does not guess a larger safe token budget from character/token ratios.
Visual assessment and fuller use of the 32K window require a separately verified
multimodal/token-counting integration.

The chart compares **configured intelligence** and **required intelligence** as
estimated ordinal levels from 1 to 5, with confidence and a plausible required
range. Missing, unfamiliar, or mixed configurations may stay unestablished.
Differences are hypotheses for controlled experiments, not percentages of excess
intelligence, proven savings, or predicted turnaround. Compare grader findings
with independent evidence to calibrate them. The assessment dashboard tracks
submitted task ranges, including chunks smaller than a full session.

Each personal grader run retains its evidence snapshot, prompts/rubric version, output schemas,
configuration (without credentials), validated findings, and separately reported
evaluator usage. Grading never overwrites a human review or adds its tokens to the
task cost. Failures retain observed usage; provider usage for failed calls can be
unknown. Runs remain inspectable via readable history pages and JSON exports;
source changes mark estimates stale. Restarting during a run can leave an unfinished
attempt. Explicit retry resumes it only when evidence, criteria, configuration and
workflow still match; otherwise submit a new run. Only one run is admitted per
viewer process at a time.

The integration follows the [official Structured Outputs documentation](https://developers.openai.com/api/docs/guides/structured-outputs).

## Testing

The repo has both fast Python tests and browser-level end-to-end tests.

Install the Python test dependencies into the same dependency directory:

```bash
python3 -m pip install --target .deps -r requirements-test.txt
```

Run the Python suite:

```bash
PYTHONPATH=.deps python3 -m unittest discover -s tests -v
```

Install the browser runner:

```bash
npm install
npm run test:e2e:install
```

On Linux, Playwright may also require system browser dependencies before Chromium can launch:

```bash
sudo npx playwright install-deps
```

Run the suite:

```bash
npm run test:e2e
```

The E2E harness starts the real FastAPI app against a temporary SQLite data directory for each test, then drives onboarding, login, dashboard, project, session, queue, and machines flows through Playwright.

## Backup And Restore

The supported lightweight backup boundary is:

- `CODEX_VIEWER_DATA_DIR`
- the SQLite database file
- raw session artifacts stored under `data/session_artifacts`
- the generated browser session secret in `data/.session-secret`
- the grader credential encryption key in `data/.grader-encryption-key`, if configured

What this does not try to do yet:

- project-level export/import
- selective restore
- archive retention policies
- in-app project archive lifecycle

Recommended workflow:

1. Create a backup archive with `backup create`.
2. Verify it with `backup verify`.
3. Restore it into a fresh directory with `backup restore --data-dir ...`.
4. Start the viewer against the restored directory.

Implementation detail that matters for larger installs:

- `backup create` first snapshots SQLite into a temporary file before writing the archive.
- Plan for free temp space roughly equal to the database size, plus the output archive.
- Session ingestion keeps only the currently referenced raw artifact. Replaced snapshots and
  untracked files left by failed writes are pruned after the session update commits.

`backup restore` is intentionally offline and conservative. It restores into a new or empty target directory only; it does not merge into an existing install.

If you use the default layout, the restored instance can usually be started by pointing `CODEX_VIEWER_DATA_DIR` at the restored directory. If you run SQLite outside the data directory with `CODEX_VIEWER_DB`, restore it with `--database-path` and reuse that setting when you start the restored server.
