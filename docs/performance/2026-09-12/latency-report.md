# Running viewer latency check — 2026-09-12

The running viewer has significant server-side latency and intermittent stalls. Search and the assessment dashboard exceeded a 20-second client timeout. Settings and Machines initially returned successfully, then repeatedly timed out. Database-heavy request processing and contention with sync writes are the primary leads.

## Method and limits

- Target: `http://127.0.0.1:8001`, systemd service `codex-session-viewer.service`, Python 3.14, process 478584. Measurements were taken around 07:02–07:11 UTC.
- Authenticated using the operator-provided browser session. The temporary cookie file was removed after testing; credentials and response bodies are not included here.
- Sequential GET requests, redirects disabled, recording status, time to first byte (TTFB), complete response time, and body bytes. Login redirects were excluded from page measurements.
- One authenticated sample per route in the initial pass. The follow-up pass recorded three samples each for Settings and Machines, then was stopped because of repeated timeouts. These are diagnostic samples, not p95 estimates or a controlled load benchmark.
- The production workload remained active. Logs showed session uploads, heartbeats, and a grader worker/status polling. Client timeouts did not cancel the running server queries: the stack dump confirmed assessment/search work continuing afterward. Later measurements therefore include background traffic and outstanding diagnostic requests.
- No deployment, database migration, application edit, or service restart was performed. Separate local profiling used a read-only SQLite connection before the authenticated HTTP pass. A built-in SIGUSR1 diagnostic signal captured server thread stacks.
- Localhost timings exclude the user's network and reverse proxy. They demonstrate server latency but do not measure the complete remote browser experience.

## Live HTTP results

| Page / route | Initial total | Initial TTFB | Body size | Follow-up |
| --- | ---: | ---: | ---: | --- |
| `/api/health` | 4 ms | 4 ms | 93 B | Final check: 2 ms, HTTP 200 |
| `/` | 4.101 s | 4.096 s | 177 KB | — |
| `/settings` | 2.618 s | 2.618 s | 61 KB | 6.318 s, >20 s timeout, >20 s timeout |
| `/machines` | 0.934 s | 0.934 s | 68 KB | Three >20 s timeouts |
| `/queue` | 2.325 s | 2.325 s | 52 KB | — |
| `/assessments` | >20 s timeout | No headers received | — | Still executing in subsequent thread dump |
| `/search?q=test` | >20 s timeout | No headers received | — | Still executing in subsequent thread dump |
| `/benhoff/background-recorder` | 1.676 s | 1.675 s | 34 KB | — |
| `/benhoff/background-recorder/stream` | 2.043 s | 2.043 s | 171 KB | — |
| `/sessions/01a0946a-e4cc-7a41-9883-5abfa6ce04ae` | 1.439 s | 1.438 s | 1.226 MB | Later Chromium navigation exceeded 45 s |
| Same session, `/assessment` (default one-turn range) | 0.198 s | 0.198 s | 935 KB | — |

All completed initial page requests returned HTTP 200. Sizes use decimal units. Timeouts are lower bounds, not completed response times. The browser navigation waited for `load`; it timed out before usable rendering metrics were collected, so its delay cannot be attributed specifically to rendering or server processing.

The HTTP samples spent almost all their measured time waiting for headers. Large session and assessment HTML payloads are a secondary optimization opportunity, especially over slower networks, but download time does not explain the measured localhost stalls.

## Findings and optimization order

1. **Remove blocking database writes from routine page reads.** The live thread dump caught Settings waiting in `db.write_transaction` from `render_settings_page`. A sync worker was simultaneously in `upsert_parsed_session`, deleting the session's existing events. The grader checkpoint and a heartbeat worker were also waiting for the same application write lock. Dashboard and Machines likewise call onboarding reconciliation inside a write transaction. Move reconciliation to explicit state transitions or background work, or make optional bookkeeping nonblocking while preserving setup correctness. Review full-session event replacement during sync. Relevant code: `web/routes/pages.py:258`, `:1441`, `:1614`; `db.py:1164`; `importer.py:115`.

2. **Bound assessment-dashboard evidence work.** At 07:07:38 the timed-out `/assessments` request was still in `task_assessment._turn_start`, called by `task_source` for dashboard rows. `dashboard_data` reconstructs evidence and metrics for up to ten sessions per page, with per-turn boundary queries. Existing limits are 50 turns and 20,000 events per session; they do not guarantee a fast page. Prefer persisted metrics keyed to evidence version, or load expensive metrics on demand with a work budget. Investigate a narrow/partial boundary index after measuring representative plans. Relevant code: `assessment_dashboard.py:92`; `task_assessment.py:317`, `:343`.

3. **Keep search coverage calculation off the interactive critical path.** The timed-out `/search?q=test` request was still in `_search_coverage` at line 740, before result retrieval. Coverage reconstructs indexed-turn/session sets and checks evidence existence across the accessible corpus. Maintain coverage incrementally or cache it with correct corpus-version and authorization invalidation; avoid recomputing full integrity information on every query. Relevant code: `search.py:593`, `:718`, `:740`, `:1631`.

4. **Avoid rebuilding all project groups to resolve one link.** Read-only profiling measured about 0.89 s for `resolve_project_detail_href`, which reads and groups all visible session metadata. Similar work occurs when resolving project routes. Use stable project/route lookups and preserve ACL filtering and route-collision semantics. Relevant code: `projects.py:2731`, `:2778`.

5. **Reduce stream and action-summary SQL cost.** The separate profile for the largest project measured 12.33 s in `fetch_turn_stream`, of which 12.330 s was inside two SQLite `execute` calls. Project-detail computation took 3.143 s, including 2.381 s building the action queue. The later live stream request was faster at 2.043 s, demonstrating substantial workload/cache variability; the profile is not a repeat HTTP result. Inspect count/order queries, project scoping, and reusable action summaries. Relevant code: `projects.py:1723`, `:2447`; `action_queue.py:368`, `:897`.

6. **Add request-duration and cancellation diagnostics.** A 20-second client timeout leaves these synchronous queries working in the server. Add bounded query execution and cancellation where appropriate, plus duration/error metrics for authenticated pages. `/api/health` explicitly bypasses database auth and remains responsive during these stalls; retain it as a liveness probe and use a separate representative read-latency check.

## Supporting database observations

The live SQLite file was approximately 11 GB. The accessible metadata query returned 1,211 sessions with stored rollups totaling 11,111 turns and 923,965 events; these are summed session rollups, not independent table counts.

Query-plan inspection confirms that the turn-boundary query already uses `idx_events_session_index` for session/event-range lookup. Evidence-existence checks use that index as a covering index. This is not evidence of a missing basic session index; repeated boundary lookups still inspect event rows for `record_type` and `payload_type`, and the overall evidence/coverage workload remains expensive. Index or query changes should be validated against this data before deployment.

## Evidence files

- [Initial authenticated HTTP samples](http-initial.json)
- [Completed follow-up samples](http-repeat.json)
- [Browser attempt](browser.json)
- [Read-only profile timings](profiles.json)
- [Project stream profile](project-stream-profile.txt)
- [Project detail profile](project-detail-profile.txt)
- [Live server thread stacks](thread-stacks.txt)
- [SQLite query plans](query-plans.json)

The JSON `median_ms` fields are calculated over successful samples only and must not be used alone when a route has timeouts. The table above includes every completed follow-up sample, including failures.
