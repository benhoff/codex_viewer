# Page-load optimization pass and reassessment

Two bounded application changes are implemented and loaded by the user's restart at **2026-09-12 07:23:58 UTC** (viewer PID 497802). The regression checks pass. The subsequent live measurements **do not demonstrate a speedup**: the host was under substantial storage pressure and multiple pages still timed out.

## Changes

1. **Read-only onboarding status for page loads.** Dashboard, Settings, Machines, setup rendering/status, and the authenticated login redirect now call `read_onboarding_status` without acquiring the application write lock or a SQLite write transaction. The helper calculates current status, including legacy timestamp fallbacks, without persisting changes. Existing setup and sync mutation handlers retain transactional reconciliation. Missing onboarding rows are safe to read without creating them.

2. **Smaller project-route queries.** Project URL and route resolvers read routing metadata without loading session summaries, latest-turn previews, usage fields, or other unnecessary session text. The existing grouping, row ordering, host selection, override behavior, slug collisions, and project ACL filtering are retained. This reduces materialized data; it does not eliminate the all-session route lookup.

Implementation: `agent_operations_viewer/onboarding.py`, `agent_operations_viewer/web/routes/pages.py`, and `agent_operations_viewer/projects.py`. The other pre-existing checkout changes were preserved. No schema, index, database-engine, sync-ingestion, or grading changes were made in this pass.

## Validation

**67 tests passed** across:

- `tests.test_page_load_performance`
- `tests.test_projects`
- `tests.test_setup_reset`
- `tests.test_assessment_dashboard`
- `tests.test_task_assessment`
- `tests.test_startup_performance`
- `tests.test_route_auth_audit`
- `tests.test_search`

The new regression tests prove that:

- Dashboard, Settings, Machines, and setup status finish while another thread holds both the application write lock and a SQLite `BEGIN IMMEDIATE` transaction.
- Status calculation succeeds with SQLite `query_only=ON`, even before an onboarding row exists, and agrees with persisted reconciliation for the same fixture.
- Route results match the existing grouping logic for colliding slugs, multiple hosts, overrides, and restricted projects. A SQLite authorizer rejects attempts to read the large session preview columns during the optimized lookup.

The first sandboxed TestClient run stalled in the event-loop wakeup before entering the route. Rerunning those tests outside the sandbox passed. `git diff --check` also passed.

## Live reassessment

Same localhost server and supplied authenticated session as the initial check. One sequential request per route, with a 20-second client timeout. The temporary cookie file was removed afterward.

| Route | Initial earlier sample | Sample after restart |
| --- | ---: | ---: |
| `/api/health` | 4 ms | 4 ms |
| `/settings` | 2.618 s; later 6.318 s and timeouts | 6.270 s, HTTP 200 |
| `/machines` | 0.934 s; later three timeouts | >20 s timeout |
| `/queue` | 2.325 s | >20 s timeout |
| `/` | 4.101 s | >20 s timeout |
| `/assessments` | >20 s timeout | >20 s timeout |
| `/benhoff/background-recorder` | 1.676 s | >20 s timeout |

The pass was stopped before completing the remaining routes to avoid adding outstanding queries. Client timeouts do not cancel synchronous database work already running in the server. No percentiles or percentage improvement can be inferred from these samples.

A read-only old/new route microbenchmark was also attempted with a query budget. Both the original full-metadata query and the lean query exceeded the budget in separate attempts during the same period, so no speedup is claimed from that experiment either.

## New evidence

The 07:25:43 server thread dump confirms the modified code is running:

- Machines is inside `query_group_rows`, called by `fetch_agents_dashboard`.
- Dashboard is inside `query_group_rows`.
- The startup maintenance thread is scanning session rollup versions.
- All four history/auth executor workers are serving agent manifest queries.
- A sync-upload worker is waiting for the write lock held by startup maintenance.

The sampled page threads are now executing read queries instead of waiting on their former onboarding write transaction. This validates removal of that dependency, but read/query cost and competition for the shared history/auth workers remain.

Host-visible storage measurements during the recheck:

- Database filesystem: `/media`, ZFS dataset `myZFS/subvol-101-disk-0`.
- Two one-second `vmstat` samples reported **63% and 66% CPU I/O wait**, with 6–8 blocked tasks.
- `/proc/pressure/io`: `some avg10=70.71`, `full avg10=62.92`; 60-second averages were 75.36 and 69.32 respectively.
- `/proc/pressure/memory`: zero current 10-, 60-, and 300-second averages.
- WAL disk usage at inspection was approximately 4.5 KB; an oversized WAL was not established as the cause at that point.

These measurements establish substantial I/O waiting visible to this environment. They do **not** identify the responsible physical device or workload, or establish that ZFS itself is the cause. Startup work, agent traffic, and outstanding diagnostic requests were active, so the before/after runs are not controlled comparisons.

## Reassessment

Keep the two application changes: their behavior is verified and they remove avoidable locking and data loading. However, page latency remains unacceptable in the live samples.

The next useful investigation is storage attribution on the host/pool and the amount of repeated metadata work from startup and agent manifests. After that pressure is understood or controlled, rerun the latency matrix. Expensive assessment evidence reconstruction and search coverage calculation remain outstanding application targets from the original report. These results still do not establish that moving from SQLite to PostgreSQL would resolve the observed stalls.

Evidence: [after-restart HTTP results](http-after.json), [after-restart thread stacks](thread-stacks-after.txt), and [original latency report](latency-report.md).

Follow-up: [storage attribution](storage-attribution.md) identifies competing writes on another ZFS dataset, measures viewer I/O stalls, and records the host-side investigation still needed to name the writer.
