# Root and project browsing optimization

Implemented after the 08:46 UTC HTTP check. The running service has not been restarted for this change, and the production database has not been migrated by this investigation.

## Changes

- `session_browse` holds compact session metadata and previews capped at 1,024 characters. Full session text and audit evidence remain in their original tables. Database triggers maintain browsing rows in the same transaction as session inserts, updates and deletes, including reimports and maintenance writes. The initial schema migration backfills existing sessions once.
- A bounded, per-process project catalog groups the compact rows once per browsing revision and access scope. It serves project summaries, statistics, membership and a direct route-to-key dictionary. It retains the established host selection, overrides, reserved routes and collision suffix rules. There is no TTL or background refresh delay: session/project/source/override/ignore changes advance the revision transactionally. Access-role changes use a different cache key, and project visibility changes advance the revision. Uncommitted writer results never enter the shared cache. Old revisions for a database/access scope are evicted.
- Root uses the catalog instead of reading wide session rows and rebuilding the same groups on every navigation. Recent activity remains a small query against daily rollups. An expression index accelerates the existing exact-timezone prompt count; it does not replace today's count with a differently defined calendar-day total.
- Project Activity skips session-preview construction and the unused historical action/health builders. Sessions skips the timeline query and builds previews only for the selected SQL page. Existing full-detail helpers remain available to diagnostic/edit surfaces that need them.
- Timeline queries join compact browsing rows rather than wide session rows. Main project pages also restrict the query to session IDs from the authorized catalog. Full-text filtering on root retains its existing search behavior and is not covered by the unfiltered-page performance claim.

The catalog is an in-memory derived summary and route index, not a new persistent project-route authority. This avoids changing established access-dependent route collision behavior while removing repeated routing scans. SQLite remains the database.

## Isolated live-data benchmark

A connection opened the current production SQLite database with `mode=ro`. A TEMP browsing table was populated from live session metadata, with the same bounded previews as the migration. The root and project route handlers then rendered their actual Jinja templates sequentially, using that connection. The original session/turn/project/heartbeat data remained read-only. Neither writes to the production database nor a service restart were performed.

| Handler | First observed render | Subsequent three renders |
| --- | ---: | ---: |
| Root | 270 ms | 76–79 ms |
| Project Activity | 59 ms | 38–39 ms |
| Project Sessions | 3 ms | 2.9–3.2 ms |

The temporary projection took 696 ms to populate from 1,213 sessions. The root request warmed the catalog before the first project request. These figures include template rendering but exclude the HTTP/authentication middleware, network, browser, production concurrency and the migration's disk writes. The new prompt-time index was not installed on the read-only live database; root therefore still used the old count path during this experiment. This is evidence that request-time work is substantially reduced, not a production latency claim or a p95 estimate.

Raw samples: [render benchmark](browse-render-benchmark.json). Previous production measurements: [HTTP recheck](home-cleanup-latency.md).

## Validation and rollout

47 distinct focused Python checks passed across browsing, startup, database connection lifetime, project semantics, onboarding and route authorization. Nine browsing tests specifically cover migration/backfill, committed cross-connection updates, deletes, rollbacks, route/visibility invalidation, bounded session preview construction, skipped inactive-tab work, forbidden transcript/action reads, and timezone/index behavior. All 12 selected browser scenarios passed, covering root desktop/mobile, project timeline pagination/back navigation, session pagination, edits, ACL propagation, deletion and ignore/reimport behavior. One stale browser assertion was updated from the old search heading to the current heading.

A service restart runs the initial browsing migration and loads the new handlers. Recheck authenticated root and both project tabs afterward, including first visits and normal background uploads. The isolated measurements support the intended few-hundred-millisecond server budget; production performance still needs that validation.
