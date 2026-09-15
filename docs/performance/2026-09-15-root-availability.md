# Root availability during sync uploads

## Cause and change

An unauthenticated localhost GET `/` timed out after 20.002 seconds without
receiving any bytes, while health returned in 1–4 ms. Two live thread snapshots
showed all four history workers waiting for `WRITE_LOCK` in sync heartbeats.
The upload worker held that lock while updating session repository mappings.
Browser authentication used the same history pool, blocking requests before
the root handler ran. Service I/O pressure was also high during the incident.

- Browser auth now has a separate bounded pool (two workers, 64 in flight).
  Sync authentication, including required machine nonce writes, stays in the
  history pool. Auth middleware returns a retryable 503 when its pool is full.
- Project and repository refreshes read transactionally maintained compact
  session metadata. Session, project, and source mappings update only when
  their values change. An unchanged refresh makes zero database changes and
  preserves the browsing cache revision.
- Registry identity resolution still examines all visible sessions to preserve
  repository merges and project grouping; this is not a fully incremental sync.

## Validation

125 focused tests passed across root availability, repositories, startup,
page loading, browsing, projects, sync ingestion, route authorization, search
API, and setup. The new authenticated-root test holds both upload write locks
while four real heartbeat jobs occupy the history workers. Root returns 200
before those jobs finish. Substituting the old shared auth pool causes that
same regression test to fail its three-second deadline as expected.

The sandboxed TestClient stalled before application dispatch. The focused
suites passed outside the sandbox. No production authentication was bypassed.

## After the operator's restart

Service startup completed at 2026-09-15 07:27:45 UTC, PID 2160469.

| Measurement | Result |
| --- | ---: |
| Local HTTP root, unauthenticated login redirect (303) | 52.3 ms |
| Local HTTP health (200) | 1.9 ms |
| Direct root render, live read-only database, first sample | 193.1 ms |
| Direct root render, subsequent samples | 17.6 / 15.6 ms |
| Rendered HTML size | 116,717 bytes |

The direct handler probe used a separate diagnostic process and an unrestricted
operator view, with every SQLite connection opened `mode=ro` and `query_only`.
It includes actual Jinja rendering but excludes auth middleware, network,
browser, and production worker contention. It performed no database writes.
The running service also logged a subsequent root 200 and stylesheet 200 from
a browser; those requests were not timed. These samples establish recovery,
not a production p95 or a guarantee against storage contention.
