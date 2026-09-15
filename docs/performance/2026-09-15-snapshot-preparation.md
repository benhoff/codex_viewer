# Faster snapshot preparation and accurate timing

## Observed failure

The live builder started at 07:30:21 UTC and failed at 07:41:26 UTC on
2026-09-15. Its final diagnostics recorded 664.912 seconds in preparation,
with 2,044,416 of 3,047,566 database pages copied. The configured build budget
was 600 seconds. During the stall, clients had seen an older backup count of
2,041,344 pages and continued receiving `snapshot_building`.

The backup callback enforces the cooperative deadline, but it cannot run while
a database copy step waits for storage. Pending status only updated on backup
callbacks and stage transitions, so its elapsed time also stopped advancing.
This is a preparation failure; it provides no evidence about search results or
the presence or absence of hardware evidence in the corpus.

## Changes

- Allow snapshot files to live on a separate disk with
  `CODEX_VIEWER_SEARCH_SNAPSHOT_DIR`. The live database stays in place; the
  signing key and builder lock remain shared, and existing snapshot IDs continue
  to resolve in the original location. Cleanup and capacity cover both locations.
- Start timing when a build is accepted, including worker startup delay.
- Advance total and current-stage elapsed time on every poll using a monotonic
  clock. Separately report the age of the last actual worker progress update.
- Mark backup page counters as database-copy progress only.
- Persist a terminal timeout when a poll observes an overdue active build.
  `worker_stopping` identifies a worker that has not yet returned from blocking
  work. The same snapshot cannot later become a successful result.
- Measure ready time after metadata commit, database close and file publication.
  Save total and stage durations, including publication, in an atomic manifest.
  These measurements remain fixed across searches using the same snapshot.
- Require that manifest for new snapshots, while retaining old snapshot support.

The measured interval ends at database publication and excludes the subsequent
diagnostic manifest write, HTTP queueing/authentication, search execution and
network transfer. The timeout allowance and polling interval are not ETAs.
Timing fixes cannot forcibly cancel kernel I/O. Builder capacity is released
only when the worker exits.

## Live storage benchmark

The original destination was beside the live database on ZFS storage. A second
production attempt copied 3,049,782 pages in 559.042 seconds, then failed during
coverage inventory at 600.630 seconds. The source and destination were competing
for the same storage.

After that builder released its lock, a full snapshot was built on the separate
local ext4 filesystem under `.snapshot-cache`. It completed at 07:56:38 UTC on
2026-09-15:

| Stage | Seconds |
| --- | ---: |
| Database copy | 176.289 |
| Coverage inventory | 6.686 |
| Coverage | 0.092 |
| Metadata | 0.013 |
| Total through publication | **183.082** |

The resulting snapshot was 12,515,749,888 bytes, copying all 3,055,603 pages.
The database-copy stage was about **3.17 times faster** than the preceding
production attempt. Total creation finished in about three minutes, within the
unchanged 600-second budget. The benchmark artifact was removed after completion.

These are sequential live observations, not a controlled storage benchmark:
cache state and background I/O varied, and the source grew by about 0.2%.
Future builds can still vary with source disk load. No evidence was omitted.
The local deployment now sets `CODEX_VIEWER_SEARCH_SNAPSHOT_DIR` to
`/home/hoff/agent_operations_viewer/.snapshot-cache`.

## Validation

62 tests passed across configuration, snapshot construction, search API
integration, request budgets and personal search tokens. New regressions cover
relocated storage, existing IDs, shared builder locking/capacity, stalled inventory
polls, elapsed time after 600 seconds while the worker remains blocked, terminal
timeout persistence, partial publication, publication failure, legacy snapshots,
and timing reuse through HTTP responses.

A deterministic timing test simulates four seconds waiting for the worker,
five seconds committing metadata, three seconds closing the database and seven
seconds publishing it. The reported total is 19 seconds; ready time and the
read lifetime start after publication. Reusing the snapshot 60 seconds later
preserves that same 19-second measurement. These are controlled correctness
checks, not production throughput benchmarks.


## Shared generation follow-up

The server can now reuse a recent prepared generation across separate owner-bound
handles instead of repeating this full copy for each investigation. The reuse
window is five minutes from publication; every handle inherits the generation's
fixed expiration 15 minutes after publication. `fresh_snapshot=true` bypasses
ready reuse, while compatible in-progress builds can still be joined.

Regression tests instrument inventory construction and verify exactly one build
and one physical SQLite file for three independently authorized handles, including
an administrator and restricted readers. A subprocess also reuses a generation
with its builder disabled, proving reuse does not rely on the originating process's
cache. HTTP tests verify private evidence remains inaccessible and explicit fresh
requests produce a different generation. These validate avoidance of duplicate
copy work, not a new production latency measurement. The initial production-sized
build still incurs the measured copying cost; reused requests perform authorization,
coverage and query work without that copy.
