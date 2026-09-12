# Host follow-up and connection cleanup

**Deployment follow-up:** the user restarted the viewer at 08:09:17 UTC. [Live validation](restart-validation.md) confirms the new process, small descriptor counts, improved core-page responses, and remaining Search/Assessments latency. The implementation-time status below is retained for context.

## Updated diagnosis

The user's host-side capture identifies host PID **262954** as container PID **497802**, running `/usr/bin/python3.14` from `/home/hoff/agent_operations_viewer` in `/lxc/101/ns/system.slice/codex-session-viewer.service`. This is the viewer, not an unrelated Python job. The start time, 03:23:58 host-local time, agrees with the previously observed 07:23:58 UTC restart.

The preceding `pidstat` report attributed 45,797.90 KiB/s of reads and 1,329.06 KiB/s of writes to this process (approximately 44.7 and 1.3 MiB/s). These are process-accounting measurements, not a demonstrated rate of physical reads from the ZFS disks: the concurrent pool and device reports showed almost no reads. That mismatch remains unresolved, and the process rate must not be used to claim the HDDs were delivering 44.7 MiB/s to the viewer.

The host evidence also establishes:

- `myZFS` is a two-disk mirror, online, with no reported device or data errors. Its last reported scrub repaired zero bytes with zero errors.
- The capture shows intermittent writes and several idle intervals, rather than reproducing the earlier sustained read/write contention.
- Device flush latency is substantial during some intervals: `sda` reports `f_await` of 480 ms and 720 ms; `sdb` reports 368 ms in another interval. These are interval averages for flush requests, not page response times or ordinary read latency. See the [sysstat iostat definitions](https://github.com/sysstat/sysstat/blob/master/man/iostat.in).
- CPU idle time is generally high. Approximately 8 GiB of swap is occupied, but current swap-in is only 0–2 KiB/s and swap-out is zero in the interval rows. Swap occupancy alone does not establish current memory thrashing.
- Many open descriptors refer to the same viewer database. Descriptor count does not establish how many transactions are active or which ones hold locks.

The earlier observations of writes on the root `myZFS` dataset remain historical evidence. These newer samples do not identify an external writer responsible for the viewer's ongoing latency. Investigation should now prioritize the viewer's connection lifetime, repeated metadata reads, and maintenance lock duration. Neither capture establishes that the database engine must be replaced.

## Implemented cleanup

Code inspection and a local reproduction confirmed that the common pattern `with connect(path) as connection` left its connection open after exit. Python's SQLite connection context manager commits or rolls back; it does not close the connection. See [Python's documentation](https://docs.python.org/3/library/sqlite3.html#how-to-use-the-connection-context-manager).

Added `connection_scope` in `agent_operations_viewer/db.py`. It retains the native transaction context inside `contextlib.closing`, ensuring that connections close after successful work, exceptions, or commit failures. Connection setup also closes its handle if PRAGMA configuration fails.

Replaced 92 direct connection scopes across authentication, page routes, sync endpoints, startup/backfills, templates, importer, artifact pruning, alerts, and CLI export. Raw `connect` still returns a normal SQLite connection for callers that explicitly manage its lifetime; existing `closing(connect(...))` callers retain their behavior. This is a lifetime change, not a connection pool or query optimization. Raw backup/restore connections are outside this pass.

This removes reliance on garbage collection for completed scopes. It will not close a connection whose query or handler is still running. It does not change maintenance batching, writer-lock scope, query plans, SQLite settings, database placement, or the live service.

## Validation and deployment state

Six new regression tests in `tests/test_db_connection_scope.py` pass:

- Forty completed scopes close immediately even while Python references are retained, and their writes persist.
- Body exceptions roll back and close.
- Deferred foreign-key failures at commit roll back and close.
- Inner transaction contexts leave the outer lifetime scope usable.
- Configuration failures close their connection.
- Schema initialization and backfills close their connections.

The full Python suite ran **294 tests in 49.668 seconds**, reporting **8 failures**, all in action-queue tests. Re-running that module with `db.connection_scope = db.connect` restores the old transaction-only behavior and reproduces all eight failure records (including two subtest failures in one test method). No additional failures appeared in the full run. The suite is therefore not wholly green; those existing failures were not changed as part of this cleanup. `git diff --check` passed.

The running viewer has **not been restarted** to load this patch. No production latency improvement is claimed. On the next restart, recheck completed-request connection counts and the authenticated URL matrix. Explicit closure also changes when SQLite can perform last-connection cleanup/checkpoint work; production timing needs measurement, particularly given the host's observed flush latency.
