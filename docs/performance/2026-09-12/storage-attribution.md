# Storage attribution after the page-load pass

**Later host evidence:** [host follow-up and connection cleanup](connection-lifecycle.md) identifies the busy Python process as the viewer itself, records a quieter pool interval with slow flushes, and documents the resulting application fix. The measurements below describe the earlier interval and do not identify an external process responsible for ongoing page latency.

The next investigation found **heavy competing writes on another dataset in the same ZFS pool**, alongside expensive viewer metadata reads and startup work holding the application writer lock. This establishes a storage-contention problem worth addressing before interpreting another page-latency comparison. It does not establish how much latency would disappear without the competing workload, or identify that workload's host process.

## Measurements

The viewer runs in an LXC container. Its database is `/media/agent_operations_viewer/data/codex_sessions.sqlite3`, on `myZFS/subvol-101-disk-0`. Viewer cgroup I/O is charged to the rotational devices `sda` and `sdb`. The pool topology still needs confirmation on the host; similar device rates alone do not establish its redundancy configuration.

Six consecutive five-second samples were collected from **07:48:40–07:49:10 UTC**, without starting new database queries or page requests. Rates below use decimal MB and the entire 30.01-second interval.

| Measurement | Result |
| --- | ---: |
| Other dataset `myZFS`: logical writes | **28.33 MB/s average**, 126.37 MB/s in the busiest five-second sample |
| Viewer dataset: logical writes | **546 bytes/s average**, about 16 KiB total |
| Viewer dataset: logical reads | 0.71 MB/s average |
| `sda`: physical reads / writes | 9.43 / 30.69 MB/s |
| `sdb`: physical reads / writes | 10.32 / 30.63 MB/s |
| `sda` / `sdb`: mean completed-read wait | 14.81 / 13.55 ms |
| `sda` / `sdb`: busy time | 82.5% / 80.3% |
| Viewer cgroup: full I/O stall time | **34.85%** of the interval |
| System-visible full I/O stall time | 55.71% of the interval |

In the last five-second interval, completed-read waits reached 41.40 ms on `sda` and 35.91 ms on `sdb`; viewer full I/O stall time reached 62.6%. Earlier short samples also observed read waits around 28–33 ms.

Dataset counters measure logical traffic, whereas block counters measure device traffic. They must not be subtracted to attribute physical writes: buffering, transaction-group timing, caching, compression, and redundancy can separate these measurements. The direct observation is that the substantial logical write workload was on **`myZFS`, not the viewer's dataset**. Its contribution to the viewer's latency is an inference supported by concurrent disk activity and viewer stalls, rather than a controlled on/off experiment.

Linux defines full PSI as time when all non-idle tasks in the measured scope are stalled on the resource. This is a service-level waiting measurement, not the percentage of page latency caused by storage. See [Linux PSI documentation](https://docs.kernel.org/accounting/psi.html). `/proc/diskstats` is exposed through LXCFS here; host-side measurements should confirm the device view.

## Viewer work contributing to the problem

The **07:37:47** stack dump, almost 14 minutes after the 07:23:58 restart, showed:

- Startup maintenance inside the stale-session selection in `backfill_session_turns`. `run_db_backfills` acquires the application writer lock and starts `BEGIN IMMEDIATE` before that selection.
- Three history workers waiting for the writer lock in sync heartbeat handling, and the upload worker waiting for it in artifact pruning.
- Another history worker reading a sync manifest; a page worker reading project-route metadata.

A later five-second thread sample found all four history workers performing reads, plus the maintenance and page workers. Their combined logical read calls were about 15.8 MB/s, with no writes in those sampled threads. The dataset reported approximately 16 MB/s of reads in that interval. Sampled wait channels included block-request allocation and ZFS condition waits. The viewer is therefore also producing substantial read work, independently of the other dataset's writes.

These observations identify two application targets after or alongside host remediation:

1. Avoid discovering backfill work while holding the writer slot; bound the actual maintenance transactions. Current batch limits do not bound the time spent discovering stale sessions, and some earlier backfill phases are not batched.
2. Reduce repeated metadata scans from agent manifests and project routing. Manifest and authentication work share four history workers, so long manifest reads or heartbeat lock waits can delay page authentication before page rendering starts.

The thread sample also found many database descriptors, but descriptor count alone does not prove active transactions, a connection leak, or a checkpoint problem. The earlier tiny WAL and absence of current memory PSI do not justify WAL or memory tuning as the first intervention.

## Host attribution boundary and next action

The container cannot enumerate host processes. In the process sample, only 21 process I/O files were readable and 150 were denied; passwordless sudo was unavailable. Neither `zpool` nor `zfs` is installed here. The competing job cannot responsibly be named or stopped from this evidence. It could be a host task or another workload with access to that dataset; no particular backup, sync, scrub, or application has been established as the cause.

On the ZFS host, collect the following read-only observations during a slow interval:

```sh
zpool status -P myZFS
zfs list -o name,mountpoint,used,refer -r myZFS
zpool iostat -v -l -q -y myZFS 1 10
pidstat -d -p ALL 1 10
```

`pidstat` requires sysstat. Run it with host privileges sufficient to see all tasks. Match the busy writer's open files to the `myZFS` mountpoint, then determine what the job does before choosing a scheduling or rate-limit change. If asynchronous kernel writeback obscures the originating writer, correlate several samples and the host's scheduled jobs. The [OpenZFS iostat documentation](https://openzfs.github.io/openzfs-docs/man/master/8/zpool-iostat.8.html) explains the pool latency and queue columns and why physical and logical I/O differ.

After controlling the identified workload, let outstanding viewer work drain and repeat the same authenticated URL matrix with one request at a time. If metadata reads remain slow during a quiet interval, measure their query plans and data access before deciding between an index/schema change and different database storage. The present measurements do not demonstrate that changing from SQLite to PostgreSQL would remove the storage bottleneck.

No application code, live database, service configuration, or host workload was changed during this investigation. No new page speedup is claimed; the last authenticated page measurements remain those in [the optimization reassessment](optimization-pass.md).

Evidence: [30-second dataset/device/PSI series](storage-io-series.json), [process sample](storage-process-sample.json), [thread I/O sample](storage-thread-sample.json), and [server thread stacks](storage-thread-stacks.txt).
