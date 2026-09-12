# Validation after connection cleanup restart

The restart at **2026-09-12 08:09:17 UTC** created viewer PID **536131**, after the connection cleanup files were saved. The service reported successful startup. The startup journal check found no matching errors, tracebacks, or unclosed-database warnings.

The live measurements support the cleanup change and show substantially better availability than the earlier timeout-heavy pass. **Search and Assessments remain slow.** Different storage conditions, cache state, and background activity prevent attributing the timing improvement solely to connection cleanup.

## Authenticated HTTP results

Measured from localhost between **08:10:04 and 08:12:16 UTC**, using the previously supplied session cookie. Requests were sequential with a 20-second timeout. Redirects were not followed. There were **24 attempts: 23 HTTP 200 responses and one search timeout**. No writes, grading runs, or sync runs were initiated by these probes; existing agent sync traffic continued.

| Page | Attempts | Current result | Earlier 07:24–07:26 pass |
| --- | ---: | ---: | ---: |
| Health | 2 | 2.5–6.0 ms | 3.7 ms |
| Settings | 5 | **60 ms median**; first request 839 ms, subsequent 58–65 ms | 6.27 s |
| Machines | 3 | **0.96 s median**, range 0.95–1.03 s | >20 s timeout |
| Dashboard | 3 | **1.51 s median**, range 1.23–1.67 s | >20 s timeout |
| Queue | 3 | **1.72 s median**, range 1.58–1.78 s | >20 s timeout |
| Project detail | 3 | **1.66 s median**, range 1.65–2.40 s | >20 s timeout |
| Project stream | 1 | 2.30 s | Not attempted in that pass |
| Session detail | 1 | 1.24 s | Not attempted in that pass |
| Session assessment | 1 | 0.23 s | Not attempted in that pass |
| Assessments dashboard | 1 | **16.69 s** | >20 s timeout |
| Search `q=test` | 1 | **>20 s timeout** | Not attempted in that pass; timed out in initial investigation |

Project detail/stream used `/benhoff/background-recorder`. Session routes used `01a0946a-e4cc-7a41-9883-5abfa6ce04ae`, with the default assessment range.

These are server-response/body-transfer timings, not browser rendering or full page-load metrics. The session detail response was approximately **4.82 MB**, and the session assessment response approximately **0.94 MB**, so network transfer and browser rendering remain relevant. The first settings request is a first observation after restart, not a controlled cold-cache benchmark.

## Connection and worker observations

The descriptor snapshots count handles pointing to the main database separately from WAL and shared-memory files:

- Initial requests returned to **2–3 main-database handles** between requests.
- During the later repeats, with the timed-out search still outstanding and normal sync activity continuing, counts rose to **5**.
- With no further diagnostic HTTP requests, five snapshots over 20 seconds remained at **5 main-database handles**, plus 1–3 WAL handles and one shared-memory handle.

This is a substantial reduction from the dozens seen before cleanup, and no continued handle growth was observed during the final observation period. It is a short live validation, not proof against every connection leak or a count of active transactions. Closing a completed scope does not interrupt an ongoing query.

The diagnostic thread dump after the search timeout showed:

- The search page worker inside `search._search_stage_counts`, called by `search_turn_hits_raw`.
- All four history/authentication workers waiting for queued work, rather than blocked together on manifests or the writer lock.
- No startup-maintenance thread in the dump.

Thus the search timeout is not explained by a saturated authentication worker pool in that snapshot. The client timeout did not cancel its synchronous database work. Search and the expensive Assessments dashboard were not retried; only the shorter successful routes were repeated.

Viewer full I/O pressure was much lower during the first core-page pass than during the earlier investigation. It increased while the search remained outstanding; background sync was also active. That correlation does not isolate search's contribution to disk pressure, but reinforces the need to reduce the remaining expensive queries rather than treat the successful restart as complete latency remediation.

## Result

The cleanup is loaded, core pages respond consistently in this sample, and descriptor counts remain small. Settings is now fast after its initial request. Dashboard, Queue, Project detail, and Stream still take roughly 1–2 seconds, while Assessments and Search need further optimization. The next application targets are search count/coverage work and assessment evidence reconstruction.

No application changes or additional restart were made during validation. The temporary authentication cookie file was removed after the requests completed.

Evidence: [HTTP results and per-request handle snapshots](http-after-connection-cleanup.json), [final handle observation](handles-after-cleanup.json), and [search thread dump](search-stacks-after-cleanup.txt).
