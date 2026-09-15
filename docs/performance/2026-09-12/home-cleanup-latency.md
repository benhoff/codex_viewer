# Main-page latency recheck after home cleanup

Measured 2026-09-12, 08:46:28–08:47:32 UTC against `http://127.0.0.1:8001`, authenticated with the operator-supplied session. The service restarted at 08:35:03 UTC and remained PID 563787 throughout the check. Home responses contain Active Repos and no Needs Attention or Verification failed text, confirming the cleaned-up view is served.

Requests were sequential GETs, redirects disabled, with a 20-second timeout. Eight core pages were measured in three interleaved rounds. Assessments and Search were attempted once each afterward. All 24 core-page requests and the Assessments request returned HTTP 200 without redirects. Search timed out and was not retried, since client timeout may leave synchronous query work running. Health returned HTTP 200 in 3.28 ms initially and 1.68 ms after the timeout.

| Page | Previous result | Current median/result | Current range |
| --- | ---: | ---: | ---: |
| Home | 1.51 s | 1.08 s | 1.00–1.11 s |
| Settings | 60 ms | 59 ms | 59–153 ms |
| Machines | 0.96 s | 0.90 s | 0.87–0.91 s |
| Queue | 1.72 s | 1.58 s | 1.50–1.65 s |
| Project detail | 1.66 s | 2.88 s | 1.45–6.32 s |
| Project stream | 2.30 s | 1.55 s | 1.50–1.70 s |
| Session detail | 1.24 s | 1.23 s | 1.17–1.37 s |
| Session assessment | 227 ms | 169 ms | 159–199 ms |
| Assessments dashboard | 16.69 s | 4.32 s, one sample | — |
| Search `q=test` | >20 s timeout | >20 s timeout | — |

Previous figures are from the 08:10–08:12 [restart validation](restart-validation.md). Previous stream, session, session assessment, and Assessments figures were single samples; other displayed previous successful page figures were medians. Project routes use `/benhoff/background-recorder`; session routes use `01a0946a-e4cc-7a41-9883-5abfa6ce04ae`, with the default assessment range.

The home median is about 28% lower. Almost all localhost elapsed time is spent waiting for response headers. Home HTML is approximately 113 KB; the sampled session HTML is 7.38–7.67 MB, so browser rendering and remote transfer may add meaningful time there. These measurements exclude browser rendering, assets, reverse proxy, and user network latency. The live corpus, caches, background work, and other application changes were not controlled; differences cannot be attributed solely to the home cleanup. Three samples are insufficient for a useful p95 estimate.

Search remains the clearest unresolved slow route. Project detail has intermittent multi-second delays, and Assessments remains slow despite its better single sample. No service restart, grading, sync trigger, or application edits were performed in this check.

Raw timings and response sizes: [HTTP results](http-after-home-cleanup.json). No credentials or response bodies are stored in the report.
