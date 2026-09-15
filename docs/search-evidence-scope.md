# Recorded-evidence API: scope and implementation review

The contract is: what was recorded, where it came from, and what may be missing.
A reviewer decides whether those records support a hardware claim or submission.

## Included

| Capability | Implementation and limits |
| --- | --- |
| Snapshot reliability and progress | Separate local snapshot storage measured a full creation at 183 seconds; the copy stage took 176 seconds versus 559 previously. Progress separates copy from later preparation stages; terminal failures and expiry never fall back to live data. See the [measurement](performance/2026-09-15-snapshot-preparation.md). |
| Shared prepared generations | Users and worker processes reuse one recent physical copy through separate owner-bound handles. Current permissions are frozen per handle; exact session/project scopes and ongoing revocation checks prevent cross-user access. Expiry is fixed for the generation. `fresh_snapshot=true` bypasses ready reuse. |
| Output availability | Recorded output values distinguish captured-empty, captured, explicit truncation/unavailability, missing linked results, and unknown. Capture completeness remains unknown without an explicit producer flag. Historical normalized text is not proof that a result was completely captured. |
| Command/result linkage | Stable session/event references retain results consumed by activity merging. Exact call IDs associate records within a turn; ambiguous or reused IDs remain unknown. |
| Provenance | Recorded role/type identifies user messages, assistant responses and tool calls/results. Pasted terminal-looking text remains a user message. |
| Decoded output | Versioned text-envelope decoding alongside the preserved stored JSON value. Unsupported or mixed-media representations are not silently reduced to partial text. |
| Coverage diagnostics | Bounded issue summaries plus a paginated endpoint identify affected visible sessions, warning text and known missing-turn/search/evidence counts. Snapshot pinning, structural filters and current ACL checks apply. |
| Reference client | Tested standard-library client handles preparation retries, pagination, complete turns, activity pagination and digest checks. Failed or expired snapshots stop an investigation. |
| Recorded artifact references | Existing image-generation `saved_path` fields are exposed as references with unknown content-capture status. No file is opened or hosted. |

Response normalization changes to `evidence-2`; the server explicitly rejects
older snapshots after deployment. This requires new snapshots, not a source-data
migration or search reindex. Event IDs identify imported positions; content digests
identify their contents within a pinned generation. The storage optimization is
already deployed; the response/client and shared-generation additions need a service restart after
review.

## Investigated and deferred

- **Build IDs and generic artifact metadata:** there is no established cross-tool
  structured build/test record in the current normalization. A producer-specific
  adapter can expose a captured field when its semantics are documented. Parsing
  arbitrary prose into build-to-patch links is enrichment and remains deferred.
- **Output completeness backfill:** old records sometimes lack structured output
  even when display text exists. This slice reports unknown. Recovering original
  data would require producer-specific ingestion changes and source availability;
  it cannot be repaired truthfully by the response serializer.
- **Cross-turn command linkage:** this slice reports the exact recorded results
  in the indexed turn. A session-wide call index could expand linkage later, with
  explicit handling of reused IDs, streaming results and incomplete imports.
- **Artifact hosting/retention:** requires a separate ingestion, storage and access
  design. A recorded path does not establish that its target exists or is captured.
- **Generic test-result schemas, automatic build-to-patch matching, hardware
  conclusions, submission readiness and confidence scores:** remain outside this
  retrieval implementation. Digest verification proves content identity, not
  technical validity or completeness of the original capture.

The older [roadmap](search-api-roadmap.md) describes broader possible work. Its
repository probes, durable bundles and semantic reranking are not authorized by
this scope and were not added here.

## Validation

119 focused tests passed: configuration, snapshot construction, search and chunk
retrieval, evidence annotations, client behavior, HTTP integration, access tokens
and query budgets, plus project/repository and route-authorization regressions.
Shared-generation cases verify one inventory/copy across owners, independent
process reuse, joining a pending build, exact current-permission scopes,
revocation isolation, reassigned-session freshness errors, explicit fresh
captures, capacity reuse, fixed expiry and legacy handles. The HTTP tests exercise the reference client against a temporary
server, including search pagination, empty command output, source-result links,
turn/activity digest verification, coverage issue pagination, snapshot stability,
private-session exclusion and cursor/filter binding. Unit cases cover missing and
ambiguous result links, explicit truncation/unavailability, mixed representations,
retry deadlines, terminal errors, changed snapshots, corrupt digests and missing
activity pages. `git diff --check` also passed.
