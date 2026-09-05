# Search API Implementation Roadmap

This roadmap breaks the search API improvements into independently deployable slices. The ordering prioritizes evidence reconstruction and trustworthy negative results before adding broader retrieval techniques.

## Slice 1: Complete Evidence and Provenance (Implemented)

The first slice is implemented without a database migration:

- `GET /api/v1/sessions/{session_id}/turns/{turn_number}` returns the complete normalized prompt and response, structured commands, patches, files, execution context, and up to ten neighboring turns on each side.
- `include=activity` adds normalized chronological detail events.
- Every search hit and complete-turn response includes `repository.remote`, `root`, `branch`, `head`, and `dirty`.
- The existing `search:read` token and project ACL checks protect both endpoints.
- Inaccessible sessions return `404`, and successful responses use `Cache-Control: private, no-store`.

The current schema does not capture repository dirty state or a distinct repository-root field. Therefore `repository.dirty` is deliberately `null`, and `repository.root` currently contains the stored session working directory. Neither value should be inferred from the repository's later state.

## Slice 2: Ingestion-Time Repository State

Goal: make provenance complete and explicitly distinguish captured values from unavailable ones.

Schema and ingestion work:

1. Add nullable `sessions.git_root`, `sessions.git_dirty`, and `sessions.git_state_captured_at` columns.
2. Accept dirty state from native session metadata when the producer supplies it.
3. For local imports only, optionally probe `git status --porcelain` while importing and record when the probe occurred.
4. Carry the fields through raw sync payloads and session upserts.
5. Backfill existing rows as unknown, not clean.

API behavior:

- Return a distinct repository root rather than treating the session working directory as the root.
- Return `dirty: true|false` only for a captured value.
- Keep `dirty: null` for historical or unreachable repositories.
- Consider adding `captured_at` and `source` (`session_metadata` or `import_probe`) so clients can judge provenance quality.

## Slice 3: Result Ordering, Grouping, and Deduplication (Implemented)

Goal: make the final state of an experiment easy to distinguish from superseded intermediate work.

Request parameters:

```text
sort=relevance|time_asc|time_desc
group_by=none|session
max_hits_per_session=3
```

The route validates and passes these options into the search service, and cursors are bound to all three values. Flat searches paginate by hit; grouped searches paginate by session and apply `max_hits_per_session` inside each selected group. `total_count` remains the backward-compatible raw matching-turn count, while `session_count` reports the distinct group count. The `pagination` object states which unit is being paged.

Both relevance and chronological order are explicit in SQL. For grouped chronological results, `time_asc` orders sessions by their earliest matching turn and `time_desc` by their latest matching turn. Hits within a session use the requested direction. Each group reports both its full `match_count` and the number of hits returned after capping.

Page cursors remain page-number based. Replacing them with keyset cursors containing the last timestamp/rank, session ID, and turn number remains a future hardening step to prevent duplicate or skipped results if new sessions arrive between page requests.

Turn/chunk overlap is already deduplicated to one result per `(session_id, turn_number)` by the current candidate-ranking query. This slice should retain that behavior and add grouping across turns in the same session.

## Slice 4: Coverage and Freshness (Implemented)

Goal: let clients distinguish “no indexed evidence” from “the indexed corpus shows no match.”

Every search response now includes an ACL- and filter-aware `coverage` object:

```json
{
  "coverage": {
    "first_session_at": "2026-01-03T10:20:00Z",
    "last_session_at": "2026-09-05T07:10:00Z",
    "last_indexed_at": "2026-09-05T07:11:12Z",
    "sessions_total": 123,
    "sessions_indexed": 123,
    "turns_total": 2840,
    "turns_indexed": 2840,
    "pending_reindex_sessions": 0,
    "projects_searched": [
      {"id": "...", "label": "benhoff/hws"}
    ]
  }
}
```

The aggregate query uses the same project ACL, `project_id`, `host`, and date conditions as search without applying the text expression. It reports eligible and fully indexed session/turn counts, current index versions, per-project counts, and pending reindexes. Tests prove private and out-of-filter projects do not appear in counts, ranges, or `projects_searched`.

The schema now persists `sessions.search_indexed_at` only after the turn, compact-search, and full-content chunk index versions are all current. Existing rows migrate to `NULL`; the API reports `timestamp_unknown` rather than substituting import time. Future imports, incremental suffix indexing, project reindexing, and bounded chunk/turn backfills update the timestamp at actual completion.

## Slice 5: Canonical Repository Identity and Project Discovery (Implemented)

Goal: link histories such as `my-laptop/hws` and `github:benhoff/hws` without collapsing unrelated repositories that share a basename.

Implemented model:

- `projects.id` remains the access-control and presentation identity.
- Stable `repositories` records are separate from project grouping and survive source remapping.
- `repository_aliases` records normalized non-local remotes, `(host, root)` locations, and project-source keys.
- Projects and sessions link to a nullable `repository_id`; conflicting manually grouped projects remain explicitly ambiguous.

Identity rules:

1. Prefer a normalized non-local Git remote (`host` plus remote path).
2. Treat SSH and HTTPS forms of the same remote as aliases.
3. Use `(source_host, normalized_root)` only as a local fallback.
4. Never merge solely on repository basename.
5. Require an explicit manual merge when evidence is ambiguous.
6. Preserve per-project ACL enforcement even when multiple projects refer to one canonical repository.

`GET /api/v1/projects` supports `repository_id`, `remote`, `root`, and `host` filters and returns project IDs, repository IDs, known aliases, sources, time ranges, and session counts. Search and batch search accept `repository_id`, `remote`, and `root` alongside the existing project and host filters. All discovery, search, coverage, and pagination paths retain project-level ACL enforcement.

Remote IDs are derived from normalized remote identities, so SSH and HTTPS forms converge. Local fallbacks are scoped by source host and normalized working directory and never merge by basename. When later remote evidence supersedes a location fallback, the older repository record becomes a durable redirect to the canonical record.

The current location alias uses the session working directory because a captured Git root is not available yet. Slice 2 remains responsible for replacing that approximation with ingestion-time repository-root provenance.

## Slice 6: Search Modes, Fields, Batch Requests, and Facets (Implemented)

Goal: support exact experimental evidence without weakening deterministic lexical retrieval.

Implemented parameters:

```text
mode=all|any|phrase|exact
fields=prompt,response,activity,commands,paths,commit_ids,tool_output
facets=project,session,date,branch,matched_field
```

`all` remains the compatibility default. `any`, `phrase`, and `exact` generate escaped FTS expressions; `exact` means an exact sequence of complete lexical tokens, not a byte-for-byte substring. Explicit field filters disable automatic query relaxation. The derived turn index now separates prompt, response, generic activity, commands, parsed paths, captured commit IDs, and tool output, with full-content chunks for potentially long command and tool-output fields.

Facets are ACL- and filter-aware, computed across the deduplicated matching turns before pagination, and capped at 100 values per requested family. Turn/chunk and overlapping-chunk results remain deduplicated to one hit per turn.

`POST /api/v1/search/batch` accepts 1 to 20 bounded query objects using the GET schema without cursors. `max_total_hits` is capped at 500 and accounts for grouped per-session hit caps before any query executes. All batch queries run under the same search-token identity and project ACL snapshot.

A true raw-substring mode remains a possible future addition. It would need a deliberately slower scan or a separate n-gram index so punctuation and case can be preserved without changing the deterministic lexical contract.

Numeric values, SHAs, register names, filenames, and exact messages are strong reasons to keep lexical modes available even if semantic reranking is added later.

## Slice 7: Durable Evidence Export

Goal: make an investigation reproducible without depending on mutable web pages.

Add:

- A JSONL search export with one complete hit per line.
- An evidence bundle containing the original queries, filters, coverage snapshot, complete-turn payloads, and a manifest.
- Durable citations such as `session:{session_id}:turn:{turn_number}:event:{event_index}`.
- Content hashes for cited prompt, response, command output, and patch bodies.

The existing per-session JSON and ZIP export builders can be reused, but the evidence bundle must apply search-token ACLs and include only the requested visible turns.

## Slice 8: Optional Semantic Reranking

Goal: improve discovery for conceptual queries while keeping exact evidence retrieval predictable.

Semantic processing should rerank a bounded lexical candidate set rather than replace lexical retrieval. Responses should report both retrieval stages, the model/index version, and whether reranking was skipped. Exact, phrase, path, SHA, and numeric searches should be able to opt out explicitly.

## Suggested Delivery Order

| Order | Slice | Why |
| --- | --- | --- |
| 1 | Complete evidence and provenance | Removes the largest practical blocker with no migration. |
| 2 | Ingestion-time dirty state | Completes the provenance contract honestly. |
| 3 | Sorting and session grouping | Makes evolving experiments readable. |
| 4 | Coverage and freshness | Makes negative search conclusions defensible. |
| 5 | Canonical repository identity and projects API | Requires careful schema and ACL migration. |
| 6 | Modes, fields, batch, and facets | Builds on stable identity and aggregate-query primitives. |
| 7 | Durable evidence export | Packages the now-complete retrieval contracts. |
| 8 | Semantic reranking | Adds recall without compromising deterministic search. |
