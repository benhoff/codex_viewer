# Search API Guide

The Agent Operations Viewer search API provides read-only, programmatic access to imported session turns. The hosted service is available at:

```text
https://codex.home.benhoff.net/
```

The API returns evidence from conversations; it does not generate or synthesize an answer.

## Quick Start

### 1. Create a personal token

1. Sign in to [codex.home.benhoff.net](https://codex.home.benhoff.net/).
2. Open **Settings → Search API**.
3. Give the token a label and select **Create token**.
4. Copy the token immediately. The full value is shown only once.

Search tokens begin with `csvr_read_` and have the `search:read` scope. A token has the same project access as its owner. It cannot upload sessions or perform administrative actions.

Keep the token out of source control and request URLs. Send it in the `Authorization` header. You can revoke it from the same Settings page.

### 2. Make a search request

Set the token in your environment, then call `GET /api/v1/search`:

```bash
export CODEX_SEARCH_TOKEN="csvr_read_REPLACE_ME"

curl --get "https://codex.home.benhoff.net/api/v1/search" \
  --header "Accept: application/json" \
  --header "Authorization: Bearer $CODEX_SEARCH_TOKEN" \
  --data-urlencode "q=authentication failure" \
  --data-urlencode "limit=20"
```

Use `--data-urlencode` (or an equivalent URL encoder) for every query parameter. This is especially important for spaces, timestamps, and cursor values.

## Request Reference

**Endpoint:** `GET https://codex.home.benhoff.net/api/v1/search`

**Authentication:** `Authorization: Bearer <personal-search-token>`

| Parameter | Required | Description |
| --- | --- | --- |
| `q` | Yes | Search text, from 1 to 500 characters after URL decoding. Whitespace-only queries are rejected. |
| `project_id` | No | Exact, stable project ID. This is not the project's display label or grouping key. |
| `repository_id` | No | Exact canonical repository ID returned by a hit or `GET /api/v1/projects`. Multiple project histories may share this ID without sharing ACLs. |
| `remote` | No | Git remote URL or normalized `host/path`. SSH and HTTPS forms resolve to the same non-local repository identity. |
| `root` | No | Exact normalized session working directory. Until repository-root ingestion is implemented, this is a working-directory filter rather than a guaranteed Git root. |
| `host` | No | Exact source-host name. |
| `from` | No | Inclusive lower timestamp bound in ISO 8601 format. A timestamp without an offset is treated as UTC. |
| `to` | No | Inclusive upper timestamp bound in ISO 8601 format. A timestamp without an offset is treated as UTC. |
| `mode` | No | `all` (default), `any`, `phrase`, or `exact`. See [Lexical modes and fields](#lexical-modes-and-fields). |
| `fields` | No | Comma-separated fields to search: `prompt`, `response`, `activity`, `commands`, `paths`, `commit_ids`, `tool_output`, and `patches`. Omit this parameter to search every indexed field plus project metadata. |
| `exclude_session_id` | No | Repeatable exact session ID exclusion; at most 100 entries, each 1–128 characters. Values are trimmed, deduplicated, and sorted. Exclusions apply to hits, facets, eligible coverage, and pagination. Batch queries use an array with this same name. |
| `snapshot_id` | No | Pin a previously returned searchable database generation. Omission creates a snapshot; a cursor implicitly selects its original snapshot. |
| `facets` | No | Comma-separated result counts to return: `project`, `session`, `date`, `branch`, and `matched_field`. Facets are computed before pagination. |
| `sort` | No | `relevance` (default), `time_asc` (oldest first), or `time_desc` (newest first). |
| `group_by` | No | `none` (default) returns flat hits. `session` returns session groups. |
| `max_hits_per_session` | No | Maximum hits returned in each session group, from 1 to 100. Defaults to 3. Supplying it explicitly requires `group_by=session`; otherwise the request returns `422`. |
| `limit` | No | Page size, from 1 to 100. Defaults to 20. It counts hits when ungrouped and sessions when grouped. |
| `cursor` | No | Opaque value returned as `next_cursor` by the preceding page. |

For example, restrict a search to a host and a UTC date range:

```bash
curl --get "https://codex.home.benhoff.net/api/v1/search" \
  --header "Accept: application/json" \
  --header "Authorization: Bearer $CODEX_SEARCH_TOKEN" \
  --data-urlencode "q=database migration" \
  --data-urlencode "host=workstation-01" \
  --data-urlencode "from=2026-08-01T00:00:00Z" \
  --data-urlencode "to=2026-08-31T23:59:59Z" \
  --data-urlencode "limit=50"
```

Filters are combined. Results are also limited automatically to projects the token owner may view. An inaccessible project produces no hits rather than exposing its content.

### Ordering and session grouping

Use chronological sorting when reconstruction order matters:

```bash
curl --get "https://codex.home.benhoff.net/api/v1/search" \
  --header "Accept: application/json" \
  --header "Authorization: Bearer $CODEX_SEARCH_TOKEN" \
  --data-urlencode "q=recovery experiment" \
  --data-urlencode "sort=time_asc"
```

Use session grouping to prevent a long session from filling a page and to keep related turns together:

```bash
curl --get "https://codex.home.benhoff.net/api/v1/search" \
  --header "Accept: application/json" \
  --header "Authorization: Bearer $CODEX_SEARCH_TOKEN" \
  --data-urlencode "q=recovery experiment" \
  --data-urlencode "sort=time_desc" \
  --data-urlencode "group_by=session" \
  --data-urlencode "max_hits_per_session=3" \
  --data-urlencode "limit=20"
```

For `sort=relevance`, ordinary keyword searches rank sessions and turns by their best lexical match. Project-history questions that express a latest/next-step intent retain their newest-first preference. With chronological sorting, session groups are ordered by their earliest matching turn for `time_asc` and their latest matching turn for `time_desc`; hits inside each group use the same direction.

## Search Behavior

The service uses lexical search across prompts, assistant responses, and recorded activity. It also indexes overlapping chunks of full event text, so a match can be found beyond the shorter excerpts stored for a turn. Punctuation is ignored, matching is case-insensitive, and word prefixes are supported.

Plain keyword searches work well:

```text
authentication failure
database migration rollback
pytest timeout
```

The API also recognizes a small set of project-history questions, including latest/next-step, remaining-issue, and resolved-issue requests:

```text
What was the last thing we were going to do on the hws project?
What issues remain on the hws project?
What was resolved in the hws repo?
```

For these questions, the service can resolve a project reference, relax the keyword match, or return recent project history as evidence. Inspect the response's `retrieval` object to see which behavior was used. Because this is lexical retrieval rather than an answer-generation API, the client is responsible for interpreting or summarizing the returned hits.

For deterministic project scoping, use `project_id`. To search the same repository across imported project identities or machines, use `repository_id` or `remote`. A hit's `project.id` and `repository.id` values can be reused in later requests. Repository identity never broadens project permissions: the token owner's project ACL is still applied to every matching session.

### Lexical modes and fields

The non-default modes and all field-filtered searches are deterministic. Unfiltered `all` searches retain the compatibility fallback described below:

- `all` first requires every normalized query term. Terms use prefix matching, preserving the original API behavior; an unfiltered unsuccessful search may use the existing relaxed retrieval stage.
- `any` requires at least one normalized query term.
- `phrase` requires the terms to be adjacent and ordered; the final term may be a prefix.
- `exact` requires an adjacent, ordered sequence of complete indexed tokens. Matching remains case-insensitive and punctuation-neutral because it uses the lexical index; it is not a byte-for-byte raw substring comparison.

For example, search exact hardware values only in commands and tool output:

```bash
curl --get "https://codex.home.benhoff.net/api/v1/search" \
  --header "Accept: application/json" \
  --header "Authorization: Bearer $CODEX_SEARCH_TOKEN" \
  --data-urlencode "q=VDONE 2073600" \
  --data-urlencode "mode=all" \
  --data-urlencode "fields=commands,tool_output" \
  --data-urlencode "facets=project,branch,matched_field"
```

The `commands` and `tool_output` fields have separate full-content chunk indexes. `paths` comes from parsed file changes, and `commit_ids` comes from repository state captured with the session. A turn found through both its compact row and overlapping full-content chunks is returned only once.

## Response

A successful request returns HTTP `200` with JSON. This representative response is shortened to one hit:

```json
{
  "query": "authentication failure",
  "filters": {
    "project_id": null,
    "repository_id": null,
    "remote": null,
    "root": null,
    "host": null,
    "from": null,
    "to": null,
    "fields": ["prompt", "response"]
  },
  "mode": "all",
  "sort": "relevance",
  "group_by": "none",
  "max_hits_per_session": 3,
  "retrieval": {
    "strategy": "strict",
    "mode": "all",
    "intent": "keyword",
    "time_focus": "any",
    "status_focus": null,
    "natural_language": false,
    "terms": ["authentication", "failure"],
    "project": {
      "hint": null,
      "resolution": "not_requested",
      "id": null,
      "key": null,
      "label": null
    },
    "stage_counts": {
      "strict": 3,
      "relaxed": null,
      "project_history": null
    },
    "index": {
      "mode": "hybrid_lexical",
      "chunk_version": 3
    }
  },
  "coverage": {
    "first_session_at": "2026-08-01T09:00:00Z",
    "last_session_at": "2026-08-23T13:00:00Z",
    "last_indexed_at": "2026-09-05T08:14:02Z",
    "sessions_total": 12,
    "sessions_indexed": 12,
    "turns_total": 184,
    "turns_indexed": 184,
    "pending_reindex_sessions": 0,
    "projects_searched": [
      {
        "id": "01JPROJECTID",
        "repository_id": "01JREPOSITORYID",
        "key": "acme/viewer",
        "label": "Agent Operations Viewer",
        "session_count": 12,
        "turn_count": 184,
        "sessions_indexed": 12,
        "pending_reindex_sessions": 0
      }
    ],
    "index_versions": {
      "turn": 6,
      "turn_search": 3,
      "search_chunk": 3
    },
    "freshness": {
      "state": "current",
      "indexed_at_known_sessions": 12,
      "indexed_at_unknown_sessions": 0
    }
  },
  "facets": {
    "project": [
      {
        "value": "acme/viewer",
        "label": "Agent Operations Viewer",
        "project_id": "01JPROJECTID",
        "count": 3
      }
    ],
    "matched_field": [
      {"value": "response", "label": "response", "count": 3}
    ]
  },
  "hits": [
    {
      "project": {
        "id": "01JPROJECTID",
        "key": "acme/viewer",
        "label": "Agent Operations Viewer",
        "host": "workstation-01"
      },
      "repository": {
        "id": "01JREPOSITORYID",
        "remote": "https://github.com/acme/viewer.git",
        "root": "/workspace/viewer",
        "branch": "feature/search-api",
        "head": "2d48e17abc123",
        "dirty": null
      },
      "session_id": "019-session-id",
      "turn_number": 12,
      "timestamp": "2026-08-23T13:00:00+00:00",
      "matched_field": "response",
      "match_source": "turn",
      "chunk": null,
      "snippet": "...the authentication failure was caused by...",
      "score": 7.42,
      "prompt_excerpt": "Find the login regression.",
      "response_excerpt": "The authentication failure was caused by...",
      "response_state": "final",
      "stats": {
        "commands": 8,
        "patches": 2,
        "failures": 1,
        "files_touched": 3
      },
      "signals": [],
      "links": {
        "turn": "/api/v1/sessions/019-session-id/turns/12",
        "conversation": "/sessions/019-session-id?turn=12",
        "audit": "/sessions/019-session-id?view=audit&turn=12&focus=1"
      }
    }
  ],
  "groups": [],
  "total_count": 3,
  "session_count": 2,
  "pagination": {
    "unit": "hit",
    "total_count": 3,
    "returned_count": 1
  },
  "limit": 20,
  "next_cursor": null
}
```

Important response fields:

- `mode` and `filters.fields` echo the normalized lexical contract. An empty `filters.fields` array means all indexed content fields plus project metadata were searched.
- `hits` contains the evidence returned for the current page.
- `groups` is empty for the default flat response. With `group_by=session`, `hits` is empty and `groups` contains the page of sessions. Each group includes `session_id`, the session's full `match_count`, `returned_hit_count`, and its capped `hits` list.
- `total_count` always counts all matching turns for the selected retrieval strategy and filters, before the per-session cap.
- `session_count` counts distinct sessions containing those matches.
- `pagination.unit` is `hit` for flat results and `session` for grouped results. `pagination.total_count` is the count in that unit, while `pagination.returned_count` describes the current page.
- `coverage` describes the visible, structurally filtered turn corpus that was eligible to be searched. It is independent of whether the query text produced a hit.
- `facets` contains only the requested facet families. Counts cover the complete matching result set after ACL and structural filters but before pagination.
- `next_cursor` is `null` on the final page.
- `repository.id` is the canonical repository identity. `remote`, `root`, `branch`, and `head` remain the provenance stored with that particular session. The current `root` value is the session working directory and may be below the actual repository root. `dirty` is currently `null` because ingestion does not yet capture dirty state.
- `snippet`, `prompt_excerpt`, and `response_excerpt` are plain text, not HTML.
- `score` is a relative lexical relevance score. Treat it as meaningful within a result set, not as a calibrated probability.
- `match_source` identifies a compact turn match (`turn`), full-content match (`chunk`), or project-history fallback (`history`).
- `chunk` is non-null for a full-content match and includes its field, chunk index, and source-text offsets.
- `links` are relative to `https://codex.home.benhoff.net`; `turn` retrieves complete API evidence, while `conversation` and `audit` open browser views.
- `signals` contains any viewer warning or status badges associated with the session.

A grouped response uses this shape (hit objects are abbreviated here):

```json
{
  "group_by": "session",
  "hits": [],
  "groups": [
    {
      "session_id": "019-session-id",
      "match_count": 7,
      "returned_hit_count": 3,
      "hits": [
        {"session_id": "019-session-id", "turn_number": 12},
        {"session_id": "019-session-id", "turn_number": 9},
        {"session_id": "019-session-id", "turn_number": 4}
      ]
    }
  ],
  "total_count": 31,
  "session_count": 8,
  "pagination": {
    "unit": "session",
    "total_count": 8,
    "returned_count": 1
  }
}
```

The `retrieval.strategy` value is one of:

- `strict`: all normalized query terms matched.
- `relaxed`: the service broadened an abstract or unsuccessful query.
- `project_history`: the service returned recent turns from a resolved project as evidence.
- `no_match`: no usable match or unambiguous accessible project was found.

When `fields` is present, `all` remains strict and the natural-language relaxation stage is disabled. This prevents a field-specific evidence query from silently broadening from all terms to any term.

## Coverage and Freshness

Coverage uses the same personal-token ACL, `project_id`, `repository_id`, `remote`, `root`, `host`, `from`, and `to` restrictions as the search itself, but does not apply the text query. Private projects that the token owner cannot access are absent from every range, count, and `projects_searched` entry.

The counts have deliberately distinct meanings:

- `sessions_total` and `turns_total` describe the complete eligible corpus. Only sessions containing an in-scope normalized turn are included.
- `sessions_indexed` and `turns_indexed` count the portion whose turn, compact-search, and full-content chunk index versions are all current.
- `pending_reindex_sessions` identifies incomplete index coverage. A zero value is required before treating an unsuccessful query as evidence that the entire eligible corpus was searched.
- `last_indexed_at` is the latest persisted completion time from a fully current index. It is never inferred from session import or request time.
- `first_session_at` and `last_session_at` bound the sessions containing eligible turns. Values are normalized to UTC.
- Each `projects_searched` item repeats its eligible and indexed counts, making the exact ACL-safe project scope inspectable.

`coverage.freshness.state` is one of:

- `current`: all eligible sessions use current index versions and have persisted completion timestamps.
- `pending_reindex`: one or more eligible sessions need an index rebuild.
- `timestamp_unknown`: index versions are current, but at least one historical session predates persisted completion timestamps. Its content was searched, but its exact indexing time is unknown.
- `empty`: no normalized turns exist under the structural filters and ACL.
- `unresolved_scope`: a natural-language project reference was inaccessible, unmatched, or ambiguous, so the service intentionally searched no broader corpus.

For legacy installations, the schema migration adds a nullable `search_indexed_at` field. Existing values remain unknown until a real indexing operation completes; deployment does not manufacture historical completion times.

## Project and Repository Discovery

Use `GET /api/v1/projects` to discover stable project and repository IDs under the token owner's ACL:

```bash
curl --get "https://codex.home.benhoff.net/api/v1/projects" \
  --header "Accept: application/json" \
  --header "Authorization: Bearer $CODEX_SEARCH_TOKEN" \
  --data-urlencode "remote=git@github.com:acme/viewer.git" \
  --data-urlencode "limit=50"
```

The optional filters are `repository_id`, `remote`, `root`, and `host`. Filters are combined. `limit` ranges from 1 to 100 and defaults to 50; follow `next_cursor` to retrieve another page. `snapshot_id` pins discovery to the same corpus as subsequent searches.

`session_count` is the distinct count of non-ignored sessions assigned to that exact project, regardless of the discovery filters used to select the project. `repository_session_count` counts distinct visible sessions across its canonical repository. Each session belongs to at most one project (the source-key mapping is unique); `project_membership_exclusive=true` makes this explicit. Source counts have `count_scope=project_source` and partition their project's sessions by source key, host, root, captured remote, and repository. Counts never aggregate inaccessible projects.

Project, source, repository, and coverage first/last timestamps are the minimum/maximum UTC instants of each session's first nonempty `session_timestamp`, `started_at`, or `imported_at`, rendered to second precision. Repository aggregates use `repository_first_session_at` and `repository_last_session_at`. Search time filters operate on turn timestamps and can therefore narrow eligible coverage further.

Each project contains its project ID, label, visibility, canonical `repository_id`, time range, session count, source histories, and repository aliases. A repository discovered through SSH and HTTPS remotes uses one normalized remote identity. Histories without a non-local remote fall back to `(source host, normalized working directory)` and are never merged solely because they share a basename.

```json
{
  "projects": [
    {
      "id": "01JPROJECTID",
      "key": "github:acme/viewer",
      "label": "acme/viewer",
      "repository_id": "01JREPOSITORYID",
      "repository_ids": ["01JREPOSITORYID"],
      "repository": {
        "id": "01JREPOSITORYID",
        "kind": "remote",
        "host": "github.com",
        "path": "acme/viewer",
        "remote": "https://github.com/acme/viewer",
        "aliases": [
          {"type": "remote", "value": "github.com/acme/viewer"},
          {"type": "location", "host": "workstation-01", "root": "/workspace/viewer"}
        ]
      },
      "sources": [
        {
          "project_key": "github:acme/viewer",
          "host": "workstation-01",
          "root": "/workspace/viewer",
          "session_count": 12
        }
      ],
      "session_count": 12,
      "first_session_at": "2026-08-01T09:00:00Z",
      "last_session_at": "2026-08-23T13:00:00Z"
    }
  ],
  "total_count": 1,
  "limit": 50,
  "next_cursor": null
}
```

`repository_ids` contains every repository represented by the project's sessions. Normally it contains one value and `repository` contains the corresponding canonical record. If a manually grouped project contains conflicting repository evidence, `repository_id` and `repository` are `null` while `repository_ids` preserves the ambiguity.

Repository records that later gain stronger remote evidence are merged through a durable redirect. Clients may continue passing the older ID; the service resolves it to the current canonical ID.

## Batch Search

Use `POST /api/v1/search/batch` to run related searches against one corpus and index snapshot with one current authorization check. The endpoint accepts 1 to 20 query objects. Each object supports the same query, filters, mode, fields, facets, ordering, grouping, exclusions, and limit options as GET, except cursors. Supply `snapshot_id` at the top level to reuse a snapshot; per-query snapshots are rejected. Batch results contain the first page and a usable `next_cursor` when more matches exist. Continue through GET using the same query parameters and that cursor. Every result carries the batch's snapshot identifier.

```bash
curl "https://codex.home.benhoff.net/api/v1/search/batch" \
  --header "Accept: application/json" \
  --header "Content-Type: application/json" \
  --header "Authorization: Bearer $CODEX_SEARCH_TOKEN" \
  --data '{
    "queries": [
      {
        "id": "done-register",
        "q": "VDONE",
        "mode": "exact",
        "fields": ["commands", "tool_output"],
        "facets": ["branch", "matched_field"],
        "limit": 20
      },
      {
        "id": "frame-size",
        "q": "2072576 2073600",
        "mode": "any",
        "fields": ["prompt", "response", "tool_output"],
        "sort": "time_asc",
        "limit": 20
      }
    ],
    "max_total_hits": 40
  }'
```

The response preserves each optional `id` and embeds the normal search response under each item in `results`:

```json
{
  "query_count": 2,
  "returned_hit_count": 7,
  "max_total_hits": 40,
  "results": [
    {
      "id": "done-register",
      "query": "VDONE",
      "mode": "exact",
      "hits": [],
      "groups": [],
      "total_count": 0,
      "next_cursor": null
    }
  ]
}
```

`max_total_hits` ranges from 1 to 500 and limits the sum of potential returned hits. For grouped queries, the budget cost is `limit × max_hits_per_session`; otherwise it is `limit`. A request exceeding its declared budget returns `422` before any query runs.

## Retrieve a Complete Turn

Use a search hit's `session_id` and `turn_number` to retrieve the complete normalized turn:

```bash
curl --get \
  "https://codex.home.benhoff.net/api/v1/sessions/019-session-id/turns/12" \
  --header "Accept: application/json" \
  --header "Authorization: Bearer $CODEX_SEARCH_TOKEN" \
  --data-urlencode "context=2" \
  --data-urlencode "include=activity"
```

This endpoint uses the same personal search token and project ACLs as search. Its parameters are:

| Parameter | Required | Description |
| --- | --- | --- |
| `context` | No | Number of neighboring turns to return on each side, from 0 to 10. Defaults to 0. Context is clipped at the beginning and end of the session. |
| `include` | No | Comma-separated optional sections. The currently supported value is `activity`. |
| `snapshot_id` | No | Reuse a searchable corpus snapshot, including its captured events. |

Each item in `turns` contains:

- The complete normalized prompt and final response captured for that turn.
- Structured commands with command text, working directory, full captured output, exit status, duration, and parsed command metadata.
- Structured patches with patch text, affected files, status, and captured tool output.
- An aggregated file manifest, execution context, model information, and turn statistics.
- `activity` when requested, containing the normalized chronological detail events for the turn.
- `is_target`, which distinguishes the requested turn from its neighbors.

The top-level response also repeats the project and repository provenance and provides relative conversation and audit links. Missing or inaccessible sessions are both returned as HTTP `404`, so the endpoint does not reveal private project membership.

## Pagination

When `next_cursor` is not `null`, repeat the same request and add that value as `cursor`:

```bash
curl --get "https://codex.home.benhoff.net/api/v1/search" \
  --header "Accept: application/json" \
  --header "Authorization: Bearer $CODEX_SEARCH_TOKEN" \
  --data-urlencode "q=authentication failure" \
  --data-urlencode "limit=20" \
  --data-urlencode "cursor=PASTE_NEXT_CURSOR_HERE"
```

A search cursor is cryptographically signed and bound to the normalized `q`, filters, exclusions, `mode`, `fields`, `facets`, `sort`, `group_by`, `max_hits_per_session`, `limit`, snapshot, and snapshot owner's authorization scope. A project-discovery cursor binds its repository, remote, root, host, and limit filters. Repeat the applicable parameters on every page. The cursor implicitly reuses its snapshot; an explicitly different `snapshot_id` is rejected. Changing a bound parameter returns HTTP `400` with `cursor_mismatch`. Cursors are opaque implementation details; do not decode or construct them. Unsigned cursors from older API versions are invalid.

For flat requests, each page contains up to `limit` items in `hits`. For grouped requests, each page contains up to `limit` items in `groups`, and each group contains up to `max_hits_per_session` hits. Follow `next_cursor` in the same way for either response shape.

Here is a complete Python example that follows every page using only the standard library:

```python
import json
import os
from urllib.parse import urlencode
from urllib.request import Request, urlopen

endpoint = "https://codex.home.benhoff.net/api/v1/search"
token = os.environ["CODEX_SEARCH_TOKEN"]
params = {
    "q": "authentication failure",
    "limit": 50,
}

while True:
    request = Request(
        f"{endpoint}?{urlencode(params)}",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urlopen(request, timeout=30) as response:
        page = json.load(response)

    for hit in page["hits"]:
        print(hit["timestamp"], hit["project"]["label"], hit["snippet"])

    cursor = page["next_cursor"]
    if cursor is None:
        break
    params["cursor"] = cursor
```

## Errors and Troubleshooting

Errors are JSON objects with a `detail` field when the client sends `Accept: application/json`.

| Status | Meaning | What to check |
| --- | --- | --- |
| `400` | The cursor is malformed or does not belong to this query. | Start again without a cursor, or repeat the original query, filters, and limit. |
| `403` | Snapshot belongs to another user, or its access scope has shrunk. | Obtain a new snapshot using current authorization. |
| `409` | Invalid snapshot, unsupported snapshot schema/normalization, or indexed evidence cannot be reconstructed. | Inspect `detail.code`; do not silently substitute current evidence. |
| `410` | Snapshot expired or is unavailable. | Start a new investigation snapshot. |
| `401` | Authentication failed. | Confirm the bearer header uses an active personal search token. Sync/daemon tokens are not accepted. |
| `403` | The server is not ready for normal authenticated use. | An administrator may need to complete initial setup. |
| `422` | A parameter or batch budget failed validation. | Check `q`, repository filters, timestamps, the date range, `mode`, `fields`, `facets`, `sort`, `group_by`, limits, and `max_total_hits`. |
| `503` | Snapshot preparing, capacity reached, or build failed/interrupted. | Inspect `detail.code`. For `snapshot_building`, honor `Retry-After` and retry with `detail.snapshot_id` when present. |

## Reproducible research contract

All search endpoints reject unknown query parameters and body properties with `422`. Errors identify the offending name in `detail[].loc` and supply `allowed` names. For example, an extra field on the second batch query has location `["body", "queries", 1, "field"]`. Query parameters other than `exclude_session_id` cannot be repeated. Unsupported and explicitly inapplicable constraints never silently succeed.

Search responses expose `normalized_query`, including defaults, canonical exclusion order, effective search fields, and whether project metadata is searched. Discovery, batch, and evidence responses expose `normalized_request`. The returned `snapshot_id` identifies the effective snapshot even when it was supplied through a cursor. Request cursors themselves are transport state, not evidence filters.

### Snapshot lifetime and current access

Every discovery, search, batch, complete-turn, and activity response includes `snapshot_id` and `snapshot`. Metadata contains `created_at`, `expires_at`, `index_generation`, `normalization_version`, `index_versions`, and creation-time coverage/freshness. `snapshot.coverage_scope` explicitly identifies the whole authorized corpus before query filters. Use the search response's top-level `coverage` to assess the eligible corpus after repository, source, date, and exclusion filters.

A snapshot is an immutable SQLite backup, including indexed text and captured events. It survives process restarts, expires after 15 minutes, and can be reused across all endpoints. Live indexing never changes its content or ordering. Each batch creates or reuses exactly one backup. Snapshots are bound to the authenticated user; they do not freeze authorization grants. Every request checks current session/project visibility. Any revoked access, removed session, ignored source, or changed project assignment invalidates the old access scope with `403 snapshot_access_revoked`. Newly granted access does not broaden an existing snapshot. Invalid and expired snapshots never fall back to live data.

Large snapshots are prepared by one background builder per database, independently of the client connection. The initial request waits at most one second for preparation; if unfinished, it returns `503` with `detail.code=snapshot_building`, a signed `detail.snapshot_id`, and `Retry-After: 2`. Retry the same request with that ID (at the top level of a batch body). Other workers contending for creation return the same retryable code, potentially without an ID; they do not queue indefinitely. Valid existing snapshots and invalid identifiers never wait for the builder lock. Small snapshots still return an ordinary `200` on the first request. Preparation has a ten-minute work budget, leaving room for inventory/metadata after a large full-database copy; a failed or interrupted build returns an explicit `503` error and requires a new snapshot. This budget is not an expected completion time: production storage can spend approximately five minutes on the backup alone. New snapshots receive a separate 15-minute read lifetime from `ready_at`; `created_at` still records when preparation was requested. `expires_at` is authoritative. Polling and reading never renew pinned content or its lifetime. The same signed ID is retained throughout preparation, with an upper bound of preparation allowance plus read lifetime. Older snapshots retain their original expiration.

Build responses include progress when available: `detail.build_id` for log correlation, `stage` (`cleanup`, `backup`, `coverage_inventory`, `coverage`, `metadata`, or `publish`), `elapsed_seconds`, `stage_elapsed_seconds`, `stage_timings_seconds` for completed stages, and `budget_seconds`. Backup progress adds `backup_pages_copied` and `backup_pages_total`. Pending progress is a last-reported measurement, updated during backup and on stage transitions. A `snapshot_build_failed` response preserves these diagnostics and adds a sanitized `reason` (for example `deadline_exceeded`, `database_busy`, `disk_full`, or `sqlite_error`), `error_type`, and, for SQLite exceptions, `sqlite_errorname`. SQL, private paths, and raw exception messages are not returned. Server logs contain the exception and correlate stage timings by snapshot generation. Older failures created before this instrumentation may lack these fields.

Snapshot preparation records an indexed inventory of searchable turns and evidence availability, plus compact session/turn metadata tables. Filtered coverage reads use those compact tables instead of the wide captured records; unfiltered coverage can reuse the snapshot's authorized totals. Project discovery counts sessions directly and does not recompute full coverage for every project. Patch evidence retrieval uses the indexed chunk ID and shared full-text row ID, without scanning full-text metadata. A batch reuses turn digests across queries without retaining all reconstructed activity bodies.

After a ready snapshot is opened, each request has a shared 20-second cooperative work budget, including all queries in a batch. SQLite work is interrupted on expiry; Python evidence work checks the deadline at stage boundaries. Exceeding it returns `503` with `detail.code=query_timeout`, `stage`, `elapsed_seconds`, and `budget_seconds`. Narrow the scope, reduce hits, or split the batch before retrying. This is not a hard end-to-end wall-clock guarantee: authentication, worker queueing, snapshot preparation, blocking I/O, and final HTTP serialization are not preempted by this budget. Server logs report snapshot generation, outcome, and timings for authorization, coverage, retrieval counts, facets, page retrieval, and evidence processing without logging query contents. Cancellation on client disconnect is not guaranteed; the work budget bounds subsequent SQL work.

A current index can legitimately contain a session with no turns when its only captured message is an environment-context wrapper. Reindexing cannot invent missing conversation content. Such sessions remain visible in `sessions_without_turns` and keep the conservative exhaustive-readiness gate false; do not silently discard them to claim complete coverage.

Backups and their signing key live in the database directory's private `search-snapshots/` directory (0700; backup/key files 0600). At most 32 unexpired snapshots are retained per database; creation cleans up expired backups and returns `503 snapshot_capacity` if full. Reusing a snapshot avoids another full database copy. Operators should allow disk space for these backups; keep the signing key when restarting workers. Expired backups and abandoned preparation files are removed on subsequent creation, so an idle installation may retain expired files until its next research request. An index or normalization upgrade invalidates incompatible snapshots explicitly.

### Exhaustive readiness

`coverage.exhaustive_ready` is true only when `exhaustive_reasons` is empty. The gate checks `sessions_pending`, `sessions_stale`, `sessions_failed`, `turns_pending`, `turns_stale`, and `turns_failed`, plus missing turn/search/evidence rows, import warnings, unknown index timestamps, and unresolved scope. Discovery includes sessions that have no turn index; known rollup turns missing from the index are pending. Versions behind the current index with retained searchable content count as stale; versions behind without searchable content count as pending. This importer does not persist a separate per-session failed-index state: failed counts are zero, while unsuccessful indexing remains pending/stale and import warnings independently block readiness. The gate covers imported, discovered sessions, not upstream sessions the server has never received.

A snapshot can remain incomplete throughout its lifetime. Positive hits can still be useful. A no-match result supports an exhaustive claim only for its explicitly pinned, eligible corpus when this gate is true and the reported retrieval strategy searched the requested terms.

### Content identity and canonical serialization

Search hits, complete-turn records, and activity pages expose `content_digest`, `content_version`, `normalization_version`, and `activity_digest`. The turn digest covers the same complete normalized turn regardless of context or whether activity was requested. To reproduce it, retrieve with `include=activity`, take the target element of `turns`, and remove `is_target`, `content_digest`, `content_version`, `normalization_version`, and `activity_digest`. The remaining object is `T`. Hash the UTF-8 bytes of canonical JSON for `{"normalization_version":"evidence-1","turn":T}` using SHA-256 and prefix the hexadecimal result with `sha256:`.

Canonical JSON uses recursively sorted object keys, array order unchanged, no insignificant whitespace (`separators=(",", ":")`), Unicode characters unescaped (`ensure_ascii=False`), and finite JSON numbers serialized as by Python's `json.dumps(..., allow_nan=False)`. The included turn fields are `turn_number`, `turn_id`, `prompt`, `response`, `duration_seconds`, `agent`, `execution_context`, `commands`, `patches`, `files`, `stats`, and `activity`. The activity digest hashes `{"normalization_version":"evidence-1","activity":T.activity}` under the same rules. Activity is the merged normalized detail collection, not every upstream transport record. Links, coverage, snapshot identifiers, requested context, and retrieval timestamps are outside both scopes. `content_version` is the turn digest's hex suffix. Corrections change the digest; changes to normalization semantics require a new normalization version.

Complete-turn retrieval returns a weak ETag equal to the target digest for `context=0&include=activity`; other projections hash the ordered turn-digest list and selected includes. Activity ETags bind the collection digest, filters, limit, and page. Tags are weak because volatile snapshot/retrieval metadata is excluded from the evidence identity. `If-None-Match` supports a matching tag, a list of tags, or `*`, and returns `304` after authorization and snapshot validation. `X-Snapshot-ID` is also returned on `304`.

### Bounded activity retrieval

`GET /api/v1/sessions/{session_id}/turns/{turn_number}/activity` accepts:

| Parameter | Meaning |
| --- | --- |
| `limit` | 1–100 events per page, default 50. |
| `cursor`, `snapshot_id` | Signed continuation and reusable snapshot, as above. |
| `kind` | Exact normalized activity kind, such as `tool_call`. |
| `event_type` | Exact normalized `payload_type`, such as `function_call`. |
| `tool_name` | Exact normalized tool name, such as `apply_patch`. |
| `from_event_index`, `to_event_index` | Inclusive nonnegative event-index bounds. |
| `from`, `to` | Inclusive ISO timestamp bounds; missing offsets mean UTC. Undated events do not match time bounds. |

The response includes `activity`, filtered `total_count`, `returned_count`, `next_cursor`, `first_event_index`, `last_event_index`, snapshot metadata, and both content identities. Activity sorts by UTC timestamp (undated events first), event index, then immutable position in the captured normalized collection. Each returned event has a stable `activity_id` and its original `activity_ordinal`. To verify the collection digest from all unfiltered pages, restore ordinal order and remove these two pagination annotations before hashing. Cursors bind the session, turn, every filter, page size, snapshot, and authorization scope. Complete-turn `include=activity` remains available for compatibility.

### Patch-only search

`fields=patches` uses a separate full-content chunk index. It indexes submitted `apply_patch` bodies (including patches embedded in recognized shell invocations) and structured applied unified diffs. Headers, context, additions, and deletions are included; unrelated command text and patch status messages are excluded. Filenames are searchable in patch headers but patch body tokens are not attributed to `paths`. Matches report `matched_field=patches`, bounded character offsets, and `chunk.lines` ranges labeled `header`, `context`, `addition`, or `deletion` where available (`unknown` for a potentially partial leading line). All four lexical modes, facets, batches, snapshots, and pagination use the same ACL-scoped retrieval path. Chunk schema version 3 triggers background reindexing of existing sessions; coverage remains incomplete until that work finishes.

Common problems:

- **A request opens the sign-in page:** add `Accept: application/json` and verify the bearer token header. Browser session cookies are not a substitute for a programmatic search token.
- **A known session is missing:** confirm that the token owner can view the project, then verify project, repository, host, root, and date filters.
- **A project-history question returns no hits:** use an unambiguous project name or pass its exact `project_id`.
- **Only an excerpt is returned:** search returns evidence snippets by design. Pass the hit's `session_id` and `turn_number` to the complete-turn endpoint, or follow `links.conversation` in a browser.

Successful responses include `Cache-Control: private, no-store`; clients and shared proxies should not cache them.
