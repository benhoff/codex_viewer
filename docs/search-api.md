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
| `host` | No | Exact source-host name. |
| `from` | No | Inclusive lower timestamp bound in ISO 8601 format. A timestamp without an offset is treated as UTC. |
| `to` | No | Inclusive upper timestamp bound in ISO 8601 format. A timestamp without an offset is treated as UTC. |
| `sort` | No | `relevance` (default), `time_asc` (oldest first), or `time_desc` (newest first). |
| `group_by` | No | `none` (default) returns flat hits. `session` returns session groups. |
| `max_hits_per_session` | No | Maximum hits returned in each session group, from 1 to 100. Defaults to 3 and is ignored when `group_by=none`. |
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

For deterministic project scoping, use `project_id`. A hit's `project.id` value can be reused in later requests.

## Response

A successful request returns HTTP `200` with JSON. This representative response is shortened to one hit:

```json
{
  "query": "authentication failure",
  "filters": {
    "project_id": null,
    "host": null,
    "from": null,
    "to": null
  },
  "sort": "relevance",
  "group_by": "none",
  "max_hits_per_session": 3,
  "retrieval": {
    "strategy": "strict",
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
      "chunk_version": 1
    }
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

- `hits` contains the evidence returned for the current page.
- `groups` is empty for the default flat response. With `group_by=session`, `hits` is empty and `groups` contains the page of sessions. Each group includes `session_id`, the session's full `match_count`, `returned_hit_count`, and its capped `hits` list.
- `total_count` always counts all matching turns for the selected retrieval strategy and filters, before the per-session cap.
- `session_count` counts distinct sessions containing those matches.
- `pagination.unit` is `hit` for flat results and `session` for grouped results. `pagination.total_count` is the count in that unit, while `pagination.returned_count` describes the current page.
- `next_cursor` is `null` on the final page.
- `repository` records the remote, working directory, branch, and commit stored with the session. The current `root` value is the session working directory and may be below the actual repository root. `dirty` is currently `null` because ingestion does not yet capture dirty state.
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

A cursor is bound to the original `q`, filters, `sort`, `group_by`, `max_hits_per_session`, and `limit`. Repeat those parameters exactly on every page. Changing one of them while reusing the cursor returns HTTP `400`. Cursors are opaque implementation details; do not decode or construct them.

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
| `401` | Authentication failed. | Confirm the bearer header uses an active personal search token. Sync/daemon tokens are not accepted. |
| `403` | The server is not ready for normal authenticated use. | An administrator may need to complete initial setup. |
| `422` | A parameter failed validation. | Check `q`, timestamps, the date range, `sort`, `group_by`, `limit`, and `max_hits_per_session`. |

Common problems:

- **A request opens the sign-in page:** add `Accept: application/json` and verify the bearer token header. Browser session cookies are not a substitute for a programmatic search token.
- **A known session is missing:** confirm that the token owner can view the project, then verify `project_id`, `host`, and date filters.
- **A project-history question returns no hits:** use an unambiguous project name or pass its exact `project_id`.
- **Only an excerpt is returned:** search returns evidence snippets by design. Pass the hit's `session_id` and `turn_number` to the complete-turn endpoint, or follow `links.conversation` in a browser.

Successful responses include `Cache-Control: private, no-store`; clients and shared proxies should not cache them.
