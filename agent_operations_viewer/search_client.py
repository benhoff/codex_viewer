"""Standard-library reference client for pinned, verified evidence retrieval."""
from __future__ import annotations

import hashlib
import json
import time
from urllib.error import HTTPError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class SearchClientError(RuntimeError):
    def __init__(self, code, status=None):
        self.code, self.status = code, status
        super().__init__(f"Search API error: {code}" + (f" (HTTP {status})" if status else ""))


def content_digest(value):
    serialized = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return "sha256:" + hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def verify_turn(turn, expected_digest=None):
    version = turn["normalization_version"]
    if version not in {"evidence-1", "evidence-2"}:
        raise SearchClientError("unsupported_normalization")
    if "activity" not in turn:
        raise SearchClientError("activity_required_for_verification")
    omitted = {"is_target", "content_digest", "content_version", "normalization_version", "activity_digest"}
    canonical = {key: value for key, value in turn.items() if key not in omitted}
    actual = content_digest({"normalization_version": version, "turn": canonical})
    activity = content_digest({"normalization_version": version, "activity": turn["activity"]})
    if actual != turn["content_digest"] or activity != turn["activity_digest"] or turn["content_version"] != actual[7:]:
        raise SearchClientError("digest_mismatch")
    if expected_digest is not None and actual != expected_digest:
        raise SearchClientError("search_hit_digest_mismatch")
    return turn


class _NoRedirects(HTTPRedirectHandler):
    # Do not forward bearer credentials to a different host or a login page.
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class SearchClient:
    def __init__(self, base_url, token, *, preparation_timeout=660, request_timeout=30):
        parts = urlsplit(base_url)
        if parts.scheme not in {"http", "https"} or not parts.netloc or parts.username or parts.password or parts.query or parts.fragment:
            raise ValueError("base_url must be an HTTP(S) origin or base path without credentials")
        if preparation_timeout <= 0 or request_timeout <= 0:
            raise ValueError("Timeouts must be positive")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.preparation_timeout = preparation_timeout
        self.request_timeout = request_timeout
        self.snapshot_id = None
        self._opener = build_opener(_NoRedirects())

    def get(self, path, params=None):
        if not path.startswith("/api/v1/") or "?" in path or "#" in path:
            raise ValueError("Use an API path and separate query parameters")
        params = dict(params or {})
        if not self.snapshot_id and params.get("snapshot_id"):
            self.snapshot_id = params["snapshot_id"]
        if self.snapshot_id:
            if params.get("snapshot_id", self.snapshot_id) != self.snapshot_id:
                raise SearchClientError("snapshot_changed")
            params["snapshot_id"] = self.snapshot_id
        deadline = time.monotonic() + self.preparation_timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SearchClientError("client_preparation_timeout")
            request = Request(self.base_url + path + "?" + urlencode(params, doseq=True), headers={
                "Accept": "application/json", "Authorization": "Bearer " + self.token,
            })
            try:
                with self._opener.open(request, timeout=min(self.request_timeout, remaining)) as response:
                    payload = json.load(response)
            except HTTPError as exc:
                try:
                    error = json.load(exc)
                except (ValueError, TypeError):
                    raise SearchClientError("invalid_error_response", exc.code) from None
                finally:
                    exc.close()
                detail = error.get("detail", {})
                code = detail.get("code", "request_failed") if isinstance(detail, dict) else "invalid_input"
                if exc.code != 503 or code != "snapshot_building":
                    # Failed/expired snapshots require a new investigation, never a silent replacement.
                    raise SearchClientError(code, exc.code) from None
                pending = detail.get("snapshot_id")
                if pending:
                    if self.snapshot_id and pending != self.snapshot_id:
                        raise SearchClientError("snapshot_changed")
                    self.snapshot_id = params["snapshot_id"] = pending
                try:
                    delay = max(0.1, float(exc.headers.get("Retry-After", detail.get("retry_after", 2))))
                except (ValueError, TypeError):
                    delay = 2
                if delay >= deadline - time.monotonic():
                    raise SearchClientError("client_preparation_timeout")
                time.sleep(delay)
                continue
            snapshot = payload.get("snapshot_id")
            if not snapshot or (self.snapshot_id and snapshot != self.snapshot_id):
                raise SearchClientError("snapshot_changed")
            self.snapshot_id = snapshot
            return payload

    def pages(self, path, params=None):
        params = dict(params or {})
        seen = set()
        while True:
            page = self.get(path, params)
            yield page
            cursor = page.get("next_cursor")
            if cursor is None:
                return
            if cursor in seen:
                raise SearchClientError("repeated_cursor")
            seen.add(cursor)
            params["cursor"] = cursor

    def search(self, query, **filters):
        return self.pages("/api/v1/search", {"q": query, **filters})

    def coverage(self, **filters):
        return self.pages("/api/v1/search/coverage", filters)

    def turn(self, session_id, turn_number, *, expected_digest=None):
        path = f"/api/v1/sessions/{quote(session_id, safe='')}/turns/{int(turn_number)}"
        payload = self.get(path, {"include": "activity", "context": 0})
        target = next((turn for turn in payload["turns"] if turn.get("is_target")), None)
        if target is None or target["turn_number"] != turn_number or payload["session_id"] != session_id:
            raise SearchClientError("unexpected_turn")
        verify_turn(target, expected_digest)
        return payload

    def activity(self, session_id, turn_number, *, limit=50, expected_digest=None):
        path = f"/api/v1/sessions/{quote(session_id, safe='')}/turns/{int(turn_number)}/activity"
        events = {}
        identity = None
        for page in self.pages(path, {"limit": limit}):
            current = (page["normalization_version"], page["activity_digest"], page["total_count"])
            if identity is not None and identity != current:
                raise SearchClientError("activity_changed")
            identity = current
            for event in page["activity"]:
                ordinal = event["activity_ordinal"]
                if ordinal in events or not isinstance(ordinal, int) or ordinal < 0:
                    raise SearchClientError("invalid_activity_ordinal")
                if event["activity_id"] != content_digest({"turn": page["activity_digest"], "ordinal": ordinal}):
                    raise SearchClientError("activity_id_mismatch")
                events[ordinal] = {key: value for key, value in event.items() if key not in {"activity_id", "activity_ordinal"}}
        version, expected, total = identity
        if version not in {"evidence-1", "evidence-2"}:
            raise SearchClientError("unsupported_normalization")
        if sorted(events) != list(range(total)):
            raise SearchClientError("incomplete_activity")
        ordered = [events[index] for index in range(total)]
        actual = content_digest({"normalization_version": version, "activity": ordered})
        if actual != expected or (expected_digest is not None and actual != expected_digest):
            raise SearchClientError("activity_digest_mismatch")
        return ordered
