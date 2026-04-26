from __future__ import annotations

from datetime import UTC, datetime
from io import BytesIO
import json
import sqlite3
from typing import Any
import zipfile

from .session_insights import parse_raw_meta_json


SESSION_META_EXPORT_KEYS = (
    "cwd",
    "originator",
    "cli_version",
    "source",
    "model_provider",
    "base_instructions",
    "dynamic_tools",
    "memory_mode",
    "forked_from_id",
    "agent_nickname",
    "agent_role",
    "agent_path",
)

EXECUTION_CONTEXT_EVENT_TYPES = {
    ("turn_context", None): "turn_contexts",
    ("event_msg", "session_configured"): "session_configured_events",
    ("event_msg", "mcp_startup_complete"): "mcp_startup_events",
    ("event_msg", "guardian_assessment"): "guardian_assessments",
    ("event_msg", "request_permissions"): "permission_requests",
    ("event_msg", "request_user_input"): "user_input_requests",
    ("event_msg", "model_reroute"): "model_reroutes",
}


def _value(source: sqlite3.Row | dict[str, Any], key: str) -> Any:
    if isinstance(source, sqlite3.Row):
        try:
            return source[key]
        except (IndexError, KeyError):
            return None
    return source.get(key)


def _record_payload(event: sqlite3.Row | dict[str, Any]) -> dict[str, Any] | None:
    record_json = _value(event, "record_json")
    if not isinstance(record_json, str) or not record_json.strip():
        return None
    try:
        parsed = json.loads(record_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    payload = parsed.get("payload")
    if isinstance(payload, dict):
        return payload
    return parsed


def _session_meta_export(session: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    raw_meta = parse_raw_meta_json(_value(session, "raw_meta_json"))
    exported = {
        key: raw_meta[key]
        for key in SESSION_META_EXPORT_KEYS
        if key in raw_meta
    }
    for key in (
        "cwd",
        "originator",
        "cli_version",
        "source",
        "model_provider",
        "memory_mode",
        "forked_from_id",
        "agent_nickname",
        "agent_role",
        "agent_path",
    ):
        if key in exported:
            continue
        value = _value(session, key)
        if value not in {None, ""}:
            exported[key] = value
    return exported


def _event_export_entry(event: sqlite3.Row | dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "event_index": _value(event, "event_index"),
        "timestamp": _value(event, "timestamp"),
        "payload": payload,
    }
    call_id = _value(event, "call_id")
    if isinstance(call_id, str) and call_id.strip():
        entry["call_id"] = call_id
    return entry


def build_execution_context_export(
    session: sqlite3.Row | dict[str, Any],
    events: list[sqlite3.Row | dict[str, Any]],
) -> dict[str, Any]:
    categorized: dict[str, list[dict[str, Any]]] = {
        export_key: []
        for export_key in EXECUTION_CONTEXT_EVENT_TYPES.values()
    }
    for event in events:
        record_type = str(_value(event, "record_type") or "")
        payload_type = _value(event, "payload_type")
        normalized_payload_type = payload_type if isinstance(payload_type, str) and payload_type.strip() else None
        export_key = EXECUTION_CONTEXT_EVENT_TYPES.get((record_type, normalized_payload_type))
        if export_key is None:
            continue
        payload = _record_payload(event)
        if payload is None:
            continue
        categorized[export_key].append(_event_export_entry(event, payload))

    return {
        "session_meta": _session_meta_export(session),
        "latest_turn_context": (
            categorized["turn_contexts"][-1]["payload"]
            if categorized["turn_contexts"]
            else None
        ),
        "latest_session_configured": (
            categorized["session_configured_events"][-1]["payload"]
            if categorized["session_configured_events"]
            else None
        ),
        "counts": {
            key: len(items)
            for key, items in categorized.items()
            if items
        },
        **categorized,
    }


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def export_json_payload(
    session: sqlite3.Row | dict[str, Any],
    events: list[sqlite3.Row | dict[str, Any]],
) -> dict[str, Any]:
    return {
        "session": dict(session),
        "execution_context": build_execution_context_export(session, events),
        "events": [dict(event) for event in events],
    }


def build_session_bundle(
    session: sqlite3.Row | dict[str, Any],
    events: list[sqlite3.Row | dict[str, Any]],
    *,
    raw_jsonl: str,
    raw_export_info: dict[str, Any],
) -> bytes:
    session_payload = dict(session)
    events_payload = [dict(event) for event in events]
    execution_context = build_execution_context_export(session, events)
    metadata = {
        "bundle_version": 2,
        "exported_at": utc_now_iso(),
        "session_id": session_payload.get("id"),
        "source_host": session_payload.get("source_host"),
        "summary": session_payload.get("summary"),
        "raw_export": raw_export_info,
    }

    output = BytesIO()
    with zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr(
            "metadata.json",
            json.dumps(metadata, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        )
        bundle.writestr(
            "session.json",
            json.dumps(session_payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        )
        bundle.writestr(
            "execution_context.json",
            json.dumps(execution_context, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        )
        bundle.writestr(
            "events.json",
            json.dumps(events_payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        )
        bundle.writestr("raw.jsonl", raw_jsonl)
    return output.getvalue()
