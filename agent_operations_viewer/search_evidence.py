"""Evidence annotations derived only from persisted events, never live files."""
from __future__ import annotations

import json
from urllib.parse import quote


RESULT_TYPES = {"function_call_output", "custom_tool_call_output", "exec_command_end"}


def event_id(session_id, index):
    return f"session:{quote(str(session_id), safe='')}:event:{index}"


def _decode(value):
    """Decode only known text envelopes; preserve the original value separately."""
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            return value, "text"
        if isinstance(parsed, (dict, list)):
            text, method = _decode(parsed)
            if text is not None:
                return text, "json/" + method
        return value, "text"
    if isinstance(value, dict):
        for key in ("output", "text"):
            if isinstance(value.get(key), str):
                return value[key], key
        if isinstance(value.get("content"), list):
            return _decode(value["content"])
        if isinstance(value.get("stdout"), str) and isinstance(value.get("stderr"), str):
            return value["stdout"] + value["stderr"], "stdout_then_stderr"
    if isinstance(value, list) and all(
        isinstance(item, dict) and item.get("type") in {"text", "output_text"}
        and isinstance(item.get("text"), str) for item in value
    ):
        return "\n".join(item["text"] for item in value), "text_blocks"
    return None, "unsupported"


def output_record(payload, payload_type):
    key = "aggregated_output" if payload_type == "exec_command_end" else "output"
    present = key in payload
    value = payload.get(key)
    decoded, method = _decode(value) if present else (None, "absent")
    status = "unknown"
    basis = "output_field_absent" if not present else "unsupported_or_null_representation"
    completeness = "unknown"
    # Only explicit producer flags are authoritative. Do not classify prose.
    if payload.get("output_available") is False:
        status, basis = "unavailable", "output_available_false"
    elif payload.get("output_truncated") is True or payload.get("truncated") is True:
        status, basis, completeness = "truncated", "producer_truncation_flag", "truncated"
    elif present and value is not None:
        # Empty decoded text can coexist with other structured content. Only an
        # explicitly empty stored output value establishes captured-empty.
        status = "captured_empty" if value == "" or value == [] else "captured"
        basis = "recorded_output_field"
        if payload.get("output_complete") is True:
            completeness = "complete"
    return {
        "availability": status, "basis": basis, "completeness": completeness,
        "representation_present": present, "representation": value,
        "decoded_text": decoded, "decoding": method,
    }


def annotate_turn(payload, rows, session_id):
    """Keep source records addressable even when the UI merges calls/results."""
    sources = []
    calls = {}
    results = {}
    for row in rows:
        row = dict(row)
        try:
            record = json.loads(row.get("record_json") or "{}")
        except (ValueError, TypeError):
            record = {}
        raw = record.get("payload", {}) if isinstance(record, dict) else {}
        raw = raw if isinstance(raw, dict) else {}
        kind, role = row.get("kind"), row.get("role")
        is_result = (
            row.get("payload_type") in RESULT_TYPES
            or kind == "tool_result"
            or row.get("payload_type") == "patch_apply_end"
        )
        provenance = (
            "user_message" if kind == "message" and role == "user" else
            "assistant_response" if kind == "message" and role == "assistant" else
            "tool_output" if is_result or kind == "tool_result" else
            "tool_call" if kind == "tool_call" else "other"
        )
        source = {key: row.get(key) for key in (
            "event_index", "timestamp", "record_type", "payload_type", "kind", "role",
            "tool_name", "call_id",
        )}
        source.update(event_id=event_id(session_id, row["event_index"]), provenance=provenance)
        if is_result:
            source["output"] = output_record(raw, row["payload_type"])
        # Only typed, already-recorded references: no extraction from prose and
        # no attempt to open, archive, or infer availability of referenced files.
        if row.get("payload_type") in {"image_generation_call", "image_generation_end"} and isinstance(raw.get("saved_path"), str):
            source["artifact_references"] = [{
                "path": raw["saved_path"], "source_field": "saved_path",
                "content_captured": "unknown",
            }]
        sources.append(source)
        call = row.get("call_id")
        if call and kind == "tool_call":
            calls.setdefault(call, []).append(source)
        if call and is_result:
            results.setdefault(call, []).append(source)

    by_index = {source["event_index"]: source for source in sources}
    for source in sources:
        call = source["call_id"]
        candidates = calls.get(call, [])
        linked = results.get(call, [])
        # Reused/missing call IDs cannot support an unambiguous association.
        unambiguous = len(candidates) == 1 and all(
            result["event_index"] > candidates[0]["event_index"] for result in linked
        )
        source["command_event_id"] = candidates[0]["event_id"] if unambiguous else None
        source["result_event_ids"] = [result["event_id"] for result in linked] if unambiguous else []
        source["linkage"] = "call_id" if unambiguous else "unknown"
        if source["kind"] == "tool_call":
            source["output_availability"] = (
                "missing" if unambiguous and not linked else "unknown"
            )

    for section in ("commands", "patches", "activity"):
        for item in payload[section]:
            source = by_index.get(item["event_index"])
            if source is None:
                continue
            for key in ("event_id", "provenance", "command_event_id", "result_event_ids", "linkage"):
                item[key] = source[key]
            if section == "commands":
                item["output_event_id"] = None
                item["output_completeness"] = "unknown"
                linked = [result for result in results.get(source["call_id"], [])
                          if result["event_id"] in source["result_event_ids"]]
                if not linked:
                    item["output_availability"] = "missing" if source["linkage"] == "call_id" else "unknown"
                else:
                    # Prefer the explicit command-end capture; retain every representation.
                    selected = next((r for r in linked if r["payload_type"] == "exec_command_end"), linked[0])
                    item["output_event_id"] = selected["event_id"]
                    item["output_completeness"] = selected["output"]["completeness"]
                    item["output_availability"] = selected["output"]["availability"]
                    if selected["output"]["decoded_text"] is not None:
                        item["output"] = selected["output"]["decoded_text"]
                item["output_availability_scope"] = "recorded_results_in_turn"
    payload["prompt"]["provenance"] = "user_message"
    payload["response"]["provenance"] = "assistant_response"
    payload["source_events"] = sources
