from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from math import ceil
from typing import Any


USAGE_CONTEXT_BASELINE_TOKENS = 12_000

AGENT_ROLE_LABELS = {
    "default": "Agent",
    "worker": "Worker",
    "explorer": "Explorer",
}


def _event_value(event: sqlite3.Row | dict[str, Any] | object, key: str) -> Any:
    if isinstance(event, sqlite3.Row):
        try:
            return event[key]
        except (IndexError, KeyError):
            return None
    if isinstance(event, dict):
        return event.get(key)
    return getattr(event, key, None)


def _value(source: sqlite3.Row | dict[str, Any] | object, key: str) -> Any:
    return _event_value(source, key)


def trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def coerce_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def parse_raw_meta_json(raw_meta_json: object) -> dict[str, Any]:
    if isinstance(raw_meta_json, dict):
        return raw_meta_json
    if not isinstance(raw_meta_json, str):
        return {}
    text = raw_meta_json.strip()
    if not text or text[0] != "{":
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def extract_agent_metadata(raw_meta: dict[str, Any] | None) -> dict[str, str | None]:
    payload = raw_meta if isinstance(raw_meta, dict) else {}
    return {
        "forked_from_id": trimmed(payload.get("forked_from_id")),
        "agent_nickname": trimmed(payload.get("agent_nickname")),
        "agent_role": trimmed(payload.get("agent_role")) or trimmed(payload.get("agent_type")),
        "agent_path": trimmed(payload.get("agent_path")),
        "memory_mode": trimmed(payload.get("memory_mode")),
    }


def agent_role_label(role: str | None) -> str:
    normalized = trimmed(role)
    if not normalized:
        return "Agent"
    return AGENT_ROLE_LABELS.get(normalized, normalized.replace("_", " ").title())


def default_usage_rollup() -> dict[str, Any]:
    return {
        "latest_usage_timestamp": None,
        "latest_input_tokens": 0,
        "latest_cached_input_tokens": 0,
        "latest_output_tokens": 0,
        "latest_reasoning_output_tokens": 0,
        "latest_total_tokens": 0,
        "latest_context_window": None,
        "latest_context_remaining_percent": None,
        "latest_primary_limit_used_percent": None,
        "latest_primary_limit_resets_at": None,
        "latest_secondary_limit_used_percent": None,
        "latest_secondary_limit_resets_at": None,
        "latest_rate_limit_name": None,
        "latest_rate_limit_reached_type": None,
    }


def _record_payload(event: sqlite3.Row | dict[str, Any] | object) -> dict[str, Any]:
    record_json = _event_value(event, "record_json")
    if not isinstance(record_json, str) or not record_json.strip():
        return {}
    try:
        parsed = json.loads(record_json)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    payload = parsed.get("payload")
    return payload if isinstance(payload, dict) else {}


def _timestamp_from_unix_seconds(value: object) -> str | None:
    if isinstance(value, bool):
        return None
    if not isinstance(value, (int, float)):
        return None
    return datetime.fromtimestamp(float(value), tz=UTC).replace(microsecond=0).isoformat()


def _context_remaining_percent(total_tokens: int, context_window: int | None) -> int | None:
    if context_window is None or context_window <= USAGE_CONTEXT_BASELINE_TOKENS:
        return None
    effective_window = context_window - USAGE_CONTEXT_BASELINE_TOKENS
    used = max(total_tokens - USAGE_CONTEXT_BASELINE_TOKENS, 0)
    remaining = max(effective_window - used, 0)
    return round((remaining / effective_window) * 100.0)


def compute_usage_rollup(
    events: list[sqlite3.Row | dict[str, Any] | object],
) -> dict[str, Any]:
    usage = default_usage_rollup()

    for event in events:
        if _event_value(event, "record_type") != "event_msg" or _event_value(event, "payload_type") != "token_count":
            continue
        payload = _record_payload(event)
        info = payload.get("info") if isinstance(payload.get("info"), dict) else {}
        total_usage = info.get("total_token_usage") if isinstance(info.get("total_token_usage"), dict) else {}
        rate_limits = payload.get("rate_limits") if isinstance(payload.get("rate_limits"), dict) else {}
        primary = rate_limits.get("primary") if isinstance(rate_limits.get("primary"), dict) else {}
        secondary = rate_limits.get("secondary") if isinstance(rate_limits.get("secondary"), dict) else {}

        timestamp = trimmed(_event_value(event, "timestamp"))
        if timestamp:
            usage["latest_usage_timestamp"] = timestamp

        input_tokens = coerce_int(total_usage.get("input_tokens"))
        cached_input_tokens = coerce_int(total_usage.get("cached_input_tokens"))
        output_tokens = coerce_int(total_usage.get("output_tokens"))
        reasoning_output_tokens = coerce_int(total_usage.get("reasoning_output_tokens"))
        total_tokens = coerce_int(total_usage.get("total_tokens"))
        context_window = coerce_int(info.get("model_context_window"))

        if input_tokens is not None:
            usage["latest_input_tokens"] = input_tokens
        if cached_input_tokens is not None:
            usage["latest_cached_input_tokens"] = cached_input_tokens
        if output_tokens is not None:
            usage["latest_output_tokens"] = output_tokens
        if reasoning_output_tokens is not None:
            usage["latest_reasoning_output_tokens"] = reasoning_output_tokens
        if total_tokens is not None:
            usage["latest_total_tokens"] = total_tokens
        if context_window is not None:
            usage["latest_context_window"] = context_window

        usage["latest_context_remaining_percent"] = _context_remaining_percent(
            int(usage["latest_total_tokens"] or 0),
            coerce_int(usage["latest_context_window"]),
        )

        usage["latest_rate_limit_name"] = trimmed(rate_limits.get("limit_name")) or usage["latest_rate_limit_name"]
        usage["latest_rate_limit_reached_type"] = (
            trimmed(rate_limits.get("rate_limit_reached_type")) or usage["latest_rate_limit_reached_type"]
        )

        primary_used = coerce_float(primary.get("used_percent"))
        secondary_used = coerce_float(secondary.get("used_percent"))
        if primary_used is not None:
            usage["latest_primary_limit_used_percent"] = primary_used
        if secondary_used is not None:
            usage["latest_secondary_limit_used_percent"] = secondary_used

        primary_resets_at = _timestamp_from_unix_seconds(primary.get("resets_at"))
        secondary_resets_at = _timestamp_from_unix_seconds(secondary.get("resets_at"))
        if primary_resets_at is not None:
            usage["latest_primary_limit_resets_at"] = primary_resets_at
        if secondary_resets_at is not None:
            usage["latest_secondary_limit_resets_at"] = secondary_resets_at

    return usage


def blended_total_tokens(source: sqlite3.Row | dict[str, Any] | object) -> int:
    input_tokens = int(_value(source, "latest_input_tokens") or 0)
    cached_input_tokens = int(_value(source, "latest_cached_input_tokens") or 0)
    output_tokens = int(_value(source, "latest_output_tokens") or 0)
    return max(input_tokens - cached_input_tokens, 0) + max(output_tokens, 0)


def usage_metrics(source: sqlite3.Row | dict[str, Any] | object) -> dict[str, Any]:
    return {
        "usage_timestamp": trimmed(_value(source, "latest_usage_timestamp")),
        "input_tokens": int(_value(source, "latest_input_tokens") or 0),
        "cached_input_tokens": int(_value(source, "latest_cached_input_tokens") or 0),
        "output_tokens": int(_value(source, "latest_output_tokens") or 0),
        "reasoning_output_tokens": int(_value(source, "latest_reasoning_output_tokens") or 0),
        "total_tokens": int(_value(source, "latest_total_tokens") or 0),
        "blended_total_tokens": blended_total_tokens(source),
        "context_window": coerce_int(_value(source, "latest_context_window")),
        "context_remaining_percent": coerce_int(_value(source, "latest_context_remaining_percent")),
        "primary_limit_used_percent": coerce_float(_value(source, "latest_primary_limit_used_percent")),
        "primary_limit_resets_at": trimmed(_value(source, "latest_primary_limit_resets_at")),
        "secondary_limit_used_percent": coerce_float(_value(source, "latest_secondary_limit_used_percent")),
        "secondary_limit_resets_at": trimmed(_value(source, "latest_secondary_limit_resets_at")),
        "rate_limit_name": trimmed(_value(source, "latest_rate_limit_name")),
        "rate_limit_reached_type": trimmed(_value(source, "latest_rate_limit_reached_type")),
    }


def humanize_rate_limit_reached_type(value: str | None) -> str:
    normalized = trimmed(value)
    if not normalized:
        return ""
    return normalized.replace("_", " ").title()


def usage_pressure_snapshot(source: sqlite3.Row | dict[str, Any] | object) -> dict[str, Any]:
    metrics = usage_metrics(source)
    primary_used = metrics["primary_limit_used_percent"]
    secondary_used = metrics["secondary_limit_used_percent"]
    context_remaining = metrics["context_remaining_percent"]
    reached_type = metrics["rate_limit_reached_type"]
    max_limit_used = max(
        [value for value in [primary_used, secondary_used] if value is not None],
        default=None,
    )

    score = 0
    tone = "stone"
    label = "Healthy"
    title = "No usage pressure detected"

    if reached_type:
        score = 100
        tone = "rose"
        label = "Rate Limited"
        title = humanize_rate_limit_reached_type(reached_type)
    elif context_remaining is not None and context_remaining <= 10:
        score = 92
        tone = "rose"
        label = "Near Cap"
        title = f"Context window is down to {context_remaining}% remaining"
    elif max_limit_used is not None and max_limit_used >= 95:
        score = 88
        tone = "rose"
        label = "Rate Window"
        title = f"Rate limit usage is at {ceil(max_limit_used)}%"
    elif context_remaining is not None and context_remaining <= 25:
        score = 72
        tone = "amber"
        label = "High Usage"
        title = f"Context window is down to {context_remaining}% remaining"
    elif max_limit_used is not None and max_limit_used >= 80:
        score = 68
        tone = "amber"
        label = "High Usage"
        title = f"Rate limit usage is at {ceil(max_limit_used)}%"

    detail_parts: list[str] = []
    if context_remaining is not None:
        detail_parts.append(f"{context_remaining}% context left")
    elif metrics["context_window"] is not None:
        detail_parts.append(f"{metrics['context_window']:,} ctx")
    if primary_used is not None:
        detail_parts.append(f"primary {ceil(primary_used)}%")
    if secondary_used is not None:
        detail_parts.append(f"secondary {ceil(secondary_used)}%")
    if reached_type and not detail_parts:
        detail_parts.append(humanize_rate_limit_reached_type(reached_type))

    badges: list[dict[str, str]] = []
    if reached_type:
        badges.append({"tone": "rose", "label": humanize_rate_limit_reached_type(reached_type)})
    if context_remaining is not None:
        badges.append(
            {
                "tone": "rose" if context_remaining <= 10 else ("amber" if context_remaining <= 25 else "stone"),
                "label": f"Context {context_remaining}%",
            }
        )
    if primary_used is not None:
        badges.append(
            {
                "tone": "rose" if primary_used >= 95 else ("amber" if primary_used >= 80 else "stone"),
                "label": f"Primary {ceil(primary_used)}%",
            }
        )
    if secondary_used is not None:
        badges.append(
            {
                "tone": "rose" if secondary_used >= 95 else ("amber" if secondary_used >= 80 else "stone"),
                "label": f"Secondary {ceil(secondary_used)}%",
            }
        )

    metrics.update(
        {
            "score": score,
            "status_tone": tone,
            "status_label": label,
            "status_title": title,
            "summary": " · ".join(detail_parts),
            "has_pressure": score > 0,
            "badges": badges,
        }
    )
    return metrics


def session_agent_snapshot(source: sqlite3.Row | dict[str, Any] | object) -> dict[str, Any]:
    role = trimmed(_value(source, "agent_role"))
    nickname = trimmed(_value(source, "agent_nickname"))
    path = trimmed(_value(source, "agent_path"))
    forked_from_id = trimmed(_value(source, "forked_from_id"))
    memory_mode = trimmed(_value(source, "memory_mode"))
    has_agent = any([role, nickname, path, forked_from_id, memory_mode])
    return {
        "has_agent": has_agent,
        "agent_role": role,
        "agent_role_label": agent_role_label(role),
        "agent_nickname": nickname,
        "agent_path": path,
        "forked_from_id": forked_from_id,
        "memory_mode": memory_mode,
    }
