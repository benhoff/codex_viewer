from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from .importer import (
    friendly_tool_title,
    parse_jsonish,
    summarize_tool_call_input,
)
from .session_status import (
    abort_display_label,
    is_assistant_final_message,
    is_assistant_update,
    is_task_complete,
    is_turn_aborted,
    is_user_turn_start,
    legacy_terminal_assistant_event,
)
from .text_utils import shorten, strip_codex_wrappers


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def humanize_timestamp(value: str | None) -> str:
    parsed = parse_timestamp(value)
    if parsed is None:
        return "Unknown activity"
    now = datetime.now(UTC)
    delta = now - parsed.astimezone(UTC)
    seconds = max(int(delta.total_seconds()), 0)
    if seconds < 60:
        return "Just now"
    if seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    if seconds < 86400:
        hours = seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = seconds // 86400
    if days == 1:
        return "Yesterday"
    if days < 7:
        return f"{days} days ago"
    local = parsed.astimezone()
    return local.strftime("%b %d, %Y").replace(" 0", " ")


def full_timestamp(value: str | None) -> str:
    parsed = parse_timestamp(value)
    if parsed is None:
        return value or "Unknown time"
    return parsed.astimezone().strftime("%b %d, %Y %I:%M %p %Z").replace(" 0", " ")


def kind_style(kind: str, role: str | None = None) -> str:
    if kind == "message" and role == "user":
        return "rose"
    if kind == "message" and role == "assistant":
        return "emerald"
    if kind == "tool_call":
        return "sky"
    if kind == "tool_result":
        return "cyan"
    if kind == "command":
        return "amber"
    if kind == "telemetry":
        return "slate"
    return "stone"


def event_preview_text(text: str, *, max_lines: int = 8, max_chars: int = 700) -> tuple[str, bool]:
    normalized = text.strip()
    if not normalized:
        return "", False

    lines = normalized.splitlines()
    truncated_by_lines = len(lines) > max_lines
    preview_lines = lines[:max_lines]
    preview = "\n".join(preview_lines).rstrip()

    truncated_by_chars = len(preview) > max_chars
    if truncated_by_chars:
        preview = preview[: max_chars - 1].rstrip()

    truncated = truncated_by_lines or truncated_by_chars or len(normalized) > len(preview)
    if truncated:
        preview = preview.rstrip() + "\n..."
    return preview, truncated


def styled_event(event: sqlite3.Row | dict[str, object]) -> dict[str, object]:
    row = dict(event)
    if str(row.get("kind") or "") == "tool_call":
        tool_name = row.get("tool_name")
        if not isinstance(tool_name, str) or not tool_name.strip():
            tool_name = row.get("title")
        row["title"] = friendly_tool_title(tool_name if isinstance(tool_name, str) else None)
        display_text, command_text = summarize_tool_call_input(
            tool_name if isinstance(tool_name, str) else None,
            row.get("display_text"),
        )
        if command_text and not row.get("command_text"):
            row["command_text"] = command_text
        if display_text:
            row["display_text"] = display_text
        if tool_name == "update_plan":
            parsed_plan = parse_update_plan_payload(row.get("detail_text") or row.get("display_text"))
            if parsed_plan is not None:
                row.update(parsed_plan)
    row["style"] = kind_style(str(row.get("kind") or ""), row.get("role"))
    display_text = str(row.get("display_text") or "")
    preview_text, preview_truncated = event_preview_text(display_text)
    row["preview_text"] = preview_text
    row["preview_truncated"] = preview_truncated
    return row


def parse_update_plan_payload(value: object) -> dict[str, object] | None:
    parsed = parse_jsonish(value)
    if not isinstance(parsed, dict):
        return None

    raw_steps = parsed.get("plan")
    if not isinstance(raw_steps, list):
        raw_steps = []

    steps: list[dict[str, str]] = []
    counts: dict[str, int] = {}
    for index, item in enumerate(raw_steps, start=1):
        if not isinstance(item, dict):
            continue
        step_text = str(item.get("step") or "").strip()
        if not step_text:
            continue
        status = str(item.get("status") or "").strip().lower() or "unknown"
        steps.append(
            {
                "index": str(index),
                "status": status,
                "status_label": status.replace("_", " "),
                "step": step_text,
            }
        )
        counts[status] = counts.get(status, 0) + 1

    explanation = parsed.get("explanation")
    explanation_text = explanation.strip() if isinstance(explanation, str) else ""

    summary_parts: list[str] = []
    if steps:
        summary_parts.append(f"{len(steps)} step{'s' if len(steps) != 1 else ''}")
    for status in ("in_progress", "completed", "pending"):
        count = counts.get(status)
        if count:
            summary_parts.append(f"{count} {status.replace('_', ' ')}")
    for status, count in sorted(counts.items()):
        if status not in {"in_progress", "completed", "pending"}:
            summary_parts.append(f"{count} {status.replace('_', ' ')}")

    return {
        "plan_update": True,
        "plan_steps": steps,
        "plan_explanation": explanation_text,
        "plan_counts": counts,
        "plan_summary": " | ".join(summary_parts),
    }


def summarize_patch_changes(detail_text: str) -> tuple[str, str]:
    parsed = parse_jsonish(detail_text)
    if not isinstance(parsed, dict) or not parsed:
        return "Patch applied", detail_text.strip()

    paths = sorted(str(path) for path in parsed.keys())
    basenames = [Path(path).name or path for path in paths]
    summary_lines = [f"Updated {len(paths)} file{'s' if len(paths) != 1 else ''}"]
    summary_lines.extend(basenames[:6])
    if len(paths) > 6:
        summary_lines.append(f"+{len(paths) - 6} more")
    return "\n".join(summary_lines), detail_text.strip()


def is_command_like_tool_call(event: dict[str, object]) -> bool:
    return (
        str(event.get("kind") or "") == "tool_call"
        and str(event.get("tool_name") or "") in {"exec_command", "write_stdin"}
        and bool(event.get("call_id"))
    )


def is_patch_tool_call(event: dict[str, object]) -> bool:
    return (
        str(event.get("kind") or "") == "tool_call"
        and str(event.get("tool_name") or "") == "apply_patch"
        and bool(event.get("call_id"))
    )


def is_plan_update_tool_call(event: dict[str, object]) -> bool:
    return (
        str(event.get("kind") or "") == "tool_call"
        and str(event.get("tool_name") or "") == "update_plan"
        and bool(event.get("call_id"))
    )


def merge_compound_tool_events(detail_events: list[dict[str, object]]) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    consumed: set[int] = set()

    for index, event in enumerate(detail_events):
        if index in consumed:
            continue

        if is_plan_update_tool_call(event):
            call_id = str(event.get("call_id") or "")
            tool_result_event: dict[str, object] | None = None
            tool_result_index: int | None = None

            for later_index in range(index + 1, len(detail_events)):
                if later_index in consumed:
                    continue
                candidate = detail_events[later_index]
                if str(candidate.get("call_id") or "") != call_id:
                    continue
                if str(candidate.get("kind") or "") == "tool_result":
                    tool_result_event = candidate
                    tool_result_index = later_index
                    break

            merged = dict(event)
            if tool_result_event is not None and tool_result_event.get("timestamp"):
                merged["timestamp"] = tool_result_event.get("timestamp")
            result.append(merged)
            consumed.add(index)
            if tool_result_index is not None:
                consumed.add(tool_result_index)
            continue

        if is_command_like_tool_call(event):
            call_id = str(event.get("call_id") or "")
            tool_name = str(event.get("tool_name") or "")
            command_event: dict[str, object] | None = None
            tool_result_event: dict[str, object] | None = None
            command_index: int | None = None
            tool_result_index: int | None = None

            for later_index in range(index + 1, len(detail_events)):
                if later_index in consumed:
                    continue
                candidate = detail_events[later_index]
                if str(candidate.get("call_id") or "") != call_id:
                    continue
                if tool_name == "exec_command" and command_event is None and str(candidate.get("kind") or "") == "command":
                    command_event = candidate
                    command_index = later_index
                    continue
                if tool_result_event is None and str(candidate.get("kind") or "") == "tool_result":
                    tool_result_event = candidate
                    tool_result_index = later_index
                    continue

            if command_event is None and tool_result_event is None:
                result.append(event)
                continue

            command_summary = str(event.get("display_text") or "").strip()
            command_text = (
                str(command_event.get("command_text") or "").strip()
                if command_event is not None
                else ""
            ) or str(event.get("command_text") or "").strip()

            output_text = ""
            if command_event is not None:
                output_candidate = str(command_event.get("detail_text") or "").strip()
                if output_candidate and output_candidate != str(command_event.get("display_text") or "").strip():
                    output_text = output_candidate
            if not output_text and tool_result_event is not None:
                output_candidate = str(tool_result_event.get("detail_text") or "").strip()
                if output_candidate and output_candidate != str(tool_result_event.get("display_text") or "").strip():
                    output_text = output_candidate

            display_text = command_summary or command_text or "Shell command"
            if output_text:
                display_text = f"{display_text}\n\nOutput:\n{output_text}"

            merged = dict(event)
            merged["kind"] = "command"
            merged["group_key"] = "command"
            merged["style"] = kind_style("command")
            merged["title"] = friendly_tool_title(tool_name)
            merged["command_text"] = command_text or None
            merged["exit_code"] = command_event.get("exit_code") if command_event is not None else None
            merged["timestamp"] = (
                command_event.get("timestamp")
                if command_event is not None and command_event.get("timestamp")
                else (
                    tool_result_event.get("timestamp")
                    if tool_result_event is not None and tool_result_event.get("timestamp")
                    else event.get("timestamp")
                )
            )
            merged["display_text"] = display_text.strip()
            merged["detail_text"] = (output_text or command_text or str(event.get("detail_text") or "")).strip()
            preview_text, preview_truncated = event_preview_text(str(merged["display_text"]))
            merged["preview_text"] = preview_text
            merged["preview_truncated"] = preview_truncated

            result.append(merged)
            consumed.add(index)
            if command_index is not None:
                consumed.add(command_index)
            if tool_result_index is not None:
                consumed.add(tool_result_index)
            continue

        if is_patch_tool_call(event):
            call_id = str(event.get("call_id") or "")
            patch_apply_event: dict[str, object] | None = None
            tool_result_event: dict[str, object] | None = None
            patch_apply_index: int | None = None
            tool_result_index: int | None = None

            for later_index in range(index + 1, len(detail_events)):
                if later_index in consumed:
                    continue
                candidate = detail_events[later_index]
                if str(candidate.get("call_id") or "") != call_id:
                    continue
                if (
                    patch_apply_event is None
                    and str(candidate.get("record_type") or "") == "event_msg"
                    and str(candidate.get("payload_type") or "") == "patch_apply_end"
                ):
                    patch_apply_event = candidate
                    patch_apply_index = later_index
                    continue
                if tool_result_event is None and str(candidate.get("kind") or "") == "tool_result":
                    tool_result_event = candidate
                    tool_result_index = later_index
                    continue

            if patch_apply_event is None and tool_result_event is None:
                result.append(event)
                continue

            detail_text = (
                str(patch_apply_event.get("detail_text") or "").strip()
                if patch_apply_event is not None
                else ""
            )
            display_text, merged_detail = summarize_patch_changes(detail_text)

            if tool_result_event is not None:
                tool_output = str(tool_result_event.get("detail_text") or "").strip()
                if tool_output and tool_output != str(tool_result_event.get("display_text") or "").strip():
                    merged_detail = f"{merged_detail}\n\nTool Output:\n{tool_output}".strip()

            merged = dict(event)
            merged["kind"] = "tool_call"
            merged["group_key"] = "patch"
            merged["style"] = kind_style("tool_call")
            merged["title"] = "Patch"
            merged["timestamp"] = (
                patch_apply_event.get("timestamp")
                if patch_apply_event is not None and patch_apply_event.get("timestamp")
                else (
                    tool_result_event.get("timestamp")
                    if tool_result_event is not None and tool_result_event.get("timestamp")
                    else event.get("timestamp")
                )
            )
            merged["display_text"] = display_text.strip()
            merged["detail_text"] = merged_detail.strip() or str(event.get("detail_text") or "").strip()
            preview_text, preview_truncated = event_preview_text(str(merged["display_text"]))
            merged["preview_text"] = preview_text
            merged["preview_truncated"] = preview_truncated

            result.append(merged)
            consumed.add(index)
            if patch_apply_index is not None:
                consumed.add(patch_apply_index)
            if tool_result_index is not None:
                consumed.add(tool_result_index)
            continue

        if not is_command_like_tool_call(event) and not is_patch_tool_call(event):
            result.append(event)

    return result


def work_entry_label(group_key: str, count: int) -> str:
    labels = {
        "patch": ("patch", "patches"),
        "command": ("command", "commands"),
        "tool_call": ("tool call", "tool calls"),
        "tool_result": ("tool result", "tool results"),
    }
    singular, plural = labels.get(group_key, ("event", "events"))
    label = singular if count == 1 else plural
    return f"{count} {label}"


def grouped_work_entries(detail_events: list[dict[str, object]]) -> list[dict[str, object]]:
    detail_events = merge_compound_tool_events(detail_events)
    groupable_keys = {"patch", "command"}
    entries: list[dict[str, object]] = []
    current_group_key: str | None = None
    current_events: list[dict[str, object]] = []

    def flush_group() -> None:
        nonlocal current_group_key, current_events
        if not current_events:
            return
        if len(current_events) == 1:
            entries.append({"entry_type": "event", "event": current_events[0]})
        else:
            entries.append(
                {
                    "entry_type": "group",
                    "kind": current_group_key,
                    "style": current_events[0]["style"],
                    "label": work_entry_label(str(current_group_key or ""), len(current_events)),
                    "count": len(current_events),
                    "events": list(current_events),
                    "timestamp_start": current_events[0]["timestamp"],
                    "timestamp_end": current_events[-1]["timestamp"],
                }
            )
        current_group_key = None
        current_events = []

    for event in detail_events:
        group_key = str(event.get("group_key") or event.get("kind") or "")
        if group_key in groupable_keys:
            if current_events and current_group_key == group_key:
                current_events.append(event)
                continue
            flush_group()
            current_group_key = group_key
            current_events = [event]
            continue

        flush_group()
        entries.append({"entry_type": "event", "event": event})

    flush_group()
    return entries


def build_turns(events: list[sqlite3.Row]) -> list[dict[str, object]]:
    prefer_event_msg = any(is_user_turn_start(row, True) for row in events)

    turns: list[dict[str, object]] = []
    current: dict[str, object] | None = None

    def finalize_turn(turn: dict[str, object]) -> dict[str, object]:
        assistant_messages: list[sqlite3.Row] = turn["assistant_messages"]  # type: ignore[assignment]
        assistant_updates: list[sqlite3.Row] = turn["assistant_updates"]  # type: ignore[assignment]
        completion_events: list[sqlite3.Row] = turn["completion_events"]  # type: ignore[assignment]
        aborted_events: list[sqlite3.Row] = turn["aborted_events"]  # type: ignore[assignment]
        all_events: list[sqlite3.Row] = turn["events"]  # type: ignore[assignment]

        completion_event = completion_events[-1] if completion_events else None
        final_response_event = None
        if completion_event is not None:
            completed_messages = [
                event
                for event in assistant_messages
                if event["event_index"] < completion_event["event_index"]
            ]
            final_response_event = completed_messages[-1] if completed_messages else completion_event
        elif all_events:
            legacy_event = legacy_terminal_assistant_event(all_events)
            if legacy_event is not None:
                final_response_event = legacy_event
        update_event = assistant_updates[-1] if assistant_updates else None
        abort_event = aborted_events[-1] if aborted_events else None

        response_state = "missing"
        response_label = "Turn Outcome"
        response_text = ""
        response_timestamp = None

        if completion_event is not None:
            if final_response_event is completion_event:
                response_text = str(completion_event["detail_text"] or completion_event["display_text"] or "")
            else:
                response_text = str(final_response_event["display_text"] or "")
            response_timestamp = completion_event["timestamp"] or final_response_event["timestamp"]
            response_state = "final"
            response_label = "Final Response"
        elif final_response_event is not None:
            response_text = str(final_response_event["display_text"] or "")
            response_timestamp = final_response_event["timestamp"]
            response_state = "final"
            response_label = "Final Response"
        elif abort_event is not None:
            response_text = abort_display_label(abort_event)
            response_timestamp = abort_event["timestamp"]
            response_state = "canceled"
        elif update_event is not None:
            final_response_event = update_event
            response_text = str(update_event["display_text"] or "")
            response_timestamp = update_event["timestamp"]
            response_state = "update"
            response_label = "Latest Update"

        prompt_text = str(turn["prompt_text"])
        prompt_excerpt = shorten(prompt_text, 220)
        response_excerpt = shorten(response_text, 280) if response_text else "No assistant response captured."

        detail_events: list[dict[str, object]] = []
        for event in all_events:
            skip = False
            if event["kind"] == "telemetry":
                skip = True
            if event["kind"] == "reasoning":
                skip = True
            if event["record_type"] == "turn_context":
                skip = True
            if event["record_type"] == "event_msg" and event["payload_type"] == "task_started":
                skip = True
            if event["record_type"] == "compacted":
                skip = True
            if event["kind"] == "message" and event["role"] == "user":
                skip = True
            if completion_event is not None and event["event_index"] == completion_event["event_index"]:
                skip = True
            if (
                final_response_event is not None
                and final_response_event is not completion_event
                and event["event_index"] == final_response_event["event_index"]
            ):
                skip = True
            if abort_event is not None and event["event_index"] == abort_event["event_index"]:
                skip = True
            if (
                event["kind"] == "message"
                and event["role"] == "assistant"
                and event["record_type"] == "response_item"
            ):
                skip = True
            if skip:
                continue
            detail_events.append(styled_event(event))

        detail_entries = grouped_work_entries(detail_events)

        return {
            "number": turn["number"],
            "prompt_text": prompt_text,
            "prompt_excerpt": prompt_excerpt,
            "prompt_timestamp": turn["prompt_timestamp"],
            "response_text": response_text,
            "response_excerpt": response_excerpt,
            "response_timestamp": response_timestamp,
            "response_state": response_state,
            "response_label": response_label,
            "detail_events": detail_events,
            "detail_entries": detail_entries,
            "work_count": len(detail_entries),
        }

    for event in events:
        if is_user_turn_start(event, prefer_event_msg):
            cleaned_prompt = strip_codex_wrappers(str(event["display_text"] or "")).strip()
            if not cleaned_prompt:
                continue
            if current is not None:
                turns.append(finalize_turn(current))
            current = {
                "number": len(turns) + 1,
                "prompt_text": cleaned_prompt,
                "prompt_timestamp": event["timestamp"],
                "events": [],
                "assistant_messages": [],
                "assistant_updates": [],
                "completion_events": [],
                "aborted_events": [],
            }
            continue

        if current is None:
            continue

        current["events"].append(event)
        if is_assistant_final_message(event):
            current["assistant_messages"].append(event)
        elif is_assistant_update(event):
            current["assistant_updates"].append(event)
        elif is_task_complete(event):
            current["completion_events"].append(event)
        elif is_turn_aborted(event):
            current["aborted_events"].append(event)

    if current is not None:
        turns.append(finalize_turn(current))

    return turns
