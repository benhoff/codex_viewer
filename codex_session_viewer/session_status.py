from __future__ import annotations

import sqlite3
from collections.abc import Sequence

from .text_utils import shorten, strip_codex_wrappers


USER_CANCELED_LABEL = "User canceled"
TURN_ABORTED_LABEL = "Turn aborted"


def prefers_event_msg_user_turns(
    events: Sequence[sqlite3.Row | dict[str, object]],
) -> bool:
    return any(
        event["kind"] == "message"
        and event["role"] == "user"
        and event["record_type"] == "event_msg"
        and event["payload_type"] == "user_message"
        for event in events
    )


def is_turn_aborted_wrapper_message(event: sqlite3.Row | dict[str, object]) -> bool:
    return (
        event["kind"] == "message"
        and event["role"] == "user"
        and event["record_type"] == "response_item"
        and event["payload_type"] == "message"
        and "<turn_aborted>" in str(event["display_text"] or "")
    )


def is_user_turn_start(
    event: sqlite3.Row | dict[str, object],
    prefer_event_msg: bool,
) -> bool:
    if event["kind"] != "message" or event["role"] != "user":
        return False
    if is_turn_aborted_wrapper_message(event):
        return False
    if prefer_event_msg:
        return event["record_type"] == "event_msg" and event["payload_type"] == "user_message"
    return event["record_type"] == "response_item" and event["payload_type"] == "message"


def is_assistant_final_message(event: sqlite3.Row | dict[str, object]) -> bool:
    return (
        event["kind"] == "message"
        and event["role"] == "assistant"
        and event["record_type"] == "response_item"
        and event["payload_type"] == "message"
    )


def is_assistant_update(event: sqlite3.Row | dict[str, object]) -> bool:
    return (
        event["kind"] == "message"
        and event["role"] == "assistant"
        and event["record_type"] == "event_msg"
        and event["payload_type"] == "agent_message"
    )


def is_task_complete(event: sqlite3.Row | dict[str, object]) -> bool:
    return event["record_type"] == "event_msg" and event["payload_type"] == "task_complete"


def is_turn_aborted(event: sqlite3.Row | dict[str, object]) -> bool:
    return event["record_type"] == "event_msg" and event["payload_type"] == "turn_aborted"


def abort_display_label(event: sqlite3.Row | dict[str, object]) -> str:
    reason = str(event["display_text"] or "").strip().lower()
    if reason == "interrupted":
        return USER_CANCELED_LABEL
    return TURN_ABORTED_LABEL


def is_legacy_terminal_noise(event: sqlite3.Row | dict[str, object]) -> bool:
    if is_turn_aborted_wrapper_message(event) or is_turn_aborted(event):
        return True
    if event["record_type"] == "turn_context":
        return True
    if event["kind"] in {"context", "telemetry", "reasoning", "system"}:
        return True
    return False


def legacy_terminal_assistant_event(
    events: Sequence[sqlite3.Row | dict[str, object]],
) -> sqlite3.Row | dict[str, object] | None:
    for event in reversed(events):
        if is_legacy_terminal_noise(event):
            continue
        if is_assistant_final_message(event) or is_assistant_update(event):
            return event
        return None
    return None


def legacy_terminal_assistant_summary(
    events: Sequence[sqlite3.Row | dict[str, object]],
) -> str | None:
    event = legacy_terminal_assistant_event(events)
    if event is None:
        return None
    cleaned = strip_codex_wrappers(str(event["display_text"] or "")).strip()
    if not cleaned:
        return None
    return shorten(cleaned, 220)


def terminal_turn_summary(events: Sequence[sqlite3.Row | dict[str, object]]) -> str | None:
    if not events:
        return None

    prefer_event_msg = prefers_event_msg_user_turns(events)
    turn_open = False
    saw_completion = False
    abort_event: sqlite3.Row | dict[str, object] | None = None
    current_turn_events: list[sqlite3.Row | dict[str, object]] = []

    for event in events:
        if is_user_turn_start(event, prefer_event_msg):
            turn_open = True
            saw_completion = False
            abort_event = None
            current_turn_events = []
            continue

        if not turn_open:
            continue

        current_turn_events.append(event)
        if is_task_complete(event):
            saw_completion = True
            abort_event = None
            continue

        if is_turn_aborted(event) and not saw_completion:
            abort_event = event

    if turn_open and not saw_completion:
        if (legacy_summary := legacy_terminal_assistant_summary(current_turn_events)) is not None:
            return legacy_summary
        if abort_event is not None:
            return abort_display_label(abort_event)
    return None
