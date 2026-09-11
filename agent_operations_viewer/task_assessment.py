"""Deterministic, source-backed task accounting and personal human reviews."""
from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
import math
import sqlite3
from typing import Any

VERSION = "task-assessment-v1"
MAX_TURNS = 50
MAX_EVENTS = 20_000
TOKEN_FIELDS = ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens")
REQUIRED_TOKENS = TOKEN_FIELDS[:3]
WEIGHT_FIELDS = ("uncached_input", "cached_input", "output")
DEFAULT_POLICY = {
    "version": "work-units-v1",
    "resource": {"uncached_input": 1.0, "cached_input": 0.1, "output": 4.0},
    "models": {},
}
JUDGMENTS = {
    "outcome": ("unknown", "pass", "partial", "fail"),
    "verification_fit": ("unknown", "adequate", "inadequate"),
    "execution_fit": ("uncertain", "insufficient", "appropriate", "excessive"),
    "content_fit": ("uncertain", "insufficient", "appropriate", "excessive"),
    "effort_fit": ("unestablished", "appropriate", "candidate_for_reduction", "candidate_for_increase"),
    "model_fit": ("unestablished", "appropriate", "candidate_for_comparison"),
}
DEMAND_FIELDS = ("complexity", "ambiguity", "environment_risk", "change_risk", "verification_need")
TEXT_FIELDS = ("acceptance_criteria", "verification_notes", "findings", "recommended_experiment")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def payload(event: dict) -> dict:
    try:
        record = json.loads(event.get("record_json") or "{}")
    except (ValueError, TypeError):
        return {}
    result = record.get("payload") if isinstance(record, dict) else None
    return result if isinstance(result, dict) else {}


def validate_policy(value: Any) -> dict:
    if not isinstance(value, dict) or set(value) != {"version", "resource", "models"}:
        raise ValueError("Policy requires version, resource, and models.")
    if not isinstance(value["version"], str) or not 1 <= len(value["version"].strip()) <= 100:
        raise ValueError("Policy version must contain 1–100 characters.")

    def weights(raw: Any) -> dict:
        if not isinstance(raw, dict) or set(raw) != set(WEIGHT_FIELDS):
            raise ValueError("Weights require uncached_input, cached_input, and output.")
        if any(type(v) not in (int, float) or not 0 <= v <= 1e9 or not math.isfinite(v) for v in raw.values()):
            raise ValueError("Weights must be finite numbers between 0 and 1 billion.")
        if not any(raw.values()):
            raise ValueError("At least one weight must be positive.")
        return {key: float(raw[key]) for key in WEIGHT_FIELDS}

    resource = weights(value["resource"])
    if not isinstance(value["models"], dict) or len(value["models"]) > 100:
        raise ValueError("Models must be a mapping of at most 100 exact model IDs.")
    models = {}
    for model, entry in value["models"].items():
        if not isinstance(model, str) or not 1 <= len(model.strip()) <= 200 or not isinstance(entry, dict):
            raise ValueError("Each model requires an exact ID and a mapping.")
        if set(entry) - {"weights", "basis", "family", "generation", "tier"}:
            raise ValueError("Unsupported model policy field.")
        if not isinstance(entry.get("basis"), str) or not 1 <= len(entry["basis"].strip()) <= 2000:
            raise ValueError("Each model weight mapping requires a documented basis.")
        models[model] = {"weights": weights(entry.get("weights")), "basis": entry["basis"].strip()}
        for key in ("family", "generation", "tier"):
            if key in entry:
                if not isinstance(entry[key], str) or len(entry[key]) > 200:
                    raise ValueError(f"Model {key} must be text of at most 200 characters.")
                models[model][key] = entry[key]
    return {"version": value["version"].strip(), "resource": resource, "models": models}


def empty_review() -> dict:
    return {
        **{key: options[0] for key, options in JUDGMENTS.items()},
        **{key: "" for key in TEXT_FIELDS},
        "demand": {key: None for key in DEMAND_FIELDS},
        "evidence_events": [],
        "evidence_level": "human_trace_review",
        "rubric_version": VERSION,
    }


def validate_review(fields: dict, event_indexes: set[int]) -> dict:
    review = empty_review()
    for key, options in JUDGMENTS.items():
        value = fields.get(key, options[0])
        if value not in options:
            raise ValueError(f"Invalid {key}.")
        review[key] = value
    for key in TEXT_FIELDS:
        value = fields.get(key, "")
        if not isinstance(value, str) or len(value) > 12_000:
            raise ValueError(f"{key} must be text of at most 12,000 characters.")
        review[key] = value.strip()
    for key in DEMAND_FIELDS:
        raw = fields.get(key, "")
        if raw not in (None, ""):
            if str(raw) not in {"1", "2", "3", "4", "5"}:
                raise ValueError(f"{key} must be blank or an integer from 1 to 5.")
            review["demand"][key] = int(raw)
    raw_refs = fields.get("evidence_events", "")
    try:
        refs = sorted({int(item.strip()) for item in raw_refs.split(",") if item.strip()})
    except (AttributeError, ValueError) as exc:
        raise ValueError("Evidence events must be comma-separated event indexes.") from exc
    if len(refs) > 100 or not set(refs).issubset(event_indexes):
        raise ValueError("Cite at most 100 events from the selected task range.")
    review["evidence_events"] = refs
    if any(review[key] != options[0] for key, options in JUDGMENTS.items()):
        if not refs or not review["findings"]:
            raise ValueError("A judgment requires findings and at least one source event.")
    if review["outcome"] == "pass" and (not review["acceptance_criteria"] or not review["verification_notes"]):
        raise ValueError("A passing outcome requires acceptance criteria and verification notes.")
    return review


def _tokens(raw: Any) -> dict[str, int | None]:
    raw = raw if isinstance(raw, dict) else {}
    values = {key: raw.get(key) if type(raw.get(key)) is int and 0 <= raw[key] <= 10**15 else None for key in TOKEN_FIELDS}
    if values["input_tokens"] is not None and values["cached_input_tokens"] is not None:
        if values["cached_input_tokens"] > values["input_tokens"]:
            values["cached_input_tokens"] = None
    if values["output_tokens"] is not None and values["reasoning_output_tokens"] is not None:
        if values["reasoning_output_tokens"] > values["output_tokens"]:
            values["reasoning_output_tokens"] = None
    return values


def _usage(event: dict | None) -> dict | None:
    if not event or event.get("record_type") != "event_msg" or event.get("payload_type") != "token_count":
        return None
    info = payload(event).get("info")
    return info if isinstance(info, dict) and isinstance(info.get("total_token_usage"), dict) else None


def _wu(tokens: dict, weights: dict) -> float:
    return ((tokens["input_tokens"] - tokens["cached_input_tokens"]) * weights["uncached_input"]
            + tokens["cached_input_tokens"] * weights["cached_input"]
            + tokens["output_tokens"] * weights["output"]) / 1000


def _time(value: Any) -> datetime | None:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result if result.tzinfo else None
    except ValueError:
        return None


def measure_task(events: list[dict], *, baseline: dict | None, prior_context: dict | None,
                 origin_allowed: bool, supported: bool, policy: dict) -> dict:
    """Account only intervals whose source values establish a valid delta."""
    totals = {key: None for key in TOKEN_FIELDS}
    missing_fields = {key: False for key in TOKEN_FIELDS}
    missing_interval = False
    issues: set[str] = set()
    baseline_info = _usage(baseline)
    previous = _tokens(baseline_info["total_token_usage"]) if baseline_info else None
    configurations = []
    current_model = None
    current_effort = None
    active_models: set[str | None] = set()
    if prior_context:
        context = payload(prior_context)
        current_model = context.get("model") if isinstance(context.get("model"), str) else None
        current_effort = context.get("effort") if isinstance(context.get("effort"), str) else None
        configurations.append({"event_index": prior_context["event_index"], "model": current_model,
                               "effort": current_effort, "source": "preceding_context"})
    intervals = []
    checkpoints = 0
    duplicate_checkpoints = 0
    complete_intervals = 0
    priced_intervals = 0
    resource_wu = 0.0
    model_wu = 0.0
    if not supported:
        issues.add("Usage accounting is unsupported for this provider; Codex cumulative semantics were not applied.")
    for event in events:
        data = payload(event)
        if event.get("record_type") == "turn_context":
            # Each context describes this configuration. Do not silently inherit omitted effort.
            current_model = data.get("model") if isinstance(data.get("model"), str) else None
            current_effort = data.get("effort", data.get("reasoning_effort"))
            if not isinstance(current_effort, str):
                current_effort = None
            configurations.append({"event_index": event["event_index"], "model": current_model,
                                   "effort": current_effort, "source": "recorded_context"})
        elif event.get("payload_type") == "model_reroute":
            # Do not continue pricing against the old context after a reroute.
            # Producer variants need a dedicated serving-model adapter before
            # their destination can be treated as actual model attribution.
            current_model = None
            active_models.add(None)
            configurations.append({"event_index": event["event_index"], "model": None,
                                   "effort": current_effort, "source": "model_reroute"})
        if event.get("kind") in {"tool_call", "reasoning", "message"} and event.get("role") != "user":
            active_models.add(current_model)
        info = _usage(event)
        if not supported or info is None:
            continue
        checkpoints += 1
        current = _tokens(info["total_token_usage"])
        if previous is not None and current == previous:
            duplicate_checkpoints += 1
            continue
        if previous is None and origin_allowed and checkpoints == 1:
            last = _tokens(info.get("last_token_usage"))
            if all(current[k] is not None and current[k] == last[k] for k in REQUIRED_TOKENS):
                previous = {k: 0 if current[k] is not None else None for k in TOKEN_FIELDS}
        if previous is None:
            issues.add("Starting usage baseline is unknown; the initial interval is excluded.")
            missing_interval = True
            previous = current
            active_models.clear()
            continue
        reset = any(previous[k] is not None and current[k] is not None and current[k] < previous[k] for k in TOKEN_FIELDS)
        if reset:
            issues.add("Usage counters decreased; the discontinuous interval is excluded.")
            missing_interval = True
            previous = current
            active_models.clear()
            continue
        delta = {k: current[k] - previous[k] if current[k] is not None and previous[k] is not None else None for k in TOKEN_FIELDS}
        delta = _tokens(delta)
        for key, value in delta.items():
            if value is not None:
                totals[key] = (totals[key] or 0) + value
            else:
                missing_fields[key] = True
        cost_known = all(delta[k] is not None for k in REQUIRED_TOKENS)
        candidates = active_models | {current_model}
        model = next(iter(candidates)) if len(candidates) == 1 else None
        interval = {"event_index": event["event_index"], "tokens": delta, "model": model,
                    "model_basis": "recorded_configuration", "resource_wu": None, "model_weighted_wu": None}
        if cost_known:
            complete_intervals += 1
            interval["resource_wu"] = _wu(delta, policy["resource"])
            resource_wu += interval["resource_wu"]
            model_policy = policy["models"].get(model)
            if model_policy:
                priced_intervals += 1
                interval["model_weighted_wu"] = _wu(delta, model_policy["weights"])
                model_wu += interval["model_weighted_wu"]
        else:
            issues.add("Some usage fields are missing or inconsistent; cost covers measurable intervals only.")
        intervals.append(interval)
        previous = current
        active_models.clear()
    if supported and not checkpoints:
        issues.add("No cumulative usage checkpoints were recorded in this range.")
    if checkpoints and not intervals:
        issues.add("No attributable usage increments were established.")
    if priced_intervals < complete_intervals:
        issues.add("Some cost intervals have an unknown, mixed, or unpriced model configuration.")
    # Model coverage is independent of usage coverage.
    usage_issues = [issue for issue in issues if "unpriced model" not in issue]
    usage_status = "unknown" if not complete_intervals else "partial" if usage_issues else "recorded"
    model_status = "unknown" if not priced_intervals else "recorded" if priced_intervals == complete_intervals and usage_status == "recorded" else "partial"

    calls = {event.get("call_id") or f"event:{event['event_index']}" for event in events if event.get("kind") in {"tool_call", "command"}}
    shell_calls = {event.get("call_id") or f"event:{event['event_index']}" for event in events
                   if event.get("kind") == "command" or (event.get("kind") == "tool_call" and str(event.get("tool_name") or "").split(".")[-1] in {"exec_command", "shell", "shell_command"})}
    messages = [e for e in events if e.get("record_type") == "response_item" and e.get("payload_type") == "message" and e.get("role") == "assistant"]
    if not messages:
        messages = [e for e in events if e.get("record_type") == "event_msg" and e.get("payload_type") == "agent_message"]
    final_chars = commentary_chars = unclassified_chars = 0
    for event in messages:
        data = payload(event)
        channel = data.get("phase", data.get("channel"))
        count = len(event.get("display_text") or "")
        if channel in {"final", "final_answer"}:
            final_chars += count
        elif channel == "commentary":
            commentary_chars += count
        else:
            unclassified_chars += count
    times = [stamp for event in events if (stamp := _time(event.get("timestamp"))) is not None]
    return {
        "tokens": totals,
        "token_coverage": {key: "unknown" if totals[key] is None else "partial" if missing_interval or missing_fields[key] else "recorded" for key in TOKEN_FIELDS},
        "resource_wu": resource_wu if complete_intervals else None,
        "model_weighted_wu": model_wu if priced_intervals else None,
        "usage_coverage": usage_status, "model_coverage": model_status,
        "usage_checkpoints": checkpoints, "duplicate_checkpoints": duplicate_checkpoints,
        "measured_intervals": complete_intervals, "priced_intervals": priced_intervals,
        "limitations": sorted(issues), "intervals": intervals, "configurations": configurations,
        "selector": None, "actual_serving_model": None,
        "tool_calls": len(calls), "shell_invocations": len(shell_calls),
        "final_characters": final_chars, "commentary_characters": commentary_chars,
        "unclassified_assistant_characters": unclassified_chars,
        "elapsed_seconds": (max(times) - min(times)).total_seconds() if len(times) >= 2 else None,
        "scope": "Selected session only; child work excluded.",
    }


def _turn_start(connection: sqlite3.Connection, session_id: str, turn: dict) -> int:
    """The search index groups by prompt; Codex emits start/context before it."""
    previous = connection.execute(
        "SELECT start_event_index FROM session_turns WHERE session_id = ? AND turn_number = ?",
        (session_id, turn["turn_number"] - 1)).fetchone()
    floor = previous["start_event_index"] + 1 if previous else 0
    started = connection.execute(
        "SELECT event_index FROM events WHERE session_id = ? AND event_index BETWEEN ? AND ? "
        "AND record_type = 'event_msg' AND payload_type = 'task_started' ORDER BY event_index DESC LIMIT 1",
        (session_id, floor, turn["start_event_index"])).fetchone()
    return started["event_index"] if started else turn["start_event_index"]


def task_source(connection: sqlite3.Connection, session: dict, start: int, end: int) -> dict:
    if start < 1 or end < start or end - start + 1 > MAX_TURNS:
        raise ValueError(f"Select a valid range of at most {MAX_TURNS} turns.")
    turns = [dict(row) for row in connection.execute(
        "SELECT turn_number, start_event_index, end_event_index, prompt_excerpt, response_state FROM session_turns "
        "WHERE session_id = ? AND turn_number BETWEEN ? AND ? ORDER BY turn_number", (session["id"], start, end))]
    if len(turns) != end - start + 1:
        raise LookupError("The requested turns are not indexed or do not exist.")
    following = connection.execute(
        "SELECT turn_number, start_event_index FROM session_turns WHERE session_id = ? AND turn_number = ?",
        (session["id"], end + 1)).fetchone()
    # Keep the next task's leading control/context events out of this task. This
    # also makes a completed review stable when unrelated turns are appended.
    starts = [_turn_start(connection, session["id"], turn) for turn in turns]
    next_start = _turn_start(connection, session["id"], dict(following)) if following else None
    for index, turn in enumerate(turns):
        turn["start_event_index"] = starts[index]
        boundary = starts[index + 1] if index + 1 < len(starts) else next_start
        if boundary is not None:
            turn["end_event_index"] = boundary - 1
    first, last = turns[0]["start_event_index"], turns[-1]["end_event_index"]
    # Exclude SQLite row IDs, which can change on reimport without changing evidence.
    columns = "event_index, timestamp, record_type, payload_type, kind, role, title, display_text, detail_text, tool_name, call_id, command_text, exit_code, record_json"
    events = [dict(row) for row in connection.execute(
        f"SELECT {columns} FROM events WHERE session_id = ? AND event_index BETWEEN ? AND ? ORDER BY event_index LIMIT ?",
        (session["id"], first, last, MAX_EVENTS + 1))]
    if len(events) > MAX_EVENTS:
        raise ValueError(f"Select a smaller range; this task exceeds {MAX_EVENTS:,} events.")
    prior_turn = connection.execute(
        "SELECT start_event_index, end_event_index FROM session_turns WHERE session_id = ? AND turn_number = ?",
        (session["id"], start - 1)).fetchone()
    baseline = None
    if prior_turn:
        # Null-info rate updates are not a usage baseline. Search only the immediate prior turn.
        for row in connection.execute(
            f"SELECT {columns} FROM events WHERE session_id = ? AND event_index BETWEEN ? AND ? "
            "AND record_type = 'event_msg' AND payload_type = 'token_count' ORDER BY event_index DESC LIMIT ?",
            (session["id"], prior_turn["start_event_index"], first - 1, MAX_EVENTS)):
            candidate = dict(row)
            if _usage(candidate):
                baseline = candidate
                break
    prior = connection.execute(
        f"SELECT {columns} FROM events WHERE session_id = ? AND event_index < ? AND record_type = 'turn_context' "
        "ORDER BY event_index DESC LIMIT 1", (session["id"], first)).fetchone()
    context = dict(prior) if prior else None
    identity = {key: session.get(key) for key in ("id", "model_provider", "source", "forked_from_id", "cli_version",
                                                "cwd", "source_host", "git_commit_hash", "git_repository_url")}
    evidence = {"version": VERSION, "identity": identity, "turns": turns, "events": events,
                "baseline": baseline, "prior_context": context}
    return {**evidence, "evidence_digest": digest(evidence), "start_turn": start, "end_turn": end}


def report_for_source(source: dict, policy: dict) -> dict:
    identity = source["identity"]
    supported = identity.get("model_provider") != "anthropic" and identity.get("source") != "claude"
    metrics = measure_task(source["events"], baseline=source["baseline"], prior_context=source["prior_context"],
                           origin_allowed=source["start_turn"] == 1 and not identity.get("forked_from_id"),
                           supported=supported, policy=policy)
    return {"schema_version": VERSION, "session_id": identity["id"], "start_turn": source["start_turn"],
            "provenance": identity,
            "end_turn": source["end_turn"], "evidence_digest": source["evidence_digest"],
            "policy": policy, "policy_hash": digest(policy), "metrics": metrics,
            "turns": source["turns"], "evidence": source["events"],
            "baseline": source["baseline"], "prior_context": source["prior_context"]}


def revisions(connection: sqlite3.Connection, owner: str, session_id: str, start: int, end: int) -> list[dict]:
    return [dict(row) for row in connection.execute(
        "SELECT id, created_at, evidence_digest FROM task_assessment_revisions "
        "WHERE owner_scope = ? AND session_id = ? AND start_turn = ? AND end_turn = ? ORDER BY id DESC LIMIT 100",
        (owner, session_id, start, end))]


def read_revision(connection: sqlite3.Connection, owner: str, session_id: str, start: int, end: int, revision: int) -> dict:
    row = connection.execute(
        "SELECT * FROM task_assessment_revisions WHERE id = ? AND owner_scope = ? AND session_id = ? AND start_turn = ? AND end_turn = ?",
        (revision, owner, session_id, start, end)).fetchone()
    if row is None:
        raise LookupError("Review revision not found.")
    return {"id": row["id"], "created_at": row["created_at"], "evidence_digest": row["evidence_digest"],
            "review": json.loads(row["review_json"]), "policy": json.loads(row["policy_json"]),
            "snapshot": json.loads(row["snapshot_json"])}


def save_revision(connection: sqlite3.Connection, owner: str, report: dict, review: dict) -> int:
    cursor = connection.execute(
        "INSERT INTO task_assessment_revisions (owner_scope, session_id, start_turn, end_turn, created_at, "
        "evidence_digest, review_json, policy_json, snapshot_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (owner, report["session_id"], report["start_turn"], report["end_turn"], datetime.now(UTC).isoformat(),
         report["evidence_digest"], canonical_json(review), canonical_json(report["policy"]), canonical_json(report)))
    return int(cursor.lastrowid)
