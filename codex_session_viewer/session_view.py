from __future__ import annotations

import json
import shlex
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
import re

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
from .text_utils import shorten, strip_codex_wrappers_preserve_layout


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


def humanize_duration(value: object) -> str:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return ""
    if seconds < 0:
        return ""
    if seconds < 1:
        milliseconds = max(int(round(seconds * 1000)), 1)
        return f"{milliseconds}ms"
    if seconds < 10:
        return f"{seconds:.1f}s"
    if seconds < 60:
        return f"{int(round(seconds))}s"
    minutes = int(seconds // 60)
    remaining = int(round(seconds % 60))
    if seconds < 3600:
        return f"{minutes}m {remaining}s"
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours}h {minutes}m"


def coerce_duration_seconds(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        secs = value.get("secs")
        nanos = value.get("nanos")
        if isinstance(secs, (int, float)) or isinstance(nanos, (int, float)):
            return float(secs or 0) + (float(nanos or 0) / 1_000_000_000.0)
    return None


def _should_collapse_prompt_block(block: str) -> bool:
    stripped = block.strip()
    if not stripped:
        return False
    lines = stripped.splitlines()
    line_count = len(lines)
    char_count = len(stripped)
    first_line = lines[0].strip() if lines else ""
    last_line = lines[-1].strip() if lines else ""

    if first_line.startswith("```") and last_line.startswith("```"):
        return True
    if first_line.startswith("# AGENTS.md instructions"):
        return True
    if first_line.startswith("<") and line_count >= 4 and last_line.startswith("</"):
        return True
    if line_count >= 14:
        return True
    if line_count >= 8 and char_count >= 600:
        return True
    if char_count >= 1400:
        return True
    return False


def prompt_segments(prompt_text: str) -> list[dict[str, object]]:
    normalized = prompt_text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return []

    blocks = [block.strip("\n") for block in normalized.split("\n\n") if block.strip("\n").strip()]
    if not blocks:
        return []

    segments: list[dict[str, object]] = []
    current_kind: str | None = None
    current_blocks: list[str] = []

    def flush() -> None:
        nonlocal current_kind, current_blocks
        if not current_blocks or current_kind is None:
            current_kind = None
            current_blocks = []
            return
        text = "\n\n".join(current_blocks).strip()
        if not text:
            current_kind = None
            current_blocks = []
            return
        segments.append(
            {
                "kind": current_kind,
                "text": text,
                "char_count": len(text),
                "line_count": len(text.splitlines()),
            }
        )
        current_kind = None
        current_blocks = []

    for block in blocks:
        kind = "collapsed" if _should_collapse_prompt_block(block) else "text"
        if kind == current_kind:
            current_blocks.append(block)
            continue
        flush()
        current_kind = kind
        current_blocks = [block]

    flush()
    return segments


def collapse_path_for_display(path: str, cwd: str | None) -> str:
    candidate = str(path or "").strip()
    if not candidate:
        return ""
    if cwd:
        try:
            cwd_path = Path(cwd).expanduser().resolve()
            candidate_path = Path(candidate).expanduser()
            if candidate_path.is_absolute():
                try:
                    return candidate_path.resolve().relative_to(cwd_path).as_posix()
                except ValueError:
                    pass
        except OSError:
            pass
    candidate_path = Path(candidate)
    parts = [part for part in candidate_path.parts if part not in {"/", ""}]
    if candidate.startswith("/") and parts:
        if len(parts) >= 2:
            return f"…/{'/'.join(parts[-2:])}"
        return parts[-1]
    return candidate_path.as_posix()


def split_tool_output(detail_text: str) -> tuple[str, str]:
    marker = "\n\nTool Output:\n"
    if marker in detail_text:
        primary, tool_output = detail_text.split(marker, 1)
        return primary.strip(), tool_output.strip()
    return detail_text.strip(), ""


def decode_json_string(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith('"') and text.endswith('"'):
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            return text
        if isinstance(decoded, str):
            return decoded.strip()
    return text


def parse_record_payload(record_json: object) -> dict[str, object] | None:
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


def parse_patch_files(detail_text: str, *, cwd: str | None = None) -> list[dict[str, str]]:
    primary_text, _ = split_tool_output(detail_text)
    parsed = parse_jsonish(primary_text)
    if not isinstance(parsed, dict):
        return []

    files: list[dict[str, str]] = []
    for raw_path, metadata in parsed.items():
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue
        operation = "update"
        if isinstance(metadata, dict):
            operation = str(metadata.get("type") or "update").strip().lower() or "update"
        files.append(
            {
                "path": collapse_path_for_display(raw_path, cwd),
                "operation": operation,
            }
        )
    files.sort(key=lambda item: item["path"])
    return files


def diff_stat_counts(unified_diff: str) -> tuple[int, int, int]:
    additions = 0
    deletions = 0
    hunks = 0
    for line in decode_json_string(unified_diff).splitlines():
        if line.startswith("@@"):
            hunks += 1
        elif line.startswith("+") and not line.startswith("+++"):
            additions += 1
        elif line.startswith("-") and not line.startswith("---"):
            deletions += 1
    return additions, deletions, hunks


def parse_patch_manifest(detail_text: str, *, cwd: str | None = None) -> list[dict[str, object]]:
    primary_text, _ = split_tool_output(detail_text)
    parsed = parse_jsonish(primary_text)
    if not isinstance(parsed, dict):
        return []

    manifest: list[dict[str, object]] = []
    for raw_path, metadata in parsed.items():
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue
        operation = "update"
        move_path = ""
        additions = 0
        deletions = 0
        hunks = 0
        if isinstance(metadata, dict):
            operation = str(metadata.get("type") or "update").strip().lower() or "update"
            move_path = collapse_path_for_display(str(metadata.get("move_path") or ""), cwd)
            additions, deletions, hunks = diff_stat_counts(str(metadata.get("unified_diff") or ""))
        manifest.append(
            {
                "path": collapse_path_for_display(raw_path, cwd),
                "operation": operation,
                "move_path": move_path,
                "additions": additions,
                "deletions": deletions,
                "hunks": hunks,
            }
        )
    manifest.sort(key=lambda item: str(item["path"]))
    return manifest


def patch_diff_lines(raw_patch_text: str, *, cwd: str | None = None) -> list[dict[str, str]]:
    text = decode_json_string(raw_patch_text)
    if not text:
        return []

    lines: list[dict[str, str]] = []
    for raw_line in text.splitlines():
        line = raw_line.rstrip("\n")
        rendered = line
        kind = "plain"
        prefix = ""
        content = line

        for marker in ("*** Update File: ", "*** Add File: ", "*** Delete File: ", "*** Move to: "):
            if line.startswith(marker):
                kind = "file_header"
                path = line[len(marker) :].strip()
                rendered = f"{marker}{collapse_path_for_display(path, cwd)}"
                content = rendered
                break
        else:
            if line.startswith("*** Begin Patch") or line.startswith("*** End Patch"):
                kind = "boundary"
            elif line.startswith("@@"):
                kind = "hunk"
            elif line.startswith("+") and not line.startswith("+++"):
                kind = "addition"
                prefix = "+"
                content = line[1:]
            elif line.startswith("-") and not line.startswith("---"):
                kind = "deletion"
                prefix = "-"
                content = line[1:]
            elif line.startswith(" "):
                kind = "context"
                prefix = " "
                content = line[1:]

        lines.append(
            {
                "kind": kind,
                "rendered": rendered,
                "prefix": prefix,
                "content": content,
            }
        )
    return lines


def classify_verification_command(command_text: str | None) -> str | None:
    text = str(command_text or "").strip().lower()
    if not text:
        return None

    patterns: list[tuple[str, tuple[str, ...]]] = [
        ("test", ("pytest", "go test", "cargo test", "ctest", "npm test", "pnpm test", "yarn test", "uv run pytest", "rspec", "phpunit")),
        ("lint", ("eslint", "ruff", "flake8", "mypy", "shellcheck", "hadolint", "npm run lint", "pnpm lint", "yarn lint", "cargo clippy", "prettier --check")),
        ("check", ("cargo check", "go vet", "npm run check", "pnpm check", "yarn check", "tsc --noemit", "python3 -m py_compile", "python -m py_compile", "bash -n ")),
        ("build", ("cmake --build", "cargo build", "go build", "npm run build", "pnpm build", "yarn build")),
    ]
    for label, needles in patterns:
        if any(needle in text for needle in needles):
            return label
    if re.fullmatch(r"(make|ninja)(\s+[-\w=./]+)*", text):
        return "build"
    return None


SHELL_WRAPPER_NAMES = {"bash", "sh", "zsh", "dash", "ksh", "fish"}


def command_tokens(command_text: str | None) -> list[str]:
    text = str(command_text or "").strip()
    if not text:
        return []
    try:
        return shlex.split(text, posix=True)
    except ValueError:
        return []


def _consume_env_wrapper(tokens: list[str]) -> tuple[str | None, list[str]]:
    if not tokens or Path(tokens[0]).name != "env":
        return None, tokens

    env_tokens = [tokens[0]]
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            env_tokens.append(token)
            index += 1
            break
        if token == "-i" or ("=" in token and not token.startswith("-")):
            env_tokens.append(token)
            index += 1
            continue
        break

    remainder = tokens[index:]
    if not remainder:
        return None, tokens
    return " ".join(env_tokens), remainder


def unwrap_command_display(command_text: str | None) -> tuple[str, str | None]:
    original = str(command_text or "").strip()
    if not original:
        return "", None

    current = original
    wrappers: list[str] = []

    for _ in range(4):
        tokens = command_tokens(current)
        if not tokens:
            break

        env_wrapper, tokens = _consume_env_wrapper(tokens)
        if env_wrapper and tokens and Path(tokens[0]).name in SHELL_WRAPPER_NAMES:
            wrappers.append(env_wrapper)

        if not tokens:
            break

        shell_name = Path(tokens[0]).name
        if shell_name not in SHELL_WRAPPER_NAMES:
            break

        wrapper_tokens = [shell_name]
        inner_command: str | None = None
        index = 1
        while index < len(tokens):
            token = tokens[index]
            if token in {"-l", "-i", "-s"}:
                wrapper_tokens.append(token)
                index += 1
                continue
            if token in {"-c", "-lc", "-cl"}:
                wrapper_tokens.append(token)
                if index + 1 < len(tokens):
                    inner_command = tokens[index + 1]
                break
            if token.startswith("-") and set(token[1:]).issubset({"l", "c", "i", "s"}):
                wrapper_tokens.append(token)
                if "c" in token[1:] and index + 1 < len(tokens):
                    inner_command = tokens[index + 1]
                break
            break

        if not inner_command:
            break

        wrappers.append(" ".join(wrapper_tokens))
        current = inner_command.strip()
        if not current:
            break

    display_command_text = current or original
    command_wrapper = " · ".join(wrapper for wrapper in wrappers if wrapper) or None
    if display_command_text == original:
        command_wrapper = None
    return display_command_text, command_wrapper


def command_primary_label(command_text: str | None) -> str | None:
    tokens = command_tokens(command_text)
    if not tokens:
        return None

    primary = Path(tokens[0]).name or tokens[0]
    second = tokens[1] if len(tokens) > 1 else ""
    third = tokens[2] if len(tokens) > 2 else ""

    if primary in {"npm", "pnpm", "yarn"}:
        if second == "run" and third and not third.startswith("-"):
            return f"{primary} run {third}"
        if second and not second.startswith("-"):
            return f"{primary} {second}"
    if primary == "uv":
        if second == "run" and third and not third.startswith("-"):
            return f"{primary} {second} {third}"
        if second and not second.startswith("-"):
            return f"{primary} {second}"
    if primary in {"git", "cargo", "go", "docker", "kubectl", "cmake"} and second and not second.startswith("-"):
        return f"{primary} {second}"

    return primary


def command_intent(command_text: str | None, verification_kind: str | None) -> str:
    if verification_kind:
        return "verify"

    tokens = command_tokens(command_text)
    if not tokens:
        return "other"

    primary = Path(tokens[0]).name or tokens[0]
    second = tokens[1] if len(tokens) > 1 else ""
    raw = " ".join(tokens).lower()

    inspect_commands = {
        "cat",
        "sed",
        "rg",
        "grep",
        "find",
        "ls",
        "nl",
        "head",
        "tail",
        "wc",
        "pwd",
        "which",
        "stat",
        "tree",
        "readlink",
        "awk",
        "sort",
        "uniq",
        "cut",
    }
    mutate_commands = {
        "mv",
        "cp",
        "mkdir",
        "touch",
        "chmod",
        "chown",
        "rm",
        "tee",
        "npm",
        "pnpm",
        "yarn",
        "pip",
        "uv",
        "docker",
        "systemctl",
        "service",
    }

    if primary == "git" and second in {"status", "diff", "show", "log", "branch", "remote"}:
        return "inspect"
    if primary == "git" and second in {"commit", "push", "pull", "checkout", "switch", "rebase", "merge", "reset"}:
        return "mutate"
    if primary in inspect_commands:
        return "inspect"
    if primary in mutate_commands:
        if primary in {"npm", "pnpm", "yarn", "uv"} and any(token in raw for token in (" build", " test", " lint", " check")):
            return "verify"
        return "mutate"
    if raw.startswith("python3 -m py_compile") or raw.startswith("python -m py_compile") or raw.startswith("bash -n "):
        return "verify"
    return "other"


WARNING_LINE_RE = re.compile(r"\b(warning|deprecated|outdated|deprecation)\b", re.IGNORECASE)


def extract_warning_lines(text: str, *, max_items: int = 4) -> list[str]:
    warnings: list[str] = []
    for line in text.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        if not WARNING_LINE_RE.search(candidate):
            continue
        if candidate.startswith(("Chunk ID:", "Wall time:", "Original token count:", "Process ")):
            continue
        if candidate not in warnings:
            warnings.append(candidate)
        if len(warnings) >= max_items:
            break
    return warnings


def verification_status_label(commands: list[dict[str, object]]) -> str:
    verification_commands = [command for command in commands if command.get("verification_kind")]
    if not verification_commands:
        return "none"
    if any(command.get("exit_code") not in (None, 0) for command in verification_commands):
        return "failed"
    return "passed"


def verification_verdict(commands: list[dict[str, object]]) -> dict[str, object]:
    verification_commands = [command for command in commands if command.get("verification_kind")]
    if not verification_commands:
        return {
            "status": "none",
            "label": "No verification",
            "tone": "stone",
            "warning_count": 0,
            "command_count": 0,
        }
    if any(command.get("exit_code") not in (None, 0) for command in verification_commands):
        return {
            "status": "failed",
            "label": "Verification failed",
            "tone": "rose",
            "warning_count": sum(int(command.get("warning_count") or 0) for command in verification_commands),
            "command_count": len(verification_commands),
        }
    warning_count = sum(int(command.get("warning_count") or 0) for command in verification_commands)
    if warning_count:
        return {
            "status": "passed_with_warnings",
            "label": "Passed with warnings",
            "tone": "amber",
            "warning_count": warning_count,
            "command_count": len(verification_commands),
        }
    return {
        "status": "passed",
        "label": "Verification passed",
        "tone": "emerald",
        "warning_count": 0,
        "command_count": len(verification_commands),
    }


def plan_step_map(steps: list[dict[str, str]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for step in steps:
        step_text = str(step.get("step") or "").strip()
        status = str(step.get("status") or "").strip()
        if step_text:
            mapping[step_text] = status
    return mapping


def plan_diff(previous_steps: list[dict[str, str]], current_steps: list[dict[str, str]]) -> dict[str, list[str]]:
    previous_map = plan_step_map(previous_steps)
    current_map = plan_step_map(current_steps)

    added = [step for step in current_map if step not in previous_map]
    removed = [step for step in previous_map if step not in current_map]
    completed = [
        step
        for step, status in current_map.items()
        if status == "completed" and previous_map.get(step) != "completed"
    ]
    in_progress = [
        step
        for step, status in current_map.items()
        if status == "in_progress" and previous_map.get(step) != "in_progress"
    ]
    return {
        "added": added,
        "removed": removed,
        "completed": completed,
        "in_progress": in_progress,
    }


def grouped_work_entries_from_merged(merged_events: list[dict[str, object]]) -> list[dict[str, object]]:
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

    for event in merged_events:
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


def command_call_workdir(event: dict[str, object]) -> str | None:
    parsed = parse_jsonish(event.get("detail_text"))
    if not isinstance(parsed, dict):
        return None
    workdir = parsed.get("workdir")
    if isinstance(workdir, str) and workdir.strip():
        return workdir.strip()
    return None


def build_phase_strip(
    *,
    plan_count: int,
    research_count: int,
    inspect_count: int,
    modify_count: int,
    verification_count: int,
    response_present: bool,
) -> list[dict[str, object]]:
    phases: list[dict[str, object]] = []
    if plan_count:
        phases.append({"key": "plan", "label": "Plan", "count": plan_count, "tone": "amber"})
    if research_count:
        phases.append({"key": "research", "label": "Research", "count": research_count, "tone": "cyan"})
    if inspect_count:
        phases.append({"key": "inspect", "label": "Inspect", "count": inspect_count, "tone": "sky"})
    if modify_count:
        phases.append({"key": "modify", "label": "Modify", "count": modify_count, "tone": "violet"})
    if verification_count:
        phases.append({"key": "verify", "label": "Verify", "count": verification_count, "tone": "emerald"})
    if response_present:
        phases.append({"key": "respond", "label": "Respond", "count": 1, "tone": "stone"})
    return phases


def is_research_event(event: dict[str, object]) -> bool:
    if str(event.get("kind") or "") not in {"tool_call", "tool_result"}:
        return False
    tool_name = str(event.get("tool_name") or "").strip()
    payload_type = str(event.get("payload_type") or "").strip()
    return tool_name in {"web_search", "tool_search", "view_image"} or payload_type in {
        "web_search_call",
        "web_search_end",
        "tool_search_call",
        "tool_search_output",
        "image_generation_call",
        "image_generation_end",
    }


def is_context_event(event: dict[str, object]) -> bool:
    record_type = str(event.get("record_type") or "").strip()
    payload_type = str(event.get("payload_type") or "").strip()
    return (
        (record_type == "event_msg" and payload_type in {"context_compacted", "thread_rolled_back", "item_completed"})
        or record_type == "compacted"
    )


def aggregate_file_manifest(patch_events: list[dict[str, object]]) -> list[dict[str, object]]:
    aggregated: dict[str, dict[str, object]] = {}
    for event in patch_events:
        for item in event.get("patch_manifest", []):
            path = str(item.get("path") or "")
            if not path:
                continue
            entry = aggregated.setdefault(
                path,
                {
                    "path": path,
                    "touches": 0,
                    "additions": 0,
                    "deletions": 0,
                    "hunks": 0,
                    "operations": set(),
                },
            )
            entry["touches"] = int(entry["touches"]) + 1
            entry["additions"] = int(entry["additions"]) + int(item.get("additions") or 0)
            entry["deletions"] = int(entry["deletions"]) + int(item.get("deletions") or 0)
            entry["hunks"] = int(entry["hunks"]) + int(item.get("hunks") or 0)
            operations = entry["operations"]
            if isinstance(operations, set):
                operation = str(item.get("operation") or "").strip()
                if operation:
                    operations.add(operation)

    results: list[dict[str, object]] = []
    for entry in aggregated.values():
        operations = entry.pop("operations")
        entry["operations"] = ", ".join(sorted(operations)) if isinstance(operations, set) and operations else "update"
        results.append(entry)
    results.sort(key=lambda item: str(item["path"]))
    return results


def classify_risky_file_path(path: str | None) -> str | None:
    normalized = str(path or "").strip().lower().lstrip("./")
    if not normalized:
        return None

    parts = [part for part in Path(normalized).parts if part not in {"", "."}]
    basename = parts[-1] if parts else normalized

    if basename.startswith(".env") or any("secret" in part for part in parts):
        return "Secrets-adjacent"
    if normalized.startswith(".github/workflows/"):
        return "CI/CD"
    if basename in {
        "dockerfile",
        "docker-compose.yml",
        "docker-compose.yaml",
        "compose.yml",
        "compose.yaml",
        "caddyfile",
        "nginx.conf",
    }:
        return "Infra"
    if any(
        part in {
            "deploy",
            "deployment",
            "deployments",
            "infra",
            "ops",
            "terraform",
            "ansible",
            "helm",
            "k8s",
            "kubernetes",
            "systemd",
            "nginx",
            "caddy",
        }
        for part in parts
    ):
        return "Infra"
    return None


def risky_file_manifest(file_manifest: list[dict[str, object]]) -> list[dict[str, str]]:
    risky_items: list[dict[str, str]] = []
    seen_paths: set[str] = set()
    for item in file_manifest:
        path = str(item.get("path") or "").strip()
        if not path or path in seen_paths:
            continue
        risk_kind = classify_risky_file_path(path)
        if not risk_kind:
            continue
        risky_items.append({"path": path, "kind": risk_kind})
        seen_paths.add(path)
    return risky_items


def task_duration_seconds(
    completion_event: sqlite3.Row | None,
    prompt_timestamp: str | None,
    response_timestamp: str | None,
    events: list[sqlite3.Row],
) -> float | None:
    if completion_event is not None:
        record_json = None
        try:
            if "record_json" in completion_event.keys():
                record_json = completion_event["record_json"]
        except Exception:
            record_json = None
        payload = parse_record_payload(record_json)
        if isinstance(payload, dict):
            duration_ms = payload.get("duration_ms")
            if isinstance(duration_ms, (int, float)):
                return float(duration_ms) / 1000.0
    start = parse_timestamp(prompt_timestamp)
    end = parse_timestamp(response_timestamp)
    if start and end:
        return max((end - start).total_seconds(), 0.0)
    if start and events:
        event_times = [parse_timestamp(str(event["timestamp"] or "")) for event in events]
        event_times = [value for value in event_times if value is not None]
        if event_times:
            return max((max(event_times) - start).total_seconds(), 0.0)
    return None


def response_evidence(
    *,
    turn_number: int,
    response_text: str,
    command_events: list[dict[str, object]],
    patch_events: list[dict[str, object]],
    research_events: list[dict[str, object]],
    verification_verdict_data: dict[str, object],
    file_manifest: list[dict[str, object]],
) -> dict[str, object]:
    links: list[dict[str, str]] = []
    if command_events:
        links.append(
            {
                "href": f"#turn-{turn_number}-commands",
                "label": f"{len(command_events)} commands",
            }
        )
    if patch_events:
        links.append(
            {
                "href": f"#turn-{turn_number}-patches",
                "label": f"{len(patch_events)} patches",
            }
        )
    if research_events:
        links.append(
            {
                "href": f"#turn-{turn_number}-research",
                "label": f"{len(research_events)} research events",
            }
        )
    verification_count = int(verification_verdict_data.get("command_count") or 0)
    if verification_count:
        links.append(
            {
                "href": f"#turn-{turn_number}-verification",
                "label": str(verification_verdict_data.get("label") or "Verification"),
            }
        )
    if file_manifest:
        links.append(
            {
                "href": f"#turn-{turn_number}-files",
                "label": f"{len(file_manifest)} files changed",
            }
        )

    warnings: list[str] = []
    normalized = response_text.lower()
    change_claim = any(token in normalized for token in ("updated ", "changed ", "added ", "removed ", "created ", "modified ", "renamed "))
    verification_claim = any(
        token in normalized
        for token in (
            "verified with",
            "verified by",
            "verified:",
            "verification passed",
            "verification:",
            "tests passed",
            "test passed",
            "checks passed",
            "build passed",
            "lint passed",
            "checked with",
            "compiled with",
        )
    )
    positive_verification_claim = any(
        token in normalized
        for token in (
            "tests passed",
            "test passed",
            "build passed",
            "build succeeded",
            "lint passed",
            "checks passed",
            "verified successfully",
            "successfully verified",
            "compiled successfully",
        )
    )
    rollout_claim = bool(
        re.search(r"\brestarted\b", normalized)
        or re.search(r"\bdeployed\b", normalized)
        or re.search(r"\breloaded\s+(?:nginx|caddy|service|the service)\b", normalized)
    )

    if change_claim and not patch_events:
        warnings.append("Response describes code or file changes, but no applied patch was recorded.")
    if verification_claim and verification_count == 0:
        warnings.append("Response describes verification or tests, but no verification command was detected.")
    if positive_verification_claim and str(verification_verdict_data.get("status") or "") == "failed":
        warnings.append("Response implies verification succeeded, but detected verification commands failed.")
    if rollout_claim:
        rollout_supported = any(
            any(keyword in str(event.get("display_command_text") or event.get("command_text") or "").lower() for keyword in ("restart", "reload", "deploy", "systemctl", "docker compose", "service "))
            for event in command_events
        )
        if not rollout_supported:
            warnings.append("Response describes restart or deploy behavior, but no matching command was detected.")

    return {
        "links": links,
        "warnings": warnings,
        "warning_count": len(warnings),
    }


def build_trust_signals(
    *,
    turn_number: int,
    verification_verdict_data: dict[str, object],
    response_evidence_data: dict[str, object],
    risky_files: list[dict[str, str]],
) -> list[dict[str, object]]:
    signals: list[dict[str, object]] = []

    verification_status = str(verification_verdict_data.get("status") or "")
    verification_warning_count = int(verification_verdict_data.get("warning_count") or 0)
    if verification_status == "passed_with_warnings" and verification_warning_count > 0:
        signals.append(
            {
                "key": "verification_warnings",
                "tone": "amber",
                "label": "Verification warnings",
                "detail": f"Verification completed, but {verification_warning_count} warning line{'s' if verification_warning_count != 1 else ''} were detected in successful checks.",
                "href": f"#turn-{turn_number}-verification",
                "examples": [],
            }
        )

    mismatch_count = int(response_evidence_data.get("warning_count") or 0)
    if mismatch_count > 0:
        signals.append(
            {
                "key": "claim_evidence_mismatch",
                "tone": "rose",
                "label": "Claim/evidence mismatch",
                "detail": f"{mismatch_count} response claim{'s were' if mismatch_count != 1 else ' was'} not backed by the recorded commands, patches, or verification in this turn.",
                "href": f"#turn-{turn_number}-response",
                "examples": [],
            }
        )

    if risky_files and verification_status == "none":
        risk_kinds = sorted({item["kind"] for item in risky_files if item.get("kind")})
        kind_summary = ", ".join(risk_kinds) if risk_kinds else "risky"
        signals.append(
            {
                "key": "risky_changes_unverified",
                "tone": "amber",
                "label": "Risky changes unverified",
                "detail": f"{len(risky_files)} {kind_summary.lower()} file change{'s were' if len(risky_files) != 1 else ' was'} recorded without any verification command in this turn.",
                "href": f"#turn-{turn_number}-files",
                "examples": [item["path"] for item in risky_files[:3]],
            }
        )

    return signals


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

            merged = dict(event)
            merged["kind"] = "command"
            merged["group_key"] = "command"
            merged["style"] = kind_style("command")
            merged["title"] = friendly_tool_title(tool_name)
            merged["command_text"] = command_text or None
            merged["summary_text"] = command_summary or command_text or "Shell command"
            merged["output_text"] = output_text
            merged["exit_code"] = command_event.get("exit_code") if command_event is not None else None
            command_payload = parse_record_payload(command_event.get("record_json")) if command_event is not None else None
            merged["command_cwd"] = (
                str(command_payload.get("cwd") or "").strip()
                if isinstance(command_payload, dict)
                else (command_call_workdir(event) or None)
            )
            duration = command_payload.get("duration") if isinstance(command_payload, dict) else None
            merged["duration_seconds"] = coerce_duration_seconds(duration)
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
            merged["warning_lines"] = extract_warning_lines(output_text)
            merged["warning_count"] = len(merged["warning_lines"])

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

            tool_output = ""
            if tool_result_event is not None:
                tool_output = str(tool_result_event.get("detail_text") or "").strip()
                if tool_output and tool_output != str(tool_result_event.get("display_text") or "").strip():
                    merged_detail = f"{merged_detail}\n\nTool Output:\n{tool_output}".strip()

            merged = dict(event)
            merged["kind"] = "tool_call"
            merged["group_key"] = "patch"
            merged["style"] = kind_style("tool_call")
            merged["title"] = "Patch"
            merged["summary_text"] = display_text.strip()
            merged["raw_patch_text"] = str(event.get("display_text") or "").strip()
            merged["patch_metadata_text"], _ = split_tool_output(merged_detail)
            merged["tool_output_text"] = tool_output
            merged["patch_files"] = parse_patch_files(merged_detail)
            merged["patch_manifest"] = parse_patch_manifest(merged_detail)
            merged["patch_file_count"] = len(merged["patch_files"])
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
    return grouped_work_entries_from_merged(merge_compound_tool_events(detail_events))


def _enumish_label(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip().replace("_", " ").replace("-", " ")


def _sandbox_label(value: object) -> str | None:
    if isinstance(value, dict):
        sandbox_type = value.get("type")
        return _enumish_label(sandbox_type)
    return _enumish_label(value)


def _network_label(payload: dict[str, object]) -> str | None:
    sandbox_policy = payload.get("sandbox_policy")
    if isinstance(sandbox_policy, dict):
        network_access = sandbox_policy.get("network_access")
        if isinstance(network_access, bool):
            return "enabled" if network_access else "disabled"
        if isinstance(network_access, str) and network_access.strip():
            return _enumish_label(network_access)
    network = payload.get("network")
    if isinstance(network, dict):
        allowed = network.get("allowed_domains")
        denied = network.get("denied_domains")
        allowed_count = len([item for item in allowed if isinstance(item, str) and item.strip()]) if isinstance(allowed, list) else 0
        denied_count = len([item for item in denied if isinstance(item, str) and item.strip()]) if isinstance(denied, list) else 0
        if allowed_count or denied_count:
            parts: list[str] = []
            if allowed_count:
                parts.append(f"{allowed_count} allow")
            if denied_count:
                parts.append(f"{denied_count} deny")
            return "restricted " + " / ".join(parts)
        return "custom"
    return None


def _truncation_label(value: object) -> str | None:
    if not isinstance(value, dict):
        return None
    mode = _enumish_label(value.get("mode"))
    limit = value.get("limit")
    if mode and isinstance(limit, int):
        return f"{mode} {limit:,}"
    return mode


def turn_context_snapshot(events: list[sqlite3.Row], *, cwd: str | None = None) -> dict[str, str]:
    snapshot: dict[str, str] = {}
    for event in events:
        if str(event["record_type"] or "") != "turn_context":
            continue
        raw_record = str(event["detail_text"] or event["record_json"] or "").strip()
        if not raw_record:
            continue
        try:
            record = json.loads(raw_record)
        except json.JSONDecodeError:
            continue
        payload = record.get("payload") if isinstance(record, dict) else None
        if not isinstance(payload, dict) and isinstance(record, dict):
            payload = record
        if not isinstance(payload, dict):
            continue
        model = str(payload.get("model") or "").strip()
        effort = str(payload.get("effort") or "").strip()
        approval = _enumish_label(payload.get("approval_policy"))
        sandbox = _sandbox_label(payload.get("sandbox_policy"))
        network = _network_label(payload)
        collaboration_mode = payload.get("collaboration_mode")
        collaboration = None
        if isinstance(collaboration_mode, dict):
            collaboration = _enumish_label(collaboration_mode.get("mode"))
        else:
            collaboration = _enumish_label(collaboration_mode)
        truncation = _truncation_label(payload.get("truncation_policy"))
        turn_cwd = str(payload.get("cwd") or "").strip()

        if model:
            snapshot["model"] = model
        if effort:
            snapshot["effort"] = effort
        if approval:
            snapshot["approval_policy"] = approval
        if sandbox:
            snapshot["sandbox_policy"] = sandbox
        if network:
            snapshot["network_access"] = network
        if collaboration:
            snapshot["collaboration_mode"] = collaboration
        if truncation:
            snapshot["truncation_policy"] = truncation
        if turn_cwd:
            snapshot["cwd_display"] = collapse_path_for_display(turn_cwd, cwd)
    return snapshot


def turn_context_details(events: list[sqlite3.Row], *, cwd: str | None = None) -> tuple[str | None, str | None, dict[str, str]]:
    snapshot = turn_context_snapshot(events, cwd=cwd)
    return snapshot.get("model"), snapshot.get("effort"), snapshot


def build_turns(
    events: list[sqlite3.Row],
    *,
    cwd: str | None = None,
    starting_turn_number: int = 1,
) -> list[dict[str, object]]:
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
            response_state = "update" if is_assistant_update(final_response_event) else "final"
            response_label = "Latest Update" if response_state == "update" else "Final Response"
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
        agent_model, agent_effort, execution_context = turn_context_details(all_events, cwd=cwd)
        turn_duration_seconds = task_duration_seconds(
            completion_event,
            str(turn["prompt_timestamp"]) if turn.get("prompt_timestamp") else None,
            str(response_timestamp) if response_timestamp else None,
            all_events,
        )

        detail_events: list[dict[str, object]] = []
        for event in all_events:
            skip = False
            if event["kind"] == "telemetry":
                skip = True
            if event["kind"] == "reasoning":
                skip = True
            if event["record_type"] == "turn_context":
                skip = True
            if event["record_type"] == "event_msg" and event["payload_type"] in {"task_started", "turn_started"}:
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
            if skip:
                continue
            detail_events.append(styled_event(event))

        merged_detail_events = merge_compound_tool_events(detail_events)
        for event in merged_detail_events:
            if str(event.get("kind") or "") == "command":
                raw_command_text = str(event.get("command_text") or "").strip()
                fallback_command_text = raw_command_text or str(event.get("summary_text") or "").strip()
                display_command_text, command_wrapper = unwrap_command_display(fallback_command_text)
                event["display_command_text"] = display_command_text or fallback_command_text or None
                event["command_wrapper"] = command_wrapper
                event["primary_command_label"] = command_primary_label(display_command_text or fallback_command_text)
                verification_kind = classify_verification_command(display_command_text or fallback_command_text)
                event["verification_kind"] = verification_kind
                event["is_verification"] = bool(verification_kind)
                event["command_intent"] = command_intent(display_command_text or fallback_command_text, verification_kind)
                event["has_failure"] = event.get("exit_code") not in (None, 0)
                event["output_preview"], event["output_preview_truncated"] = event_preview_text(str(event.get("output_text") or ""))
                event["duration_label"] = humanize_duration(event.get("duration_seconds")) if event.get("duration_seconds") is not None else ""
                event["cwd_display"] = collapse_path_for_display(str(event.get("command_cwd") or ""), cwd)
            elif str(event.get("group_key") or "") == "patch":
                patch_files = event.get("patch_files")
                if not patch_files:
                    patch_files = parse_patch_files(str(event.get("detail_text") or ""), cwd=cwd)
                    event["patch_files"] = patch_files
                    event["patch_file_count"] = len(patch_files)
                raw_patch_text = str(event.get("raw_patch_text") or "")
                event["patch_diff_lines"] = patch_diff_lines(raw_patch_text, cwd=cwd)
                if not event.get("patch_manifest"):
                    event["patch_manifest"] = parse_patch_manifest(str(event.get("detail_text") or ""), cwd=cwd)

        detail_entries = grouped_work_entries_from_merged(merged_detail_events)
        plan_events = [event for event in merged_detail_events if event.get("plan_update")]
        command_events = [event for event in merged_detail_events if str(event.get("kind") or "") == "command"]
        patch_events = [event for event in merged_detail_events if str(event.get("group_key") or "") == "patch"]
        research_events = [event for event in merged_detail_events if is_research_event(event)]
        context_events = [event for event in merged_detail_events if is_context_event(event)]
        assistant_updates = [
            event
            for event in merged_detail_events
            if str(event.get("kind") or "") == "message" and str(event.get("role") or "") == "assistant"
        ]
        verification_commands = [event for event in command_events if event.get("is_verification")]
        files_touched = sorted(
            {
                str(file_item.get("path") or "")
                for event in patch_events
                for file_item in event.get("patch_files", [])
                if str(file_item.get("path") or "")
            }
        )
        failed_commands = [event for event in command_events if event.get("has_failure")]
        warning_commands = [event for event in command_events if int(event.get("warning_count") or 0) > 0]
        verification_state = verification_status_label(command_events)
        verification_verdict_data = verification_verdict(command_events)
        file_manifest = aggregate_file_manifest(patch_events)
        risky_files = risky_file_manifest(file_manifest)
        inspect_count = sum(1 for event in command_events if event.get("command_intent") == "inspect")
        mutate_command_count = sum(1 for event in command_events if event.get("command_intent") == "mutate")
        phase_strip = build_phase_strip(
            plan_count=len(plan_events),
            research_count=len(research_events),
            inspect_count=inspect_count,
            modify_count=len(patch_events) + mutate_command_count,
            verification_count=len(verification_commands),
            response_present=bool(response_text),
        )
        response_evidence_data = response_evidence(
            turn_number=int(turn["number"]),
            response_text=response_text,
            command_events=command_events,
            patch_events=patch_events,
            research_events=research_events,
            verification_verdict_data=verification_verdict_data,
            file_manifest=file_manifest,
        )
        trust_signals = build_trust_signals(
            turn_number=int(turn["number"]),
            verification_verdict_data=verification_verdict_data,
            response_evidence_data=response_evidence_data,
            risky_files=risky_files,
        )

        return {
            "number": turn["number"],
            "prompt_text": prompt_text,
            "prompt_segments": prompt_segments(prompt_text),
            "prompt_excerpt": prompt_excerpt,
            "prompt_timestamp": turn["prompt_timestamp"],
            "duration_seconds": turn_duration_seconds,
            "duration_label": humanize_duration(turn_duration_seconds) if turn_duration_seconds is not None else "",
            "agent_model": agent_model,
            "agent_effort": agent_effort,
            "audit_execution_context": execution_context,
            "response_text": response_text,
            "response_excerpt": response_excerpt,
            "response_timestamp": response_timestamp,
            "response_state": response_state,
            "response_label": response_label,
            "detail_events": detail_events,
            "merged_detail_events": merged_detail_events,
            "detail_entries": detail_entries,
            "work_count": len(detail_entries),
            "audit_plan_events": plan_events,
            "audit_command_events": command_events,
            "audit_patch_events": patch_events,
            "audit_research_events": research_events,
            "audit_context_events": context_events,
            "audit_assistant_updates": assistant_updates,
            "audit_verification_commands": verification_commands,
            "audit_verification_state": verification_state,
            "audit_verification_verdict": verification_verdict_data,
            "audit_files_touched": files_touched,
            "audit_file_manifest": file_manifest,
            "audit_failed_commands": failed_commands,
            "audit_warning_commands": warning_commands,
            "audit_phase_strip": phase_strip,
            "audit_response_evidence": response_evidence_data,
            "audit_risky_files": risky_files,
            "audit_trust_signals": trust_signals,
            "audit_summary": {
                "command_count": len(command_events),
                "patch_count": len(patch_events),
                "research_count": len(research_events),
                "context_count": len(context_events),
                "files_touched_count": len(files_touched),
                "verification_count": len(verification_commands),
                "verification_state": verification_state,
                "warning_count": len(warning_commands),
                "failure_count": len(failed_commands),
                "canceled": response_state == "canceled",
                "evidence_mismatch_count": int(response_evidence_data.get("warning_count") or 0),
                "trust_signal_count": len(trust_signals),
                "risky_file_count": len(risky_files),
            },
        }

    for event in events:
        if is_user_turn_start(event, prefer_event_msg):
            cleaned_prompt = strip_codex_wrappers_preserve_layout(str(event["display_text"] or "")).strip()
            if not cleaned_prompt:
                continue
            if current is not None:
                turns.append(finalize_turn(current))
            current = {
                "number": starting_turn_number + len(turns),
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

    previous_plan_steps: list[dict[str, str]] = []
    for turn in turns:
        for event in turn["audit_plan_events"]:
            current_steps = event.get("plan_steps") if isinstance(event.get("plan_steps"), list) else []
            event["plan_diff"] = plan_diff(previous_plan_steps, current_steps)
            event["has_plan_diff"] = any(event["plan_diff"].values())
            previous_plan_steps = current_steps

    return turns


def build_session_audit_summary(
    session: sqlite3.Row | dict[str, object],
    turns: list[dict[str, object]],
) -> dict[str, object]:
    session_row = dict(session)
    files_changed = sorted(
        {
            path
            for turn in turns
            for path in turn.get("audit_files_touched", [])
            if isinstance(path, str) and path
        }
    )
    total_commands = sum(int(turn["audit_summary"]["command_count"]) for turn in turns)
    total_patches = sum(int(turn["audit_summary"]["patch_count"]) for turn in turns)
    total_research = sum(int(turn["audit_summary"].get("research_count") or 0) for turn in turns)
    total_context_events = sum(int(turn["audit_summary"].get("context_count") or 0) for turn in turns)
    total_failures = sum(int(turn["audit_summary"]["failure_count"]) for turn in turns)
    total_warnings = sum(int(turn["audit_summary"]["warning_count"]) for turn in turns)
    total_verification = sum(int(turn["audit_summary"]["verification_count"]) for turn in turns)
    review_signal_turn_count = sum(1 for turn in turns if int(turn["audit_summary"].get("trust_signal_count") or 0) > 0)
    verification_warning_turn_count = sum(
        1
        for turn in turns
        if any(signal.get("key") == "verification_warnings" for signal in turn.get("audit_trust_signals", []))
    )
    claim_mismatch_turn_count = sum(
        1
        for turn in turns
        if any(signal.get("key") == "claim_evidence_mismatch" for signal in turn.get("audit_trust_signals", []))
    )
    risky_unverified_turn_count = sum(
        1
        for turn in turns
        if any(signal.get("key") == "risky_changes_unverified" for signal in turn.get("audit_trust_signals", []))
    )
    return {
        "started_at": session_row.get("session_timestamp") or session_row.get("started_at"),
        "ended_at": session_row.get("ended_at"),
        "turn_count": len(turns),
        "command_count": total_commands,
        "patch_count": total_patches,
        "research_count": total_research,
        "context_event_count": total_context_events,
        "files_touched_count": len(files_changed),
        "files_changed": files_changed,
        "failed_command_count": total_failures,
        "warning_count": total_warnings,
        "aborted_turn_count": int(session_row.get("aborted_turn_count") or 0),
        "verification_count": total_verification,
        "review_signal_turn_count": review_signal_turn_count,
        "verification_warning_turn_count": verification_warning_turn_count,
        "claim_mismatch_turn_count": claim_mismatch_turn_count,
        "risky_unverified_turn_count": risky_unverified_turn_count,
        "git_branch": str(session_row.get("git_branch") or "").strip(),
        "git_commit_hash": str(session_row.get("git_commit_hash") or "").strip(),
    }
