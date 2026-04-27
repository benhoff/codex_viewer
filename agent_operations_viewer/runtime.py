from __future__ import annotations

import json
import sqlite3

from .session_exports import build_execution_context_export


def get_events(connection: sqlite3.Connection, session_id: str) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM events
        WHERE session_id = ?
        ORDER BY event_index ASC
        """,
        (session_id,),
    ).fetchall()


def export_markdown(session: sqlite3.Row, events: list[sqlite3.Row]) -> str:
    execution_context = build_execution_context_export(session, events)
    lines = [
        f"# {session['summary']}",
        "",
        f"- Session ID: `{session['id']}`",
        f"- Timestamp: `{session['session_timestamp'] or session['started_at'] or 'unknown'}`",
        f"- CWD: `{session['cwd'] or 'unknown'}`",
        f"- Host: `{session['source_host'] or 'unknown'}`",
        f"- Model Provider: `{session['model_provider'] or 'unknown'}`",
        "",
    ]

    if any(value for value in execution_context.values()):
        lines.extend(
            [
                "## Execution Context",
                "",
                "~~~json",
                json.dumps(execution_context, indent=2, ensure_ascii=False, sort_keys=True),
                "~~~",
                "",
            ]
        )

    lines.extend(
        [
        "## Timeline",
        "",
        ]
    )

    for event in events:
        lines.append(f"### {event['title']}")
        lines.append("")
        lines.append(f"- Kind: `{event['kind']}`")
        if event["role"]:
            lines.append(f"- Role: `{event['role']}`")
        if event["timestamp"]:
            lines.append(f"- Timestamp: `{event['timestamp']}`")
        if event["tool_name"]:
            lines.append(f"- Tool: `{event['tool_name']}`")
        if event["command_text"]:
            lines.append(f"- Command: `{event['command_text']}`")
        if event["call_id"]:
            lines.append(f"- Call ID: `{event['call_id']}`")
        lines.append("")
        lines.append("~~~text")
        lines.append(event["detail_text"] or event["display_text"] or "")
        lines.append("~~~")
        lines.append("")

    return "\n".join(lines).strip() + "\n"
