from __future__ import annotations

from datetime import UTC, datetime
from io import BytesIO
import json
import sqlite3
import zipfile
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def export_json_payload(
    session: sqlite3.Row | dict[str, Any],
    events: list[sqlite3.Row | dict[str, Any]],
) -> dict[str, Any]:
    return {
        "session": dict(session),
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
    metadata = {
        "bundle_version": 1,
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
            "events.json",
            json.dumps(events_payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        )
        bundle.writestr("raw.jsonl", raw_jsonl)
    return output.getvalue()
