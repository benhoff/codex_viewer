from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime
import json
import threading
from typing import Literal
from urllib.parse import parse_qs, quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.concurrency import run_in_threadpool

from ...db import connect, connection_scope, write_transaction
from ...assessment_dashboard import dashboard_data, grading_dashboard_data
from ... import llm_grader as grader
from ...projects import build_project_access_context, fetch_session_with_project, row_is_visible_to_project_access
from ...saved_turns import owner_scope_from_request
from ...task_assessment import (
    DEFAULT_POLICY, DEMAND_FIELDS, JUDGMENTS, TEXT_FIELDS, empty_review, read_revision,
    canonical_json, digest, report_for_source, revisions, save_revision, task_source, validate_policy, validate_review,
)
from ..auth import require_admin_user
from ..context import get_app_context
from .pages import render_settings_page

router = APIRouter()
PRIVATE_HEADERS = {"Cache-Control": "private, no-store"}


@router.get("/assessments", response_class=HTMLResponse)
@router.get("/assessments.json", response_class=JSONResponse)
def assessment_dashboard(request: Request, machine: str = Query("", max_length=200),
                         project: str = Query("", max_length=500),
                         review: Literal["all", "reviewed", "unreviewed"] = "all",
                         view: Literal["runs", "sessions"] = "runs",
                         status: Literal["all", "running", "completed", "failed", "cancelled", "interrupted"] = "all",
                         days: int = Query(0, ge=0, le=3650), page: int = Query(1, ge=1, le=1000000)):
    context = get_app_context(request)
    with closing(connect(context.settings.database_path)) as connection:
        with connection:
            connection.execute("BEGIN")
            access = build_project_access_context(connection, auth_user=getattr(request.state, "auth_user", None),
                                                  auth_enabled=bool(getattr(request.state, "auth_enabled", False)))
            if view == "runs":
                with grader.ACTIVE_RUNS_LOCK:
                    active_ids = [run_id for database, run_id in grader.ACTIVE_RUNS if database == str(context.settings.database_path)]
                data = grading_dashboard_data(connection, owner=owner_scope_from_request(request), access=access,
                                              machine=machine, project=project, status=status, days=days, page=page,
                                              active_run_ids=active_ids)
            else:
                data = dashboard_data(connection, owner=owner_scope_from_request(request), access=access,
                                      machine=machine, project=project, review=review, days=days, page=page)
    if request.url.path.endswith(".json"):
        return JSONResponse(data, headers=PRIVATE_HEADERS)
    return context.templates.TemplateResponse(request, name="grading_dashboard.html" if view == "runs" else "assessments.html",
                                              context={"request": request, **data}, headers=PRIVATE_HEADERS)


def visible_session(connection, request: Request, session_id: str) -> dict:
    access = build_project_access_context(connection, auth_user=getattr(request.state, "auth_user", None),
                                          auth_enabled=bool(getattr(request.state, "auth_enabled", False)))
    session = fetch_session_with_project(connection, session_id)
    if session is None or not row_is_visible_to_project_access(session, access):
        raise HTTPException(404, "Session not found")
    return dict(session)


def load_assessment(request: Request, session_id: str, start: int, end: int, revision: int | None) -> dict:
    context = get_app_context(request)
    owner = owner_scope_from_request(request)
    try:
        with connection_scope(context.settings.database_path) as connection:
            session = visible_session(connection, request, session_id)
            with connection:  # Pin source and review reads to one SQLite snapshot.
                connection.execute("BEGIN")
                history = revisions(connection, owner, session_id, start, end)
                selected = revision if revision is not None else history[0]["id"] if history else None
                saved = read_revision(connection, owner, session_id, start, end, selected) if selected is not None else None
                try:
                    source = task_source(connection, session, start, end)
                except LookupError:
                    if revision is None:
                        raise
                    # A pinned review remains inspectable after turn removal or
                    # resegmentation, provided the session is still accessible.
                    source = None
                policy = saved["policy"] if saved else validate_policy(DEFAULT_POLICY)
                report = saved["snapshot"] if revision is not None else report_for_source(source, policy)
                grader_config = grader.load_config(connection)
                grade_run = grader.read_run(connection, owner, session_id, start, end) if revision is None else None
                grade_history = [dict(row) for row in connection.execute(
                    "SELECT id, status, created_at FROM task_grader_runs WHERE owner_scope = ? AND session_id = ? "
                    "AND start_turn = ? AND end_turn = ? ORDER BY id DESC LIMIT 20", (owner, session_id, start, end))]
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    current_digest = source["evidence_digest"] if source else None
    if grade_run:
        grade_run["stale"] = grade_run["evidence_digest"] != current_digest
        grade_run["context_outdated"] = grade_run["result"].get("prompt_version") != grader.PROMPT_VERSION
        with grader.ACTIVE_RUNS_LOCK:
            grade_run["active"] = (str(context.settings.database_path), grade_run["id"]) in grader.ACTIVE_RUNS
        grade_run["completed_batches"] = sum(b["status"] == "completed" for b in grade_run["result"].get("batches", []))
    plan = None
    if grader_config["enabled"] and not revision:
        try:
            preview_criteria = grade_run["result"].get("acceptance_criteria", "") if grade_run else saved["review"]["acceptance_criteria"] if saved else ""
            preview = grader.grader_input(report, preview_criteria, grader_config)
            plan = {"batches": len(preview.get("demand_batches", [])) or 1,
                    "synthesis": bool(preview.get("demand_batches")), **preview.get("preflight", {})}
        except ValueError as exc:
            plan = {"error": str(exc)}
    return {"report": report, "review": saved["review"] if saved else empty_review(),
            "grade_run": grade_run, "grade_history": grade_history, "grader_config": grader_config, "grade_plan": plan,
            "saved": {key: saved[key] for key in ("id", "created_at", "evidence_digest")} if saved else None,
            "history": history, "stale": bool(saved and saved["evidence_digest"] != current_digest),
            "historical": revision is not None, "session_title": session.get("summary") or session_id,
            "current_evidence_digest": current_digest}


@router.get("/sessions/{session_id}/assessment", response_class=HTMLResponse)
def assessment_page(request: Request, session_id: str, start_turn: int = Query(1, ge=1),
                    end_turn: int | None = Query(None, ge=1), revision: int | None = Query(None, ge=1)):
    data = load_assessment(request, session_id, start_turn, end_turn if end_turn is not None else start_turn, revision)
    return get_app_context(request).templates.TemplateResponse(
        request, name="assessment.html", context={"request": request, **data,
        "judgments": JUDGMENTS, "demand_fields": DEMAND_FIELDS, "text_fields": TEXT_FIELDS,
        "policy_text": json.dumps(data["report"]["policy"], indent=2),
        "readable_evidence": evidence_display(data["report"]),
        "session_href": f"/sessions/{quote(session_id, safe='')}",
        }, headers=PRIVATE_HEADERS)


def evidence_display(report):
    return {e["event_index"]: e["text"] for e in grader.clean_events(report["evidence"])}


@router.get("/sessions/{session_id}/assessment.json")
def assessment_json(request: Request, session_id: str, start_turn: int = Query(1, ge=1),
                    end_turn: int | None = Query(None, ge=1), revision: int | None = Query(None, ge=1)):
    return JSONResponse(load_assessment(request, session_id, start_turn, end_turn if end_turn is not None else start_turn, revision),
                        headers=PRIVATE_HEADERS)


def persist_assessment(request: Request, session_id: str, fields: dict):
    try:
        start = int(fields.get("start_turn", "0"))
        end = int(fields.get("end_turn", "0"))
        policy = validate_policy(json.loads(fields.get("policy", "{}")))
        context = get_app_context(request)
        owner = owner_scope_from_request(request)
        with connection_scope(context.settings.database_path) as connection:
            with write_transaction(connection):
                session = visible_session(connection, request, session_id)
                source = task_source(connection, session, start, end)
                if fields.get("evidence_digest") != source["evidence_digest"]:
                    raise HTTPException(409, "Task evidence changed. Reload the assessment before saving.")
                review = validate_review(fields, {event["event_index"] for event in source["events"]})
                report = report_for_source(source, policy)
                save_revision(connection, owner, report, review)
    except (ValueError, TypeError, OverflowError) as exc:
        raise HTTPException(400, str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    return RedirectResponse(f"/sessions/{quote(session_id, safe='')}/assessment?start_turn={start}&end_turn={end}",
                            status_code=303, headers=PRIVATE_HEADERS)


async def assessment_form(request: Request) -> dict:
    origin = request.headers.get("origin")
    if origin and origin.rstrip("/") != str(request.base_url).rstrip("/"):
        raise HTTPException(403, "Cross-origin review writes are not allowed.")
    if request.headers.get("sec-fetch-site") == "cross-site":
        raise HTTPException(403, "Cross-site review writes are not allowed.")
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 131_072:
            raise HTTPException(413, "Assessment form exceeds 128 KiB.")
    try:
        parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True, max_num_fields=50)
        fields = {key: values[-1] for key, values in parsed.items()}
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(400, "Invalid assessment form.") from exc
    return fields


@router.post("/sessions/{session_id}/assessment")
async def assessment_save(request: Request, session_id: str):
    return await run_in_threadpool(persist_assessment, request, session_id, await assessment_form(request))


@router.get("/settings/grader", response_class=HTMLResponse)
def grader_settings(request: Request):
    require_admin_user(request)
    return RedirectResponse("/settings#settings-llm", status_code=303, headers=PRIVATE_HEADERS)


@router.post("/settings/grader", response_class=HTMLResponse)
async def grader_settings_save(request: Request):
    require_admin_user(request)
    fields = await assessment_form(request)
    try:
        api_key = fields.get("api_key", "").strip()
        remove_key = fields.get("remove_api_key") == "on"
        if remove_key and api_key:
            raise ValueError("Enter a replacement API key or select Remove saved API key, not both.")
        config = grader.validate_config({"enabled": fields.get("enabled") == "on",
            "synthesis_output_tokens": int(fields.get("synthesis_output_tokens", grader.DEFAULT_CONFIG["synthesis_output_tokens"])),
            **{key: fields.get(key, "") for key in ("processing", "base_url", "model", "response_format")},
            **{key: int(fields.get(key, "0")) for key in ("timeout_seconds", "max_input_chars", "max_output_tokens")}})
        with closing(connect(get_app_context(request).settings.database_path)) as connection, write_transaction(connection):
            if api_key or remove_key:
                grader.save_api_key(connection, get_app_context(request).settings.data_dir, api_key)
            grader.save_config(connection, config)
    except ValueError as exc:
        response = render_settings_page(request, grader_error=str(exc))
        response.status_code = 400
        return response
    return RedirectResponse("/settings#settings-llm", status_code=303, headers=PRIVATE_HEADERS)


def run_grader(request: Request, session_id: str, fields: dict, *, background: bool = False):
    context = get_app_context(request)
    owner = owner_scope_from_request(request)
    if not grader.GRADER_LOCK.acquire(blocking=False):
        raise HTTPException(429, "A grader request is already running. Try again after it finishes.", headers=PRIVATE_HEADERS)
    worker_owns_lock = False
    try:
        try:
            start, end = int(fields.get("start_turn", "0")), int(fields.get("end_turn", "0"))
            criteria = fields.get("acceptance_criteria", "").strip()
            if len(criteria) > 12000:
                raise ValueError("Acceptance criteria must be at most 12,000 characters.")
            with closing(connect(context.settings.database_path)) as connection, write_transaction(connection):
                session = visible_session(connection, request, session_id)
                config = grader.load_config(connection)
                if not config["enabled"]:
                    raise HTTPException(409, "The LLM grader is disabled.", headers=PRIVATE_HEADERS)
                api_key = grader.load_api_key(connection, context.settings.data_dir)
                if config["processing"] == "external" and not api_key:
                    raise HTTPException(409, "Save an API key in Settings → LLM Configuration before external grading.", headers=PRIVATE_HEADERS)
                source = task_source(connection, session, start, end)
                if source["evidence_digest"] != fields.get("evidence_digest"):
                    raise HTTPException(409, "Evidence changed. Reload before requesting grading.", headers=PRIVATE_HEADERS)
                report = report_for_source(source, validate_policy(DEFAULT_POLICY))
                inputs = grader.grader_input(report, criteria, config)
                result = {"evidence_level": "llm_trace_estimate", "prompt_version": grader.PROMPT_VERSION,
                          "rubric": grader.RUBRIC, "prompts": {"demand": grader.DEMAND_PROMPT, "configuration": grader.CONFIG_PROMPT,
                                                               "extraction": grader.EXTRACTION_PROMPT, "synthesis": grader.SYNTHESIS_PROMPT},
                          "schemas": {"demand": grader.DemandGrade.model_json_schema(), "configuration": grader.ConfigGrade.model_json_schema(),
                                      "extraction": grader.EvidenceNotes.model_json_schema()},
                          "input_hash": digest(inputs), "calls": [], "acceptance_criteria": criteria,
                          "batches": grader.batch_summary(inputs)}
                retry_id = fields.get("retry_run_id")
                if retry_id:
                    previous = connection.execute("SELECT * FROM task_grader_runs WHERE id = ? AND session_id = ? AND owner_scope = ?",
                                                  (int(retry_id), session_id, owner)).fetchone()
                    if previous is None:
                        raise HTTPException(404, "Grader run not found", headers=PRIVATE_HEADERS)
                    prior_result = json.loads(previous["result_json"])
                    if (previous["status"] not in {"failed", "running", "cancelled"} or previous["evidence_digest"] != source["evidence_digest"]
                            or previous["start_turn"] != start or previous["end_turn"] != end
                            or prior_result.get("input_hash") != digest(inputs) or json.loads(previous["config_json"]) != config
                            or prior_result.get("prompt_version") != grader.PROMPT_VERSION):
                        raise HTTPException(409, "Evidence, criteria, or configuration changed. Submit a new grading run instead of retrying.", headers=PRIVATE_HEADERS)
                    result = prior_result
                    result.pop("error", None)
                    for call in result["calls"]:
                        if call["status"] == "started":
                            call["status"] = "interrupted"
                    run_id = previous["id"]
                    connection.execute("UPDATE task_grader_runs SET status = 'running', completed_at = NULL, result_json = ? WHERE id = ?",
                                       (canonical_json(result), run_id))
                else:
                    cursor = connection.execute("INSERT INTO task_grader_runs "
                        "(owner_scope, session_id, start_turn, end_turn, created_at, status, evidence_digest, config_json, snapshot_json, result_json) "
                        "VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?, ?)",
                        (owner, session_id, start, end, datetime.now(UTC).isoformat(), source["evidence_digest"],
                         canonical_json(config), canonical_json({"report": report, "inputs": inputs}), canonical_json(result)))
                    run_id = cursor.lastrowid
        except ValueError as exc:
            raise HTTPException(400, str(exc), headers=PRIVATE_HEADERS) from exc
        except LookupError as exc:
            raise HTTPException(404, str(exc), headers=PRIVATE_HEADERS) from exc
        destination = f"/sessions/{quote(session_id, safe='')}/assessment?start_turn={start}&end_turn={end}#llm-grader"
        job_key = (str(context.settings.database_path), run_id)
        with grader.ACTIVE_RUNS_LOCK:
            grader.ACTIVE_RUNS.add(job_key)
            grader.RUN_CONTROLS[job_key] = grader.RunControl()
        if background:
            worker = threading.Thread(target=execute_grader, args=(context.settings.database_path, run_id, inputs, config, api_key, result),
                                      kwargs={"release_lock": True}, daemon=True)
            try:
                worker.start()
            except Exception:
                with grader.ACTIVE_RUNS_LOCK:
                    grader.ACTIVE_RUNS.discard(job_key)
                    grader.RUN_CONTROLS.pop(job_key, None)
                raise
            worker_owns_lock = True
            return JSONResponse({"run_id": run_id, "status_url": f"/sessions/{quote(session_id, safe='')}/assessment/grader/{run_id}/status",
                                 "result_url": destination}, status_code=202, headers=PRIVATE_HEADERS)
        execute_grader(context.settings.database_path, run_id, inputs, config, api_key, result)
        with closing(connect(context.settings.database_path)) as connection:
            visible_session(connection, request, session_id)
        return RedirectResponse(destination, status_code=303, headers=PRIVATE_HEADERS)
    finally:
        if not worker_owns_lock:
            grader.GRADER_LOCK.release()


def execute_grader(database_path, run_id, inputs, config, api_key, result, *, release_lock=False):
    job_key = (str(database_path), run_id)
    with grader.ACTIVE_RUNS_LOCK:
        grader.WORKER_STATE.control = grader.RUN_CONTROLS[job_key]
    def checkpoint():
        with closing(connect(database_path)) as connection, write_transaction(connection):
            connection.execute("UPDATE task_grader_runs SET result_json = ? WHERE id = ?", (canonical_json(result), run_id))
    try:
        status = "completed"
        try:
            grader.grade(inputs, config, api_key, result, checkpoint)
            grader.WORKER_STATE.control.check()
        except grader.GraderCancelled as exc:
            status, result["error"] = "cancelled", str(exc)
        except grader.GraderError as exc:
            status, result["error"] = "failed", str(exc)
        except Exception:
            status = "failed"
            result["error"] = "The grader could not complete this request. Check server configuration and retry unfinished batches."
        if status != "completed" and result["calls"] and result["calls"][-1]["status"] == "started":
            result["calls"][-1]["status"] = status
        with closing(connect(database_path)) as connection, write_transaction(connection):
            connection.execute("UPDATE task_grader_runs SET status = ?, completed_at = ?, result_json = ? WHERE id = ?",
                               (status, datetime.now(UTC).isoformat(), canonical_json(result), run_id))
    finally:
        with grader.ACTIVE_RUNS_LOCK:
            grader.ACTIVE_RUNS.discard(job_key)
            grader.RUN_CONTROLS.pop(job_key, None)
            if release_lock:
                grader.GRADER_LOCK.release()
        del grader.WORKER_STATE.control


@router.post("/sessions/{session_id}/assessment/grade")
async def assessment_grade(request: Request, session_id: str):
    return await run_in_threadpool(run_grader, request, session_id, await assessment_form(request),
                                  background="application/json" in request.headers.get("accept", ""))


@router.get("/sessions/{session_id}/assessment/grader/{run_id}/status")
def grader_status(request: Request, session_id: str, run_id: int):
    context = get_app_context(request)
    with closing(connect(context.settings.database_path)) as connection:
        visible_session(connection, request, session_id)
        row = connection.execute("SELECT status, result_json FROM task_grader_runs WHERE id = ? AND session_id = ? AND owner_scope = ?",
                                 (run_id, session_id, owner_scope_from_request(request))).fetchone()
    if row is None:
        raise HTTPException(404, "Grader run not found", headers=PRIVATE_HEADERS)
    result = json.loads(row["result_json"])
    with grader.ACTIVE_RUNS_LOCK:
        active = (str(context.settings.database_path), run_id) in grader.ACTIVE_RUNS
    batches = result.get("batches", [])
    return JSONResponse({"status": "running" if active else "interrupted" if row["status"] == "running" else row["status"],
                         "completed_batches": sum(batch["status"] == "completed" for batch in batches) if batches else int("demand" in result),
                         "total_batches": len(batches) or 1, "configuration_complete": "configuration" in result,
                         "stage": result["calls"][-1]["stage"] if result.get("calls") else "preparing",
                         "synthesis_complete": "demand" in result}, headers=PRIVATE_HEADERS)


@router.post("/sessions/{session_id}/assessment/grader/{run_id}/cancel")
async def grader_cancel(request: Request, session_id: str, run_id: int):
    await assessment_form(request)  # Same origin protection as submission.
    context = get_app_context(request)
    with closing(connect(context.settings.database_path)) as connection:
        visible_session(connection, request, session_id)
        row = connection.execute("SELECT id FROM task_grader_runs WHERE id = ? AND session_id = ? AND owner_scope = ?",
                                 (run_id, session_id, owner_scope_from_request(request))).fetchone()
    if row is None:
        raise HTTPException(404, "Grader run not found", headers=PRIVATE_HEADERS)
    with grader.ACTIVE_RUNS_LOCK:
        control = grader.RUN_CONTROLS.get((str(context.settings.database_path), run_id))
        if control:
            control.cancel()
    return JSONResponse({"status": "cancelling" if control else "inactive"}, headers=PRIVATE_HEADERS)


def load_grade_run(request: Request, session_id: str, run_id: int):
    context = get_app_context(request)
    with closing(connect(context.settings.database_path)) as connection:
        with connection:
            connection.execute("BEGIN")
            session = visible_session(connection, request, session_id)
            row = connection.execute("SELECT * FROM task_grader_runs WHERE id = ? AND session_id = ? AND owner_scope = ?",
                                     (run_id, session_id, owner_scope_from_request(request))).fetchone()
            if row is None:
                raise HTTPException(404, "Grader run not found", headers=PRIVATE_HEADERS)
            run = grader.read_run(connection, owner_scope_from_request(request), session_id, row["start_turn"], row["end_turn"], run_id=run_id)
            try:
                source = task_source(connection, session, row["start_turn"], row["end_turn"])
                run["stale"] = source["evidence_digest"] != row["evidence_digest"]
            except (ValueError, LookupError):
                run["stale"] = True
            run["snapshot"] = json.loads(row["snapshot_json"])
            run["context_outdated"] = run["result"].get("prompt_version") != grader.PROMPT_VERSION
            run["completed_batches"] = sum(b["status"] == "completed" for b in run["result"].get("batches", []))
            with grader.ACTIVE_RUNS_LOCK:
                run["active"] = (str(context.settings.database_path), run_id) in grader.ACTIVE_RUNS
    return run


@router.get("/sessions/{session_id}/assessment/grader/{run_id}.json")
def grader_export(request: Request, session_id: str, run_id: int):
    run = load_grade_run(request, session_id, run_id)
    return JSONResponse(run, headers=PRIVATE_HEADERS)


@router.get("/sessions/{session_id}/assessment/grader/{run_id}", response_class=HTMLResponse)
def grader_run_page(request: Request, session_id: str, run_id: int):
    run = load_grade_run(request, session_id, run_id)
    report = run["snapshot"]["report"]
    return get_app_context(request).templates.TemplateResponse(request, name="assessment.html", context={
        "request": request, "report": report, "grade_run": run, "frozen_grade": True,
        "grader_config": run["config"], "grade_plan": None, "grade_history": [],
        "historical": False, "saved": None, "history": [], "review": empty_review(),
        "current_evidence_digest": run["evidence_digest"], "session_title": f"Saved grading run {run_id}",
        "session_href": f"/sessions/{quote(session_id, safe='')}",
        "judgments": JUDGMENTS, "demand_fields": DEMAND_FIELDS, "text_fields": TEXT_FIELDS,
        "policy_text": json.dumps(report["policy"], indent=2),
        "readable_evidence": evidence_display(report),
    }, headers=PRIVATE_HEADERS)
