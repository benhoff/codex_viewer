"""Cooperative work deadlines and stage timings for one snapshot API request."""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import logging
import sqlite3
import time

SEARCH_REQUEST_TIMEOUT_SECONDS = 20.0
logger = logging.getLogger(__name__)


@dataclass
class _Work:
    started: float
    deadline: float
    stage_started: float
    stage: str = "authorization"
    timings: dict[str, float] = field(default_factory=dict)

    def record(self, now):
        self.timings[self.stage] = (
            self.timings.get(self.stage, 0.0) + now - self.stage_started
        )
        self.stage_started = now


class SearchWorkTimeout(TimeoutError):
    def __init__(self, work):
        self.stage = work.stage
        self.elapsed_seconds = round(time.monotonic() - work.started, 3)
        self.budget_seconds = work.deadline - work.started
        super().__init__(f"Search work deadline exceeded during {self.stage}")


_CURRENT: ContextVar[_Work | None] = ContextVar("search_work", default=None)


def mark_search_stage(stage):
    work = _CURRENT.get()
    if work is None:
        return
    now = time.monotonic()
    if now >= work.deadline:
        raise SearchWorkTimeout(work)
    work.record(now)
    work.stage = stage


@contextmanager
def search_work_budget(*connections, generation=None):
    """Shared across batch queries; does not include queueing or snapshot builds."""
    started = time.monotonic()
    work = _Work(started, started + SEARCH_REQUEST_TIMEOUT_SECONDS, started)
    token = _CURRENT.set(work)
    outcome = "ok"
    try:
        for connection in connections:
            connection.set_progress_handler(
                lambda: int(time.monotonic() >= work.deadline), 10000
            )
        try:
            yield
            mark_search_stage("complete")
        except sqlite3.OperationalError as exc:
            if (
                getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_INTERRUPT
                and time.monotonic() >= work.deadline
            ):
                raise SearchWorkTimeout(work) from exc
            raise
    except Exception as exc:
        outcome = "timeout" if isinstance(exc, SearchWorkTimeout) else "error"
        raise
    finally:
        work.record(time.monotonic())
        for connection in connections:
            connection.set_progress_handler(None, 0)
        _CURRENT.reset(token)
        logger.info(
            "Search work snapshot=%s outcome=%s elapsed=%.3fs stage=%s timings=%s",
            generation,
            outcome,
            time.monotonic() - started,
            work.stage,
            {key: round(value, 3) for key, value in work.timings.items()},
        )
