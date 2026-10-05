from __future__ import annotations

import asyncio
import contextvars
from dataclasses import dataclass, field
from functools import partial
from queue import Full, Queue
import threading
from typing import Any, Callable, Hashable, TypeVar


ResultT = TypeVar("ResultT")
_UPLOAD_RESERVATION = contextvars.ContextVar("upload_reservation", default=None)


class WorkQueueFull(RuntimeError):
    def __init__(self, queue_name: str, *, duplicate: bool = False) -> None:
        self.queue_name = queue_name
        self.duplicate = duplicate
        detail = (
            f"Identical {queue_name} work is already in progress"
            if duplicate
            else f"The {queue_name} work queue is at capacity"
        )
        super().__init__(detail)


@dataclass
class _HistoryWorkItem:
    context: contextvars.Context
    call: Callable[[], Any]
    inflight_key: Hashable
    done: threading.Event = field(default_factory=threading.Event)
    result: Any = None
    exception: BaseException | None = None


class _BoundedWorkExecutor:
    """Run blocking work without allowing retries to grow an unbounded queue."""

    def __init__(
        self,
        *,
        name: str,
        worker_count: int,
        max_inflight: int,
        poll_interval_seconds: float = 0.05,
    ) -> None:
        self.name = name
        self.max_inflight = max(1, int(max_inflight))
        self.poll_interval_seconds = max(0.01, float(poll_interval_seconds))
        self.queue: Queue[_HistoryWorkItem] = Queue(maxsize=self.max_inflight)
        self._inflight: dict[Hashable, _HistoryWorkItem | None] = {}
        self._inflight_lock = threading.Lock()

        for worker_index in range(max(1, int(worker_count))):
            threading.Thread(
                target=self._worker,
                name=f"{name}-worker-{worker_index + 1}",
                daemon=True,
            ).start()

    def _worker(self) -> None:
        while True:
            item = self.queue.get()
            try:
                item.result = item.context.run(item.call)
            except BaseException as exc:
                item.exception = exc
            finally:
                with self._inflight_lock:
                    if self._inflight.get(item.inflight_key) is item:
                        self._inflight.pop(item.inflight_key, None)
                item.done.set()
                self.queue.task_done()

    def _submit(
        self,
        function: Callable[..., ResultT],
        *args: Any,
        dedupe_key: Hashable | None,
        **kwargs: Any,
    ) -> _HistoryWorkItem:
        # A unique object keeps ordinary calls independent. Upload callers pass
        # a stable key so a client retry cannot enqueue the same write twice.
        inflight_key: Hashable = dedupe_key if dedupe_key is not None else object()
        item = _HistoryWorkItem(
            context=contextvars.copy_context(),
            call=partial(function, *args, **kwargs),
            inflight_key=inflight_key,
        )

        with self._inflight_lock:
            if dedupe_key is not None and inflight_key in self._inflight:
                raise WorkQueueFull(self.name, duplicate=True)
            reservation = _UPLOAD_RESERVATION.get() if self is _UPLOAD_EXECUTOR else None
            if reservation in self._inflight and self._inflight[reservation] is None:
                self._inflight.pop(reservation)
            elif len(self._inflight) >= self.max_inflight:
                raise WorkQueueFull(self.name)
            self._inflight[inflight_key] = item
            try:
                self.queue.put_nowait(item)
            except Full:
                self._inflight.pop(inflight_key, None)
                raise WorkQueueFull(self.name) from None
        return item

    async def run(
        self,
        function: Callable[..., ResultT],
        *args: Any,
        dedupe_key: Hashable | None = None,
        **kwargs: Any,
    ) -> ResultT:
        item = self._submit(
            function,
            *args,
            dedupe_key=dedupe_key,
            **kwargs,
        )
        # Python 3.14 in the deployment environment can lose selector wakeups
        # issued by call_soon_threadsafe. Polling remains portable, while the
        # hard in-flight limits above cap the number of polling coroutines.
        while not item.done.is_set():
            await asyncio.sleep(self.poll_interval_seconds)
        if item.exception is not None:
            raise item.exception
        return item.result

    def inflight_count(self) -> int:
        with self._inflight_lock:
            return len(self._inflight)


# Browser authentication must not share workers with heartbeats or machine
# nonce writes: those can all wait for the writer held by a session upload.
_AUTH_EXECUTOR = _BoundedWorkExecutor(
    name="browser-auth",
    worker_count=2,
    max_inflight=64,
)
# Uploads use one worker because SQLite permits one writer. History work can
# include blocking sync writes, so its capacity is separate from browser auth.
_HISTORY_EXECUTOR = _BoundedWorkExecutor(
    name="history-api",
    worker_count=4,
    max_inflight=64,
)
_UPLOAD_EXECUTOR = _BoundedWorkExecutor(
    name="session-upload",
    worker_count=1,
    max_inflight=2,
)

# Kept as module-level aliases for diagnostics and backwards compatibility.
HISTORY_WORK_QUEUE = _HISTORY_EXECUTOR.queue
UPLOAD_WORK_QUEUE = _UPLOAD_EXECUTOR.queue


async def run_in_auth_threadpool(
    function: Callable[..., ResultT],
    *args: Any,
    **kwargs: Any,
) -> ResultT:
    return await _AUTH_EXECUTOR.run(function, *args, **kwargs)


async def run_in_history_threadpool(
    function: Callable[..., ResultT],
    *args: Any,
    **kwargs: Any,
) -> ResultT:
    return await _HISTORY_EXECUTOR.run(function, *args, **kwargs)


async def run_in_upload_threadpool(
    function: Callable[..., ResultT],
    *args: Any,
    dedupe_key: Hashable,
    **kwargs: Any,
) -> ResultT:
    return await _UPLOAD_EXECUTOR.run(
        function,
        *args,
        dedupe_key=dedupe_key,
        **kwargs,
    )


class UploadAdmissionMiddleware:
    """Reserve upload capacity before auth, body reads, or decompression.

    Submission transfers ownership to the worker. A disconnected request must
    not release a slot while its database work is still running.
    """

    paths = {"/api/sync/session", "/api/sync/session-raw",
             "/api/sync/sessions-raw", "/api/sync/session-tail"}

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("method") != "POST" or scope.get("path", "").rstrip("/") not in self.paths:
            return await self.app(scope, receive, send)
        reservation = object()
        executor = _UPLOAD_EXECUTOR
        with executor._inflight_lock:
            busy = len(executor._inflight) >= executor.max_inflight
            if not busy:
                executor._inflight[reservation] = None
        if busy:
            from starlette.responses import JSONResponse
            response = JSONResponse(
                {"detail": "The session-upload work queue is at capacity", "retryable": True},
                status_code=503, headers={"Retry-After": "5"},
            )
            return await response(scope, receive, send)
        token = _UPLOAD_RESERVATION.set(reservation)
        try:
            await self.app(scope, receive, send)
        finally:
            _UPLOAD_RESERVATION.reset(token)
            with executor._inflight_lock:
                if executor._inflight.get(reservation, False) is None:
                    executor._inflight.pop(reservation)
