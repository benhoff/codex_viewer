from __future__ import annotations

import asyncio
import contextvars
from dataclasses import dataclass, field
from functools import partial
from queue import Queue
import threading
from typing import Any, Callable, TypeVar


ResultT = TypeVar("ResultT")

@dataclass
class _HistoryWorkItem:
    context: contextvars.Context
    call: Callable[[], Any]
    done: threading.Event = field(default_factory=threading.Event)
    result: Any = None
    exception: BaseException | None = None


HISTORY_WORK_QUEUE: Queue[_HistoryWorkItem] = Queue()


def _history_worker() -> None:
    while True:
        item = HISTORY_WORK_QUEUE.get()
        try:
            item.result = item.context.run(item.call)
        except BaseException as exc:
            item.exception = exc
        finally:
            item.done.set()
            HISTORY_WORK_QUEUE.task_done()


# Keep expensive history work bounded and separate from asyncio's default
# executor. Python 3.14 in the deployment environment can stall its default
# executor across loop shutdown/recreation, so these workers are intentionally
# long-lived daemon threads with explicit event-loop handoff.
for worker_index in range(4):
    threading.Thread(
        target=_history_worker,
        name=f"history-api-worker-{worker_index + 1}",
        daemon=True,
    ).start()


async def run_in_history_threadpool(
    function: Callable[..., ResultT],
    *args: Any,
    **kwargs: Any,
) -> ResultT:
    item = _HistoryWorkItem(
        context=contextvars.copy_context(),
        call=partial(function, *args, **kwargs),
    )
    HISTORY_WORK_QUEUE.put(item)
    # The deployed Python 3.14 runtime can lose the selector wakeup issued by
    # call_soon_threadsafe. A short async poll keeps completion independent of
    # that wakeup path while still yielding the event loop continuously.
    while not item.done.is_set():
        await asyncio.sleep(0.01)
    if item.exception is not None:
        raise item.exception
    return item.result
