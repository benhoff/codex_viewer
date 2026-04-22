from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
import threading
import time

try:
    from watchdog.events import FileSystemEvent, FileSystemEventHandler, FileSystemMovedEvent
    from watchdog.observers import Observer
except ImportError:  # pragma: no cover - exercised via fallback tests
    FileSystemEvent = object  # type: ignore[assignment]
    FileSystemEventHandler = object  # type: ignore[assignment]
    FileSystemMovedEvent = object  # type: ignore[assignment]
    Observer = None


def _normalize_roots(roots: Iterable[Path]) -> list[Path]:
    normalized: list[Path] = []
    for root in roots:
        expanded = root.expanduser()
        if expanded.exists():
            normalized.append(expanded)
    return normalized


def _scan_session_tree(roots: list[Path]) -> dict[str, tuple[int, int]]:
    snapshot: dict[str, tuple[int, int]] = {}
    for root in roots:
        for path in root.rglob("*.jsonl"):
            if not path.is_file():
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            snapshot[str(path)] = (int(stat.st_size), int(stat.st_mtime_ns))
    return snapshot


class _WatchdogHandler(FileSystemEventHandler):
    def __init__(self, watcher: "SessionFileWatcher") -> None:
        self._watcher = watcher

    def on_any_event(self, event: FileSystemEvent) -> None:
        if getattr(event, "is_directory", False):
            return
        paths = [Path(str(getattr(event, "src_path", "") or "")).expanduser()]
        if isinstance(event, FileSystemMovedEvent):
            paths.append(Path(str(getattr(event, "dest_path", "") or "")).expanduser())
        for path in paths:
            if path.suffix.lower() != ".jsonl":
                continue
            self._watcher._record_change(path)


class SessionFileWatcher:
    def __init__(
        self,
        roots: Iterable[Path],
        *,
        mode: str = "auto",
        debounce_seconds: float = 1.0,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        self.roots = _normalize_roots(roots)
        self.debounce_seconds = max(0.0, debounce_seconds)
        self.poll_interval_seconds = max(0.1, poll_interval_seconds)
        requested_mode = (mode or "auto").strip().lower()
        self.mode = requested_mode
        self.backend = "watchdog" if requested_mode in {"auto", "watchdog"} and Observer is not None else "poll"
        if requested_mode == "poll":
            self.backend = "poll"
        self._lock = threading.Lock()
        self._pending: dict[str, float] = {}
        self._observer: Observer | None = None
        self._snapshot = _scan_session_tree(self.roots)

    def start(self) -> None:
        if self.backend != "watchdog" or Observer is None or self._observer is not None:
            return
        observer = Observer()
        handler = _WatchdogHandler(self)
        for root in self.roots:
            observer.schedule(handler, str(root), recursive=True)
        observer.start()
        self._observer = observer

    def close(self) -> None:
        if self._observer is None:
            return
        self._observer.stop()
        self._observer.join(timeout=5)
        self._observer = None

    def _record_change(self, path: Path) -> None:
        with self._lock:
            self._pending[str(path)] = time.monotonic() + self.debounce_seconds

    def _poll_once(self) -> None:
        current = _scan_session_tree(self.roots)
        changed_paths = {
            *[path for path, stat in current.items() if self._snapshot.get(path) != stat],
            *[path for path in self._snapshot if path not in current],
        }
        self._snapshot = current
        for path in sorted(changed_paths):
            self._record_change(Path(path))

    def drain_ready_paths(self) -> list[Path]:
        ready: list[Path] = []
        now = time.monotonic()
        with self._lock:
            for path, ready_at in list(self._pending.items()):
                if now < ready_at:
                    continue
                ready.append(Path(path))
                self._pending.pop(path, None)
        return sorted(ready, key=str)

    def wait_for_changes(
        self,
        stop_event: threading.Event,
        *,
        timeout_seconds: float,
    ) -> list[Path]:
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        while not stop_event.is_set():
            ready = self.drain_ready_paths()
            if ready:
                return ready

            now = time.monotonic()
            if now >= deadline:
                return []

            if self.backend == "poll":
                self._poll_once()
                ready = self.drain_ready_paths()
                if ready:
                    return ready

            sleep_for = min(self.poll_interval_seconds if self.backend == "poll" else 0.1, max(0.0, deadline - now))
            if sleep_for <= 0:
                return []
            if stop_event.wait(sleep_for):
                break
        return []
