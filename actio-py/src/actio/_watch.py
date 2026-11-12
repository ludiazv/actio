"""One `watchdog` observer per agent, and an event that means only "look again" (D-23, D-18).

An `async def open(...)` classmethod starts it, because `__init__` cannot await: `open` takes a `Spawn`
so the audit task is started the one way long-lived tasks are started, and it takes the audit interval
so a test can shorten it. Two gaps fsnotify does not have: a renamed folder reports nothing, and a
kernel overflow on a non-recursive watch is discarded with no event. The audit re-checks each folder's
inode, so a loss costs at most the audit interval, never silence. Its logger gets a `NullHandler`.
"""

from __future__ import annotations

import asyncio
import logging
import os

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import ObservedWatch

from ._files import io_hop
from ._types import Spawn

type Markers = dict[str, str]

AUDIT_SECONDS = 5.0
AUDIT_TASK = "actio.audit"

logging.getLogger("watchdog").addHandler(
    logging.NullHandler()
)  # its race notices aren't the user's


class FolderWatch:
    """One subscription: wait on `changed`, clear it, look, and check `lost`."""

    def __init__(self, folder: str) -> None:
        self.folder = folder
        self.changed = asyncio.Event()
        self.lost: OSError | None = None


class _Handler(FileSystemEventHandler):
    def __init__(self, watcher: Watcher, folder: str) -> None:
        self._watcher = watcher
        self._folder = folder

    def on_any_event(self, event: FileSystemEvent) -> None:
        try:
            self._watcher._loop.call_soon_threadsafe(
                self._watcher._on_event,
                self._folder,
                event.event_type,
                os.fsdecode(event.src_path),
            )
        except RuntimeError:
            pass  # the loop is closed after `close()`: nobody is waiting


class Watcher:
    """One per agent: `open()` at start, `close()` at stop; folders are watched non-recursively."""

    def __init__(self, audit_seconds: float) -> None:
        self._loop = asyncio.get_running_loop()
        self._audit_seconds = audit_seconds
        self._observer = Observer()
        self._observer.start()
        self._folders: dict[str, tuple[ObservedWatch, set[FolderWatch], int]] = {}
        self._audit_task: asyncio.Task[None] | None = None

    @classmethod
    async def open(cls, spawn: Spawn, audit_seconds: float = AUDIT_SECONDS) -> Watcher:
        """Create it on the running loop, and start its audit task with `spawn`."""
        watcher = cls(audit_seconds)
        watcher._audit_task = spawn(watcher._audit(), AUDIT_TASK)
        return watcher

    def subscribe(self, folder: str) -> FolderWatch:
        """Arm the folder if nobody watches it; a missing folder raises `FileNotFoundError`."""
        folder = os.path.normpath(folder)
        entry = self._folders.get(folder)
        if entry is None:
            inode = os.stat(folder).st_ino  # on the loop, as measured: no thread
            watch = self._observer.schedule(_Handler(self, folder), folder, recursive=False)
            entry = (watch, set(), inode)
            self._folders[folder] = entry
        sub = FolderWatch(folder)
        entry[1].add(sub)
        return sub

    def unsubscribe(self, sub: FolderWatch) -> None:
        """Safe twice, after a loss, and after `close`.

        Dropping a folder's last watch is what unsubscribes the folder, so the watch is the property of
        whoever is watching: an exchange handed to another task is released by that task when the exchange
        ends, not by a caller that unwinds early. A caller that released it early would leave the peer's
        answer arriving nowhere, and the exchange would end as abandoned while it sat on disk."""
        entry = self._folders.get(sub.folder)
        if entry is None:
            return
        entry[1].discard(sub)
        if not entry[1]:
            del self._folders[sub.folder]
            self._unschedule(entry[0])

    async def close(self) -> None:
        """Stop the observer and join it on the file layer's pool (`Observer.join` blocks)."""
        if self._audit_task is not None:
            self._audit_task.cancel()
            await asyncio.gather(self._audit_task, return_exceptions=True)
            self._audit_task = None
        self._folders.clear()
        self._observer.stop()
        await io_hop(self._observer.join)

    def _unschedule(self, watch: ObservedWatch) -> None:
        try:
            self._observer.unschedule(watch)
        except KeyError:
            pass  # the observer already dropped it

    def _on_event(self, folder: str, kind: str, path: str) -> None:
        if path == folder and kind in ("deleted", "moved"):
            self._lose(folder, OSError(f"the watched folder {folder} was removed or renamed"))
        else:
            self._wake(folder)

    def _wake(self, folder: str) -> None:
        entry = self._folders.get(folder)
        if entry is None:
            return
        for sub in entry[1]:
            sub.changed.set()  # the consumer that never looks must not stall the others

    def _lose(self, folder: str, error: OSError) -> None:
        entry = self._folders.pop(folder, None)
        if entry is None:
            return
        self._unschedule(entry[0])
        for sub in entry[1]:
            sub.lost = error
            sub.changed.set()

    async def _audit(self) -> None:
        """A folder with another inode, or none, is lost; every other one looks again."""
        while True:
            await asyncio.sleep(self._audit_seconds)
            for folder, entry in list(self._folders.items()):
                try:
                    stat = await io_hop(os.stat, folder)
                except OSError as error:
                    if self._folders.get(folder) is entry:
                        self._lose(folder, error)
                    continue
                if self._folders.get(folder) is not entry:
                    continue  # it changed while we looked
                if stat.st_ino != entry[2]:
                    self._lose(folder, OSError(f"the watched folder {folder} was renamed"))
                else:
                    self._wake(folder)


async def scan(folder: str, prev: Markers) -> tuple[list[str], Markers]:
    """`scan_folder` on the file layer's pool: a folder read is a thread hop, and it is the pool's
    hop rather than asyncio's default one, so a storm of handlers' own work cannot delay a scan and
    leave a peer unread (D-22's loss). It raises `OSError` when the folder is gone."""
    return await io_hop(scan_folder, folder, prev)


def scan_folder(folder: str, prev: Markers) -> tuple[list[str], Markers]:
    """Which names changed since `prev`, and the new markers (mtime_ns, size, inode). Blocking."""
    markers: Markers = {}
    changed: list[str] = []
    with os.scandir(folder) as entries:
        for entry in sorted(entries, key=lambda e: e.name):
            try:
                stat = entry.stat()
            except OSError:
                continue
            if entry.name.startswith(".") or not entry.is_file():
                continue
            marker = f"{stat.st_mtime_ns}:{stat.st_size}:{stat.st_ino}"
            markers[entry.name] = marker
            if prev.get(entry.name) != marker:
                changed.append(entry.name)
    return changed, markers
