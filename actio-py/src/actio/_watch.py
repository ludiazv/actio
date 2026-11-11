"""One `watchdog` observer per agent, and an event that means only "look again" (D-23, D-18).

An `async def open(...)` classmethod starts it, because `__init__` cannot await: `open` takes a `Spawn`
so the audit task is started the one way long-lived tasks are started, and it takes the audit interval
so a test can shorten it. Two gaps fsnotify does not have: a renamed folder reports nothing, and a
kernel overflow on a non-recursive watch is discarded with no event. The audit re-checks each folder's
inode, so a loss costs at most the audit interval, never silence. Its logger gets a `NullHandler`.
"""

from __future__ import annotations

import asyncio

from ._types import Spawn

type Markers = dict[str, str]

AUDIT_SECONDS = 5.0


class FolderWatch:
    """One subscription: wait on `changed`, clear it, look, and check `lost`."""

    folder: str
    changed: asyncio.Event
    lost: OSError | None


class Watcher:
    """One per agent: `open()` at start, `close()` at stop; folders are watched non-recursively."""

    @classmethod
    async def open(cls, spawn: Spawn, audit_seconds: float = AUDIT_SECONDS) -> Watcher:
        """Create it on the running loop, and start its audit task with `spawn`."""
        raise NotImplementedError("WP1")

    def subscribe(self, folder: str) -> FolderWatch:
        """Arm the folder if nobody watches it; a missing folder raises `FileNotFoundError`."""
        raise NotImplementedError("WP1")

    def unsubscribe(self, sub: FolderWatch) -> None:
        """Safe twice, after a loss, and after `close`."""
        raise NotImplementedError("WP1")

    async def close(self) -> None:
        """Stop the observer and join it in `to_thread` (`Observer.join` blocks)."""
        raise NotImplementedError("WP1")


def scan_folder(folder: str, prev: Markers) -> tuple[list[str], Markers]:
    """Which names changed since `prev`, and the new markers (mtime_ns, size, inode). Blocking."""
    raise NotImplementedError("WP1")
