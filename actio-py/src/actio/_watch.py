"""One `watchdog` observer per agent, and an event that means only "look again" (D-23, D-18). WP1.

Start from the oracle's sketch at `orchestra/oracle/sketches/watch_sketch.py.txt`, with its two
corrections from the plan: the audit interval is a parameter (a test shortens it), and the audit task
is started by an `async def open(...)` classmethod through `_spawn`, not in `__init__`, which cannot
await.

watchdog leaves two gaps fsnotify does not: a renamed folder reports nothing, and a kernel overflow on
a non-recursive watch is discarded with no event. The audit re-checks each folder's inode, so a loss
costs at most the audit interval, never silence. Its logger gets a `NullHandler` (the traps table).
"""

from __future__ import annotations

import asyncio

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
    async def open(cls, audit_seconds: float = AUDIT_SECONDS) -> Watcher:
        """Create it on the running loop, and start its audit task through `_spawn`."""
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
