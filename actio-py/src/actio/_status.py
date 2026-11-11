"""Self-publication: `spec.json`, `status.json`, the announce file, the heartbeat, and health
(*Agent's status*, *Timers and thresholds*, *Conventions*, D-8, D-9).

Every write here is validated first: we produced the document, so a rejection is our bug and
raises. The health thresholds are protocol constants, not configuration. `StatusPublisher` is the only
place in the package that takes an `asyncio.Lock`: two `to_thread` writes to one file can finish in
either order, and a write must survive its awaiter being cancelled (D-9) — so the locked section is its
own task, and the publisher latches after writing `inactive` rather than reading anyone's state.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from typing import Any, Final

from ._files import make_dirs, write_json_atomic
from ._protocol import AgentId, Announce, CapabilityStatusEntry, Spec, Status, StatusState
from ._types import AgentFolders, Health, Spawn, SyncProvider
from ._util import format_iso, parse_iso
from ._validate import SchemaKind, validate

ACTIO_VERSION: Final = "0.1"  # *Version*: the only version this implementation speaks

HEALTHY_SECONDS: Final = 60.0  # rule 4: updated less than a minute ago
ONLINE_SECONDS: Final = 100.0  # rule 5
DEGRADED_SECONDS: Final = 600.0  # rule 6; rule 7 makes it a zombie only after this

# The one name of the publisher's guarded-write tasks: one lock in the package, in the publisher.
_PUBLISH_TASK = "actio.publish"


def _must_validate(what: str, kind: SchemaKind, doc: Any) -> None:
    """Self-validation before a write: we produced the document, so a rejection is our bug — it raises.

    The one-line reason is for the log, not for a peer: nothing of this document ever crosses a folder."""
    reason = validate(kind, doc)
    if reason is not None:
        raise ValueError(f"{what} is invalid: {reason}")


def build_status(
    id: AgentId,
    state: StatusState,
    ts: float,
    capability_status: list[CapabilityStatusEntry] | None = None,
) -> Status:
    """A valid status object with a fresh timestamp from the injected clock."""
    return {
        "id": id,
        "status": state,
        "datetime": format_iso(ts),
        "capability_status": capability_status if capability_status is not None else [],
    }


def health(status: Status, now: float) -> Health:
    """Public: healthy, online, degraded or zombie by the status age; None for inactive and error."""
    if status["status"] not in ("idle", "busy"):
        return None  # rule 8: those two have no health
    updated = parse_iso(status["datetime"])
    if updated is None:
        return None  # a datetime the reader cannot parse, the reader cannot rate
    age = now - updated  # a datetime in the future is the freshest possible
    if age < HEALTHY_SECONDS:
        return "healthy"
    if age < ONLINE_SECONDS:
        return "online"
    if age <= DEGRADED_SECONDS:
        return "degraded"
    return "zombie"


class StatusPublisher:
    """One per run: the only writer of our own `spec.json`, `status.json` and announce file (D-9).

    It owns the lock, the latch, the sync provider and a `Spawn`, and knows the five folders. The guarded
    write runs as its own task from `spawn`, awaited through `asyncio.shield`, so a caller cancelled
    mid-write cannot free the lock while the write is still in flight. After this publisher has written
    a status of `inactive` the latch is set and every later write through it is a no-op: it latches on
    its own write, because it can't see the agent's state. A restart builds a fresh publisher, and with
    it a fresh latch.
    """

    def __init__(self, folders: AgentFolders, provider: SyncProvider, spawn: Spawn) -> None:
        self._folders = folders
        self._provider = provider
        self._spawn = spawn
        self._lock = asyncio.Lock()
        self._latched = False

    @property
    def latched(self) -> bool:
        """True once `inactive` has been written here; from then on every write is a no-op."""
        return self._latched

    async def _publish(self, path: str, doc: Any, latches: bool) -> None:
        """The guarded write (D-9): the locked section is its own task from `spawn`, and the caller
        awaits it through `asyncio.shield`. A caller cancelled mid-write keeps the lock held until the
        write lands; a latched publisher writes nothing — not an exception, not a log line, a no-op."""

        async def locked() -> None:
            async with self._lock:
                if self._latched:
                    return
                await write_json_atomic(path, doc, self._folders.agent, self._provider)
                if latches:
                    self._latched = True

        await asyncio.shield(self._spawn(locked(), _PUBLISH_TASK))

    async def publish_spec(self, spec: Spec) -> None:
        """Write `<agent folder>/spec.json` atomically, after validating it."""
        _must_validate("spec.json", "spec", spec)
        await self._publish(os.path.join(self._folders.agent, "spec.json"), spec, latches=False)

    async def publish_status(self, status: Status) -> None:
        """Write `<agent folder>/status.json` atomically: at once on a change, and every heartbeat tick.
        A status of `inactive` is the last thing this publisher ever writes."""
        _must_validate("status.json", "status", status)
        await self._publish(
            os.path.join(self._folders.agent, "status.json"),
            status,
            latches=status["status"] == "inactive",
        )

    async def announce(self, id: AgentId, text: str, ts: float) -> None:
        """Write `nuntium/<id>.n.json` (*Announcements*); the diarium line is the agent's to append."""
        doc: Announce = {
            "id": id,
            "datetime": format_iso(ts),
            "ts": int(ts),
            "nuntium": text,
        }
        _must_validate(f"the announce file for {id!r}", "announce", doc)
        await make_dirs(self._folders.nuntium)
        await self._publish(os.path.join(self._folders.nuntium, f"{id}.n.json"), doc, latches=False)


async def heartbeat(
    interval: float, tick: Callable[[], Awaitable[None]], stopped: asyncio.Event
) -> None:
    """The body of the heartbeat task, which the agent starts with its `Spawn`: a tick starts only after
    the previous one finished, the tick is never cancelled by the loop ending, and the loop ends when
    `stopped` is set.

    The interval is measured from the **end** of the previous tick, as the reference measures it: the
    next tick starts `interval` seconds after the previous one finished, never during it, and the first
    tick comes after one interval — the agent publishes the first status itself. Nothing is armed when
    this returns.
    """
    while True:
        try:
            await asyncio.wait_for(stopped.wait(), interval)
        except TimeoutError:
            pass
        if stopped.is_set():
            return
        await tick()
