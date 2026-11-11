"""Self-publication: `spec.json`, `status.json`, the announce file, the heartbeat, and health
(*Agent's status*, *Timers and thresholds*, *Version*, D-8, D-9).

Every write here is validated first (G-5): we produced the document, so a rejection is our bug and
raises. The health thresholds are protocol constants, not configuration. `StatusPublisher` is the only
place in the package that takes an `asyncio.Lock`: two `to_thread` writes to one file can finish in
either order, and a write must survive its awaiter being cancelled (D-9) — so the locked section is its
own task, and the publisher latches after writing `inactive` rather than reading anyone's state.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Final

from ._protocol import AgentId, CapabilityStatusEntry, Spec, Status, StatusState
from ._types import AgentFolders, Health, Spawn, SyncProvider

ACTIO_VERSION: Final = "0.1"  # *Version*: the only version this implementation speaks

HEALTHY_SECONDS: Final = 60.0  # rule 4: updated less than a minute ago
ONLINE_SECONDS: Final = 100.0  # rule 5
DEGRADED_SECONDS: Final = 600.0  # rule 6; rule 7 makes it a zombie only after this


def build_status(
    id: AgentId,
    state: StatusState,
    ts: float,
    capability_status: list[CapabilityStatusEntry] | None = None,
) -> Status:
    """A valid status object with a fresh timestamp from the injected clock."""
    raise NotImplementedError("WP3")


class StatusPublisher:
    """One per run: the only writer of our own `spec.json`, `status.json` and announce file (D-9).

    It owns the lock, the latch, the sync provider and a `Spawn`, and knows the five folders. The guarded
    write runs as its own task from `spawn`, awaited through `asyncio.shield`, so a caller cancelled
    mid-write cannot free the lock while the write is still in flight. After this publisher has written
    a status of `inactive` the latch is set and every later write through it is a no-op: it latches on
    its own write, because it can't see the agent's state. A restart builds a fresh publisher, and with
    it a fresh latch.
    """

    def __init__(self, folders: AgentFolders, provider: SyncProvider, spawn: Spawn) -> None: ...

    @property
    def latched(self) -> bool:
        """True once `inactive` has been written here; from then on every write is a no-op."""
        raise NotImplementedError("WP3")

    async def publish_spec(self, spec: Spec) -> None:
        """Write `<agent folder>/spec.json` atomically, after validating it."""
        raise NotImplementedError("WP3")

    async def publish_status(self, status: Status) -> None:
        """Write `<agent folder>/status.json` atomically: at once on a change, and every heartbeat tick.
        A status of `inactive` is the last thing this publisher ever writes."""
        raise NotImplementedError("WP3")

    async def announce(self, id: AgentId, text: str, ts: float) -> None:
        """Write `nuntium/<id>.n.json` (*Announcements*); the diarium line is the agent's to append."""
        raise NotImplementedError("WP3")


def health(status: Status, now: float) -> Health:
    """Public: healthy, online, degraded or zombie by the status age; None for inactive and error."""
    raise NotImplementedError("WP3")


async def heartbeat(
    interval: float, tick: Callable[[], Awaitable[None]], stopped: asyncio.Event
) -> None:
    """The body of the heartbeat task, which the agent starts with its `Spawn`: a tick starts only after
    the previous one finished, the tick is never cancelled by the loop ending, and the loop ends when
    `stopped` is set."""
    raise NotImplementedError("WP3")
