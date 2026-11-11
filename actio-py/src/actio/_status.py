"""Self-publication: `spec.json`, `status.json`, the announce file, the heartbeat, and health
(*Agent's status*, *Timers and thresholds*, D-8, D-9). WP3.

Every write here is validated first (G-5): we produced the document, so a rejection is our bug and
raises. The health thresholds are protocol constants, not configuration. `_publish` is the only place
in the package that takes an `asyncio.Lock` (plan §5.1): two `to_thread` writes to one file can finish
in either order, and the write must survive a cancellation (D-9).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, Final

from ._protocol import AgentId, CapabilityStatusEntry, Spec, Status, StatusState
from ._types import Health, SyncProvider

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


async def _publish(
    path: str, obj: Any, tmp_dir: str, provider: SyncProvider, lock: asyncio.Lock
) -> None:
    """The one guarded write (D-9): hold the lock inside, so a cancellation cannot free it mid-write."""
    raise NotImplementedError("WP3")


async def publish_spec(
    agent_folder: str, spec: Spec, provider: SyncProvider, lock: asyncio.Lock
) -> None:
    """Write `<agent folder>/spec.json` atomically, after validating it."""
    raise NotImplementedError("WP3")


async def publish_status(
    agent_folder: str, status: Status, provider: SyncProvider, lock: asyncio.Lock
) -> None:
    """Write `<agent folder>/status.json` atomically: at once on a change, and every heartbeat tick."""
    raise NotImplementedError("WP3")


async def announce(
    agent_folder: str,
    nuntium_folder: str,
    id: AgentId,
    text: str,
    ts: float,
    provider: SyncProvider,
    lock: asyncio.Lock,
) -> None:
    """Write `nuntium/<id>.n.json` and append the diarium `announce` event (I-5: two folders, one call)."""
    raise NotImplementedError("WP3")


def health(status: Status, now: float) -> Health:
    """Public: healthy, online, degraded or zombie by the status age; None for inactive and error."""
    raise NotImplementedError("WP3")


async def heartbeat(
    interval: float, tick: Callable[[], Awaitable[None]], stopped: asyncio.Event
) -> None:
    """The body of the heartbeat task, which the agent spawns with `_spawn` (§5.2): a tick starts only
    after the previous one finished, and the loop ends when `stopped` is set."""
    raise NotImplementedError("WP3")
