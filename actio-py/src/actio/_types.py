"""The public non-protocol types, the exceptions, and the handler contracts (*Public API*, plan §4).

These are the types a caller passes or receives, other than the protocol shapes in `_protocol.py`.
Everything here is frozen at M0: a work package that wants one changed stops and asks.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Literal, Protocol, runtime_checkable

from ._config import ActioConfig
from ._protocol import (
    AgentId,
    Capability,
    CapabilityName,
    DiariumEntry,
    ResponseBody,
    Rid,
    Spec,
    Status,
    StatusState,
)

# Health as derived from a peer's status age (*Status* rules 5-8); None for inactive and error.
Health = Literal["healthy", "online", "degraded", "zombie"] | None

# The two handler signatures. Handlers take decoded JSON and return JSON-serializable data.
CapabilityHandler = Callable[[Any, "CapabilityContext"], Awaitable[Any]]
CommandHandler = Callable[[Any, "CommandContext"], Awaitable[None]]


@runtime_checkable
class CapabilityContext(Protocol):
    """One per request: who asked, how to notice a cancellation, and the attachments."""

    rid: Rid
    src: AgentId
    cancelled: asyncio.Event  # set when the request is cancelled (TS: ctx.signal)
    attachments: list[str]

    async def publish_attachment(self, name: str, data: bytes) -> str: ...


@runtime_checkable
class CommandContext(Protocol):
    """Commands are never cancelled and never answered (*Request and command files* rule 5)."""

    rid: Rid
    src: AgentId


@dataclasses.dataclass(frozen=True, slots=True)
class RequestOutcome:
    """How an outgoing exchange ended. `response` is None when no response file ever arrived."""

    final_status: Literal["confirmed", "cancelled", "abandoned"]
    response: ResponseBody | None


class ActioError(Exception):
    """A handler refuses a request or fails: `code >= 1000` (*Acceptance errors*, D-17)."""

    def __init__(self, code: int, message: str, details: Any = None) -> None:
        if code < 1000:
            raise ValueError(
                f"ActioError code must be >= 1000 (the protocol's own codes are 0-3): {code}"
            )
        super().__init__(message)
        self.code = code
        self.details = details


class ActioCallError(Exception):
    """Refused locally by the callability rules; nothing is written (rule 11)."""

    def __init__(self, dst: AgentId, name: str, reason: str) -> None:
        super().__init__(f"cannot call {dst}/{name}: {reason}")
        self.dst = dst
        self.name = name
        self.reason = reason


class ActioResultError(Exception):
    """The exchange ended, but the response `result` failed the capability's `result` schema (D-12)."""

    def __init__(self, message: str, response: ResponseBody) -> None:
        super().__init__(message)
        self.response = response


@dataclasses.dataclass(frozen=True, slots=True)
class StatusEvent:
    state: StatusState


@dataclasses.dataclass(frozen=True, slots=True)
class AnnounceEvent:
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class InreqEvent:
    rid: Rid
    final_status: str


@dataclasses.dataclass(frozen=True, slots=True)
class OutreqEvent:
    rid: Rid
    final_status: str


@dataclasses.dataclass(frozen=True, slots=True)
class LogEvent:
    message: str


@dataclasses.dataclass(frozen=True, slots=True)
class IgnoredEvent:
    path: str
    reason: str


ActioEvent = StatusEvent | AnnounceEvent | InreqEvent | OutreqEvent | LogEvent | IgnoredEvent


@dataclasses.dataclass(frozen=True, slots=True)
class AgentView:
    """A peer as the callability rules leave it usable (*Callability rules*)."""

    id: AgentId
    folder: str
    spec: Spec
    status: Status
    health: Health
    commands: bool
    capabilities: Mapping[CapabilityName, Capability]


@dataclasses.dataclass(frozen=True, slots=True)
class DiscoveredAgent:
    """One agent folder as discovery read it; `status` is None when `status.json` is absent."""

    folder: str
    spec: Spec
    status: Status | None


@dataclasses.dataclass(frozen=True, slots=True)
class IgnoredFile:
    path: str
    reason: str


@dataclasses.dataclass(frozen=True, slots=True)
class ScanResult:
    agents: list[DiscoveredAgent]
    duplicates: list[str]
    ignored: list[IgnoredFile]


@dataclasses.dataclass(frozen=True, slots=True)
class IgnoredLine:
    line: int
    reason: str


@dataclasses.dataclass(frozen=True, slots=True)
class DiariumRead:
    lines: list[DiariumEntry]
    ignored: list[IgnoredLine]


@runtime_checkable
class SyncProvider(Protocol):
    """How the file layer makes a write durable on this storage (D-21)."""

    fsync: bool
    delay_ms: int

    def notify(self) -> None: ...

    async def flush(self) -> None: ...

    def close(self) -> None: ...


@dataclasses.dataclass(frozen=True, slots=True)
class AgentFolders:
    """The five folders of one agent inside the `.actio` root, all absolute (*Folder Layout*)."""

    root: str
    agent: str
    inbox: str
    forum: str
    nuntium: str


@runtime_checkable
class AgentContext(Protocol):
    """The internal seam `ActioAgent` hands its receiver, requestor and publisher (not public API).

    `ActioAgent` satisfies it. Keeping it a Protocol is what stops `_receiver.py` and `_requestor.py`
    from importing `_agent.py`, which would be a cycle.
    """

    config: ActioConfig
    id: AgentId
    folders: AgentFolders
    sync: SyncProvider

    def now(self) -> float: ...

    def spec(self) -> Spec: ...

    def state(self) -> StatusState: ...

    def emit(self, event: ActioEvent) -> None: ...

    async def append_diarium(self, entry: DiariumEntry) -> None: ...


__all__ = [
    "ActioCallError",
    "ActioError",
    "ActioEvent",
    "ActioResultError",
    "AgentView",
    "AnnounceEvent",
    "CapabilityContext",
    "CapabilityHandler",
    "CommandContext",
    "CommandHandler",
    "DiariumRead",
    "DiscoveredAgent",
    "Health",
    "IgnoredEvent",
    "IgnoredFile",
    "IgnoredLine",
    "InreqEvent",
    "LogEvent",
    "OutreqEvent",
    "RequestOutcome",
    "ScanResult",
    "StatusEvent",
    "SyncProvider",
]
