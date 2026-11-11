"""The diarium: the agent's own event log, one JSON object per line (Diarium, WP3).

Five protocol events: `status`, `announce`, `inreq`, `outreq`, `log`. An implementation may define its
own; the builders here produce only the protocol's, and `append` validates before writing (G-5), so an
invalid line means a bug in this package, not a peer. Archive is two steps in one order (D-11…D-16,
v1.8): the `inreq` line first, then the deletion of the exchange's files.
"""

from __future__ import annotations

from ._protocol import (
    AgentId,
    DiariumEntry,
    ExchangeRecord,
    InreqFinalStatus,
    OutreqFinalStatus,
    StatusState,
)
from ._types import DiariumRead, SyncProvider


def status_event(id: AgentId, ts: float, state: StatusState) -> DiariumEntry:
    """`event_data` is the state itself."""
    raise NotImplementedError("WP3")


def announce_event(id: AgentId, ts: float) -> DiariumEntry:
    """`event_data` is null: the text is in the announce file, not here."""
    raise NotImplementedError("WP3")


def log_event(id: AgentId, ts: float, message: str) -> DiariumEntry:
    """`event_data` is the message."""
    raise NotImplementedError("WP3")


def inreq_event(
    id: AgentId, ts: float, final_status: InreqFinalStatus, exchange: ExchangeRecord
) -> DiariumEntry:
    """The receiver's final record of one inbound exchange, envelopes as exchanged."""
    raise NotImplementedError("WP3")


def outreq_event(
    id: AgentId, ts: float, final_status: OutreqFinalStatus, exchange: ExchangeRecord
) -> DiariumEntry:
    """The requestor's final record of one outbound exchange."""
    raise NotImplementedError("WP3")


async def append(folder: str, entry: DiariumEntry, provider: SyncProvider) -> None:
    """Append one validated line to `<folder>/diarium.jsonl`; raises on an invalid entry (G-5)."""
    raise NotImplementedError("WP3")


async def read_diarium(folder: str) -> DiariumRead:
    """Public: the valid lines in file order, and the ignored ones with their number and reason."""
    raise NotImplementedError("WP3")
