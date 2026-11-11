"""The diarium: the agent's own event log, one JSON object per line (*Diarium*).

Five protocol events: `status`, `announce`, `inreq`, `outreq`, `log`. An implementation may define its
own; the builders here produce only the protocol's, and `append` validates the line before writing it, so an
invalid line means a bug in this package, not a peer. Archive is two steps in one order (D-11…D-16): the `inreq` line first, then the deletion of the exchange's files.
"""

from __future__ import annotations

import asyncio
import os

from ._files import append_jsonl, loads, read_bytes
from ._protocol import (
    AgentId,
    DiariumEntry,
    ExchangeRecord,
    InreqFinalStatus,
    OutreqFinalStatus,
    StatusState,
)
from ._types import DiariumRead, IgnoredLine, SyncProvider
from ._util import format_iso
from ._validate import malformed_reason, validate


def _line(id: AgentId, ts: float, event: str, event_data: object) -> DiariumEntry:
    """One line: the agent's id, a whole-second datetime from `ts`, the event name and its data."""
    return {
        "id": id,
        "datetime": format_iso(ts),
        "event": event,
        "event_data": event_data,
    }


def status_event(id: AgentId, ts: float, state: StatusState) -> DiariumEntry:
    """`event_data` is the state itself."""
    return _line(id, ts, "status", state)


def announce_event(id: AgentId, ts: float) -> DiariumEntry:
    """`event_data` is null: the text is in the announce file, not here."""
    return _line(id, ts, "announce", None)


def log_event(id: AgentId, ts: float, message: str) -> DiariumEntry:
    """`event_data` is the message."""
    return _line(id, ts, "log", message)


def inreq_event(
    id: AgentId, ts: float, final_status: InreqFinalStatus, exchange: ExchangeRecord
) -> DiariumEntry:
    """The receiver's final record of one inbound exchange, envelopes as exchanged."""
    data: ExchangeRecord = {**exchange, "final_status": final_status}
    return _line(id, ts, "inreq", data)


def outreq_event(
    id: AgentId, ts: float, final_status: OutreqFinalStatus, exchange: ExchangeRecord
) -> DiariumEntry:
    """The requestor's final record of one outbound exchange."""
    data: ExchangeRecord = {**exchange, "final_status": final_status}
    return _line(id, ts, "outreq", data)


async def append(folder: str, entry: DiariumEntry, provider: SyncProvider) -> None:
    """Append one validated line to `<folder>/diarium.jsonl`; raises on an invalid entry."""
    reason = validate("diarium", entry)
    if reason is not None:
        raise ValueError(f"diarium {entry['event']!r} event is invalid: {reason}")
    await append_jsonl(os.path.join(folder, "diarium.jsonl"), entry, provider)


def _read_lines(raw: str) -> DiariumRead:
    """The line split of one diarium file: the valid lines in file order, the rest as `IgnoredLine`s."""
    lines: list[DiariumEntry] = []
    ignored: list[IgnoredLine] = []
    parts = raw.split("\n")
    last = len(parts) - 1
    for i, text in enumerate(parts):
        # A last line without a trailing newline is an append in progress: one ignored line, and the
        # lines before it still come back.
        if i == last and text != "" and not raw.endswith("\n"):
            ignored.append(IgnoredLine(line=i + 1, reason="truncated last line"))
            continue
        if text.strip() == "":
            continue  # a blank line carries no event
        try:
            entry = loads(text)
        except ValueError as err:
            ignored.append(IgnoredLine(line=i + 1, reason=malformed_reason(err)))
            continue
        reason = validate("diarium", entry)
        if reason is None:
            lines.append(entry)
        else:
            ignored.append(IgnoredLine(line=i + 1, reason=reason))
    return DiariumRead(lines=lines, ignored=ignored)


async def read_diarium(folder: str) -> DiariumRead:
    """Public: the valid lines in file order, and the ignored ones with their number and reason."""
    path = os.path.join(folder, "diarium.jsonl")
    try:
        data = await read_bytes(path)
    except FileNotFoundError:
        return DiariumRead(lines=[], ignored=[])  # a missing file reads as empty, not an error
    return await asyncio.to_thread(_read_lines, data.decode("utf-8"))
