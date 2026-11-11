"""Lane B: requests and commands (D-6, D-12, D-18, D-22). An exchange subscribes to the destination
inbox **before** it writes the request (D-18), then loops: clear `changed`, look for its own
`<rid>.res.json`, wait on `changed`, the timeout and cancel flag, and unsubscribes when it ends.
`timeout > 0` ends in B4 and the give-up; `timeout == 0` waits forever, then ends in the background.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, cast

from ._diarium import log_event, outreq_event
from ._files import copy_file, loads, remove, write_json_atomic
from ._protocol import (
    AgentId,
    CancellationFile,
    ConfirmationFile,
    ExchangeRecord,
    Header,
    RequestFile,
    ResponseBody,
    ResponseFile,
    Rid,
)
from ._status import ACTIO_VERSION
from ._types import AgentContext, LogEvent, OutreqEvent, RequestOutcome
from ._util import format_iso, nonce, ulid
from ._validate import same_header, validate
from ._watch import FolderWatch, Watcher

SettleStatus = Literal["cancelled", "confirmed", "abandoned"]


class RequestorHooks(Protocol):
    def on_watch_error(self, folder: str, error: BaseException) -> None: ...

    def on_state_change(self) -> None: ...


@dataclass
class _Outbound:
    """One outbound exchange: the envelopes as exchanged, and the resumable state of this lane."""

    rid: Rid
    dst: AgentId
    name: str
    inbox: str
    req: RequestFile | None = None
    can: CancellationFile | None = None
    res: ResponseFile | None = None
    response_body: ResponseBody | None = None
    attachments: list[str] = field(default_factory=list)
    cancelled: bool = False
    cancel_deadline: float | None = None
    done: bool = False
    outcome: RequestOutcome | None = None
    task: asyncio.Task[RequestOutcome] | None = None
    # D-11 dedup, bounded: one entry per distinct (path, mtime) this exchange saw; it dies with it.
    ignored: set[tuple[str, int]] = field(default_factory=set)


def _peek(path: str) -> tuple[int, str] | None:
    """(mtime_ns, text) of one file, or None when it is not there or unreadable (a thread hop)."""
    try:
        with open(path, encoding="utf-8") as fh:
            return os.stat(path).st_mtime_ns, fh.read()
    except (OSError, UnicodeDecodeError):
        return None


class Requestor:
    """Issues requests and commands for one agent, and ends every exchange it starts."""

    def __init__(self, ctx: AgentContext, hooks: RequestorHooks, watch: Watcher) -> None:
        self._ctx = ctx
        self._hooks = hooks
        self._watch = watch
        self._exchanges: dict[Rid, _Outbound] = {}

    async def request(
        self,
        dst: AgentId,
        name: str,
        params: Any = None,
        *,
        timeout: float = 0,
        attachments: Sequence[str] = (),
        cancel: asyncio.Event | None = None,
    ) -> RequestOutcome:
        """One exchange, end to end. Raises `OSError` for a write that failed, never for a peer's error."""
        inbox = self._dst_inbox(dst)
        sub = self._watch.subscribe(inbox)  # D-18: before the write
        ex = _Outbound(ulid(), dst, name, inbox)
        try:
            ex.req = await self._begin_outbound(ex, name, params, timeout, list(attachments))
        except BaseException:
            self._watch.unsubscribe(sub)
            raise
        self._exchanges[ex.rid] = ex
        self._hooks.on_state_change()
        try:
            return await self._drive(ex, sub, timeout, cancel)
        except (asyncio.CancelledError, TimeoutError):
            # The caller is gone; the exchange still ends correctly in the background (semantics 1).
            # The exchange holds the task, so the finisher stays referenced while it is live.
            ex.task = asyncio.create_task(
                self._drive(ex, sub, timeout, cancel, give_up=True), name=f"actio.out.{ex.rid}"
            )
            raise

    async def command(self, dst: AgentId, name: str, params: Any = None) -> Rid:
        """Write the command and return its rid: a command is never answered and is not tracked."""
        rid = ulid()
        req = self._envelope(rid, dst, name, params, 0.0, [])
        req["type"] = "cmd"
        reason = validate("request", req)
        if reason is not None:
            raise ValueError(f"requestor: built an invalid command for {dst}/{name}: {reason}")
        path = os.path.join(self._dst_inbox(dst), f"{rid}.json")
        await write_json_atomic(path, req, self._ctx.folders.agent, self._ctx.sync)
        return rid

    async def stop(self, *, force: bool = False) -> None:
        """End what is outstanding, in D-9's order; nothing is left armed afterwards."""
        live = list(self._exchanges.values())
        for ex in live:
            if ex.task is not None:
                if not ex.task.done():
                    ex.task.cancel()
                with contextlib.suppress(BaseException):
                    await ex.task
        for ex in live:
            await self._settle(ex, "abandoned", None)

    # -- the envelopes ----------------------------------------------------

    def _header(self, rid: Rid, dst: AgentId, now: float) -> dict[str, Any]:
        return {
            "rid": rid,
            "src": self._ctx.id,
            "dst": dst,
            "datetime": format_iso(now),
            "type": "req",
            "actio_version": ACTIO_VERSION,
        }

    def _envelope(
        self,
        rid: Rid,
        dst: AgentId,
        name: str,
        params: Any,
        timeout: float,
        attachments: list[str],
    ) -> RequestFile:
        now = self._ctx.config.clock()
        req = cast("RequestFile", self._header(rid, dst, now))
        req["request"] = {
            "ts": int(now),
            "nonce": nonce(),
            "signature": None,
            "name": name,
            "parameters": params,
            "timeout": timeout,
            "attachments": attachments,
        }
        return req

    # -- B1 ---------------------------------------------------------------

    async def _begin_outbound(
        self, ex: _Outbound, name: str, params: Any, timeout: float, attachment_paths: list[str]
    ) -> RequestFile:
        """B1: copy the attachments into `forum` first, then write `<dst inbox>/<rid>.json`."""
        ctx = self._ctx
        by_name: dict[str, str] = {}
        for path in attachment_paths:
            base = os.path.basename(path)
            if not base:
                raise ValueError(f"attachment path {path!r} names no file")
            other = by_name.get(base)
            if other is not None:
                raise ValueError(f"attachments clash on a forum name: {other!r} and {path!r}")
            by_name[base] = path
        copied: list[str] = []
        try:
            forum = ctx.folders.forum
            for base, path in by_name.items():
                await copy_file(path, os.path.join(forum, base), ctx.folders.agent, ctx.sync)
                copied.append(base)
        except BaseException:
            await self._remove_from_forum(copied)
            raise
        ex.attachments.extend(by_name)
        req = self._envelope(ex.rid, ex.dst, name, params, timeout, list(by_name))
        reason = validate("request", req)
        if reason is not None:
            await self._remove_from_forum(copied)  # the call is refused: nothing stays in forum
            raise ValueError(f"requestor: built an invalid request for {ex.dst}/{name}: {reason}")
        path = os.path.join(ex.inbox, f"{ex.rid}.json")
        await write_json_atomic(path, req, ctx.folders.agent, ctx.sync)
        return req

    async def _remove_from_forum(self, names: list[str]) -> None:
        for name in names:
            with contextlib.suppress(OSError):
                await remove(os.path.join(self._ctx.folders.forum, name))

    # -- B2 ---------------------------------------------------------------

    async def _drive(
        self,
        ex: _Outbound,
        sub: FolderWatch,
        timeout: float,
        cancel: asyncio.Event | None,
        give_up: bool = False,
    ) -> RequestOutcome:
        """The B2 loop and its tail, ending with the unsubscribe; `give_up` says the caller went away."""
        try:
            return await self._loop(ex, sub, timeout, cancel, give_up)
        finally:
            self._watch.unsubscribe(sub)

    async def _loop(
        self,
        ex: _Outbound,
        sub: FolderWatch,
        timeout: float,
        cancel: asyncio.Event | None,
        give_up: bool = False,
    ) -> RequestOutcome:
        clock = self._ctx.config.clock
        deadline = clock() + timeout if timeout > 0 else None
        while True:
            if ex.done:  # stop() (or a race) settled it
                assert ex.outcome is not None
                return ex.outcome
            sub.changed.clear()
            await self._capture(ex)
            if ex.res is not None:
                return await self._complete(ex)
            if sub.lost is not None:
                self._hooks.on_watch_error(ex.inbox, sub.lost)
                return await self._settle(ex, "abandoned", None)
            due = (
                give_up
                or (cancel is not None and cancel.is_set())
                or (deadline is not None and clock() >= deadline)
            )
            if not ex.cancelled and due:
                if not await self._b4(ex):
                    return await self._settle(ex, "abandoned", None)  # the self-cancellation failed
            if ex.cancel_deadline is not None and clock() >= ex.cancel_deadline:
                return await self._settle(ex, "abandoned", None)
            ds = [d for d in (deadline, ex.cancel_deadline) if d is not None]
            stop_at = min(ds) if ds else None
            await self._wake(sub, cancel, stop_at)

    async def _wake(
        self, sub: FolderWatch, cancel: asyncio.Event | None, stop_at: float | None
    ) -> None:
        """Sleep until the folder changed, `cancel` is set, or `stop_at` passes — then look again."""
        tasks: list[asyncio.Task[Any]] = [asyncio.create_task(sub.changed.wait())]
        if cancel is not None:
            tasks.append(asyncio.create_task(cancel.wait()))
        if stop_at is not None:
            tasks.append(asyncio.create_task(asyncio.sleep(max(0.0, stop_at - self._ctx.now()))))
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _capture(self, ex: _Outbound) -> None:
        """One look for `<rid>.res.json` (a thread hop): broken files stay silent; mismatches log once."""
        assert ex.req is not None
        path = os.path.join(ex.inbox, f"{ex.rid}.res.json")
        if (peeked := await asyncio.to_thread(_peek, path)) is None:
            return
        mtime_ns, raw = peeked
        try:
            file = loads(raw)
        except ValueError:
            return
        if not isinstance(file, dict) or validate("response", file) is not None:
            return
        if not same_header(cast("Header", file), ex.req):
            if (key := (path, mtime_ns)) in ex.ignored:
                return
            ex.ignored.add(key)
            message = f"ignored {path}: header mismatch (D-11)"
            self._ctx.emit(LogEvent(message=message))
            await self._ctx.append_diarium(log_event(self._ctx.id, self._ctx.now(), message))
            return
        ex.res = cast("ResponseFile", file)
        # an encrypted (base64) body is not a ResponseBody; the outcome carries no body
        body = file.get("response")
        ex.response_body = cast("ResponseBody", body) if isinstance(body, dict) else None

    # -- B3, B4, B5 -------------------------------------------------------

    async def _b4(self, ex: _Outbound) -> bool:
        """B4: write `<rid>.can.json` (a request to stop, not a stop) and arm the cancel-wait."""
        ex.cancelled = True
        now = self._ctx.config.clock()
        can = cast("CancellationFile", self._header(ex.rid, ex.dst, now))
        can["cancellation"] = {
            "ts": int(now),
            "nonce": nonce(),
            "signature": None,
            "name": ex.name,
            "force": False,
        }
        reason = validate("cancellation", can)
        if reason is not None:
            self._ctx.emit(LogEvent(message=f"cancellation of {ex.rid} failed: {reason}"))
            return False
        ex.cancel_deadline = now + self._ctx.config.cancel_wait_timeout
        can_path = os.path.join(ex.inbox, f"{ex.rid}.can.json")
        await write_json_atomic(can_path, can, self._ctx.folders.agent, self._ctx.sync)
        ex.can = can
        return True

    async def _complete(self, ex: _Outbound) -> RequestOutcome:
        """B3 (+ the D-13 guard) and B5: the `outreq` line, the con (if due), the B5 deletions."""
        con: ConfirmationFile | None = None
        try:
            if await asyncio.to_thread(_peek, os.path.join(ex.inbox, f"{ex.rid}.json")) is not None:
                # the request is still there: confirm it (the protocol's SHOULD)
                now = self._ctx.config.clock()
                con = cast("ConfirmationFile", self._header(ex.rid, ex.dst, now))
                con["confirmation"] = {"ts": int(now), "nonce": nonce(), "signature": None}
                reason = validate("confirmation", con)
                if reason is not None:
                    raise ValueError(f"invalid confirmation for {ex.rid}: {reason}")
        except Exception as err:
            self._ctx.emit(LogEvent(message=f"confirmation of {ex.rid} failed: {err}"))
            con = None
        outcome = await self._settle(ex, "cancelled" if ex.cancelled else "confirmed", con)
        if con is not None:
            con_path = os.path.join(ex.inbox, f"{ex.rid}.con.json")
            try:
                await write_json_atomic(con_path, con, self._ctx.folders.agent, self._ctx.sync)
            except Exception as err:
                self._ctx.emit(LogEvent(message=f"confirmation of {ex.rid} failed: {err}"))
        return outcome

    async def _settle(
        self, ex: _Outbound, final: SettleStatus, con: ConfirmationFile | None
    ) -> RequestOutcome:
        """Settle an exchange once: the `outreq` line, the event, the deletions; failures only log."""
        if (prev := self._claim(ex)) is not None:
            return prev
        outcome = RequestOutcome(final_status=final, response=ex.response_body)
        ex.outcome = outcome
        try:
            rec: ExchangeRecord
            rec = {"final_status": final, "req": ex.req, "can": ex.can, "res": ex.res, "con": con}
            await self._ctx.append_diarium(outreq_event(self._ctx.id, self._ctx.now(), final, rec))
            self._ctx.emit(OutreqEvent(rid=ex.rid, final_status=final))
            await self._remove_from_forum(ex.attachments)
            ex.attachments.clear()
        except Exception as err:
            self._ctx.emit(LogEvent(message=f"requestor: settlement of {ex.rid} failed: {err}"))
        return outcome

    def _claim(self, ex: _Outbound) -> RequestOutcome | None:
        if ex.done:
            return ex.outcome
        ex.done = True
        self._exchanges.pop(ex.rid, None)
        self._hooks.on_state_change()
        return None

    def _dst_inbox(self, dst: AgentId) -> str:
        return os.path.join(self._ctx.folders.root, dst, "inbox")
