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
from ._files import copy_file, loads, read_text_with_mtime, remove, write_json_atomic
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
from ._types import AgentContext, LogEvent, OutreqEvent, RequestOutcome, Spawn
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
    # A caller that went away: the exchange task ends the exchange anyway (semantics 1), and B4 is its way
    # of saying so to the peer.
    give_up: bool = False
    sub: FolderWatch | None = None  # the exchange's own watch, so a give-up can wake the exchange
    cancel_deadline: float | None = None
    done: bool = False
    outcome: RequestOutcome | None = None
    task: asyncio.Task[RequestOutcome] | None = None
    # The one settlement, created with the agent's `Spawn` and awaited through `shield` by everyone who
    # wants the outcome: the exchange task, and `stop()`.
    settling: asyncio.Task[RequestOutcome] | None = None
    # D-11 dedup, bounded: one entry per distinct (path, mtime) this exchange saw; it dies with it.
    ignored: set[tuple[str, int]] = field(default_factory=set)


async def _peek(path: str) -> tuple[int, str] | None:
    """`(mtime_ns, text)` of one file, or None when it is not there or cannot be read.

    A response file this lane can't read stays silent: the exchange stays alive and looks again on the
    next wake (*Sync and Race Conditions*, D-11's dedup of an ignored file).
    """
    try:
        return await read_text_with_mtime(path)
    except (OSError, UnicodeDecodeError):
        return None


class Requestor:
    """Issues requests and commands for one agent, and ends every exchange it starts."""

    def __init__(
        self, ctx: AgentContext, hooks: RequestorHooks, watch: Watcher, spawn: Spawn
    ) -> None:
        self._ctx = ctx
        self._hooks = hooks
        self._watch = watch
        self._spawn = spawn  # every task this lane starts, including a settlement, is the agent's
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
        """One exchange, end to end. Raises `OSError` for a write that failed, never for a peer's error.

        B1 through B5 run as one exchange task, created with the agent's `Spawn`; this call only awaits it,
        through a shield (D-18). A caller's cancellation therefore cannot interrupt protocol code: the
        write, the attachment copies, the subscription and the settlement are the task's from end to end.
        """
        ex = _Outbound(ulid(), dst, name, self._dst_inbox(dst))
        ex.task = self._spawn(
            self._exchange(ex, params, timeout, list(attachments), cancel), f"actio.out.{ex.rid}"
        )
        try:
            return await asyncio.shield(ex.task)
        except asyncio.CancelledError:
            if ex.settling is not None:
                # Not the caller's doing alone: the lane went down and settled the exchange. Its outcome is
                # owed to whoever was waiting, and the settlement is already the exchange's one record (D-9).
                return await asyncio.shield(ex.settling)
            # Semantics 1: the caller is gone, the exchange is not. It ends in the background — B4 asks the
            # peer to stop, and the `outreq` line lands when it settles — with the same subscription it has
            # held since before the request was written (D-18).
            ex.give_up = True
            if ex.sub is not None:
                ex.sub.changed.set()  # wake the exchange: B4 now, not at the next folder event
            raise

    async def _exchange(
        self,
        ex: _Outbound,
        params: Any,
        timeout: float,
        attachment_paths: list[str],
        cancel: asyncio.Event | None,
    ) -> RequestOutcome:
        """B1 to B5, one task, one subscription: `sub` is taken before the write and released when the
        exchange settles, whoever is waiting for it (D-18). Nothing here is interruptible by the caller: a
        cancellation reaches it only through `ex.give_up`."""
        # The table knows the exchange before its first write, so `stop()` can reach one whose B1 is still
        # in flight (D-9's step 4 reaches everything owed).
        self._exchanges[ex.rid] = ex
        sub = ex.sub = self._watch.subscribe(ex.inbox)  # D-18: before the write
        try:
            ex.req = await self._begin_outbound(ex, ex.name, params, timeout, attachment_paths)
            self._hooks.on_state_change()
            return await self._loop(ex, sub, timeout, cancel)
        except BaseException:
            # B1 refused the call (an attachment clash, an invalid envelope): nothing reached the peer and
            # no line is owed, so the exchange leaves the table with the exception.
            if self._exchanges.get(ex.rid) is ex:
                del self._exchanges[ex.rid]
            raise
        finally:
            self._watch.unsubscribe(sub)

    async def command(self, dst: AgentId, name: str, params: Any = None) -> Rid:
        """Write the command and return its rid: a command is never answered and is not tracked."""
        rid = ulid()
        req = self._envelope(rid, dst, name, params, 0.0, [])
        req["type"] = "cmd"
        reason = validate("request", req)
        if reason is not None:
            raise ValueError(f"requestor: built an invalid command for {dst}/{name}: {reason}")
        # D-2: the recommended name. A reader matches by `rid` and body key, never by this name.
        path = os.path.join(self._dst_inbox(dst), f"{rid}.json")
        await write_json_atomic(path, req, self._ctx.folders.agent, self._ctx.sync)
        # B1: for a command, "append an `outreq` event (`final_status="sent"`, only `req`)". A command
        # is never answered, so this line is the only record of it there will ever be.
        rec: ExchangeRecord = {
            "final_status": "sent",
            "req": req,
            "can": None,
            "res": None,
            "con": None,
        }
        await self._ctx.append_diarium(outreq_event(self._ctx.id, self._ctx.now(), "sent", rec))
        self._ctx.emit(OutreqEvent(rid=rid, final_status="sent"))
        return rid

    async def stop(self, *, force: bool = False) -> None:
        """End what is outstanding, in D-9's order; nothing is left armed afterwards."""
        live = list(self._exchanges.values())
        for ex in live:
            if ex.req is None:
                # B1 never wrote: the peer has nothing, and no line is owed.
                self._exchanges.pop(ex.rid, None)
                continue
            await self._settle(
                ex, "abandoned", None
            )  # an exchange leaves the table only when settled,
        # so a settlement already in flight is still here, and `shield` waits for it (D-9: every owed
        # `outreq` line is on disk before the agent publishes `inactive`)
        # The lines are down; now the waits may go. An exchange task cancelled here is the lane's own doing,
        # which is why the caller above takes the settlement as the outcome rather than a cancellation.
        for ex in live:
            task = ex.task
            if task is None:
                continue
            if not task.done():
                task.cancel()
            with contextlib.suppress(BaseException):
                await task

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
        path = os.path.join(ex.inbox, f"{ex.rid}.json")
        try:
            forum = ctx.folders.forum
            for base, src in by_name.items():
                await copy_file(src, os.path.join(forum, base), ctx.folders.agent, ctx.sync)
                copied.append(base)
            ex.attachments.extend(by_name)
            req = self._envelope(ex.rid, ex.dst, name, params, timeout, list(by_name))
            reason = validate("request", req)
            if reason is not None:
                raise ValueError(
                    f"requestor: built an invalid request for {ex.dst}/{name}: {reason}"
                )
            await write_json_atomic(path, req, ctx.folders.agent, ctx.sync)
        except BaseException:
            # B1 did not finish, and the copies it made are owed an owner. A torn write is not a cancelled
            # one: `write_json_atomic` runs in a thread that no cancellation stops, so the request may be at
            # the peer after all — then its attachments stay for the peer to read, and D-16's forum sweep
            # clears what an exchange never finished. With no request at the peer, nothing may stay.
            if await _peek(path) is None:
                await self._remove_from_forum(copied)
                ex.attachments.clear()
            raise
        return req

    async def _remove_from_forum(self, names: list[str]) -> None:
        for name in names:
            with contextlib.suppress(OSError):
                await remove(os.path.join(self._ctx.folders.forum, name))

    # -- B2 ---------------------------------------------------------------

    async def _loop(
        self,
        ex: _Outbound,
        sub: FolderWatch,
        timeout: float,
        cancel: asyncio.Event | None,
    ) -> RequestOutcome:
        clock = self._ctx.config.clock
        deadline = clock() + timeout if timeout > 0 else None
        while True:
            if ex.settling is not None:  # stop() (or a race) settled it
                return await asyncio.shield(ex.settling)
            sub.changed.clear()
            await self._capture(ex)
            if ex.res is not None:
                return await self._complete(ex)
            if sub.lost is not None:
                self._hooks.on_watch_error(ex.inbox, sub.lost)
                return await self._settle(ex, "abandoned", None)
            due = (
                ex.give_up
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
        tasks: list[asyncio.Task[Any]] = [asyncio.create_task(sub.changed.wait())]  # fan-out
        if cancel is not None:
            tasks.append(asyncio.create_task(cancel.wait()))  # fan-out
        if stop_at is not None:
            wake = asyncio.sleep(max(0.0, stop_at - self._ctx.now()))
            tasks.append(asyncio.create_task(wake))  # fan-out
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
        if (peeked := await _peek(path)) is None:
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
            if await _peek(os.path.join(ex.inbox, f"{ex.rid}.json")) is not None:
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
        return await self._settle(ex, "cancelled" if ex.cancelled else "confirmed", con)

    async def _settle(
        self, ex: _Outbound, final: SettleStatus, con: ConfirmationFile | None
    ) -> RequestOutcome:
        """Settle an exchange once. The settlement is its own task, awaited through `shield`, so a
        cancelled waiter can't orphan its writes; `stop()` awaits it like any other (D-9)."""
        if ex.settling is None:
            ex.done = True
            ex.outcome = RequestOutcome(final_status=final, response=ex.response_body)
            ex.settling = self._spawn(self._settle_now(ex, final, con), f"actio.settle.{ex.rid}")
        return await asyncio.shield(ex.settling)

    async def _settle_now(
        self, ex: _Outbound, final: SettleStatus, con: ConfirmationFile | None
    ) -> RequestOutcome:
        """B3/B5 in the protocol's order: the `outreq` line, the event, the confirmation, the attachments."""
        assert ex.outcome is not None
        try:
            rec: ExchangeRecord
            rec = {"final_status": final, "req": ex.req, "can": ex.can, "res": ex.res, "con": con}
            await self._ctx.append_diarium(outreq_event(self._ctx.id, self._ctx.now(), final, rec))
            self._ctx.emit(OutreqEvent(rid=ex.rid, final_status=final))
            if con is not None:
                con_path = os.path.join(ex.inbox, f"{ex.rid}.con.json")
                try:
                    await write_json_atomic(con_path, con, self._ctx.folders.agent, self._ctx.sync)
                except Exception as err:
                    self._ctx.emit(LogEvent(message=f"confirmation of {ex.rid} failed: {err}"))
            await self._remove_from_forum(ex.attachments)
            ex.attachments.clear()
        except Exception as err:
            self._ctx.emit(LogEvent(message=f"requestor: settlement of {ex.rid} failed: {err}"))
        finally:
            self._exchanges.pop(ex.rid, None)
            self._hooks.on_state_change()
        return ex.outcome

    def _dst_inbox(self, dst: AgentId) -> str:
        return os.path.join(self._ctx.folders.root, dst, "inbox")
