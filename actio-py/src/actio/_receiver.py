"""Lane A (inbound requests) and lane D (commands): the receiver of one agent (D-3, D-4, D-5, D-11, D-15, D-22)."""

from __future__ import annotations

import asyncio
import dataclasses
import os
import re
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any, Protocol, cast

from ._diarium import inreq_event, log_event
from ._files import (
    loads,
    make_dirs,
    mtime_ns,
    read_text_with_mtime,
    remove,
    unreadable_reason,
    write_bytes_atomic,
    write_json_atomic,
)
from ._protocol import (
    AgentId,
    CancellationBody,
    CancellationFile,
    Capability,
    ConfirmationFile,
    ExchangeRecord,
    Header,
    InreqFinalStatus,
    RequestBody,
    RequestFile,
    ResponseFile,
    Spec,
)
from ._types import (
    ActioError,
    AgentContext,
    CapabilityHandler,
    IgnoredEvent,
    InreqEvent,
    LogEvent,
    Spawn,
)
from ._util import format_iso, nonce
from ._validate import (
    CapabilityValidator,
    SchemaKind,
    check_params,
    compile_capability,
    envelope_kind,
    malformed_reason,
    name_matches,
    same_header,
    validate,
)
from ._watch import FolderWatch, Watcher, scan

_SUFFIX: dict[str, str] = {"cancellation": "can", "response": "res", "confirmation": "con"}
_SUFFIX_RE = re.compile(r"\.(can|res|con)\.json$")

# `Observer.join` blocks, so the close of the watcher is bounded.
WATCH_JOIN_TIMEOUT = 5.0

# The A2 answer of a work abandoned by `stop()` (D-10).
AGENT_STOPPING_MSG = "agent_stopping: the agent stops before answering"
STOPPING_RESULT: dict[str, Any] = {"code": 300, "msg": AGENT_STOPPING_MSG}
# D-9's bound on the receiver's last phase: how long `stop()` waits for a handler it has cancelled. The
# reference awaits no handler at all — its `stop()` aborts the handler's controller and awaits only the
# archives (`actio-ts/src/receiver.ts`, `stop()`). Python waits, but only up to this bound: a handler that
# swallows its cancellation would otherwise hold `stop()` — and with it C1, the watcher's close, and every
# later lifecycle step — forever.
HANDLER_CANCEL_WAIT = 1.0


class ReceiverHooks(Protocol):
    """What the receiver asks its agent to do: D-22 watch errors, the peer's spec (106), D-8 state changes, commands."""

    def on_watch_error(self, folder: str, error: BaseException) -> None: ...

    def peer_spec(self, src: AgentId) -> Spec | None: ...

    def on_state_change(self) -> None: ...

    async def on_command_stop(self, force: bool) -> None: ...

    async def on_command_restart(self, force: bool, delay_seconds: float) -> None: ...

    async def on_command_enable(self, enabled: bool) -> None: ...

    async def on_command_ping(self) -> None: ...


class _HandlerTable(Protocol):
    """The capability handlers, carried by the agent's private context object (`capability_handler`)."""

    def capability_handler(self, name: str) -> CapabilityHandler | None: ...


@dataclasses.dataclass
class _Entry:
    """One registered capability: its definition, its compiled checks, its handler, its running count."""

    cap: Capability
    validator: CapabilityValidator
    handler: CapabilityHandler | None
    running: int = 0


@dataclasses.dataclass
class _Exchange:
    """The per-`rid` state of one inbound exchange (in memory only; D-4 re-arms from files at start)."""

    rid: str
    header: Header
    request: RequestFile | None
    phase: str  # "accepting" | "working" | "responded"
    cancelled: asyncio.Event
    can: CancellationFile | None = None
    res: ResponseFile | None = None
    con: ConfirmationFile | None = None
    attachments: list[str] = dataclasses.field(default_factory=list)
    task: asyncio.Task[None] | None = None  # the handler task
    serve: asyncio.Task[None] | None = None  # the exchange task (A0 through A2)
    timer: asyncio.TimerHandle | None = None  # the confirmation wait
    result: Any = None  # the handler's result, read after the handler task settles


class _CapabilityContext:
    """The per-request context a handler sees: `rid`, `src`, the cancel flag, attachments, publish."""

    rid: str
    src: AgentId
    cancelled: asyncio.Event
    attachments: list[str]

    def __init__(self, receiver: Receiver, ex: _Exchange) -> None:
        self.rid, self.src = ex.rid, ex.header["src"]
        self.cancelled = ex.cancelled
        body = ex.request["request"] if ex.request is not None else None
        self.attachments = list(body.get("attachments") or []) if isinstance(body, dict) else []
        self._receiver, self._ex = receiver, ex

    async def publish_attachment(self, name: str, data: bytes) -> str:
        return await self._receiver._publish_attachment(self._ex, name, data)


class Receiver:
    """Serves one agent's inbox until `stop`; created at `start`, closed by `stop`."""

    def __init__(
        self,
        ctx: AgentContext,
        hooks: ReceiverHooks,
        watch: Watcher,
        spawn: Spawn,
        handler_cancel_wait: float = HANDLER_CANCEL_WAIT,
    ) -> None:
        self._ctx, self._hooks, self._watch, self._spawn = ctx, hooks, watch, spawn
        # D-9's bound, shrinkable in tests; the agent leaves it at the module constant.
        self._handler_cancel_wait = handler_cancel_wait
        self._handlers: _HandlerTable = cast(_HandlerTable, ctx)
        self._inbox, self._forum = ctx.folders.inbox, ctx.folders.forum
        self._exchanges: dict[str, _Exchange] = {}
        self._entries: dict[str, _Entry] = {}
        self._archived: set[str] = set()  # D-5: rids archived in this run
        self._command_rids: set[str] = set()  # D-5 for commands: executed once per run
        self._deferred: dict[str, list[tuple[str, Any]]] = {}
        self._ignored_seen: set[str] = set()  # D-15: one log per (path, mtime)
        self._markers: dict[str, str] = {}
        self._archiving: dict[str, asyncio.Task[Any]] = {}
        self._command_queue: list[tuple[str, Callable[[], Coroutine[Any, Any, None]]]] = []
        self._drain: asyncio.Task[Any] | None = None
        self._lifecycle: set[asyncio.Task[Any]] = set()  # command tasks: outside teardown's cancel
        self._tasks: set[asyncio.Task[Any]] = set()
        self._sub: FolderWatch | None = None
        self._loop_task: asyncio.Task[Any] | None = None
        self._stopped = False

    async def start(self) -> None:
        # Recover pending exchanges (D-4), then watch the inbox and serve it.
        if self._sub is not None:
            return
        self._sub = self._watch.subscribe(self._inbox)
        self._entries = self._build_entries()
        await self._scan_once()
        self._loop_task = self._start(self._loop_coro(), "actio.receiver", "receiver loop failed")

    async def stop(self, *, force: bool = False) -> None:
        """`force` abandons work still running (D-9, D-10); the grace wait, when wanted, is the agent's."""
        # Both modes settle the exchanges the same way: abandon the work, answer an accepted and
        # unanswered request (code 300, or the cancellation if it was cancelled), archive every
        # exchange that has a response. When this returns, nothing of the receiver is armed. The
        # `stop` and `restart` command tasks are the exception: they own the teardown, are left running.
        if self._stopped:
            return
        self._stopped = True
        if self._loop_task is not None:
            self._loop_task.cancel()
            await asyncio.gather(self._loop_task, return_exceptions=True)
            self._loop_task = None
        for ex in list(self._exchanges.values()):
            self._clear_timer(ex)
            if ex.phase == "working" and ex.res is None:
                if ex.task is not None and not ex.task.done():
                    ex.task.cancel()
                # D-9: accepted and unanswered → code 300, the single response
                await self._respond(ex, "error", result=STOPPING_RESULT, forced=True)
            elif ex.phase == "accepting" and ex.serve is not None and not ex.serve.done():
                ex.serve.cancel()
        # (the serve task) -> (the exchange it serves, its handler task), captured now: an exchange can
        # leave `_exchanges` during the bound — its confirmation timer fires, and it archives itself — and a
        # pending task that nobody cancels is a `stop()` that never returns.
        waits = {
            ex.serve: (ex, ex.task)
            for ex in self._exchanges.values()
            if ex.serve and not ex.serve.done()
        }
        if waits:
            # D-9's phase bound: the wait for a cancelled handler is `HANDLER_CANCEL_WAIT`, no longer. The
            # exchange is already answered with the code 300 above, so this wait can only collect a late
            # answer that `_respond`'s guard would drop anyway (probed: one response, one content). Past the
            # bound the handler task is left to end on its own — the one documented exception to "nothing
            # of ours is armed after stop", named by its rid — and its exchange is archived like any
            # answered one. A handler that answers inside the bound is answered as before.
            _, pending = await asyncio.wait(list(waits), timeout=self._handler_cancel_wait)
            for task in pending:
                task.cancel()  # `serve` is ours; the handler whose cancellation it swallowed is not
            await asyncio.gather(*pending, return_exceptions=True)
            for task in pending:
                ex, handler = waits[task]
                if handler is not None and not handler.done():
                    self._ctx.emit(
                        LogEvent(
                            message=f"stop: the handler of {ex.rid} outlived {HANDLER_CANCEL_WAIT}s "
                            f"of D-9's bound; it is left to end on its own"
                        )
                    )
        for ex in list(self._exchanges.values()):
            if ex.res is not None:
                self._start_archive(ex)
        if self._archiving:
            await asyncio.gather(*list(self._archiving.values()), return_exceptions=True)
        if self._drain is not None and not self._drain.done():
            self._drain.cancel()
            await asyncio.gather(self._drain, return_exceptions=True)
        if self._sub is not None:
            self._watch.unsubscribe(self._sub)
            self._sub = None
        try:
            await asyncio.wait_for(self._watch.close(), WATCH_JOIN_TIMEOUT)
        except TimeoutError:
            self._ctx.emit(LogEvent(message=f"watcher join timed out: {WATCH_JOIN_TIMEOUT}s"))

    # --- the one task helper -------------------------------------------------

    async def _run_guarded(self, coro: Coroutine[Any, Any, Any], message: str) -> None:
        """The crash report of an unexpected exception, as exactly one `LogEvent` (never a crash);
        a cancelled task reports nothing."""
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception as err:
            self._ctx.emit(LogEvent(message=f"{message}: {err}"))

    def _start(
        self, coro: Coroutine[Any, Any, Any], name: str, message: str | None = None
    ) -> asyncio.Task[Any]:
        """One of this receiver's tasks: the wrapped coroutine handed to the injected `Spawn`, and a
        strong reference kept until it ends (a task with none can be garbage-collected mid-run)."""
        task = self._spawn(self._run_guarded(coro, message) if message is not None else coro, name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    # --- the inbox loop ------------------------------------------------------

    async def _loop_coro(self) -> None:
        assert (sub := self._sub) is not None
        while not self._stopped:
            sub.changed.clear()
            if sub.lost is not None:
                # D-22: report the loss, then wait on nothing from the inbox until `stop`.
                error, sub.lost = sub.lost, None
                self._hooks.on_watch_error(self._inbox, error)
            else:
                await self._scan_once()
            await sub.changed.wait()

    async def _scan_once(self) -> None:
        try:
            changed, self._markers = await scan(self._inbox, self._markers)
        except OSError as err:
            cast(
                FolderWatch, self._sub
            ).lost = err  # a failed scan is a loss (D-22); the loop reports it
            return
        for name in changed:
            await self._process_file(name)

    async def _process_file(self, name: str) -> None:
        if self._stopped or not name.endswith(".json"):
            return
        path = os.path.join(self._inbox, name)
        value, reason = await self._read_envelope(path)
        if value is None:
            if reason is not None:
                await self._ignore(path, name, reason)
            return
        if (kind := envelope_kind(value)) is None:
            await self._ignore(path, name, validate("request", value) or "no envelope body key")
            return
        if isinstance(value[kind], str):
            await self._ignore(path, name, "base64 body (encryption is not supported)")
            return
        if (reason := validate(kind, value)) is not None:
            await self._ignore(path, name, reason)
            return
        if kind == "request":
            await self._dispatch_request(cast(RequestFile, value), path, name)
        elif kind == "response":
            await self._dispatch_response(cast(ResponseFile, value))
        else:
            await self._dispatch_later(
                cast(CancellationFile | ConfirmationFile, value), path, name, kind
            )

    async def _read_envelope(self, path: str) -> tuple[dict[str, Any] | None, str | None]:
        # (value, ignore-reason); a vanished file is (None, None).
        try:
            read = await read_text_with_mtime(path)
        except OSError as err:
            return None, unreadable_reason(err)
        except UnicodeDecodeError as err:
            # Readable, but not text: an unparsable file, so `_validate` words it (D-15). The wording
            # names the parser's position, never a byte of the peer's value; `unreadable` is for the
            # read that failed, which is what `_files.unreadable_reason` is for.
            return None, malformed_reason(err)
        if read is None:
            return None, None
        try:
            value = loads(read[1])
        except ValueError as err:
            return None, malformed_reason(err)
        return (value, None) if isinstance(value, dict) else (None, "not a JSON object")

    # --- lane A ---------------------------------------------------------------

    async def _dispatch_request(self, file: RequestFile, path: str, name: str) -> None:
        if file["dst"] != self._ctx.id:
            return await self._ignore(path, name, "dst is not this agent")
        if file["type"] == "cmd":
            return await self._dispatch_command(file)
        rid = file["rid"]
        if rid in self._archived or rid in self._exchanges:
            return  # D-5: a duplicate rid is ignored, not re-served
        header = cast(
            Header, {k: file[k] for k in ("rid", "src", "dst", "datetime", "type", "actio_version")}
        )
        ex = _Exchange(rid, header, file, "accepting", asyncio.Event())
        self._exchanges[rid] = ex
        self._drain_deferred(ex)
        ex.serve = self._start(self._serve(ex), f"actio.serve.{rid}", f"{rid} left unsettled")

    async def _serve(self, ex: _Exchange) -> None:
        # A0, then A1 (the handler as its own task), then A2.
        request = ex.request
        if request is None:
            return
        body = request["request"]
        if not isinstance(body, dict):
            return  # a base64 body was filtered out in `_process_file`
        if not ex.cancelled.is_set() and (pending := await self._pending_cancellation(ex)):
            ex.can = pending
            ex.cancelled.set()
        if ex.cancelled.is_set():
            return await self._respond(ex, "cancellation")
        verdict = self._accept(ex, body)
        if verdict is not None:
            return await self._respond(ex, "error", result={"code": verdict[0], "msg": verdict[1]})
        entry = self._entries[body["name"]]
        ex.phase = "working"
        entry.running += 1
        self._hooks.on_state_change()
        started = self._ctx.now()
        try:
            ex.task = self._start(self._work(ex, entry, body), f"actio.work.{ex.rid}")
            await asyncio.wait({ex.task})
            if ex.cancelled.is_set():
                await self._respond(ex, "cancellation")
            elif not ex.task.cancelled():  # a cancelled task is answered by `stop()` itself
                failure = ex.task.exception()
                kind, result = (
                    ("ok", ex.result) if failure is None else ("error", self._error_result(failure))
                )
                wt = max(0, int((self._ctx.now() - started) // 1))
                await self._respond(
                    ex, kind, result=result, attachments=ex.attachments, work_time=wt
                )
        finally:
            entry.running -= 1
            self._hooks.on_state_change()

    async def _work(self, ex: _Exchange, entry: _Entry, body: RequestBody) -> None:
        # The handler runs bare: its `CancelledError` or exception stays in the task, and `_serve`
        # reads it through `asyncio.wait`. A capability with no handler is refused in `_accept`.
        handler = cast(CapabilityHandler, entry.handler)
        ex.result = await handler(body["parameters"], _CapabilityContext(self, ex))

    def _accept(self, ex: _Exchange, body: RequestBody) -> tuple[int, str] | None:
        # A0, in the protocol's table order; the first failure found is answered. There is no 104:
        # a foreign `actio_version` fails the schema first, with a schema reason.
        if self._ctx.spec().get("disabled") is True:
            return 100, "agent_disabled: the agent is disabled"
        entry = self._entries.get(body["name"])
        if entry is None or entry.handler is None:
            return 101, f"unknown_capability: no capability named {body['name']!r}"
        actio = entry.cap["actio"]
        if actio.get("disabled") is True:
            return 102, f"capability_disabled: {body['name']!r} is disabled"
        src = ex.header["src"]
        if (accepts_from := actio.get("accepts_from")) and src not in accepts_from:
            return 103, f"requestor_not_allowed: {body['name']!r} does not accept {src!r}"
        if (reason := check_params(entry.cap, body["parameters"] or {})) is not None:
            return 105, f"invalid_parameters: {body['name']!r}: {reason}"
        if any(not self._inside_forum(a) for a in body["attachments"] or []):
            return 105, "invalid_parameters: an attachment path escapes forum"
        peer = self._hooks.peer_spec(src)
        if peer is not None and not self._shares_namespace(self._ctx.spec(), peer):
            return 106, f"namespace_mismatch: no shared namespace with {src!r}"
        if (max_parallel := actio.get("max_parallel") or 0) > 0 and entry.running >= max_parallel:
            return 107, f"capacity_exceeded: {body['name']!r} is at max_parallel {max_parallel}"
        if (timeout := body["timeout"] or 0) and body["ts"] + timeout < self._ctx.now():
            return 108, f"request_expired: the timeout of {timeout}s passed before acceptance"
        return None

    def _inside_forum(self, name: str) -> bool:
        forum = Path(self._forum)
        return (forum / name).resolve().is_relative_to(forum.resolve())

    @staticmethod
    def _shares_namespace(a: Spec, b: Spec) -> bool:
        na = a.get("namespaces") or [None]
        nb = b.get("namespaces") or [None]
        return "*" in na or "*" in nb or any(n in nb for n in na)

    @staticmethod
    def _error_result(failure: BaseException) -> dict[str, Any]:
        if not isinstance(failure, ActioError):
            return {"msg": str(failure)}
        result: dict[str, Any] = {"code": failure.code, "msg": str(failure)}
        if failure.details is not None:
            result["details"] = str(failure.details)
        return result

    async def _pending_cancellation(self, ex: _Exchange) -> CancellationFile | None:
        # A0: a valid cancellation file already present for the rid (matching header and name).
        value = await self._read_inbox_value(f"{ex.rid}.can.json")
        if value is None or validate("cancellation", value) is not None:
            return None
        if not same_header(cast(Header, value), ex.header):
            return None
        req = ex.request
        if req is not None and not name_matches(
            cast(RequestBody, req["request"]), cast(CancellationBody, value["cancellation"])
        ):
            return None
        return cast(CancellationFile, value)

    async def _respond(
        self,
        ex: _Exchange,
        kind: str,
        *,
        result: Any = None,
        attachments: list[str] | None = None,
        work_time: int | None = None,
        forced: bool = False,
    ) -> None:
        # A2: write once; after `stop()` starts, only the forced code-300 write may still land.
        if ex.res is not None or (self._stopped and not forced):
            return
        now = self._ctx.now()
        body: dict[str, Any] = {
            "ts": int(now // 1),
            "nonce": nonce(),
            "signature": None,
            "error": kind == "error",
            "cancellation": kind == "cancellation",
            "result": None if kind == "cancellation" else result,
            "attachments": [] if kind == "cancellation" else list(attachments or []),
        }
        if work_time is not None:
            body["work_time"] = work_time
        file: dict[str, Any] = {**ex.header, "datetime": format_iso(now), "response": body}
        if (reason := validate("response", file)) is not None:
            raise ValueError(f"invalid response for {ex.rid}: {reason}")
        res_path = os.path.join(self._inbox, f"{ex.rid}.res.json")
        await write_json_atomic(res_path, file, self._ctx.folders.agent, self._ctx.sync)
        self._apply_response(ex, cast(ResponseFile, file))

    async def _dispatch_later(
        self, file: CancellationFile | ConfirmationFile, path: str, name: str, kind: str
    ) -> None:
        ex = self._exchanges.get(file["rid"])
        if ex is None:
            if f"{file['rid']}.json" in self._markers:
                self._defer(file["rid"], kind, file)  # matched once the request is tracked
            else:
                await self._ignore(path, name, f"unmatched rid {file['rid']} (no tracked exchange)")
            return
        reason = self._apply_later(ex, file, kind)
        if reason is not None:
            await self._ignore(path, name, reason)

    def _apply_later(
        self, ex: _Exchange, file: CancellationFile | ConfirmationFile, kind: str
    ) -> str | None:
        # The effect of one matched envelope, by kind. A cancellation sets the flag the handler
        # checks at safe points; the handler task is never killed by it (D-2).
        if not same_header(cast(Header, file), ex.header):
            return f"header mismatch for rid {ex.rid} (D-11)"
        if kind == "cancellation":
            if ex.can is not None:
                return None  # the cancellation file is written once; later ones are no-ops
            can_body = cast(CancellationBody, cast(CancellationFile, file)["cancellation"])
            req = ex.request
            if req is not None and not name_matches(cast(RequestBody, req["request"]), can_body):
                return f"cancellation name mismatch for rid {ex.rid}"
            ex.cancelled.set()
            # A `can` after the response is recorded too: the archive ends `cancelled`
            # (*Envelope lifecycle*, "Cancelled after the response"); no second response.
            ex.can = cast(CancellationFile, file)
            return None
        if ex.con is not None:
            return None  # the confirmation file is written once; later ones are no-ops
        ex.con = cast(ConfirmationFile, file)
        if ex.res is not None:
            self._clear_timer(ex)
            self._start_archive(ex)
        # a confirmation before the response (out of order) is stored: A2 archives immediately
        return None

    async def _dispatch_response(self, file: ResponseFile) -> None:
        ex = self._exchanges.get(file["rid"])
        if ex is None:
            if f"{file['rid']}.json" in self._markers:
                self._defer(file["rid"], "response", file)  # the request file is still pending
            else:
                await self._retrack_from_response(file)  # D-4: a bare `.res.json` of a previous run
            return
        if ex.res is None:
            self._apply_response(ex, file)

    def _apply_response(self, ex: _Exchange, file: ResponseFile) -> None:
        # The exchange's response, from A2 (our own write) or from a stored file: complete the
        # state, then archive on an existing confirmation or arm the confirmation wait.
        if ex.res is not None:
            return
        ex.res = file
        ex.phase = "responded"
        self._start_archive(ex) if ex.con is not None else self._arm_timer(ex)

    async def _retrack_from_response(self, file: ResponseFile) -> None:
        # D-4: re-track a pending `.res.json` (previous run) and restart the confirmation wait.
        rid = file["rid"]
        if self._stopped or rid in self._archived or rid in self._exchanges:
            return
        header = cast(
            Header, {k: file[k] for k in ("rid", "src", "dst", "datetime", "type", "actio_version")}
        )
        ex = _Exchange(rid, header, None, "responded", asyncio.Event(), res=file)
        can = await self._read_known_envelope(f"{rid}.can.json", "cancellation", file)
        if can is not None:
            # A stored late `can` is recorded: the archive ends `cancelled`
            # (*Envelope lifecycle*, "Cancelled after the response").
            ex.can = cast(CancellationFile, can)
            ex.cancelled.set()
        con = await self._read_known_envelope(f"{rid}.con.json", "confirmation", file)
        if con is not None:
            ex.con = cast(ConfirmationFile, con)
        self._exchanges[rid] = ex
        self._start_archive(ex) if ex.con is not None else self._arm_timer(ex)

    def _defer(self, rid: str, kind: str, file: Any) -> None:
        self._deferred.setdefault(rid, []).append((kind, file))

    def _drain_deferred(self, ex: _Exchange) -> None:
        # Cancellation first, then the rest in arrival order; runs before A0 starts.
        if (pending := self._deferred.pop(ex.rid, None)) is None:
            return
        items = sorted(pending, key=lambda item: 0 if item[0] == "cancellation" else 1)
        for kind, file in items:
            name = f"{ex.rid}.{_SUFFIX[kind]}.json"
            full = os.path.join(self._inbox, name)
            if kind == "response":
                self._apply_response(ex, cast(ResponseFile, file))
                reason = None
            else:
                reason = self._apply_later(
                    ex, cast(CancellationFile | ConfirmationFile, file), kind
                )
            if reason is not None:
                self._start(self._ignore(full, name, reason), f"actio.ignore.{ex.rid}")

    async def _read_inbox_value(self, name: str) -> dict[str, Any] | None:
        try:
            read = await read_text_with_mtime(os.path.join(self._inbox, name))
        except (OSError, UnicodeDecodeError):
            return None
        if read is None:
            return None
        try:
            value = loads(read[1])
        except ValueError:
            return None
        return value if isinstance(value, dict) else None

    async def _read_known_envelope(
        self, name: str, kind: str, header: Header
    ) -> dict[str, Any] | None:
        value = await self._read_inbox_value(name)
        if value is None or validate(cast(SchemaKind, kind), value) is not None:
            return None
        return value if same_header(cast(Header, value), header) else None

    # --- lane D ---------------------------------------------------------------

    async def _dispatch_command(self, file: RequestFile) -> None:
        # D-5: one command runs once per rid per run. From `stop`, the inbox is owned by the teardown.
        if self._stopped:
            return  # the file stays in the inbox for the next start (D-4)
        rid = file["rid"]
        if rid in self._archived or rid in self._command_rids:
            return
        self._command_rids.add(rid)
        body = file["request"]
        if not isinstance(body, dict):
            return  # a base64 body was filtered out in `_process_file`
        reason = self._accept_command(file, body["name"])
        if reason is None:

            def step() -> Coroutine[Any, Any, None]:
                return self._run_command(file, body["name"])

            self._queue_command(rid, step)
        else:

            def step_drop() -> Coroutine[Any, Any, None]:
                return self._archive_command(file, "dropped", reason)

            self._queue_command(rid, step_drop)

    def _accept_command(self, file: RequestFile, name: str) -> str | None:
        # D0: may this `src` command us at all, and do we know the name? Disabled agents still
        # answer, so that they can be enabled again or shut down.
        spec = self._ctx.spec()  # read fresh: `enable` may have changed the list since we started
        if file["src"] not in spec.get("accepts_commands_from", []):
            return f"{file['src']} is not in accepts_commands_from"
        if name not in ("stop", "enable", "restart", "ping"):
            return f"unknown command {name!r}"
        return None

    def _queue_command(self, rid: str, step: Callable[[], Coroutine[Any, Any, None]]) -> None:
        self._command_queue.append((rid, step))
        if self._drain is None or self._drain.done():
            self._drain = self._start(self._drain_commands(), "actio.commands", "drain failed")

    async def _drain_commands(self) -> None:
        # Lane D runs one command at a time, sorted by rid (a ULID: the requestor's creation order),
        # whatever order the scan handed the files over. A broken step never breaks the drain.
        while self._command_queue and not self._stopped:
            self._command_queue.sort(key=lambda item: item[0])
            rid, step = self._command_queue.pop(0)
            task = self._start(step(), f"actio.command.{rid}")
            self._lifecycle.add(task)
            task.add_done_callback(self._lifecycle.discard)
            try:
                await task
            except Exception as err:
                self._ctx.emit(LogEvent(message=f"command {rid} failed: {err}"))

    async def _run_command(self, file: RequestFile, name: str) -> None:
        # D1-D3: the archive always completes BEFORE the lifecycle hook runs; the `restart` delay
        # only starts after it (D-10).
        await self._archive_command(file, "completed")
        body = file["request"]
        params = body.get("parameters") if isinstance(body, dict) else {}
        params = params if isinstance(params, dict) else {}
        if name == "stop":
            await self._hooks.on_command_stop(params.get("force") is True)
        elif name == "restart":
            delay = params.get("delay")
            if not isinstance(delay, (int, float)) or isinstance(delay, bool):
                delay = 0.0
            await self._hooks.on_command_restart(params.get("force") is True, float(delay))
        elif name == "enable":
            await self._hooks.on_command_enable(params.get("enabled") is True)
        else:
            await self._hooks.on_command_ping()

    async def _archive_command(
        self, file: RequestFile, final_status: InreqFinalStatus, reason: str | None = None
    ) -> None:
        # D3/D4: the `inreq` line with the command envelope alone, then delete the envelope. Commands
        # are never cancelled and never answered: only `req` is filled.
        record: ExchangeRecord = {
            "final_status": final_status,
            "req": file,
            "can": None,
            "res": None,
            "con": None,
        }
        event = inreq_event(self._ctx.id, self._ctx.now(), final_status, record)
        await self._ctx.append_diarium(event)
        await remove(os.path.join(self._inbox, f"{file['rid']}.json"))
        self._archived.add(file["rid"])
        if final_status == "dropped":
            self._ctx.emit(LogEvent(message=f"dropped command {file['rid']}: {reason}"))
        self._ctx.emit(InreqEvent(rid=file["rid"], final_status=final_status))

    # --- the archive (A4) ------------------------------------------------------

    def _start_archive(self, ex: _Exchange) -> None:
        # D-9: the promise of every archive in flight is tracked, so `stop()` can await them all.
        if ex.rid in self._archiving:
            return
        task = self._start(
            self._archive(ex), f"actio.archive.{ex.rid}", f"archive of {ex.rid} failed"
        )
        self._archiving[ex.rid] = task

        def done(t: asyncio.Task[Any]) -> None:
            if self._archiving.get(ex.rid) is t:
                del self._archiving[ex.rid]

        task.add_done_callback(done)

    async def _archive(self, ex: _Exchange) -> None:
        # A4, two steps in one order (*Archive*): the `inreq` line, then the deletions. `archived`
        # is marked BEFORE the first await, so a late envelope cannot add a second `inreq` (D-5).
        if ex.rid not in self._exchanges or ex.rid in self._archived:
            return
        self._archived.add(ex.rid)
        self._clear_timer(ex)
        has_can, has_con = ex.can is not None, ex.con is not None
        final_status: InreqFinalStatus = (
            "cancelled" if has_can else "confirmed" if has_con else "completed"
        )
        record: ExchangeRecord = {
            "final_status": final_status,
            "req": ex.request,
            "can": ex.can,
            "res": ex.res,
            "con": ex.con,
        }
        event = inreq_event(self._ctx.id, self._ctx.now(), final_status, record)
        await self._ctx.append_diarium(event)
        for suffix in (".json", ".can.json", ".res.json", ".con.json"):
            await remove(os.path.join(self._inbox, ex.rid + suffix))
        for name in ex.attachments:
            await remove(os.path.join(self._forum, name))
        del self._exchanges[ex.rid]
        self._ctx.emit(InreqEvent(rid=ex.rid, final_status=final_status))
        self._hooks.on_state_change()

    def _arm_timer(self, ex: _Exchange) -> None:
        if ex.timer is None:
            ex.timer = asyncio.get_running_loop().call_later(
                self._ctx.config.confirm_timeout, self._start_archive, ex
            )

    def _clear_timer(self, ex: _Exchange) -> None:
        if ex.timer is not None:
            ex.timer.cancel()
            ex.timer = None

    # --- ignored files (D-15) ---------------------------------------------------

    async def _ignore(self, path: str, name: str, reason: str) -> None:
        # One line, once per (path, mtime); the file stays in the inbox, nothing else is touched.
        try:
            key = f"{path}:{await mtime_ns(path)}"
        except OSError:
            return  # vanished: nothing to ignore
        if key in self._ignored_seen:
            return
        self._ignored_seen.add(key)
        self._ctx.emit(IgnoredEvent(path=path, reason=reason))
        line = log_event(self._ctx.id, self._ctx.now(), f"ignored {name}: {reason}")
        await self._ctx.append_diarium(line)
        if not _SUFFIX_RE.match(name):
            self._deferred.pop(name[: -len(".json")], None)  # the request was never tracked

    # --- attachments --------------------------------------------------------------

    async def _publish_attachment(self, ex: _Exchange, name: str, data: bytes) -> str:
        # Written to `forum` in one atomic operation through the sync provider's hooks; a half-taken
        # attachment is what a peer reads back as a truncated file. The archive deletes it.
        target = Path(self._forum) / name
        if not target.resolve().is_relative_to(Path(self._forum).resolve()):
            raise ValueError(f"attachment {name!r} escapes forum")
        await make_dirs(str(target.parent))
        await write_bytes_atomic(str(target), data, self._ctx.folders.agent, self._ctx.sync)
        ex.attachments.append(name)
        return name

    # --- the entries ----------------------------------------------------------------

    def _build_entries(self) -> dict[str, _Entry]:
        entries: dict[str, _Entry] = {}
        for cap in self._ctx.spec().get("capabilities", []):
            name = cap["name"]
            if name not in entries:
                entries[name] = _Entry(
                    cap, compile_capability(cap), self._handlers.capability_handler(name)
                )
        return entries
