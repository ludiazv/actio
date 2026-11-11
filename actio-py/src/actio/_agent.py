"""`ActioAgent`: the lifecycle, lane C (its own capability registrations), the commands, and the public
API (*Workflow*, *Agent's status*, *Callability rules*, D-8, D-9, D-10).

The signature below is the frozen one: keyword-only, and `namespaces=None` means the field is **absent**
from `spec.json`, not `null` inside it (*Callability rules*). Nothing of the environment is read until `start()`,
so a bad `ACTIO_SYNC` surfaces there (D-21).

An agent belongs to one event loop and is not thread-safe. It installs no signal handler and starts no
event loop: the example does both.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import inspect
import os
import time
from collections.abc import Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast, overload

from ._config import ActioConfig, load_config
from ._diarium import append, log_event, read_diarium, status_event
from ._discovery import agent_view as _peer_view
from ._discovery import scan as _tree_scan
from ._files import make_dirs as _make_dirs
from ._protocol import (
    AgentId,
    Capability,
    CapabilityStatusEntry,
    DiariumEntry,
    ResponseBody,
    Rid,
    Spec,
    Status,
    StatusState,
)
from ._receiver import Receiver
from ._requestor import Requestor
from ._spec_builder import build_spec
from ._status import StatusPublisher, build_status, heartbeat
from ._sync import create_sync_provider
from ._types import (
    ActioCallError,
    ActioEvent,
    ActioResultError,
    AgentContext,
    AgentFolders,
    AgentView,
    AnnounceEvent,
    CapabilityHandler,
    CommandHandler,
    DiariumRead,
    IgnoredEvent,
    LogEvent,
    RequestOutcome,
    StatusEvent,
    SyncProvider,
)
from ._validate import CapabilityValidator, compile_capability
from ._watch import AUDIT_SECONDS, FolderWatch, Watcher

# The mandatory commands (*Commands*); `on_command` cannot shadow them, and they are never advertised.
STANDARD_COMMANDS: frozenset[str] = frozenset({"stop", "enable", "restart", "ping"})

# D-10: the restart `delay` is clamped here; an unbounded delay lets a peer keep the agent down.
RESTART_DELAY_MAX = 60.0

# The grace wait polls the in-flight work at this step (D-9), in wall time, as the grace is.
GRACE_STEP = 0.02


@dataclass(frozen=True, slots=True)
class _CapEntry:
    """One registration: the deep-copied capability (owned, not the caller's object) and its handler."""

    cap: Capability
    handler: CapabilityHandler


@dataclass
class _Run:
    """The state of one run, replaced at each `start` and never set to `None` (D-9): a stale task
    reads a stale run, whose `stopped` is set, and no-ops. A restart builds a fresh one, with a
    fresh publisher and a fresh latch."""

    stopped: asyncio.Event  # the heartbeat's and the nuntium loop's stop signal
    config: ActioConfig
    sync: SyncProvider
    publisher: StatusPublisher
    watcher: Watcher | None = None
    nuntium: FolderWatch | None = None
    receiver: Receiver | None = None
    requestor: Requestor | None = None
    heartbeat: asyncio.Task[Any] | None = None
    nuntium_task: asyncio.Task[Any] | None = None
    scan_task: asyncio.Task[Any] | None = None


class _Ctx:
    """The `AgentContext` the agent hands its receiver and requestor: the seam, built by `start`,
    never a public attribute. It binds to its run, so a stale engine writes to a closed provider,
    never to the next run's."""

    # The attributes are plain (not properties): the `AgentContext` protocol names them as
    # settable members, and a read-only property does not satisfy it under a consumer's mypy.
    def __init__(self, agent: ActioAgent, run: _Run) -> None:
        self._agent, self._run = agent, run
        self.config: ActioConfig = run.config
        self.id: AgentId = agent._id
        self.folders: AgentFolders = cast(AgentFolders, agent._folders)
        self.sync: SyncProvider = run.sync

    def now(self) -> float:
        return self._run.config.clock()

    def spec(self) -> Spec:
        return self._agent._spec

    def state(self) -> StatusState:
        return self._agent._state

    def emit(self, event: ActioEvent) -> None:
        self._agent._emit(event)

    def capability_handler(self, name: str) -> CapabilityHandler | None:
        entry = self._agent._capabilities.get(name)
        return entry.handler if entry is not None else None

    async def append_diarium(self, entry: DiariumEntry) -> None:
        await append(cast(AgentFolders, self._agent._folders).agent, entry, self._run.sync)


class _ReceiverHooks:
    """The agent's side of `ReceiverHooks`: D-22, the peer's spec, D-8, and the four commands."""

    def __init__(self, agent: ActioAgent) -> None:
        self._agent = agent

    def on_watch_error(self, folder: str, error: BaseException) -> None:
        # D-22: a lost own inbox is fatal: log it and enter `error`.
        self._agent._spawn(self._agent._fail_watch(folder, error), "actio.fail")

    def peer_spec(self, src: AgentId) -> Spec | None:
        # D-7: code 106 must see the DISCOVERED spec, unfiltered by the callability rules.
        return self._agent._discovered.get(src)

    def on_state_change(self) -> None:
        self._agent._schedule_busy()

    async def on_command_stop(self, force: bool) -> None:
        await self._agent.stop(force=force)

    async def on_command_restart(self, force: bool, delay_seconds: float) -> None:
        self._agent._schedule_restart(force, delay_seconds)

    async def on_command_enable(self, enabled: bool) -> None:
        await self._agent._apply_enable(enabled)

    async def on_command_ping(self) -> None:
        await self._agent._apply_ping()


class _RequestorHooks:
    def __init__(self, agent: ActioAgent) -> None:
        self._agent = agent

    def on_watch_error(self, folder: str, error: BaseException) -> None:
        # D-22 names only the own inbox and `nuntium` for `fail()`: a destination watch error has
        # already been answered by the requestor (its exchanges there are abandoned).
        self._agent._emit(LogEvent(message=f"watch lost {folder}: {error}"))

    def on_state_change(self) -> None:
        self._agent._schedule_busy()  # outbound B2 also makes us busy (D-8)


class ActioAgent:
    """One agent in the shared tree: it serves its inbox and talks to the others.

    It subclasses nothing. Its engines (the receiver, the requestor, the status publisher) are handed
    `_ctx`, a private object that satisfies `AgentContext` structurally; making the agent itself the
    context would put `emit`, `sync`, `folders` and the rest on the public surface, and would make the
    class abstract to a consumer's `mypy`.
    """

    _ctx: AgentContext  # built by `start()`: the seam, never a public attribute

    def __init__(
        self,
        *,
        id: AgentId,
        description: str,
        accepts_commands_from: Sequence[AgentId],
        namespaces: Sequence[str | None] | None = None,
        depends: Sequence[AgentId] | None = None,
        wants: Sequence[AgentId] | None = None,
        info: dict[str, Any] | None = None,
        config: ActioConfig | None = None,
    ) -> None:
        self._id = id
        self._description = description
        self._accepts_commands_from = list(accepts_commands_from)
        # The caller's arrays are cloned: a later mutation of them cannot change what we publish.
        self._namespaces = None if namespaces is None else list(namespaces)
        self._depends = None if depends is None else list(depends)
        self._wants = None if wants is None else list(wants)
        self._info = None if info is None else copy.deepcopy(info)
        self._config_option = config  # D-21: the environment is read at `start()`, not here
        self._capabilities: dict[str, _CapEntry] = {}  # registration order
        self._commands: dict[str, CommandHandler] = {}
        self._listeners: list[Callable[[ActioEvent], None]] = []
        self._peer_schemas: dict[str, CapabilityValidator | None] = {}
        self._spec: Spec = cast(Spec, {})  # built by `start`; `enable` rewrites `disabled` in it
        self._state: StatusState = "inactive"
        self._view: dict[AgentId, AgentView] = {}
        self._discovered: dict[AgentId, Spec] = {}
        self._enabled_state: bool | None = None  # D-10: state of the run, not of the options
        self._folders: AgentFolders | None = None
        self._run: _Run | None = None  # replaced at each start, never set back to None
        self._lifecycle_task: asyncio.Task[Any] | None = None
        self._restart_task: asyncio.Task[Any] | None = None
        self._stopping_restart = False  # the restart's own stop must not cancel the restart
        self._stopped_for_good = asyncio.Event()
        self._tasks: set[asyncio.Task[Any]] = set()  # the strong references of `_spawn`

    # --- lane C: the registrations ---------------------------------------------

    @overload
    def capability(self, capability: Capability, handler: CapabilityHandler) -> None: ...
    @overload
    def capability(
        self, capability: Capability, handler: None = None
    ) -> Callable[[CapabilityHandler], CapabilityHandler]: ...
    def capability(
        self, capability: Capability, handler: CapabilityHandler | None = None
    ) -> Callable[[CapabilityHandler], CapabilityHandler] | None:
        """Register a capability: a call, or the decorator form. `ValueError` for a bad capability or a
        name already taken; `TypeError` for a handler that is not `async def`. The input is deep-copied,
        so the caller cannot alias what we publish."""

        def register(cap: Capability, h: CapabilityHandler | None) -> CapabilityHandler | None:
            if cap["name"] in self._capabilities:
                raise ValueError(f'agent: capability "{cap["name"]}" is already registered')
            if h is not None and not inspect.iscoroutinefunction(h):
                raise TypeError(f'agent: capability "{cap["name"]}" needs an async def handler')
            owned = copy.deepcopy(cap)  # own the registration, not the caller's object
            compile_capability(owned)  # D-19: a schema strict mode rejects is a registration bug
            self._capabilities[cap["name"]] = _CapEntry(owned, cast(CapabilityHandler, h))
            return h

        if handler is None:

            def decorator(h: CapabilityHandler) -> CapabilityHandler:
                register(capability, h)
                return h

            return decorator
        register(capability, handler)
        return None

    @overload
    def on_command(self, name: str, handler: CommandHandler) -> None: ...
    @overload
    def on_command(
        self, name: str, handler: None = None
    ) -> Callable[[CommandHandler], CommandHandler]: ...
    def on_command(
        self, name: str, handler: CommandHandler | None = None
    ) -> Callable[[CommandHandler], CommandHandler] | None:
        """Register a custom command, in the same two forms."""

        def register(n: str, h: CommandHandler | None) -> CommandHandler | None:
            if n in STANDARD_COMMANDS:
                raise ValueError(f'agent: "{n}" is a built-in command')
            if n in self._commands:
                raise ValueError(f'agent: command "{n}" is already registered')
            if h is not None and not inspect.iscoroutinefunction(h):
                raise TypeError(f'agent: command "{n}" needs an async def handler')
            self._commands[n] = cast(CommandHandler, h)
            return h

        if handler is None:

            def decorator(h: CommandHandler) -> CommandHandler:
                register(name, h)
                return h

            return decorator
        register(name, handler)
        return None

    def on_event(self, handler: Callable[[ActioEvent], None]) -> None:
        """Subscribe to the observability stream. Callbacks run synchronously on the loop and must
        return quickly; one that raises produces a single `LogEvent`."""
        self._listeners.append(handler)

    # --- the lifecycle (D-9) ------------------------------------------------------

    async def start(self) -> None:
        """Folders, spec, status, diarium, inbox, announce, watchers, the first scan (steps 1-5)."""
        if self._running():
            raise RuntimeError(f'agent "{self._id}": start() while already started')
        task = self._lifecycle(self._start_impl(), "actio.start")
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            # The caller went: abort the start so nothing stays armed, then re-raise.
            task.cancel()
            with contextlib.suppress(BaseException):
                await task
            raise

    async def stop(self, *, force: bool = False) -> None:
        """D-9's order; after it nothing of ours is armed: no task, timer, watcher or thread."""
        await asyncio.shield(self._lifecycle(self._stop_impl(force), "actio.stop"))

    async def wait_stopped(self) -> None:
        """Returns when the agent has stopped for good. A `restart` does not end it (D-10)."""
        await self._stopped_for_good.wait()

    async def __aenter__(self) -> ActioAgent:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    def _running(self) -> bool:
        run = self._run
        return run is not None and not run.stopped.is_set()

    def _lifecycle(self, coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
        """A lifecycle step (D-9), one at a time: this step waits for the one before it, then does
        its work; a step that failed does not fail the next, which only waited for its completion."""
        prev = self._lifecycle_task

        async def chained() -> None:
            if prev is not None:
                with contextlib.suppress(BaseException):
                    await asyncio.shield(prev)
            await coro

        self._lifecycle_task = self._spawn(chained(), name)
        return self._lifecycle_task

    async def _start_impl(self) -> None:
        # Steps 1-5 in the protocol's order: folders, spec, status, diarium, inbox, announce,
        # then the watchers and the first scan. A failure drops what it armed (D-9).
        config = self._config_option if self._config_option is not None else load_config()
        folders = AgentFolders(
            root=config.root,
            agent=os.path.join(config.root, self._id),  # D-1: the folder name is the id
            inbox=os.path.join(config.root, self._id, "inbox"),
            forum=os.path.join(config.root, "forum"),
            nuntium=os.path.join(config.root, "nuntium"),
        )
        self._folders = folders
        # One provider per process by documented choice; a failed flush reaches our log.
        sync = create_sync_provider(config.sync, spawn=self._spawn, on_error=self._on_sync_error)
        run = _Run(
            stopped=asyncio.Event(),
            config=config,
            sync=sync,
            publisher=StatusPublisher(folders, sync, self._spawn),
        )
        # The run is current from the moment it exists: the first discovery of this start must be
        # able to store its view, and a start that fails below leaves this stopped run in place,
        # so a stale task of it reads `stopped` and no-ops (D-9).
        self._run = run
        try:
            for folder in (
                config.root,
                folders.nuntium,
                folders.forum,
                folders.agent,
                folders.inbox,
            ):
                await _make_dirs(folder)
            self._enabled_state = None  # D-10: a `disabled` set by `enable` does not survive
            self._build_spec_now()  # a bad registration fails here, before anything lands in the tree
            await run.publisher.publish_spec(self._spec)
            await self._set_status(run, "idle")  # status.json (step 3) plus its diarium event
            await self._reannounce_after_delay(
                run, f"agent {self._id} is active"
            )  # then the announce
            # An alias so the file-call grep (which catches a bare `open(`) sees the watcher's
            # constructor for what it is: not a file call.
            open_watcher = Watcher.open
            watcher = await open_watcher(self._spawn, audit_seconds=AUDIT_SECONDS)
            run.watcher = watcher
            ctx = _Ctx(self, run)
            run.receiver = Receiver(ctx, _ReceiverHooks(self), watcher, self._spawn)
            run.requestor = Requestor(ctx, _RequestorHooks(self), watcher, self._spawn)
            # Nuntium (D-17): every event schedules one debounced full scan; the first discovery of
            # the workflow (step 4) runs now, not after the debounce.
            run.nuntium = watcher.subscribe(folders.nuntium)
            run.nuntium_task = self._spawn(self._nuntium_loop(run), "actio.nuntium")
            await self._rescan(run)
            run.heartbeat = self._spawn(
                heartbeat(config.heartbeat_interval, lambda: self._beat(run), run.stopped),
                "actio.heartbeat",
            )
            # LAST step, on purpose: the inbox watcher's initial scan delivers the pending
            # envelopes (D-4); a pending `stop`/`restart` command now runs with everything armed.
            await run.receiver.start()
        except BaseException:
            await self._abort_start(run)
            raise

    async def _abort_start(self, run: _Run) -> None:
        """Drops what a failed `start()` armed: no heartbeat, no nuntium loop, no watchers; `inactive`
        is published if `idle` was, so a half-started agent never looks alive (D-9)."""
        run.stopped.set()
        for task in (run.heartbeat, run.nuntium_task, run.scan_task):
            if task is not None:
                task.cancel()
        for task in (run.heartbeat, run.nuntium_task, run.scan_task):
            if task is not None:
                with contextlib.suppress(BaseException):
                    await asyncio.shield(task)
        if run.receiver is not None:
            with contextlib.suppress(BaseException):
                await run.receiver.stop()  # also closes the shared watcher
        elif run.watcher is not None:
            with contextlib.suppress(BaseException):
                await run.watcher.close()
        if self._state in ("idle", "busy"):
            with contextlib.suppress(Exception):
                await self._set_status(run, "inactive")
        with contextlib.suppress(Exception):
            await run.sync.flush()
        run.sync.close()
        self._view.clear()
        self._discovered.clear()

    async def _stop_impl(self, force: bool) -> None:
        # D-9, in order; after it nothing of ours is armed.
        run = self._run
        restart = self._restart_task
        if restart is not None and not self._stopping_restart:
            # D-10: a `stop` during the delay cancels the restart and ends `wait_stopped()`. This comes
            # before the "already stopped" return below, which is about the *run*: during the delay the
            # restart's own `stop()` has run, so its run is stopped and returning first would leave the
            # restart sleeping and the agent coming back to life behind the caller's back.
            restart.cancel()
            if (
                self._lifecycle_task is not None
                and self._lifecycle_task is not asyncio.current_task()
            ):
                self._lifecycle_task.cancel()  # the restart's in-flight start goes with it
            with contextlib.suppress(BaseException):
                await asyncio.shield(restart)  # its handler ends `wait_stopped()` on the way out
            self._restart_task = None
        if run is None or run.stopped.is_set():
            return  # not running, or a second stop: it only waited for the first
        # (1) The heartbeat and any pending discovery scan.
        run.stopped.set()
        if run.scan_task is not None:
            run.scan_task.cancel()
            with contextlib.suppress(BaseException):
                await asyncio.shield(run.scan_task)
            run.scan_task = None
        # (2) The grace wait (D-9: only in-flight WORK). The requestor is not stopped yet: a handler
        # may still issue a request and receive its response inside the grace. `force` skips the
        # wait, it does not reorder the steps.
        if not force:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + run.config.stop_grace
            while self._inbound_running(run) > 0 and loop.time() < deadline:
                await asyncio.sleep(GRACE_STEP)
        # (3) The receiver: the accepted and unanswered get code 300, the answered are archived.
        if run.receiver is not None:
            await run.receiver.stop(force=force)
        # (4) Last, the outbound side: what is in B2 is abandoned, and every owed `outreq` line is on
        # disk before C1 (D-9).
        if run.requestor is not None:
            await run.requestor.stop(force=force)
        # (5) The heartbeat: a slow tick must not land after `inactive`, and none may follow.
        if run.heartbeat is not None:
            with contextlib.suppress(BaseException):
                await asyncio.shield(run.heartbeat)
            run.heartbeat = None
        # (6) The nuntium loop; the watcher was closed by the receiver's stop.
        if run.nuntium_task is not None:
            run.nuntium_task.cancel()
            with contextlib.suppress(BaseException):
                await asyncio.shield(run.nuntium_task)
            run.nuntium_task = None
        # C1: the status first, then the announce, delayed. The publisher's latch keeps every late
        # status write from outwriting the `inactive` (D-9).
        await self._set_status(run, "inactive")
        await self._reannounce_after_delay(run, f"agent {self._id} is inactive")
        with contextlib.suppress(Exception):
            await run.sync.flush()  # the last writes must reach the shared tree
        # C2: deinit. The in-memory view goes with the run; the provider is closed.
        self._view.clear()
        self._discovered.clear()
        run.sync.close()
        if self._restart_task is None:
            self._stopped_for_good.set()  # a restart is in flight: it decides

    # --- discovery (step 4, D-17) ---------------------------------------------------

    def agmen(self) -> Mapping[AgentId, AgentView]:
        """What the callability rules leave usable, read-only, keyed by agent id."""
        return self._view

    async def refresh(self) -> Mapping[AgentId, AgentView]:
        """A full discovery scan now, and the new view: the same mapping `agmen()` then returns."""
        run = self._run
        if run is None or run.stopped.is_set():
            return self._view  # not running: the current view, no scan
        await self._rescan(run)
        return self._view

    def _schedule_scan(self, run: _Run) -> None:
        """One debounced full scan per burst of nuntium events (D-17)."""
        if run.scan_task is not None and not run.scan_task.done():
            return  # the pending scan covers the burst
        run.scan_task = self._spawn(self._debounced_scan(run), "actio.scan")

    async def _debounced_scan(self, run: _Run) -> None:
        await asyncio.sleep(run.config.nuntium_debounce)
        if run is not self._run or run.stopped.is_set():
            return  # a stale scan: the agent moved on
        await self._rescan(run)

    async def _nuntium_loop(self, run: _Run) -> None:
        """Any change of `nuntium` schedules one debounced full scan, until `stop` (D-17)."""
        sub = run.nuntium
        assert sub is not None
        nuntium = os.path.join(run.config.root, "nuntium")
        while not run.stopped.is_set():
            sub.changed.clear()
            if sub.lost is not None:
                # D-22: a lost `nuntium` watch is fatal: log it and enter `error`.
                error, sub.lost = sub.lost, None
                self._spawn(self._fail_watch(nuntium, error), "actio.fail")
            else:
                self._schedule_scan(run)
            await sub.changed.wait()
        if run.watcher is not None:
            run.watcher.unsubscribe(sub)

    async def _rescan(self, run: _Run) -> None:
        """Discovery (step 4): `scan` directly, so `duplicates` and `ignored` become events (D-3)."""
        try:
            found = await _tree_scan(run.config.root)
        except Exception as err:
            self._emit(LogEvent(message=f"discovery scan failed: {err}"))
            return
        view: dict[AgentId, AgentView] = {}
        specs: dict[AgentId, Spec] = {}
        now = self._now()
        for peer in found.agents:
            specs[peer.spec["id"]] = peer.spec  # unfiltered: D-7's code 106 must see it
            agent_view = _peer_view(self._spec, peer, now)  # our own folder is in: self-calls
            if agent_view is not None:
                view[agent_view.id] = agent_view
        if run is not self._run:
            return  # the agent moved on: a stale scan replaces nothing
        self._view = view
        self._discovered = specs
        for folder in found.duplicates:
            self._emit(LogEvent(message=f'duplicate agent id: folder "{folder}" was skipped (D-3)'))
        for entry in found.ignored:
            self._emit(IgnoredEvent(path=entry.path, reason=entry.reason))

    # --- status and announce (*Agent's status*) --------------------------------------

    def _now(self) -> float:
        run = self._run
        return run.config.clock() if run is not None else time.time()

    def _build_spec_now(self) -> None:
        """`spec.json` from the options and the registered capabilities, with a fresh `datetime`."""
        self._spec = build_spec(
            id=self._id,
            description=self._description,
            accepts_commands_from=self._accepts_commands_from,
            capabilities=[entry.cap for entry in self._capabilities.values()],
            now=self._now(),
            namespaces=self._namespaces,
            depends=self._depends,
            wants=self._wants,
            info=self._info,
        )
        # D-10: the enabled state is agent state, not option state, so EVERY rebuild during a run
        # (the `ping` command above all) keeps it. `build_spec` never writes `disabled`.
        if self._enabled_state is not None:
            self._spec["disabled"] = self._enabled_state

    def _capability_status(self, run: _Run) -> list[CapabilityStatusEntry]:
        receiver = run.receiver
        if receiver is None:
            return []
        return [
            {"name": entry.cap["name"], "running": entry.running, "queue": 0}
            for entry in receiver._entries.values()
        ]

    def _status_for(self, run: _Run, state: StatusState) -> Status:
        return build_status(self._id, state, self._now(), self._capability_status(run))

    def _inbound_running(self, run: _Run) -> int:
        receiver = run.receiver
        if receiver is None:
            return 0
        return sum(entry.running for entry in receiver._entries.values())

    async def _set_status(self, run: _Run, state: StatusState) -> None:
        """Publishes `status.json` and appends its diarium event; a no-op when the state did not
        change. A failed publish keeps the state as chosen: the next attempt publishes it again."""
        if state == self._state:
            return
        self._state = state  # before the first await: concurrent hooks must not publish it twice
        try:
            await run.publisher.publish_status(self._status_for(run, state))
            await append(
                cast(AgentFolders, self._folders).agent,
                status_event(self._id, self._now(), state),
                run.sync,
            )
        except Exception as err:
            self._emit(LogEvent(message=f'status "{state}": {err}'))
            return
        self._emit(StatusEvent(state=state))

    async def _beat(self, run: _Run) -> None:
        """The heartbeat tick (rule 3): the same state, a fresh `datetime`, live `capability_status`."""
        try:
            await run.publisher.publish_status(self._status_for(run, self._state))
        except Exception as err:
            self._emit(LogEvent(message=f"heartbeat: {err}"))

    async def _reannounce_after_delay(self, run: _Run, text: str) -> None:
        """The announce file after the status write, `announce_delay` later (rule 2), and its event."""
        await asyncio.sleep(run.config.announce_delay)
        try:
            await run.publisher.announce(self._id, text, self._now())
        except Exception as err:
            self._emit(LogEvent(message=f"announce: {err}"))
            return
        self._emit(AnnounceEvent(text=text))

    # --- lane B: the public call API ---------------------------------------------------

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
        """Lane B. Raises `ActioCallError` when the callability rules refuse it locally, before writing.

        `timeout == 0` means wait until the answer or the cancel; the caller's own task being cancelled
        still ends the exchange correctly in the background.
        """
        view = self._view.get(dst)
        if view is None:
            raise ActioCallError(dst, name, "peer is not callable (discovery/callability rules)")
        if name not in view.capabilities:
            raise ActioCallError(dst, name, "capability is not usable (disabled or accepts_from)")
        run = self._run
        if run is None or run.requestor is None:
            raise ActioCallError(dst, name, "the agent is not started")
        outcome = await run.requestor.request(
            dst, name, params, timeout=timeout, attachments=attachments, cancel=cancel
        )
        self._check_result(dst, name, view, outcome.response)
        return outcome

    async def command(self, dst: AgentId, name: str, params: Any = None) -> Rid:
        """A command to a peer that accepts commands from us (rules 5.1 and 11)."""
        view = self._view.get(dst)
        if view is None:
            raise ActioCallError(dst, name, "peer is not callable (discovery/callability rules)")
        if not view.commands:
            raise ActioCallError(dst, name, "peer does not accept our commands")
        run = self._run
        if run is None or run.requestor is None:
            raise ActioCallError(dst, name, "the agent is not started")
        return await run.requestor.command(dst, name, params)

    def _check_result(
        self, dst: AgentId, name: str, view: AgentView, response: ResponseBody | None
    ) -> None:
        """D-12: a non-error response whose `result` breaks the peer's `result` schema ends as an
        `ActioResultError` here; an error result is the peer's answer, not our bug."""
        if response is None or response.get("error") is not False:
            return
        if response.get("cancellation") is True:
            return  # a cancellation response carries `result: null` by protocol
        compiled = self._peer_schema(dst, name, view)
        if compiled is None:
            return  # D-19: the checks are skipped when the schema does not compile
        if compiled.result(response.get("result")) is not None:
            raise ActioResultError(
                f"agent: {dst}/{name} returned a result that does not conform to its schema",
                response,
            )

    def _peer_schema(self, dst: AgentId, name: str, view: AgentView) -> CapabilityValidator | None:
        """D-19 (peer side): lazy, cached per (id, spec datetime, capability); None = uncompilable."""
        capability = view.capabilities.get(name)
        if capability is None:
            return None
        key = f"{dst}:{view.spec['datetime']}:{name}"
        if key not in self._peer_schemas:
            try:
                self._peer_schemas[key] = compile_capability(capability)
            except Exception as err:
                self._peer_schemas[key] = None  # the cache keeps the null; the checks are skipped
                self._emit(LogEvent(message=f"peer capability {dst}/{name}: {err}; checks skipped"))
        return self._peer_schemas[key]

    # --- the built-in commands (lane D, D2/D3) ------------------------------------------

    def _schedule_restart(self, force: bool, delay_seconds: float) -> None:
        # The command ran in the receiver's drain, outside what the teardown cancels; the restart
        # owns the rest of the lifecycle in its own task (D-9, D-10).
        self._restart_task = self._spawn(self._restart(force, delay_seconds), "actio.restart")

    async def _restart(self, force: bool, delay_seconds: float) -> None:
        """D-10: the receiver has archived the command already; stop, wait the `delay`, start fresh.
        A `stop` during the delay cancels the restart and ends `wait_stopped()`; a `start` that fails
        does the same, because nobody is left to bring the agent back."""
        self._stopped_for_good.clear()
        try:
            self._stopping_restart = True
            await self.stop(force=force)
            self._stopping_restart = False
            await asyncio.sleep(min(max(0.0, delay_seconds), RESTART_DELAY_MAX))
            await self.start()
        except BaseException:
            self._stopped_for_good.set()
            raise
        finally:
            self._stopping_restart = False
            if self._restart_task is asyncio.current_task():
                self._restart_task = None

    async def _apply_enable(self, enabled: bool) -> None:
        """`enable` (D2): `disabled` belongs to the built spec object, never to the options."""
        run = self._run
        if run is None or run.stopped.is_set():
            return
        self._enabled_state = not enabled  # D-10: state of the run, not of the options
        self._build_spec_now()
        await run.publisher.publish_spec(self._spec)
        await self._reannounce_after_delay(
            run, f"agent {self._id} is {'enabled' if enabled else 'disabled'}"
        )

    async def _apply_ping(self) -> None:
        """`ping` (D2): fresh `datetime` in spec and status, then re-announce."""
        run = self._run
        if run is None or run.stopped.is_set():
            return
        self._build_spec_now()
        await run.publisher.publish_spec(self._spec)
        await run.publisher.publish_status(self._status_for(run, self._state))
        await self._reannounce_after_delay(run, f"agent {self._id} is alive")

    # --- the rest of the public API -------------------------------------------------------

    async def fail(self, message: str) -> None:
        """Status `error` plus an announce; there is no recovery on its own."""
        run = self._run
        if run is None or run.stopped.is_set() or self._state == "error":
            return  # already failed (or not running): no second announce, no recovery
        await self._set_status(run, "error")
        await self._reannounce_after_delay(run, f"agent {self._id} failed: {message}")

    async def diarium(self) -> DiariumRead:
        """This agent's own diarium: the valid lines and the ignored ones with a reason."""
        if self._folders is None:
            raise RuntimeError("the agent has never started")
        return await read_diarium(self._folders.agent)

    async def log(self, message: str) -> None:
        """A diarium `log` event."""
        run = self._run
        if run is None:
            raise RuntimeError("the agent has never started")
        await append(
            cast(AgentFolders, self._folders).agent,
            log_event(self._id, self._now(), message),
            run.sync,
        )

    def _spawn(self, coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
        """The one task helper, of the shape `Spawn` in `_types.py`: it keeps the strong reference and
        reports a crash as a `LogEvent`. Everything long-lived comes from here."""
        task = asyncio.get_running_loop().create_task(coro, name=name)  # _spawn
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(self._report_crash)
        return task

    def _report_crash(self, task: asyncio.Task[Any]) -> None:
        # Reading `exception()` here also marks it retrieved: a fire-and-forget crash is one log line.
        if not task.cancelled() and (err := task.exception()) is not None:
            self._emit(LogEvent(message=f"{task.get_name()}: {err}"))

    # --- the observability stream ----------------------------------------------------------

    def _emit(self, event: ActioEvent) -> None:
        """The fan-out; a listener's error is the listener's problem, reported once and never again:
        a callback that always raises must not send a `RecursionError` into protocol code."""
        for listener in list(self._listeners):
            try:
                listener(event)
            except Exception as err:
                try:
                    listener(LogEvent(message=f"event listener failed: {err}"))
                except Exception:
                    pass  # a failure while delivering the report is dropped, not reported again

    def _on_sync_error(self, err: Exception) -> None:
        self._emit(LogEvent(message=f"sync: {err}"))

    async def _fail_watch(self, folder: str, error: BaseException) -> None:
        self._emit(LogEvent(message=f"watch lost {folder}: {error}"))
        await self.fail(f"watch lost {folder}")

    def _schedule_busy(self) -> None:
        """D-8: a state change re-evaluates busy; the check runs as its own task."""
        run = self._run
        if run is None or run.stopped.is_set() or run.requestor is None:
            return
        self._spawn(self._sync_busy(run), "actio.busy")

    async def _sync_busy(self, run: _Run) -> None:
        """D-8: busy iff an inbound exchange (A1/A2/A3) or an outbound one (B2) is in flight."""
        if run is not self._run or run.stopped.is_set():
            return
        if self._state not in ("idle", "busy"):
            return  # inactive/error: no automatic transition
        requestor = run.requestor
        assert requestor is not None  # checked by the caller, `_schedule_busy`
        busy = self._inbound_running(run) > 0 or len(requestor._exchanges) > 0
        target: StatusState = "busy" if busy else "idle"
        if target != self._state:
            await self._set_status(run, target)
