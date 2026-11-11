"""`_receiver.py`: lane A (inbound requests, A0 through A4) and lane D (commands), one receiver.

A fake context (fake clock, in-memory events, a real diarium in a temporary tree) feeds a real
`Receiver` on a real `Watcher`. Timeouts are generous on purpose: watchdog timing varies under
load, and the suite runs eight times at once at each milestone, where a flake is a failure.

The late-cancellation expectations here pin the brief's rule (a late `can` leaves `can: null` and
the status follows the confirmation); see `orchestra/oracle/Q1.md` for the conflict with the
reference and the protocol.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast

from actio._config import load_config
from actio._diarium import append, read_diarium
from actio._protocol import DiariumEntry, ExchangeRecord, Spec, StatusState
from actio._receiver import Receiver
from actio._sync import create_sync_provider
from actio._types import (
    ActioEvent,
    AgentFolders,
    CapabilityHandler,
    IgnoredEvent,
    InreqEvent,
    LogEvent,
)
from actio._watch import Watcher
from helpers import ActioTestCase, actio_tree

TS_ISO = "2026-10-24T10:00:00Z"
TS_INT = 1_792_836_000
NONCE = "52ffe97b3a5e6e2f00a69edb27551465"
SETTLE = 4.0  # watchdog wake + handler + the (shrunken) confirmation timeout


class Clock:
    """The fake clock a test ticks; the receiver sees it through `ctx.now()`."""

    def __init__(self, start: float = TS_INT) -> None:
        self._now = start

    def value(self) -> float:
        return self._now

    def tick(self, seconds: float) -> None:
        self._now += seconds


def rid(n: int) -> str:
    """A deterministic valid ULID (Crockford base32, 26 chars, first digit 0-7)."""
    return "01JB7Q3W8K2M4N6P8R0S2T4V" + f"{n % 256:02X}"


def header(
    r: str,
    src: str = "planner",
    dst: str = "summarizer",
    type_: str = "req",
    version: str = "0.1",
) -> dict[str, Any]:
    return {
        "rid": r,
        "src": src,
        "dst": dst,
        "datetime": TS_ISO,
        "type": type_,
        "actio_version": version,
    }


def request_file(
    r: str,
    name: str = "summarize",
    parameters: dict[str, Any] | None = None,
    timeout: int = 300,
    ts: int = TS_INT,
    attachments: list[str] | None = None,
    **kw: Any,
) -> dict[str, Any]:
    return {
        **header(r, **kw),
        "request": {
            "ts": ts,
            "nonce": NONCE,
            "signature": None,
            "name": name,
            "parameters": parameters if parameters is not None else {"text": "a text to summarize"},
            "timeout": timeout,
            "attachments": attachments or [],
        },
    }


def cmd_file(
    r: str,
    name: str = "ping",
    parameters: dict[str, Any] | None = None,
    **kw: Any,
) -> dict[str, Any]:
    return {
        **header(r, type_="cmd", **kw),
        "request": {
            "ts": TS_INT,
            "nonce": NONCE,
            "signature": None,
            "name": name,
            "parameters": parameters,
            "timeout": 0,
            "attachments": [],
        },
    }


def cancellation_file(
    r: str, name: str = "summarize", force: bool = False, **kw: Any
) -> dict[str, Any]:
    return {
        **header(r, **kw),
        "cancellation": {
            "ts": TS_INT,
            "nonce": NONCE,
            "signature": None,
            "name": name,
            "force": force,
        },
    }


def confirmation_file(r: str, **kw: Any) -> dict[str, Any]:
    return {**header(r, **kw), "confirmation": {"ts": TS_INT, "nonce": NONCE, "signature": None}}


def response_file(
    r: str,
    result: Any = "short",
    error: bool = False,
    cancellation: bool = False,
    **kw: Any,
) -> dict[str, Any]:
    return {
        **header(r, **kw),
        "response": {
            "ts": TS_INT,
            "nonce": NONCE,
            "signature": None,
            "error": error,
            "cancellation": cancellation,
            "result": None if cancellation else result,
            "attachments": [],
            "work_time": 1,
        },
    }


def summarize_cap(**actio: Any) -> dict[str, Any]:
    return {
        "name": "summarize",
        "description": "Summarizes a text",
        "parameters": {
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
        },
        "actio": {"result": {"type": "string"}, **actio},
    }


def make_spec(
    *,
    capabilities: list[dict[str, Any]] | None = None,
    accepts_commands_from: list[str] | None = None,
    disabled: bool = False,
    namespaces: list[str] | None = None,
) -> Spec:
    spec: dict[str, Any] = {
        "id": "summarizer",
        "actio_version": "0.1",
        "datetime": TS_ISO,
        "description": "Summarizes texts",
        "accepts_commands_from": (
            accepts_commands_from if accepts_commands_from is not None else ["planner"]
        ),
        "capabilities": [summarize_cap()] if capabilities is None else capabilities,
        "security_support": [],
        "security_posture": "trusted",
    }
    if disabled:
        spec["disabled"] = True
    if namespaces is not None:
        spec["namespaces"] = namespaces
    return cast(Spec, spec)


class FakeHooks:
    """The `ReceiverHooks` side of the seam: records, never acts (the agent's work is the test's)."""

    def __init__(self, peers: dict[str, Spec] | None = None) -> None:
        self.peers = peers or {}
        self.watch_errors: list[tuple[str, BaseException]] = []
        self.state_changes = 0
        self.commands: list[tuple[Any, ...]] = []
        self.before_command: Callable[[str], Awaitable[None]] | None = None

    def on_watch_error(self, folder: str, error: BaseException) -> None:
        self.watch_errors.append((folder, error))

    def peer_spec(self, src: str) -> Spec | None:
        return self.peers.get(src)

    def on_state_change(self) -> None:
        self.state_changes += 1

    async def _before(self, name: str) -> None:
        if self.before_command is not None:
            await self.before_command(name)

    async def on_command_stop(self, force: bool) -> None:
        await self._before("stop")
        self.commands.append(("stop", force))

    async def on_command_restart(self, force: bool, delay_seconds: float) -> None:
        await self._before("restart")
        self.commands.append(("restart", force, delay_seconds))

    async def on_command_enable(self, enabled: bool) -> None:
        await self._before("enable")
        self.commands.append(("enable", enabled))

    async def on_command_ping(self) -> None:
        await self._before("ping")
        self.commands.append(("ping",))


class FakeContext:
    """A private context object of the kind `ActioAgent` would hand its receiver (not a Protocol)."""

    def __init__(
        self,
        root: Path,
        clock: Clock,
        spec: Spec,
        handlers: dict[str, CapabilityHandler],
        *,
        confirm_timeout: float = 0.3,
    ) -> None:
        agent = root / "summarizer"
        self.config = load_config(
            env={}, root=str(root), confirm_timeout=confirm_timeout, clock=clock.value
        )
        self.id = "summarizer"
        self.folders = AgentFolders(
            root=str(root),
            agent=str(agent),
            inbox=str(agent / "inbox"),
            forum=str(agent / "forum"),
            nuntium=str(agent / "nuntium"),
        )
        self.sync = create_sync_provider("none")
        self._clock = clock
        self._spec = spec
        self._handlers = handlers
        self.events: list[ActioEvent] = []

    def now(self) -> float:
        return self._clock.value()

    def spec(self) -> Spec:
        return self._spec

    def state(self) -> StatusState:
        return "idle"

    def emit(self, event: ActioEvent) -> None:
        self.events.append(event)

    async def append_diarium(self, entry: DiariumEntry) -> None:
        await append(self.folders.agent, entry, self.sync)

    def capability_handler(self, name: str) -> CapabilityHandler | None:
        return self._handlers.get(name)


def spawn(coro: Any, name: str) -> asyncio.Task[Any]:
    return asyncio.get_running_loop().create_task(coro, name=name)


async def wait_for(what: str, predicate: Callable[[], bool], timeout: float = SETTLE) -> None:
    """Poll until `predicate` is true; the failure names what was waited for."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError(f"timed out waiting for: {what}")
        await asyncio.sleep(0.02)


async def wait_until(
    what: str, check: Callable[[], Awaitable[bool]], timeout: float = SETTLE
) -> None:
    """The async twin of `wait_for`, for checks that read the diarium."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not await check():
        if loop.time() > deadline:
            raise AssertionError(f"timed out waiting for: {what}")
        await asyncio.sleep(0.02)


def inbox(root: Path, agent: str = "summarizer") -> Path:
    return root / agent / "inbox"


def agent_dir(root: Path, agent: str = "summarizer") -> Path:
    return root / agent


def _record_rid(data: dict[str, Any]) -> str | None:
    """The rid of an archived record: any of its four envelopes carries it (the retrack's `req` is null)."""
    for key in ("req", "can", "res", "con"):
        env = data.get(key)
        if isinstance(env, dict) and env.get("rid"):
            return str(env["rid"])
    return None


async def inreq_for(root: Path, r: str) -> ExchangeRecord | None:
    """The archived `inreq` record of `r` (the diarium is the receiver's own write)."""
    read = await read_diarium(str(agent_dir(root)))
    for line in read.lines:
        if line["event"] != "inreq":
            continue
        data = line["event_data"]
        if isinstance(data, dict) and _record_rid(data) == r:
            return cast(ExchangeRecord, data)
    return None


async def ignored_lines(root: Path) -> list[str]:
    read = await read_diarium(str(agent_dir(root)))
    return [
        line["event_data"]
        for line in read.lines
        if line["event"] == "log" and isinstance(line["event_data"], str)
    ]


def archived_check(root: Path, r: str) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        return (await inreq_for(root, r)) is not None

    return check


def ignore_line_check(root: Path, needle: str) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        return any(needle in line for line in (await ignored_lines(root)))

    return check


def ignore_count_check(root: Path, needle: str, count: int) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        lines = [line for line in (await ignored_lines(root)) if needle in line]
        return len(lines) == count

    return check


def write_files_sync(root: Path, **by_name: dict[str, dict[str, Any]]) -> None:
    """Write several envelopes with no loop tick between them: one watchdog wake, one scan."""
    for name, value in by_name.items():
        (inbox(root) / name).write_text(json.dumps(value), encoding="utf-8")


def running(root: Path, r: str) -> bool:
    """The request file is still there and no response has landed yet."""
    return (inbox(root) / f"{r}.json").exists() and not (inbox(root) / f"{r}.res.json").exists()


def res_body(record: ExchangeRecord) -> dict[str, Any]:
    """The response body of an archived record, narrowed (the schema allows a base64 string)."""
    res = record["res"]
    assert res is not None
    body = res["response"]
    assert isinstance(body, dict)
    return cast(dict[str, Any], body)


async def quick_handler(parameters: Any, ctx: Any) -> str:
    return "short"


async def slow_handler(parameters: Any, ctx: Any) -> str:
    await asyncio.sleep(0.4)
    return "short"


async def settle_tasks(receiver: Receiver) -> None:
    """Join the receiver's exchange tasks (a handler can outlive its exchange) before `stop` runs;
    the loop task is the exception: `stop` cancels it."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10.0
    while True:
        live = [t for t in receiver._tasks if t.get_name() != "actio.receiver" and not t.done()]
        if not live or loop.time() > deadline:
            return
        await asyncio.sleep(0.02)


class ReceiverTest(ActioTestCase):
    """Boots a receiver for `summarizer` on a fresh tree; every test stops it before it ends."""

    async def boot(
        self,
        root: Path,
        *,
        spec: Spec | None = None,
        handlers: dict[str, CapabilityHandler] | None = None,
        peers: dict[str, Spec] | None = None,
        confirm_timeout: float = 0.3,
    ) -> tuple[FakeContext, FakeHooks, Receiver]:
        for sub in ("inbox", "forum", "nuntium"):
            (agent_dir(root) / sub).mkdir(parents=True, exist_ok=True)
        clock = Clock()
        ctx = FakeContext(
            root,
            clock,
            spec if spec is not None else make_spec(),
            handlers if handlers is not None else {"summarize": quick_handler},
            confirm_timeout=confirm_timeout,
        )
        hooks = FakeHooks(peers)
        watcher = await Watcher.open(spawn)
        self.addAsyncCleanup(watcher.close)
        receiver = Receiver(ctx, hooks, watcher)
        self.addAsyncCleanup(receiver.stop)
        self.addAsyncCleanup(settle_tasks, receiver)
        await receiver.start()
        return ctx, hooks, receiver

    async def files(self, root: Path, **by_name: dict[str, dict[str, Any]]) -> None:
        write_files_sync(root, **by_name)

    def res_path(self, root: Path, r: str) -> Path:
        return inbox(root) / f"{r}.res.json"

    async def read_res(self, root: Path, r: str) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(self.res_path(root, r).read_text(encoding="utf-8")))


class LaneATest(ReceiverTest):
    async def test_a_valid_request_is_served_and_archived(self) -> None:
        seen: list[tuple[Any, Any]] = []

        async def handler(parameters: Any, ctx: Any) -> str:
            seen.append((parameters, ctx))
            return "short"

        with actio_tree() as root:
            ctx, _, _ = await self.boot(root, handlers={"summarize": handler})
            r = rid(1)
            await self.files(root, **{f"{r}.json": request_file(r)})
            await wait_for("the response file", lambda: self.res_path(root, r).exists())
            res = await self.read_res(root, r)
            self.assertEqual(res["response"]["result"], "short")
            self.assertFalse(res["response"]["error"])
            self.assertFalse(res["response"]["cancellation"])
            self.assertIsInstance(res["response"]["work_time"], int)
            for key in ("rid", "src", "dst", "type", "actio_version"):
                self.assertEqual(res[key], request_file(r)[key])
            params, hctx = seen[0]
            self.assertEqual(params, {"text": "a text to summarize"})
            self.assertEqual(hctx.rid, r)
            self.assertEqual(hctx.src, "planner")
            self.assertFalse(hctx.cancelled.is_set())
            await wait_until("the archive", archived_check(root, r))
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(record["final_status"], "completed")
            self.assertIsNone(record["can"])
            self.assertIsNone(record["con"])
            self.assertEqual(res_body(record)["result"], "short")
            self.assertEqual(list(inbox(root).iterdir()), [])
            inreqs = [e for e in ctx.events if isinstance(e, InreqEvent)]
            self.assertEqual([(e.rid, e.final_status) for e in inreqs], [(r, "completed")])

    async def test_the_handler_publishes_an_attachment_that_the_archive_deletes(self) -> None:
        async def handler(parameters: Any, ctx: Any) -> str:
            await ctx.publish_attachment("notes/out.txt", b"the summary")
            return "short"

        with actio_tree() as root:
            await self.boot(root, handlers={"summarize": handler})
            r = rid(2)
            await self.files(root, **{f"{r}.json": request_file(r)})
            forum_file = agent_dir(root) / "forum" / "notes" / "out.txt"
            await wait_for("the attachment in the forum", lambda: forum_file.exists())
            self.assertEqual(forum_file.read_bytes(), b"the summary")
            await wait_until("the archive", archived_check(root, r))
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(res_body(record)["attachments"], ["notes/out.txt"])
            self.assertFalse(forum_file.exists())

    async def test_work_time_uses_the_clock_the_receiver_reads(self) -> None:
        with actio_tree() as root:
            for sub in ("inbox", "forum", "nuntium"):
                (agent_dir(root) / sub).mkdir(parents=True)
            clock = Clock()

            async def handler(parameters: Any, ctx: Any) -> str:
                await asyncio.sleep(0.05)
                clock.tick(5)
                return "short"

            ctx = FakeContext(root, clock, make_spec(), {"summarize": handler}, confirm_timeout=0.3)
            hooks = FakeHooks()
            watcher = await Watcher.open(spawn)
            self.addAsyncCleanup(watcher.close)
            receiver = Receiver(ctx, hooks, watcher)
            self.addAsyncCleanup(receiver.stop)
            await receiver.start()
            r = rid(3)
            write_files_sync(root, **{f"{r}.json": request_file(r)})
            await wait_for("the response file", lambda: self.res_path(root, r).exists())
            res = await self.read_res(root, r)
            self.assertEqual(res["response"]["work_time"], 5)


class AcceptanceTest(ReceiverTest):
    async def _expect_code(self, root: Path, file: dict[str, Any], code: int, what: str) -> None:
        r = file["rid"]
        await self.files(root, **{f"{r}.json": file})
        await wait_for(f"the {code} response for {what}", lambda: self.res_path(root, r).exists())
        res = await self.read_res(root, r)
        self.assertTrue(res["response"]["error"])
        self.assertFalse(res["response"]["cancellation"])
        self.assertEqual(res["response"]["result"]["code"], code)

    async def test_agent_disabled_is_100(self) -> None:
        with actio_tree() as root:
            await self.boot(root, spec=make_spec(disabled=True))
            await self._expect_code(root, request_file(rid(1)), 100, "disabled agent")

    async def test_unknown_capability_is_101(self) -> None:
        with actio_tree() as root:
            await self.boot(root)
            await self._expect_code(
                root, request_file(rid(2), name="translate"), 101, "unknown capability"
            )

    async def test_capability_disabled_is_102(self) -> None:
        with actio_tree() as root:
            await self.boot(root, spec=make_spec(capabilities=[summarize_cap(disabled=True)]))
            await self._expect_code(root, request_file(rid(3)), 102, "disabled capability")

    async def test_requestor_not_allowed_is_103(self) -> None:
        with actio_tree() as root:
            await self.boot(
                root, spec=make_spec(capabilities=[summarize_cap(accepts_from=["other"])])
            )
            await self._expect_code(root, request_file(rid(4)), 103, "forbidden requestor")

    async def test_invalid_parameters_is_105(self) -> None:
        with actio_tree() as root:
            await self.boot(root)
            await self._expect_code(
                root, request_file(rid(5), parameters={}), 105, "missing required parameter"
            )

    async def test_attachment_escaping_the_forum_is_105(self) -> None:
        with actio_tree() as root:
            await self.boot(root)
            await self._expect_code(
                root, request_file(rid(6), attachments=["../evil.txt"]), 105, "escaping attachment"
            )

    async def test_no_shared_namespace_is_106(self) -> None:
        with actio_tree() as root:
            peer = make_spec(namespaces=["b"])
            await self.boot(root, spec=make_spec(namespaces=["a"]), peers={"planner": peer})
            await self._expect_code(root, request_file(rid(7)), 106, "namespace mismatch")

    async def test_a_star_namespace_is_shared(self) -> None:
        with actio_tree() as root:
            peer = make_spec(namespaces=["*"])
            await self.boot(root, spec=make_spec(namespaces=["a"]), peers={"planner": peer})
            r = rid(8)
            await self.files(root, **{f"{r}.json": request_file(r)})
            await wait_for("the response", lambda: self.res_path(root, r).exists())
            res = await self.read_res(root, r)
            self.assertFalse(res["response"]["error"])

    async def test_capacity_exceeded_is_107(self) -> None:
        gate = asyncio.Event()

        async def gated(parameters: Any, ctx: Any) -> str:
            await gate.wait()
            return "released"

        with actio_tree() as root:
            _, hooks, _ = await self.boot(
                root,
                spec=make_spec(capabilities=[summarize_cap(max_parallel=1)]),
                handlers={"summarize": gated},
            )
            first, second = rid(9), rid(10)
            await self.files(root, **{f"{first}.json": request_file(first)})
            await wait_for("the first to start working", lambda: hooks.state_changes >= 1)
            await self.files(root, **{f"{second}.json": request_file(second)})
            await wait_for("the 107 response", lambda: self.res_path(root, second).exists())
            res = await self.read_res(root, second)
            self.assertEqual(res["response"]["result"]["code"], 107)
            gate.set()
            await wait_for("the first to finish", lambda: self.res_path(root, first).exists())
            res_first = await self.read_res(root, first)
            self.assertEqual(res_first["response"]["result"], "released")

    async def test_an_expired_request_is_108(self) -> None:
        with actio_tree() as root:
            ctx, _, _ = await self.boot(root)
            ctx._clock.tick(100)
            await self._expect_code(root, request_file(rid(11), timeout=1), 108, "expired request")

    async def test_a_foreign_version_fails_the_schema_not_code_104(self) -> None:
        with actio_tree() as root:
            ctx, _, _ = await self.boot(root)
            r = rid(12)
            # "9" is not "<major>.<minor>": the schema rejects it; there is no acceptance code 104.
            await self.files(root, **{f"{r}.json": request_file(r, version="9")})
            await wait_until("the ignore line", ignore_line_check(root, f"{r}.json"))
            self.assertFalse(self.res_path(root, r).exists())
            self.assertTrue((inbox(root) / f"{r}.json").exists())
            self.assertTrue(any(isinstance(e, IgnoredEvent) for e in ctx.events))

    async def test_a_dst_that_is_not_us_is_ignored(self) -> None:
        with actio_tree() as root:
            ctx, _, _ = await self.boot(root)
            r = rid(13)
            await self.files(root, **{f"{r}.json": request_file(r, dst="other")})
            await wait_until("the ignore line", ignore_line_check(root, f"{r}.json"))
            self.assertFalse(self.res_path(root, r).exists())
            self.assertTrue(any(isinstance(e, IgnoredEvent) for e in ctx.events))


class CancellationTest(ReceiverTest):
    async def test_a_cancellation_during_work_wins(self) -> None:
        async def handler(parameters: Any, ctx: Any) -> str:
            try:
                await asyncio.wait_for(ctx.cancelled.wait(), timeout=3.0)
                return "should not get here"
            except TimeoutError:
                return "done"

        with actio_tree() as root:
            await self.boot(root, handlers={"summarize": handler})
            r = rid(14)
            await self.files(root, **{f"{r}.json": request_file(r)})
            await wait_for("the work to start", lambda: running(root, r))
            await self.files(root, **{f"{r}.can.json": cancellation_file(r)})
            await wait_for("the cancellation response", lambda: self.res_path(root, r).exists())
            res = await self.read_res(root, r)
            self.assertTrue(res["response"]["cancellation"])
            self.assertIsNone(res["response"]["result"])
            await wait_until("the archive", archived_check(root, r))
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(record["final_status"], "cancelled")
            can = record["can"]
            assert can is not None
            can_body = can["cancellation"]
            assert isinstance(can_body, dict)
            self.assertEqual(can_body["name"], "summarize")

    async def test_a_late_cancellation_leaves_can_null(self) -> None:
        """The brief's Q1 rule: a `can` after the response does not become the record's `can`."""
        with actio_tree() as root:
            await self.boot(root)
            r = rid(15)
            await self.files(root, **{f"{r}.json": request_file(r)})
            await wait_for("the response file", lambda: self.res_path(root, r).exists())
            await self.files(root, **{f"{r}.can.json": cancellation_file(r)})
            await self.files(root, **{f"{r}.con.json": confirmation_file(r)})
            await wait_until("the archive", archived_check(root, r))
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(record["final_status"], "confirmed")
            self.assertIsNone(record["can"])
            self.assertIsNotNone(record["con"])

    async def test_a_confirmation_before_the_response_archives_on_the_response(self) -> None:
        with actio_tree() as root:
            await self.boot(root, handlers={"summarize": slow_handler})
            r = rid(16)
            write_files_sync(
                root,
                **{
                    f"{r}.con.json": confirmation_file(r),
                    f"{r}.json": request_file(r),
                },
            )
            await wait_until("the archive", archived_check(root, r))
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(record["final_status"], "confirmed")
            self.assertEqual(res_body(record)["result"], "short")
            self.assertIsNotNone(record["con"])
            self.assertEqual(list(inbox(root).iterdir()), [])

    async def test_a_header_mismatch_on_the_cancellation_is_ignored(self) -> None:
        with actio_tree() as root:
            ctx, _, _ = await self.boot(root, handlers={"summarize": slow_handler})
            r = rid(17)
            await self.files(root, **{f"{r}.json": request_file(r)})
            await wait_for("the work to start", lambda: running(root, r))
            await self.files(root, **{f"{r}.can.json": cancellation_file(r, src="other")})
            await wait_for("the normal response", lambda: self.res_path(root, r).exists())
            res = await self.read_res(root, r)
            self.assertFalse(res["response"]["cancellation"])
            self.assertTrue((inbox(root) / f"{r}.can.json").exists())
            self.assertTrue(
                any(
                    isinstance(e, IgnoredEvent) and "header mismatch" in e.reason
                    for e in ctx.events
                )
            )

    async def test_a_name_mismatch_on_the_cancellation_is_ignored(self) -> None:
        with actio_tree() as root:
            ctx, _, _ = await self.boot(root, handlers={"summarize": slow_handler})
            r = rid(18)
            await self.files(root, **{f"{r}.json": request_file(r)})
            await wait_for("the work to start", lambda: running(root, r))
            await self.files(root, **{f"{r}.can.json": cancellation_file(r, name="translate")})
            await wait_for("the normal response", lambda: self.res_path(root, r).exists())
            res = await self.read_res(root, r)
            self.assertFalse(res["response"]["cancellation"])
            self.assertTrue(
                any(isinstance(e, IgnoredEvent) and "name mismatch" in e.reason for e in ctx.events)
            )

    async def test_a_stray_cancellation_without_a_request_is_ignored(self) -> None:
        with actio_tree() as root:
            ctx, _, _ = await self.boot(root)
            r = rid(19)
            await self.files(root, **{f"{r}.can.json": cancellation_file(r)})
            await wait_until("the ignore line", ignore_line_check(root, f"{r}.can.json"))
            self.assertTrue((inbox(root) / f"{r}.can.json").exists())
            self.assertTrue(any(isinstance(e, IgnoredEvent) for e in ctx.events))


class IgnoreTest(ReceiverTest):
    async def test_a_malformed_file_is_ignored_and_stays(self) -> None:
        with actio_tree() as root:
            ctx, _, _ = await self.boot(root)
            (inbox(root) / "broken.json").write_text("not json", encoding="utf-8")
            await wait_until("the ignore line", ignore_line_check(root, "broken.json"))
            self.assertEqual(await _count(root, "broken.json"), 1)
            self.assertTrue((inbox(root) / "broken.json").exists())
            self.assertTrue(any(isinstance(e, IgnoredEvent) for e in ctx.events))

    async def test_a_non_object_file_is_ignored(self) -> None:
        with actio_tree() as root:
            ctx, _, _ = await self.boot(root)
            (inbox(root) / "array.json").write_text("[1, 2, 3]", encoding="utf-8")
            await wait_until("the ignore line", ignore_line_check(root, "array.json"))
            self.assertTrue(any(isinstance(e, IgnoredEvent) for e in ctx.events))

    async def test_a_base64_body_is_ignored(self) -> None:
        with actio_tree() as root:
            ctx, _, _ = await self.boot(root)
            r = rid(20)
            file = request_file(r)
            file["request"] = "aGVsbG8gd29ybGQ="
            await self.files(root, **{f"{r}.json": file})
            await wait_until("the ignore line", ignore_line_check(root, f"{r}.json"))
            self.assertFalse(self.res_path(root, r).exists())
            self.assertTrue(any(isinstance(e, IgnoredEvent) for e in ctx.events))

    async def test_a_schema_invalid_request_is_ignored_with_the_schema_reason(self) -> None:
        with actio_tree() as root:
            ctx, _, _ = await self.boot(root)
            r = rid(21)
            file = request_file(r)
            del file["request"]["name"]
            await self.files(root, **{f"{r}.json": file})
            await wait_until("the ignore line", ignore_line_check(root, f"{r}.json"))
            self.assertTrue(any(isinstance(e, IgnoredEvent) for e in ctx.events))

    async def test_one_line_per_path_and_mtime(self) -> None:
        with actio_tree() as root:
            await self.boot(root)
            path = inbox(root) / "broken.json"
            path.write_text("not json", encoding="utf-8")
            mtime = os.stat(path).st_mtime_ns
            await wait_until("the ignore line", ignore_count_check(root, "broken.json", 1))
            # The same (path, mtime) is logged once: rewrite the same bytes, restore the mtime,
            # and force a rescan with a fresh file of its own.
            path.write_text("not json", encoding="utf-8")
            os.utime(path, ns=(mtime, mtime))
            (inbox(root) / "marker.json").write_text("{}", encoding="utf-8")
            await wait_until("the rescan", ignore_line_check(root, "marker.json"))
            self.assertEqual(await _count(root, "broken.json"), 1)
            # A new mtime of the same path is a new (path, mtime): it may be logged again.
            path.write_text("still not json", encoding="utf-8")
            await wait_until(
                "the second ignore line for the new mtime",
                ignore_count_check(root, "broken.json", 2),
            )


async def _count(root: Path, needle: str) -> int:
    return len([line for line in (await ignored_lines(root)) if needle in line])


class RecoveryTest(ReceiverTest):
    async def test_a_bare_response_from_a_previous_run_is_retracked(self) -> None:
        with actio_tree() as root:
            for sub in ("inbox", "forum", "nuntium"):
                (agent_dir(root) / sub).mkdir(parents=True)
            r = rid(22)
            (inbox(root) / f"{r}.res.json").write_text(
                json.dumps(response_file(r, result="old")), encoding="utf-8"
            )
            await self.boot(root)
            await self.files(root, **{f"{r}.con.json": confirmation_file(r)})
            await wait_until("the archive", archived_check(root, r))
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(record["final_status"], "confirmed")
            self.assertEqual(res_body(record)["result"], "old")
            self.assertIsNone(record["can"])
            self.assertEqual(list(inbox(root).iterdir()), [])

    async def test_a_request_with_a_stored_response_keeps_the_stored_one(self) -> None:
        """Both files at startup: the stored response completes the state; the slow handler's
        A2 never overwrites it (the reference has the same ordering)."""

        async def slow(parameters: Any, ctx: Any) -> str:
            await asyncio.sleep(0.6)
            return "new"

        with actio_tree() as root:
            for sub in ("inbox", "forum", "nuntium"):
                (agent_dir(root) / sub).mkdir(parents=True)
            r = rid(23)
            write_files_sync(
                root,
                **{
                    f"{r}.json": request_file(r),
                    f"{r}.res.json": response_file(r, result="old"),
                },
            )
            await self.boot(root, handlers={"summarize": slow})
            await self.files(root, **{f"{r}.con.json": confirmation_file(r)})
            await wait_until("the archive", archived_check(root, r))
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(res_body(record)["result"], "old")
            self.assertEqual(record["final_status"], "confirmed")
            self.assertEqual(list(inbox(root).iterdir()), [])

    async def test_a_duplicate_rid_is_served_once(self) -> None:
        with actio_tree() as root:
            await self.boot(root)
            r = rid(24)
            await self.files(root, **{f"{r}.json": request_file(r)})
            await wait_until("the archive", archived_check(root, r))
            # The requestor retries with the same rid: it is ignored and never re-served; the
            # reference keeps the file (a next run would re-serve it, D-4), so it stays in the inbox.
            await self.files(root, **{f"{r}.json": request_file(r)})
            await asyncio.sleep(0.5)
            self.assertEqual(await _inreq_count(root, r), 1)
            self.assertFalse(self.res_path(root, r).exists())
            self.assertEqual(list(inbox(root).iterdir()), [inbox(root) / f"{r}.json"])

    async def test_a_late_cancellation_on_a_retracked_exchange_leaves_can_null(self) -> None:
        """Q1, retrack side: a stored non-cancellation response plus a late `can` keeps `can: null`."""
        with actio_tree() as root:
            for sub in ("inbox", "forum", "nuntium"):
                (agent_dir(root) / sub).mkdir(parents=True)
            r = rid(25)
            (inbox(root) / f"{r}.res.json").write_text(
                json.dumps(response_file(r, result="old")), encoding="utf-8"
            )
            await self.boot(root)
            await self.files(root, **{f"{r}.can.json": cancellation_file(r)})
            await self.files(root, **{f"{r}.con.json": confirmation_file(r)})
            await wait_until("the archive", archived_check(root, r))
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(record["final_status"], "confirmed")
            self.assertIsNone(record["can"])


async def _inreq_count(root: Path, r: str) -> int:
    """How many times `r` was archived: a re-served rid would add a second `inreq`."""
    read = await read_diarium(str(agent_dir(root)))
    return len(
        [
            line
            for line in read.lines
            if line["event"] == "inreq"
            and isinstance(line["event_data"], dict)
            and _record_rid(line["event_data"]) == r
        ]
    )


class CommandTest(ReceiverTest):
    async def test_stop_is_archived_before_the_hook_runs(self) -> None:
        archived_at_hook: list[bool] = []

        with actio_tree() as root:
            ctx, hooks, _ = await self.boot(root)
            r = rid(26)

            async def before(name: str) -> None:
                assert name == "stop"
                archived_at_hook.append((await inreq_for(root, r)) is not None)

            hooks.before_command = before
            await self.files(root, **{f"{r}.json": cmd_file(r, "stop", {"force": True})})
            await wait_for("the command hook", lambda: bool(hooks.commands))
            self.assertEqual(hooks.commands, [("stop", True)])
            self.assertTrue(archived_at_hook[0])
            await wait_until("the archive", archived_check(root, r))
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(record["final_status"], "completed")
            self.assertIsNone(record["can"])
            self.assertIsNone(record["res"])
            self.assertIsNone(record["con"])
            self.assertEqual(list(inbox(root).iterdir()), [])
            inreqs = [e for e in ctx.events if isinstance(e, InreqEvent)]
            self.assertEqual([(e.rid, e.final_status) for e in inreqs], [(r, "completed")])

    async def test_restart_reports_the_delay_in_seconds(self) -> None:
        with actio_tree() as root:
            _, hooks, _ = await self.boot(root)
            r = rid(27)
            await self.files(
                root, **{f"{r}.json": cmd_file(r, "restart", {"force": False, "delay": 2})}
            )
            await wait_for("the command hook", lambda: bool(hooks.commands))
            self.assertEqual(hooks.commands, [("restart", False, 2.0)])

    async def test_enable_and_ping(self) -> None:
        with actio_tree() as root:
            _, hooks, _ = await self.boot(root)
            await self.files(
                root,
                **{
                    f"{rid(28)}.json": cmd_file(rid(28), "enable", {"enabled": True}),
                    f"{rid(29)}.json": cmd_file(rid(29), "ping"),
                },
            )
            await wait_for("both commands", lambda: len(hooks.commands) == 2)
            self.assertEqual(hooks.commands, [("enable", True), ("ping",)])

    async def test_commands_run_in_rid_order_within_a_batch(self) -> None:
        with actio_tree() as root:
            _, hooks, _ = await self.boot(root)
            low, high = rid(30), rid(31)
            # Write the larger rid first: one scan must still run the smaller one first.
            write_files_sync(
                root,
                **{
                    f"{high}.json": cmd_file(high, "ping"),
                    f"{low}.json": cmd_file(low, "stop", {"force": False}),
                },
            )
            await wait_for("both commands", lambda: len(hooks.commands) == 2)
            self.assertEqual(hooks.commands, [("stop", False), ("ping",)])

    async def test_a_command_from_a_foreign_src_is_dropped(self) -> None:
        with actio_tree() as root:
            ctx, hooks, _ = await self.boot(root)
            r = rid(0)
            await self.files(root, **{f"{r}.json": cmd_file(r, "stop", {}, src="other")})
            await wait_until("the archive", archived_check(root, r))
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(record["final_status"], "dropped")
            self.assertEqual(hooks.commands, [])
            self.assertTrue(
                any(isinstance(e, LogEvent) and "dropped command" in e.message for e in ctx.events)
            )
            self.assertEqual(list(inbox(root).iterdir()), [])

    async def test_an_unknown_command_is_dropped(self) -> None:
        with actio_tree() as root:
            _, hooks, _ = await self.boot(root)
            r = rid(31)
            await self.files(root, **{f"{r}.json": cmd_file(r, "reboot")})
            await wait_until("the archive", archived_check(root, r))
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(record["final_status"], "dropped")
            self.assertEqual(hooks.commands, [])

    async def test_a_disabled_agent_still_answers_commands(self) -> None:
        with actio_tree() as root:
            _, hooks, _ = await self.boot(root, spec=make_spec(disabled=True))
            r = rid(0)
            await self.files(root, **{f"{r}.json": cmd_file(r, "enable", {"enabled": True})})
            await wait_for("the command hook", lambda: bool(hooks.commands))
            self.assertEqual(hooks.commands, [("enable", True)])

    async def test_a_duplicate_command_rid_runs_once(self) -> None:
        with actio_tree() as root:
            _, hooks, _ = await self.boot(root)
            r = rid(0)
            await self.files(root, **{f"{r}.json": cmd_file(r, "ping")})
            await wait_until("the archive", archived_check(root, r))
            await self.files(root, **{f"{r}.json": cmd_file(r, "ping")})
            await asyncio.sleep(0.5)
            self.assertEqual(hooks.commands, [("ping",)])


class StopTest(ReceiverTest):
    async def test_stop_settles_a_working_exchange_with_code_300(self) -> None:
        finished = asyncio.Event()
        gate = asyncio.Event()

        async def gated(parameters: Any, ctx: Any) -> str:
            await gate.wait()
            finished.set()
            return "released"

        with actio_tree() as root:
            ctx, hooks, receiver = await self.boot(
                root, handlers={"summarize": gated}, confirm_timeout=5.0
            )
            r = rid(0)
            await self.files(root, **{f"{r}.json": request_file(r)})
            await wait_for("the work to start", lambda: hooks.state_changes >= 1)
            await receiver.stop()
            self.assertFalse(finished.is_set())
            # The inbox is clean: `stop` archived the exchange, so the code-300 answer is in the
            # diarium record, not in a file.
            await wait_until("the archive", archived_check(root, r))
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(record["final_status"], "completed")
            self.assertEqual(res_body(record)["result"]["code"], 300)
            self.assertEqual(list(inbox(root).iterdir()), [])
            self.assertEqual([e for e in ctx.events if isinstance(e, LogEvent)], [])

    async def test_stop_archives_a_responded_exchange(self) -> None:
        with actio_tree() as root:
            _, _, receiver = await self.boot(root, confirm_timeout=5.0)
            r = rid(0)
            await self.files(root, **{f"{r}.json": request_file(r)})
            await wait_for("the response file", lambda: self.res_path(root, r).exists())
            await receiver.stop()
            record = await inreq_for(root, r)
            assert record is not None
            self.assertEqual(record["final_status"], "completed")
            self.assertEqual(list(inbox(root).iterdir()), [])

    async def test_stop_is_idempotent(self) -> None:
        with actio_tree() as root:
            _, _, receiver = await self.boot(root)
            await receiver.stop()
            await receiver.stop()  # the second stop is a no-op, not an error

    async def test_a_command_file_written_after_stop_is_left_for_the_next_run(self) -> None:
        with actio_tree() as root:
            _, _, receiver = await self.boot(root)
            r = rid(0)
            await receiver.stop()
            (inbox(root) / f"{r}.json").write_text(
                json.dumps(cmd_file(r, "ping")), encoding="utf-8"
            )
            self.assertTrue((inbox(root) / f"{r}.json").exists())


class WatchLossTest(ReceiverTest):
    async def test_a_lost_inbox_is_reported_once_and_does_not_crash(self) -> None:
        with actio_tree() as root:
            _, hooks, receiver = await self.boot(root)
            inbox(root).rmdir()
            await wait_for(
                "the watch-loss report",
                lambda: any(folder == str(inbox(root)) for folder, _ in hooks.watch_errors),
            )
            self.assertEqual(len(hooks.watch_errors), 1)
            self.assertIsInstance(hooks.watch_errors[0][1], OSError)
            # The receiver is still alive (the loop waits on nothing) until `stop`.
            await receiver.stop()
