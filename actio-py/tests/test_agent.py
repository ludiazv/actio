"""`_agent.py`: the lifecycle — `start`, `stop`, `restart`, the heartbeat, and the public seams.

The agent runs on a real `Watcher`, a real diarium and a real inbox in a temporary tree; the clock is
fake and injected through `ActioConfig`, so a test decides when a "fresh datetime" is. Timeouts are
generous on purpose: watchdog timing varies under load, and the suite runs eight times at once at each
milestone, where a flake is a failure.

Named by the rules they pin: *Workflow* steps 1-5 and C1, D-9 (`stop`), D-10 (`restart`), D-17
(`nuntium`), D-18 (the self-call), D-19 (registration), D-22 (a lost own inbox).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import shutil
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any, cast
from unittest import mock

import referencing
from jsonschema import Draft202012Validator

from actio import ActioAgent, Capability, CapabilityHandler, CommandHandler, LogEvent
from actio._config import ActioConfig
from actio._protocol import DiariumEntry
from actio._types import AgentView
from actio._util import format_iso
from helpers import ActioTestCase, actio_tree

SETTLE = 6.0  # watchdog wake + debounce + a handler: a failure, not a timing expectation
WATCH_SETTLE = 20.0  # a file-event wait under parallel load: the bound, not the expectation
AGENT = "alpha"
PEER = "beta"
NONCE = "52ffe97b3a5e6e2f00a69edb27551465"
TS_ISO = "2026-10-24T10:00:00Z"
TS = 1_792_836_000.0  # the vector's clock, in seconds


class Clock:
    """The fake clock a test ticks; the agent sees it through the injected `config.clock`."""

    def __init__(self, start: float = TS) -> None:
        self._now = start

    def value(self) -> float:
        return self._now

    def tick(self, seconds: float) -> None:
        self._now += seconds


def config_for(root: Path, clock: Clock, **overrides: Any) -> ActioConfig:
    """A test config: fast timers, `sync="none"`, the injected clock."""
    base: dict[str, Any] = {
        "root": str(root),
        "sync": "none",
        "heartbeat_interval": 0.1,
        "confirm_timeout": 1.0,
        "cancel_wait_timeout": 0.5,
        "announce_delay": 0.01,
        "nuntium_debounce": 0.2,
        "stop_grace": 1.0,
        "clock": clock.value,
    }
    base.update(overrides)
    return ActioConfig(**base)


def cap(name: str = "summarize", **actio_over: Any) -> Capability:
    """A minimal valid capability: object parameters, an object result, no optional `actio` fields."""
    actio: dict[str, Any] = {"result": {"type": "object"}}
    actio.update(actio_over)
    return cast(
        Capability,
        {
            "name": name,
            "description": f"the {name} capability",
            "parameters": {"type": "object"},
            "actio": actio,
        },
    )


def rid(n: int) -> str:
    """A deterministic valid ULID (Crockford base32, 26 chars, first digit 0-7)."""
    return "01JB7Q3W8K2M4N6P8R0S2T4V" + f"{n % 256:02X}"


def cmd_file(
    r: str, name: str, parameters: dict[str, Any] | None, *, src: str = PEER, dst: str = AGENT
) -> dict[str, Any]:
    """A lane D command envelope: the header the receiver validates, and the command body."""
    return {
        "rid": r,
        "src": src,
        "dst": dst,
        "datetime": TS_ISO,
        "type": "cmd",
        "actio_version": "0.1",
        "request": {
            "ts": int(TS),
            "nonce": NONCE,
            "signature": None,
            "name": name,
            "parameters": parameters,
            "timeout": 0,
            "attachments": [],
        },
    }


def request_file(
    r: str, name: str = "summarize", *, src: str = PEER, dst: str = AGENT
) -> dict[str, Any]:
    """A lane A request envelope, as a peer would write it."""
    return {
        "rid": r,
        "src": src,
        "dst": dst,
        "datetime": TS_ISO,
        "type": "req",
        "actio_version": "0.1",
        "request": {
            "ts": int(TS),
            "nonce": NONCE,
            "signature": None,
            "name": name,
            "parameters": {"text": "a text to summarize"},
            "timeout": 300,
            "attachments": [],
        },
    }


def peer_tree(
    root: Path,
    peer: str = PEER,
    *,
    disabled: bool = False,
    accepts: list[str] | None = None,
    namespaces: list[str | None] | None = None,
    posture: str = "trusted",
) -> None:
    """A peer folder with a valid spec and an `idle` status: enough for discovery to rate it. The peer
    never answers: the tests that use it are about the view and the local refusal, not the exchange."""
    folder = root / peer
    (folder / "inbox").mkdir(parents=True)
    spec: dict[str, Any] = {
        "id": peer,
        "actio_version": "0.1",
        "datetime": TS_ISO,
        "description": "a peer in the tree",
        "accepts_commands_from": accepts if accepts is not None else [],
        "capabilities": [cap("echo")],
        "security_support": [],
        "security_posture": posture,
    }
    if disabled:
        spec["disabled"] = True
    if namespaces is not None:
        spec["namespaces"] = namespaces
    write_file(folder / "spec.json", spec)
    write_file(
        folder / "status.json",
        {"id": peer, "status": "idle", "datetime": TS_ISO, "capability_status": []},
    )


def write_file(path: Path, doc: dict[str, Any]) -> None:
    path.write_text(json.dumps(doc), encoding="utf-8")


def doc(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


# The schemas, read straight from the committed copy: the verdict a test asserts on must not come
# from the module under test. `actio._validate` loads the same bytes for its own registry.
_SCHEMA_DIR = Path(__file__).resolve().parents[1] / "src" / "actio" / "schemas"


def _schema_registry() -> referencing.Registry:
    reg: referencing.Registry = referencing.Registry()
    for name in ("common", "spec", "status"):
        schema = doc(_SCHEMA_DIR / f"{name}.schema.json")
        reg = reg.with_resource(schema["$id"], referencing.Resource.from_contents(schema))
    return reg


_VALIDATORS: dict[str, Draft202012Validator] = {}


def schema_reason(kind: str, checked: dict[str, Any]) -> str | None:
    """The pinned schema's verdict on `checked`: None when it conforms, the reasons when not."""
    if kind not in _VALIDATORS:
        _VALIDATORS[kind] = Draft202012Validator(
            doc(_SCHEMA_DIR / f"{kind}.schema.json"), registry=_schema_registry()
        )
    errors = sorted(_VALIDATORS[kind].iter_errors(checked), key=lambda e: list(e.absolute_path))
    if not errors:
        return None
    return "; ".join(f"{'/'.join(map(str, e.absolute_path))}: {e.message}" for e in errors)


def status_path(root: Path, agent: str = AGENT) -> Path:
    return root / agent / "status.json"


def spec_path(root: Path, agent: str = AGENT) -> Path:
    return root / agent / "spec.json"


def inbox_dir(root: Path, agent: str = AGENT) -> Path:
    return root / agent / "inbox"


def announce_path(root: Path, agent: str = AGENT) -> Path:
    return root / "nuntium" / f"{agent}.n.json"


def announce_files(root: Path, agent: str = AGENT) -> list[Path]:
    return sorted((root / "nuntium").glob(f"{agent}.*.json"))


def lines(root: Path, agent: str = AGENT) -> list[DiariumEntry]:
    """The diarium's lines in file order: the order the rules assert on is the file's order."""
    path = root / agent / "diarium.jsonl"
    if not path.exists():
        return []
    raw = path.read_text(encoding="utf-8")
    return cast(
        list[DiariumEntry],
        [json.loads(part) for part in raw.splitlines() if part.strip()],
    )


async def wait_for(what: str, predicate: Callable[[], bool], timeout: float = SETTLE) -> None:
    """Poll until `predicate` is true; the failure names what was waited for. A test that asserts a
    file is gone or a line has landed waits for it: reading the disk before the fact is the flake."""
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


async def quick_handler(parameters: Any, ctx: Any) -> dict[str, str]:
    return {"text": "short"}


async def echo_handler(parameters: Any, ctx: Any) -> dict[str, Any]:
    return {"echoed": (parameters or {}).get("text", "")}


class AgentTest(ActioTestCase):
    """Builds an agent for `alpha` in a fresh tree; every test ends with it stopped.

    The base class fails the test if a task or a thread of ours is still armed, so a lifecycle test
    that leaks a heartbeat or a watcher thread cannot pass.
    """

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self._agents: list[ActioAgent] = []  # every agent this test built, in build order

    def track(self, agent: ActioAgent) -> ActioAgent:
        """Register an agent so `tree()` below stops it before its folders go away."""
        self._agents.append(agent)
        self.addAsyncCleanup(agent.stop)
        return agent

    @contextlib.asynccontextmanager
    async def tree(self) -> AsyncIterator[Path]:
        """`actio_tree()`, with the agents this test built stopped **before** its folders go away.

        A test that ends with an agent running deletes its tree under that agent's feet: a delayed
        write (D-2's `announce_delay` re-announce, D-8's status write) can land mid-removal, and
        `TemporaryDirectory.cleanup()` then raises for a reason that has nothing to do with what the
        test asked. The folders outlive every task of the agents that own them — which is what D-9's
        stop order is for. `stop()` on an already stopped agent returns at once, so a test that stopped
        its agent on purpose is unaffected.
        """
        started = len(self._agents)
        with actio_tree() as root:
            try:
                yield root
            finally:
                for agent in self._agents[started:]:
                    await agent.stop()

    def build(
        self,
        root: Path,
        *,
        caps: dict[str, CapabilityHandler] | None = None,
        accepts: list[str] | None = None,
        clock: Clock | None = None,
        **cfg_over: Any,
    ) -> tuple[ActioAgent, Clock]:
        the_clock = clock if clock is not None else Clock()
        agent = ActioAgent(
            id=AGENT,
            description="the test agent",
            # self plus the peer: a capability-less agent must accept commands from itself to stay
            # in its own view (a peer with nothing usable and no commands is discarded from it)
            accepts_commands_from=accepts if accepts is not None else [AGENT, PEER],
            config=config_for(root, the_clock, **cfg_over),
        )
        for name, handler in (caps or {}).items():
            agent.capability(cap(name), handler)
        self.track(agent)
        return agent, the_clock

    async def boot(self, root: Path, **kw: Any) -> tuple[ActioAgent, Clock]:
        agent, the_clock = self.build(root, **kw)
        await agent.start()
        return agent, the_clock


class SpineTest(AgentTest):
    async def test_the_spine_of_one_run(self) -> None:
        """*Workflow* steps 1-5 and C1, once: `start` writes a valid spec, an `idle` status and the
        announce (status first, with a delay); on exit `inactive` and the announce after it. Every
        announcement writes its diarium line, so the run's diarium is `idle`, `announce`, `inactive`,
        `announce`. Asserted on the files and the diarium, not internal state."""
        async with self.tree() as root:
            agent, _ = self.build(root)
            async with agent:
                spec = doc(spec_path(root))
                self.assertIsNone(schema_reason("spec", spec))
                self.assertEqual(spec["id"], AGENT)
                status = doc(status_path(root))
                self.assertIsNone(schema_reason("status", status))
                self.assertEqual(status["status"], "idle")
                # Step 3: status is written first, then the announce. The announce is one file per
                # agent, rewritten on every announce, so the rule is asserted on the content and
                # the order of the mtimes.
                self.assertEqual(doc(announce_path(root))["nuntium"], f"agent {AGENT} is active")
                self.assertLessEqual(
                    status_path(root).stat().st_mtime_ns,
                    announce_path(root).stat().st_mtime_ns,
                )
                # Step 4: the first discovery ran; our own folder is in the view (D-18 hangs on it).
                self.assertIn(AGENT, agent.agmen())
                # The `idle` status line and the start's `announce` line, in that order.
                self.assertEqual([line["event_data"] for line in lines(root)], ["idle", None])
            # `__aexit__` ran C1: `inactive` first, then the announce (it rewrites the same file).
            self.assertEqual(doc(status_path(root))["status"], "inactive")
            self.assertEqual(doc(announce_path(root))["nuntium"], f"agent {AGENT} is inactive")
            self.assertLessEqual(
                status_path(root).stat().st_mtime_ns,
                announce_path(root).stat().st_mtime_ns,
            )
            # C1's order: the `inactive` status line, then the `announce` line, then the file.
            self.assertEqual(
                [line["event_data"] for line in lines(root)], ["idle", None, "inactive", None]
            )


class StopTest(AgentTest):
    async def test_three_in_flight_requests_are_abandoned_before_inactive(self) -> None:
        """D-9: what is in B2 is abandoned, and every owed `outreq` line is on disk before C1's
        `inactive` write — the outbound side is stopped (step 4) before the status (C1). The test's
        caller tasks are not the agent's: the stop tears the agent down, and the callers go away
        with it, as a torn-down handler would (the exchange ends correctly in the background)."""
        async with self.tree() as root:
            peer_tree(root)  # the peer never answers
            agent, _ = await self.boot(root)
            tasks = [asyncio.create_task(agent.request(PEER, "echo", {"n": i})) for i in range(3)]
            await wait_for(
                "the three request files in the peer's inbox",
                lambda: len(list(inbox_dir(root, PEER).iterdir())) == 3,
            )
            await agent.stop()
            for task in tasks:
                task.cancel()
            with contextlib.suppress(BaseException):
                await asyncio.gather(*tasks, return_exceptions=True)
            await wait_for(
                "the background finishers to end",
                lambda: (
                    not any(
                        not t.done() and t.get_name().startswith("actio.out.")
                        for t in asyncio.all_tasks()
                    )
                ),
            )
            di = lines(root)
            outreq = [line for line in di if line["event"] == "outreq"]
            self.assertEqual(len(outreq), 3)
            for line in outreq:
                self.assertEqual(
                    cast(dict[str, Any], line["event_data"])["final_status"], "abandoned"
                )
            # The order on disk: every `abandoned` line before the `inactive` status line.
            inactive_idx = max(
                i
                for i, line in enumerate(di)
                if line["event"] == "status" and line["event_data"] == "inactive"
            )
            for line in outreq:
                self.assertLess(di.index(line), inactive_idx)

    async def test_a_tick_after_stop_leaves_status_untouched(self) -> None:
        """D-9 and the publisher's latch: the heartbeat is stopped with the run, and nothing may
        outwrite the `inactive`. The clock is ticked past a fresh datetime, so an armed tick would
        be visible in the file."""
        async with self.tree() as root:
            agent, clock = await self.boot(root, heartbeat_interval=0.05)
            await agent.stop()
            before = status_path(root).read_bytes()
            clock.tick(10)
            await asyncio.sleep(0.3)  # six intervals, if a tick were armed
            self.assertEqual(status_path(root).read_bytes(), before)

    async def test_a_slow_tick_racing_stop_ends_with_inactive(self) -> None:
        """D-9: a slow tick must not land after `inactive`. The stop awaits the heartbeat (step 5)
        before C1, and the publisher's latch keeps a late write out; this test gives the in-flight
        tick every chance to land (it finishes after `stop()` returns) and then reads the file."""
        async with self.tree() as root:
            agent, _ = await self.boot(root, heartbeat_interval=0.05)
            run = agent._run
            assert run is not None
            publisher: Any = run.publisher
            publish = publisher.publish_status

            async def slow(status: Any) -> None:
                if status["status"] != "inactive":
                    await asyncio.sleep(0.3)  # the tick's write is the slow one; C1's is not
                await publish(status)

            publisher.publish_status = slow
            await asyncio.sleep(0.1)  # a tick is in flight
            await agent.stop()
            await wait_for(
                "the in-flight tick to finish",
                lambda: (
                    not any(
                        not t.done() and t.get_name() == "actio.heartbeat"
                        for t in asyncio.all_tasks()
                    )
                ),
            )
            self.assertEqual(doc(status_path(root))["status"], "inactive")


class LifecycleTest(AgentTest):
    async def test_a_stop_during_start_waits_for_it_and_ends_inactive(self) -> None:
        """D-9: a stop during start waits for it — the lifecycle steps run one at a time — and ends
        with the agent inactive and nothing armed."""
        async with self.tree() as root:
            agent, _ = self.build(root, announce_delay=1.0)  # a catchable start
            start = asyncio.create_task(agent.start())
            await wait_for(
                "start to publish the idle status",
                lambda: status_path(root).exists() and doc(status_path(root))["status"] == "idle",
            )
            await agent.stop()  # returns only after start finished and the teardown is done
            await start
            self.assertFalse(agent._running())
            self.assertEqual(doc(status_path(root))["status"], "inactive")

    async def test_a_cancelled_start_leaves_nothing_armed_and_publishes_inactive(self) -> None:
        """D-9: a caller who cancels `start` aborts it and leaves nothing armed, publishing
        `inactive` if `idle` was."""
        async with self.tree() as root:
            agent, _ = self.build(root, announce_delay=1.0)  # a catchable start
            start = asyncio.create_task(agent.start())
            await wait_for("start to publish the idle status", lambda: status_path(root).exists())
            start.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await start
            self.assertFalse(agent._running())
            self.assertEqual(doc(status_path(root))["status"], "inactive")
            # The announce never landed: the abort came before it.
            self.assertEqual(len(announce_files(root)), 0)

    async def test_a_second_stop_waits_for_the_first(self) -> None:
        """D-9: a second `stop` only waits for the first; the teardown runs once, in order. An
        in-flight request makes the first teardown spend its grace, so "the second waited" is
        observable: without the wait, the status would still be `busy` when both calls return."""
        async with self.tree() as root:
            peer_tree(root)  # the request's source, discovered and callable
            release = asyncio.Event()

            async def slow_handler(params: Any, ctx: Any) -> dict[str, str]:
                await release.wait()
                return {"text": "done"}

            agent, _ = await self.boot(root, caps={"summarize": slow_handler})
            write_file(inbox_dir(root) / f"{rid(1)}.json", request_file(rid(1)))
            await wait_for(
                "the accepted request to make us busy",
                lambda: doc(status_path(root))["status"] == "busy",
            )
            order: list[str] = []

            async def stopping(tag: str) -> None:
                await agent.stop()
                order.append(tag)

            first = asyncio.create_task(stopping("first"))
            await asyncio.sleep(0.1)  # the first stop is in its grace wait
            second = asyncio.create_task(stopping("second"))
            await asyncio.gather(first, second)
            release.set()
            self.assertEqual(order, ["first", "second"])
            self.assertEqual(doc(status_path(root))["status"], "inactive")

    async def test_wait_stopped_returns_when_stopped_for_good(self) -> None:
        async with self.tree() as root:
            agent, _ = await self.boot(root)
            stopped = asyncio.create_task(agent.wait_stopped())
            await asyncio.sleep(0.1)
            self.assertFalse(stopped.done())
            await agent.stop()
            await asyncio.wait_for(stopped, SETTLE)

    async def test_fail_sets_error_and_announces(self) -> None:
        """*Agent's status*: `fail` is status `error` plus an announce; there is no recovery, and a
        second `fail` does not announce again. Every announcement writes its diarium line, so `fail`
        appends one `announce` line and the no-op second `fail` appends none."""
        async with self.tree() as root:
            agent, _ = await self.boot(root)
            # The start's announce wrote one line; the `fail`'s will be the second.
            self.assertEqual(
                [line["event_data"] for line in lines(root) if line["event"] == "announce"],
                [None],
            )
            await agent.fail("the disk is on fire")
            self.assertEqual(doc(status_path(root))["status"], "error")
            self.assertEqual(
                doc(announce_path(root))["nuntium"],
                f"agent {AGENT} failed: the disk is on fire",
            )
            self.assertEqual(
                [line["event_data"] for line in lines(root) if line["event"] == "status"],
                ["idle", "error"],
            )
            # The `fail` appended one `announce` line: the start's and the fail's.
            self.assertEqual(
                [line["event_data"] for line in lines(root) if line["event"] == "announce"],
                [None, None],
            )
            mtime = announce_path(root).stat().st_mtime_ns
            await agent.fail("again")  # a no-op: already in error
            self.assertEqual(announce_path(root).stat().st_mtime_ns, mtime)
            self.assertEqual(doc(status_path(root))["status"], "error")
            # The no-op appended no `announce` line.
            self.assertEqual(
                [line["event_data"] for line in lines(root) if line["event"] == "announce"],
                [None, None],
            )

    async def test_a_lost_own_inbox_is_a_log_event_and_an_error_status(self) -> None:
        """D-22: a lost own inbox is fatal: log it and enter `error`, with the announce. The log is
        the observability event (the diarium keeps the `status` line), as the reference does. The `fail`
        the loss arms writes its `announce` diarium line — exactly one, on top of the start's."""
        async with self.tree() as root:
            agent, _ = await self.boot(root)
            events: list[Any] = []
            agent.on_event(lambda e: events.append(e))
            inbox_dir(root).rmdir()
            await wait_for(
                "the error status",
                lambda: status_path(root).exists() and doc(status_path(root))["status"] == "error",
            )
            await wait_for(
                "the log event",
                lambda: any(isinstance(e, LogEvent) and "watch lost" in e.message for e in events),
            )
            self.assertIn("failed", doc(announce_path(root))["nuntium"])
            # The `fail` the loss armed wrote one `announce` line: the start's and the fail's.
            self.assertEqual(
                sum(1 for line in lines(root) if line["event"] == "announce"),
                2,
            )

    async def test_a_stop_supersedes_a_fail_armed_by_a_lost_watch(self) -> None:
        """D-22 arms a `fail` task and D-9's step 6 owns it: a `stop()` cancels what the run armed instead
        of leaving it armed behind the stop, and the run ends at C1's `inactive`. The sync provider holds
        each write, so the `fail` is still in flight when `stop()` runs. The base class's leftover check is
        the other half: an untracked `actio.fail` is still armed at teardown."""
        async with self.tree() as root:
            agent, _ = await self.boot(root, heartbeat_interval=1000, sync="delay:400")
            events: list[Any] = []
            agent.on_event(lambda e: events.append(e))
            inbox_dir(root).rmdir()  # D-22: the own inbox is gone
            await wait_for(
                "the watch-lost log event",
                lambda: any(isinstance(e, LogEvent) and "watch lost" in e.message for e in events),
                timeout=WATCH_SETTLE,
            )
            run = agent._run
            assert run is not None
            armed = list(run.fail_tasks)
            self.assertEqual(len(armed), 1)  # the arm is the run's, not a loose end
            await agent.stop()
            self.assertTrue(all(task.done() for task in armed))
            self.assertEqual(run.fail_tasks, [])
            self.assertEqual(doc(status_path(root))["status"], "inactive")  # C1 supersedes


class RestartTest(AgentTest):
    async def test_a_restart_keeps_the_object_and_registrations_and_drops_disabled(self) -> None:
        """D-10: the agent object stays the same, `wait_stopped()` does not return, registrations
        survive, and a `disabled` set by `enable` does not: the fresh start rebuilds the spec."""
        async with self.tree() as root:
            agent, _ = await self.boot(root, caps={"summarize": quick_handler})
            write_file(
                inbox_dir(root) / f"{rid(1)}.json", cmd_file(rid(1), "enable", {"enabled": False})
            )
            await wait_for(
                "the disabled spec",
                lambda: spec_path(root).exists() and doc(spec_path(root)).get("disabled") is True,
            )
            write_file(
                inbox_dir(root) / f"{rid(2)}.json",
                cmd_file(rid(2), "restart", {"force": False, "delay": 0.2}),
            )
            await wait_for(
                "the restarted agent (a fresh view that serves us, without `disabled`)",
                lambda: AGENT in agent.agmen() and "disabled" not in doc(spec_path(root)),
            )
            # The registration survived the restart.
            self.assertEqual(
                [c["name"] for c in doc(spec_path(root))["capabilities"]], ["summarize"]
            )
            # And the new run serves: a self-call through the fresh engines.
            outcome = await agent.request(AGENT, "summarize", {"text": "hi"}, timeout=5)
            self.assertEqual(outcome.final_status, "confirmed")
            # `wait_stopped()` does not return during (or after) a restart.
            stopped = asyncio.create_task(agent.wait_stopped())
            await asyncio.sleep(0.1)
            self.assertFalse(stopped.done())
            await agent.stop()
            await asyncio.wait_for(stopped, SETTLE)

    async def test_a_restart_whose_start_fails_ends_wait_stopped(self) -> None:
        """D-10: a restart whose `start` fails ends `wait_stopped()` too, because nobody is left to
        bring the agent back."""
        async with self.tree() as root:
            agent, _ = await self.boot(root)
            write_file(
                inbox_dir(root) / f"{rid(1)}.json",
                cmd_file(rid(1), "restart", {"force": False, "delay": 0.5}),
            )
            await wait_for(
                "the restart's stop",
                lambda: (
                    status_path(root).exists() and doc(status_path(root))["status"] == "inactive"
                ),
            )
            # The next start must fail: the agent folder is a file, so `make_dirs` raises.
            shutil.rmtree(root / AGENT)
            (root / AGENT).write_text("not a folder", encoding="utf-8")
            stopped = asyncio.create_task(agent.wait_stopped())
            await asyncio.wait_for(stopped, SETTLE)
            self.assertFalse(agent._running())

    async def test_a_stop_during_a_restart_delay_cancels_the_restart(self) -> None:
        """D-10: *"a `stop` during the delay cancels the restart and ends `wait_stopped()`"*. The window
        opens inside the restart's own teardown — its `inactive` is on disk before the sleep — and closes
        when the fresh `start` begins, and the run is stopped throughout. A `stop` that looked only at the
        run would find nothing to do and let the agent come back behind the caller's back. The delay is
        long on purpose, so that load cannot close the window on its own; the base class's leftover check
        is half the test, since a restart still sleeping is still armed."""
        async with self.tree() as root:
            agent, _ = await self.boot(root)
            write_file(
                inbox_dir(root) / f"{rid(1)}.json",
                cmd_file(rid(1), "restart", {"force": False, "delay": 10.0}),
            )
            await wait_for(
                "the restart's stop: `inactive` on disk, the delay running",
                lambda: (
                    status_path(root).exists() and doc(status_path(root))["status"] == "inactive"
                ),
            )
            stopped = asyncio.create_task(agent.wait_stopped())
            await agent.stop()
            await asyncio.wait_for(stopped, SETTLE)
            self.assertFalse(agent._running())
            self.assertEqual(doc(status_path(root))["status"], "inactive")


class CommandTest(AgentTest):
    """Lane D: the four standard commands, and what they may and may not change (*Commands*, D0-D4).

    A command is a file in the inbox, as a peer would write it; the assertions are on the files, the
    diarium and the announce, never on the in-memory state. The heartbeat is set far out in the
    command tests, so a rewritten `status.json` is the command's write, not a tick's.
    """

    @staticmethod
    def _announce_count(root: Path) -> int:
        return sum(1 for line in lines(root) if line["event"] == "announce")

    @staticmethod
    def _inreq_final(root: Path) -> list[str]:
        return [
            cast(dict[str, Any], line["event_data"])["final_status"]
            for line in lines(root)
            if line["event"] == "inreq"
        ]

    async def test_enable_sets_disabled_and_reannounces(self) -> None:
        """D2 and *Agent Spec* ("`disabled` … Changed with the `enable` command"): setting it false
        then true follows in `spec.json`, and each change re-announces — the file, the diarium line,
        the announce."""
        async with self.tree() as root:
            agent, _ = await self.boot(root, heartbeat_interval=1000)
            # The start's announce wrote one line; each `enable` below adds one more.
            self.assertEqual(self._announce_count(root), 1)
            write_file(
                inbox_dir(root) / f"{rid(1)}.json", cmd_file(rid(1), "enable", {"enabled": False})
            )
            await wait_for(
                "the disabled spec and its re-announce",
                lambda: (
                    spec_path(root).exists()
                    and doc(spec_path(root)).get("disabled") is True
                    and doc(announce_path(root))["nuntium"] == f"agent {AGENT} is disabled"
                ),
                timeout=WATCH_SETTLE,
            )
            self.assertEqual(self._announce_count(root), 2)
            write_file(
                inbox_dir(root) / f"{rid(2)}.json", cmd_file(rid(2), "enable", {"enabled": True})
            )
            await wait_for(
                "the re-enabled spec and its re-announce",
                lambda: (
                    doc(spec_path(root)).get("disabled") is False
                    and doc(announce_path(root))["nuntium"] == f"agent {AGENT} is enabled"
                ),
                timeout=WATCH_SETTLE,
            )
            self.assertEqual(self._announce_count(root), 3)

    async def test_the_announce_line_is_on_disk_before_the_announce_file(self) -> None:
        """D-24: *"append `status`/`announce` events to `diarium.jsonl`, then write the announce file
        `nuntium/<agent_id>.n.json`"* — the line, then the file, in the one place every announcement goes
        through. The delay now sits before both writes, so the order is pinned the way the unit pins it:
        the `.n.json` write is slowed, and while it is in flight the diarium already carries the line. On
        a real agent, with `enable` and an `announce_delay` large enough that the boot is settled."""
        import actio._files as files_mod
        import actio._status as status_mod

        real_write = files_mod.write_json_atomic

        async def slow_write(path: str, obj: Any, tmp_dir: str, provider: Any) -> None:
            if path.endswith(".n.json"):
                await asyncio.sleep(0.3)  # the announce file's write is the slow one
            await real_write(path, obj, tmp_dir, provider)

        async with self.tree() as root:
            agent, _ = await self.boot(root, heartbeat_interval=1000, announce_delay=0.5)
            self.assertEqual(doc(announce_path(root))["nuntium"], f"agent {AGENT} is active")
            # The start's announce wrote one line; the `enable`'s will be the second.
            self.assertEqual(self._announce_count(root), 1)
            with mock.patch.object(status_mod, "write_json_atomic", slow_write):
                write_file(
                    inbox_dir(root) / f"{rid(1)}.json",
                    cmd_file(rid(1), "enable", {"enabled": False}),
                )
                # The line lands (the append is before the write); the slowed file write is in flight.
                await wait_for(
                    "the `enable`'s `announce` diarium line",
                    lambda: self._announce_count(root) == 2,
                    timeout=WATCH_SETTLE,
                )
                # While the slowed `.n.json` write is in flight, the file still carries the start's text.
                self.assertEqual(doc(announce_path(root))["nuntium"], f"agent {AGENT} is active")
                self.assertTrue(doc(spec_path(root)).get("disabled") is True)
                await wait_for(
                    "the announce file itself",
                    lambda: doc(announce_path(root))["nuntium"] == f"agent {AGENT} is disabled",
                    timeout=WATCH_SETTLE,
                )

    async def test_a_command_while_disabled_is_processed(self) -> None:
        """D0: "commands are processed even if the agent is disabled, so that it can be enabled
        again or shut down." With `disabled: true` in the spec, an `enable` back must land and
        clear it; *Callability rules* keeps the command path open for the same reason."""
        async with self.tree() as root:
            agent, _ = await self.boot(root, heartbeat_interval=1000)
            write_file(
                inbox_dir(root) / f"{rid(1)}.json", cmd_file(rid(1), "enable", {"enabled": False})
            )
            await wait_for(
                "the disabled spec",
                lambda: spec_path(root).exists() and doc(spec_path(root)).get("disabled") is True,
                timeout=WATCH_SETTLE,
            )
            write_file(
                inbox_dir(root) / f"{rid(2)}.json", cmd_file(rid(2), "enable", {"enabled": True})
            )
            await wait_for(
                "the disabled flag cleared by the command that a disabled agent still processes",
                lambda: doc(spec_path(root)).get("disabled") is False,
                timeout=WATCH_SETTLE,
            )

    async def test_ping_republishes_spec_and_status(self) -> None:
        """D2: "`ping` re-publishes both". Both files are rewritten and the announce is written
        again; the heartbeat is 1000 s out, so the fresh datetime can only be `ping`'s write from
        the ticked clock, not a tick's."""
        async with self.tree() as root:
            agent, clock = await self.boot(root, heartbeat_interval=1000)
            spec_before = doc(spec_path(root))["datetime"]
            status_before = doc(status_path(root))["datetime"]
            clock.tick(7)
            fresh = format_iso(clock.value())
            self.assertNotEqual(fresh, spec_before)
            self.assertNotEqual(fresh, status_before)
            write_file(inbox_dir(root) / f"{rid(1)}.json", cmd_file(rid(1), "ping", None))
            await wait_for(
                "ping to re-publish the spec with the fresh datetime",
                lambda: doc(spec_path(root))["datetime"] == fresh,
                timeout=WATCH_SETTLE,
            )
            await wait_for(
                "ping to re-publish the status with the fresh datetime",
                lambda: doc(status_path(root))["datetime"] == fresh,
                timeout=WATCH_SETTLE,
            )
            await wait_for(
                "ping to re-announce",
                lambda: doc(announce_path(root))["nuntium"] == f"agent {AGENT} is alive",
                timeout=WATCH_SETTLE,
            )

    async def test_a_command_writes_no_response(self) -> None:
        """D1: "no response file is ever written for `type=\"cmd\"`." After a command, no
        `<rid>.res.json` and no `<rid>.con.json` exist, the envelope is archived (D3), and the
        diarium carries one `inreq` line with `final_status` `completed`."""
        async with self.tree() as root:
            agent, _ = await self.boot(root, heartbeat_interval=1000)
            r = rid(1)
            write_file(inbox_dir(root) / f"{r}.json", cmd_file(r, "ping", None))
            await wait_for(
                "the command's completed inreq line",
                lambda: "completed" in self._inreq_final(root),
                timeout=WATCH_SETTLE,
            )
            inbox = inbox_dir(root)
            self.assertFalse((inbox / f"{r}.res.json").exists())
            self.assertFalse((inbox / f"{r}.con.json").exists())
            await wait_for(
                "the archived envelope to be gone from the inbox",
                lambda: not (inbox / f"{r}.json").exists(),
                timeout=WATCH_SETTLE,
            )

    async def test_a_command_from_an_unlisted_src_is_dropped(self) -> None:
        """D4: a command from a `src` not in `accepts_commands_from` is dropped without touching what
        peers see — `status.json`, `spec.json` and the announce file are **not** touched, and the
        diarium carries the `inreq` line with `final_status` `dropped`."""
        async with self.tree() as root:
            agent, _ = await self.boot(root, heartbeat_interval=1000)
            status_before = status_path(root).read_bytes()
            spec_before = spec_path(root).read_bytes()
            announce_before = announce_path(root).read_bytes()
            r = rid(9)
            # `gamma` is not in `accepts_commands_from` (only `alpha` and `beta` are).
            write_file(inbox_dir(root) / f"{r}.json", cmd_file(r, "ping", None, src="gamma"))
            await wait_for(
                "the dropped inreq line",
                lambda: "dropped" in self._inreq_final(root),
                timeout=WATCH_SETTLE,
            )
            self.assertEqual(status_path(root).read_bytes(), status_before)
            self.assertEqual(spec_path(root).read_bytes(), spec_before)
            self.assertEqual(announce_path(root).read_bytes(), announce_before)

    async def test_on_command_in_both_forms(self) -> None:
        """`on_command(name, handler)` in both frozen forms: the direct call and the decorator. The
        commands are never advertised, so the registration is asserted on the agent's own table."""
        async with self.tree() as root:
            agent, _ = self.build(root)

            async def direct_handler(params: Any, ctx: Any) -> None:
                return None

            agent.on_command("direct_cmd", direct_handler)

            @agent.on_command("decorated_cmd")
            async def decorated_handler(params: Any, ctx: Any) -> None:
                return None

            self.assertIn("direct_cmd", agent._commands)
            self.assertIn("decorated_cmd", agent._commands)
            self.assertIs(agent._commands["decorated_cmd"], decorated_handler)

    async def test_on_command_refuses_a_standard_name(self) -> None:
        """D0: the agent knows "either a protocol-standard command (`stop`, `enable`, `restart`,
        `ping`) or a custom command it implements internally"; the standard four are built-in, so
        `on_command` refuses to shadow them."""
        async with self.tree() as root:
            agent, _ = self.build(root)

            async def handler(params: Any, ctx: Any) -> None:
                return None

            for name in ("stop", "enable", "restart", "ping"):
                with self.assertRaises(ValueError):
                    agent.on_command(name, handler)

    async def test_on_command_refuses_a_duplicate(self) -> None:
        async with self.tree() as root:
            agent, _ = self.build(root)

            async def handler(params: Any, ctx: Any) -> None:
                return None

            agent.on_command("my_cmd", handler)
            with self.assertRaises(ValueError):
                agent.on_command("my_cmd", handler)

    async def test_on_command_needs_an_async_handler(self) -> None:
        async with self.tree() as root:
            agent, _ = self.build(root)

            def sync_handler(params: Any, ctx: Any) -> None:
                return None

            with self.assertRaises(TypeError):
                agent.on_command("sync_cmd", cast(CommandHandler, sync_handler))


class RegistrationTest(AgentTest):
    """Lane C: the capability registrations and their schemas (D-19), and how they reach
    `spec.json`. The own side is strict (a bad schema is a registration error); the peer side is
    lenient (a schema that will not run here skips the check and logs once)."""

    async def test_a_good_capability_lands_in_the_spec_with_its_schema(self) -> None:
        """A valid capability reaches `spec.json` whole, its `actio.result` schema and all: the
        schema a peer validates against is the one the agent published."""
        async with self.tree() as root:
            agent, _ = await self.boot(root, caps={"summarize": quick_handler})
            caps = {c["name"]: c for c in doc(spec_path(root))["capabilities"]}
            self.assertIn("summarize", caps)
            self.assertEqual(caps["summarize"]["actio"]["result"], {"type": "object"})
            self.assertEqual(caps["summarize"]["parameters"], {"type": "object"})

    async def test_a_bad_own_schema_raises_and_leaves_the_registration_unchanged(self) -> None:
        """D-19 (own side): "a failing `check_schema` or an **unknown keyword** is a registration
        error." The `capability` call raises, and the rejected capability is not registered."""
        async with self.tree() as root:
            agent, _ = self.build(root)
            broken = cap("broken")
            broken["actio"]["result"] = {"type": "object", "bogusKeyword": True}  # unknown keyword
            with self.assertRaises(ValueError):
                agent.capability(broken, quick_handler)
            self.assertNotIn("broken", agent._capabilities)

    async def test_a_peers_bad_capability_is_skipped_and_logged_once(self) -> None:
        """D-19 (peer side): "a failing `check_schema`, or an error from the validator itself as
        opposed to a validation result, skips the checks and logs once." The skip is not fatal, and
        the log line is one, not one per call."""
        async with self.tree() as root:
            agent, _ = await self.boot(root)
            events: list[Any] = []
            agent.on_event(lambda e: events.append(e))
            broken = cap("echo")
            broken["actio"]["result"] = {"type": "object", "bogusKeyword": True}
            view = AgentView(
                id=PEER,
                folder=str(root / PEER),
                # only `spec["datetime"]` is read by `_peer_schema`; the rest satisfies the shape
                spec={
                    "id": PEER,
                    "actio_version": "0.1",
                    "datetime": TS_ISO,
                    "description": "a peer under test",
                    "accepts_commands_from": [],
                    "capabilities": [],
                    "security_support": [],
                    "security_posture": "trusted",
                },
                status={"id": PEER, "status": "idle", "datetime": TS_ISO, "capability_status": []},
                health="healthy",
                commands=False,
                capabilities={"echo": broken},
            )
            self.assertIsNone(agent._peer_schema(PEER, "echo", view))
            self.assertEqual(
                len(
                    [e for e in events if isinstance(e, LogEvent) and "checks skipped" in e.message]
                ),
                1,
            )
            # The same key again: the cache holds the null, and nothing is logged a second time.
            self.assertIsNone(agent._peer_schema(PEER, "echo", view))
            self.assertEqual(
                len(
                    [e for e in events if isinstance(e, LogEvent) and "checks skipped" in e.message]
                ),
                1,
            )

    async def test_registration_before_start_and_while_running_reach_the_spec(self) -> None:
        """A capability registered before `start` is in the spec the start publishes; one registered
        while running reaches `spec.json` on the next rebuild, which a `ping` is."""
        async with self.tree() as root:
            the_clock = Clock()
            agent = ActioAgent(
                id=AGENT,
                description="the registration test",
                accepts_commands_from=[AGENT, PEER],
                config=config_for(root, the_clock, heartbeat_interval=1000),
            )
            agent.capability(cap("early"), quick_handler)  # before start
            self.track(agent)
            await agent.start()
            names = [c["name"] for c in doc(spec_path(root))["capabilities"]]
            self.assertIn("early", names)
            agent.capability(cap("late"), quick_handler)  # while running
            write_file(inbox_dir(root) / f"{rid(1)}.json", cmd_file(rid(1), "ping", None))
            await wait_for(
                "the late registration to reach the spec on the ping's rebuild",
                lambda: "late" in [c["name"] for c in doc(spec_path(root))["capabilities"]],
            )
            self.assertIn("early", [c["name"] for c in doc(spec_path(root))["capabilities"]])

    async def test_a_duplicate_capability_name_raises(self) -> None:
        """The code's rule for a duplicate name is a registration-time `ValueError`, not a silent
        single entry: the second `capability` call with a taken name raises, and the first stands."""
        async with self.tree() as root:
            agent, _ = self.build(root)
            agent.capability(cap("same"), quick_handler)
            with self.assertRaises(ValueError):
                agent.capability(cap("same"), quick_handler)
            self.assertEqual(list(agent._capabilities), ["same"])


class PeerViewTest(AgentTest):
    """The peer view: what the callability rules leave usable (*Callability rules* 1-8), read through
    `agmen()` and `refresh()`. The view is asserted on, never the raw scan."""

    async def test_agmen_returns_the_current_view(self) -> None:
        """`agmen()` returns the current view, read-only, keyed by agent id; our own folder is in it
        (a self-call hangs on it, D-18)."""
        async with self.tree() as root:
            agent, _ = await self.boot(root)
            view = agent.agmen()
            self.assertIn(AGENT, view)
            self.assertEqual(view[AGENT].id, AGENT)

    async def test_refresh_returns_the_new_view(self) -> None:
        """`refresh()` runs a full scan now and returns the new view — the same mapping `agmen()`
        then returns (a frozen interface: "`refresh()` … it returns the new view")."""
        async with self.tree() as root:
            agent, _ = await self.boot(root)
            self.assertNotIn(PEER, agent.agmen())
            peer_tree(root)  # beta appears after the start
            new_view = await agent.refresh()
            self.assertIn(PEER, new_view)
            self.assertIn(PEER, agent.agmen())  # the refresh updated the view agmen() reads

    async def test_a_peer_with_a_usable_capability_is_in_the_view(self) -> None:
        """Rule 5.2 and 6: a peer whose capability is usable by us (no `accepts_from`) is in the
        view, with that capability."""
        async with self.tree() as root:
            agent, _ = await self.boot(root)
            peer_tree(root)  # beta offers `echo` to everyone
            await agent.refresh()
            self.assertIn(PEER, agent.agmen())
            self.assertIn("echo", agent.agmen()[PEER].capabilities)

    async def test_a_disabled_peer_is_in_the_view_without_its_capabilities(self) -> None:
        """Rule 1: "If agent B's spec has `disabled=true`, agent A will not consider agent B's
        capabilities. Commands can still be sent, so that the agent can be enabled again." A disabled
        peer that accepts our commands stays in the view, with no capabilities."""
        async with self.tree() as root:
            agent, _ = await self.boot(root)
            peer_tree(
                root, disabled=True, accepts=[AGENT]
            )  # beta is disabled, but we may command it
            await agent.refresh()
            self.assertIn(PEER, agent.agmen())
            self.assertEqual(agent.agmen()[PEER].capabilities, {})
            self.assertTrue(agent.agmen()[PEER].commands)

    async def test_a_peer_with_nothing_usable_is_discarded(self) -> None:
        """Rule 6: "If the previous filters show that no commands and no capabilities are usable by
        agent A, agent A will discard agent B." A peer that accepts no commands from us and offers no
        capability we may use is not in the view."""
        async with self.tree() as root:
            agent, _ = await self.boot(root)
            folder = root / "gamma"
            (folder / "inbox").mkdir(parents=True)
            # gamma's only capability is restricted to `beta`, and it accepts no commands from us.
            write_file(
                folder / "spec.json",
                {
                    "id": "gamma",
                    "actio_version": "0.1",
                    "datetime": TS_ISO,
                    "description": "a peer alpha cannot use",
                    "accepts_commands_from": [],
                    "capabilities": [
                        {
                            "name": "echo",
                            "description": "only for beta",
                            "parameters": {"type": "object"},
                            "actio": {"result": {"type": "object"}, "accepts_from": [PEER]},
                        }
                    ],
                    "security_support": [],
                    "security_posture": "trusted",
                },
            )
            write_file(
                folder / "status.json",
                {"id": "gamma", "status": "idle", "datetime": TS_ISO, "capability_status": []},
            )
            await agent.refresh()
            self.assertNotIn("gamma", agent.agmen())

    async def test_a_foreign_posture_peer_is_discarded(self) -> None:
        """Rule 3: "If agent A does not support the security features required by agent B's security
        posture, agent A will not consider agent B's specification." The core profile supports no
        feature, so a peer that requires any posture but `trusted` is not in the view."""
        async with self.tree() as root:
            agent, _ = await self.boot(root)
            peer_tree(root, peer="delta", posture="untrusted")
            await agent.refresh()
            self.assertNotIn("delta", agent.agmen())


class ScanTest(AgentTest):
    """D-17: the scan that keeps the view current without any `refresh()` call."""

    async def test_the_view_gains_a_later_peer_without_refresh(self) -> None:
        """D-17: "alpha runs, beta starts afterwards, and with no `refresh()` anywhere `alpha.agmen()`
        gains beta within the debounce plus a named margin." The assertion is that the view gains beta
        with **no call** at all; a mutant that drops the scan's scheduling fails it. The margin is a
        bound on the wait, not a sleep: the fact asserted is the gain, and it must come for free."""
        async with self.tree() as root:
            agent, _ = await self.boot(root, nuntium_debounce=0.2)
            self.assertNotIn(PEER, agent.agmen())
            # beta starts afterwards: its folder and its announce land in the tree.
            peer_tree(root)
            write_file(
                root / "nuntium" / f"{PEER}.n.json",
                {
                    "id": PEER,
                    "datetime": TS_ISO,
                    "ts": int(TS),
                    "nuntium": f"agent {PEER} is active",
                },
            )
            DEBOUNCE = 0.2  # the configured nuntium debounce
            MARGIN = 2.0  # the named margin: a bound, so load cannot flip the verdict
            await wait_for(
                "the view to gain beta without any refresh() call",
                lambda: PEER in agent.agmen(),
                timeout=DEBOUNCE + MARGIN,
            )
            self.assertIn(PEER, agent.agmen())


class SelfCallTest(AgentTest):
    """D-18: the self-call, served through the same files as any other."""

    async def test_a_self_call_is_served_through_the_own_inbox(self) -> None:
        """D-18: "A self-call is no special case: it covers its own destination inbox just like any
        other destination." An agent MAY call itself (*Callability rules*: "the protocol does not
        inhibit it"); the request lands in its own inbox, the outcome is `confirmed` with the archive
        intact, and the diarium carries both an `inreq` and an `outreq` line."""
        async with self.tree() as root:
            agent, _ = await self.boot(root, caps={"summarize": quick_handler}, confirm_timeout=0.2)
            outcome = await agent.request(AGENT, "summarize", {"text": "hi"}, timeout=5)
            self.assertEqual(outcome.final_status, "confirmed")
            self.assertTrue(any(line["event"] == "outreq" for line in lines(root)))
            # The receiver archives `confirm_timeout` after the response: both sides then carry a line.
            await wait_for(
                "the self-call's `inreq` line to land after the confirm timeout",
                lambda: any(line["event"] == "inreq" for line in lines(root)),
                timeout=WATCH_SETTLE,
            )
            self.assertTrue(any(line["event"] == "inreq" for line in lines(root)))
            # The exchange is archived: its files are gone from the (own) inbox.
            await wait_for(
                "the self-call's archive to empty the inbox",
                lambda: not list(inbox_dir(root).iterdir()),
                timeout=WATCH_SETTLE,
            )
            self.assertIsNone(agent._restart_task)  # the delay is 10 s: it is not merely sleeping


class TwoAgentTest(AgentTest):
    """Two live agents in one tree, talking through the folders.

    Every request test before this one talked to a **fake peer**: it wrote files under `beta/` by
    hand and no agent ran there. Here both `alpha` and `beta` are live `ActioAgent`s in the same
    `actio_tree()` root, serving each other's inboxes. `ActioAgent` takes its folder from
    `config.root` and its own id, so the second agent differs from `build` only in id and defaults;
    both keep `addAsyncCleanup(agent.stop)`, so the base class' leftover check covers both.
    """

    def build_peer(
        self,
        root: Path,
        *,
        caps: dict[str, CapabilityHandler] | None = None,
        accepts: list[str] | None = None,
        clock: Clock | None = None,
        **cfg_over: Any,
    ) -> tuple[ActioAgent, Clock]:
        """The second live agent, `beta`, in the same tree. Only `alpha` may command it by default:
        a `gamma` envelope is the D4 foreign one."""
        the_clock = clock if clock is not None else Clock()
        agent = ActioAgent(
            id=PEER,
            description="the second live agent",
            accepts_commands_from=accepts if accepts is not None else [AGENT],
            config=config_for(root, the_clock, **cfg_over),
        )
        for name, handler in (caps or {}).items():
            agent.capability(cap(name), handler)
        self.track(agent)
        return agent, the_clock

    async def _see(self, alpha: ActioAgent, name: str | None = None) -> None:
        """Wait until `alpha`'s view carries `beta` (and, if named, that capability) — no `refresh`."""

        def there() -> bool:
            view = alpha.agmen()
            if PEER not in view:
                return False
            return name is None or name in view[PEER].capabilities

        await wait_for(
            f"alpha's view to gain beta{'' if name is None else f' and {name}'}",
            there,
            timeout=WATCH_SETTLE,
        )

    async def test_a_round_trip_between_two_live_agents(self) -> None:
        """*Envelope lifecycle* / D-11: `alpha` (no capabilities) requests `echo` from a live `beta`;
        the outcome is `confirmed` and carries the handler's result, `alpha`'s diarium has an `outreq`
        line with `final_status: "confirmed"`, `beta`'s an `inreq` line with the same, and `beta`'s
        inbox is empty again with the forum holding nothing. The `inreq` line is the archive's: it is
        written when the exchange is archived, so the test waits for it."""
        async with self.tree() as root:
            alpha, _ = await self.boot(root, confirm_timeout=0.2)
            beta, _ = self.build_peer(root, caps={"echo": echo_handler}, confirm_timeout=0.2)
            await beta.start()
            await self._see(alpha, "echo")
            outcome = await alpha.request(PEER, "echo", {"text": "hello"}, timeout=10)
            self.assertEqual(outcome.final_status, "confirmed")
            assert outcome.response is not None  # a confirmed outcome carries the response body
            self.assertEqual(outcome.response["result"], {"echoed": "hello"})
            # `alpha`'s side: the `outreq` line, `confirmed`.
            self.assertTrue(
                any(
                    line["event"] == "outreq"
                    and cast(dict[str, Any], line["event_data"])["final_status"] == "confirmed"
                    for line in lines(root, AGENT)
                )
            )
            # `beta`'s side: the `inreq` line, `confirmed` (the archive lands it; the timeout is short).
            await wait_for(
                "beta's confirmed inreq line",
                lambda: any(
                    line["event"] == "inreq"
                    and cast(dict[str, Any], line["event_data"])["final_status"] == "confirmed"
                    for line in lines(root, PEER)
                ),
                timeout=WATCH_SETTLE,
            )
            # The exchange is archived: `beta`'s inbox is empty again, and the forum holds nothing.
            await wait_for(
                "beta's inbox to empty",
                lambda: not list(inbox_dir(root, PEER).iterdir()),
                timeout=WATCH_SETTLE,
            )
            self.assertEqual(list((root / "forum").iterdir()), [])

    async def test_d8_busy_and_idle_across_the_wire(self) -> None:
        """D-8 across the wire: while `beta`'s handler is parked, `beta/status.json` reads `busy`;
        after it returns, `idle`. Pinned on the file, not on internals: a state change re-evaluates
        busy and publishes it, so the file flips on the handler's start and end, not on a tick."""
        async with self.tree() as root:
            release = asyncio.Event()

            async def parking_handler(params: Any, ctx: Any) -> dict[str, Any]:
                await release.wait()
                return {"text": "done"}

            alpha, _ = await self.boot(root)
            beta, _ = self.build_peer(root, caps={"park": parking_handler})
            await beta.start()
            await self._see(alpha, "park")
            task = asyncio.create_task(alpha.request(PEER, "park", {"n": 1}, timeout=30))
            # The handler is parked: `beta`'s status is `busy`.
            await wait_for(
                "beta's status to read busy while its handler is parked",
                lambda: (
                    status_path(root, PEER).exists()
                    and doc(status_path(root, PEER))["status"] == "busy"
                ),
                timeout=WATCH_SETTLE,
            )
            release.set()
            outcome = await task
            self.assertEqual(outcome.final_status, "confirmed")
            # After the handler returns, `beta`'s status is `idle` again.
            await wait_for(
                "beta's status to read idle after its handler returns",
                lambda: doc(status_path(root, PEER))["status"] == "idle",
                timeout=WATCH_SETTLE,
            )

    async def test_d17_the_view_gains_a_later_live_peer(self) -> None:
        """D-17 between two agents: `alpha` starts first; `beta` starts and announces; `alpha.agmen()`
        gains `beta` and its capability **without any `refresh()` call** — `alpha`'s own `nuntium`
        watch did it. The control is the debounced scan (a body that returns at once) must fail this
        too. The margin is a bound on the wait under parallel load, not a sleep: the fact asserted is
        the gain, and it must come for free."""
        async with self.tree() as root:
            alpha, _ = await self.boot(root, nuntium_debounce=0.2)
            self.assertNotIn(PEER, alpha.agmen())
            beta, _ = self.build_peer(root, caps={"echo": echo_handler})
            # `beta` starts and announces; `alpha`'s `nuntium` watch triggers the debounced scan.
            start_beta = asyncio.create_task(beta.start())
            DEBOUNCE = 0.2  # the configured `nuntium_debounce`
            MARGIN = 5.0  # the named margin: a bound, so load cannot flip the verdict
            await wait_for(
                "alpha's view to gain beta and its echo capability, with no refresh() call",
                lambda: PEER in alpha.agmen() and "echo" in alpha.agmen()[PEER].capabilities,
                timeout=DEBOUNCE + MARGIN,
            )
            await start_beta
            self.assertIn(PEER, alpha.agmen())
            self.assertIn("echo", alpha.agmen()[PEER].capabilities)

    async def test_a_cancellation_across_the_wire(self) -> None:
        """A cancellation across the wire: `alpha` requests a capability whose handler parks on the
        cancel flag; `alpha`'s task is cancelled; the handler reports it was cancelled (the point of
        the row, not a side effect); `beta` answers the cancellation per *Envelope lifecycle*; both
        diariums end `cancelled`; nothing is armed on either side afterwards. Control: make `beta`
        ignore a cancellation for a running handler — the handler never wakes, the exchange never
        ends, and this test times out."""
        async with self.tree() as root:
            reported = {"cancelled": False}

            async def cancel_handler(params: Any, ctx: Any) -> dict[str, Any]:
                await ctx.cancelled.wait()  # park until the cancellation arrives
                reported["cancelled"] = True  # the handler saw the cancel: that is the row
                return {"text": "cancelled"}

            alpha, _ = await self.boot(root, confirm_timeout=0.2, cancel_wait_timeout=10.0)
            beta, _ = self.build_peer(
                root, caps={"cancel_echo": cancel_handler}, confirm_timeout=0.2
            )
            await beta.start()
            await self._see(alpha, "cancel_echo")
            task = asyncio.create_task(
                alpha.request(PEER, "cancel_echo", {"text": "x"}, timeout=30)
            )
            # The request is in `beta`'s inbox and the handler is parked.
            await wait_for(
                "the request in beta's inbox",
                lambda: any(p.name.endswith(".json") for p in inbox_dir(root, PEER).iterdir()),
                timeout=WATCH_SETTLE,
            )
            # Cancel `alpha`'s task: the background finisher writes the `.can.json`.
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            # The handler reported that it was cancelled.
            await wait_for(
                "the handler to report that it was cancelled",
                lambda: reported["cancelled"],
                timeout=WATCH_SETTLE,
            )
            # `beta` answered the cancellation; its `inreq` line ends `cancelled`.
            await wait_for(
                "beta's inreq line to end cancelled",
                lambda: any(
                    line["event"] == "inreq"
                    and cast(dict[str, Any], line["event_data"])["final_status"] == "cancelled"
                    for line in lines(root, PEER)
                ),
                timeout=WATCH_SETTLE,
            )
            # `alpha`'s `outreq` line ends `cancelled`.
            await wait_for(
                "alpha's outreq line to end cancelled",
                lambda: any(
                    line["event"] == "outreq"
                    and cast(dict[str, Any], line["event_data"])["final_status"] == "cancelled"
                    for line in lines(root, AGENT)
                ),
                timeout=WATCH_SETTLE,
            )
            # Nothing is armed on either side afterwards: the finisher ended, the inbox is empty.
            await wait_for(
                "the background finisher to end and beta's inbox to empty",
                lambda: (
                    not any(
                        not t.done() and t.get_name().startswith("actio.out.")
                        for t in asyncio.all_tasks()
                    )
                    and not list(inbox_dir(root, PEER).iterdir())
                ),
                timeout=WATCH_SETTLE,
            )

    async def test_a_command_across_the_wire(self) -> None:
        """A command across the wire: `alpha` sends `enable` to `beta` through the requestor's command
        path (not a hand-written file); `beta`'s spec gains `disabled` and then clears it, and `beta`'
        diarium carries the `inreq` lines with `final_status: "completed"` (D3). A disabled peer that
        accepts our commands stays in the view (*Callability rules*), so the second command reaches it."""
        async with self.tree() as root:
            alpha, _ = await self.boot(root, heartbeat_interval=1000)
            beta, _ = self.build_peer(root, heartbeat_interval=1000)
            await beta.start()
            await self._see(alpha)  # a peer that accepts our commands is in the view
            # First disable `beta`: its spec gains `disabled: true`.
            await alpha.command(PEER, "enable", {"enabled": False})
            await wait_for(
                "beta's spec to gain disabled",
                lambda: (
                    spec_path(root, PEER).exists()
                    and doc(spec_path(root, PEER)).get("disabled") is True
                ),
                timeout=WATCH_SETTLE,
            )
            # Then enable it: its spec clears `disabled`.
            await alpha.command(PEER, "enable", {"enabled": True})
            await wait_for(
                "beta's spec to clear disabled",
                lambda: doc(spec_path(root, PEER)).get("disabled") is not True,
                timeout=WATCH_SETTLE,
            )
            # `beta`'s diarium carries the `completed` `inreq` lines (D3), one per command.
            self.assertEqual(
                [
                    cast(dict[str, Any], line["event_data"])["final_status"]
                    for line in lines(root, PEER)
                    if line["event"] == "inreq"
                ],
                ["completed", "completed"],
            )

    async def test_a_foreign_command_is_dropped(self) -> None:
        """D4 across the wire: a command from a `src` not in `beta`'s `accepts_commands_from` is dropped
        without touching what peers see — `beta`'s `status.json`, `spec.json` and the announce file are
        byte-for-byte what they were, and the diarium carries the `inreq` line with `final_status:
        "dropped". The envelope is written by hand, as a foreign peer would write it."""
        async with self.tree() as root:
            alpha, _ = await self.boot(root, heartbeat_interval=1000)
            # `beta` accepts commands from `alpha` only: `gamma`'s command is the D4 foreign one.
            beta, _ = self.build_peer(root, accepts=[AGENT], heartbeat_interval=1000)
            await beta.start()
            await wait_for(
                "beta's spec, status and announce to be settled",
                lambda: (
                    spec_path(root, PEER).exists()
                    and status_path(root, PEER).exists()
                    and announce_path(root, PEER).exists()
                ),
                timeout=WATCH_SETTLE,
            )
            status_before = status_path(root, PEER).read_bytes()
            spec_before = spec_path(root, PEER).read_bytes()
            announce_before = announce_path(root, PEER).read_bytes()
            # A hand-written command envelope from `gamma` (not in `beta`'s `accepts_commands_from`).
            r = rid(7)
            write_file(
                inbox_dir(root, PEER) / f"{r}.json",
                cmd_file(r, "enable", {"enabled": False}, src="gamma", dst=PEER),
            )
            await wait_for(
                "the dropped inreq line",
                lambda: any(
                    line["event"] == "inreq"
                    and cast(dict[str, Any], line["event_data"])["final_status"] == "dropped"
                    for line in lines(root, PEER)
                ),
                timeout=WATCH_SETTLE,
            )
            self.assertEqual(status_path(root, PEER).read_bytes(), status_before)
            self.assertEqual(spec_path(root, PEER).read_bytes(), spec_before)
            self.assertEqual(announce_path(root, PEER).read_bytes(), announce_before)
