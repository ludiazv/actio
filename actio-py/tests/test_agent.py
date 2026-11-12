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
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast

import referencing
from jsonschema import Draft202012Validator

from actio import ActioAgent, Capability, CapabilityHandler, LogEvent
from actio._config import ActioConfig
from actio._protocol import DiariumEntry
from helpers import ActioTestCase, actio_tree

SETTLE = 6.0  # watchdog wake + debounce + a handler: a failure, not a timing expectation
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


# The schemas, read straight from the committed copy. They are not read through `actio._validate`
# here: that module loads them with `importlib.resources`, which imports `actio.schemas` and leaves
# a public `schemas` attribute on the package — the API surface test detects exactly that, and this
# file runs before it.
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


class AgentTest(ActioTestCase):
    """Builds an agent for `alpha` in a fresh tree; every test ends with it stopped.

    The base class fails the test if a task or a thread of ours is still armed, so a lifecycle test
    that leaks a heartbeat or a watcher thread cannot pass.
    """

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
        self.addAsyncCleanup(agent.stop)
        return agent, the_clock

    async def boot(self, root: Path, **kw: Any) -> tuple[ActioAgent, Clock]:
        agent, the_clock = self.build(root, **kw)
        await agent.start()
        return agent, the_clock


class SpineTest(AgentTest):
    async def test_the_spine_of_one_run(self) -> None:
        """*Workflow* steps 1-5 and C1, once: `start` writes a valid spec, an `idle` status and the
        announce (status first, with a delay); on exit `inactive` and the announce after it. The
        diarium carries the status lines. Asserted on the files and the diarium, not internal state."""
        with actio_tree() as root:
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
                self.assertEqual([line["event_data"] for line in lines(root)], ["idle"])
            # `__aexit__` ran C1: `inactive` first, then the announce (it rewrites the same file).
            self.assertEqual(doc(status_path(root))["status"], "inactive")
            self.assertEqual(doc(announce_path(root))["nuntium"], f"agent {AGENT} is inactive")
            self.assertLessEqual(
                status_path(root).stat().st_mtime_ns,
                announce_path(root).stat().st_mtime_ns,
            )
            self.assertEqual([line["event_data"] for line in lines(root)], ["idle", "inactive"])


class StopTest(AgentTest):
    async def test_three_in_flight_requests_are_abandoned_before_inactive(self) -> None:
        """D-9: what is in B2 is abandoned, and every owed `outreq` line is on disk before C1's
        `inactive` write — the outbound side is stopped (step 4) before the status (C1). The test's
        caller tasks are not the agent's: the stop tears the agent down, and the callers go away
        with it, as a torn-down handler would (the exchange ends correctly in the background)."""
        with actio_tree() as root:
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
        with actio_tree() as root:
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
        with actio_tree() as root:
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
        with actio_tree() as root:
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
        with actio_tree() as root:
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
        with actio_tree() as root:
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
        with actio_tree() as root:
            agent, _ = await self.boot(root)
            stopped = asyncio.create_task(agent.wait_stopped())
            await asyncio.sleep(0.1)
            self.assertFalse(stopped.done())
            await agent.stop()
            await asyncio.wait_for(stopped, SETTLE)

    async def test_fail_sets_error_and_announces(self) -> None:
        """*Agent's status*: `fail` is status `error` plus an announce; there is no recovery, and a
        second `fail` does not announce again."""
        with actio_tree() as root:
            agent, _ = await self.boot(root)
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
            mtime = announce_path(root).stat().st_mtime_ns
            await agent.fail("again")  # a no-op: already in error
            self.assertEqual(announce_path(root).stat().st_mtime_ns, mtime)
            self.assertEqual(doc(status_path(root))["status"], "error")

    async def test_a_lost_own_inbox_is_a_log_event_and_an_error_status(self) -> None:
        """D-22: a lost own inbox is fatal: log it and enter `error`, with the announce. The log is
        the observability event (the diarium keeps the `status` line), as the reference does."""
        with actio_tree() as root:
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


class RestartTest(AgentTest):
    async def test_a_restart_keeps_the_object_and_registrations_and_drops_disabled(self) -> None:
        """D-10: the agent object stays the same, `wait_stopped()` does not return, registrations
        survive, and a `disabled` set by `enable` does not: the fresh start rebuilds the spec."""
        with actio_tree() as root:
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
        with actio_tree() as root:
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
        with actio_tree() as root:
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
            self.assertIsNone(agent._restart_task)  # the delay is 10 s: it is not merely sleeping
