"""`_status.py`: the status builder, the health scale, the guarded write, the latch, the heartbeat.

The publisher is driven directly — no agent exists above it yet. The guarded write is the rule that
two `to_thread` writes to one file must never land out of order and a write must survive its awaiter
being cancelled: the locked section is its own task from `spawn`, awaited through `asyncio.shield`,
and the latch makes every write after `inactive` a no-op. The 50-iteration cancel case runs that rule
against this publisher, not a toy.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import time
from collections.abc import Coroutine
from pathlib import Path
from typing import Any, cast

from actio._protocol import CapabilityStatusEntry, Spec, Status, StatusState
from actio._status import (
    ACTIO_VERSION,
    DEGRADED_SECONDS,
    HEALTHY_SECONDS,
    ONLINE_SECONDS,
    StatusPublisher,
    build_status,
    health,
    heartbeat,
)
from actio._sync import create_sync_provider
from actio._types import AgentFolders
from actio._util import format_iso
from actio._validate import validate
from helpers import ActioTestCase, actio_tree, no_leftovers

AGENT = "worker"
TS = 1_792_836_000.0  # 2026-10-24T10:00:00Z, the vectors' clock


def folders_for(root: Path) -> AgentFolders:
    agent = str(root / AGENT)
    return AgentFolders(
        root=str(root),
        agent=agent,
        inbox=f"{agent}/inbox",
        forum=str(root / "forum"),
        nuntium=str(root / "nuntium"),
    )


def spawn(coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
    """The stand-in for the agent's `Spawn`: one named task per call, nothing else."""
    return asyncio.get_running_loop().create_task(coro, name=name)


def load_spec() -> Spec:
    path = Path(__file__).parent / "vectors" / "spec" / "valid" / "01-minimal.json"
    return cast(Spec, json.loads(path.read_text(encoding="utf-8")))


def status_at(state: StatusState) -> Status:
    return {
        "id": AGENT,
        "status": state,
        "datetime": "2026-10-24T10:00:00Z",
        "capability_status": [],
    }


class BuildStatusTest(ActioTestCase):
    def test_it_validates_against_the_schema(self) -> None:
        self.assertIsNone(validate("status", build_status(AGENT, "idle", TS)))
        cs = [CapabilityStatusEntry(name="summarize", running=1, queue=2)]
        self.assertIsNone(validate("status", build_status(AGENT, "busy", TS, cs)))

    def test_capability_status_defaults_to_empty(self) -> None:
        self.assertEqual(build_status(AGENT, "idle", TS)["capability_status"], [])

    def test_the_datetime_is_the_whole_second_of_ts(self) -> None:
        self.assertEqual(build_status(AGENT, "idle", TS + 0.9)["datetime"], "2026-10-24T10:00:00Z")

    def test_the_version_constant(self) -> None:
        self.assertEqual(ACTIO_VERSION, "0.1")


class HealthTest(ActioTestCase):
    def test_the_rows_and_the_boundaries(self) -> None:
        t = 1_792_836_000.0  # the fixed datetime above, as seconds
        idle = status_at("idle")
        busy = status_at("busy")
        self.assertEqual(health(idle, t + 59.999), "healthy")
        self.assertEqual(health(idle, t + 60.0), "online")
        self.assertEqual(health(idle, t + 99.999), "online")
        self.assertEqual(health(idle, t + 100.0), "degraded")
        self.assertEqual(health(idle, t + 599.999), "degraded")
        self.assertEqual(health(idle, t + 600.0), "degraded")
        self.assertEqual(health(idle, t + 600.001), "zombie")
        self.assertEqual(health(busy, t + 60.0), "online")

    def test_inactive_and_error_have_no_health(self) -> None:
        for state in ("inactive", "error"):
            self.assertIsNone(health(status_at(state), 1_792_836_000.0))

    def test_a_future_datetime_is_the_freshest_possible(self) -> None:
        self.assertEqual(health(status_at("idle"), 1_792_835_940.0), "healthy")

    def test_an_unparsable_datetime_has_no_health(self) -> None:
        bad = status_at("idle")
        bad["datetime"] = "not a datetime"
        self.assertIsNone(health(bad, 1_792_836_000.0))

    def test_the_constants_are_the_protocol_rows(self) -> None:
        self.assertEqual(HEALTHY_SECONDS, 60.0)
        self.assertEqual(ONLINE_SECONDS, 100.0)
        self.assertEqual(DEGRADED_SECONDS, 600.0)


class PublisherTest(ActioTestCase):
    def test_latched_is_a_property(self) -> None:
        self.assertIsInstance(StatusPublisher.latched, property)

    async def test_publish_spec_writes_the_validated_spec(self) -> None:
        with actio_tree() as root:
            folders = folders_for(root)
            publisher = StatusPublisher(folders, create_sync_provider("none"), spawn)
            spec = load_spec()
            await publisher.publish_spec(spec)
            doc = json.loads((Path(folders.agent) / "spec.json").read_text(encoding="utf-8"))
            self.assertIsNone(validate("spec", doc))
            self.assertEqual(doc, spec)

    async def test_publish_status_writes_a_validated_status(self) -> None:
        with actio_tree() as root:
            folders = folders_for(root)
            publisher = StatusPublisher(folders, create_sync_provider("none"), spawn)
            await publisher.publish_status(build_status(AGENT, "idle", TS))
            doc = json.loads((Path(folders.agent) / "status.json").read_text(encoding="utf-8"))
            self.assertIsNone(validate("status", doc))
            self.assertEqual(doc["status"], "idle")

    async def test_an_invalid_status_raises_and_writes_nothing(self) -> None:
        with actio_tree() as root:
            folders = folders_for(root)
            publisher = StatusPublisher(folders, create_sync_provider("none"), spawn)
            bad = cast(
                Status,
                {
                    "id": AGENT,
                    "status": "asleep",
                    "datetime": format_iso(TS),
                    "capability_status": [],
                },
            )
            with self.assertRaises(ValueError):
                await publisher.publish_status(bad)
            self.assertFalse((Path(folders.agent) / "status.json").exists())

    async def test_an_invalid_spec_raises_and_writes_nothing(self) -> None:
        with actio_tree() as root:
            folders = folders_for(root)
            publisher = StatusPublisher(folders, create_sync_provider("none"), spawn)
            bad_spec = cast(Spec, {k: v for k, v in load_spec().items() if k != "id"})
            with self.assertRaises(ValueError):
                await publisher.publish_spec(bad_spec)
            self.assertFalse((Path(folders.agent) / "spec.json").exists())

    async def test_announce_writes_the_nuntium_file(self) -> None:
        with actio_tree() as root:
            folders = folders_for(root)
            publisher = StatusPublisher(folders, create_sync_provider("none"), spawn)
            await publisher.announce(AGENT, "agent is active", TS)
            path = Path(folders.nuntium) / f"{AGENT}.n.json"
            doc = json.loads(path.read_text(encoding="utf-8"))
            self.assertIsNone(validate("announce", doc))
            self.assertEqual(
                doc,
                {
                    "id": AGENT,
                    "datetime": "2026-10-24T10:00:00Z",
                    "ts": 1_792_836_000,
                    "nuntium": "agent is active",
                },
            )

    async def test_the_latch_makes_every_later_write_a_no_op(self) -> None:
        with actio_tree() as root:
            folders = folders_for(root)
            publisher = StatusPublisher(folders, create_sync_provider("none"), spawn)
            spec = load_spec()
            await publisher.publish_spec(spec)
            await publisher.publish_status(build_status(AGENT, "idle", TS))
            self.assertFalse(publisher.latched)
            await publisher.publish_status(build_status(AGENT, "inactive", TS))
            self.assertTrue(publisher.latched)
            status_path = Path(folders.agent) / "status.json"
            spec_path = Path(folders.agent) / "spec.json"
            announce_path = Path(folders.nuntium) / f"{AGENT}.n.json"
            # The latch gates *status* writes: not an exception, not a log line, a no-op. C1 writes
            # `inactive` and then the announce, and both are MUSTs, so the announce still lands.
            await publisher.publish_status(build_status(AGENT, "busy", TS))
            await publisher.publish_status(build_status(AGENT, "idle", TS))
            await publisher.publish_spec(spec)
            await publisher.announce(AGENT, "late news", TS)
            self.assertEqual(
                json.loads(status_path.read_text(encoding="utf-8"))["status"], "inactive"
            )
            self.assertEqual(json.loads(spec_path.read_text(encoding="utf-8")), spec)
            self.assertTrue(announce_path.exists(), "C1's announce must follow `inactive`")
            self.assertEqual(
                json.loads(announce_path.read_text(encoding="utf-8"))["nuntium"], "late news"
            )

    async def test_a_fresh_publisher_writes_again(self) -> None:
        with actio_tree() as root:
            folders = folders_for(root)
            publisher = StatusPublisher(folders, create_sync_provider("none"), spawn)
            await publisher.publish_status(build_status(AGENT, "inactive", TS))
            self.assertTrue(publisher.latched)
            status_path = Path(folders.agent) / "status.json"
            # A restart builds a fresh publisher, and with it a fresh latch.
            fresh = StatusPublisher(folders, create_sync_provider("none"), spawn)
            self.assertFalse(fresh.latched)
            await fresh.publish_status(build_status(AGENT, "idle", TS))
            self.assertEqual(json.loads(status_path.read_text(encoding="utf-8"))["status"], "idle")

    async def test_nothing_is_armed_after_use(self) -> None:
        with actio_tree() as root:
            folders = folders_for(root)
            publisher = StatusPublisher(folders, create_sync_provider("none"), spawn)
            with no_leftovers():
                await publisher.publish_status(build_status(AGENT, "idle", TS))
                await publisher.announce(AGENT, "agent is active", TS)
                await publisher.publish_status(build_status(AGENT, "inactive", TS))
                self.assertTrue(publisher.latched)


class GuardedWriteTest(ActioTestCase):
    async def test_a_cancelled_caller_cannot_leave_the_write_behind(self) -> None:
        for i in range(50):
            with actio_tree() as root:
                folders = folders_for(root)
                publisher = StatusPublisher(folders, create_sync_provider("none"), spawn)
                status_path = Path(folders.agent) / "status.json"

                async def beat(publisher: StatusPublisher = publisher) -> None:
                    while True:
                        await publisher.publish_status(build_status(AGENT, "idle", time.time()))
                        await asyncio.sleep(random.uniform(0.0, 0.002))

                t = asyncio.create_task(beat())
                await asyncio.sleep(random.uniform(0.001, 0.006))
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await t
                # C1: the stop's `inactive` write must be the last word on disk.
                await publisher.publish_status(build_status(AGENT, "inactive", time.time()))
                await asyncio.sleep(0.02)  # a straggler `to_thread` write gets its chance to land
                self.assertEqual(
                    json.loads(status_path.read_text(encoding="utf-8"))["status"],
                    "inactive",
                    f"iteration {i}: a cancelled tick's write landed after C1",
                )

    async def test_late_publishers_cannot_outwrite_the_inactive(self) -> None:
        for i in range(50):
            with actio_tree() as root:
                folders = folders_for(root)
                publisher = StatusPublisher(folders, create_sync_provider("none"), spawn)
                status_path = Path(folders.agent) / "status.json"
                idle = build_status(AGENT, "idle", time.time())
                before = [
                    asyncio.ensure_future(publisher.publish_status(idle))
                    for _ in range(random.randint(0, 3))
                ]
                c1 = asyncio.ensure_future(
                    publisher.publish_status(build_status(AGENT, "inactive", time.time()))
                )
                after = [
                    asyncio.ensure_future(publisher.publish_status(idle))
                    for _ in range(random.randint(1, 3))
                ]
                await asyncio.gather(*before, c1, *after)
                self.assertEqual(
                    json.loads(status_path.read_text(encoding="utf-8"))["status"],
                    "inactive",
                    f"iteration {i}: a late publisher outwrote the latch",
                )
                self.assertTrue(publisher.latched)

    async def test_a_slow_write_races_the_inactive_write(self) -> None:
        with actio_tree() as root:
            folders = folders_for(root)
            # A 50 ms sync provider: the tick's write is measurably in flight when C1 comes.
            publisher = StatusPublisher(folders, create_sync_provider("delay:50"), spawn)
            status_path = Path(folders.agent) / "status.json"

            async def beat() -> None:
                while True:
                    await publisher.publish_status(build_status(AGENT, "idle", time.time()))
                    await asyncio.sleep(0.01)

            t = asyncio.create_task(beat())
            await asyncio.sleep(0.04)  # a tick's write is in flight in the delay provider
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
            await publisher.publish_status(build_status(AGENT, "inactive", time.time()))
            self.assertEqual(
                json.loads(status_path.read_text(encoding="utf-8"))["status"], "inactive"
            )


class HeartbeatTest(ActioTestCase):
    async def test_ticks_never_overlap_and_the_interval_is_measured_from_the_end(self) -> None:
        loop = asyncio.get_running_loop()
        starts: list[float] = []
        ends: list[float] = []
        in_flight = 0
        peak = 0

        async def tick() -> None:
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            starts.append(loop.time())
            await asyncio.sleep(0.15)  # a slow tick, longer than the interval
            ends.append(loop.time())
            in_flight -= 1

        stopped = asyncio.Event()
        hb = asyncio.create_task(heartbeat(0.05, tick, stopped))
        await asyncio.sleep(0.85)
        stopped.set()
        with no_leftovers():
            await asyncio.wait_for(hb, 2.0)
        self.assertGreaterEqual(len(starts), 3)
        self.assertEqual(peak, 1)  # a tick starts only after the previous one finished
        for end, next_start in zip(ends, starts[1:], strict=False):
            self.assertGreaterEqual(next_start, end)
            self.assertGreaterEqual(
                next_start - end, 0.04
            )  # the interval, from the end of the tick
            self.assertLess(next_start - end, 0.2)

    async def test_the_loop_ends_on_stopped_and_leaves_nothing_armed(self) -> None:
        ticks = 0

        async def tick() -> None:
            nonlocal ticks
            ticks += 1

        stopped = asyncio.Event()
        hb = asyncio.create_task(heartbeat(0.05, tick, stopped))
        await asyncio.sleep(0.16)
        stopped.set()
        with no_leftovers():
            await asyncio.wait_for(hb, 2.0)
        self.assertGreaterEqual(ticks, 2)

    async def test_a_running_tick_is_never_cancelled_by_the_loop_ending(self) -> None:
        finished = False

        async def tick() -> None:
            nonlocal finished
            await asyncio.sleep(0.2)
            finished = True

        stopped = asyncio.Event()
        hb = asyncio.create_task(heartbeat(0.05, tick, stopped))
        await asyncio.sleep(0.1)  # the first tick is in flight (it started at ~0.05)
        stopped.set()
        with no_leftovers():
            await asyncio.wait_for(hb, 2.0)
        self.assertTrue(finished)  # the tick ran to completion

    async def test_the_first_tick_comes_after_one_interval(self) -> None:
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        first: list[float] = []

        async def tick() -> None:
            if not first:
                first.append(loop.time())

        stopped = asyncio.Event()
        hb = asyncio.create_task(heartbeat(0.2, tick, stopped))
        try:
            await asyncio.sleep(0.55)
        finally:
            stopped.set()
            await hb
        # The agent publishes the first status itself; the first tick is not before one interval.
        self.assertGreaterEqual(first[0] - t0, 0.19)
        self.assertLess(first[0] - t0, 0.4)
