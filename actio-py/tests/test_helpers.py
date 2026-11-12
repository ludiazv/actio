"""The instrument comes before the measurements: `no_leftovers` and the base test class, tested.

The three cases the rule names: a `to_thread` call leaves nothing of ours behind, a thread still running
fails, and a thread that reuses the name of one already alive fails too. The base class then fails a test
that leaves an agent running — a stub agent is enough — and passes one that stops it.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import threading
import unittest
import weakref

import helpers
from actio._files import io_hop


class NoLeftoversTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_to_thread_call_is_not_a_leftover(self) -> None:
        """asyncio parks an `asyncio_<n>` executor worker after the first `to_thread`; every agent writes
        through `to_thread`, so failing here would make every end-to-end test fail and invite loosening
        the check."""
        with helpers.no_leftovers():
            await asyncio.to_thread(lambda: None)
            await asyncio.to_thread(lambda: None)

    async def test_the_file_layer_s_pooled_workers_are_not_leftovers(self) -> None:
        """`_files.io_hop` parks its workers under `actio-io_<n>`, like asyncio's own `asyncio_<n>`: that
        pool is the one documented piece of module state, and it is the only name the check excludes
        besides asyncio's. The exclusion is shown to have something to exclude, and a thread we start
        ourselves keeps its name and is still found."""
        with helpers.no_leftovers():
            await io_hop(lambda: None)  # the first hop creates the pool; its worker parks
        pooled = {t for t in threading.enumerate() if t.name.startswith("actio-io")}
        self.assertTrue(pooled)  # the pool is real, so the exclusion is not vacuous
        self.assertFalse(pooled & helpers.our_threads())
        hold = threading.Event()
        try:
            with self.assertRaises(AssertionError) as got:
                with helpers.no_leftovers():
                    threading.Thread(target=hold.wait, name="one-of-ours", daemon=True).start()
                    await asyncio.sleep(0)
            self.assertIn("one-of-ours", str(got.exception))
        finally:
            hold.set()

    async def test_a_thread_still_running_fails(self) -> None:
        hold = threading.Event()
        try:
            with self.assertRaises(AssertionError) as got:
                with helpers.no_leftovers():
                    threading.Thread(target=hold.wait, name="leftover", daemon=True).start()
                    await asyncio.sleep(0)
            self.assertIn("leftover", str(got.exception))
        finally:
            hold.set()

    async def test_a_thread_that_reuses_a_name_fails(self) -> None:
        """Compared by name, a second thread called `same-name` hides behind the first and passes."""
        hold = threading.Event()
        threading.Thread(target=hold.wait, name="same-name", daemon=True).start()
        try:
            with self.assertRaises(AssertionError):
                with helpers.no_leftovers():
                    threading.Thread(target=hold.wait, name="same-name", daemon=True).start()
        finally:
            hold.set()

    async def test_a_task_still_armed_fails(self) -> None:
        hold = asyncio.Event()

        async def idle() -> None:
            await hold.wait()

        try:
            with self.assertRaises(AssertionError) as got:
                with helpers.no_leftovers():
                    asyncio.create_task(idle(), name="fake-agent-receiver")
                    await asyncio.sleep(0)
            self.assertIn("fake-agent-receiver", str(got.exception))
        finally:
            hold.set()
            await asyncio.sleep(0)


class _FakeAgent:
    """What an agent looks like to the base class: `start` arms a task and a watcher thread; `stop`
    takes both away. The base class cannot tell this apart from the real agent, which is the point."""

    def __init__(self) -> None:
        self._hold = asyncio.Event()
        self._thread_hold = threading.Event()
        self._task: asyncio.Task[None] | None = None
        self._thread: threading.Thread | None = None

    async def start(self) -> None:
        async def serve() -> None:
            await self._hold.wait()

        self._task = asyncio.create_task(serve(), name="fake-agent-receiver")
        self._thread = threading.Thread(
            target=self._thread_hold.wait, name="fake-watcher", daemon=True
        )
        self._thread.start()
        await asyncio.sleep(0)

    async def stop(self) -> None:
        assert self._task is not None and self._thread is not None
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._thread_hold.set()
        await asyncio.to_thread(self._thread.join)


def run_one(case: type[unittest.TestCase], name: str) -> unittest.TestResult:
    """Run one test of a throwaway class and hand back its result, for testing the base class."""
    result = unittest.TestResult()
    case(name).run(result)
    return result


class BaseClassTest(unittest.TestCase):
    def test_the_base_class_passes_a_test_that_stops_its_agent(self) -> None:
        class StopsIt(helpers.ActioTestCase):
            async def test_one_agent(self) -> None:
                agent = _FakeAgent()
                await agent.start()
                self.addAsyncCleanup(agent.stop)

        result = run_one(StopsIt, "test_one_agent")
        self.assertEqual(
            (result.failures, result.errors), ([], []), result.failures or result.errors
        )

    def test_the_base_class_fails_a_test_that_leaves_its_agent_running(self) -> None:
        class LeavesIt(helpers.ActioTestCase):
            async def test_one_agent(self) -> None:
                # On `self`, not a local: a real agent is kept alive by the cleanup that stops it, and a
                # task with no strong reference can vanish before the check sees it (that is the trap the
                # plan's hygiene grep exists for, and it showed up as a difference between 3.13 and 3.14).
                self.agent = _FakeAgent()
                await self.agent.start()

        result = run_one(LeavesIt, "test_one_agent")
        self.assertEqual(len(result.failures), 1, "a running agent must fail the base class")
        self.assertIn("left something armed", result.failures[0][1])
        self.assertIn("fake-agent-receiver", result.failures[0][1])
        self.assertIn("fake-watcher", result.failures[0][1])

    def test_the_baseline_keeps_its_tasks_alive(self) -> None:
        """Identity is only sound if the compared objects stay alive.

        A set of `id()`s lets a finished baseline task be freed and its address reused by a leftover,
        which then matches the baseline and hides — found on 3.13 under `-X dev`, where the reuse happened
        and the same test that passed on 3.14 reported no tasks at all. The helper therefore holds the
        tasks themselves, so this weak reference must survive the test.
        """
        held: list[bool] = []

        class HoldsBaseline(helpers.ActioTestCase):
            async def asyncSetUp(self) -> None:
                self.decoy = asyncio.create_task(asyncio.Event().wait(), name="baseline-task")
                await asyncio.sleep(0)
                await super().asyncSetUp()  # the decoy is in the baseline from here on
                self.ref = weakref.ref(self.decoy)
                del self.decoy

            async def test_nothing(self) -> None:
                gc.collect()
                held.append(self.ref() is not None)

        run_one(HoldsBaseline, "test_nothing")
        self.assertEqual(held, [True], "the baseline must hold its tasks, not their ids")
