"""The instrument comes before the measurements: `no_leftovers` and the base test class, tested.

The three cases the rule names: a `to_thread` call leaves nothing of ours behind, a thread still running
fails, and a thread that reuses the name of one already alive fails too. The base class then fails a test
that leaves an agent running — a stub agent is enough — and passes one that stops it.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import unittest

import helpers


class NoLeftoversTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_to_thread_call_is_not_a_leftover(self) -> None:
        """asyncio parks an `asyncio_<n>` executor worker after the first `to_thread`; every agent writes
        through `to_thread`, so failing here would make every end-to-end test fail and invite loosening
        the check."""
        with helpers.no_leftovers():
            await asyncio.to_thread(lambda: None)
            await asyncio.to_thread(lambda: None)

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
    takes both away. The real agent is WP7's; the check cannot tell them apart."""

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
                agent = _FakeAgent()
                await agent.start()

        result = run_one(LeavesIt, "test_one_agent")
        self.assertEqual(len(result.failures), 1, "a running agent must fail the base class")
        self.assertIn("left something armed", result.failures[0][1])
        self.assertIn("fake-agent-receiver", result.failures[0][1])
        self.assertIn("fake-watcher", result.failures[0][1])
