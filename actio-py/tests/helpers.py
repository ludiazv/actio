"""The helpers every later test uses: `unittest`, one event loop per test, `-X dev -W error`.

`no_leftovers` is the "after `stop`, nothing is armed" rule as a check: every end-to-end scenario and
every lifecycle test uses it. `ActioTestCase` makes it automatic, for the case that rule exists to
catch: a test that left an agent running raced its own temporary folder's cleanup and failed on one
machine and passed on another. The default executor's parked worker threads are not ours to stop — and
threads are compared as objects, never by name, because a name proves nothing.
"""

from __future__ import annotations

import asyncio
import contextlib
import tempfile
import threading
import unittest
from collections.abc import Coroutine, Iterator
from pathlib import Path
from typing import Any


def spawn(coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
    """The stand-in for the agent's `Spawn`: one named task per call, nothing else.

    Every seam that takes a `Spawn` is handed this in tests, so no test invents a second spawn shape.
    """
    return asyncio.get_running_loop().create_task(coro, name=name)


def our_threads() -> set[threading.Thread]:
    """The threads alive now, minus the default executor's workers, which asyncio names `asyncio_<n>`
    and parks for reuse after a `to_thread` call. Compared by identity, so a name proves nothing."""
    return {t for t in threading.enumerate() if not t.name.startswith("asyncio_")}


def task_names() -> set[str]:
    """The tasks running on the current loop other than the caller's own."""
    loop = asyncio.get_running_loop()
    me = asyncio.current_task(loop)
    return {t.get_name() for t in asyncio.all_tasks(loop) if t is not me}


@contextlib.contextmanager
def no_leftovers() -> Iterator[None]:
    """Fail if a task or a thread of ours is still armed when the block ends.

    Both sides are compared as objects, never by `id()` or by name: once the old object is freed, a new
    one can land on the same address, and then a leftover hides behind the baseline. Keeping the objects
    in the set is what makes identity sound.
    """
    before_threads = our_threads()
    loop = asyncio.get_running_loop()
    before_tasks = set(asyncio.all_tasks(loop))
    yield
    left_tasks = sorted(t.get_name() for t in asyncio.all_tasks(loop) if t not in before_tasks)
    left_threads = sorted(t.name for t in our_threads() - before_threads)
    if left_tasks or left_threads:
        raise AssertionError(f"still armed after stop: tasks={left_tasks} threads={left_threads}")


class ActioTestCase(unittest.IsolatedAsyncioTestCase):
    """The base class for anything that starts an agent: after the test, none of it is still running.

    The instrument is `no_leftovers`'s, armed in `asyncSetUp` and checked by a cleanup registered there,
    which runs after this test's own `addAsyncCleanup(agent.stop)` and before the loop is closed — the
    only moment at which a forgotten `stop` is still visible. A running agent keeps tasks armed (its
    heartbeat, its receiver) and a watcher thread, so it fails the check. Registering it as a cleanup,
    rather than overriding a hook, is what makes unittest *record* the failure: an exception raised from
    `tearDown`'s or `doCleanups`'s own body escapes the run instead.
    """

    async def asyncSetUp(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._threads_before = our_threads()
        # Objects, not `id()`s: see `no_leftovers`.
        self._tasks_before = set(asyncio.all_tasks(self._loop))
        self.addCleanup(self._assert_nothing_armed)  # added first, so it runs last

    def _assert_nothing_armed(self) -> None:
        left_tasks = sorted(
            t.get_name() for t in asyncio.all_tasks(self._loop) if t not in self._tasks_before
        )
        left_threads = sorted(t.name for t in our_threads() - self._threads_before)
        if left_tasks or left_threads:
            self.fail(f"a test left something armed: tasks={left_tasks} threads={left_threads}")


@contextlib.contextmanager
def actio_tree() -> Iterator[Path]:
    """An empty temporary `.actio` root: the tests build the layout of *Folder Layout* on top of it."""
    with tempfile.TemporaryDirectory(prefix="actio-") as tmp:
        yield Path(tmp)
