"""The helpers every later test uses (plan §9: `unittest`, one event loop, `-X dev -W error`).

`no_leftovers` is §5.2's rule as a check: after `stop`, nothing of ours is armed. Every end-to-end
scenario (WP9) and the lifecycle tests (WP7) use it. The default executor's parked worker threads are
excluded, as the plan says; watchdog's threads are not, because an agent that stopped should have none.
"""

from __future__ import annotations

import asyncio
import contextlib
import tempfile
import threading
from collections.abc import Iterator
from pathlib import Path


def thread_names() -> set[str]:
    """The threads alive now, minus the default executor's idle workers."""
    return {t.name for t in threading.enumerate() if not t.name.startswith("ThreadPoolExecutor")}


def task_names() -> set[str]:
    """The tasks running on the current loop other than the caller's own."""
    loop = asyncio.get_running_loop()
    me = asyncio.current_task(loop)
    return {t.get_name() for t in asyncio.all_tasks(loop) if t is not me}


@contextlib.contextmanager
def no_leftovers() -> Iterator[None]:
    """Fail if a task or a thread of ours is still armed when the block ends."""
    before_threads = thread_names()
    loop = asyncio.get_running_loop()
    before_tasks = {id(t) for t in asyncio.all_tasks(loop)}
    yield
    left_tasks = sorted(t.get_name() for t in asyncio.all_tasks(loop) if id(t) not in before_tasks)
    left_threads = sorted(thread_names() - before_threads)
    if left_tasks or left_threads:
        raise AssertionError(f"still armed after stop: tasks={left_tasks} threads={left_threads}")


@contextlib.contextmanager
def actio_tree() -> Iterator[Path]:
    """An empty temporary `.actio` root: the tests build the layout of *Folder Layout* on top of it."""
    with tempfile.TemporaryDirectory(prefix="actio-") as tmp:
        yield Path(tmp)
