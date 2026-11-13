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
import ctypes
import ctypes.util
import os
import struct
import sys
import tempfile
import threading
import unittest
from collections.abc import Callable, Coroutine, Iterator
from pathlib import Path
from typing import Any


def spawn(coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
    """The stand-in for the agent's `Spawn`: one named task per call, nothing else.

    Every seam that takes a `Spawn` is handed this in tests, so no test invents a second spawn shape.
    """
    return asyncio.get_running_loop().create_task(coro, name=name)


# Two pools park their idle workers instead of letting them die: asyncio's default executor (`asyncio_<n>`,
# what a user's `to_thread` work runs on) and the file layer's own (`actio-io_<n>`, `_files.io_hop`, the
# one documented piece of module state). Their idle workers are not leftovers of an agent; every thread an
# agent starts is still found, because the comparison below is by identity, not by name.
POOLED = ("asyncio_", "actio-io")


def our_threads() -> set[threading.Thread]:
    """The threads alive now, minus the two pools' idle workers."""
    return {t for t in threading.enumerate() if not t.name.startswith(POOLED)}


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


# --- end-to-end helpers --------------------------------------------------------


async def until(what: str, predicate: Callable[[], bool], bound: float) -> None:
    """Poll until `predicate` is true, and never longer than `bound`: the failure names the
    bound and what was waited for. A test that reads the disk before the fact flakes; a test
    that waits without a bound hangs the run (no `Ran N tests` at all), so the bound is the
    row's failure, not the machine's."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + bound
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError(f"timed out after {bound}s waiting for: {what}")
        await asyncio.sleep(0.01)


async def bounded[T](
    waiter: Coroutine[Any, Any, T] | asyncio.Task[T], bound: float, what: str
) -> T:
    """Await `waiter` (a coroutine or a task) and end inside `bound`, raising naming the
    bound. The failure mode this exists for is a run that never returns: it prints no
    `Ran N tests`, so the row looks slow instead of wrong; a caller's outcome or an
    `agent.stop()` that a row leans on is wrapped here, so the row fails inside a stated
    bound."""
    task = asyncio.ensure_future(waiter)
    try:
        _, pending = await asyncio.wait({task}, timeout=bound)
    except asyncio.CancelledError:
        task.cancel()
        raise
    if pending:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise AssertionError(f"{what} had not finished within {bound}s")
    return task.result()


assert sys.platform == "linux", "the e2e suite needs inotify (Linux)"


# CPython's stdlib has no inotify: the two calls go through libc (the kernel ABI is
# `inotify_event { int wd; uint32 mask; uint32 cookie; uint32 len; char name[] }`), and
# the constants are <linux/inotify.h>'s.
_libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
_libc.inotify_init.argtypes = []
_libc.inotify_init.restype = ctypes.c_int
_libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
_libc.inotify_add_watch.restype = ctypes.c_int
_IN_CREATE = 0x100  # a file was created in the watched directory
_IN_MOVED_TO = 0x80  # a file was moved into the watched directory


def _inotify_init() -> int:
    fd = int(_libc.inotify_init())
    if fd < 0:
        raise OSError(ctypes.get_errno(), "inotify_init")
    return fd


def _inotify_add_watch(fd: int, path: str, mask: int) -> int:
    wd = int(_libc.inotify_add_watch(fd, path.encode("utf-8"), mask))
    if wd < 0:
        raise OSError(ctypes.get_errno(), "inotify_add_watch")
    return wd


_KIND_SUFFIXES: dict[str, str] = {
    ".json": "req",
    ".res.json": "res",
    ".can.json": "can",
    ".con.json": "con",
}


class InboxRecorder:
    """The files that appeared in one inbox, in the order they appeared.

    An inotify watch, not a poll: an appearance is a kernel event (`IN_CREATE` for a direct
    write, `IN_MOVED_TO` for the library's atomic temp-file + rename), so no file can vanish
    from the record in the milliseconds it exists — a `con` that the archive deletes a beat
    after it lands is still seen. Disappearances are asserted on disk, never from this record:
    the event stream orders appearances, and a file that is gone before the next read is gone.
    """

    def __init__(self, inbox: Path) -> None:
        self._inbox = inbox
        self._order: list[str] = []  # names, in kernel event order
        self._buf = b""  # the unparsed tail: a read can end mid-event
        self._lock = threading.Lock()
        self._fd: int | None = None
        self._wfd: int | None = None  # a pipe that wakes the watcher's select
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        fd = _inotify_init()
        os.set_blocking(fd, False)  # a read is only ever done when select (or the stop)
        # says events are due; the final drain after the stop must not block
        _inotify_add_watch(fd, str(self._inbox), _IN_CREATE | _IN_MOVED_TO)
        r, w = os.pipe()
        os.set_blocking(r, False)
        self._fd, self._wfd = fd, w
        self._thread = threading.Thread(target=self._run, args=(fd, r), name="e2e.inbox-recorder")
        self._thread.start()

    def _run(self, fd: int, wake_r: int) -> None:
        import select

        while True:
            ready, _, _ = select.select([fd, wake_r], [], [])
            if wake_r in ready:
                os.read(wake_r, 1)
                break
            if fd in ready:
                self._drain(fd)
        self._drain(fd)  # the last events before the fd closes

    def _drain(self, fd: int) -> None:
        while True:
            try:
                data = os.read(fd, 65536)
            except BlockingIOError:
                break  # nothing due: select (or the stop's last drain) called it
            self._buf += data
            while len(self._buf) >= 16:
                _, mask, _, name_len = struct.unpack_from("iIII", self._buf, 0)
                total = 16 + name_len
                if len(self._buf) < total:
                    break  # a partial event: the rest arrives with the next read
                name = self._buf[16:total].rstrip(b"\0").decode("utf-8")
                self._buf = self._buf[total:]
                with self._lock:
                    if mask & (_IN_CREATE | _IN_MOVED_TO) and name not in self._order:
                        self._order.append(name)

    async def stop(self) -> None:
        if self._wfd is not None:
            os.write(self._wfd, b"\x00")
            self._wfd = None
        thread, self._thread = self._thread, None
        if thread is not None:
            await asyncio.to_thread(thread.join, 5.0)
            if thread.is_alive():
                raise AssertionError("the inbox recorder did not stop within 5 s")
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

    def _names(self) -> list[str]:
        with self._lock:
            return list(self._order)

    def rids(self) -> list[str]:
        """The distinct rids seen, in first-appearance order (a ULID carries no dots)."""
        out: list[str] = []
        for name in self._names():
            rid = name.split(".", 1)[0]
            if rid and rid not in out:
                out.append(rid)
        return out

    def appeared(self, rid: str) -> list[str]:
        """One rid's envelope kinds, in appearance order: `req`, `res`, `can`, `con`."""
        out: list[str] = []
        for name in self._names():
            for suffix, kind in _KIND_SUFFIXES.items():
                if name == rid + suffix:
                    out.append(kind)
                    break
        return out
