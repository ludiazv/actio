"""The sync providers (D-21), parsed from `ACTIO_SYNC`: `none`, `delay[:<ms>]`, `fsync+delay[:<ms>]`,
`mutagen:<session>`.

The value's unit is **milliseconds** (it is the protocol's own storage knob, not an asyncio timer).
A delay must be an integer in 1-10 000; omitted means 50. A mutagen run never throws into protocol
code: failures go to `on_error`, which the agent wires to its log event. The mutagen binary comes
from `ACTIO_MUTAGEN_BIN` (default `mutagen`), read at run time.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Callable
from typing import Any

from ._types import SyncProvider

MIN_DELAY_MS = 1
MAX_DELAY_MS = 10_000
DEFAULT_DELAY_MS = 50
MUTAGEN_DEBOUNCE_MS = 200

_DELAY = re.compile(r"^(delay|fsync\+delay)(?::(\d+))?$")


def _delay_ms(value: str, raw: str | None) -> int:
    if raw is None:
        return DEFAULT_DELAY_MS
    ms = int(raw)  # the grammar allows digits only
    if ms < MIN_DELAY_MS or ms > MAX_DELAY_MS:
        raise ValueError(
            f"ACTIO_SYNC: invalid delay {value!r}: <ms> must be an integer between "
            f"{MIN_DELAY_MS} and {MAX_DELAY_MS}"
        )
    return ms


class _NopProvider:
    """`none` and the delay/fsync variants: their hooks live in the file layer, not here."""

    def __init__(self, fsync: bool, delay_ms: int) -> None:
        self.fsync = fsync
        self.delay_ms = delay_ms

    def notify(self) -> None:
        pass

    async def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class _MutagenProvider:
    """A debounced `mutagen sync flush <session>`; a failed run reaches `on_error` and nothing else.

    A flush request is never absorbed by the run already in flight: while a run is going, at most one
    follow-up run is promised, and `flush()` resolves only once a run that started after the call has
    finished, so the writes made during a flush are covered by the next one.
    """

    def __init__(self, session: str, on_error: Callable[[Exception], None] | None) -> None:
        self.fsync = False
        self.delay_ms = 0
        self._session = session
        self._on_error = on_error
        self._loop = asyncio.get_running_loop()  # the agent creates the provider at `start`
        self._timer: asyncio.TimerHandle | None = None
        self._running: asyncio.Task[None] | None = None
        self._pending: asyncio.Task[None] | None = None

    def notify(self) -> None:
        # The file layer calls this from a worker thread; the hop to the loop is the only work.
        self._loop.call_soon_threadsafe(self._schedule)

    def _schedule(self) -> None:
        if self._timer is not None:
            return  # the one debounced run covers the burst
        self._timer = self._loop.call_later(MUTAGEN_DEBOUNCE_MS / 1000, self._fire)

    def _fire(self) -> None:
        self._timer = None
        self._run()

    def _run(self) -> asyncio.Task[None]:
        if self._running is None:
            return self._spawn()
        if self._pending is None:
            in_flight = self._running
            assert in_flight is not None
            self._pending = self._loop.create_task(self._follow_up(in_flight))
        return self._pending

    async def _follow_up(self, in_flight: asyncio.Task[None]) -> None:
        await in_flight
        self._pending = None  # free the slot first: the follow-up becomes the new in-flight run
        self._running = self._spawn()
        await self._running

    def _spawn(self) -> asyncio.Task[None]:
        task = self._loop.create_task(self._flush_once())
        self._running = task

        def _settled(done: asyncio.Task[None]) -> None:
            if self._running is done:
                self._running = None

        task.add_done_callback(_settled)
        return task

    async def _flush_once(self) -> None:
        binary = os.environ.get("ACTIO_MUTAGEN_BIN") or "mutagen"
        try:
            proc = await asyncio.create_subprocess_exec(binary, "sync", "flush", self._session)
            code = await proc.wait()
            if code != 0:
                raise RuntimeError(f"mutagen sync flush exited with {code}")
        except Exception as err:
            if self._on_error is not None:
                try:
                    self._on_error(err)
                except Exception:
                    pass  # a failing on_error must not break the protocol

    async def flush(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None  # the forced run below covers the debounced one
        await self._run()

    def close(self) -> None:
        # Only the debounced run is cancelled: a promised follow-up still executes, because a caller
        # of `flush()` is already waiting on it. `close()` is idempotent.
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None


def create_sync_provider(
    value: str, on_error: Callable[[Exception], None] | None = None
) -> SyncProvider:
    """Parse one `ACTIO_SYNC` value; a bad value raises `ValueError` at `start()`, never silently."""
    if value == "none":
        return _NopProvider(False, 0)
    match = _DELAY.match(value)
    if match is not None:
        return _NopProvider(match.group(1) == "fsync+delay", _delay_ms(value, match.group(2)))
    if value.startswith("mutagen:"):
        session: Any = value.removeprefix("mutagen:")
        if session:
            return _MutagenProvider(session, on_error)
    raise ValueError(
        f"ACTIO_SYNC: unknown sync provider {value!r} (expected none, delay[:<ms>], "
        "fsync+delay[:<ms>] or mutagen:<session>)"
    )
