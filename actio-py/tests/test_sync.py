"""`_sync.py`: the whole grammar, the bounds, and a mutagen that never throws into protocol code.

The mutagen tests need no mutagen: `ACTIO_MUTAGEN_BIN` points at a path that cannot run, or at a
throwaway script that records its runs.
"""

from __future__ import annotations

import asyncio
import os
import stat
import time
import unittest
from unittest import mock

from actio._files import write_json_atomic
from actio._sync import (
    DEFAULT_DELAY_MS,
    MAX_DELAY_MS,
    MIN_DELAY_MS,
    MUTAGEN_DEBOUNCE_MS,
    create_sync_provider,
)
from helpers import actio_tree, spawn


class GrammarTest(unittest.TestCase):
    def test_the_values_and_what_they_parse_to(self) -> None:
        cases = {
            "none": (False, 0),
            "delay": (False, DEFAULT_DELAY_MS),
            "delay:1": (False, MIN_DELAY_MS),
            "delay:10000": (False, MAX_DELAY_MS),
            "fsync+delay": (True, DEFAULT_DELAY_MS),
            "fsync+delay:250": (True, 250),
        }
        for value, (fsync, delay_ms) in cases.items():
            provider = create_sync_provider(value, spawn=spawn)
            self.assertEqual(provider.fsync, fsync, value)
            self.assertEqual(provider.delay_ms, delay_ms, value)
            provider.close()

    def test_every_malformed_spelling_raises_value_error(self) -> None:
        for value in (
            "delay:0",
            "delay:10001",
            "delay:abc",
            "delay:5.5",
            "mutagen:",
            "fsync",
            "sync",
            "",
        ):
            with self.assertRaises(ValueError, msg=value):
                create_sync_provider(value, spawn=spawn)

    def test_none_does_no_fsync_and_no_delay(self) -> None:
        provider = create_sync_provider("none", spawn=spawn)
        self.assertFalse(provider.fsync)
        self.assertEqual(provider.delay_ms, 0)
        provider.close()


class DelayWaitsTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_delay_actually_waits_about_that_long(self) -> None:
        provider = create_sync_provider("delay:80", spawn=spawn)
        with actio_tree() as root:
            start = time.monotonic()
            await write_json_atomic(str(root / "a.json"), {}, str(root / "tmp"), provider)
            elapsed = time.monotonic() - start
        provider.close()
        self.assertGreaterEqual(elapsed, 0.08)


class MutagenTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_run_that_cannot_start_reaches_on_error_and_escapes_nothing(self) -> None:
        errors: list[Exception] = []
        provider = create_sync_provider("mutagen:test-session", on_error=errors.append, spawn=spawn)
        with mock.patch.dict(os.environ, {"ACTIO_MUTAGEN_BIN": "/nonexistent/actio-mutagen"}):
            await provider.flush()  # must not raise
            provider.close()
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], OSError)

    async def test_two_notifies_before_the_debounce_are_one_run(self) -> None:
        with actio_tree() as root:
            log = root / "runs.log"
            fake = root / "mutagen"
            fake.write_text(f"#!/bin/sh\necho run >> {log}\n", encoding="utf-8")
            fake.chmod(fake.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            provider = create_sync_provider("mutagen:s", spawn=spawn)
            with mock.patch.dict(os.environ, {"ACTIO_MUTAGEN_BIN": str(fake)}):
                provider.notify()
                provider.notify()
                await asyncio.sleep(0.05)  # well under the debounce
                self.assertFalse(log.exists())  # nothing before the debounce
                await provider.flush()
            # `flush` awaits the run, which awaits the process: the line is on disk by now.
            self.assertEqual(log.read_text(encoding="utf-8").splitlines(), ["run"])
            provider.close()

    async def test_the_debounce_fires_a_single_run(self) -> None:
        with actio_tree() as root:
            log = root / "runs.log"
            fake = root / "mutagen"
            fake.write_text(f"#!/bin/sh\necho run >> {log}\n", encoding="utf-8")
            fake.chmod(fake.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            provider = create_sync_provider("mutagen:s", spawn=spawn)
            with mock.patch.dict(os.environ, {"ACTIO_MUTAGEN_BIN": str(fake)}):
                provider.notify()
                provider.notify()
                provider.notify()
                await asyncio.sleep(MUTAGEN_DEBOUNCE_MS / 1000 + 1.0)
            self.assertEqual(log.read_text(encoding="utf-8").splitlines(), ["run"])
            provider.close()


if __name__ == "__main__":
    unittest.main()
