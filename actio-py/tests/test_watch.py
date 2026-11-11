"""`_watch.py`: one observer, events that mean only "look again", the audit, and `scan_folder`.

Timeouts are generous on purpose: watchdog timing varies under load, and the suite runs eight times
at once at each milestone, where a flake is a failure.
"""

from __future__ import annotations

import asyncio
import os
import time
import unittest
from collections.abc import Coroutine
from typing import Any

from actio._watch import AUDIT_TASK, FolderWatch, Watcher, scan_folder
from helpers import ActioTestCase, actio_tree, no_leftovers

WAKE_TIMEOUT = 2.0


def spawn(coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
    return asyncio.get_running_loop().create_task(coro, name=name)


async def wake(sub: FolderWatch, timeout: float = WAKE_TIMEOUT) -> None:
    await asyncio.wait_for(sub.changed.wait(), timeout)
    sub.changed.clear()


class TwoFolderWakeTest(ActioTestCase):
    async def test_a_consumer_that_never_looks_cannot_stall_the_other_folder(self) -> None:
        with actio_tree() as root:
            a, b = root / "a", root / "b"
            a.mkdir()
            b.mkdir()
            watcher = await Watcher.open(spawn)
            try:
                sub_a = watcher.subscribe(str(a))
                sub_b = watcher.subscribe(str(b))
                await asyncio.sleep(0.05)  # let both watches arm
                sub_a.changed.set()  # folder a's consumer never looks: leave it set and unread
                (b / "x.json").write_text("{}", encoding="utf-8")
                await wake(sub_b)  # folder b's wake must not rest on folder a
            finally:
                await watcher.close()

    async def test_a_file_created_after_subscribe_wakes_the_subscriber(self) -> None:
        with actio_tree() as root:
            inbox = root / "inbox"
            inbox.mkdir()
            watcher = await Watcher.open(spawn)
            try:
                sub = watcher.subscribe(str(inbox))
                await asyncio.sleep(0.05)
                (inbox / "f.json").write_text("{}", encoding="utf-8")
                await wake(sub)
            finally:
                await watcher.close()

    async def test_two_subscribers_of_one_folder_both_wake(self) -> None:
        with actio_tree() as root:
            inbox = root / "inbox"
            inbox.mkdir()
            watcher = await Watcher.open(spawn)
            try:
                first = watcher.subscribe(str(inbox))
                second = watcher.subscribe(str(inbox))
                await asyncio.sleep(0.05)
                (inbox / "f.json").write_text("{}", encoding="utf-8")
                await wake(first)
                await wake(second)
            finally:
                await watcher.close()


class SubscribeTest(ActioTestCase):
    async def test_a_missing_folder_raises_file_not_found(self) -> None:
        with actio_tree() as root:
            watcher = await Watcher.open(spawn)
            try:
                with self.assertRaises(FileNotFoundError):
                    watcher.subscribe(str(root / "nope"))
            finally:
                await watcher.close()

    async def test_unsubscribe_is_safe_twice_after_a_loss_and_after_close(self) -> None:
        with actio_tree() as root:
            inbox = root / "inbox"
            inbox.mkdir()
            watcher = await Watcher.open(spawn, audit_seconds=0.2)
            sub = watcher.subscribe(str(inbox))
            watcher.unsubscribe(sub)
            watcher.unsubscribe(sub)  # safe twice
            os.rename(str(inbox), str(root / "inbox.old"))
            deadline = time.monotonic() + 2.0
            while sub.lost is None and time.monotonic() < deadline:
                try:
                    await asyncio.wait_for(sub.changed.wait(), 0.25)
                except TimeoutError:
                    pass
                sub.changed.clear()
            watcher.unsubscribe(sub)  # safe after a loss
            await watcher.close()
            watcher.unsubscribe(sub)  # safe after close


class AuditTest(ActioTestCase):
    async def test_the_audit_task_comes_through_spawn_by_name(self) -> None:
        calls: list[str] = []

        def named_spawn(coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
            calls.append(name)
            return spawn(coro, name)

        watcher = await Watcher.open(named_spawn)
        try:
            self.assertEqual(calls, [AUDIT_TASK])
            self.assertTrue(watcher._audit_task is not None)
        finally:
            await watcher.close()

    async def test_a_renamed_folder_is_found_by_the_audit(self) -> None:
        with actio_tree() as root:
            inbox = root / "inbox"
            inbox.mkdir()
            watcher = await Watcher.open(spawn, audit_seconds=0.2)
            try:
                sub = watcher.subscribe(str(inbox))
                await asyncio.sleep(0.05)
                os.rename(str(inbox), str(root / "inbox.old"))
                deadline = time.monotonic() + 2.0
                while sub.lost is None and time.monotonic() < deadline:
                    try:
                        await asyncio.wait_for(sub.changed.wait(), 0.25)
                    except TimeoutError:
                        pass
                    sub.changed.clear()
                self.assertIsInstance(sub.lost, OSError)
            finally:
                await watcher.close()

    async def test_a_deleted_folder_is_a_loss_too(self) -> None:
        with actio_tree() as root:
            inbox = root / "inbox"
            inbox.mkdir()
            watcher = await Watcher.open(spawn)
            try:
                sub = watcher.subscribe(str(inbox))
                await asyncio.sleep(0.05)
                os.rmdir(str(inbox))
                await wake(sub)  # the event itself reports the deletion
                self.assertIsInstance(sub.lost, OSError)
            finally:
                await watcher.close()


class CloseTest(ActioTestCase):
    async def test_close_leaves_no_thread_of_ours(self) -> None:
        with actio_tree() as root:
            inbox = root / "inbox"
            inbox.mkdir()
            with no_leftovers():
                watcher = await Watcher.open(spawn)
                watcher.subscribe(str(inbox))
                await asyncio.sleep(0.05)
                await watcher.close()


class ScanFolderTest(unittest.TestCase):
    def test_new_a_changed_size_and_a_removal(self) -> None:
        with actio_tree() as root:
            inbox = root / "inbox"
            inbox.mkdir()
            (inbox / "b.json").write_text("1", encoding="utf-8")
            (inbox / ".hidden").write_text("x", encoding="utf-8")
            (inbox / "sub").mkdir()
            first, markers = scan_folder(str(inbox), {})
            self.assertEqual(first, ["b.json"])  # dot files and sub-folders are not names
            self.assertEqual(set(markers), {"b.json"})

            (inbox / "a.json").write_text("2", encoding="utf-8")  # new
            (inbox / "b.json").write_text("33", encoding="utf-8")  # a changed size
            os.remove(inbox / "b.json")  # a removal
            changed, next_markers = scan_folder(str(inbox), markers)
            self.assertEqual(changed, ["a.json"])  # gone and unchanged names are not reported
            self.assertEqual(set(next_markers), {"a.json"})

    def test_the_changed_names_are_lexical(self) -> None:
        with actio_tree() as root:
            inbox = root / "inbox"
            inbox.mkdir()
            for name in ("z.json", "m.json", "a2.json"):
                (inbox / name).write_text("x", encoding="utf-8")
            changed, _ = scan_folder(str(inbox), {})
            self.assertEqual(changed, ["a2.json", "m.json", "z.json"])


if __name__ == "__main__":
    unittest.main()
