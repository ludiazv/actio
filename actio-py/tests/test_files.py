"""`_files.py`: the one JSON writer and reader, the atomic write, the JSONL append, the small helpers.

The tests are rooted in `helpers.actio_tree()`. `NaN`/`Infinity` and a 20 000-deep document are
unparsable, not values; an atomic write leaves no temp behind; the JSONL append is one line each.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

from actio._files import (
    append_jsonl,
    copy_file,
    dumps,
    list_dir,
    loads,
    make_dirs,
    read_bytes,
    read_json,
    read_text_with_mtime,
    remove,
    unreadable_reason,
    write_bytes_atomic,
    write_json_atomic,
)
from actio._sync import create_sync_provider
from helpers import actio_tree, spawn


class DumpsTest(unittest.TestCase):
    def test_compact_output_and_non_ascii_kept(self) -> None:
        self.assertEqual(dumps({"a": [1, 2], "b": "é"}), '{"a":[1,2],"b":"é"}')

    def test_nan_and_infinity_refuse_to_be_written(self) -> None:
        with self.assertRaises(ValueError):
            dumps({"v": float("nan")})
        with self.assertRaises(ValueError):
            dumps(float("inf"))

    def test_a_tuple_is_written_as_an_array(self) -> None:
        self.assertEqual(dumps(("a", 1)), '["a",1]')
        self.assertEqual(dumps({"t": ("a", 1)}), '{"t":["a",1]}')

    def test_a_set_a_datetime_and_bytes_refuse_to_be_written(self) -> None:
        # Each raises `TypeError` by itself; the reason text is never used anywhere (it never
        # reaches a peer), so the tests pin the type of the raise, not its wording.
        for bad in ({"s": {1, 2}}, {"d": datetime.datetime(2026, 1, 1)}, {"b": b"x"}):
            with self.assertRaises(TypeError):
                dumps(bad)


class LoadsTest(unittest.TestCase):
    def test_nan_and_infinity_refuse_to_be_read(self) -> None:
        with self.assertRaises(ValueError):
            loads("NaN")
        with self.assertRaises(ValueError):
            loads("-Infinity")
        with self.assertRaises(ValueError):
            loads('{"v": Infinity}')
        self.assertEqual(loads('{"v": 1}'), {"v": 1})

    def test_twenty_thousand_deep_is_unparsable_not_a_recursion_error(self) -> None:
        deep = "[" * 20_000 + "]" * 20_000
        with self.assertRaises(ValueError):
            loads(deep)


class ReadJsonTest(unittest.IsolatedAsyncioTestCase):
    async def test_missing_and_unparsable_are_none(self) -> None:
        with actio_tree() as root:
            self.assertIsNone(await read_json(str(root / "nope.json")))
            (root / "broken.json").write_text("{", encoding="utf-8")
            self.assertIsNone(await read_json(str(root / "broken.json")))
            (root / "nan.json").write_text("NaN", encoding="utf-8")
            self.assertIsNone(await read_json(str(root / "nan.json")))
            (root / "deep.json").write_text("[" * 20_000 + "]" * 20_000, encoding="utf-8")
            self.assertIsNone(await read_json(str(root / "deep.json")))

    async def test_a_parsable_file_is_its_value(self) -> None:
        with actio_tree() as root:
            (root / "ok.json").write_text('{"v": 1}', encoding="utf-8")
            self.assertEqual(await read_json(str(root / "ok.json")), {"v": 1})


class AtomicWriteTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_successful_write_leaves_no_temp_in_tmp_dir(self) -> None:
        with actio_tree() as root:
            tmp = root / "tmp"
            target = root / "agents" / "a" / "status.json"
            target.parent.mkdir(parents=True)  # the folder layout is not the file layer's job
            await write_json_atomic(
                str(target), {"s": 1}, str(tmp), create_sync_provider("none", spawn=spawn)
            )
            self.assertEqual(json.loads(target.read_text(encoding="utf-8")), {"s": 1})
            self.assertEqual([p.name for p in tmp.iterdir() if p.name.endswith(".tmp")], [])

    async def test_the_temp_name_has_the_documented_shape(self) -> None:
        with actio_tree() as root:
            tmp = root / "tmp"
            target = root / "x.json"
            seen: list[str] = []
            real_replace = os.replace

            def spy(src: object, dst: object) -> None:
                seen.append(str(src))
                real_replace(src, dst)  # type: ignore[arg-type]

            with (
                mock.patch("actio._files.nonce", return_value="f" * 32),
                mock.patch("actio._files.os.replace", side_effect=spy),
            ):
                await write_json_atomic(
                    str(target), {}, str(tmp), create_sync_provider("none", spawn=spawn)
                )
            self.assertEqual(seen, [str(tmp / f".x.json.{'f' * 32}.tmp")])


class AppendJsonlTest(unittest.IsolatedAsyncioTestCase):
    async def test_two_appends_are_two_lines_with_a_trailing_newline(self) -> None:
        with actio_tree() as root:
            path = root / "nuntium.jsonl"
            provider = create_sync_provider("none", spawn=spawn)
            await append_jsonl(str(path), {"n": 1}, provider)
            await append_jsonl(str(path), {"n": 2}, provider)
            self.assertEqual(path.read_text(encoding="utf-8"), '{"n":1}\n{"n":2}\n')


class SmallHelpersTest(unittest.IsolatedAsyncioTestCase):
    async def test_copy_file_leaves_no_temp(self) -> None:
        with actio_tree() as root:
            src = root / "src.bin"
            src.write_bytes(b"the bytes")
            tmp = root / "tmp"
            dst = root / "dst.bin"
            await copy_file(str(src), str(dst), str(tmp), create_sync_provider("none", spawn=spawn))
            self.assertEqual(dst.read_bytes(), b"the bytes")
            self.assertEqual([p.name for p in tmp.iterdir() if p.name.endswith(".tmp")], [])

    async def test_read_bytes_reads_them(self) -> None:
        with actio_tree() as root:
            (root / "f.bin").write_bytes(b"\x00\x01")
            self.assertEqual(await read_bytes(str(root / "f.bin")), b"\x00\x01")

    async def test_list_dir_raises_when_the_folder_is_gone(self) -> None:
        with actio_tree() as root:
            with self.assertRaises(OSError):
                await list_dir(str(root / "nope"))

    async def test_make_dirs_is_idempotent(self) -> None:
        with actio_tree() as root:
            target = root / "a" / "b"
            await make_dirs(str(target))
            await make_dirs(str(target))
            self.assertTrue(target.is_dir())

    async def test_remove_on_a_missing_path_is_silent(self) -> None:
        with actio_tree() as root:
            await remove(str(root / "ghost"))
            gone = root / "f.json"
            gone.write_text("{}", encoding="utf-8")
            await remove(str(gone))
            self.assertFalse(gone.exists())


if __name__ == "__main__":
    unittest.main()


class SyncHooksTest(unittest.IsolatedAsyncioTestCase):
    """The provider's hooks: `fsync` in the thread, the `delay` pause on the loop after it."""

    async def test_the_delay_pause_does_not_hold_the_executor_thread(self) -> None:
        # One worker thread, and two writes with a 1.5 s pause each. A pause slept inside the thread
        # would serialise them: 3.0 s. A pause awaited on the loop: 1.5 s, and the thread is free.
        with actio_tree() as root:
            here = str(root)
            loop = asyncio.get_running_loop()
            provider = create_sync_provider("delay:1500", spawn=spawn)
            pool = ThreadPoolExecutor(max_workers=1)
            loop.set_default_executor(pool)
            try:
                start = time.monotonic()
                await asyncio.gather(
                    write_json_atomic(os.path.join(here, "a.json"), {"n": 1}, here, provider),
                    write_json_atomic(os.path.join(here, "b.json"), {"n": 2}, here, provider),
                )
                elapsed = time.monotonic() - start
            finally:
                pool.shutdown()  # its one worker is joined here, before any leftover check
                fresh = ThreadPoolExecutor()
                loop.set_default_executor(fresh)
                fresh.shutdown()  # nothing was submitted, so it created no thread
            self.assertLess(elapsed, 2.5, f"the pause held the thread: {elapsed:.2f}s")
            self.assertGreaterEqual(elapsed, 1.4, "the pause must still slow the write down")

    async def test_write_bytes_atomic_replaces_once_and_leaves_no_temp(self) -> None:
        with actio_tree() as root:
            here = str(root)
            target = os.path.join(here, "inbox", "part.dat")
            await make_dirs(os.path.dirname(target))
            await write_bytes_atomic(
                target, b"\x00\xff bytes", here, create_sync_provider("none", spawn=spawn)
            )
            with open(target, "rb") as fh:
                self.assertEqual(fh.read(), b"\x00\xff bytes")
            self.assertEqual([n for n in os.listdir(root) if n.endswith(".tmp")], [])

    async def test_read_text_with_mtime_tells_missing_from_unreadable(self) -> None:
        with actio_tree() as root:
            path = os.path.join(root, "inbox", "x.json")
            self.assertIsNone(await read_text_with_mtime(path))  # missing is not an error
            os.makedirs(os.path.dirname(path))
            with open(path, "w", encoding="utf-8") as fh:
                fh.write('{"a": 1}')
            got = await read_text_with_mtime(path)
            self.assertIsNotNone(got)
            mtime_ns, text = got or (0, "")
            self.assertEqual(text, '{"a": 1}')
            self.assertGreater(mtime_ns, 0)
            os.chmod(path, 0o000)
            try:
                await read_text_with_mtime(path)
            except OSError as err:
                reason = unreadable_reason(err)
                self.assertLessEqual(len(reason), 200)
                self.assertNotIn("\n", reason)
            else:
                self.fail("an unreadable file must raise, not read as missing")
            finally:
                os.chmod(path, 0o600)

    def test_the_unreadable_reason_is_one_line_and_bounded(self) -> None:
        reason = unreadable_reason(OSError("boom\nwith a newline and " + "x" * 400))
        self.assertLessEqual(len(reason), 200)
        self.assertNotIn("\n", reason)
        self.assertTrue(reason.startswith("unreadable: "))
