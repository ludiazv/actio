"""`_files.py`: the one JSON writer and reader, the atomic write, the JSONL append, the small helpers.

The tests are rooted in `helpers.actio_tree()`. `NaN`/`Infinity` and a 20 000-deep document are
unparsable, not values; an atomic write leaves no temp behind; the JSONL append is one line each.
"""

from __future__ import annotations

import datetime
import json
import os
import unittest
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
    remove,
    write_json_atomic,
)
from actio._sync import create_sync_provider
from helpers import actio_tree


class DumpsTest(unittest.TestCase):
    def test_compact_output_and_non_ascii_kept(self) -> None:
        self.assertEqual(dumps({"a": [1, 2], "b": "é"}), '{"a":[1,2],"b":"é"}')

    def test_nan_and_infinity_refuse_to_be_written(self) -> None:
        with self.assertRaises(ValueError):
            dumps({"v": float("nan")})
        with self.assertRaises(ValueError):
            dumps(float("inf"))

    def test_a_tuple_a_set_and_a_datetime_are_not_serializable(self) -> None:
        for bad in ({"t": (1, 2)}, {"s": {1, 2}}, {"d": datetime.datetime(2026, 1, 1)}):
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
            await write_json_atomic(str(target), {"s": 1}, str(tmp), create_sync_provider("none"))
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
                await write_json_atomic(str(target), {}, str(tmp), create_sync_provider("none"))
            self.assertEqual(seen, [str(tmp / f".x.json.{'f' * 32}.tmp")])


class AppendJsonlTest(unittest.IsolatedAsyncioTestCase):
    async def test_two_appends_are_two_lines_with_a_trailing_newline(self) -> None:
        with actio_tree() as root:
            path = root / "nuntium.jsonl"
            provider = create_sync_provider("none")
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
            await copy_file(str(src), str(dst), str(tmp), create_sync_provider("none"))
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
