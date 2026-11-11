"""The file layer: the only module in `src` that touches the filesystem.

Every whole operation runs in `asyncio.to_thread`, so the loop never blocks on a disk write. There is
no module state: the sync provider of the agent is passed in, and its hooks are applied here rather
than in the callers. JSON is written compact (`separators=(",", ":")`, `ensure_ascii=False`,
`allow_nan=False`) and read with a `parse_constant` that raises, so `NaN` and `Infinity` from a peer
are an unparsable file, not a value.

The `delay` provider's pause is awaited **on the loop after the thread returns**, never slept inside
the thread: a `delay:10000` write would otherwise hold an executor thread for ten seconds, and a
burst of writes would starve every other disk operation. Nothing in this package sleeps; a pause is
always an `await`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
from collections.abc import Callable
from typing import Any

from ._types import SyncProvider
from ._util import nonce
from ._validate import LIMIT


def _reject_constant(name: str) -> Any:
    raise ValueError(f"an unparsable JSON constant from a peer: {name}")


# Nesting beyond this is unparsable: deep enough to hit the interpreter's recursion limit (or the
# C scanner's stack), never a value the protocol has. The pre-scan keeps the C scanner far from a
# stack overflow, which no exception would report.
MAX_DEPTH = 1000


def _depth_ok(text: str) -> bool:
    depth = 0
    in_string = False
    escaped = False
    for ch in text:
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
        elif ch == '"':
            in_string = True
        elif ch in "[{":
            depth += 1
            if depth > MAX_DEPTH:
                return False
        elif ch in "]}":
            depth -= 1
    return True


def dumps(obj: Any) -> str:
    """The one JSON writer of the protocol: compact, non-ASCII kept, no NaN or Infinity."""
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def loads(text: str) -> Any:
    """The one JSON reader: raises on NaN, Infinity and nesting too deep (D-15)."""
    if not _depth_ok(text):
        raise ValueError("a document nested too deep to parse")
    try:
        return json.loads(text, parse_constant=_reject_constant)
    except RecursionError as err:
        # A RecursionError is not a ValueError, and a caller must not see it (D-15).
        raise ValueError("a document nested too deep to parse") from err


def _fsync_dir(folder: str) -> None:
    try:
        fd = os.open(folder, os.O_RDONLY)
    except OSError:
        return  # not openable on some platforms
    try:
        os.fsync(fd)
    except OSError:
        pass  # a directory fsync is not supported on some platforms
    finally:
        os.close(fd)


def _post_write(target: str, provider: SyncProvider) -> None:
    """The synchronous half of a write's aftermath, in the same thread as the write."""
    if provider.fsync:
        _fsync_dir(os.path.dirname(target))
    provider.notify()


async def _after_write(op: Callable[[], None], provider: SyncProvider) -> None:
    """One disk operation in a thread, then the provider's pause awaited on the loop, not in the thread."""
    await asyncio.to_thread(op)
    if provider.delay_ms > 0:
        await asyncio.sleep(provider.delay_ms / 1000)


def unreadable_reason(err: OSError) -> str:
    """The one wording of a file that exists but cannot be read (I-7): one line, at most `LIMIT`, and
    never a value of the peer's. A schema rejection is worded by `_validate.reason`; this is the only
    place an I/O rejection is worded, because the reader is the thing that knows."""
    return f"unreadable: {err}".replace("\n", " ")[:LIMIT]


def _temp_in(tmp_dir: str, base: str) -> str:
    os.makedirs(tmp_dir, exist_ok=True)
    return os.path.join(tmp_dir, f".{base}.{nonce()}.tmp")


def _replace(tmp: str, target: str) -> None:
    try:
        os.replace(tmp, target)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp)  # a failed rename leaves no temp behind
        raise


async def read_json(path: str) -> Any:
    """The whole file as a value; None when it is missing or unparsable (*Files* rule 1.5)."""

    def _read() -> Any:
        try:
            with open(path, encoding="utf-8") as fh:
                return loads(fh.read())
        except (OSError, UnicodeDecodeError, ValueError):
            return None

    return await asyncio.to_thread(_read)


async def write_json_atomic(path: str, obj: Any, tmp_dir: str, provider: SyncProvider) -> None:
    """Temp file `.<base>.<nonce>.tmp` in `tmp_dir`, then `os.replace`; the provider's hooks follow."""

    def _write() -> None:
        tmp = _temp_in(tmp_dir, os.path.basename(path))
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(dumps(obj))
            if provider.fsync:
                fh.flush()
                os.fsync(fh.fileno())
        _replace(tmp, path)
        _post_write(path, provider)

    await _after_write(_write, provider)


async def write_bytes_atomic(path: str, data: bytes, tmp_dir: str, provider: SyncProvider) -> None:
    """An attachment's bytes, written once: temp file in `tmp_dir`, then `os.replace`.

    One operation, not a temp write plus a copy plus a remove: a half-taken attachment is what a peer
    reads back as a truncated file.
    """

    def _write() -> None:
        tmp = _temp_in(tmp_dir, os.path.basename(path))
        with open(tmp, "wb") as fh:
            fh.write(data)
            if provider.fsync:
                fh.flush()
                os.fsync(fh.fileno())
        _replace(tmp, path)
        _post_write(path, provider)

    await _after_write(_write, provider)


async def read_text_with_mtime(path: str) -> tuple[int, str] | None:
    """`(mtime_ns, text)` of one file in a single thread hop.

    None when the file (or a parent folder) is missing; it **raises** when the file is there but cannot
    be read or decoded, because a caller must report that as an ignored file with a reason (I-7) rather
    than lose it as a missing one. The dedup of D-11 needs the mtime and the text from one hop: a file
    read and then stat'ed can be a different file.
    """

    def _read() -> tuple[int, str] | None:
        try:
            with open(path, encoding="utf-8") as fh:
                text = fh.read()
            return os.stat(path).st_mtime_ns, text
        except (FileNotFoundError, NotADirectoryError):
            return None

    return await asyncio.to_thread(_read)


async def append_jsonl(path: str, obj: Any, provider: SyncProvider) -> None:
    """One `os.write` of one line on an `O_APPEND` descriptor (*File Format* rule 6)."""

    def _append() -> None:
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            os.write(fd, dumps(obj).encode("utf-8") + b"\n")
            if provider.fsync:
                os.fsync(fd)
        finally:
            os.close(fd)
        _post_write(path, provider)

    await _after_write(_append, provider)


async def copy_file(src: str, dst: str, tmp_dir: str, provider: SyncProvider) -> None:
    """An attachment: copy to a temp file in the destination's `tmp_dir`, then rename."""

    def _copy() -> None:
        tmp = _temp_in(tmp_dir, os.path.basename(dst))
        shutil.copyfile(src, tmp)
        if provider.fsync:
            fd = os.open(tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            except OSError:
                pass
            finally:
                os.close(fd)
        _replace(tmp, dst)
        _post_write(dst, provider)

    await _after_write(_copy, provider)


async def read_bytes(path: str) -> bytes:
    """An attachment's bytes, for `ctx.publish_attachment` and the CLI."""

    def _read() -> bytes:
        with open(path, "rb") as fh:
            return fh.read()

    return await asyncio.to_thread(_read)


async def list_dir(path: str) -> list[str]:
    """The entry names of a folder; raises `OSError` when it is gone (the caller decides)."""
    return await asyncio.to_thread(os.listdir, path)


async def make_dirs(path: str) -> None:
    """Create a folder and its parents; idempotent."""

    def _make() -> None:
        os.makedirs(path, exist_ok=True)

    await asyncio.to_thread(_make)


async def remove(path: str) -> None:
    """Remove a file or folder, ignoring a missing path (housekeeping, D-16)."""

    def _remove() -> None:
        if not os.path.lexists(path):
            return
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        else:
            os.remove(path)

    await asyncio.to_thread(_remove)
