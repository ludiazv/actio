"""The file layer: the only module in `src` that touches the filesystem.

Every whole operation takes one thread hop on the protocol's own executor (`io_hop`), so the loop never
blocks on a disk write and a user handler's blocking work cannot starve a heartbeat, a response or a
scan. That pool is the one piece of module state in the package — marked where it is declared, and
named in the README; everything else is passed in — the sync provider of the agent and its hooks are applied here
rather than in the callers. JSON is written compact (`separators=(",", ":")`, `ensure_ascii=False`,
`allow_nan=False`) and read with a `parse_constant` that raises, so `NaN` and `Infinity` from a peer are
an unparsable file, not a value.

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
from collections.abc import Callable, Container, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from ._types import SyncProvider
from ._util import nonce
from ._validate import LIMIT, flat_reason

# The protocol's own thread pool, and the only one its I/O takes a hop on: semantics 6 tells a user to
# put blocking work in `asyncio.to_thread`, and sharing asyncio's default pool with that work makes a
# peer's answer wait behind a handler's job. Four workers, because these are short filesystem calls.
IO_WORKERS = 4
_io_pool: ThreadPoolExecutor | None = None  # module state


def _io() -> ThreadPoolExecutor:
    global _io_pool
    # On first use only, and from the loop thread: no lock, and no thread at import.
    if _io_pool is None:
        _io_pool = ThreadPoolExecutor(max_workers=IO_WORKERS, thread_name_prefix="actio-io")
    return _io_pool


async def io_hop[T](operation: Callable[..., T], *args: Any) -> T:
    """One thread hop on the pool above — the only way this package takes a thread.

    Its idle workers are the documented exception to "no thread of ours is armed after `stop`": like
    asyncio's own default pool, they park for reuse instead of dying, and the leftover check in
    `tests/helpers.py` excludes them by that prefix while every thread an agent starts is still found
    by identity.
    """
    return await asyncio.get_running_loop().run_in_executor(_io(), operation, *args)


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
    await io_hop(op)
    if provider.delay_ms > 0:
        await asyncio.sleep(provider.delay_ms / 1000)


def unreadable_reason(err: OSError) -> str:
    """The one wording of a file that exists but cannot be read (D-15): one line, at most `LIMIT`, and
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

    return await io_hop(_read)


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


async def read_text(path: str) -> str:
    """The whole file as text, in one thread hop; raises `OSError` when it is missing or unreadable and
    `UnicodeDecodeError` when it is not UTF-8 (a caller that ignores files decides which it reports)."""

    def _read() -> str:
        with open(path, encoding="utf-8") as fh:
            return fh.read()

    return await io_hop(_read)


async def mtime_ns(path: str) -> int:
    """A file's mtime in nanoseconds: the dedup key of D-11's once-per-file line. Raises `OSError` when
    the file is gone or unreadable, which is the caller's signal that there is nothing to key."""
    return (await io_hop(os.stat, path)).st_mtime_ns


async def read_text_with_mtime(path: str) -> tuple[int, str] | None:
    """`(mtime_ns, text)` of one file in a single thread hop.

    None when the file (or a parent folder) is missing; it **raises** when the file is there but cannot
    be read or decoded, because a caller must report that as an ignored file with a reason (D-15) rather
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

    return await io_hop(_read)


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

    return await io_hop(_read)


async def list_dir(path: str) -> list[str]:
    """The entry names of a folder; raises `OSError` when it is gone (the caller decides)."""
    return await io_hop(os.listdir, path)


# What one publication file came back as: `("text", the text)`, `("missing", "")`, `("unreadable",
# the reason)` or `("unparsable", the reason)`. A reason is one line and never a value of the peer's
# (D-15); a file that is readable but not UTF-8 is unparsable, and its reason is the flat one — the
# read itself succeeded, so the decoder's message stays out of a peer's file's report (I-9).
Publication = tuple[str, str]


def read_publication(path: str) -> Publication:
    """One file, reported rather than raised: the caller of a scan has to say what it skipped (D-15)."""
    try:
        with open(path, "rb") as handle:
            data = handle.read()
    except FileNotFoundError:
        return "missing", ""
    except OSError as err:
        return "unreadable", unreadable_reason(err)
    try:
        return "text", data.decode()
    except UnicodeDecodeError:
        return "unparsable", flat_reason()


def read_tree(
    root: str, files: Sequence[str], reserved: Container[str]
) -> dict[str, dict[str, Publication]]:
    """A whole discovery scan's reads in one call, so the scan is one thread hop: Python has no async
    file API, and a hop per file would be one per agent on a network mount. The listing of `root`, then
    `files` inside every entry that is a real folder, neither hidden nor
    `reserved`. The row keyed `""` is the root's own, where the team metadata lives; a missing file is a
    row too, so the caller never asks for it twice. A `root` that is gone raises `OSError`."""
    rows: dict[str, dict[str, Publication]] = {
        "": {name: read_publication(os.path.join(root, name)) for name in files}
    }
    for entry in sorted(os.listdir(root)):
        folder = os.path.join(root, entry)
        if entry.startswith(".") or entry in reserved or not os.path.isdir(folder):
            continue
        rows[entry] = {name: read_publication(os.path.join(folder, name)) for name in files}
    return rows


async def scan_tree(
    root: str, files: Sequence[str], reserved: Container[str]
) -> dict[str, dict[str, Publication]]:
    """`read_tree` in a thread: the file layer's one hop for a scan of the whole tree."""
    return await io_hop(read_tree, root, files, reserved)


async def make_dirs(path: str) -> None:
    """Create a folder and its parents; idempotent."""

    def _make() -> None:
        os.makedirs(path, exist_ok=True)

    await io_hop(_make)


async def remove(path: str) -> None:
    """Remove a file or folder, ignoring a missing path (housekeeping, D-16)."""

    def _remove() -> None:
        if not os.path.lexists(path):
            return
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        else:
            os.remove(path)

    await io_hop(_remove)
