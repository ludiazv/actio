"""The file layer: the only module in `src` that touches the filesystem (§5.3). WP1.

Every whole operation runs in `asyncio.to_thread`, so the loop never blocks on a disk write. There is
no module state: the sync provider of the agent is passed in, and its hooks are applied here rather
than in the callers. JSON is written compact (`separators=(",", ":")`, `ensure_ascii=False`,
`allow_nan=False`) and read with a `parse_constant` that raises, so `NaN` and `Infinity` from a peer
are an unparsable file, not a value.
"""

from __future__ import annotations

from typing import Any

from ._types import SyncProvider


def dumps(obj: Any) -> str:
    """The one JSON writer of the protocol: compact, non-ASCII kept, no NaN or Infinity."""
    raise NotImplementedError("WP1")


def loads(text: str) -> Any:
    """The one JSON reader: raises on NaN, Infinity and nesting too deep (D-15, the traps table)."""
    raise NotImplementedError("WP1")


async def read_json(path: str) -> Any:
    """The whole file as a value; None when it is missing or unparsable (*Files* rule 1.5)."""
    raise NotImplementedError("WP1")


async def write_json_atomic(path: str, obj: Any, tmp_dir: str, provider: SyncProvider) -> None:
    """Temp file `.<base>.<nonce>.tmp` in `tmp_dir`, then `os.replace`; the provider's hooks follow."""
    raise NotImplementedError("WP1")


async def append_jsonl(path: str, obj: Any, provider: SyncProvider) -> None:
    """One `os.write` of one line on an `O_APPEND` descriptor (*File Format* rule 6)."""
    raise NotImplementedError("WP1")


async def copy_file(src: str, dst: str, tmp_dir: str, provider: SyncProvider) -> None:
    """An attachment: copy to a temp file in the destination's `tmp_dir`, then rename."""
    raise NotImplementedError("WP1")


async def read_bytes(path: str) -> bytes:
    """An attachment's bytes, for `ctx.publish_attachment` and the CLI."""
    raise NotImplementedError("WP1")


async def list_dir(path: str) -> list[str]:
    """The entry names of a folder; raises `OSError` when it is gone (the caller decides)."""
    raise NotImplementedError("WP1")


async def make_dirs(path: str) -> None:
    """Create a folder and its parents; idempotent."""
    raise NotImplementedError("WP1")


async def remove(path: str) -> None:
    """Remove a file or folder, ignoring a missing path (housekeeping, D-16)."""
    raise NotImplementedError("WP1")
