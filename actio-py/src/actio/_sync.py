"""The sync providers (D-21), parsed from `ACTIO_SYNC`: `none`, `delay[:<ms>]`, `fsync+delay[:<ms>]`,
`mutagen:<session>`. WP1.

The value's unit is **milliseconds** (it is the protocol's own storage knob, not an asyncio timer).
A delay must be an integer in 1-10 000; omitted means 50. A mutagen run never throws into protocol
code: failures go to `on_error`, which the agent wires to its log event. The mutagen binary comes
from `ACTIO_MUTAGEN_BIN` (default `mutagen`), read at run time.
"""

from __future__ import annotations

from collections.abc import Callable

from ._types import SyncProvider

MIN_DELAY_MS = 1
MAX_DELAY_MS = 10_000
DEFAULT_DELAY_MS = 50
MUTAGEN_DEBOUNCE_MS = 200


def create_sync_provider(
    value: str, on_error: Callable[[Exception], None] | None = None
) -> SyncProvider:
    """Parse one `ACTIO_SYNC` value; a bad value raises `ValueError` at `start()`, never silently."""
    raise NotImplementedError("WP1")
