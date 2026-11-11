"""Time and identity primitives, as `../actio-ts/src/util.ts` has them (*Conventions*, *Envelope lifecycle*).

`format_iso` writes the protocol's timestamp at **whole-second** resolution: UTC,
`YYYY-MM-DDTHH:MM:SSZ`, the floor of the seconds, because that is what `common.schema.json`'s
`datetime` pattern allows — a fraction is rejected, not ignored — and it is what the reference does
(`toIso` floors, then strips the `.mmm` `toISOString` adds). `parse_iso` is as strict as `fromIso`: that
exact pattern or `None`, and a date that does not survive the round trip (`2026-02-30T00:00:00Z`) is
`None` too. Neither raises: reading a peer's file must not crash the agent (D-15).

Seconds, not milliseconds: every duration in this package is a float of seconds (*Timers and thresholds*), so these
two take and return seconds where `util.ts` takes and returns milliseconds.
"""

from __future__ import annotations

from typing import Final

ISO_PATTERN: Final = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"  # common.schema.json#/$defs/datetime
CROCKFORD: Final = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"  # the *rid* alphabet, upper case


def format_iso(seconds: float) -> str:
    """Whole seconds since the epoch to `YYYY-MM-DDTHH:MM:SSZ`; the fraction is floored away."""
    raise NotImplementedError("WP1")


def parse_iso(iso: object) -> float | None:
    """`YYYY-MM-DDTHH:MM:SSZ` to seconds; None for anything else, including a fraction or a dead date."""
    raise NotImplementedError("WP1")


def ulid() -> str:
    """A sortable, unique `rid`: 48 bits of milliseconds plus 80 random bits, Crockford base 32."""
    raise NotImplementedError("WP1")


def nonce() -> str:
    """A 128-bit random nonce as 32 hex characters (common.schema.json#/$defs/nonce)."""
    raise NotImplementedError("WP1")
