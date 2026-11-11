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

import datetime
import re
import secrets
import time
from typing import Final

# The exact pattern of common.schema.json#/$defs/datetime: [0-9], not \d, which would also match
# Unicode digits that the schema rejects.
ISO_PATTERN: Final = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
_ISO = re.compile(ISO_PATTERN)
CROCKFORD: Final = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"  # the *rid* alphabet, upper case


def format_iso(seconds: float) -> str:
    """Whole seconds since the epoch to `YYYY-MM-DDTHH:MM:SSZ`; the fraction is floored away."""
    whole = int(seconds // 1)
    moment = datetime.datetime.fromtimestamp(whole, tz=datetime.UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(iso: object) -> float | None:
    """`YYYY-MM-DDTHH:MM:SSZ` to seconds; None for anything else, including a fraction or a dead date."""
    if not isinstance(iso, str) or _ISO.fullmatch(iso) is None:
        return None
    try:
        moment = datetime.datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.UTC)
    except ValueError:
        return None  # a date the calendar does not have, like February 30
    if format_iso(moment.timestamp()) != iso:
        return None  # the round trip is the second gate, as in `fromIso`
    return moment.timestamp()


def ulid() -> str:
    """A sortable, unique `rid`: 48 bits of milliseconds plus 80 random bits, Crockford base 32."""
    stamp = int(time.time() * 1000) & 0xFFFFFFFFFFFF
    chars = [CROCKFORD[(stamp >> 45) & 0x7]]
    for shift in range(40, -5, -5):
        chars.append(CROCKFORD[(stamp >> shift) & 0x1F])
    bits = int.from_bytes(secrets.token_bytes(10), "big")
    for shift in range(75, -5, -5):
        chars.append(CROCKFORD[(bits >> shift) & 0x1F])
    return "".join(chars)


def nonce() -> str:
    """A 128-bit random nonce as 32 hex characters (common.schema.json#/$defs/nonce)."""
    return secrets.token_hex(16)
