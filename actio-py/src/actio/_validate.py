"""Protocol validators, the one rejection reason, `check_params`, and capability schemas (D-15, D-19). WP2.

The schemas in `schemas/` are authoritative at run time (G-5): every protocol document is validated
before it is written and after it is read. A rejection is reported as **one line** built from the path
and the schema, never from the peer's value (D-15), because the reason reaches `IgnoredEvent`,
`IgnoredFile`, `IgnoredLine` and the diarium. Start from the oracle's sketch at
`orchestra/oracle/sketches/lessons_fmt.py.txt`, which is probed over all 111 vectors and hostile values.

Capability schemas are per capability, on an empty `referencing.Registry()`: a peer's `$ref` can reach
out otherwise (D-19). A peer schema that will not run here skips the check and logs once; a schema that
is not ours to blame never fails the exchange.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any, Literal

from ._protocol import CancellationBody, Capability, Header, RequestBody, Spec

type SchemaKind = Literal[
    "actio",
    "announce",
    "cancellation",
    "confirmation",
    "diarium",
    "error",
    "request",
    "response",
    "spec",
    "status",
]
type EnvelopeKind = Literal["request", "cancellation", "response", "confirmation"]

LIMIT: int = 200  # D-15: one line of at most this many characters


def schema(kind: SchemaKind) -> dict[str, Any]:
    """One of the 11 protocol schemas, read from the package data (D-20) and cached."""
    raise NotImplementedError("WP2")


def file_kind(name: str) -> SchemaKind | None:
    """The one name inference of the protocol files, including announces (plan §6): `spec.json`,
    `status.json`, `actio.json`, `diarium.jsonl`, `<id>.n.json` and `nuntium.json` as announces,
    `.can`/`.res`/`.con`, and any other `.json` as a request."""
    raise NotImplementedError("WP2")


def reason(error: Any) -> str:
    """The one reason function (D-15): `(root) missing 'rid', 'src'`, `/status must be one of ...`.
    Never `error.message`, `error.instance` or `best_match`: those quote the peer's value."""
    raise NotImplementedError("WP2")


def malformed_reason(error: BaseException) -> str:
    """An inbox file that is not JSON: `malformed JSON: <the parser's error>` (D-15, v1.7)."""
    raise NotImplementedError("WP2")


def validate(kind: SchemaKind, value: Any) -> str | None:
    """None when the document is valid; the one-line reason when it is not (G-5)."""
    raise NotImplementedError("WP2")


def envelope_kind(value: Any) -> EnvelopeKind | None:
    """Which envelope a decoded object is, by its single body key (*Files* rule 2)."""
    raise NotImplementedError("WP2")


def same_header(a: Header, b: Header) -> bool:
    """D-11: every file of one exchange carries the same header, `datetime` excluded."""
    raise NotImplementedError("WP2")


def name_matches(req: RequestBody, can: CancellationBody) -> bool:
    """A cancellation names the request it cancels (*Cancellation files*)."""
    raise NotImplementedError("WP2")


def unique_capability_names(spec: Spec) -> str | None:
    """The duplicated name, or None: the schema says nothing about uniqueness across the list."""
    raise NotImplementedError("WP2")


def check_params(cap: Capability, params: Any) -> str | None:
    """Public: None when `params` satisfies the capability's `parameters` schema, else the reason."""
    raise NotImplementedError("WP2")


@dataclasses.dataclass(frozen=True, slots=True)
class CapabilityValidator:
    """One capability's compiled checks (D-19). A skipped check returns None and says so once."""

    parameters: Callable[[Any], str | None]
    result: Callable[[Any], str | None]
    skipped: frozenset[str]  # which of "parameters", "result" could not run here


def compile_capability(cap: Capability) -> CapabilityValidator:
    """Compile `parameters` and `result` for one capability, with an empty registry (D-19)."""
    raise NotImplementedError("WP2")
