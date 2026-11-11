"""Protocol validators, the one rejection reason, `check_params`, and capability schemas (D-15, D-19).

The schemas in `schemas/` are authoritative at run time: every protocol document is validated before it
is written and after it is read. A rejection is reported as **one line** built from the path and the
schema, never from the peer's value (D-15), because the reason reaches `IgnoredEvent`, `IgnoredFile`,
`IgnoredLine` and the diarium.

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
    """The one name inference, and it exists for the `actio validate` command (D-15).

    A loose file being validated has no body key of its own to go by, so the CLI reads the name:
    `spec.json`, `status.json`, `actio.json`, `diarium.jsonl`, `<id>.n.json` and `nuntium.json` are
    announces, `.can`/`.res`/`.con` mark the rest of an exchange, and any other `.json` is a request.
    The *receiver* classifies what it finds in a folder by its **body key** with `envelope_kind` and never
    by this function. A name is never authority to accept a document, and one inference of a kind from a
    name exists in this package so that a second copy cannot disagree with the first; `None` says the name
    tells us nothing."""
    raise NotImplementedError("WP2")


def reason(error: Any) -> str:
    """The one reason function (D-15): `(root) missing 'rid', 'src'`, `/status must be one of ...`.
    Never `error.message`, `error.instance` or `best_match`: those quote the peer's value."""
    raise NotImplementedError("WP2")


def malformed_reason(error: BaseException) -> str:
    """An inbox file that is not JSON: `malformed JSON: <the parser's error>` (D-15)."""
    raise NotImplementedError("WP2")


def validate(kind: SchemaKind, value: Any) -> str | None:
    """None when the document is valid; the one-line reason when it is not."""
    raise NotImplementedError("WP2")


def envelope_kind(value: Any) -> EnvelopeKind | None:
    """Which exchange envelope a decoded object is, by its single **body key** (*Requests*) — never its name.

    The four kinds that travel through an inbox: `request`, `cancellation`, `response`, `confirmation`.
    This is how a receiver classifies what it finds in a folder, and it is the protocol's own rule: names
    are "recommended but not enforced", the kind of a file comes from its body key, and the exchange is
    identified by `rid`. `None` means the document is not one of the four — ignore it, and say why."""
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
