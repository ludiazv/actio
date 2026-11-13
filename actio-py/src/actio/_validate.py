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
import json
import logging
from collections.abc import Callable
from importlib import resources
from typing import Any, Literal, cast

import referencing
from jsonschema.validators import Draft202012Validator

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

_log = logging.getLogger("actio.validate")

# The eleven schema files. `common` is package data, not a kind: no file in the tree is a common
# document, but every other schema reaches its `$defs` through `$ref`s.
_SCHEMA_FILES: tuple[str, ...] = (
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
    "common",
)

_schemas: dict[str, dict[str, Any]] = {}
_validators: dict[str, Draft202012Validator] = {}
_registry: referencing.Registry | None = None
_peer_skips_logged: set[tuple[str, str]] = set()


def _schema_registry() -> referencing.Registry:
    """The one registry holding all eleven schema files, built once and reused (D-20).

    A `$ref` written as `#/$defs/rid` in `request.schema.json` is sent by the base-URI rule to
    `common.schema.json#/$defs/rid`; the registry is what makes that cross-file hop resolve."""
    global _registry
    if _registry is None:
        # `actio.schemas` is a folder of data, not a module to import: importing it would leave a
        # public `schemas` attribute on the package (tests/test_api.py's surface rule). The pack is
        # read through the package that owns it.
        folder = resources.files("actio").joinpath("schemas")
        reg = referencing.Registry()
        for name in _SCHEMA_FILES:
            # The schema pack is package data, read once: not a file of the tree (D-20).
            pack = folder.joinpath(f"{name}.schema.json").read_text(encoding="utf-8")  # schema pack
            schema = json.loads(pack)
            _schemas[f"{name}.schema.json"] = schema
            reg = reg.with_resource(schema["$id"], referencing.Resource.from_contents(schema))
        _registry = reg
    return _registry


def schema(kind: SchemaKind) -> dict[str, Any]:
    """One of the ten protocol schemas, read from the package data (D-20) and cached.

    `common.schema.json` is package data too, but not a kind: no file in the tree is a common
    document. It is loaded into the registry as well, because the other schemas reach its `$defs`."""
    _schema_registry()
    return _schemas[f"{kind}.schema.json"]


def _validator(kind: SchemaKind) -> Draft202012Validator:
    """The compiled validator of one kind, cached per kind on the shared registry."""
    if kind not in _validators:
        _validators[kind] = Draft202012Validator(schema(kind), registry=_schema_registry())
    return _validators[kind]


def validate(kind: SchemaKind, value: Any) -> str | None:
    """None when the document is valid; the one-line reason when it is not.

    The reason is for the first error of `iter_errors` (D-15)."""
    first = next(iter(_validator(kind).iter_errors(value)), None)
    return reason(first) if first is not None else None


def file_kind(name: str) -> SchemaKind | None:
    """The one name inference, and it exists for the `actio validate` command.

    A loose file being validated has no body key of its own to go by, so the CLI reads the name:
    `spec.json`, `status.json`, `actio.json`, `diarium.jsonl`, `<id>.n.json` and `nuntium.json` are
    announces, `.can`/`.res`/`.con` mark the rest of an exchange, and any other `.json` is a request.
    The *receiver* classifies what it finds in a folder by its **body key** with `envelope_kind` and
    never by this function. A name is never authority to accept a document, and one inference of a
    kind from a name exists in this package so that a second copy cannot disagree with the first;
    `None` says the name tells us nothing."""
    base = name.replace("\\", "/").rsplit("/", 1)[-1]
    if base in _EXACT_NAMES:
        return _EXACT_NAMES[base]
    if base.endswith(".n.json"):
        return "announce"
    for suffix, kind in _SUFFIX_KINDS.items():
        if base.endswith(suffix):
            return kind
    if base.endswith(".json"):
        return "request"
    return None


_EXACT_NAMES: dict[str, SchemaKind] = {
    "spec.json": "spec",
    "status.json": "status",
    "actio.json": "actio",
    "diarium.jsonl": "diarium",
    "nuntium.json": "announce",
}
_SUFFIX_KINDS: dict[str, SchemaKind] = {
    ".can.json": "cancellation",
    ".res.json": "response",
    ".con.json": "confirmation",
}


def reason(error: Any) -> str:
    """The one reason function (D-15): `(root) missing 'rid', 'src'`, `/status must be one of ...`.

    Built from the instance path and the schema keyword, never from `error.message`, `error.instance`
    or `best_match`: those quote the peer's value. A path segment can be a peer's key, so it is
    escaped with `json.dumps`, which keeps the reason on one line."""
    path = error.absolute_path
    where = "(root)" if not path else "/" + "/".join(json.dumps(str(part))[1:-1] for part in path)
    keyword = error.validator
    value = error.validator_value
    match keyword:
        case "required":
            what = "missing " + ", ".join(repr(k) for k in value if k not in error.instance)
        case "enum":
            what = "must be one of " + ", ".join(repr(k) for k in value)
        case "const":
            what = f"must be {value!r}"
        case "type":
            what = f"must be of type {value}"
        case "additionalProperties" | "unevaluatedProperties":
            what = "has a property that is not allowed"
        case _:
            what = f"fails {keyword!r}"
    return f"{where} {what}"[:LIMIT]


def malformed_reason(error: BaseException) -> str:
    """An inbox file that is not JSON: `malformed JSON: <the parser's error>` (D-15).

    The parser's error names positions, never the value: a 100 KB input comes back as
    `Expecting value: line 1 column 1 (char 0)`, nothing more."""
    return f"malformed JSON: {error}"


def flat_reason() -> str:
    """The flat reason: an unparsable publication in a scan, and an unparsable diarium line —
    `not valid JSON`, without the parser's detail. The detail belongs to the inbox, where the
    file is one of the exchange's own (`malformed_reason`); a peer's file is named, not quoted
    (../actio-ts/src/discovery.ts:101, ../actio-ts/src/diarium.ts:100)."""
    return "not valid JSON"


def envelope_kind(value: Any) -> EnvelopeKind | None:
    """Which exchange envelope a decoded object is, by its single **body key** (*Requests*) — never its name.

    The four kinds that travel through an inbox: `request`, `cancellation`, `response`, `confirmation`.
    This is how a receiver classifies what it finds in a folder, and it is the protocol's own rule: names
    are "recommended but not enforced", the kind of a file comes from its body key, and the exchange is
    identified by `rid`. `None` means the document is not one of the four — ignore it, and say why."""
    if not isinstance(value, dict):
        return None
    present = [
        key for key in ("request", "cancellation", "response", "confirmation") if key in value
    ]
    return cast(EnvelopeKind, present[0]) if len(present) == 1 else None


def same_header(a: Header, b: Header) -> bool:
    """D-11: every file of one exchange carries the same header, `datetime` excluded."""
    return all(a[key] == b[key] for key in ("rid", "src", "dst", "type", "actio_version"))


def name_matches(req: RequestBody, can: CancellationBody) -> bool:
    """A cancellation names the request it cancels (*Cancellation files*)."""
    return req["name"] == can["name"]


def unique_capability_names(spec: Spec) -> str | None:
    """The duplicated name, or None: the schema says nothing about uniqueness across the list."""
    seen: set[str] = set()
    for cap in spec["capabilities"]:
        if cap["name"] in seen:
            return cap["name"]
        seen.add(cap["name"])
    return None


# The JSON Schema keywords of the 2020-12 dialect, and the ones whose values are schemas themselves.
_KNOWN_KEYWORDS: frozenset[str] = frozenset(
    {
        "$schema",
        "$id",
        "$ref",
        "$defs",
        "$anchor",
        "$dynamicRef",
        "$dynamicAnchor",
        "$comment",
        "type",
        "enum",
        "const",
        "allOf",
        "anyOf",
        "oneOf",
        "not",
        "if",
        "then",
        "else",
        "properties",
        "patternProperties",
        "additionalProperties",
        "unevaluatedProperties",
        "propertyNames",
        "items",
        "prefixItems",
        "contains",
        "unevaluatedItems",
        "required",
        "dependentRequired",
        "dependentSchemas",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
        "pattern",
        "minItems",
        "maxItems",
        "uniqueItems",
        "minContains",
        "maxContains",
        "minProperties",
        "maxProperties",
        "title",
        "description",
        "default",
        "examples",
        "format",
        "contentMediaType",
        "contentEncoding",
        "readOnly",
        "writeOnly",
        "deprecated",
    }
)
_SCHEMA_VALUES: frozenset[str] = frozenset(
    {
        "allOf",
        "anyOf",
        "oneOf",
        "not",
        "if",
        "then",
        "else",
        "additionalProperties",
        "propertyNames",
        "items",
        "contains",
        "unevaluatedItems",
    }
)
_MAP_VALUES: frozenset[str] = frozenset(
    {"$defs", "properties", "patternProperties", "dependentSchemas"}
)


def _check_known_keywords(node: Any, where: str) -> None:
    """D-19: this library ignores an unknown keyword silently, and Ajv's strict mode rejects it.

    The walker walks only the values that are schemas themselves; the data of `enum` or `default`
    is not a schema, so it is left alone."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key not in _KNOWN_KEYWORDS:
                raise ValueError(f"{where}: unknown JSON Schema keyword {key!r}")
            subs = (
                value.values() if key in _MAP_VALUES else [value] if key in _SCHEMA_VALUES else ()
            )
            for sub in subs:
                _check_known_keywords(sub, f"{where}.{key}")
    elif isinstance(node, list):
        for index, item in enumerate(node):
            _check_known_keywords(item, f"{where}[{index}]")


def _log_peer_skip(cap_name: str, check: str, error: Exception) -> None:
    """D-19: the skip is named in `CapabilityValidator.skipped`, and it is logged once."""
    if (key := (cap_name, check)) not in _peer_skips_logged:
        _peer_skips_logged.add(key)
        _log.warning("capability %s: skipping the %s check: %s", cap_name, check, error)


def _own_check(validator: Draft202012Validator) -> Callable[[Any], str | None]:
    """The check of an own capability: the reason of the first error, or None."""

    def check(value: Any) -> str | None:
        errors = list(validator.iter_errors(value))
        return reason(errors[0]) if errors else None

    return check


def _peer_check(validator: Draft202012Validator) -> Callable[[Any], str | None]:
    """The check of a peer's capability: an error of the validator itself is not a validation
    result, so the check passes and the peer decides (D-19)."""

    def check(value: Any) -> str | None:
        try:
            errors = list(validator.iter_errors(value))
        except Exception:
            return None
        return reason(errors[0]) if errors else None

    return check


def check_params(cap: Capability, params: Any) -> str | None:
    """Public: None when `params` satisfies the capability's `parameters` schema, else the reason.

    The reason is in D-15's grammar, for the first error. A schema that will not run here — one that
    fails `check_schema`, names an unknown keyword, or holds a `$ref` that cannot resolve — skips the
    check and returns None (D-19): the peer decides."""
    try:
        _check_known_keywords(cap["parameters"], f"capability {cap['name']!r}: parameters")
        Draft202012Validator.check_schema(cap["parameters"])
        validator = Draft202012Validator(cap["parameters"], registry=referencing.Registry())
        errors = list(validator.iter_errors(params))
    except Exception:
        return None
    return reason(errors[0]) if errors else None


@dataclasses.dataclass(frozen=True, slots=True)
class CapabilityValidator:
    """One capability's compiled checks (D-19). A skipped check returns None and says so once."""

    parameters: Callable[[Any], str | None]
    result: Callable[[Any], str | None]
    skipped: frozenset[str]  # which of "parameters", "result" could not run here


def compile_capability(cap: Capability) -> CapabilityValidator:
    """Compile `parameters` and `result` for one capability, with an empty registry (D-19).

    This is the registration path: a failing `check_schema` or an unknown keyword is a registration
    error, raised here. The peer's capability is compiled by `_compile_peer`, which skips what cannot
    run here and names it. On an empty registry a repeated `$id` is harmless and no `$ref` can reach
    out: nothing is registered for it to find."""

    def compile_one(check: str, schema_doc: Any) -> Callable[[Any], str | None]:
        _check_known_keywords(schema_doc, f"capability {cap['name']!r}: {check}")
        Draft202012Validator.check_schema(schema_doc)
        return _own_check(Draft202012Validator(schema_doc, registry=referencing.Registry()))

    return CapabilityValidator(
        parameters=compile_one("parameters", cap["parameters"]),
        result=compile_one("result", cap["actio"]["result"]),
        skipped=frozenset(),
    )


def _compile_peer(cap: Capability) -> CapabilityValidator:
    """D-19: a peer's schema that will not run here skips the check, names it in `skipped`, logs once.

    The checks that survive behave like the own path; a `$ref` that cannot resolve fails the validator
    at call time, which the check catches and lets pass."""
    checks: dict[str, Callable[[Any], str | None]] = {}
    skipped: list[str] = []
    for check, schema_doc in (
        ("parameters", cap["parameters"]),
        ("result", cap["actio"]["result"]),
    ):
        try:
            _check_known_keywords(schema_doc, f"capability {cap['name']!r}: {check}")
            Draft202012Validator.check_schema(schema_doc)
            checks[check] = _peer_check(
                Draft202012Validator(schema_doc, registry=referencing.Registry())
            )
        except Exception as error:
            checks[check] = lambda _: None
            skipped.append(check)
            _log_peer_skip(cap["name"], check, error)
    return CapabilityValidator(
        parameters=checks["parameters"],
        result=checks["result"],
        skipped=frozenset(skipped),
    )
