"""D-20's drift test: `schemas/` and `_protocol.py` must say the same thing, and D-20's mutations prove
that the comparison is sharp.

Three rules, per pinned pair of schema node and `TypedDict`: every schema property has a key with that
name and vice versa; `__required_keys__` matches `required`; and the property's type matches its
annotation (`integer`↔`int`, `number`↔`int | float`, `boolean`↔`bool`, a nullable ↔ `| None`). Every
`enum` in the schemas matches a `Literal` in `_protocol.py` by value. Both directions of mutation are
tested: the protocol lying about the schema, and the schema lying about the protocol.
"""

from __future__ import annotations

import copy
import json
import types
import typing
import unittest
from collections.abc import Callable
from importlib import resources
from typing import Any, cast

from actio import _protocol
from actio._protocol import (
    ActioMeta,
    Announce,
    CancellationBody,
    CancellationFile,
    Capability,
    CapabilityActio,
    CapabilityStatusEntry,
    ConfirmationBody,
    ConfirmationFile,
    DiariumEntry,
    ErrorResult,
    ExchangeRecord,
    Header,
    RequestBody,
    RequestFile,
    ResponseBody,
    ResponseFile,
    SecurityAttribute,
    Spec,
    Status,
    StatusTask,
)

SCHEMA_NAMES = (
    "actio.schema.json",
    "announce.schema.json",
    "cancellation.schema.json",
    "common.schema.json",
    "confirmation.schema.json",
    "diarium.schema.json",
    "error.schema.json",
    "request.schema.json",
    "response.schema.json",
    "spec.schema.json",
    "status.schema.json",
)

Entry = tuple[str, str, type]

# Every object node of the 11 schemas, pinned to the TypedDict that mirrors it. A file's own node merges
# the header it refers to in `allOf`, which is how `Header` reaches the four envelope types.
MAP: tuple[Entry, ...] = (
    ("actio.schema.json", "", ActioMeta),
    ("announce.schema.json", "", Announce),
    ("common.schema.json", "$defs/header", Header),
    ("common.schema.json", "$defs/requestHeader", Header),
    ("cancellation.schema.json", "", CancellationFile),
    ("cancellation.schema.json", "$defs/body", CancellationBody),
    ("confirmation.schema.json", "", ConfirmationFile),
    ("confirmation.schema.json", "$defs/body", ConfirmationBody),
    ("diarium.schema.json", "", DiariumEntry),
    ("diarium.schema.json", "$defs/exchange", ExchangeRecord),
    ("error.schema.json", "", ErrorResult),
    ("request.schema.json", "", RequestFile),
    ("request.schema.json", "$defs/body", RequestBody),
    ("response.schema.json", "", ResponseFile),
    ("response.schema.json", "$defs/body", ResponseBody),
    ("spec.schema.json", "", Spec),
    ("spec.schema.json", "$defs/capability", Capability),
    ("spec.schema.json", "$defs/capabilityActio", CapabilityActio),
    ("spec.schema.json", "properties/security/items", SecurityAttribute),
    ("status.schema.json", "", Status),
    ("status.schema.json", "properties/capability_status/items", CapabilityStatusEntry),
    ("status.schema.json", "properties/tasks/items", StatusTask),
)


def load_schemas() -> dict[str, dict[str, Any]]:
    """The 11 schemas as the package reads them at run time (D-20: `importlib.resources`)."""
    folder = resources.files("actio").joinpath("schemas")
    return {
        name: json.loads(folder.joinpath(name).read_text(encoding="utf-8")) for name in SCHEMA_NAMES
    }


def canon_join(pointer: str, tail: str) -> str:
    """A pointer inside a node: `""` + `"properties/x"` is `"properties/x"`."""
    return f"{pointer}/{tail}" if pointer else tail


def canon(sid: str, pointer: str) -> str:
    """One way to name a schema node, so the map and the schemas can be compared by name."""
    return f"{sid}#{'/'.join(filter(None, pointer.split('/')))}"


def at(schemas: dict[str, dict[str, Any]], sid: str, pointer: str) -> dict[str, Any]:
    node: Any = schemas[sid]
    for part in filter(None, pointer.split("/")):
        node = node[part]
    assert isinstance(node, dict), f"{sid} {pointer}"
    return node


def split_ref(sid: str, ref: str) -> tuple[str, str]:
    file, _, fragment = ref.partition("#")
    return (file or sid, fragment.strip("/"))


def merged(
    schemas: dict[str, dict[str, Any]],
    sid: str,
    pointer: str,
    seen: frozenset[tuple[str, str]] = frozenset(),
) -> tuple[dict[str, list[tuple[str, str, dict[str, Any]]]], set[str]]:
    """The properties and required names a node states, including the ones it inherits through `allOf`.

    Each property comes with the file and pointer its own `$ref`s are relative to: a `#/$defs/rid` written
    inside `common.schema.json` still means `common.schema.json` after the property is merged into an
    envelope node, which is JSON Schema's base-URI rule and the drift test has to respect it too.
    """
    node = at(schemas, sid, pointer)
    props: dict[str, list[tuple[str, str, dict[str, Any]]]] = {}
    required: set[str] = set()
    for name, spec in (node.get("properties") or {}).items():
        props.setdefault(name, []).append((sid, canon_join(pointer, f"properties/{name}"), spec))
    required.update(node.get("required") or ())
    here = (sid, pointer)
    if here not in seen:
        for branch in node.get("allOf") or []:
            if "$ref" in branch:
                rsid, rpointer = split_ref(sid, str(branch["$ref"]))
                sub_props, sub_required = merged(schemas, rsid, rpointer, seen | {here})
                for name, specs in sub_props.items():
                    props.setdefault(name, []).extend(specs)
                required.update(sub_required)
    return props, required


def token(values: Any) -> str:
    return "L{" + ",".join(sorted(repr(value) for value in values)) + "}"


def union(parts: list[str]) -> str:
    return "|".join(sorted(set(parts)))


ATOMS = {
    "string": "str",
    "integer": "int",
    "number": "num",
    "boolean": "bool",
    "object": "dict",
    "null": "none",
}


def schema_form(
    schemas: dict[str, dict[str, Any]], sid: str, spec: dict[str, Any], where: str = ""
) -> str:
    """What the schema says about one value, in a form an annotation can be compared to.

    `where` is the JSON pointer of `spec` inside `sid`, so an inline object shape is named by the same
    canonical name the map pins it under, rather than collapsing to "some object".
    """
    if "properties" in spec:
        return canon(sid, where)  # an object shape of its own: pinned, or the map has a hole
    if "anyOf" in spec:
        return union(
            [
                schema_form(schemas, sid, branch, f"{where}/anyOf/{index}")
                for index, branch in enumerate(spec["anyOf"])
            ]
        )
    if "enum" in spec:
        return token(spec["enum"])
    if "const" in spec:
        return token([spec["const"]])
    if "$ref" in spec:
        rsid, pointer = split_ref(sid, str(spec["$ref"]))
        target = at(schemas, rsid, pointer)
        return schema_form(schemas, rsid, target, pointer)
    kind = spec.get("type")
    if kind is None:
        return "any"
    forms = []
    for one in kind if isinstance(kind, list) else [kind]:
        if one == "array":
            items = spec.get("items")
            forms.append(
                f"list[{schema_form(schemas, sid, items, f'{where}/items') if items else 'any'}]"
            )
        else:
            forms.append(ATOMS[str(one)])
    return union(forms) if len(forms) > 1 else forms[0]


def annotation_form(ann: Any, pinned: dict[Any, str]) -> str:
    """What the annotation says, in the same form."""
    if isinstance(
        ann, typing.TypeAliasType
    ):  # Python 3.14 keeps `type X = ...` aliases as they are
        ann = ann.__value__
    if ann is Any or ann is object:
        return "any"  # `AnyValue` in `_protocol.py` is `object`: any JSON value, narrowed after validation
    if ann in (None, type(None)):
        return "none"
    if ann in (int, float, bool, str):
        return {int: "int", float: "float", bool: "bool", str: "str"}[ann]
    origin = typing.get_origin(ann)
    args = typing.get_args(ann)
    if origin is typing.Literal:
        return token(args)
    if origin in (typing.Union, types.UnionType):
        if set(args) == {int, float}:
            return "num"  # D-20: a schema `number` is `int | float`
        return union([annotation_form(arg, pinned) for arg in args])
    if origin in (list, typing.Sequence):
        return f"list[{annotation_form(args[0], pinned)}]" if args else "list[any]"
    if origin in (dict, typing.Mapping):
        return "dict"
    if ann in pinned:
        return pinned[ann]
    raise LookupError(f"{ann!r} is not pinned to a schema node")


def enum_sites(schemas: dict[str, dict[str, Any]]) -> list[tuple[str, list[Any]]]:
    """Every `enum` anywhere in the 11 schemas, with where it sits."""
    found: list[tuple[str, list[Any]]] = []

    def walk(node: Any, where: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "enum" and isinstance(value, list):
                    found.append((where, value))
                walk(value, f"{where}/{key}")
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, f"{where}[{index}]")

    for sid, schema in schemas.items():
        walk(schema, sid)
    return found


def protocol_literals() -> set[str]:
    namespace: dict[str, Any] = dict(vars(_protocol))
    found: set[str] = set()
    for value in namespace.values():
        if isinstance(value, typing.TypeAliasType):  # a `type X = Literal[...]` alias
            value = value.__value__
        if typing.get_origin(value) is typing.Literal:
            found.add(token(typing.get_args(value)))
    return found


def protocol_typeddicts() -> set[type]:
    namespace: dict[str, Any] = dict(vars(_protocol))
    return {
        value
        for value in namespace.values()
        if isinstance(value, type)
        and issubclass(value, dict)
        and hasattr(value, "__required_keys__")
    }


def drift_problems(schemas: dict[str, dict[str, Any]], entries: tuple[Entry, ...]) -> list[str]:
    """Every way the pinned types and the schemas disagree; empty means they agree."""
    problems: list[str] = []
    pinned: dict[Any, str] = {cls: canon(sid, pointer) for sid, pointer, cls in entries}
    for sid, pointer, cls in entries:
        props, required = merged(schemas, sid, pointer)
        annotations = typing.get_type_hints(cls)
        label = f"{sid} {pointer or '(root)'} -> {cls.__name__}"
        missing = sorted(set(props) - set(annotations))
        extra = sorted(set(annotations) - set(props))
        if missing:
            problems.append(
                f"{label}: no key for propert{'ies' if len(missing) > 1 else 'y'} {missing}"
            )
        if extra:
            problems.append(
                f"{label}: key{'s' if len(extra) > 1 else ''} {extra} with no schema property"
            )
        got_required = set(getattr(cls, "__required_keys__", set()))
        if got_required != required:
            problems.append(
                f"{label}: required {sorted(got_required)} != schema {sorted(required)}"
            )
        for name, specs in sorted(props.items()):
            if name not in annotations:
                continue
            try:
                want = annotation_form(annotations[name], pinned)
            except LookupError as exc:
                problems.append(f"{label}.{name}: {exc}")
                continue
            forms = {schema_form(schemas, osid, spec, optr) for osid, optr, spec in specs}
            if want not in forms:
                problems.append(f"{label}.{name}: {want} != {' or '.join(sorted(forms))}")
    literals = protocol_literals()
    for where, values in enum_sites(schemas):
        if token(values) not in literals:
            problems.append(f"{where}: no Literal for {token(values)}")
    for cls in sorted(protocol_typeddicts() - set(pinned), key=lambda c: c.__name__):
        problems.append(f"{cls.__name__} is not pinned to a schema node")
    return problems


class DriftTest(unittest.TestCase):
    def setUp(self) -> None:
        self.schemas = load_schemas()

    def test_all_eleven_schemas_are_installed(self) -> None:
        self.assertEqual(sorted(self.schemas), sorted(SCHEMA_NAMES))

    def test_no_drift(self) -> None:
        self.assertEqual(drift_problems(self.schemas, MAP), [])

    def test_every_object_shape_is_pinned(self) -> None:
        """No hole in the map: every `$defs` object and every inline object shape a pinned node points
        at is pinned itself, so a new shape in a schema cannot slip past the drift test. A conditional
        refinement (`if`/`then`) is not a shape, and is not pinned, as in the reference's own types."""
        pinned = {canon(sid, pointer) for sid, pointer, _ in MAP}
        holes: list[str] = []

        def consider(where: str, target: dict[str, Any]) -> None:
            if "properties" in target and where not in pinned:
                holes.append(where)

        for sid, schema in self.schemas.items():
            for name, definition in (schema.get("$defs") or {}).items():
                consider(canon(sid, f"$defs/{name}"), definition)
        for sid, pointer, _ in MAP:
            props, _ = merged(self.schemas, sid, pointer)
            for _name, specs in props.items():
                for origin, optr, spec in specs:
                    for branch in spec.get("anyOf") or [spec]:
                        if "$ref" in branch:
                            continue
                        consider(canon(origin, optr), branch)
                        items = branch.get("items")
                        if isinstance(items, dict) and "$ref" not in items:
                            consider(canon(origin, canon_join(optr, "items")), items)
        self.assertEqual(holes, [])

    def test_the_eight_enums_are_reachable(self) -> None:
        """D-20 counts eight protocol enums; the count is the walker's own proof."""
        sites = enum_sites(self.schemas)
        self.assertEqual(len(sites), 8, [where for where, _ in sites])
        literals = protocol_literals()
        for where, values in sites:
            self.assertIn(token(values), literals, where)


class MutationTest(unittest.TestCase):
    """D-20's negative controls: each one must make the drift test fail."""

    def setUp(self) -> None:
        self.schemas = load_schemas()

    def check(
        self,
        label: str,
        entries: tuple[Entry, ...] | None = None,
        schemas: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        problems = drift_problems(schemas or self.schemas, entries or MAP)
        self.assertTrue(problems, f"the drift test did not catch: {label}")

    def swap(self, cls: type, replacement: type) -> tuple[Entry, ...]:
        return tuple(
            (sid, pointer, replacement if old is cls else old) for sid, pointer, old in MAP
        )

    def fields(self, td: type) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for base in reversed(td.__mro__):
            if hasattr(base, "__required_keys__"):
                out.update(getattr(base, "__annotations__", {}))
        return out

    def rebuilt(self, td: type, fields: dict[str, Any]) -> type:
        """The functional form of `TypedDict`, so a mutation can rebuild one with different fields."""
        make = cast("Callable[[str, dict[str, Any]], type]", typing.TypedDict)
        return make(td.__name__, fields)

    def test_key_dropped(self) -> None:
        fields = self.fields(Spec)
        fields.pop("security_posture")
        self.check("a key dropped", self.swap(Spec, self.rebuilt(Spec, fields)))

    def test_key_renamed(self) -> None:
        fields = self.fields(Status)
        fields["message"] = fields.pop("msg")
        self.check("a key renamed", self.swap(Status, self.rebuilt(Status, fields)))

    def test_required_made_not_required(self) -> None:
        fields = self.fields(Capability)
        fields["name"] = typing.NotRequired[str]
        self.check(
            "a required key made NotRequired",
            self.swap(Capability, self.rebuilt(Capability, fields)),
        )

    def test_optional_made_required(self) -> None:
        fields = self.fields(Status)
        fields["msg"] = str
        self.check("an optional key made required", self.swap(Status, self.rebuilt(Status, fields)))

    def test_nullable_annotated_plain(self) -> None:
        fields = self.fields(ResponseBody)
        fields["signature"] = str
        self.check(
            "nullable `signature` annotated `str`",
            self.swap(ResponseBody, self.rebuilt(ResponseBody, fields)),
        )

    def test_integer_annotated_float(self) -> None:
        fields = self.fields(ResponseBody)
        fields["ts"] = float
        self.check(
            "`ts` annotated `float`", self.swap(ResponseBody, self.rebuilt(ResponseBody, fields))
        )

    def test_number_annotated_str(self) -> None:
        fields = self.fields(Status)
        fields["load"] = typing.NotRequired[str]
        self.check("`load` annotated `str`", self.swap(Status, self.rebuilt(Status, fields)))

    def test_literal_missing_a_value(self) -> None:
        fields = self.fields(Status)
        fields["status"] = typing.Literal["inactive", "idle", "busy"]
        self.check("a Literal missing a value", self.swap(Status, self.rebuilt(Status, fields)))

    def test_literal_with_an_extra_value(self) -> None:
        fields = self.fields(Status)
        fields["status"] = typing.Literal["inactive", "idle", "busy", "error", "asleep"]
        self.check("a Literal with an extra value", self.swap(Status, self.rebuilt(Status, fields)))

    def test_schema_new_property(self) -> None:
        schemas = copy.deepcopy(self.schemas)
        schemas["spec.schema.json"]["properties"]["mood"] = {"type": "string"}
        self.check("a new schema property", schemas=schemas)

    def test_schema_newly_required_key(self) -> None:
        schemas = copy.deepcopy(self.schemas)
        schemas["spec.schema.json"]["required"].append("info")
        self.check("a newly required key", schemas=schemas)

    def test_schema_new_status_state(self) -> None:
        schemas = copy.deepcopy(self.schemas)
        schemas["status.schema.json"]["properties"]["status"]["enum"].append("asleep")
        self.check("a new status state", schemas=schemas)

    def test_schema_new_final_status(self) -> None:
        schemas = copy.deepcopy(self.schemas)
        branch = schemas["diarium.schema.json"]["allOf"][2]["then"]["properties"]["event_data"][
            "properties"
        ]
        branch["final_status"]["enum"].append("forgotten")
        self.check("a new final status", schemas=schemas)

    def test_a_pinned_node_removed(self) -> None:
        """The map's own control: dropping one pinned pair must be seen, not silently tolerated."""
        self.check("one pinned pair removed", MAP[:-1])


if __name__ == "__main__":
    unittest.main()
