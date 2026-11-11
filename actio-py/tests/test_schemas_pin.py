"""D-20: `src/actio/schemas/` is a byte-identical committed copy of `../doc/schemas/`, and the copy is
usable — it parses, it passes the metaschema, and every `$ref` in it resolves.

The schemas are authoritative at run time, so a copy that drifts from the source is a protocol bug
waiting to happen; the drift test reads this copy, so the two tests together tie the code to
`../doc/schemas/`. Only `jsonschema` is used here (D-21): cross-file `$ref` resolution is checked as JSON,
not through a resolver, so no second dependency sneaks in.
"""

from __future__ import annotations

import json
import unittest
from importlib import resources
from pathlib import Path
from typing import Any

COPY = Path(__file__).resolve().parents[1] / "src" / "actio" / "schemas"
REFERENCE = Path(__file__).resolve().parents[2] / "doc" / "schemas"


def installed() -> dict[str, dict[str, Any]]:
    folder = resources.files("actio.schemas")
    names = sorted(p.name for p in COPY.glob("*.json"))
    return {name: json.loads(folder.joinpath(name).read_text(encoding="utf-8")) for name in names}


class InstalledSchemasTest(unittest.TestCase):
    def test_there_are_eleven_of_them(self) -> None:
        names = sorted(p.name for p in COPY.glob("*.json"))
        self.assertEqual(len(names), 11, names)

    def test_each_copy_names_itself(self) -> None:
        """A copy whose `$id` is not its file name breaks every `$ref` between the schemas."""
        for name, schema in installed().items():
            self.assertEqual(schema["$id"], name)

    def test_the_package_reads_its_own_schemas(self) -> None:
        """D-20: the schemas reach the code through `importlib.resources`, not through the source tree."""
        folder = resources.files("actio.schemas")
        names = sorted(p.name for p in COPY.glob("*.json"))
        self.assertEqual(
            sorted(p.name for p in folder.iterdir() if p.name.endswith(".json")), names
        )


@unittest.skipUnless(
    REFERENCE.is_dir(), f"the reference tree is not next to this checkout: {REFERENCE}"
)
class ByteIdenticalCopyTest(unittest.TestCase):
    def test_the_names_match_the_reference(self) -> None:
        self.assertEqual(
            sorted(p.name for p in COPY.glob("*.json")),
            sorted(p.name for p in REFERENCE.glob("*.json")),
        )

    def test_every_byte_matches(self) -> None:
        for source in sorted(REFERENCE.glob("*.json")):
            installed_copy = COPY / source.name
            self.assertTrue(installed_copy.exists(), f"{installed_copy} is missing")
            self.assertEqual(
                installed_copy.read_bytes(), source.read_bytes(), f"{source.name} differs"
            )


class UsableSchemasTest(unittest.TestCase):
    """As in `actio-go`'s scaffold test: the pinned schemas compile with the one validator dependency."""

    def test_every_schema_passes_the_metaschema(self) -> None:
        import jsonschema

        for name, schema in installed().items():
            jsonschema.Draft202012Validator.check_schema(schema)
            self.assertEqual(
                schema["$schema"], "https://json-schema.org/draft/2020-12/schema", name
            )

    def test_every_ref_resolves(self) -> None:
        """A dangling `$ref` breaks every validator, and JSON alone can check it (D-19 depends on this)."""
        schemas = installed()

        def walk(node: Any, sid: str) -> None:
            if isinstance(node, dict):
                for key, value in node.items():
                    if key == "$ref":
                        file, _, fragment = str(value).partition("#")
                        target: Any = schemas[file or sid]
                        for part in filter(None, fragment.split("/")):
                            self.assertIn(part, target, f"{sid}: {value}")
                            target = target[part]
                    else:
                        walk(value, sid)
            elif isinstance(node, list):
                for item in node:
                    walk(item, sid)

        for sid, schema in schemas.items():
            walk(schema, sid)


class CommonDefsTest(unittest.TestCase):
    """The leaf constraints of `common.schema.json`, pinned because they are easy to read wrongly.

    `datetime` has **no** fractional second: the pattern says so, and every example in the protocol is
    whole seconds. `ts` is an epoch **integer** in seconds, not a string. The schemas are the authority at
    run time (D-16), and `format_iso` must emit exactly what these accept."""

    def setUp(self) -> None:
        self.defs = installed()["common.schema.json"]["$defs"]

    def validate(self, def_name: str, value: Any) -> list[Any]:
        import jsonschema

        schema = dict(self.defs[def_name])
        schema["$schema"] = "https://jsonschema.org/draft/2020-12/schema"
        return list(jsonschema.Draft202012Validator(schema).iter_errors(value))

    def test_datetime_has_no_milliseconds(self) -> None:
        self.assertEqual(self.validate("datetime", "2026-10-24T10:00:00Z"), [])
        self.assertEqual(len(self.validate("datetime", "2026-10-24T10:00:00.000Z")), 1)
        self.assertEqual(len(self.validate("datetime", "2026-10-24 10:00:00Z")), 1)

    def test_ts_is_a_non_negative_epoch_integer(self) -> None:
        self.assertEqual(self.validate("ts", 1792836000), [])
        self.assertEqual(self.validate("ts", 0), [])
        self.assertEqual(len(self.validate("ts", -1)), 1)
        self.assertEqual(len(self.validate("ts", "1792836000")), 1)
        self.assertEqual(len(self.validate("ts", 1.5)), 1)

    def test_duration_is_a_non_negative_number(self) -> None:
        self.assertEqual(self.validate("duration", 0), [])
        self.assertEqual(self.validate("duration", 0.5), [])
        self.assertEqual(len(self.validate("duration", -0.5)), 1)

    def test_the_name_and_id_patterns(self) -> None:
        self.assertEqual(self.validate("agentId", "worker-a"), [])
        self.assertEqual(len(self.validate("agentId", "worker.a")), 1)
        self.assertEqual(len(self.validate("agentId", "x" * 33)), 1)
        self.assertEqual(self.validate("capabilityName", "summarize_text"), [])
        self.assertEqual(len(self.validate("capabilityName", "summarize text")), 1)
        self.assertEqual(self.validate("rid", "01J4ZZ000000000000000000AB"), [])
        self.assertEqual(len(self.validate("rid", "01j4zz000000000000000000ab")), 1)
        self.assertEqual(self.validate("actioVersion", "0.1"), [])
        self.assertEqual(len(self.validate("actioVersion", "0.1.2")), 1)
        self.assertEqual(self.validate("attachments", []), [])
        self.assertEqual(self.validate("attachments", ["forum/01ABC/file.txt"]), [])
        self.assertEqual(len(self.validate("attachments", ["/absolute/path"])), 1)
        self.assertEqual(len(self.validate("attachments", None)), 1)

    def test_signature_can_be_null(self) -> None:
        self.assertEqual(self.validate("signature", None), [])
        self.assertEqual(self.validate("signature", "base64ish"), [])
        self.assertEqual(len(self.validate("signature", 7)), 1)


if __name__ == "__main__":
    unittest.main()
