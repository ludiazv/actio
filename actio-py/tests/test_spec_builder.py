"""`_spec_builder.build_spec`: the shape of a `spec.json` (*Agent Spec*), field by field.

The builder is the only place a spec object is born, so this file pins its shape: the required
fields, the core profile's security fields, the injected clock, and the optional fields that must be
**absent** — not `null`, not `[]` — when nothing was set. The same file is checked against
`schemas/spec.schema.json`, so a shape drift is caught by the validator, not by eye.
"""

from __future__ import annotations

import unittest
from typing import Any, cast

from actio._spec_builder import build_spec
from actio._status import ACTIO_VERSION
from actio._util import format_iso
from test_agent import AGENT, PEER, TS, cap, schema_reason


class ShapeTest(unittest.TestCase):
    """The shape of a built spec: a plain `unittest` class, no agent, no tree."""

    def test_the_required_fields_are_present_and_typed(self) -> None:
        """*Agent Spec*: `id`, `actio_version`, `datetime`, `description`, `accepts_commands_from`,
        `capabilities`, `security_support` and `security_posture` are the mandatory fields."""
        spec = build_spec(
            id=AGENT,
            description="a spec under test",
            accepts_commands_from=[PEER],
            capabilities=[cap("summarize")],
            now=TS,
        )
        self.assertEqual(spec["id"], AGENT)
        self.assertEqual(spec["actio_version"], ACTIO_VERSION)
        self.assertEqual(
            spec["datetime"], format_iso(TS)
        )  # the injected clock, never the wall clock
        self.assertEqual(spec["description"], "a spec under test")
        self.assertEqual(spec["accepts_commands_from"], [PEER])
        self.assertEqual([c["name"] for c in spec["capabilities"]], ["summarize"])
        # The core profile: no security features, trust the shared tree.
        self.assertEqual(spec["security_support"], [])
        self.assertEqual(spec["security_posture"], "trusted")

    def test_the_optional_fields_are_absent_when_unset(self) -> None:
        """*Agent Spec* / *Namespaces*: absent, `null` and `[]` are three legal ways to say the null
        namespace, so the builder must not invent a value the operator did not write. `disabled` is
        never written here (the `enable` command is its only writer during a run)."""
        spec = build_spec(
            id=AGENT,
            description="a minimal spec",
            accepts_commands_from=[],
            capabilities=[],
            now=TS,
        )
        for field in ("namespaces", "depends", "wants", "info", "disabled", "security"):
            self.assertNotIn(field, spec, f"`{field}` must be absent, not null or empty")

    def test_the_optional_fields_are_present_when_set(self) -> None:
        spec = build_spec(
            id=AGENT,
            description="a full spec",
            accepts_commands_from=[PEER],
            capabilities=[cap("summarize")],
            now=TS,
            namespaces=["ns1", None],
            depends=[PEER],
            wants=[PEER],
            info={"author": "a test"},
        )
        self.assertEqual(spec["namespaces"], ["ns1", None])
        self.assertEqual(spec["depends"], [PEER])
        self.assertEqual(spec["wants"], [PEER])
        self.assertEqual(spec["info"], {"author": "a test"})

    def test_the_options_are_copied_not_aliased(self) -> None:
        """A later mutation of the options cannot alter what the builder published."""
        accepts = [PEER]
        info: dict[str, Any] = {"author": "a test"}
        spec = build_spec(
            id=AGENT,
            description="a copy, not a view",
            accepts_commands_from=accepts,
            capabilities=[],
            now=TS,
            info=info,
        )
        accepts.append(AGENT)
        info["author"] = "changed"
        self.assertEqual(spec["accepts_commands_from"], [PEER])
        self.assertEqual(cast(dict[str, Any], spec["info"]), {"author": "a test"})

    def test_the_capabilities_keep_registration_order(self) -> None:
        spec = build_spec(
            id=AGENT,
            description="order matters",
            accepts_commands_from=[],
            capabilities=[cap("zeta"), cap("alpha"), cap("mid")],
            now=TS,
        )
        self.assertEqual([c["name"] for c in spec["capabilities"]], ["zeta", "alpha", "mid"])

    def test_the_capability_schema_lands_in_the_spec(self) -> None:
        """The capability is carried whole, `actio.result` and all: the schema a peer validates
        against is the one the builder published."""
        spec = build_spec(
            id=AGENT,
            description="a capability with a result schema",
            accepts_commands_from=[],
            capabilities=[cap("summarize", result={"type": "string"})],
            now=TS,
        )
        capability = spec["capabilities"][0]
        self.assertEqual(capability["actio"]["result"], {"type": "string"})
        self.assertEqual(capability["parameters"], {"type": "object"})

    def test_a_duplicate_capability_name_raises(self) -> None:
        """The schema describes one capability, not the uniqueness of a name across the list, so the
        builder rejects the duplicate: a duplicated name would make the view silently lose one."""
        with self.assertRaises(ValueError):
            build_spec(
                id=AGENT,
                description="a duplicate name",
                accepts_commands_from=[],
                capabilities=[cap("same"), cap("same")],
                now=TS,
            )

    def test_the_built_spec_validates_against_the_schema(self) -> None:
        """The frozen drift test's job: the built file is a `spec.json`, so it must pass
        `schemas/spec.schema.json`. The shape is pinned here; the validator catches the rest."""
        spec = build_spec(
            id=AGENT,
            description="a spec that must validate",
            accepts_commands_from=[PEER],
            capabilities=[cap("summarize")],
            now=TS,
            namespaces=["ns1"],
            depends=[PEER],
            wants=[PEER],
            info={"author": "a test"},
        )
        self.assertIsNone(schema_reason("spec", cast(dict[str, Any], spec)))

    def test_a_schema_rejection_is_a_build_error(self) -> None:
        """A spec the schema rejects is our bug and must not reach the tree: the builder raises. An
        `id` outside the `agentId` pattern is the rejection, so the build fails, not the tree."""
        with self.assertRaises(ValueError):
            build_spec(
                id="not a valid id!",
                description="a bad id",
                accepts_commands_from=[],
                capabilities=[],
                now=TS,
            )
