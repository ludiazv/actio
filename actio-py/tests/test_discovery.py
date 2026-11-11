"""`_discovery`: the scan's four steps (reserved names, missing and broken publications, duplicate ids)
and the eight callability rules, one test per rule.

The scan reads publications only — never an agent's `inbox` — in one `to_thread` call. The view tests
are pure: `agent_view` takes `now` as a parameter and reads no clock and no disk.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any, cast

from actio._discovery import agent_view, scan
from actio._protocol import Capability, CapabilityActio, Spec, Status
from actio._types import DiscoveredAgent
from actio._util import format_iso
from helpers import actio_tree

NOW = 1_000_000.0


def spec(**changes: Any) -> Spec:
    doc: dict[str, Any] = {
        "id": "alpha",
        "actio_version": "0.1",
        "datetime": format_iso(NOW),
        "description": "d",
        "accepts_commands_from": [],
        "capabilities": [],
        "security_support": [],
        "security_posture": "trusted",
    }
    doc.update(changes)
    return cast(Spec, doc)


def capability(name: str, **actio: Any) -> Capability:
    actio_doc: dict[str, Any] = {"result": True, **actio}
    return {
        "name": name,
        "description": "d",
        "parameters": {"type": "object"},
        "actio": cast("CapabilityActio", actio_doc),
    }


def status(state: str, age: float = 10.0) -> Status:
    return cast(
        Status,
        {
            "id": "beta",
            "status": state,
            "datetime": format_iso(NOW - age),
            "capability_status": [],
        },
    )


def peer(s: Spec | None = None, st: Status | None = None) -> DiscoveredAgent:
    return DiscoveredAgent(
        folder="beta",
        spec=s if s is not None else spec(id="beta"),
        status=st if st is not None else status("idle"),
    )


def write(tree: Path, rel: str, doc: Any) -> None:
    path = tree / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc), encoding="utf-8")


def agent_folder(
    tree: Path, name: str, agent_id: str, state: str = "idle", **spec_changes: Any
) -> None:
    """One agent folder with a conforming `spec.json` and `status.json`."""
    write(tree, f"{name}/spec.json", spec(id=agent_id, **spec_changes))
    write(tree, f"{name}/status.json", status(state, 10.0) | {"id": agent_id})


class ScanTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_full_tree_scans_to_the_exact_result(self) -> None:
        with actio_tree() as tree:
            # ten agent folders: good, missing-file, invalid-schema, unreadable-file and duplicate id
            agent_folder(tree, "alpha", "alpha")
            agent_folder(tree, "beta", "beta", state="busy")
            write(tree, "missing-spec/status.json", status("idle", 10.0) | {"id": "missing-spec"})
            write(tree, "missing-status/spec.json", spec(id="missing-status"))
            write(
                tree, "bad-spec/spec.json", spec(id="bad-spec") | {"id": None}
            )  # the schema says string
            write(tree, "bad-spec/status.json", status("idle", 10.0) | {"id": "bad-spec"})
            write(tree, "bad-status/spec.json", spec(id="bad-status"))
            write(
                tree,
                "bad-status/status.json",
                status("sleeping", 10.0) | {"id": "bad-status"},  # not a state
            )
            (tree / "unreadable").mkdir()
            (
                tree / "unreadable/spec.json"
            ).mkdir()  # a directory where a file should be: unreadable
            (tree / "unreadable/status.json").write_text(
                json.dumps(status("idle", 10.0) | {"id": "unreadable"}), encoding="utf-8"
            )
            agent_folder(tree, "dup-1", "gamma")
            agent_folder(tree, "dup-2", "gamma")  # the same id, later in lexical order
            # reserved names, a hidden folder and a root file: none of them is an agent
            agent_folder(tree, "tools", "tools-agent")
            agent_folder(tree, "forum", "forum-agent")
            agent_folder(tree, "nuntium", "nuntium-agent")
            agent_folder(tree, ".hidden", "hidden-agent")
            (tree / "stray.txt").write_text("not an agent", encoding="utf-8")
            # the team metadata: optional, and a conforming one is not reported
            write(tree, "actio.json", {"description": "the team"})

            result = await scan(str(tree))

        self.assertEqual(
            [a.folder for a in result.agents],
            [
                str(tree / "alpha"),
                str(tree / "beta"),
                str(tree / "dup-1"),
            ],
        )
        # the scan never produces a None status: a folder without status.json is not an agent
        for agent in result.agents:
            self.assertIsNotNone(agent.status)
        self.assertEqual([a.spec["id"] for a in result.agents], ["alpha", "beta", "gamma"])
        self.assertEqual(result.duplicates, [str(tree / "dup-2")])
        self.assertEqual(
            [p.path for p in result.ignored],
            [
                str(tree / "bad-spec/spec.json"),
                str(tree / "bad-status/status.json"),
                str(tree / "missing-spec/spec.json"),
                str(tree / "missing-status/status.json"),
                str(tree / "unreadable/spec.json"),
            ],
        )
        self.assertEqual(
            [p.reason for p in result.ignored[:4]],
            [
                "/id must be of type string",
                "/status must be one of 'inactive', 'idle', 'busy', 'error'",
                "missing spec.json",
                "missing status.json",
            ],
        )
        # the unreadable file: D-15's one line, the error text and no value of the peer's
        unreadable_reason = result.ignored[4].reason
        self.assertTrue(unreadable_reason.startswith("unreadable: "))
        self.assertLessEqual(len(unreadable_reason), 200)
        self.assertNotIn("\n", unreadable_reason)

    async def test_reserved_hidden_and_files_alone_scan_to_nothing(self) -> None:
        with actio_tree() as tree:
            agent_folder(tree, "tools", "t")
            agent_folder(tree, "forum", "f")
            agent_folder(tree, "nuntium", "n")
            agent_folder(tree, ".hidden", "h")
            (tree / "actio.json").write_text(json.dumps({"description": "t"}), encoding="utf-8")
            result = await scan(str(tree))
        self.assertEqual(result.agents, [])
        self.assertEqual(result.duplicates, [])
        self.assertEqual(result.ignored, [])

    async def test_a_folder_missing_both_files_reports_both(self) -> None:
        with actio_tree() as tree:
            (tree / "empty").mkdir()
            result = await scan(str(tree))
        self.assertEqual(
            [(p.path, p.reason) for p in result.ignored],
            [
                (str(tree / "empty/spec.json"), "missing spec.json"),
                (str(tree / "empty/status.json"), "missing status.json"),
            ],
        )

    async def test_an_unparsable_publication_gets_the_malformed_reason(self) -> None:
        with actio_tree() as tree:
            (tree / "broken").mkdir()
            (tree / "broken/spec.json").write_text('{"id": ', encoding="utf-8")
            write(tree, "broken/status.json", status("idle", 10.0) | {"id": "broken"})
            result = await scan(str(tree))
        self.assertEqual(result.agents, [])
        self.assertEqual(len(result.ignored), 1)
        self.assertEqual(result.ignored[0].path, str(tree / "broken/spec.json"))
        self.assertTrue(result.ignored[0].reason.startswith("malformed JSON: "))
        self.assertLessEqual(len(result.ignored[0].reason), 200)

    async def test_a_broken_actio_json_is_ignored(self) -> None:
        with actio_tree() as tree:
            agent_folder(tree, "alpha", "alpha")
            write(tree, "actio.json", {"metadata": "not an object"})
            result = await scan(str(tree))
        self.assertEqual([a.folder for a in result.agents], [str(tree / "alpha")])
        self.assertEqual(
            [(p.path, p.reason) for p in result.ignored],
            [(str(tree / "actio.json"), "/metadata must be of type object")],
        )

    async def test_a_duplicate_capability_name_is_ignored(self) -> None:
        with actio_tree() as tree:
            write(
                tree,
                "twins/spec.json",
                spec(
                    id="twins",
                    capabilities=[capability("echo"), capability("echo")],
                ),
            )
            write(tree, "twins/status.json", status("idle", 10.0) | {"id": "twins"})
            result = await scan(str(tree))
        self.assertEqual(result.agents, [])
        self.assertEqual(
            [(p.path, p.reason) for p in result.ignored],
            [(str(tree / "twins/spec.json"), 'duplicate capability name "echo"')],
        )


class AgentViewTest(unittest.TestCase):
    """The eight callability rules, in the protocol's order, one test each."""

    def test_rule_1_a_disabled_peer_keeps_commands_and_drops_capabilities(self) -> None:
        self_spec = spec(id="alpha")
        disabled_peer = peer(
            spec(
                id="beta",
                disabled=True,
                accepts_commands_from=["alpha"],
                capabilities=[capability("echo")],
            )
        )
        view = agent_view(self_spec, disabled_peer, NOW)
        self.assertIsNotNone(view)
        assert view is not None
        self.assertEqual(view.capabilities, {})
        self.assertTrue(view.commands)
        # the same peer, not disabled: the capability is back
        enabled_peer = peer(
            spec(id="beta", accepts_commands_from=["alpha"], capabilities=[capability("echo")])
        )
        view = agent_view(self_spec, enabled_peer, NOW)
        self.assertIsNotNone(view)
        assert view is not None
        self.assertEqual(set(view.capabilities), {"echo"})

    def test_rule_2_the_namespace_filter(self) -> None:
        cases: list[tuple[dict[str, Any] | None, dict[str, Any] | None, bool]] = [
            (None, None, True),  # both absent: both in the null namespace
            ({"namespaces": None}, None, True),  # an explicit null is the null namespace
            (None, {"namespaces": []}, True),  # an empty list is the null namespace
            (None, {"namespaces": [None]}, True),  # null listed explicitly
            (None, {"namespaces": ["other"]}, False),  # a filled list does not include the null one
            ({"namespaces": ["a"]}, {"namespaces": ["b"]}, False),
            ({"namespaces": ["a"]}, {"namespaces": ["a"]}, True),
            ({"namespaces": ["a"]}, {"namespaces": ["*"]}, True),  # "*" shares with everyone
            (None, {"namespaces": ["*"]}, True),
            ({"namespaces": ["a"]}, {"namespaces": [None]}, False),
        ]
        for mine, theirs, shared in cases:
            with self.subTest(mine=mine, theirs=theirs):
                # a usable capability on the peer, so that only the namespace rule can discard it
                view = agent_view(
                    spec(id="alpha", **(mine or {})),
                    peer(spec(id="beta", capabilities=[capability("echo")], **(theirs or {}))),
                    NOW,
                )
                if shared:
                    self.assertIsNotNone(view, (mine, theirs))
                else:
                    self.assertIsNone(view, (mine, theirs))

    def test_rules_3_and_4_only_the_trusted_posture_is_supported(self) -> None:
        # the core profile implements no security feature: a peer that requires any posture but
        # `trusted` needs something we do not have, and the zero-trust issuer check is unreachable
        self_spec = spec(id="alpha")
        for posture in ("trusted", "untrusted", "secret", "zero-trust"):
            with self.subTest(posture=posture):
                p = peer(
                    spec(
                        id="beta",
                        security_posture=posture,
                        accepts_commands_from=["alpha"],
                    )
                )
                view = agent_view(self_spec, p, NOW)
                if posture == "trusted":
                    self.assertIsNotNone(view)
                else:
                    self.assertIsNone(view)

    def test_rule_5_1_the_command_filter(self) -> None:
        self_spec = spec(id="alpha")
        for accepts, commands in (
            ([], False),  # an empty accepts_commands_from means nobody
            (["alpha"], True),
            (["other"], False),
        ):
            with self.subTest(accepts=accepts):
                p = peer(
                    spec(
                        id="beta",
                        accepts_commands_from=accepts,
                        capabilities=[capability("echo")],
                    )
                )
                view = agent_view(self_spec, p, NOW)
                self.assertIsNotNone(view)
                assert view is not None
                self.assertEqual(view.commands, commands)

    def test_rule_5_2_the_capability_filter(self) -> None:
        self_spec = spec(id="alpha")
        p = peer(
            spec(
                id="beta",
                capabilities=[
                    capability("open"),  # accepts_from absent: everybody
                    capability("empty", accepts_from=[]),  # empty: everybody
                    capability("mine", accepts_from=["alpha"]),  # containing our id
                    capability("theirs", accepts_from=["other"]),  # excluding us
                    capability("off", disabled=True),
                    capability("off2", disabled=False, accepts_from=["other"]),
                ],
            )
        )
        view = agent_view(self_spec, p, NOW)
        self.assertIsNotNone(view)
        assert view is not None
        self.assertEqual(set(view.capabilities), {"open", "empty", "mine"})

    def test_rule_6_nothing_usable_discards(self) -> None:
        self_spec = spec(id="alpha")
        # no commands, and the only capability is disabled
        p = peer(spec(id="beta", capabilities=[capability("echo", disabled=True)]))
        self.assertIsNone(agent_view(self_spec, p, NOW))
        # no commands, no capabilities at all
        self.assertIsNone(agent_view(self_spec, peer(spec(id="beta")), NOW))

    def test_rule_7_inactive_and_error_are_filtered(self) -> None:
        self_spec = spec(id="alpha")
        p = peer(spec(id="beta", accepts_commands_from=["alpha"]))
        for state, kept in (
            ("idle", True),
            ("busy", True),
            ("inactive", False),
            ("error", False),
        ):
            with self.subTest(state=state):
                view = agent_view(self_spec, peer(p.spec, status(state, 10.0)), NOW)
                if kept:
                    self.assertIsNotNone(view)
                else:
                    self.assertIsNone(view)

    def test_rule_8_health_is_flagged_never_filtered(self) -> None:
        # *Agent's status* rule 8: degraded (more than 100 s) and zombie (more than 600 s) peers are
        # flagged, not filtered out
        self_spec = spec(id="alpha")
        p = peer(spec(id="beta", accepts_commands_from=["alpha"]))
        for state, age, expected in (
            ("idle", 10.0, "healthy"),
            ("idle", 70.0, "online"),
            ("idle", 200.0, "degraded"),
            ("busy", 1000.0, "zombie"),
        ):
            with self.subTest(age=age):
                view = agent_view(self_spec, peer(p.spec, status(state, age)), NOW)
                self.assertIsNotNone(view, f"a {expected} peer must stay in the view")
                assert view is not None
                self.assertEqual(view.health, expected)


if __name__ == "__main__":
    unittest.main()
