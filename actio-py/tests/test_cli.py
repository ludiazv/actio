"""`_cli.py`: the `actio` command — the exact lines and the reference's exit codes.

Every row runs `main` in-process — on a worker thread, so the vehicle's `asyncio.run` has its own
loop — against a temporary root, with stdout and stderr captured, and ends with `no_leftovers`: a
watchdog thread or an armed task left behind is a failure. The last rows run a real process, so
`__main__.py` and the console script are pinned, not just the import.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import subprocess
import sys
import unittest
import unittest.mock as mock
from pathlib import Path
from typing import Any, cast

from actio import ActioAgent
from actio._cli import USAGE, main
from actio._config import ActioConfig
from actio._diarium import log_event, status_event
from actio._protocol import Capability
from actio._spec_builder import build_spec
from actio._status import build_status
from actio._types import ActioError
from actio._util import format_iso, nonce, ulid
from actio._validate import file_kind
from helpers import ActioTestCase, actio_tree, no_leftovers

TS = 1735689600.0  # a fixed instant: the hand-written documents of the validate rows


class CliTest(ActioTestCase):
    """One `main` call per row: a temporary root, captured streams, nothing left armed."""

    async def run_cli(self, *args: str, root: Path, **env: str) -> tuple[int, str, str]:
        out, error = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(error):
            with mock.patch.dict(os.environ, {"ACTIO_DIR": str(root), **env}):
                code = await asyncio.to_thread(main, list(args))
        return code, out.getvalue(), error.getvalue()

    @staticmethod
    def cap(name: str, **actio_over: Any) -> dict[str, Any]:
        meta: dict[str, Any] = {"result": {"type": "object"}, **actio_over}
        return {
            "name": name,
            "description": f"the {name} capability",
            "parameters": {"type": "object"},
            "actio": meta,
        }

    @staticmethod
    async def handler(params: Any, ctx: Any) -> Any:
        return {"echo": params}

    def peer(
        self,
        root: Path,
        *,
        failing: bool = False,
        caps: list[tuple[str, dict[str, Any]]] | None = None,
    ) -> ActioAgent:
        agent = ActioAgent(
            id="peer",
            description="the cli's peer",
            accepts_commands_from=["peer"],
            config=ActioConfig(
                root=str(root), heartbeat_interval=30.0, announce_delay=0.01, stop_grace=0.5
            ),
        )
        for name, over in caps if caps is not None else [("echo", {})]:
            cap = cast("Capability", self.cap(name, **over))
            if failing:

                async def broken(params: Any, ctx: Any) -> Any:
                    raise ActioError(1001, "nope")

                agent.capability(cap, broken)
            else:
                agent.capability(cap, self.handler)
        return agent


class LsTest(CliTest):
    async def test_no_agents(self) -> None:
        with actio_tree() as root:
            with no_leftovers():
                code, out_txt, err_txt = await self.run_cli("ls", root=root)
            self.assertEqual(code, 0)
            self.assertEqual(out_txt, f"no agents under {root}\n")
            self.assertEqual(err_txt, "")

    async def test_one_started_agent_and_its_capabilities(self) -> None:
        with actio_tree() as root:
            with no_leftovers():
                agent = self.peer(
                    root,
                    caps=[("echo", {}), ("slow", {"disabled": True, "accepts_from": ["other"]})],
                )
                await agent.start()
                try:
                    code, out_txt, _ = await self.run_cli("ls", root=root)
                    status = json.loads((root / "peer" / "status.json").read_text(encoding="utf-8"))
                finally:
                    await agent.stop()
            self.assertEqual(code, 0)
            self.assertEqual(
                out_txt,
                f"peer  {status['status']}  healthy  {status['datetime']}  {root / 'peer'}\n"
                "    echo\n"
                "    slow [disabled] [accepts_from: other]\n",
            )

    async def test_the_json_object(self) -> None:
        with actio_tree() as root:
            with no_leftovers():
                agent = self.peer(root)
                await agent.start()
                try:
                    code, out_txt, _ = await self.run_cli("ls", "--json", root=root)
                finally:
                    await agent.stop()
            self.assertEqual(code, 0)
            doc = json.loads(out_txt)
            self.assertEqual(doc["root"], str(root))
            self.assertEqual(doc["counts"], {"agents": 1, "duplicates": 0, "ignored": 0})
            self.assertEqual(doc["duplicates"], [])
            self.assertEqual(doc["ignored"], [])
            (row,) = doc["agents"]
            self.assertEqual(row["id"], "peer")
            self.assertEqual(row["folder"], str(root / "peer"))
            self.assertEqual(row["status"], "idle")
            self.assertEqual(row["health"], "healthy")
            self.assertEqual(
                row["capabilities"], [{"name": "echo", "disabled": False, "accepts_from": []}]
            )

    async def test_an_unrated_age_prints_unknown(self) -> None:
        with actio_tree() as root:
            folder = root / "napping"
            folder.mkdir()
            spec = build_spec(
                id="napping", description="d", accepts_commands_from=[], capabilities=[], now=TS
            )
            (folder / "spec.json").write_text(json.dumps(spec), encoding="utf-8")
            (folder / "status.json").write_text(
                json.dumps(build_status("napping", "inactive", TS)), encoding="utf-8"
            )
            with no_leftovers():
                code, out_txt, _ = await self.run_cli("ls", root=root)
            self.assertEqual(code, 0)
            self.assertEqual(out_txt, f"napping  inactive  unknown  {format_iso(TS)}  {folder}\n")

    async def test_duplicate_id_and_ignored_file(self) -> None:
        with actio_tree() as root:
            for folder in ("peer", "twin"):
                f = root / folder
                f.mkdir()
                spec = build_spec(
                    id="peer", description="d", accepts_commands_from=[], capabilities=[], now=TS
                )
                (f / "spec.json").write_text(json.dumps(spec), encoding="utf-8")
                (f / "status.json").write_text(
                    json.dumps(build_status("peer", "idle", TS)), encoding="utf-8"
                )
            junk = root / "junk"
            junk.mkdir()
            (junk / "spec.json").write_text("{ not json", encoding="utf-8")
            (junk / "status.json").write_text(
                json.dumps(build_status("junk", "idle", TS)), encoding="utf-8"
            )
            with no_leftovers():
                code, out_txt, _ = await self.run_cli("ls", root=root)
            self.assertEqual(code, 0)
            lines = out_txt.splitlines()
            self.assertIn(f"duplicate agent id: {root / 'twin'}", lines)
            self.assertIn(
                f"ignored: {junk / 'spec.json'}: malformed JSON: "
                "Expecting property name enclosed in double quotes: line 1 column 3 (char 2)",
                lines,
            )


class DiariumTest(CliTest):
    async def test_the_last_n_lines_raw(self) -> None:
        with actio_tree() as root:
            folder = root / "peer"
            folder.mkdir()
            (folder / "diarium.jsonl").write_text(
                "".join(json.dumps({"i": i}) + "\n" for i in range(5)), encoding="utf-8"
            )
            with no_leftovers():
                code, out_txt, _ = await self.run_cli("diarium", "peer", "-n", "2", root=root)
            self.assertEqual(code, 0)
            self.assertEqual(out_txt, '{"i": 3}\n{"i": 4}\n')

    async def test_indented_under_json(self) -> None:
        with actio_tree() as root:
            folder = root / "peer"
            folder.mkdir()
            (folder / "diarium.jsonl").write_text('{"i": 4}\n', encoding="utf-8")
            with no_leftovers():
                code, out_txt, _ = await self.run_cli(
                    "diarium", "peer", "-n", "1", "--json", root=root
                )
            self.assertEqual(code, 0)
            self.assertEqual(out_txt, json.dumps({"i": 4}, indent=2) + "\n")

    async def test_a_missing_agent_is_the_reference_failure(self) -> None:
        with actio_tree() as root:
            with no_leftovers():
                code, out_txt, err_txt = await self.run_cli("diarium", "ghost", root=root)
            self.assertEqual(code, 1)
            self.assertEqual(out_txt, "")
            path = str(root / "ghost" / "diarium.jsonl")
            self.assertEqual(
                err_txt,
                f"cannot read \"{path}\": [Errno 2] No such file or directory: '{path}'\n",
            )

    async def test_an_unparsable_line_is_printed_as_it_is(self) -> None:
        with actio_tree() as root:
            folder = root / "peer"
            folder.mkdir()
            (folder / "diarium.jsonl").write_text('{"i": 0}\nnot json\n', encoding="utf-8")
            with no_leftovers():
                code, out_txt, _ = await self.run_cli("diarium", "peer", root=root)
            self.assertEqual(code, 0)
            self.assertEqual(out_txt, '{"i": 0}\nnot json\n')


class CallTest(CliTest):
    async def test_a_roundtrip_to_a_live_agent(self) -> None:
        with actio_tree() as root:
            with no_leftovers():
                agent = self.peer(root)
                await agent.start()
                try:
                    code, out_txt, err_txt = await self.run_cli(
                        "call", "cli", "peer", "echo", '{"x": 1}', root=root
                    )
                finally:
                    await agent.stop()
            self.assertEqual(code, 0)
            self.assertEqual(out_txt, 'finalStatus: confirmed\nresult: {"echo": {"x": 1}}\n')
            self.assertEqual(err_txt, "cli: outreq\n")

    async def test_the_json_outcome(self) -> None:
        with actio_tree() as root:
            with no_leftovers():
                agent = self.peer(root)
                await agent.start()
                try:
                    code, out_txt, _ = await self.run_cli(
                        "--json", "call", "cli", "peer", "echo", "{}", root=root
                    )
                finally:
                    await agent.stop()
            self.assertEqual(code, 0)
            doc = json.loads(out_txt)
            self.assertEqual(doc["finalStatus"], "confirmed")
            self.assertEqual(doc["response"]["result"], {"echo": {}})

    async def test_an_error_answer_is_an_answer(self) -> None:
        with actio_tree() as root:
            with no_leftovers():
                agent = self.peer(root, failing=True)
                await agent.start()
                try:
                    code, out_txt, _ = await self.run_cli(
                        "call", "cli", "peer", "echo", "{}", root=root
                    )
                finally:
                    await agent.stop()
            self.assertEqual(code, 0)
            self.assertEqual(out_txt, 'finalStatus: confirmed\nerror: 1001 "nope"\n')

    async def test_a_peer_that_never_answers(self) -> None:
        with actio_tree() as root:
            (root / "peer" / "inbox").mkdir(parents=True)
            with no_leftovers():
                code, out_txt, _ = await self.run_cli(
                    "call",
                    "cli",
                    "peer",
                    "echo",
                    "{}",
                    "--timeout",
                    "0.3",
                    root=root,
                    ACTIO_CANCEL_WAIT="0.3",
                )
            self.assertEqual(code, 1)
            self.assertEqual(out_txt, "finalStatus: abandoned\nresponse: none\n")


class CmdTest(CliTest):
    async def test_the_rid_line_and_exit_zero(self) -> None:
        with actio_tree() as root:
            (root / "peer" / "inbox").mkdir(parents=True)
            with no_leftovers():
                code, out_txt, _ = await self.run_cli("cmd", "cli", "peer", "stop", "{}", root=root)
            self.assertEqual(code, 0)
            (line,) = out_txt.splitlines()
            self.assertRegex(line, r"^rid: [0-9A-HJKMNP-TV-Z]{26}$")


class BroadcastTest(CliTest):
    async def test_the_signal_file_and_the_roundtrip(self) -> None:
        with actio_tree() as root:
            with no_leftovers():
                code, out_txt, _ = await self.run_cli("broadcast", "hello", root=root)
            self.assertEqual(code, 0)
            target = root / "nuntium" / "nuntium.json"
            self.assertEqual(out_txt, f"wrote {target}\n")
            doc = json.loads(target.read_text(encoding="utf-8"))
            self.assertEqual(doc["id"], "broadcast")
            self.assertEqual(doc["nuntium"], "hello")
            self.assertRegex(doc["datetime"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
            self.assertIsInstance(doc["ts"], int)
            # the round trip: what `broadcast` writes, the name inference reads as `announce`
            self.assertEqual(file_kind("nuntium.json"), "announce")
            with no_leftovers():
                code2, out2, _ = await self.run_cli("validate", str(target), root=root)
            self.assertEqual(code2, 0)
            self.assertEqual(out2, "OK announce\n")


def _header(rid: str, src: str, dst: str) -> dict[str, Any]:
    return {
        "rid": rid,
        "src": src,
        "dst": dst,
        "datetime": format_iso(TS),
        "type": "req",
        "actio_version": "0.1",
    }


class ValidateTest(CliTest):
    def documents(self) -> tuple[str, dict[str, str]]:
        """A valid document for every kind, under the name that infers it, and the request's rid."""
        rid = ulid()
        n1, n2, n3, n4 = nonce(), nonce(), nonce(), nonce()
        request = {
            **_header(rid, "a", "b"),
            "request": {
                "ts": int(TS),
                "nonce": n1,
                "signature": None,
                "name": "echo",
                "parameters": {},
                "timeout": 60,
                "attachments": [],
            },
        }
        cancellation = {
            **_header(rid, "a", "b"),
            "cancellation": {
                "ts": int(TS),
                "nonce": n2,
                "signature": None,
                "name": "echo",
                "force": False,
            },
        }
        response = {
            **_header(rid, "b", "a"),
            "response": {
                "ts": int(TS),
                "nonce": n3,
                "signature": None,
                "error": False,
                "cancellation": False,
                "result": {"ok": True},
                "attachments": [],
            },
        }
        confirmation = {
            **_header(rid, "a", "b"),
            "confirmation": {"ts": int(TS), "nonce": n4, "signature": None},
        }
        announce = {"id": "a", "datetime": format_iso(TS), "ts": int(TS), "nuntium": "hi"}
        return rid, {
            "spec.json": json.dumps(
                build_spec(
                    id="a", description="d", accepts_commands_from=[], capabilities=[], now=TS
                )
            ),
            "status.json": json.dumps(build_status("a", "idle", TS)),
            "actio.json": json.dumps({"description": "the team"}),
            f"{rid}.json": json.dumps(request),
            f"{rid}.can.json": json.dumps(cancellation),
            f"{rid}.res.json": json.dumps(response),
            f"{rid}.con.json": json.dumps(confirmation),
            "a.n.json": json.dumps(announce),
            os.path.join("nuntium", "nuntium.json"): json.dumps(announce),
            "diarium.jsonl": (
                json.dumps(status_event("a", TS, "idle"))
                + "\n"
                + json.dumps(log_event("a", TS, "hi"))
                + "\n"
            ),
        }

    async def test_ok_for_each_kind_by_name(self) -> None:
        with actio_tree() as root:
            rid, docs = self.documents()
            for name, text in docs.items():
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).write_text(text, encoding="utf-8")
            for name, expected in [
                ("spec.json", "spec"),
                ("status.json", "status"),
                ("actio.json", "actio"),
                ("a.n.json", "announce"),
                (os.path.join("nuntium", "nuntium.json"), "announce"),
                (f"{rid}.json", "request"),
                (f"{rid}.can.json", "cancellation"),
                (f"{rid}.res.json", "response"),
                (f"{rid}.con.json", "confirmation"),
            ]:
                with no_leftovers():
                    code, out_txt, _ = await self.run_cli("validate", str(root / name), root=root)
                self.assertEqual(code, 0, name)
                self.assertEqual(out_txt, f"OK {expected}\n", name)
            with no_leftovers():
                code, out_txt, _ = await self.run_cli(
                    "validate", str(root / "diarium.jsonl"), root=root
                )
            self.assertEqual(code, 0)
            self.assertEqual(out_txt, "OK diarium (2 lines)\n")

    async def test_as_overrides_the_name(self) -> None:
        with actio_tree() as root:
            doc = json.dumps(build_status("a", "idle", TS))
            (root / "renamed.json").write_text(doc, encoding="utf-8")
            with no_leftovers():
                code, out_txt, _ = await self.run_cli(
                    "validate", str(root / "renamed.json"), "--as", "status", root=root
                )
            self.assertEqual(code, 0)
            self.assertEqual(out_txt, "OK status\n")

    async def test_a_bad_document_names_the_kind_and_the_first_error(self) -> None:
        with actio_tree() as root:
            (root / "broken.json").write_text('{"id": "a"}', encoding="utf-8")
            with no_leftovers():
                code, out_txt, err_txt = await self.run_cli(
                    "validate", str(root / "broken.json"), root=root
                )
            self.assertEqual(code, 1)
            self.assertEqual(out_txt, "")
            self.assertEqual(len(err_txt.splitlines()), 1)
            self.assertTrue(err_txt.startswith("request: "), err_txt)

    async def test_a_diarium_reports_the_offending_line(self) -> None:
        with actio_tree() as root:
            good = json.dumps(status_event("a", TS, "idle"))
            (root / "diarium.jsonl").write_text(f"{good}\nnot json\n", encoding="utf-8")
            with no_leftovers():
                code, out_txt, err_txt = await self.run_cli(
                    "validate", str(root / "diarium.jsonl"), root=root
                )
            self.assertEqual(code, 1)
            self.assertEqual(out_txt, "")
            self.assertIn("diarium: line 2 is not valid JSON:", err_txt)

    async def test_no_validator_for_the_name(self) -> None:
        with actio_tree() as root:
            (root / "notes.txt").write_text("{}", encoding="utf-8")
            with no_leftovers():
                code, out_txt, err_txt = await self.run_cli(
                    "validate", str(root / "notes.txt"), root=root
                )
            self.assertEqual(code, 1)
            self.assertEqual(out_txt, "")
            self.assertEqual(
                err_txt,
                'no validator for "notes.txt": use --as '
                "actio|spec|status|announce|request|cancellation|response|confirmation|diarium\n",
            )


class UsageTest(CliTest):
    async def test_no_subcommand(self) -> None:
        with actio_tree() as root:
            with no_leftovers():
                code, out_txt, err_txt = await self.run_cli(root=root)
            self.assertEqual(code, 1)
            self.assertEqual(out_txt, "")
            self.assertEqual(err_txt, USAGE + "\n")

    async def test_an_unknown_subcommand(self) -> None:
        with actio_tree() as root:
            with no_leftovers():
                code, _, err_txt = await self.run_cli("bogus", root=root)
            self.assertEqual(code, 1)
            self.assertEqual(err_txt, f'unknown subcommand "bogus"\n{USAGE}\n')

    async def test_missing_positionals(self) -> None:
        with actio_tree() as root:
            for args, message in [
                (["diarium"], "diarium needs an agent id"),
                (
                    ["call", "a", "b"],
                    "call needs <src-id> <dst> <capability> <json-params> [--timeout s]",
                ),
                (["cmd", "a"], "cmd needs <src-id> <dst> <name> [json-params]"),
                (["broadcast"], 'broadcast needs "<text>"'),
                (["validate"], "validate needs <file>"),
            ]:
                with no_leftovers():
                    code, _, err_txt = await self.run_cli(*args, root=root)
                self.assertEqual(code, 1, args)
                self.assertEqual(err_txt, f"{message}\n{USAGE}\n", args)

    async def test_timeout_zero_and_n_zero_are_usage_errors(self) -> None:
        with actio_tree() as root:
            for args, message in [
                (
                    ["call", "a", "b", "c", "{}", "--timeout", "0"],
                    '--timeout must be > 0 seconds, got "0"',
                ),
                (
                    ["call", "a", "b", "c", "{}", "--timeout", "-1"],
                    '--timeout must be > 0 seconds, got "-1"',
                ),
                (["diarium", "x", "-n", "0"], '-n must be a positive integer, got "0"'),
                (["diarium", "x", "-n", "abc"], '-n must be a number >= 0, got "abc"'),
            ]:
                with no_leftovers():
                    code, _, err_txt = await self.run_cli(*args, root=root)
                self.assertEqual(code, 1, args)
                self.assertEqual(err_txt, f"{message}\n{USAGE}\n", args)


class SubprocessTest(unittest.IsolatedAsyncioTestCase):
    """The real process: `__main__.py` and `[project.scripts]`, not just the import."""

    def _env(self, root: Path) -> dict[str, str]:
        return {**os.environ, "ACTIO_DIR": str(root)}

    def test_python_minus_m_actio(self) -> None:
        with actio_tree() as root:
            proc = subprocess.run(
                [sys.executable, "-m", "actio", "ls"],
                env=self._env(root),
                capture_output=True,
                text=True,
            )
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, f"no agents under {root}\n")
        self.assertEqual(proc.stderr, "")

    def test_the_console_script(self) -> None:
        script = Path(sys.executable).parent / "actio"
        self.assertTrue(script.exists(), "the venv's console script is missing")
        with actio_tree() as root:
            proc = subprocess.run(
                [str(script), "ls"], env=self._env(root), capture_output=True, text=True
            )
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, f"no agents under {root}\n")


if __name__ == "__main__":
    unittest.main()
