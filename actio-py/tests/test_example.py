"""`examples/echo_agent.py`: the conformance target, driven as a black box.

Every row spawns the example as a child process on a `helpers.actio_tree()` root and drives it
with a caller built from the library in this process. A child is always reaped inside the tree's
context and its pipes drained (`communicate`), so no child outlives its row and no interpreter
noise leaks out. Every wait has a bound: `WAIT`, never the runner's timeout.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import unittest
from pathlib import Path
from typing import Any, cast

from actio import ActioAgent
from actio._config import ActioConfig
from actio._diarium import append, read_diarium
from actio._files import make_dirs
from actio._protocol import DiariumEntry, Spec, StatusState
from actio._requestor import Requestor
from actio._sync import create_sync_provider
from actio._types import ActioEvent, AgentFolders, RequestOutcome
from actio._watch import Watcher
from helpers import ActioTestCase, actio_tree, no_leftovers, spawn

PROJECT = Path(__file__).resolve().parent.parent
EXAMPLE = PROJECT / "examples" / "echo_agent.py"
WAIT = 15.0  # the bound of every wait in this file


class _Child:
    """The example as a child process: a clean ACTIO environment, pipes always drained."""

    def __init__(self, root: Path, **env: str) -> None:
        clean = {k: v for k, v in os.environ.items() if not k.startswith("ACTIO_")}
        self.proc = subprocess.Popen(
            [sys.executable, str(EXAMPLE)],
            env={**clean, "ACTIO_DIR": str(root), **env},
            cwd=str(PROJECT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def __enter__(self) -> _Child:
        return self

    def __exit__(self, *exc: object) -> None:
        # A child that outlives the tree recreates folders in a folder being deleted.
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.communicate(timeout=WAIT)  # drains and closes the pipes

    def alive(self) -> bool:
        return self.proc.poll() is None

    def reap(self, sig: int | None = None, timeout: float = WAIT) -> tuple[int, str, str]:
        if sig is not None and self.alive():
            self.proc.send_signal(sig)
        out, err = self.proc.communicate(timeout=timeout)
        return self.proc.returncode, out.decode(), err.decode()


async def wait_up(root: Path, agent_id: str, timeout: float = WAIT) -> Path:
    """The moment the child has published its spec and an `idle` status."""
    folder = root / agent_id
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        spec, status = folder / "spec.json", folder / "status.json"
        if spec.is_file() and status.is_file():
            if json.loads(status.read_text(encoding="utf-8")).get("status") == "idle":
                return folder
        await asyncio.sleep(0.05)
    raise AssertionError(f"the child never published an idle {agent_id} under {root}")


async def direct_request(
    root: Path, src: str, dst: str, name: str, params: Any, timeout: float
) -> RequestOutcome:
    """A lane B sender with no local callability check: the answer, if any, is the peer's own."""
    config = ActioConfig(root=str(root), announce_delay=0.01)
    folders = AgentFolders(
        root=str(root),
        agent=os.path.join(str(root), src),
        inbox=os.path.join(str(root), src, "inbox"),
        forum=os.path.join(str(root), "forum"),
        nuntium=os.path.join(str(root), "nuntium"),
    )
    await make_dirs(folders.agent)
    await make_dirs(folders.inbox)
    sync = create_sync_provider("none", spawn=spawn, on_error=lambda e: None)

    class _Vehicle:
        def __init__(self) -> None:
            self.config = config
            self.id = src
            self.folders = folders
            self.sync = sync

        def now(self) -> float:
            return config.clock()

        def spec(self) -> Spec:
            raise RuntimeError("a direct sender has no spec: it is not a started agent")

        def state(self) -> StatusState:
            return "idle"

        def emit(self, event: ActioEvent) -> None:
            pass

        async def append_diarium(self, entry: DiariumEntry) -> None:
            await append(folders.agent, entry, sync)

    class _Hooks:
        def on_watch_error(self, folder: str, error: BaseException) -> None:
            pass

        def on_state_change(self) -> None:
            pass

    watcher = await Watcher.open(spawn)
    requestor = Requestor(_Vehicle(), _Hooks(), watcher, spawn)
    try:
        return await requestor.request(dst, name, params, timeout=timeout)
    finally:
        await requestor.stop()
        await watcher.close()
        sync.close()


class ExampleTest(ActioTestCase):
    def make_caller(self, root: Path, name: str = "caller") -> ActioAgent:
        caller = ActioAgent(
            id=name,
            description="the test's caller",
            accepts_commands_from=[],
            config=ActioConfig(
                root=str(root),
                heartbeat_interval=30.0,
                announce_delay=0.01,
                cancel_wait_timeout=3.0,
                stop_grace=0.5,
            ),
        )
        self.addAsyncCleanup(caller.stop)
        return caller

    async def discover(self, caller: ActioAgent, peer: str, timeout: float = WAIT) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            await caller.refresh()
            if peer in caller.agmen():
                return
            await asyncio.sleep(0.05)
        self.fail(f"{peer} never appeared in {caller._id}'s view")

    async def settled(self, timeout: float = 5.0) -> None:
        """Until the agent's bookkeeping (the busy re-evaluation, the guarded publish) has drained:
        an exchange ends, and its last state change is still being written, when `request` returns."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            live = [
                t
                for t in asyncio.all_tasks()
                if t.get_name() in ("actio.busy", "actio.publish") and not t.done()
            ]
            if not live:
                return
            await asyncio.sleep(0.02)
        self.fail("the agent's bookkeeping did not settle")

    async def test_the_published_spec(self) -> None:
        with actio_tree() as root:
            with _Child(root) as child:
                folder = await wait_up(root, "echo")
                spec = json.loads((folder / "spec.json").read_text(encoding="utf-8"))
                self.assertEqual(spec["id"], "echo")
                self.assertEqual(spec["description"], "conformance target agent (echo)")
                self.assertEqual(spec["accepts_commands_from"], [])
                self.assertNotIn("namespaces", spec)  # absent by default, never []
                caps = [c for c in spec["capabilities"]]
                self.assertEqual(
                    [c["name"] for c in caps], ["echo", "sleep", "fail", "off", "private"]
                )
                by_name = {c["name"]: c for c in caps}
                with self.subTest(capability="echo"):
                    self.assertEqual(by_name["echo"]["description"], "echoes the text it gets")
                    self.assertEqual(
                        by_name["echo"]["parameters"],
                        {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"],
                            "additionalProperties": False,
                        },
                    )
                    self.assertEqual(
                        by_name["echo"]["actio"],
                        {
                            "result": {
                                "type": "object",
                                "properties": {"text": {"type": "string"}},
                                "required": ["text"],
                            }
                        },
                    )
                with self.subTest(capability="sleep"):
                    self.assertEqual(
                        by_name["sleep"]["parameters"],
                        {
                            "type": "object",
                            "properties": {"seconds": {"type": "number"}},
                            "required": ["seconds"],
                            "additionalProperties": False,
                        },
                    )
                    self.assertEqual(
                        by_name["sleep"]["actio"],
                        {
                            "result": {
                                "type": "object",
                                "properties": {"slept": {"type": "number"}},
                                "required": ["slept"],
                            },
                            "max_parallel": 1,
                        },
                    )
                with self.subTest(capability="fail"):
                    self.assertEqual(by_name["fail"]["actio"], {"result": True})
                with self.subTest(capability="off"):
                    self.assertEqual(by_name["off"]["actio"], {"result": True, "disabled": True})
                with self.subTest(capability="private"):
                    self.assertEqual(
                        by_name["private"]["actio"], {"result": True, "accepts_from": ["nobody"]}
                    )
                for cap in caps:  # the no-parameter three share one strict `{}` schema
                    if cap["name"] in ("fail", "off", "private"):
                        self.assertEqual(
                            cap["parameters"],
                            {"type": "object", "properties": {}, "additionalProperties": False},
                        )
                code, out, err = child.reap(signal.SIGTERM)
                self.assertEqual(code, 0)
                self.assertEqual(out, "")
                self.assertEqual(err, "")

    async def test_echo_answers_with_the_text_it_got(self) -> None:
        with actio_tree() as root:
            with _Child(root) as child:
                await wait_up(root, "echo")
                caller = self.make_caller(root)
                await caller.start()
                await self.discover(caller, "echo")
                with no_leftovers():
                    outcome = await caller.request("echo", "echo", {"text": "hello"})
                    await self.settled()
                self.assertEqual(outcome.final_status, "confirmed")
                assert outcome.response is not None
                self.assertIs(outcome.response["error"], False)
                self.assertEqual(outcome.response["result"], {"text": "hello"})
                code, _, _ = child.reap(signal.SIGTERM)
                self.assertEqual(code, 0)

    async def test_fail_answers_code_1000(self) -> None:
        with actio_tree() as root:
            with _Child(root) as child:
                await wait_up(root, "echo")
                caller = self.make_caller(root)
                await caller.start()
                await self.discover(caller, "echo")
                with no_leftovers():
                    outcome = await caller.request("echo", "fail", None)
                    await self.settled()
                self.assertEqual(outcome.final_status, "confirmed")
                assert outcome.response is not None
                self.assertIs(outcome.response["error"], True)
                self.assertEqual(
                    outcome.response["result"], {"code": 1000, "msg": "fail: requested failure"}
                )
                code, _, _ = child.reap(signal.SIGTERM)
                self.assertEqual(code, 0)

    async def test_private_is_refused_by_the_peers_spec_with_103(self) -> None:
        # A full agent would refuse locally (rule 11, from the peer's spec); a direct sender has
        # no such check, so the 103 that comes back is the peer's own verdict on its spec.
        with actio_tree() as root:
            with _Child(root) as child:
                await wait_up(root, "echo")
                with no_leftovers():
                    outcome = await direct_request(
                        root, "stranger", "echo", "private", {}, timeout=5.0
                    )
                    await self.settled()
                self.assertEqual(outcome.final_status, "confirmed")
                assert outcome.response is not None
                result = cast("dict[str, Any]", outcome.response["result"])
                self.assertIs(outcome.response["error"], True)
                self.assertEqual(result["code"], 103)
                self.assertIn("private", result["msg"])
                code, _, _ = child.reap(signal.SIGTERM)
                self.assertEqual(code, 0)

    async def test_a_cancelled_sleep_wakes_and_frees_its_slot(self) -> None:
        with actio_tree() as root:
            with _Child(root) as child:
                await wait_up(root, "echo")
                caller = self.make_caller(root)
                await caller.start()
                await self.discover(caller, "echo")
                with no_leftovers():
                    started = time.monotonic()
                    # 2.0, not 0.5: the envelope's `ts` is whole-second, so a sub-second timeout
                    # can read as already expired on the peer (code 108) a moment after the write.
                    outcome = await caller.request("echo", "sleep", {"seconds": 5}, timeout=2.0)
                    self.assertLessEqual(time.monotonic() - started, 6.0)
                    await self.settled()
                self.assertIn(outcome.final_status, ("cancelled", "abandoned"))
                if outcome.final_status == "abandoned":
                    self.assertIsNone(outcome.response)
                self.assertTrue(child.alive())  # the example is still running
                read = await read_diarium(str(root / "echo"))
                cancelled = [
                    line
                    for line in read.lines
                    if line["event"] == "inreq"
                    and cast("dict[str, Any]", line["event_data"]).get("final_status")
                    == "cancelled"
                ]
                self.assertTrue(cancelled, "the child's diarium owes a cancelled inreq line")
                # max_parallel is 1: a second sleep only answers if the first one woke.
                with no_leftovers():
                    second = await caller.request("echo", "sleep", {"seconds": 0.2}, timeout=3.0)
                    await self.settled()
                self.assertEqual(second.final_status, "confirmed")
                assert second.response is not None
                self.assertEqual(second.response["result"], {"slept": 0.2})
                code, _, _ = child.reap(signal.SIGTERM)
                self.assertEqual(code, 0)

    async def test_the_noop_command_goes_through(self) -> None:
        with actio_tree() as root:
            with _Child(root, ACTIO_COMMANDERS="caller") as child:
                await wait_up(root, "echo")
                caller = self.make_caller(root)
                await caller.start()
                await self.discover(caller, "echo")
                with no_leftovers():
                    rid = await caller.command("echo", "noop")
                    await self.settled()
                self.assertRegex(rid, r"^[0-9A-HJKMNP-TV-Z]{26}$")
                self.assertTrue(child.alive())
                code, _, _ = child.reap(signal.SIGTERM)
                self.assertEqual(code, 0)

    async def test_sigterm_is_a_graceful_stop_and_exit_zero(self) -> None:
        with actio_tree() as root:
            with _Child(root) as child:
                folder = await wait_up(root, "echo")
                code, out, err = child.reap(signal.SIGTERM)
                self.assertEqual(code, 0)
                self.assertEqual(out, "")
                self.assertEqual(err, "")
                status = json.loads((folder / "status.json").read_text(encoding="utf-8"))
                self.assertEqual(status["status"], "inactive")

    async def test_the_environment_moves_the_folder_and_the_commanders(self) -> None:
        with actio_tree() as root:
            with _Child(root, ACTIO_AGENT_ID="other", ACTIO_COMMANDERS="caller,cmdr") as child:
                folder = await wait_up(root, "other")
                self.assertEqual(folder, root / "other")
                self.assertFalse((root / "echo").exists())
                spec = json.loads((folder / "spec.json").read_text(encoding="utf-8"))
                self.assertEqual(spec["id"], "other")
                self.assertEqual(spec["accepts_commands_from"], ["caller", "cmdr"])
                caller = self.make_caller(root)
                await caller.start()
                await self.discover(caller, "other")
                with no_leftovers():
                    rid = await caller.command("other", "noop")
                    await self.settled()
                self.assertRegex(rid, r"^[0-9A-HJKMNP-TV-Z]{26}$")
                self.assertTrue(child.alive())
                code, _, _ = child.reap(signal.SIGTERM)
                self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
