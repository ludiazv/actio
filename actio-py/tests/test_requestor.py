"""`_requestor.py`: the outbound lane — B1 through B5, D-6, D-11, D-13, D-18, D-22, and the two kinds
of cancellation.

The peer is the test: it writes responses, cancellations and archives into the real destination inbox,
and the watcher is the real one, so the timing that matters is the one the lane is built on.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import importlib
import json
import os
import shutil
import time
import unittest
import unittest.mock as mock
from collections.abc import Coroutine
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

from actio._config import load_config
from actio._diarium import append, read_diarium
from actio._protocol import DiariumEntry, ResponseBody, Spec, StatusState
from actio._requestor import Requestor
from actio._sync import create_sync_provider
from actio._types import ActioEvent, AgentFolders, LogEvent, OutreqEvent
from actio._util import format_iso, nonce
from actio._validate import SchemaKind, validate
from actio._watch import Watcher
from helpers import ActioTestCase, actio_tree, no_leftovers, task_names

AUDIT = 0.1  # watcher audit interval: generous on purpose, as in test_watch.py
CANCEL_WAIT = 0.3  # config cancel_wait_timeout for the tests
TIMEOUT = 0.25  # the call timeouts
WAKE = 5.0  # ceilings for waiting on outcomes: a failure, not a timing expectation


def spawn(coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
    return asyncio.get_running_loop().create_task(coro, name=name)


class FakeContext:
    """A structural `AgentContext` with an in-memory diarium; the forum is the real disk."""

    def __init__(self, root: Path, agent: str = "planner") -> None:
        self.config = load_config(
            root=str(root), cancel_wait_timeout=CANCEL_WAIT, confirm_timeout=1.0, sync="none"
        )
        self.id = agent
        self.folders = AgentFolders(
            root=str(root),
            agent=str(root / agent),
            inbox=str(root / agent / "inbox"),
            forum=str(root / "forum"),
            nuntium=str(root / "nuntium"),
        )
        self.sync = create_sync_provider("none", spawn=spawn)
        self.events: list[ActioEvent] = []
        # (entry, names in forum at the moment the entry was appended): pins ordering against B5
        self.diarium: list[tuple[DiariumEntry, list[str]]] = []

    def now(self) -> float:
        return time.time()

    def spec(self) -> Spec:
        raise AssertionError("the requestor does not read the spec")

    def state(self) -> StatusState:
        return "idle"

    def emit(self, event: ActioEvent) -> None:
        self.events.append(event)

    async def append_diarium(self, entry: DiariumEntry) -> None:
        forum = self.folders.forum
        self.diarium.append((entry, sorted(os.listdir(forum)) if os.path.isdir(forum) else []))


class FakeHooks:
    def __init__(self) -> None:
        self.watch_errors: list[tuple[str, BaseException]] = []
        self.state_changes = 0

    def on_watch_error(self, folder: str, error: BaseException) -> None:
        self.watch_errors.append((folder, error))

    def on_state_change(self) -> None:
        self.state_changes += 1


def make_response(req: dict[str, Any], **body_over: Any) -> dict[str, Any]:
    """A valid peer response whose header copies the request's (D-11)."""
    now = time.time()
    body: dict[str, Any] = {
        "ts": int(now),
        "nonce": nonce(),
        "signature": None,
        "error": False,
        "cancellation": False,
        "result": "done",
        "attachments": [],
        "work_time": 1,
    }
    body.update(body_over)
    return {
        "rid": req["rid"],
        "src": req["src"],
        "dst": req["dst"],
        "datetime": format_iso(now),
        "type": req["type"],
        "actio_version": req["actio_version"],
        "response": body,
    }


def read_envelope(folder: Path, rid: str, suffix: str = ".json") -> dict[str, Any]:
    return cast(
        "dict[str, Any]", json.loads((folder / f"{rid}{suffix}").read_text(encoding="utf-8"))
    )


async def write_response(inbox: Path, req: dict[str, Any], **body_over: Any) -> None:
    (inbox / f"{req['rid']}.res.json").write_text(
        json.dumps(make_response(req, **body_over)), encoding="utf-8"
    )


class RequestorTestCase(ActioTestCase):
    """The tree: our agent `planner`, a peer `summarizer`, the shared `forum`."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        tree = actio_tree()
        self.root = tree.__enter__()
        self.addCleanup(tree.__exit__, None, None, None)
        (self.root / "forum").mkdir()
        (self.root / "planner" / "inbox").mkdir(parents=True)
        self.peer_inbox = self.root / "summarizer" / "inbox"
        self.peer_inbox.mkdir(parents=True)
        self.ctx = FakeContext(self.root)
        self.hooks = FakeHooks()
        self.watcher = await Watcher.open(spawn, AUDIT)
        self.addAsyncCleanup(self.watcher.close)
        self.requestor = Requestor(self.ctx, self.hooks, self.watcher, spawn)

    async def _wait_for_files(self, folder: Path, count: int, ceiling: float = WAKE) -> list[Path]:
        async with asyncio.timeout(ceiling):
            while True:
                files = sorted(folder.glob("*.json"))
                if len(files) >= count:
                    return files
                await asyncio.sleep(0.01)

    async def _one_request_rid(self) -> str:
        (path,) = await self._wait_for_files(self.peer_inbox, 1)
        return path.name[: -len(".json")]

    def _outreq_lines(self) -> list[dict[str, Any]]:
        return [
            cast("dict[str, Any]", entry["event_data"])
            for entry, _ in self.ctx.diarium
            if entry["event"] == "outreq"
        ]


class B1Test(RequestorTestCase):
    async def test_b1_writes_the_request_file_and_copies_the_attachments(self) -> None:
        a = self.root / "a.txt"
        a.write_text("alpha", encoding="utf-8")
        b = self.root / "b.txt"
        b.write_text("beta", encoding="utf-8")
        t = asyncio.create_task(
            self.requestor.request(
                "summarizer", "summarize", {"text": "x"}, timeout=0, attachments=[str(a), str(b)]
            )
        )
        rid = await self._one_request_rid()
        req = read_envelope(self.peer_inbox, rid)
        self.assertIsNone(validate("request", req))
        self.assertEqual(req["dst"], "summarizer")
        self.assertEqual(req["src"], "planner")
        self.assertEqual(req["request"]["name"], "summarize")
        self.assertEqual(req["request"]["attachments"], ["a.txt", "b.txt"])
        # the attachments sit in forum under their names, with the peer's content
        self.assertEqual((self.root / "forum" / "a.txt").read_text(encoding="utf-8"), "alpha")
        self.assertEqual((self.root / "forum" / "b.txt").read_text(encoding="utf-8"), "beta")
        await write_response(self.peer_inbox, req)
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "confirmed")

    async def test_b1_peer_named_paths_land_inside_the_forum_under_their_basename(self) -> None:
        deep = self.root / "deep" / "deeper"
        deep.mkdir(parents=True)
        paper = deep / "paper.pdf"
        paper.write_bytes(b"%PDF-1.4")
        t = asyncio.create_task(
            self.requestor.request("summarizer", "summarize", timeout=0, attachments=[str(paper)])
        )
        rid = await self._one_request_rid()
        req = read_envelope(self.peer_inbox, rid)
        self.assertEqual(req["request"]["attachments"], ["paper.pdf"])
        await write_response(self.peer_inbox, req)
        await asyncio.wait_for(t, WAKE)
        (_, forum_at_line) = next(e for e in self.ctx.diarium if e[0]["event"] == "outreq")
        self.assertIn("paper.pdf", forum_at_line)  # the copy sat in forum under its bare name
        self.assertFalse((self.root / "forum" / "deep").exists())  # no peer-named path inside forum
        self.assertFalse((self.root / "forum" / "paper.pdf").exists())  # B5 took it out again

    async def test_b1_refuses_a_basename_collision_locally(self) -> None:
        a = self.root / "x" / "same.txt"
        a.parent.mkdir()
        a.write_text("one", encoding="utf-8")
        b = self.root / "y" / "same.txt"
        b.parent.mkdir()
        b.write_text("two", encoding="utf-8")
        with self.assertRaises(ValueError) as cm:
            await self.requestor.request("summarizer", "summarize", attachments=[str(a), str(b)])
        message = str(cm.exception)
        self.assertIn(str(a), message)
        self.assertIn(str(b), message)
        self.assertEqual(list(self.peer_inbox.glob("*.json")), [])  # no request went out
        self.assertFalse((self.root / "forum" / "same.txt").exists())  # nothing was copied


class B2Test(RequestorTestCase):
    async def test_b2_wakes_on_the_response_file_and_confirms(self) -> None:
        t = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=0))
        rid = await self._one_request_rid()
        await asyncio.sleep(0.1)  # let the exchange settle into the watch
        req = read_envelope(self.peer_inbox, rid)
        await write_response(self.peer_inbox, req)
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "confirmed")
        body = cast("ResponseBody", outcome.response)
        self.assertIsNotNone(body)
        self.assertEqual(body["result"], "done")

    async def test_a_mismatched_header_is_ignored_with_a_log_and_the_exchange_times_out(
        self,
    ) -> None:
        t = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=TIMEOUT))
        rid = await self._one_request_rid()
        req = read_envelope(self.peer_inbox, rid)
        bad = make_response(req)
        bad["src"] = "imposter"  # the header differs from the request's
        (self.peer_inbox / f"{rid}.res.json").write_text(json.dumps(bad), encoding="utf-8")
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "abandoned")
        # exactly one `log`, in the events and in the diarium, naming the file and the rule (D-11)
        logs = [
            e for e in self.ctx.events if isinstance(e, LogEvent) and "header mismatch" in e.message
        ]
        self.assertEqual(len(logs), 1)
        diarium_logs = [
            entry
            for entry, _ in self.ctx.diarium
            if entry["event"] == "log" and "header mismatch" in str(entry["event_data"])
        ]
        self.assertEqual(len(diarium_logs), 1)
        # the exchange ran to its timer: B4, then the give-up
        self.assertTrue((self.peer_inbox / f"{rid}.can.json").exists())

    async def test_a_broken_peer_file_is_a_stay_until_the_timer(self) -> None:
        t = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=TIMEOUT))
        rid = await self._one_request_rid()
        (self.peer_inbox / f"{rid}.res.json").write_text("not json at all", encoding="utf-8")
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "abandoned")
        # only the final outreq line: the broken file left no trace
        self.assertEqual([entry["event"] for entry, _ in self.ctx.diarium], ["outreq"])


class B3Test(RequestorTestCase):
    async def test_b3_the_confirmation_follows_the_response_and_b5_cleans_our_attachments(
        self,
    ) -> None:
        att = self.root / "att.txt"
        att.write_text("ours", encoding="utf-8")
        peer_att = self.root / "forum" / "res-att.txt"  # the peer's own response attachment
        peer_att.write_text("theirs", encoding="utf-8")
        t = asyncio.create_task(
            self.requestor.request("summarizer", "summarize", timeout=0, attachments=[str(att)])
        )
        rid = await self._one_request_rid()
        req = read_envelope(self.peer_inbox, rid)
        await write_response(self.peer_inbox, req, attachments=["res-att.txt"])
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "confirmed")
        # B3: the confirmation is in the peer's inbox, valid, same rid
        con = read_envelope(self.peer_inbox, rid, ".con.json")
        self.assertIsNone(validate("confirmation", con))
        self.assertEqual(con["rid"], rid)
        # the diarium line carries the con, and it went out before B5 deleted our attachment
        lines = self._outreq_lines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0]["final_status"], "confirmed")
        self.assertIsNotNone(lines[0]["con"])
        (entry, forum_at_line) = next(e for e in self.ctx.diarium if e[0]["event"] == "outreq")
        self.assertIn("att.txt", forum_at_line)
        # B5: our request attachment is gone; the peer's response attachment is not ours to delete
        self.assertFalse((self.root / "forum" / "att.txt").exists())
        self.assertTrue(peer_att.is_file())

    async def test_a_peer_that_archived_first_is_recorded_without_a_con(self) -> None:
        t = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=0))
        rid = await self._one_request_rid()
        req = read_envelope(self.peer_inbox, rid)
        (self.peer_inbox / f"{rid}.json").unlink()  # the receiver archived the request first
        await write_response(self.peer_inbox, req)  # ... and its response is what we find
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "confirmed")
        self.assertFalse((self.peer_inbox / f"{rid}.con.json").exists())
        lines = self._outreq_lines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0]["final_status"], "confirmed")
        self.assertIsNone(lines[0]["con"])


class B4Test(RequestorTestCase):
    async def test_b4_the_timeout_writes_our_cancellation_then_gives_up(self) -> None:
        att = self.root / "att.txt"
        att.write_text("x", encoding="utf-8")
        t = asyncio.create_task(
            self.requestor.request(
                "summarizer", "summarize", timeout=TIMEOUT, attachments=[str(att)]
            )
        )
        rid = await self._one_request_rid()
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "abandoned")
        self.assertIsNone(outcome.response)
        can = read_envelope(self.peer_inbox, rid, ".can.json")
        self.assertIsNone(validate("cancellation", can))
        self.assertEqual(can["rid"], rid)
        self.assertEqual(can["cancellation"]["name"], "summarize")
        self.assertFalse(can["cancellation"]["force"])
        lines = self._outreq_lines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0]["final_status"], "abandoned")
        self.assertIsNotNone(lines[0]["can"])
        self.assertFalse((self.root / "forum" / "att.txt").exists())  # B5 on the give-up

    async def test_a_cancellation_response_ends_the_exchange_cancelled(self) -> None:
        t = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=TIMEOUT))
        rid = await self._one_request_rid()
        await self._wait_for_files(self.peer_inbox, 2)  # the request, then the can
        req = read_envelope(self.peer_inbox, rid)
        # the receiver answers the cancellation: an acknowledgement, not an error
        await write_response(self.peer_inbox, req, cancellation=True, result=None)
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "cancelled")
        # confirmed as usual: the request file was still there, so the con went out
        con = read_envelope(self.peer_inbox, rid, ".con.json")
        self.assertIsNone(validate("confirmation", con))
        lines = self._outreq_lines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0]["final_status"], "cancelled")
        self.assertTrue(lines[0]["res"]["response"]["cancellation"])

    async def test_setting_cancel_starts_b4(self) -> None:
        cancel = asyncio.Event()
        t = asyncio.create_task(
            self.requestor.request("summarizer", "summarize", timeout=0, cancel=cancel)
        )
        rid = await self._one_request_rid()
        await asyncio.sleep(0.1)
        cancel.set()
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "abandoned")
        self.assertTrue((self.peer_inbox / f"{rid}.can.json").exists())


class CancelledCallerTest(RequestorTestCase):
    async def test_a_cancelled_caller_leaves_nothing_armed_and_the_exchange_ends(self) -> None:
        att = self.root / "att.txt"
        att.write_text("x", encoding="utf-8")
        t = asyncio.create_task(
            self.requestor.request("summarizer", "summarize", timeout=0, attachments=[str(att)])
        )
        rid = await self._one_request_rid()
        await asyncio.sleep(0.1)  # let it settle into the watch
        t.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await t
        with no_leftovers():
            async with asyncio.timeout(WAKE):
                while self.requestor._exchanges:
                    await asyncio.sleep(0.01)
        # the exchange ended in the background: its B4, its give-up, its diarium line, its B5
        self.assertTrue((self.peer_inbox / f"{rid}.can.json").exists())
        self.assertFalse((self.root / "forum" / "att.txt").exists())
        lines = self._outreq_lines()
        self.assertEqual([d["final_status"] for d in lines], ["abandoned"])

    async def test_a_cancelled_caller_still_watches_for_the_peer_s_late_answer(self) -> None:
        """Semantics 1, with the answer **late**: the caller's task goes away and the exchange ends in the
        background — and to end it, whoever is watching must still be watching the destination inbox
        (D-18: subscribed before the write, and the folder stays watched until the exchange ends). A peer
        that answers the cancellation after the caller is gone ends the exchange `cancelled` with its
        response on the line; the exchange task leaves the watch only then, so the watcher holds no
        folder afterwards.

        The cancel-wait timeout is deliberately long here: the loop re-polls the folder when it expires, so
        with the default the answer would be found by polling and the test would pass on an exchange
        task that watches nothing. At 8 s the answer has to arrive on a wake-up."""
        # the config is frozen: a copy with a long cancel wait (see the docstring: no polling rescue)
        self.ctx.config = replace(self.ctx.config, cancel_wait_timeout=8.0)
        t = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=0))
        rid = await self._one_request_rid()
        await asyncio.sleep(0.1)  # settled into the watch: the caller is about to go
        t.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await t
        await self._wait_for_files(self.peer_inbox, 2)  # our request, then our cancellation
        ex = self.requestor._exchanges[rid]
        # The exchange task is still the one watching the inbox.
        self.assertIsNotNone(ex.task)
        assert ex.task is not None  # for mypy: the line above already failed if it was
        self.assertFalse(ex.task.done())
        await asyncio.sleep(0.1)  # one pass, then it sleeps in the watch: the answer must wake it
        req = read_envelope(self.peer_inbox, rid)
        await write_response(self.peer_inbox, req, cancellation=True, result=None)
        with no_leftovers():
            async with asyncio.timeout(WAKE):
                while self.requestor._exchanges:
                    await asyncio.sleep(0.01)
        lines = self._outreq_lines()
        self.assertEqual([d["final_status"] for d in lines], ["cancelled"])
        self.assertTrue(lines[0]["res"]["response"]["cancellation"])
        # The exchange task released the folder it watched.
        self.assertEqual(self.watcher._folders, {})

    async def test_a_live_caller_releases_the_folder_it_watched(self) -> None:
        """The other half: an exchange the caller *waited for* releases its subscription too, so the
        watcher holds no folder once it ends (`unsubscribe` is called by whoever holds the subscription)."""
        t = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=TIMEOUT))
        rid = await self._one_request_rid()
        req = read_envelope(self.peer_inbox, rid)
        self.assertEqual(list(self.watcher._folders), [str(self.peer_inbox)])
        await write_response(self.peer_inbox, req, result="done")
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "confirmed")
        self.assertEqual(self.watcher._folders, {})

    async def test_a_failed_self_cancellation_does_not_strand_the_exchange(self) -> None:
        real = validate

        def planted(kind: SchemaKind, value: Any) -> str | None:
            if kind == "cancellation":
                return "(root) a planted rejection"
            return real(kind, value)

        with mock.patch("actio._requestor.validate", side_effect=planted):
            t = asyncio.create_task(
                self.requestor.request("summarizer", "summarize", timeout=TIMEOUT)
            )
            rid = await self._one_request_rid()
            outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "abandoned")
        self.assertEqual(self.requestor._exchanges, {})
        self.assertFalse((self.peer_inbox / f"{rid}.can.json").exists())
        logs = [
            e for e in self.ctx.events if isinstance(e, LogEvent) and "cancellation" in e.message
        ]
        self.assertEqual(len(logs), 1)
        lines = self._outreq_lines()
        self.assertEqual([d["final_status"] for d in lines], ["abandoned"])
        self.assertIsNone(lines[0]["can"])


class CommandTest(RequestorTestCase):
    async def test_command_writes_and_forgets(self) -> None:
        rid = await self.requestor.command("summarizer", "stop", {"force": False})
        self.assertEqual(len(rid), 26)
        cmd = read_envelope(self.peer_inbox, rid)
        self.assertIsNone(validate("request", cmd))
        self.assertEqual(cmd["type"], "cmd")
        self.assertEqual(cmd["request"]["name"], "stop")
        self.assertEqual(cmd["request"]["timeout"], 0)
        # B1: for `type="cmd"` the lane appends an `outreq` event with `final_status="sent"` and only
        # `req`. A command is never answered, so that line is the only record of it there will ever be.
        lines = self._outreq_lines()
        self.assertEqual([line["final_status"] for line in lines], ["sent"])
        self.assertEqual(lines[0]["req"]["rid"], rid)
        self.assertEqual(lines[0]["req"]["request"]["name"], "stop")
        for key in ("can", "res", "con"):
            self.assertIsNone(lines[0][key])
        events = [e for e in self.ctx.events if isinstance(e, OutreqEvent)]
        self.assertEqual([(e.rid, e.final_status) for e in events], [(rid, "sent")])
        # and it is still not tracked: no exchange, no watcher folder, nothing armed
        self.assertEqual(self.requestor._exchanges, {})
        self.assertEqual(self.watcher._folders, {})


class OrderTest(RequestorTestCase):
    """B3 and B5 in the protocol's order: the `outreq` line, the event, the confirmation, then the
    attachments, which are deleted *after* the confirmation (*Envelope lifecycle*, B5)."""

    async def test_the_line_the_event_the_confirmation_then_the_attachments(self) -> None:
        order: list[str] = []
        forum = str(self.ctx.folders.forum)
        module = importlib.import_module("actio._requestor")
        real_write, real_remove = module.write_json_atomic, module.remove

        async def write(path: str, obj: Any, tmp_dir: str, provider: Any) -> None:
            if path.endswith(".con.json"):
                order.append("con")
            await real_write(path, obj, tmp_dir, provider)

        async def remove(path: str) -> None:
            if path.startswith(forum):
                order.append("delete")
            await real_remove(path)

        class Ordered(FakeContext):
            async def append_diarium(self, entry: DiariumEntry) -> None:
                order.append(f"line:{cast('dict[str, Any]', entry['event_data'])['final_status']}")

            def emit(self, event: ActioEvent) -> None:
                if isinstance(event, OutreqEvent):
                    order.append("event")
                super().emit(event)

        self.ctx.__class__ = Ordered
        attachment = self.root / "in.txt"
        attachment.write_text("body", encoding="utf-8")
        with (
            mock.patch.object(module, "write_json_atomic", write),
            mock.patch.object(module, "remove", remove),
        ):
            task = asyncio.create_task(
                self.requestor.request(
                    "summarizer",
                    "summarize",
                    {"t": "x"},
                    timeout=TIMEOUT,
                    attachments=[str(attachment)],
                )
            )
            req = (await self._wait_for_files(self.peer_inbox, 1))[0]
            await write_response(self.peer_inbox, json.loads(req.read_text(encoding="utf-8")))
            outcome = await asyncio.wait_for(task, WAKE)
        self.assertEqual(outcome.final_status, "confirmed")
        self.assertEqual(order, ["line:confirmed", "event", "con", "delete"])


class DiskContext(FakeContext):
    """The diarium on the real disk, through a `delay` provider.

    The append yields the loop after its thread has returned, which is the window a settlement can be
    raced in; `appending` says when it has begun, so the test needs no timing assumption."""

    appending: asyncio.Event

    async def append_diarium(self, entry: DiariumEntry) -> None:
        self.appending.set()  # the settlement is inside its append: the line is not on disk yet
        await asyncio.to_thread(time.sleep, 0.4)  # a slow disk: the thread is what takes the time
        await append(self.folders.agent, entry, self.sync)


class SettlementTest(RequestorTestCase):
    """A `stop` during a settlement (D-9): the `outreq` line the lane owes is on disk when `stop()`
    returns, and nothing is armed afterwards."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.ctx.__class__ = DiskContext
        ctx = cast("DiskContext", self.ctx)
        ctx.appending = asyncio.Event()
        self.disk = ctx
        self.agent_folder = str(self.root / "planner")

    async def _statuses_on_disk(self) -> list[str]:
        read = await read_diarium(self.agent_folder)
        return [
            cast("dict[str, Any]", entry["event_data"])["final_status"]
            for entry in read.lines
            if entry["event"] == "outreq"
        ]

    def _armed(self) -> set[str]:
        # the lane's own tasks: the exchanges and the settlements. The watcher's audit task is the
        # watcher's, and the test closes it.
        return {n for n in task_names() if n.startswith(("actio.out.", "actio.settle."))}

    async def test_a_stop_during_a_cancelled_callers_settlement_still_writes_the_line(self) -> None:
        caller = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=0))
        await self._wait_for_files(self.peer_inbox, 1)
        await asyncio.sleep(0.05)  # B1's write has landed and the lane has entered B2
        caller.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await caller
        # The exchange task went on without its caller: B4, the cancel wait, and inside the append now.
        await asyncio.wait_for(self.disk.appending.wait(), WAKE)
        await self.requestor.stop()
        self.assertEqual(await self._statuses_on_disk(), ["abandoned"])
        self.assertEqual(self.requestor._exchanges, {})
        self.assertEqual(self._armed(), set())

    async def test_a_stop_during_a_live_callers_settlement_still_writes_the_line(self) -> None:
        caller = asyncio.create_task(
            self.requestor.request("summarizer", "summarize", timeout=TIMEOUT)
        )
        await self._wait_for_files(self.peer_inbox, 1)
        await asyncio.sleep(0.05)  # B1's write has landed and the lane has entered B2
        # The timeout fires, B4 runs, the cancel wait expires: the live caller is mid-append.
        await asyncio.wait_for(self.disk.appending.wait(), WAKE)
        await self.requestor.stop()
        self.assertEqual(await self._statuses_on_disk(), ["abandoned"])
        self.assertEqual(self._armed(), set())
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(caller, WAKE)


class StopTest(RequestorTestCase):
    async def test_stop_abandons_every_outstanding_exchange(self) -> None:
        t1 = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=0))
        t2 = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=0))
        rids = {p.name[: -len(".json")] for p in await self._wait_for_files(self.peer_inbox, 2)}
        await self.requestor.stop()
        self.assertEqual(self.requestor._exchanges, {})
        lines = self._outreq_lines()
        self.assertEqual(len(lines), 2)
        self.assertEqual({d["final_status"] for d in lines}, {"abandoned"})
        self.assertEqual({d["req"]["rid"] for d in lines}, rids)
        # the callers' laps see the settled exchange and return its outcome; nothing left armed
        results = await asyncio.wait_for(asyncio.gather(t1, t2), WAKE)
        self.assertEqual([r.final_status for r in results], ["abandoned", "abandoned"])


class LostInboxTest(RequestorTestCase):
    async def test_a_lost_inbox_abandons_the_exchange(self) -> None:
        t = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=0))
        await self._one_request_rid()
        shutil.rmtree(self.peer_inbox.parent)  # the peer agent's folder goes away
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "abandoned")
        self.assertTrue(
            any(f"{os.sep}summarizer" in folder for folder, _ in self.hooks.watch_errors)
        )
        self.assertEqual(self.requestor._exchanges, {})
        self.assertEqual(len(self._outreq_lines()), 1)


class PeerAnswerTest(RequestorTestCase):
    async def test_a_peers_error_answer_is_not_an_exception(self) -> None:
        t = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=0))
        rid = await self._one_request_rid()
        req = read_envelope(self.peer_inbox, rid)
        await write_response(
            self.peer_inbox, req, error=True, result={"code": 2000, "msg": "no such document"}
        )
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "confirmed")
        body = cast("ResponseBody", outcome.response)
        self.assertIsNotNone(body)
        self.assertTrue(body["error"])
        self.assertEqual(cast("dict[str, Any]", body["result"])["code"], 2000)

    async def test_an_encrypted_response_confirms_without_a_body(self) -> None:
        t = asyncio.create_task(self.requestor.request("summarizer", "summarize", timeout=0))
        rid = await self._one_request_rid()
        req = read_envelope(self.peer_inbox, rid)
        res = make_response(req)
        res["response"] = base64.b64encode(json.dumps({"ts": 1}).encode()).decode()
        (self.peer_inbox / f"{rid}.res.json").write_text(json.dumps(res), encoding="utf-8")
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "confirmed")
        self.assertIsNone(outcome.response)


class SelfCallTest(RequestorTestCase):
    async def test_a_self_call_is_no_special_case(self) -> None:
        t = asyncio.create_task(self.requestor.request("planner", "summarize", timeout=0))
        own_inbox = self.root / "planner" / "inbox"
        (path,) = await self._wait_for_files(own_inbox, 1)
        rid = path.name[: -len(".json")]
        req = read_envelope(own_inbox, rid)
        await write_response(own_inbox, req)  # the receiver writes beside the request, as usual
        outcome = await asyncio.wait_for(t, WAKE)
        self.assertEqual(outcome.final_status, "confirmed")


if __name__ == "__main__":
    unittest.main()
