"""The end-to-end suite, scenarios E1–E9: the request scenarios, on real temporary `.actio` roots.

Two live `ActioAgent`s in one tree (alpha on the requestor side, beta on the receiver side),
through the public API only — no `actio._*` import, no patched internals. Every scenario asserts
the four standing properties: (a) the order of the files that appeared in the receiver's inbox,
(b) the inbox empty when the exchange is archived, (c) every diarium line of both agents
validates and the `inreq`/`outreq` `final_status` values, (d) attachments cleaned per *Envelope
lifecycle*. `assert_exchange` is those four; a scenario reads as its scenario.

The timing is real: the configs carry no fake `clock`, so every protocol `timeout` is alive, and
each value a row leans on is a named bound (`CALL_BOUND`, `STOP_BOUND`, `START_BOUND`, `SETTLE`)
that ends the row with an assertion naming it instead of a run that never returns.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
from pathlib import Path
from typing import Any, cast

from actio import (
    ActioAgent,
    ActioConfig,
    Capability,
    CapabilityContext,
    CapabilityHandler,
    CommandContext,
    DiariumRead,
    RequestOutcome,
    load_config,
    read_diarium,
)
from helpers import ActioTestCase, InboxRecorder, actio_tree, bounded, until

ALPHA = "alpha"
BETA = "beta"
GAMMA = "gamma"

# The bounds, named. The gate runs this suite eight copies at once, so a bound is a promise the
# row fails inside, not a timing expectation: real latency here is milliseconds to seconds.
CALL_BOUND = 15.0  # a caller's outcome; the 107 row adds 1 s of sleep, E8 adds 1 s of timeout
STOP_BOUND = (
    10.0  # an `agent.stop()`: stop_grace 1.0 + the receiver's phase bound + the watcher join
)
START_BOUND = 10.0  # an `agent.start()`: folders, publications, watchers, the first scan
SETTLE = 10.0  # a file event (an archive, an A0 answer) under parallel load
RESTART_BOUND = 15.0  # E13: a restart's return (delay 1.0 + a fresh start); a restart that never
# comes back fails inside this, in seconds, not at the protocol's mercy
SCAN_SETTLE = 3.0  # E11: the window a malformed file is left to the watcher before the row asserts
# the watcher left it alone (a negative assertion: bounded by design, named by design)

# A stale view: 30 s of `nuntium_debounce` keeps a view from rescan, so a row can act on what the
# last scan saw while the tree has moved (E2's phantom, E3's 100, E5's and E9's stopped peer).
STALE_VIEW_DEBOUNCE = 30.0


def iso_now() -> str:
    """The protocol's `datetime` shape: Unix time as `YYYY-MM-DDTHH:MM:SSZ`."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def doc(path: Path) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads(path.read_text(encoding="utf-8")))


def code_of(outcome: RequestOutcome) -> int:
    """The `code` of an error answer (*Acceptance errors*)."""
    body = outcome.response
    assert body is not None, "an error answer carries a response"
    assert body["error"] is True, f"the answer is not an error: {body}"
    return int(cast("dict[str, Any]", body["result"])["code"])


# --- the capabilities the scenarios serve ------------------------------------------


def cap(name: str, parameters: dict[str, Any], result: dict[str, Any], **actio: Any) -> Capability:
    return cast(
        Capability,
        {
            "name": name,
            "description": f"the {name} capability",
            "parameters": parameters,
            "actio": {"result": result, **actio},
        },
    )


_TEXT_PARAMS: dict[str, Any] = {
    "type": "object",
    "properties": {"text": {"type": "string"}},
    "required": ["text"],
    "additionalProperties": False,
}
_OK_RESULT: dict[str, Any] = {
    "type": "object",
    "properties": {"ok": {"type": "boolean"}},
    "required": ["ok"],
}
_EMPTY_PARAMS: dict[str, Any] = {"type": "object", "additionalProperties": False}

ECHO = cap(
    "echo",
    _TEXT_PARAMS,
    {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]},
)
SLEEP = cap(
    "sleep",
    {
        "type": "object",
        "properties": {"seconds": {"type": "number"}},
        "required": ["seconds"],
        "additionalProperties": False,
    },
    {"type": "object", "properties": {"slept": {"type": "number"}}, "required": ["slept"]},
    max_parallel=1,  # E3's 107 sub-row: one at a time
)
OFF = cap("off", _EMPTY_PARAMS, _OK_RESULT, disabled=True)  # E3's 102: disabled from the start
PRIVATE = cap("private", _EMPTY_PARAMS, _OK_RESULT, accepts_from=[GAMMA])  # E3's 103: gamma only
STRICT = cap("strict", _TEXT_PARAMS, _OK_RESULT)  # E3's 105: `text` is required
HOLD = cap("hold", _EMPTY_PARAMS, _OK_RESULT)  # E7: the test releases the handler


async def echo_handler(params: Any, ctx: CapabilityContext) -> dict[str, Any]:
    return {"text": f"echo: {cast('dict[str, Any]', params)['text']}"}


async def ok_handler(params: Any, ctx: CapabilityContext) -> dict[str, Any]:
    return {"ok": True}


def make_sleep_handler(calls: list[bool]) -> CapabilityHandler:
    """The `sleep` work, abortable through the context's `cancelled` event (the handler's
    `signal`): the row records whether the signal aborted it."""

    async def sleep_handler(params: Any, ctx: CapabilityContext) -> dict[str, Any]:
        seconds = cast("dict[str, Any]", params)["seconds"]
        loop = asyncio.get_running_loop()
        sleeper = loop.create_task(asyncio.sleep(float(seconds)), name="e2e.sleep")
        signaller = loop.create_task(ctx.cancelled.wait(), name="e2e.signal")
        try:
            await asyncio.wait({sleeper, signaller}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (sleeper, signaller):
                if not task.done():
                    task.cancel()
            await asyncio.gather(sleeper, signaller, return_exceptions=True)
        if ctx.cancelled.is_set():
            calls.append(True)
            raise asyncio.CancelledError()
        calls.append(False)
        return {"slept": seconds}

    return sleep_handler


def make_hold_handler(gate: asyncio.Event, calls: list[int]) -> CapabilityHandler:
    """The `hold` work: the handler runs until the test sets the gate (E7)."""

    async def hold_handler(params: Any, ctx: CapabilityContext) -> dict[str, Any]:
        calls.append(1)
        await gate.wait()
        return {"ok": True}

    return hold_handler


def full_handlers(
    sleep_calls: list[bool], hold_gate: asyncio.Event, hold_calls: list[int]
) -> dict[str, CapabilityHandler]:
    return {
        "echo": echo_handler,
        "sleep": make_sleep_handler(sleep_calls),
        "off": ok_handler,
        "private": ok_handler,
        "strict": ok_handler,
        "hold": make_hold_handler(hold_gate, hold_calls),
    }


# --- writing envelopes by hand: the protocol-level idiom ----------------------------
# A request the requestor's own local gate (rule 11 of *Callability rules*) refuses to send —
# 102/103 name capabilities the view already filters, 106's sender is a peer the callability
# rules discarded, 108's timeout is already spent — is written into the inbox as a file. The
# file is the protocol; the writer's identity is the envelope's `src`.


def write_atomic(path: Path, text: str) -> None:
    """A direct write in the library's own idiom (temp file + `os.replace`): the inbox sees
    one `IN_MOVED_TO` with the complete content, never a half-written envelope."""
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def write_request(
    root: Path,
    src: str,
    dst: str,
    name: str,
    parameters: dict[str, Any],
    *,
    timeout: float = 60.0,
    ts_offset: float = 0.0,
) -> str:
    """One request file into `dst`'s inbox, fresh `ts` by default: the answer must be the A0
    code, not 108. Returns the `rid`."""
    rid = "01JB7Q3W8K2M4N6P8R0S2T4V" + f"{secrets.randbelow(256):02X}"
    req = {
        "rid": rid,
        "src": src,
        "dst": dst,
        "datetime": iso_now(),
        "type": "req",
        "actio_version": "0.1",
        "request": {
            "ts": int(time.time() + ts_offset),
            "nonce": secrets.token_hex(16),
            "signature": None,
            "name": name,
            "parameters": parameters,
            "timeout": timeout,
            "attachments": [],
        },
    }
    write_atomic(root / dst / "inbox" / f"{rid}.json", json.dumps(req))
    return rid


def write_cancellation(root: Path, src: str, dst: str, rid: str, name: str) -> None:
    """One cancellation file into `dst`'s inbox, same header as the request (Appendix A.3)."""
    can = {
        "rid": rid,
        "src": src,
        "dst": dst,
        "datetime": iso_now(),
        "type": "req",
        "actio_version": "0.1",
        "cancellation": {
            "ts": int(time.time()),
            "nonce": secrets.token_hex(16),
            "signature": None,
            "name": name,
            "force": False,
        },
    }
    write_atomic(root / dst / "inbox" / f"{rid}.can.json", json.dumps(can))


def inbox_plain(root: Path, agent: str) -> list[str]:
    """The bare request files of one inbox (no `.res`/`.can`/`.con`)."""
    return [
        n
        for n in os.listdir(root / agent / "inbox")
        if n.endswith(".json") and not n.endswith((".res.json", ".can.json", ".con.json"))
    ]


def sleep_running(root: Path) -> int:
    for entry in doc(root / BETA / "status.json").get("capability_status", []):
        if entry.get("name") == "sleep":
            return int(entry.get("running", 0))
    return 0


def state_of(root: Path, agent: str) -> str:
    return str(doc(root / agent / "status.json")["status"])


def inreq_line(read: DiariumRead, rid: str) -> dict[str, Any]:
    """One archived `inreq` line for `rid`, or the test fails naming the `rid`."""
    for entry in read.lines:
        if entry["event"] != "inreq":
            continue
        data = entry["event_data"]
        if (
            isinstance(data, dict)
            and isinstance(data.get("req"), dict)
            and data["req"].get("rid") == rid
        ):
            return cast("dict[str, Any]", data)
    raise AssertionError(f"the exchange {rid} was not archived")


def diarium_lines(root: Path, agent: str) -> list[dict[str, Any]]:
    """The raw `diarium.jsonl` lines, in file order: the order the row asserts on.

    `read_diarium` validates and reports; these rows also need the file's own order (the archive
    before the `inactive` status line, the status line before the announce line), which the
    validated view does not expose. The lines are the protocol's files, read as files."""
    path = root / agent / "diarium.jsonl"
    if not path.exists():
        return []
    return [
        cast("dict[str, Any]", json.loads(line))
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def announce_lines(root: Path, agent: str) -> list[dict[str, Any]]:
    """The `announce` diarium lines of one agent: one per announcement, `event_data` null
    (the text lives in the announce file, not the line)."""
    return [line for line in diarium_lines(root, agent) if line["event"] == "announce"]


def announce_path(root: Path, agent: str) -> Path:
    """The agent's announce file, `nuntium/<agent_id>.n.json` (one fixed name per agent: a new
    announce is a rewrite of it, visible as new bytes)."""
    return root / "nuntium" / f"{agent}.n.json"


def inreq_final(root: Path, agent: str, rid: str) -> str | None:
    """The `final_status` of one agent's archived `inreq` line for `rid`, None while it is not
    on disk: the monotonic seam the rows wait on (a line, once written, stays)."""
    for line in diarium_lines(root, agent):
        if line["event"] != "inreq":
            continue
        data = line["event_data"]
        if (
            isinstance(data, dict)
            and isinstance(data.get("req"), dict)
            and data["req"].get("rid") == rid
        ):
            return str(data["final_status"])
    return None


def diarium_has_rid(root: Path, agent: str, rid: str) -> bool:
    """Any diarium line of `agent` mentioning `rid`, in any event: E11's ignored file must not
    have one."""
    return rid in (root / agent / "diarium.jsonl").read_text(encoding="utf-8")


def nuntium_names(root: Path) -> list[str]:
    """The announce set: the files in the shared `nuntium` folder, by name."""
    return sorted(os.listdir(root / "nuntium"))


def write_command(
    root: Path, src: str, dst: str, name: str, parameters: Any, *, malformed: bool = False
) -> str:
    """One command file into `dst`'s inbox, written by hand (the protocol-level idiom, atomic).

    `malformed` drops the body's `name`: the file then fails the request schema (*Strict format*)
    and is ignored before any gate — E11's third sub-row. Returns the `rid`."""
    rid = "01JB7Q3W8K2M4N6P8R0S2T4V" + f"{secrets.randbelow(256):02X}"  # a valid Crockford ULID
    body: dict[str, Any] = {
        "ts": int(time.time()),
        "nonce": secrets.token_hex(16),
        "signature": None,
        "parameters": parameters,
        "timeout": 0,
        "attachments": [],
    }
    if not malformed:
        body["name"] = name
    cmd = {
        "rid": rid,
        "src": src,
        "dst": dst,
        "datetime": iso_now(),
        "type": "cmd",
        "actio_version": "0.1",
        "request": body,
    }
    write_atomic(root / dst / "inbox" / f"{rid}.json", json.dumps(cmd))
    return rid


# --- the suite ----------------------------------------------------------------------


class E2ETest(ActioTestCase):
    """Two live agents in one temporary tree, talking through the folders.

    The tree outlives its agents — `addAsyncCleanup(agent.stop)` is the form here: every
    agent a row started is stopped before the temporary root is removed, and a second `stop()`
    is a no-op, so a row that stopped its agents already loses nothing. The inbox recorder is
    the same way: started by the row, stopped by `assert_exchange` (or the teardown if the row
    failed before it), which is what lets (a) read a record that includes the files the
    archive deletes a beat after they land.
    """

    def e2e_config(self, root: Path, **overrides: Any) -> ActioConfig:
        # Real clock: no `clock=`, so every protocol `timeout` is alive. The timers are shrunk
        # to `tests/test_agent.py`'s `config_for` values, with one load-driven change:
        # `heartbeat_interval` 0.1 -> 0.5. No row here asserts heartbeat freshness (that is
        # E19); 0.5 s keeps `status.json` current while eight copies of the suite run at once,
        # where a 0.1 s heartbeat is a write storm, not a timer.
        base: dict[str, Any] = {
            "root": str(root),
            "sync": "none",
            "heartbeat_interval": 0.5,
            "confirm_timeout": 1.0,  # E4 and E7 wait for it: a wait-for-timer, not a race
            "cancel_wait_timeout": 0.5,  # E9 waits for it after the cancellation
            "announce_delay": 0.01,  # a delay, not a race: `config_for`'s value
            "nuntium_debounce": 0.2,  # rescans; the stale-view rows override to 30 s
            "stop_grace": 1.0,  # D-9's phase (2), in-flight work: `config_for`'s value
        }
        base.update(overrides)
        return load_config(**base)

    def make_agent(
        self,
        root: Path,
        agent_id: str,
        *,
        accepts: list[str] | None = None,
        handlers: dict[str, CapabilityHandler] | None = None,
        namespaces: list[str | None] | None = None,
        **cfg: Any,
    ) -> ActioAgent:
        agent = ActioAgent(
            id=agent_id,
            description=f"{agent_id} e2e agent",
            accepts_commands_from=accepts if accepts is not None else [],
            namespaces=namespaces,
            config=self.e2e_config(root, **cfg),
        )
        for name, handler in (handlers or {}).items():
            agent.capability(
                {
                    "echo": ECHO,
                    "sleep": SLEEP,
                    "off": OFF,
                    "private": PRIVATE,
                    "strict": STRICT,
                    "hold": HOLD,
                }[name],
                handler,
            )
        self.addAsyncCleanup(agent.stop)
        return agent

    async def pair(
        self,
        root: Path,
        *,
        handlers: dict[str, CapabilityHandler] | None = None,
        nuntium_debounce: float = 0.2,
    ) -> tuple[ActioAgent, ActioAgent]:
        """`alpha` and `beta`, both serving the full set, mutually discovered. `beta` starts
        first: `alpha`'s initial scan (start step 4) is the discovery that runs in time, and it
        needs `beta` already published."""
        beta = self.make_agent(
            root, BETA, accepts=[ALPHA], handlers=handlers, nuntium_debounce=nuntium_debounce
        )
        alpha = self.make_agent(root, ALPHA, handlers=handlers, nuntium_debounce=nuntium_debounce)
        await bounded(beta.start(), START_BOUND, "beta's start")
        await bounded(alpha.start(), START_BOUND, "alpha's start")
        await until(f"{ALPHA}'s view to gain {BETA}", lambda: BETA in alpha.agmen(), SETTLE)
        return alpha, beta

    @staticmethod
    def _finals(read: DiariumRead, event: str, rid: str) -> list[str]:
        """The `final_status` of one agent's `inreq`/`outreq` lines for one `rid`, in order."""
        out: list[str] = []
        for entry in read.lines:
            if entry["event"] != event:
                continue
            data = entry["event_data"]
            if (
                isinstance(data, dict)
                and isinstance(data.get("req"), dict)
                and data["req"].get("rid") == rid
            ):
                out.append(str(data["final_status"]))
        return out

    async def assert_exchange(
        self,
        root: Path,
        sender: str,
        receiver: str,
        rid: str,
        recorder: InboxRecorder,
        *,
        inbox_order: tuple[str, ...],
        in_final: str | None,
        out_final: str | None,
        attachments: tuple[str, ...] = (),
        inbox_must_keep: tuple[str, ...] = (),
    ) -> None:
        """The four standing assertions of one exchange. `in_final` / `out_final` are None when
        the row's side never wrote a line for the `rid` (E9's receiver; a direct write's
        requestor). `attachments` is the scenario's list: an empty tuple is "checked, none".
        `inbox_must_keep` is E9's exception to (b): the files the row leaves behind. (b) runs
        first on purpose: the archive is what ends the exchange, and the recorder must be
        alive through it, so that (a) reads a record that includes every file, even the ones
        that exist only for a beat (the `con` the archive deletes next)."""
        # (b) the inbox is empty when the exchange is archived — the archive is the `inreq` line,
        # which stores the exchange's four envelopes (*Archive*, `A4`)
        inbox = root / receiver / "inbox"
        if inbox_must_keep:
            self.assertEqual(
                sorted(os.listdir(inbox)),
                sorted(inbox_must_keep),
                f"{receiver}'s inbox keeps exactly the row's files",
            )
        else:
            await until(
                f"{receiver}'s inbox to be empty (the archive)",
                lambda: not os.listdir(inbox),
                SETTLE,
            )
            self.assertEqual(os.listdir(inbox), [], f"{receiver}'s inbox is not empty")
        # (a) the order of the files that appeared in the receiver's inbox, by name, in time order
        self.assertEqual(
            recorder.appeared(rid),
            list(inbox_order),
            f"{receiver}'s inbox did not see {rid}'s files in protocol order",
        )
        # (c) every diarium line of both agents validates — `read_diarium` reports the ignored
        # lines, and an invalid line is one — and the `inreq`/`outreq` finals are the row's
        sent = await read_diarium(str(root / sender))
        recv = await read_diarium(str(root / receiver))
        self.assertEqual(sent.ignored, [], f"{sender} has ignored diarium lines: {sent.ignored}")
        self.assertEqual(recv.ignored, [], f"{receiver} has ignored diarium lines: {recv.ignored}")
        want_in: list[str] = [] if in_final is None else [in_final]
        self.assertEqual(
            self._finals(recv, "inreq", rid), want_in, f"{receiver}'s `inreq` finals for {rid}"
        )
        want_out: list[str] = [] if out_final is None else [out_final]
        self.assertEqual(
            self._finals(sent, "outreq", rid), want_out, f"{sender}'s `outreq` finals for {rid}"
        )
        # (d) attachments are cleaned per *Envelope lifecycle*
        for name in attachments:
            self.assertFalse(
                (root / "forum" / name).exists(),
                f"the attachment {name!r} is still in the forum after the exchange",
            )
        # The recorder ran through the archive, so (a) saw every file; it is stopped here
        # (the teardown stop, if the row failed earlier, is a no-op).
        await recorder.stop()

    async def assert_command_exchanges(
        self,
        root: Path,
        receiver: str,
        recorder: InboxRecorder,
        exchanges: list[tuple[str, str, str, str | None]],
    ) -> None:
        """The standing assertions of a run of command exchanges (E10–E13). `exchanges` is
        (sender, rid, in_final, out_final) per command; the senders may differ (E11's
        non-commander). `out_final` is None when the sender wrote the file by hand (a direct
        write owes no `outreq` line). A command is never answered and carries no attachments,
        so: (a) each rid's file appeared alone (`req`), (b) the inbox is empty once every
        command is archived, (c) every diarium line validates, each receiver `inreq` final is
        the row's and each sender `outreq` final is `sent` (B1: a command's only record
        there), and (d) "checked, none" is the call.
        """
        inbox = root / receiver / "inbox"
        await until(
            f"{receiver}'s inbox to be empty (the command archives)",
            lambda: not os.listdir(inbox),
            SETTLE,
        )
        self.assertEqual(os.listdir(inbox), [], f"{receiver}'s inbox is not empty")
        for _, rid, _, _ in exchanges:
            self.assertEqual(
                recorder.appeared(rid),
                ["req"],
                f"{receiver}'s inbox did not see {rid}'s command file alone",
            )
        recv = await read_diarium(str(root / receiver))
        self.assertEqual(recv.ignored, [], f"{receiver} has ignored diarium lines: {recv.ignored}")
        for sender, rid, in_final, out_final in exchanges:
            sent = await read_diarium(str(root / sender))
            self.assertEqual(
                sent.ignored, [], f"{sender} has ignored diarium lines: {sent.ignored}"
            )
            self.assertEqual(
                self._finals(recv, "inreq", rid),
                [in_final],
                f"{receiver}'s `inreq` finals for {rid}",
            )
            want_out: list[str] = [] if out_final is None else [out_final]
            self.assertEqual(
                self._finals(sent, "outreq", rid),
                want_out,
                f"{sender}'s `outreq` finals for {rid}",
            )
        # The recorder ran through the archives, so (a) saw every file; it is stopped here
        # (the teardown stop, if the row failed earlier, is a no-op).
        await recorder.stop()

    # --- E1 ---------------------------------------------------------------------------

    async def test_e1_a_normal_request_is_confirmed_on_both_sides(self) -> None:
        """E1: a normal request (*Appendix A.1*) — `confirmed` / `confirmed`, the inbox seeing
        `req` then `res` then `con`, and empty again after the archive."""
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            handlers = full_handlers(sleep_calls, asyncio.Event(), [])
            alpha, beta = await self.pair(root, handlers=handlers)
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)
            out = await bounded(
                alpha.request(BETA, "echo", {"text": "hi"}), CALL_BOUND, "the echo request"
            )
            self.assertEqual(out.final_status, "confirmed")
            body = out.response
            assert body is not None
            self.assertIs(body["error"], False)
            self.assertEqual(body["result"], {"text": "echo: hi"})
            self.assertEqual(len(rec.rids()), 1, "one exchange, one rid")
            rid = rec.rids()[0]
            # (d) this scenario has no attachments: the call is kept so "checked, none" is visible
            await self.assert_exchange(
                root,
                ALPHA,
                BETA,
                rid,
                rec,
                inbox_order=("req", "res", "con"),
                in_final="confirmed",
                out_final="confirmed",
            )

    # --- E2 ---------------------------------------------------------------------------

    async def test_e2_an_unknown_capability_is_answered_101(self) -> None:
        """E2: unknown capability (*Appendix A.2*; *Acceptance errors* 101) — the peer's `A0`
        answers 101, `confirmed` / `confirmed`. The capability `ghost` exists only in beta's
        `spec.json`: `alpha`'s view is built from the spec, so the local gate passes, while
        beta's `A0` judges its in-memory registry, which has no such capability. A 30 s
        `nuntium_debounce` keeps the view stale, so the row exercises the real 101."""
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            handlers = full_handlers(sleep_calls, asyncio.Event(), [])
            beta = self.make_agent(
                root,
                BETA,
                accepts=[ALPHA],
                handlers=handlers,
                nuntium_debounce=STALE_VIEW_DEBOUNCE,
            )
            await bounded(beta.start(), START_BOUND, "beta's start")
            # the phantom: appended to the spec file, never registered in memory
            spec_path = root / BETA / "spec.json"
            spec = doc(spec_path)
            spec["capabilities"].append(
                {
                    "name": "ghost",
                    "description": "only in the spec file, never registered",
                    "parameters": {"type": "object"},
                    "actio": {"result": {"type": "object"}},
                }
            )
            spec["datetime"] = iso_now()
            spec_path.write_text(json.dumps(spec), encoding="utf-8")
            alpha = self.make_agent(
                root, ALPHA, handlers=handlers, nuntium_debounce=STALE_VIEW_DEBOUNCE
            )
            await bounded(alpha.start(), START_BOUND, "alpha's start")

            def sees_ghost() -> bool:
                view = alpha.agmen().get(BETA)
                return view is not None and "ghost" in view.capabilities

            await until(f"{ALPHA}'s view to see {BETA}'s ghost capability", sees_ghost, SETTLE)
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)
            out = await bounded(alpha.request(BETA, "ghost", {}), CALL_BOUND, "the ghost request")
            # an error answer is a protocol answer: the exchange still confirms (A.2)
            self.assertEqual(out.final_status, "confirmed")
            body = out.response
            assert body is not None
            self.assertIs(body["error"], True)
            self.assertEqual(code_of(out), 101, "unknown_capability")
            self.assertIn("unknown_capability", str(cast("dict[str, Any]", body["result"])["msg"]))
            self.assertEqual(len(rec.rids()), 1)
            rid = rec.rids()[0]
            # (d) this scenario has no attachments
            await self.assert_exchange(
                root,
                ALPHA,
                BETA,
                rid,
                rec,
                inbox_order=("req", "res", "con"),
                in_final="confirmed",
                out_final="confirmed",
            )

    # --- E3 ---------------------------------------------------------------------------

    async def test_e3_requests_rejected_with_the_seven_acceptance_codes(self) -> None:
        """E3: requests rejected 100, 102, 103, 105, 106, 107, 108 (*Acceptance errors*, the
        100–199 rows) — the matching code each. 100, 105 and 107 are live requests; 102, 103,
        106 and 108 are request files written directly into beta's inbox, because the
        requestor's local gate (rule 11 of *Callability rules*) refuses to send them: 102/103
        name capabilities the view already filters, 106's sender is a peer the callability
        rules discarded, 108's timeout is already spent. The 100 row leans on a 30 s
        `nuntium_debounce`: the `enable` command republishes beta's spec, and the request must
        still pass alpha's local gate to reach the A0 that answers the code."""
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            handlers = full_handlers(sleep_calls, asyncio.Event(), [])
            # gamma, namespace `blue`, is published before beta's initial scan: beta DISCOVERS
            # it (the 106 check judges the discovered spec, unfiltered) while the callability
            # rules keep it out of the views.
            gamma = self.make_agent(
                root,
                GAMMA,
                handlers=handlers,
                namespaces=["blue"],
                nuntium_debounce=STALE_VIEW_DEBOUNCE,
            )
            await bounded(gamma.start(), START_BOUND, "gamma's start")
            alpha, beta = await self.pair(
                root, handlers=handlers, nuntium_debounce=STALE_VIEW_DEBOUNCE
            )
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)
            rids: dict[int, list[str]] = {}
            with self.subTest(code=100):
                await bounded(
                    alpha.command(BETA, "enable", {"enabled": False}),
                    CALL_BOUND,
                    "the disable command",
                )
                await until(
                    "beta's spec.json to say disabled",
                    lambda: doc(root / BETA / "spec.json").get("disabled") is True,
                    SETTLE,
                )
                out = await bounded(
                    alpha.request(BETA, "echo", {"text": "x"}), CALL_BOUND, "the 100 request"
                )
                self.assertEqual(code_of(out), 100, "agent_disabled")
                rids[100] = [rec.rids()[-1]]
                await bounded(
                    alpha.command(BETA, "enable", {"enabled": True}),
                    CALL_BOUND,
                    "the re-enable command",
                )
                await until(
                    "beta's spec.json to say enabled",
                    lambda: doc(root / BETA / "spec.json").get("disabled") is False,
                    SETTLE,
                )
            with self.subTest(code=102):
                rids[102] = [write_request(root, ALPHA, BETA, "off", {})]
                await until(
                    "the 102 request to be archived",
                    lambda: not (root / BETA / "inbox" / f"{rids[102][0]}.json").exists(),
                    SETTLE,
                )
            with self.subTest(code=103):
                rids[103] = [write_request(root, ALPHA, BETA, "private", {})]
                await until(
                    "the 103 request to be archived",
                    lambda: not (root / BETA / "inbox" / f"{rids[103][0]}.json").exists(),
                    SETTLE,
                )
            with self.subTest(code=105):
                out = await bounded(
                    alpha.request(BETA, "strict", {"n": 1}), CALL_BOUND, "the 105 request"
                )
                self.assertEqual(code_of(out), 105, "invalid_parameters")
                rids[105] = [rec.rids()[-1]]
            with self.subTest(code=106):
                rids[106] = [write_request(root, GAMMA, BETA, "echo", {"text": "x"}, timeout=0)]
                await until(
                    "the 106 request to be archived",
                    lambda: not (root / BETA / "inbox" / f"{rids[106][0]}.json").exists(),
                    SETTLE,
                )
            with self.subTest(code=107):
                first = asyncio.ensure_future(alpha.request(BETA, "sleep", {"seconds": 1}))
                await until(
                    "beta's sleep to be at max_parallel",
                    lambda: sleep_running(root) == 1,
                    SETTLE,
                )
                second = await bounded(
                    alpha.request(BETA, "sleep", {"seconds": 1}), CALL_BOUND, "the 107 request"
                )
                self.assertEqual(code_of(second), 107, "capacity_exceeded")
                first_out = await bounded(first, CALL_BOUND, "the first sleep")
                self.assertEqual(first_out.final_status, "confirmed")
                rids[107] = [rec.rids()[-1]]  # the rejected one; the first confirmed below too
            with self.subTest(code=108):
                rids[108] = [
                    write_request(
                        root, ALPHA, BETA, "echo", {"text": "x"}, timeout=5, ts_offset=-100
                    )
                ]
                await until(
                    "the 108 request to be archived",
                    lambda: not (root / BETA / "inbox" / f"{rids[108][0]}.json").exists(),
                    SETTLE,
                )

            # every rejection carries its code, read from the archived `inreq` line
            for code, code_rids in rids.items():
                with self.subTest(code=code):
                    for rid in code_rids:
                        recv = await read_diarium(str(root / BETA))
                        data = inreq_line(recv, rid)
                        res = cast("dict[str, Any]", data["res"])
                        self.assertEqual(
                            int(cast("dict[str, Any]", res["response"]["result"])["code"]),
                            code,
                            f"the archived answer of {rid}",
                        )
            # the standing assertions, per rid: a live request confirms; a direct write has no
            # requestor, so no confirmation ever arrives and the archive is `completed`. The
            # 107 row also checks the first (confirmed) sleep of the pair.
            live = (100, 105, 107)
            confirmed = {107: [rids[107][0]]}
            for code, code_rids in rids.items():
                with self.subTest(code=code):
                    for rid in code_rids if code not in confirmed else code_rids + confirmed[code]:
                        await self.assert_exchange(
                            root,
                            ALPHA,
                            BETA,
                            rid,
                            rec,
                            inbox_order=("req", "res", "con") if code in live else ("req", "res"),
                            in_final="confirmed" if code in live else "completed",
                            out_final="confirmed" if code in live else None,
                        )

    # --- E4 ---------------------------------------------------------------------------

    async def test_e4_a_confirmation_timeout_archives_completed(self) -> None:
        """E4: confirmation timeout — the requestor stops before confirming; the receiver's
        `confirm_timeout` fires and the archive is `completed` (*Envelope lifecycle*,
        "Confirmation timeout"; *Response Confirmation*: the receiver archives when the
        confirmation does not arrive in time). The requestor's own side ends `abandoned`: a
        stop settles what is still in `B2`."""
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            handlers = full_handlers(sleep_calls, asyncio.Event(), [])
            alpha, beta = await self.pair(root, handlers=handlers)
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)
            call = asyncio.ensure_future(alpha.request(BETA, "sleep", {"seconds": 0.3}))
            await until(
                "the request file to land in beta's inbox",
                lambda: bool(inbox_plain(root, BETA)),
                SETTLE,
            )
            await bounded(alpha.stop(), STOP_BOUND, "alpha's stop")
            out = await bounded(call, CALL_BOUND, "the abandoned outcome")
            self.assertEqual(out.final_status, "abandoned", "a stop settles what is in B2")
            self.assertEqual(len(rec.rids()), 1)
            rid = rec.rids()[0]
            # (d) this scenario has no attachments
            await self.assert_exchange(
                root,
                ALPHA,
                BETA,
                rid,
                rec,
                inbox_order=("req", "res"),
                in_final="completed",
                out_final="abandoned",
            )

    # --- E5 ---------------------------------------------------------------------------

    async def test_e5_a_cancellation_before_acceptance(self) -> None:
        """E5: cancelled before acceptance (*Appendix A.3*) — `cancelled` both sides, a
        cancellation response, no work started. beta is stopped first, so the request lands in
        a quiet inbox; the row sets the request's `cancel` event once the request file is
        there (the caller's cancel path: `B4` writes the cancellation), and only then does
        beta start. beta's initial scan delivers the cancellation before acceptance, so `A0`
        starts no work and answers the cancellation. A 30 s `nuntium_debounce` keeps alpha's
        view of the stopped beta usable: a rescan would discard an `inactive` peer (rule 7 of
        *Callability rules*)."""
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            handlers = full_handlers(sleep_calls, asyncio.Event(), [])
            beta = self.make_agent(
                root,
                BETA,
                accepts=[ALPHA],
                handlers=handlers,
                nuntium_debounce=STALE_VIEW_DEBOUNCE,
            )
            alpha = self.make_agent(
                root, ALPHA, handlers=handlers, nuntium_debounce=STALE_VIEW_DEBOUNCE
            )
            await bounded(beta.start(), START_BOUND, "beta's start")
            await bounded(alpha.start(), START_BOUND, "alpha's start")
            await until(f"{ALPHA}'s view to gain {BETA}", lambda: BETA in alpha.agmen(), SETTLE)
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)
            await bounded(beta.stop(), STOP_BOUND, "beta's stop")
            cancel = asyncio.Event()
            call = asyncio.ensure_future(
                alpha.request(BETA, "sleep", {"seconds": 30}, cancel=cancel)
            )
            await until(
                "the request file to land in beta's quiet inbox",
                lambda: bool(inbox_plain(root, BETA)),
                SETTLE,
            )
            rid = rec.rids()[0]
            cancel.set()
            await until(
                "the cancellation file to land",
                lambda: (root / BETA / "inbox" / f"{rid}.can.json").exists(),
                SETTLE,
            )
            await bounded(beta.start(), START_BOUND, "beta's restart")
            out = await bounded(call, CALL_BOUND, "the cancelled outcome")
            self.assertEqual(out.final_status, "cancelled")
            body = out.response
            assert body is not None
            self.assertIs(body["cancellation"], True, "the A.3 cancellation response")
            self.assertIsNone(body["result"])
            self.assertEqual(
                sleep_calls, [], "no work was started (cancellation before acceptance)"
            )
            # (d) this scenario has no attachments
            await self.assert_exchange(
                root,
                ALPHA,
                BETA,
                rid,
                rec,
                inbox_order=("req", "can", "res", "con"),
                in_final="cancelled",
                out_final="cancelled",
            )

    # --- E6 ---------------------------------------------------------------------------

    async def test_e6_a_cancellation_during_work_aborts_the_handler(self) -> None:
        """E6: cancelled during work (`sleep`) — `cancelled` both sides, the handler's
        `signal` aborted (*Envelope lifecycle*, "Cancelled during work"; the cancellation sets
        the event the handler checks, and the receiver answers the cancellation)."""
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            handlers = full_handlers(sleep_calls, asyncio.Event(), [])
            alpha, beta = await self.pair(root, handlers=handlers)
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)
            cancel = asyncio.Event()
            call = asyncio.ensure_future(
                alpha.request(BETA, "sleep", {"seconds": 30}, cancel=cancel)
            )
            await until(
                "beta's status to be busy (the handler running)",
                lambda: state_of(root, BETA) == "busy",
                SETTLE,
            )
            cancel.set()
            out = await bounded(call, CALL_BOUND, "the cancelled outcome")
            self.assertEqual(out.final_status, "cancelled")
            body = out.response
            assert body is not None
            self.assertIs(body["cancellation"], True)
            self.assertEqual(sleep_calls, [True], "the handler started and its signal aborted")
            self.assertEqual(len(rec.rids()), 1)
            rid = rec.rids()[0]
            # (d) this scenario has no attachments
            await self.assert_exchange(
                root,
                ALPHA,
                BETA,
                rid,
                rec,
                inbox_order=("req", "can", "res", "con"),
                in_final="cancelled",
                out_final="cancelled",
            )

    # --- E7 ---------------------------------------------------------------------------

    async def test_e7_a_cancellation_after_the_response(self) -> None:
        """E7: cancelled after the response (*Appendix A.4*) — `cancelled`, a single response
        with `cancellation=false`. The `hold` handler keeps the response unwritten while the
        row stops the requestor (it abandons, so no confirmation ever comes); the row then
        releases the handler (the work response is written) and delivers the cancellation
        AFTER it, as a file. No second response is written, and the archive ends
        `cancelled` because a cancellation was received (*Envelope lifecycle*,
        "Cancelled after the response")."""
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            hold_gate = asyncio.Event()
            hold_calls: list[int] = []
            handlers = full_handlers(sleep_calls, hold_gate, hold_calls)
            alpha, beta = await self.pair(root, handlers=handlers)
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)
            call = asyncio.ensure_future(alpha.request(BETA, "hold", {}))
            await until(
                "beta's status to be busy (the handler holding)",
                lambda: state_of(root, BETA) == "busy",
                SETTLE,
            )
            # the requestor stops BEFORE the response: it abandons, and no confirmation
            # will ever come
            await bounded(alpha.stop(), STOP_BOUND, "alpha's stop")
            out = await bounded(call, CALL_BOUND, "the abandoned outcome")
            self.assertEqual(out.final_status, "abandoned")
            self.assertEqual(len(rec.rids()), 1)
            rid = rec.rids()[0]
            hold_gate.set()  # now the response is written ...
            await until(
                "the response file to land",
                lambda: (root / BETA / "inbox" / f"{rid}.res.json").exists(),
                SETTLE,
            )
            # ... and the cancellation arrives after it (A.4)
            write_cancellation(root, ALPHA, BETA, rid, "hold")
            # the single response: the work's answer, not a cancellation. The archive is the
            # `inreq` line; the receiver archives on the `confirm_timeout` (no confirmation
            # will come), so read it once the inbox is empty.
            await until(
                "the exchange to be archived", lambda: not os.listdir(root / BETA / "inbox"), SETTLE
            )
            recv = await read_diarium(str(root / BETA))
            data = inreq_line(recv, rid)
            res = cast("dict[str, Any]", data["res"])
            body = cast("dict[str, Any]", res["response"])
            self.assertIs(body["cancellation"], False, "the single response is the work response")
            self.assertIs(body["error"], False)
            self.assertEqual(body["result"], {"ok": True})
            self.assertEqual(hold_calls, [1], "the handler ran exactly once")
            self.assertEqual(
                [k for k in rec.appeared(rid) if k == "res"],
                ["res"],
                "the response file was written once",
            )
            # (d) this scenario has no attachments
            await self.assert_exchange(
                root,
                ALPHA,
                BETA,
                rid,
                rec,
                inbox_order=("req", "res", "can"),
                in_final="cancelled",
                out_final="abandoned",
            )

    # --- E8 ---------------------------------------------------------------------------

    async def test_e8_a_requestor_timeout_cancels_and_the_cancellation_is_answered(
        self,
    ) -> None:
        """E8: requestor timeout -> cancel -> response — `cancelled` both sides. The
        requestor's own `timeout` (the `B2` deadline) writes the cancellation; the receiver's
        handler is aborted by its `signal` and answers the cancellation (*Issue a request*,
        `B4`; *Envelope lifecycle*, "Cancelled during work")."""
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            handlers = full_handlers(sleep_calls, asyncio.Event(), [])
            alpha, beta = await self.pair(root, handlers=handlers)
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)
            out = await bounded(
                alpha.request(BETA, "sleep", {"seconds": 30}, timeout=1.0),
                CALL_BOUND,
                "the timed-out request",
            )
            self.assertEqual(out.final_status, "cancelled")
            body = out.response
            assert body is not None
            self.assertIs(body["cancellation"], True)
            self.assertIsNone(body["result"])
            self.assertEqual(sleep_calls, [True], "the handler's signal aborted")
            self.assertEqual(len(rec.rids()), 1)
            rid = rec.rids()[0]
            # (d) this scenario has no attachments
            await self.assert_exchange(
                root,
                ALPHA,
                BETA,
                rid,
                rec,
                inbox_order=("req", "can", "res", "con"),
                in_final="cancelled",
                out_final="cancelled",
            )

    # --- E9 ---------------------------------------------------------------------------

    async def test_e9_an_inactive_receiver_abandons_the_exchange(self) -> None:
        """E9: receiver inactive -> abandon — `outreq` `abandoned`, the request's attachments
        removed from `forum`, the envelope files staying in the inbox for the receiver's next
        start (*Issue a request*, `B2`: "no response after a cancellation before the requestor
        timeout expires (e.g. the receiver is inactive) -> the requestor gives up, appends an
        `outreq` event (`final_status="abandoned"`) and deletes its request attachments"). A
        30 s `nuntium_debounce` keeps alpha's view of the stopped beta usable (a rescan would
        discard an `inactive` peer, rule 7 of *Callability rules*)."""
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            handlers = full_handlers(sleep_calls, asyncio.Event(), [])
            beta = self.make_agent(
                root, BETA, handlers=handlers, nuntium_debounce=STALE_VIEW_DEBOUNCE
            )
            alpha = self.make_agent(
                root, ALPHA, handlers=handlers, nuntium_debounce=STALE_VIEW_DEBOUNCE
            )
            await bounded(beta.start(), START_BOUND, "beta's start")
            await bounded(alpha.start(), START_BOUND, "alpha's start")
            await until(f"{ALPHA}'s view to gain {BETA}", lambda: BETA in alpha.agmen(), SETTLE)
            att = root / ALPHA / "att.txt"
            att.write_text("attachment payload", encoding="utf-8")
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)
            await bounded(beta.stop(), STOP_BOUND, "beta's stop")
            cancel = asyncio.Event()
            call = asyncio.ensure_future(
                alpha.request(BETA, "echo", {"text": "x"}, cancel=cancel, attachments=[str(att)])
            )
            await until(
                "the request file to land in beta's quiet inbox",
                lambda: bool(inbox_plain(root, BETA)),
                SETTLE,
            )
            rid = rec.rids()[0]
            cancel.set()
            out = await bounded(call, CALL_BOUND, "the abandoned outcome")
            self.assertEqual(out.final_status, "abandoned")
            self.assertIsNone(out.response, "no response was ever received")
            # the `outreq` line: `abandoned`, the request carried the attachment, no response
            sent = await read_diarium(str(root / ALPHA))
            data = None
            for entry in sent.lines:
                if entry["event"] != "outreq":
                    continue
                d = entry["event_data"]
                if (
                    isinstance(d, dict)
                    and isinstance(d.get("req"), dict)
                    and d["req"].get("rid") == rid
                ):
                    data = cast("dict[str, Any]", d)
            self.assertIsNotNone(data, "the abandon was recorded")
            assert data is not None
            self.assertEqual(data["final_status"], "abandoned")
            self.assertEqual(
                cast("dict[str, Any]", cast("dict[str, Any]", data["req"])["request"])[
                    "attachments"
                ],
                ["att.txt"],
            )
            self.assertIsNone(data["res"])
            # (d) the request attachment was copied into the forum and removed again at the
            # abandon: E9's scenario HAS one
            self.assertFalse(
                (root / "forum" / "att.txt").exists(),
                "the attachment is cleaned even on abandon",
            )
            # (b, the exception) the envelope files REMAIN in beta's inbox for its next start
            await self.assert_exchange(
                root,
                ALPHA,
                BETA,
                rid,
                rec,
                inbox_order=("req", "can"),
                in_final=None,
                out_final="abandoned",
                attachments=("att.txt",),
                inbox_must_keep=(f"{rid}.json", f"{rid}.can.json"),
            )

    # --- E10 ------------------------------------------------------------------------

    async def test_e10_accepted_commands_complete_reannounce_and_toggle_the_spec(self) -> None:
        """E10: accepted commands (*Commands*, D2/D3) — `ping`, `enable {"enabled": false/true}`
        and a custom `noop`, each landing in the receiver's diarium as `inreq` with
        `final_status="completed"`; the spec's `disabled` flips to `true` and back; and after
        each standard command a new announce: the `announce` diarium line, then the
        `nuntium/<id>.n.json` rewrite it belongs to (the line is appended before the file write
        is issued, so the first-observed order cannot invert). `ping` re-announces too, with no
        spec change. Commands are never answered, so the requestor's only record is the `outreq`
        `sent` line, and they carry no attachments: "checked, none" is the call.

        The custom `noop` is registered through the public `on_command`, and the row asserts the
        protocol's sentence, not the implementation's: *Commands* says every agent implements
        the protocol-standard commands "plus any custom command of its own", and D0 says the
        receiver judges a name known when it is "either a protocol-standard command ... or a
        custom command it implements internally". The reference (`receiver.ts`) judges
        `STANDARD_COMMANDS.has(name) || this.commands.has(name)` and runs the handler before
        archiving `completed`. If the receiver drops what `on_command` registered, that is a
        source defect, and this row is built to show it rather than work around it.
        """
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            noop_calls: list[str] = []
            handlers = full_handlers(sleep_calls, asyncio.Event(), [])
            alpha, beta = await self.pair(root, handlers=handlers)

            async def noop_handler(params: Any, ctx: CommandContext) -> None:
                noop_calls.append("noop")

            beta.on_command("noop", noop_handler)
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)

            ann_file = announce_path(root, BETA)
            loop = asyncio.get_running_loop()
            spec_before = doc(root / BETA / "spec.json")
            rids: list[str] = []

            async def command_step(
                name: str,
                params: Any,
                label: str,
                expect_text: str | None,
                expect_disabled: bool | None = None,
            ) -> None:
                # The fire-and-forget write: the rid is the row's only handle on the command
                rid = await bounded(alpha.command(BETA, name, params), CALL_BOUND, label)
                rids.append(rid)
                # The archive (D3): the `inreq` line `completed`, the file gone from the inbox
                await until(
                    f"{label} to be archived",
                    lambda: inreq_final(root, BETA, rid) == "completed",
                    CALL_BOUND,
                )
                # (d) the archived command carries no attachments: checked, none
                data = inreq_line(await read_diarium(str(root / BETA)), rid)
                req = cast("dict[str, Any]", data["req"])
                self.assertEqual(
                    cast("dict[str, Any]", req["request"])["attachments"],
                    [],
                    f"{label}: a command carries no attachments",
                )
                if expect_text is None:
                    # A custom command runs and archives; the reference re-announces no text for it
                    return
                # D2, observed: exactly one new `announce` line, and the file rewrite no later
                # than the line's first observation (the append completes before the write is
                # issued, so observation cannot invert the order)
                count_before = len(announce_lines(root, BETA))
                bytes_before = ann_file.read_bytes()
                t_line: float | None = None
                t_file: float | None = None
                t0 = loop.time()
                while (t_line is None or t_file is None) and loop.time() - t0 < CALL_BOUND:
                    if t_line is None and len(announce_lines(root, BETA)) > count_before:
                        t_line = loop.time()
                    if t_file is None and ann_file.read_bytes() != bytes_before:
                        t_file = loop.time()
                    if t_line is None or t_file is None:
                        await asyncio.sleep(0.005)
                self.assertIsNotNone(t_line, f"{label}: the `announce` diarium line")
                self.assertIsNotNone(t_file, f"{label}: the announce file rewrite")
                self.assertLessEqual(
                    cast("float", t_line),
                    cast("float", t_file),
                    f"{label}: the `announce` line lands before the announce file",
                )
                self.assertEqual(
                    doc(ann_file)["nuntium"], expect_text, f"{label}: the announce file's text"
                )
                if expect_disabled is not None:
                    await until(
                        f"the spec's disabled to be {expect_disabled}",
                        lambda: doc(root / BETA / "spec.json").get("disabled") is expect_disabled,
                        SETTLE,
                    )

            await command_step("ping", None, "the ping", f"agent {BETA} is alive")
            spec_after_ping = doc(root / BETA / "spec.json")
            self.assertFalse(
                spec_after_ping.get("disabled"),
                "ping: the spec is not disabled (`disabled` is `enable`'s to write)",
            )
            self.assertEqual(
                [c["name"] for c in spec_after_ping["capabilities"]],
                [c["name"] for c in spec_before["capabilities"]],
                "ping: the capabilities are unchanged",
            )
            await command_step(
                "enable",
                {"enabled": False},
                "the disable",
                f"agent {BETA} is disabled",
                expect_disabled=True,
            )
            await command_step(
                "enable",
                {"enabled": True},
                "the re-enable",
                f"agent {BETA} is enabled",
                expect_disabled=False,
            )
            # The custom command: accepted, its handler run, archived `completed` (D0's "or a
            # custom command it implements internally") — no re-announce text of its own
            await command_step("noop", None, "the custom noop", None)
            self.assertEqual(noop_calls, ["noop"], "the custom command's handler ran")
            # The standing assertions, one run: every command file alone in the inbox, the inbox
            # empty, the `inreq` finals `completed`, the `outreq` finals `sent`, no attachments
            await self.assert_command_exchanges(
                root,
                BETA,
                rec,
                [(ALPHA, rid, "completed", "sent") for rid in rids],
            )

    # --- E11 ------------------------------------------------------------------------

    async def test_e11_dropped_and_ignored_commands_change_nothing(self) -> None:
        """E11: dropped and ignored commands (*Commands*; D0/D4; *Strict format*). A
        non-commander's `ping` and a commander's unknown name are DROPPED: an `inreq` line with
        `final_status="dropped"`, the command file gone, and `spec.json`, `status.json` and the
        `nuntium` announce set byte-for-byte unchanged — D4's "a rejected command MUST NOT
        change what peers see". A malformed command envelope (a body without `name`, which fails
        the request schema) is IGNORED, not dropped: it never reaches the gate, so it is never
        archived — no `inreq` line, `dropped` or otherwise — and the file stays in the inbox
        (the receiver may record the ignore in a `log` line; that is not an archive). This row
        is where that difference is pinned: `dropped` leaves an `inreq` line and an empty
        inbox; `ignored` leaves neither.

        The heartbeats are parked (a 1000 s interval): this row's claim is that NOTHING rewrites
        a byte, and a 0.5 s heartbeat would rewrite `status.json`'s `datetime` on its own.
        """
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            handlers = full_handlers(sleep_calls, asyncio.Event(), [])
            # `beta` accepts commands from `alpha` only; `gamma` is a real, live non-commander
            beta = self.make_agent(
                root,
                BETA,
                accepts=[ALPHA],
                handlers=handlers,
                heartbeat_interval=1000.0,
            )
            alpha = self.make_agent(root, ALPHA, handlers=handlers, heartbeat_interval=1000.0)
            gamma = self.make_agent(root, GAMMA, handlers=handlers, heartbeat_interval=1000.0)
            await bounded(beta.start(), START_BOUND, "beta's start")
            await bounded(alpha.start(), START_BOUND, "alpha's start")
            await bounded(gamma.start(), START_BOUND, "gamma's start")
            await until(f"{ALPHA}'s view to gain {BETA}", lambda: BETA in alpha.agmen(), SETTLE)
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)

            def snapshot() -> dict[str, bytes]:
                """What D4 must not touch: the spec, the status, the announce file, the set."""
                return {
                    "spec.json": (root / BETA / "spec.json").read_bytes(),
                    "status.json": (root / BETA / "status.json").read_bytes(),
                    "announce": announce_path(root, BETA).read_bytes(),
                    "nuntium set": str(nuntium_names(root)).encode("utf-8"),
                }

            def unchanged(label: str, before: dict[str, bytes]) -> None:
                after = snapshot()
                for key, value in before.items():
                    self.assertEqual(after[key], value, f"{label}: {key} moved (D4: nothing may)")

            before = snapshot()
            # (1) the non-commander's ping: dropped, never answered. The local gate (rules 5.1
            # and 11) refuses a non-commander's `command()`, so the non-commander's envelope is
            # written by hand — the file is the protocol, and the writer's identity is the
            # envelope's `src`
            rid = write_command(root, GAMMA, BETA, "ping", None)
            await until(
                "the drop to be archived",
                lambda: inreq_final(root, BETA, rid) == "dropped",
                CALL_BOUND,
            )
            await until(
                "the dropped file to be gone",
                lambda: not (root / BETA / "inbox" / f"{rid}.json").exists(),
                SETTLE,
            )
            unchanged("the non-commander's ping", before)
            # (2) the commander's unknown name: dropped the same way
            before = snapshot()
            rid2 = await bounded(
                alpha.command(BETA, "nonsense", None), CALL_BOUND, "the unknown command"
            )
            await until(
                "the drop to be archived",
                lambda: inreq_final(root, BETA, rid2) == "dropped",
                CALL_BOUND,
            )
            await until(
                "the dropped file to be gone",
                lambda: not (root / BETA / "inbox" / f"{rid2}.json").exists(),
                SETTLE,
            )
            unchanged("the unknown command", before)
            # The standing assertions of the two drops: the files alone, the inbox empty, the
            # `inreq` finals `dropped`; gamma's direct write owes no `outreq` line, alpha's
            # `command()` owes `sent`; nothing else moved
            await self.assert_command_exchanges(
                root,
                BETA,
                rec,
                [
                    (GAMMA, rid, "dropped", None),
                    (ALPHA, rid2, "dropped", "sent"),
                ],
            )
            # (3) the malformed envelope: IGNORED, not dropped — *Strict format* ignores it
            # before the gate, so no diarium line at all, and the file stays in the inbox
            before = snapshot()
            bad_rid = write_command(root, ALPHA, BETA, "ping", None, malformed=True)
            # The watcher gets its full scan window; the claim is negative (left alone), so the
            # window is the bound, named
            await asyncio.sleep(SCAN_SETTLE)
            self.assertTrue(
                (root / BETA / "inbox" / f"{bad_rid}.json").exists(),
                "an ignored file is left in the inbox (it is never archived)",
            )
            self.assertIsNone(
                inreq_final(root, BETA, bad_rid),
                "an ignored file is never archived: no `inreq` line, `dropped` or otherwise "
                "(the receiver may record the ignore in a `log` line; that is not an archive)",
            )
            self.assertFalse(
                diarium_has_rid(root, ALPHA, bad_rid),
                "the writer's diarium carries no line for a file it wrote by hand",
            )
            unchanged("the ignored envelope", before)

    # --- E12 ------------------------------------------------------------------------

    async def test_e12_a_stop_answers_the_in_flight_request_300_and_archives_before_inactive(
        self,
    ) -> None:
        """E12: `stop` with a request in flight (*Commands*, `stop`; code 300,
        `agent_stopping`; D3's "the archive is always completed before shutting down"). While
        beta is inside the `sleep` handler, its commander sends `stop`; the caller of the
        in-flight request reads the 300 and its exchange ends `confirmed` — not `completed`:
        the requestor is alive and confirms (B3), and the receiver's stop waits for that
        confirmation before archiving, so the archive the caller's side records is `confirmed`,
        and the row asserts exactly that, not "either". Beta's `inreq` line for the request is
        on disk BEFORE `status.json` says `inactive` (checked at the moment `inactive` is first
        observed: the line, once written, stays, so its presence there proves the order), and
        the archive order is the diarium's order: the command's `inreq` (`completed`) before
        the request's `inreq` (`confirmed`). The caller reads the answer through the public
        outcome — a 300 the caller cannot read would be a source defect, reported, not
        papered over with a diarium-only read.
        """
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            handlers = full_handlers(sleep_calls, asyncio.Event(), [])
            alpha, beta = await self.pair(root, handlers=handlers)
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)
            call = asyncio.ensure_future(alpha.request(BETA, "sleep", {"seconds": 30}))
            await until(
                "beta's status to be busy (the handler running)",
                lambda: state_of(root, BETA) == "busy",
                SETTLE,
            )
            cmd_rid = await bounded(
                alpha.command(BETA, "stop", {"force": False}),
                CALL_BOUND,
                "the stop command",
            )
            out = await bounded(call, CALL_BOUND, "the stopped outcome")
            # The caller reads the 300 through the public outcome
            body = out.response
            assert body is not None, "the stop's answer must be readable by the caller"
            self.assertIs(body["error"], True)
            self.assertEqual(code_of(out), 300, "agent_stopping")
            # `confirmed`, and why: the requestor is alive and confirms (B3); the receiver's
            # stop spends its phase bound waiting for the confirmation before it archives, so
            # the archive is the confirmed one, not the `completed` of a confirmation that
            # never came
            self.assertEqual(
                out.final_status,
                "confirmed",
                "the caller confirms the 300, and the receiver waits for it",
            )
            self.assertEqual(
                sleep_calls,
                [],
                "the handler was cancelled mid-work (the signal is the cancellation path)",
            )
            # The `inreq` line is on disk before `status.json` says `inactive`
            await until(
                "beta to be inactive", lambda: state_of(root, BETA) == "inactive", STOP_BOUND
            )
            lines = diarium_lines(root, BETA)
            req_rid = next(r for r in rec.rids() if r != cmd_rid)
            inreqs = [
                (i, cast("dict[str, Any]", line["event_data"]))
                for i, line in enumerate(lines)
                if line["event"] == "inreq"
                and isinstance(line["event_data"], dict)
                and isinstance(line["event_data"].get("req"), dict)
            ]
            by_rid = {cast("dict[str, Any]", d["req"])["rid"]: (i, d) for i, d in inreqs}
            self.assertIn(
                req_rid,
                by_rid,
                "the request's `inreq` line is on disk at the moment `inactive` is observed",
            )
            self.assertIn(cmd_rid, by_rid, "the command's `inreq` line is on disk")
            # The archive order is the diarium's order: the command archived before the stop
            # began, the request archived during it
            self.assertLess(
                by_rid[cmd_rid][0],
                by_rid[req_rid][0],
                "the diarium's order: the command's archive, then the request's",
            )
            # The archived request carries the 300 response
            res = by_rid[req_rid][1].get("res")
            self.assertIsInstance(res, dict, "the archived request carries the response")
            self.assertEqual(
                int(
                    cast("dict[str, Any]", cast("dict[str, Any]", res)["response"]["result"])[
                        "code"
                    ]
                ),
                300,
                "the archived response is the 300",
            )
            # The standing assertions: the command (alone, `completed`/`sent`, inbox empty) ...
            await self.assert_command_exchanges(
                root, BETA, rec, [(ALPHA, cmd_rid, "completed", "sent")]
            )
            # ... and the request (`req`, `res`, `con`; `confirmed`/`confirmed`)
            await self.assert_exchange(
                root,
                ALPHA,
                BETA,
                req_rid,
                rec,
                inbox_order=("req", "res", "con"),
                in_final="confirmed",
                out_final="confirmed",
            )

    # --- E13 ------------------------------------------------------------------------

    async def test_e13_a_restart_archives_goes_inactive_and_comes_back_after_the_delay(
        self,
    ) -> None:
        """E13: `restart {"force": false, "delay": 1.0}` (*Commands*, `restart`; D3; D-10).
        The order the row asserts: the command's archive (`inreq` `completed`, the file gone)
        and its position in the diarium before the `inactive` status line; then `status.json`
        `inactive` with its announce (the `status` line before the `announce` line, the line
        before the file); then, after at least the delay, `active` again with a new announce
        and the spec written again. The delay is 1.0 on purpose: D-10's clamp caps it at 60 s
        (the unit row `test_the_restart_delay_is_capped_at_the_maximum` pins the cap; this row
        must not pay for it), and a row that pays the cap in real time is a row that hangs.
        The row ends with a live restarted agent, stopped by the teardown before the tree goes
        away, and its own bound (`RESTART_BOUND`) so a restart that never comes back fails in
        seconds.
        """
        with actio_tree() as root:
            sleep_calls: list[bool] = []
            handlers = full_handlers(sleep_calls, asyncio.Event(), [])
            alpha, beta = await self.pair(root, handlers=handlers)
            rec = InboxRecorder(root / BETA / "inbox")
            rec.start()
            self.addAsyncCleanup(rec.stop)
            loop = asyncio.get_running_loop()
            spec_bytes_before = (root / BETA / "spec.json").read_bytes()
            t_send = loop.time()
            cmd_rid = await bounded(
                alpha.command(BETA, "restart", {"force": False, "delay": 1.0}),
                CALL_BOUND,
                "the restart command",
            )
            # (1) the archive, before the shutdown: the `inreq` line `completed`, the file gone
            await until(
                "the restart to be archived",
                lambda: inreq_final(root, BETA, cmd_rid) == "completed",
                CALL_BOUND,
            )
            self.assertFalse(
                (root / BETA / "inbox" / f"{cmd_rid}.json").exists(),
                "the archived command file is gone",
            )
            # (2) inactive, with its announce: the status first, then the announce line, then
            # the file (C1's order, the line before the file)
            t0 = loop.time()
            t_inactive: float | None = None
            while t_inactive is None and loop.time() - t0 < STOP_BOUND:
                if state_of(root, BETA) == "inactive":
                    t_inactive = loop.time()
                else:
                    await asyncio.sleep(0.01)
            self.assertIsNotNone(t_inactive, f"beta went inactive within STOP_BOUND={STOP_BOUND}s")
            t_inactive = cast("float", t_inactive)
            ann_file = announce_path(root, BETA)
            await until(
                "the inactive announce",
                lambda: (
                    ann_file.exists() and doc(ann_file)["nuntium"] == f"agent {BETA} is inactive"
                ),
                SETTLE,
            )
            lines = diarium_lines(root, BETA)
            inreq_idx = next(
                i
                for i, line in enumerate(lines)
                if line["event"] == "inreq"
                and isinstance(line["event_data"], dict)
                and isinstance(line["event_data"].get("req"), dict)
                and cast("dict[str, Any]", line["event_data"])["req"]["rid"] == cmd_rid
            )
            status_idx = next(
                (
                    i
                    for i, line in enumerate(lines)
                    if line["event"] == "status" and line["event_data"] == "inactive"
                ),
                None,
            )
            self.assertIsNotNone(status_idx, "the `inactive` status line is in the diarium")
            self.assertLess(
                inreq_idx,
                cast("int", status_idx),
                "the diarium's order: the command's archive, then the `inactive` status",
            )
            announce_idx = next(
                (
                    i
                    for i, line in enumerate(lines)
                    if line["event"] == "announce" and i > cast("int", status_idx)
                ),
                None,
            )
            self.assertIsNotNone(
                announce_idx,
                "the `inactive`'s `announce` line follows the `status` line (the line, then the file)",
            )
            # (3) active again, after at least the delay, with a new announce and the spec
            # written again. The delay starts after the archive, which is after the send, so
            # `t_active - t_send >= 1.0` is the lower bound the row asserts
            t0 = loop.time()
            t_active: float | None = None
            while t_active is None and loop.time() - t0 < RESTART_BOUND:
                if state_of(root, BETA) == "idle":
                    t_active = loop.time()
                else:
                    await asyncio.sleep(0.01)
            self.assertIsNotNone(
                t_active,
                f"beta came back active within RESTART_BOUND={RESTART_BOUND}s",
            )
            self.assertGreaterEqual(
                cast("float", t_active) - t_send,
                1.0,
                "the restart honoured at least the delay (which starts after the archive)",
            )
            await until(
                "the fresh announce",
                lambda: ann_file.exists() and doc(ann_file)["nuntium"] == f"agent {BETA} is active",
                SETTLE,
            )
            spec_bytes_after = (root / BETA / "spec.json").read_bytes()
            self.assertNotEqual(
                spec_bytes_after,
                spec_bytes_before,
                "the spec was written again on the fresh start",
            )
            spec_after = doc(root / BETA / "spec.json")
            self.assertFalse(
                spec_after.get("disabled"),
                "the fresh spec is not disabled (`disabled` is `enable`'s to write)",
            )
            self.assertEqual(
                [c["name"] for c in spec_after["capabilities"]],
                [c["name"] for c in doc(root / BETA / "spec.json")["capabilities"]],
            )
            # The row's peer sees it back: the view regains the restarted agent
            await until(f"{ALPHA}'s view to regain {BETA}", lambda: BETA in alpha.agmen(), SETTLE)
            self.assertLess(t_inactive, cast("float", t_active), "inactive before active")
            # The standing assertions of the command: the file alone, the inbox empty, the
            # `inreq` `completed`, the `outreq` `sent`, no attachments
            await self.assert_command_exchanges(
                root, BETA, rec, [(ALPHA, cmd_rid, "completed", "sent")]
            )
