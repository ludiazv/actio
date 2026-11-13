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


# --- the suite ----------------------------------------------------------------------


class E2ETest(ActioTestCase):
    """Two live agents in one temporary tree, talking through the folders.

    The tree outlives its agents (P-15's form here is `addAsyncCleanup(agent.stop)`): every
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
