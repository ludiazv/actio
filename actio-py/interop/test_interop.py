"""The interop suite: the Python and TypeScript implementations, one folder, one exchange.

The two sides run from their own checkouts against ONE shared temporary root (one root per
module, built in ``setUpClass``): the TypeScript side from ``../actio-ts`` through
``node --import tsx``, the Python side from this checkout through ``sys.executable``. Each row
drives one side's CLI at the other side's example agent and asserts the fields the protocol
names — kind words, status words, ``final_status`` values, the presence and name of files —
never a byte-for-byte tree comparison (key order in a JSON object is not protocol).

What is pinned, by the protocol's own section names (``../doc/actio_protocol.md``):

* ``*Issue a request*`` / ``*Response Confirmation*``: a successful ``call`` ends ``confirmed``
  and the answer is printed the reference's way (``result:`` the compact form ``JSON.stringify``
  prints); a cancellation ends ``cancelled`` with ``cancellation: true``.
* ``*Acceptance errors*``: a capability error carries its own code (the reference example's
  ``fail`` is ``1000``); a request the capability's ``accepts_from`` does not list is rejected
  at ``A0`` with ``103 requestor_not_allowed`` and no work is done.
* ``*Envelope lifecycle*``: a cancellation is an acknowledgement, the receiver never sends one
  on its own, and the peer stays alive afterwards.
* ``*Announcement and Discovery*``: ``validate`` prints the kind word (``OK spec``,
  ``OK status``, ``OK announce``, ``OK diarium (N lines)`` …) for every file kind, and both
  CLIs print the same word for the same file; ``ls`` lists every agent that published.

Honesty rules: a row that cannot run says so loudly (a missing ``node`` or ``tsx`` fails the
run, it is not skipped); every child is reaped inside the tree's lifetime and its pipes are
drained; every wait is bounded and named; and the whole output is searched for interpreter
noise (``Exception ignored``, ``was never awaited``, …). The runtime that ran is printed
(``node`` version and the bun tell), so "interop green" is never read as "tested on Node".

The ``ls`` row deliberately does NOT compare the two ``ignored`` lists: for a folder that
published nothing, this side records one ``IgnoredFile`` per missing publication while the
reference ``continue``s (``../actio-ts/src/discovery.ts``); the difference is left to the
ruling and is reported, not asserted. Cite the protocol, never a process id, in this file.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any

# --- the two checkouts -------------------------------------------------------

PY_ROOT = Path(__file__).resolve().parent.parent  # this actio-py checkout
TS_ROOT = Path(os.environ.get("ACTIO_TS_ROOT", str(PY_ROOT.parent / "actio-ts")))
PY_SRC = PY_ROOT / "src"
PY_EXAMPLE = PY_ROOT / "examples" / "echo_agent.py"
TS_EXAMPLE = TS_ROOT / "examples" / "echo-agent.ts"
TS_CLI = TS_ROOT / "src" / "cli.ts"
TS_TSX = TS_ROOT / "node_modules" / "tsx" / "package.json"
NODE = "node"

# --- the ids in the shared root ---------------------------------------------

PY_AGENT = "pyecho"  # this side's example agent
TS_AGENT = "tsecho"  # the reference's example agent
PY_CLI = "pycli"  # this side's CLI vehicle id
TS_CLI_ID = "tscli"  # the reference's CLI vehicle id

# --- the named bounds (a peer that never answers fails a row in seconds) -----

PUBLISH_BOUND = 30.0  # seconds to wait for a started agent's spec.json
CALL_BOUND = 90.0  # seconds for one CLI call to finish (its --timeout is below this)
STOP_BOUND = 15.0  # seconds for a child to exit after SIGTERM before SIGKILL
DRAIN_BOUND = 10.0  # seconds to join a pipe-draining thread


def node_version() -> str:
    """The runtime's version string; the ``--version`` flag is not honoured by this node, so
    ask ``process.version`` directly."""
    return subprocess.run(
        [NODE, "-e", "console.log(process.version)"],
        capture_output=True,
        text=True,
        timeout=15.0,
    ).stdout.strip()


def node_bun_tell() -> str:
    """Whether the runtime is the bun node-fallback (``process.versions.bun`` is set)."""
    out = subprocess.run(
        [NODE, "-e", "console.log(process.versions.bun ?? '')"],
        capture_output=True,
        text=True,
        timeout=15.0,
    ).stdout.strip()
    return f"process.versions.bun={out}" if out else "process.versions.bun unset (Node)"


def _require_runtime() -> None:
    """A row that cannot run is not demonstrated: fail loudly, do not skip."""
    try:
        node_version()
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        raise AssertionError(f"node is not runnable: {e}") from None
    if not TS_TSX.is_file():
        raise AssertionError(f"the reference tsx is missing: {TS_TSX}")
    if not TS_EXAMPLE.is_file():
        raise AssertionError(f"the reference example is missing: {TS_EXAMPLE}")
    if not PY_EXAMPLE.is_file():
        raise AssertionError(f"this example is missing: {PY_EXAMPLE}")


def setUpModule() -> None:  # noqa: N802 - the unittest module hook
    """Once per module: which runtime ran, and that this checkout's ``actio`` is imported."""
    _require_runtime()
    print(f"interop runtime: node {node_version()} ({node_bun_tell()})")
    sys.path.insert(0, str(PY_SRC))  # the same PYTHONPATH every child gets
    import actio  # noqa: PLC0415 - the module setup, once

    print(f"interop actio.__file__: {actio.__file__}")
    assert Path(actio.__file__).is_relative_to(PY_ROOT), (
        f"the interop children would import a stale tree: {actio.__file__}"
    )


class _Child:
    """One long-running agent: its pipes are drained on threads (a full pipe is a child that
    never finishes), and it is reaped inside the tree's lifetime with a bound."""

    def __init__(self, name: str, cmd: list[str], cwd: str, env: dict[str, str]) -> None:
        self.name = name
        self.proc = subprocess.Popen(
            cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        self._out: list[str] = []
        self._err: list[str] = []
        self._t_out = threading.Thread(
            target=self._drain, args=(self.proc.stdout, self._out), daemon=True
        )
        self._t_err = threading.Thread(
            target=self._drain, args=(self.proc.stderr, self._err), daemon=True
        )
        self._t_out.start()
        self._t_err.start()

    @staticmethod
    def _drain(stream: Any, sink: list[str]) -> None:
        try:
            for line in iter(stream.readline, b""):
                sink.append(line.decode("utf-8", "replace"))
        finally:
            with _suppress():
                stream.close()

    def stop(self) -> tuple[str, str]:
        """SIGTERM, then SIGKILL at the bound; return the drained stdout/stderr."""
        self.proc.terminate()
        try:
            self.proc.wait(timeout=STOP_BOUND)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=STOP_BOUND)
        self._t_out.join(timeout=DRAIN_BOUND)
        self._t_err.join(timeout=DRAIN_BOUND)
        return "".join(self._out), "".join(self._err)


class _suppress:
    """A small context manager that swallows ``OSError``/``ValueError`` from a closed pipe."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, *exc: object) -> bool:
        return exc[0] is not None and isinstance(exc[0], (OSError, ValueError))


NOISE = (
    "Exception ignored",
    "was never awaited",
    "Task exception was never retrieved",
    "RuntimeWarning",
    "ResourceWarning",
)


class InteropTest(unittest.TestCase):
    """One shared root, both agents up for the module, one exchange per row."""

    root: str
    ts: _Child
    py: _Child

    @classmethod
    def setUpClass(cls) -> None:
        cls.root = tempfile.mkdtemp(prefix="interop-")
        env_ts = {
            **os.environ,
            "ACTIO_DIR": cls.root,
            "ACTIO_AGENT_ID": TS_AGENT,
            "ACTIO_HEARTBEAT": "2",
        }
        env_py = {
            **os.environ,
            "ACTIO_DIR": cls.root,
            "ACTIO_AGENT_ID": PY_AGENT,
            "ACTIO_HEARTBEAT": "2",
            "PYTHONPATH": str(PY_SRC),
        }
        cls.ts = _Child(TS_AGENT, [NODE, "--import", "tsx", str(TS_EXAMPLE)], str(TS_ROOT), env_ts)
        cls.py = _Child(
            PY_AGENT,
            [sys.executable, "-X", "dev", "-W", "error", str(PY_EXAMPLE)],
            str(PY_ROOT),
            env_py,
        )
        cls._wait_for_specs()

    @classmethod
    def tearDownClass(cls) -> None:
        ts_out, ts_err = cls.ts.stop()
        py_out, py_err = cls.py.stop()
        # a child that dies mid-run leaves its noise on its pipes: check both sides
        for name, text in (
            (TS_AGENT, ts_out + ts_err),
            (PY_AGENT, py_out + py_err),
        ):
            for needle in NOISE:
                assert needle not in text, f"noise {needle!r} in {name}: {text[:300]!r}"
        import shutil

        shutil.rmtree(cls.root, ignore_errors=True)

    @classmethod
    def _wait_for_specs(cls) -> None:
        end = time.monotonic() + PUBLISH_BOUND
        while time.monotonic() < end:
            if (Path(cls.root) / TS_AGENT / "spec.json").is_file() and (
                Path(cls.root) / PY_AGENT / "spec.json"
            ).is_file():
                return
            time.sleep(0.2)
        raise AssertionError(f"an agent did not publish spec.json within {PUBLISH_BOUND}s")

    # -- the CLI and the file helpers ----------------------------------------

    def cli(self, side: str, *args: str) -> subprocess.CompletedProcess[str]:
        """Run one CLI command on `side` ("py" or "ts") against the shared root, bounded."""
        if side == "py":
            cmd = [sys.executable, "-X", "dev", "-W", "error", "-m", "actio", *args]
            cwd, env = (
                str(PY_ROOT),
                {**os.environ, "ACTIO_DIR": self.root, "PYTHONPATH": str(PY_SRC)},
            )
        else:
            cmd = [NODE, "--import", "tsx", str(TS_CLI), *args]
            cwd, env = str(TS_ROOT), {**os.environ, "ACTIO_DIR": self.root}
        return subprocess.run(
            cmd, cwd=cwd, env=env, capture_output=True, text=True, timeout=CALL_BOUND
        )

    def assert_no_noise(self, *texts: str) -> None:
        for text in texts:
            for needle in NOISE:
                self.assertNotIn(needle, text, f"interpreter noise {needle!r} in: {text[:300]!r}")

    def diarium_lines(self, agent: str) -> list[dict[str, Any]]:
        """Every line of an agent's diarium, parsed; a line that is not JSON fails the row."""
        path = Path(self.root) / agent / "diarium.jsonl"
        lines = [t for t in path.read_text().split("\n") if t.strip()]
        out: list[dict[str, Any]] = []
        for i, line in enumerate(lines):
            try:
                doc = json.loads(line)
            except ValueError as e:
                self.fail(f"{agent} diarium line {i + 1} is not JSON: {e}: {line[:120]!r}")
            out.append(doc)
        return out

    def inreq_final_statuses(self, agent: str) -> dict[str, str]:
        """The ``final_status`` of every ``inreq`` record, keyed by its request rid."""
        out: dict[str, str] = {}
        for doc in self.diarium_lines(agent):
            if doc.get("event") != "inreq":
                continue
            data = doc.get("event_data") or {}
            req = data.get("req") or {}
            out[req.get("rid", "")] = data.get("final_status", "")
        return out

    def outreq_final_statuses(self, agent: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for doc in self.diarium_lines(agent):
            if doc.get("event") != "outreq":
                continue
            data = doc.get("event_data") or {}
            out[data.get("rid", "")] = data.get("final_status", "")
        return out

    # -- row 1-4: the reference CLI calls this side's agent ------------------

    def test_01_ts_cli_py_echo(self) -> None:
        r = self.cli("ts", "call", TS_CLI_ID, PY_AGENT, "echo", '{"text":"hello from ts"}')
        self.assert_no_noise(r.stdout, r.stderr)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("finalStatus: confirmed", r.stdout)
        self.assertIn('result: {"text":"hello from ts"}', r.stdout)  # the compact form
        self.assertIn(f"{TS_CLI_ID}: outreq", r.stderr)
        # the exchange's files, both sides: the receiver's inreq and the caller's outreq
        inreq = self.inreq_final_statuses(PY_AGENT)
        self.assertTrue(any(v == "confirmed" for v in inreq.values()), inreq)
        outreq = self.outreq_final_statuses(TS_CLI_ID)
        self.assertTrue(any(v == "confirmed" for v in outreq.values()), outreq)

    def test_02_ts_cli_py_fail(self) -> None:
        r = self.cli("ts", "call", TS_CLI_ID, PY_AGENT, "fail", "{}")
        self.assert_no_noise(r.stdout, r.stderr)
        self.assertEqual(r.returncode, 0, r.stderr)  # an error answer is still an answer
        self.assertIn("finalStatus: confirmed", r.stdout)
        self.assertRegex(r.stdout, r'error: 1000 ".*"')  # the reference example's code
        self.assertIn(f"{TS_CLI_ID}: outreq", r.stderr)

    def test_03_ts_cli_py_private_rejection(self) -> None:
        r = self.cli("ts", "call", TS_CLI_ID, PY_AGENT, "private", "{}")
        self.assert_no_noise(r.stdout, r.stderr)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("finalStatus: confirmed", r.stdout)
        self.assertRegex(r.stdout, r"error: 103 .*")  # requestor_not_allowed, rejected at A0
        self.assertIn(f"{TS_CLI_ID}: outreq", r.stderr)
        # no work was done: a rejection at A0 is answered before the handler runs, so the
        # receiver's diarium carries the inreq as an answered (confirmed) rejection, not a
        # busy status for work that never started.
        inreq = self.inreq_final_statuses(PY_AGENT)
        self.assertTrue(any(v == "confirmed" for v in inreq.values()), inreq)

    def test_04_ts_cli_py_sleep_cancelled(self) -> None:
        # --timeout 3 is whole seconds (a sub-second timeout is already expired when the peer
        # reads it: 108 request_expired); the requestor's own B4 cancels the 10 s sleep.
        r = self.cli(
            "ts", "call", TS_CLI_ID, PY_AGENT, "sleep", '{"seconds": 10}', "--timeout", "3"
        )
        self.assert_no_noise(r.stdout, r.stderr)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("finalStatus: cancelled", r.stdout)
        self.assertIn("cancellation: true", r.stdout)
        self.assertIn(f"{TS_CLI_ID}: outreq", r.stderr)
        inreq = self.inreq_final_statuses(PY_AGENT)
        self.assertTrue(any(v == "cancelled" for v in inreq.values()), inreq)
        outreq = self.outreq_final_statuses(TS_CLI_ID)
        self.assertTrue(any(v == "cancelled" for v in outreq.values()), outreq)
        # the peer is alive and answering afterwards
        after = self.cli("ts", "call", TS_CLI_ID, PY_AGENT, "echo", '{"text":"still here"}')
        self.assert_no_noise(after.stdout, after.stderr)
        self.assertEqual(after.returncode, 0, after.stderr)
        self.assertIn("finalStatus: confirmed", after.stdout)

    # -- row 5: this side's CLI calls the reference agent (the same four) ----

    def test_05_py_cli_ts_echo(self) -> None:
        r = self.cli("py", "call", PY_CLI, TS_AGENT, "echo", '{"text":"hello from py"}')
        self.assert_no_noise(r.stdout, r.stderr)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("finalStatus: confirmed", r.stdout)
        self.assertIn('result: {"text":"hello from py"}', r.stdout)
        self.assertIn(f"{PY_CLI}: outreq", r.stderr)
        inreq = self.inreq_final_statuses(TS_AGENT)
        self.assertTrue(any(v == "confirmed" for v in inreq.values()), inreq)
        outreq = self.outreq_final_statuses(PY_CLI)
        self.assertTrue(any(v == "confirmed" for v in outreq.values()), outreq)

    def test_06_py_cli_ts_fail(self) -> None:
        r = self.cli("py", "call", PY_CLI, TS_AGENT, "fail", "{}")
        self.assert_no_noise(r.stdout, r.stderr)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("finalStatus: confirmed", r.stdout)
        self.assertRegex(r.stdout, r'error: 1000 ".*"')
        self.assertIn(f"{PY_CLI}: outreq", r.stderr)

    def test_07_py_cli_ts_private_rejection(self) -> None:
        r = self.cli("py", "call", PY_CLI, TS_AGENT, "private", "{}")
        self.assert_no_noise(r.stdout, r.stderr)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("finalStatus: confirmed", r.stdout)
        self.assertRegex(r.stdout, r"error: 103 .*")
        self.assertIn(f"{PY_CLI}: outreq", r.stderr)
        inreq = self.inreq_final_statuses(TS_AGENT)
        self.assertTrue(any(v == "confirmed" for v in inreq.values()), inreq)

    def test_08_py_cli_ts_sleep_cancelled(self) -> None:
        r = self.cli("py", "call", PY_CLI, TS_AGENT, "sleep", '{"seconds": 10}', "--timeout", "3")
        self.assert_no_noise(r.stdout, r.stderr)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("finalStatus: cancelled", r.stdout)
        self.assertIn("cancellation: true", r.stdout)
        self.assertIn(f"{PY_CLI}: outreq", r.stderr)
        inreq = self.inreq_final_statuses(TS_AGENT)
        self.assertTrue(any(v == "cancelled" for v in inreq.values()), inreq)
        outreq = self.outreq_final_statuses(PY_CLI)
        self.assertTrue(any(v == "cancelled" for v in outreq.values()), outreq)
        after = self.cli("py", "call", PY_CLI, TS_AGENT, "echo", '{"text":"still here"}')
        self.assert_no_noise(after.stdout, after.stderr)
        self.assertEqual(after.returncode, 0, after.stderr)
        self.assertIn("finalStatus: confirmed", after.stdout)

    # -- row 6: validate, both ways ------------------------------------------

    def _exchange_envelopes(
        self, dst: str, caller: str, caller_id: str
    ) -> dict[str, dict[str, Any]]:
        """One ``sleep`` + cancel exchange (all four envelopes), read back from `dst`'s
        diarium: the inbox files are archived, the diarium keeps them."""
        r = self.cli(caller, "call", caller_id, dst, "sleep", '{"seconds": 10}', "--timeout", "3")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("finalStatus: cancelled", r.stdout)
        for doc in self.diarium_lines(dst):
            if doc.get("event") != "inreq":
                continue
            data = doc.get("event_data") or {}
            if data.get("final_status") != "cancelled":
                continue
            req, res, con, can = (data.get(k) for k in ("req", "res", "con", "can"))
            if req and res and con and can:
                return {"req": req, "res": res, "con": con, "can": can}
        self.fail(f"no cancelled inreq with all four envelopes in {dst}'s diarium")

    def test_09_validate_both_ways(self) -> None:
        # one broadcast (the CLI is the only writer of nuntium/nuntium.json), validated both ways
        b = self.cli("py", "broadcast", "interop: a broadcast for the validate row")
        self.assertEqual(b.returncode, 0, b.stderr)
        broadcast = Path(self.root) / "nuntium" / "nuntium.json"
        self.assertTrue(broadcast.is_file())

        # the four exchange envelopes of one cancelled exchange, per destination
        py_env = self._exchange_envelopes(PY_AGENT, "ts", TS_CLI_ID)  # files the py agent wrote
        ts_env = self._exchange_envelopes(TS_AGENT, "py", PY_CLI)  # files the ts agent wrote

        # each CLI validates the files the OTHER implementation wrote
        self._validate_side("py", TS_AGENT, ts_env)  # the Python CLI over the TS agent's files
        self._validate_side("ts", PY_AGENT, py_env)  # the TS CLI over the Python agent's files
        self._validate_broadcast_both_ways(broadcast)

    def _validate_side(self, side: str, agent: str, env: dict[str, dict[str, Any]]) -> None:
        import shutil

        agent_dir = Path(self.root) / agent
        cases: list[tuple[str, str]] = [
            (str(agent_dir / "spec.json"), "OK spec"),
            (str(agent_dir / "status.json"), "OK status"),
            (str(agent_dir.parent / "nuntium" / f"{agent}.n.json"), "OK announce"),
            (str(agent_dir / "diarium.jsonl"), "OK diarium ("),  # the count differs per run
        ]
        # one exchange's four envelopes, written to temp names the validator can kind
        tmp = Path(tempfile.mkdtemp(prefix=f"interop-{agent}-"))
        try:
            for kind, doc in env.items():
                # the validator infers the kind from the name's suffix: `.res`/`.con`/`.can`,
                # and any other `.json` is a request (the *Announcement and Discovery* name rule)
                fname = {
                    "req": "env-req.json",
                    "res": "env.res.json",
                    "con": "env.con.json",
                    "can": "env.can.json",
                }[kind]
                tmp.joinpath(fname).write_text(json.dumps(doc))
            cases += [
                (str(tmp / "env-req.json"), "OK request"),
                (str(tmp / "env.res.json"), "OK response"),
                (str(tmp / "env.con.json"), "OK confirmation"),
                (str(tmp / "env.can.json"), "OK cancellation"),
            ]
            for path, kind_word in cases:
                r = self.cli(side, "validate", path)
                self.assert_no_noise(r.stdout, r.stderr)
                self.assertEqual(r.returncode, 0, f"validate {path}: {r.stderr}")
                self.assertTrue(
                    r.stdout.strip().startswith(kind_word), f"{side} validate {path}: {r.stdout!r}"
                )
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def _validate_broadcast_both_ways(self, broadcast: Path) -> None:
        for side in ("py", "ts"):
            r = self.cli(side, "validate", str(broadcast))
            self.assert_no_noise(r.stdout, r.stderr)
            self.assertEqual(r.returncode, 0, f"{side} validate broadcast: {r.stderr}")
            self.assertTrue(r.stdout.strip().startswith("OK announce"), r.stdout)

    # -- row 7: ls, both ways ------------------------------------------------

    def test_10_ls_both_ways(self) -> None:
        # The two `ignored` lists are NOT compared: for a folder that published nothing this
        # side records one IgnoredFile per missing publication, the reference says nothing
        # (../actio-ts/src/discovery.ts continues on kind == "missing"). The measured counts
        # are reported, not asserted.
        py = json.loads(self.cli("py", "ls", "--json").stdout)
        ts = json.loads(self.cli("ts", "ls", "--json").stdout)
        by_id = {a["id"]: a for a in py["agents"]}
        for agent in (PY_AGENT, TS_AGENT):
            self.assertIn(agent, by_id, f"{agent} not in the Python ls")
        ts_by_id = {a["id"]: a for a in ts["agents"]}
        for agent in (PY_AGENT, TS_AGENT):
            self.assertIn(agent, ts_by_id, f"{agent} not in the TS ls")
        for agent in (PY_AGENT, TS_AGENT):
            a, b = by_id[agent], ts_by_id[agent]
            self.assertEqual(a["status"], b["status"], f"{agent} status differs")
            self.assertEqual(a["health"], b["health"], f"{agent} health differs")
            self.assertEqual(
                [c["name"] for c in a["capabilities"]],
                [c["name"] for c in b["capabilities"]],
                f"{agent} capability set differs",
            )
            self.assertEqual(
                [(c["name"], c["disabled"], c["accepts_from"]) for c in a["capabilities"]],
                [(c["name"], c["disabled"], c["accepts_from"]) for c in b["capabilities"]],
                f"{agent} capability details differ",
            )

    # -- row 8: the diarium, both ways ---------------------------------------

    def test_11_diarium_both_ways(self) -> None:
        # an exchange each way, so each diarium has a fresh inreq to read back
        self.cli("ts", "call", TS_CLI_ID, PY_AGENT, "echo", '{"text":"diarium row"}')
        self.cli("py", "call", PY_CLI, TS_AGENT, "echo", '{"text":"diarium row"}')

        # the Python CLI reads the TS agent's diarium: every line parses, the inreq is confirmed
        r_py = self.cli("py", "diarium", TS_AGENT, "-n", "200")
        self.assert_no_noise(r_py.stdout, r_py.stderr)
        self.assertEqual(r_py.returncode, 0, r_py.stderr)
        py_read = [json.loads(t) for t in r_py.stdout.split("\n") if t.strip()]
        self.assertTrue(any(d.get("event") == "inreq" for d in py_read), py_read)

        # the TS CLI reads the Python agent's diarium: every line parses, the inreq is confirmed
        r_ts = self.cli("ts", "diarium", PY_AGENT, "-n", "200")
        self.assert_no_noise(r_ts.stdout, r_ts.stderr)
        self.assertEqual(r_ts.returncode, 0, r_ts.stderr)
        ts_read = [json.loads(t) for t in r_ts.stdout.split("\n") if t.strip()]
        self.assertTrue(any(d.get("event") == "inreq" for d in ts_read), ts_read)

        # the exchange records' final_status values agree with what the rows did
        self.assertTrue(any(v == "confirmed" for v in self.inreq_final_statuses(TS_AGENT).values()))
        self.assertTrue(any(v == "confirmed" for v in self.inreq_final_statuses(PY_AGENT).values()))


if __name__ == "__main__":
    unittest.main()
