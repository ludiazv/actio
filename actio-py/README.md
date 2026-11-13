# actio-py

A Python implementation of the **Actio protocol v0.1**, the core conformance profile: agents that
announce capabilities in a shared folder tree and exchange JSON envelopes through each other's
inboxes. There is no network layer and no daemon: an agent is a library object you run inside your
own process, and the file system is the transport.

The protocol itself lives outside this repository. `CONFORMANCE.md` cites it as `doc L…`, meaning the
line number in `actio_protocol.md` beside the schemas (`../doc/actio_protocol.md` in the checkout that
holds the reference implementations). Where a sentence here describes what the reference
implementation does, it names it: `actio-ts` is the TypeScript reference, `actio-go` the Go one.

Out of scope, as the protocol defines them: the security profile (signing, `security_posture`
beyond `trusted`, the zero-trust issuer rules), encrypted envelopes, and any transport other than
the folder tree. Section 14 of `CONFORMANCE.md` marks the security features `n/a (security)`, and
says what the core profile does instead where a rule still bites: a foreign `actio_version` fails the
schema and is ignored rather than answered with 104, and a full queue is refused with 107 rather
than queued.

---

## Install

Python 3.13 or newer. Runtime dependencies are exactly two: `jsonschema` and `watchdog`.

```bash
uv sync                    # dev extras: ruff, mypy
uv run python -c "import actio; print(actio.__name__)"
```

The distribution is `actio-py`; the import name is `actio`. Installing the package also installs the
`actio` command (`actio ls`, `actio call`, …).

---

## Two agents in one tree

An agent needs a folder it owns and a root it shares with its peers. It writes four things there:
`spec.json` (what it can do), `status.json` (that it is alive), `inbox/` (where peers leave
requests) and `diarium.jsonl` (its protocol log), plus a `nuntium/` folder it uses to tell its peers
that something changed.

```python
import asyncio
from actio import ActioAgent

ECHO = {
    "name": "echo",
    "description": "echoes the text it gets",
    "parameters": {
        "type": "object",
        "properties": {"text": {"type": "string"}},
        "required": ["text"],
        "additionalProperties": False,
    },
    "actio": {
        "result": {
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
        }
    },
}


async def echo(params, ctx):
    return {"text": params["text"]}


async def main() -> None:
    beta = ActioAgent(id="beta", description="the echo agent", accepts_commands_from=["alpha"])
    beta.capability(ECHO, echo)  # or @beta.capability(ECHO)
    await beta.start()  # ACTIO_DIR names the shared tree
    try:
        await beta.wait_stopped()  # a `stop` command, or SIGINT in the example
    finally:
        await beta.stop()


asyncio.run(main())
```

`load_config()` reads `ACTIO_DIR` (the tree, default `$HOME/.actio`), `ACTIO_SYNC`, and the durations
`ACTIO_HEARTBEAT`, `ACTIO_CONFIRM_TIMEOUT`, `ACTIO_CANCEL_WAIT` as seconds; pass
`config=load_config(root=…)`, or build an `ActioConfig` directly, to name them in code.

Its peer calls it by id:

```python
outcome = await alpha.request("beta", "echo", {"text": "hello"}, timeout=10)
print(outcome.response["result"])  # {'text': 'hello'}
print(outcome.final_status)  # 'confirmed'
```

`examples/echo_agent.py` is the black-box conformance target: five capabilities and a `noop` command
that the reference implementation drives through the same scenarios as its own example agent. Run it
with `ACTIO_DIR` pointed at a shared tree and `ACTIO_AGENT_ID` naming it, and drive it from the
command line:

```bash
uv run actio ls
uv run actio call alpha beta echo '{"text":"hello"}' --timeout 5
uv run actio cmd alpha beta stop
uv run actio diarium beta -n 20
uv run actio validate inbox/01H….json --as request
uv run actio broadcast "restarting at noon"
```

---

## The API

`src/actio/__init__.py` is the authority: `__all__` lists every export, and the protocol shapes
(`RequestFile`, `ResponseBody`, `DiariumEntry`, `Header`, …) are exported as `TypedDict`s so a file
you read or write can be typed without generating anything.

The agent object:

| Method | What it does |
| --- | --- |
| `capability(spec, handler)` | register a capability; usable as a decorator |
| `on_command(name, handler)` | register a command handler (`stop`, `restart`, `enable`, `ping`, and your own) |
| `on_event(handler)` | observe `StatusEvent`, `AnnounceEvent`, `InreqEvent`, `OutreqEvent`, `LogEvent`, `IgnoredEvent` |
| `start()` / `stop(force=…)` | the lifecycle; `stop` is the receiver's own D-9 order |
| `wait_stopped()` | return once the agent has stopped, by command or by `fail` |
| `async with ActioAgent(config) as agent:` | start and stop, including on an exception |
| `agmen()` / `refresh()` | the callable peers, and a forced discovery scan |
| `request(dst, name, params, timeout=…, attachments=…, cancel=…)` | lane B: a request, and its `RequestOutcome` |
| `command(dst, name, params)` | lane D: a command, returning its `rid` |
| `fail(message)` | stop because of an unrecoverable error, after a `status.json` of `error` |

Module functions: `load_config()`, `scan()`, `health()`, `check_params()`, `read_diarium()`.
Exceptions: `ActioError` (a handler's refusal, `code >= 1000`), `ActioCallError` (the callability
rules refused it before anything was written), `ActioResultError` (the peer's answer broke its own
declared `result` schema).

---

## Implementation choices

The protocol leaves some decisions open, and three implementations must make the same ones to
interoperate. Each choice below has a label; the code cites these labels in the places where the
choice is what the code is doing.

| Label | The choice |
| --- | --- |
| **D-1** | The folder name is the agent's `id`: an agent's folder is named for the id it publishes. |
| **D-2** | `stop` never kills a handler task. It asks the handler to stop at its next safe point and waits a bounded time; the handler is never cancelled under it. |
| **D-3** | Two folders publishing the same id: the first in lexical order keeps the id, the other is skipped and reported as a `LogEvent`. |
| **D-4** | Envelopes already in the inbox at startup are processed as if they had just arrived: requests go through `A0` (so an expired one is answered `108 request_expired`), commands through `D0`, and a response with no confirmation restarts the confirmation timer from now. The protocol lists this as one of three admissible behaviours. |
| **D-5** | A `rid` already archived in this run is ignored, not served again; a command runs once per `rid` per run. |
| **D-6** | Requestor timeout: with `timeout > 0`, expiry sends a cancellation (`B4`), waits up to `cancel_wait_timeout` for the answer, then abandons. With `timeout == 0` it waits until the answer or a `cancel` event, and a cancelled caller still ends the exchange correctly in the background. |
| **D-7** | The optional acceptance checks: 105, 106 (checked against the unfiltered scan, so a peer we would not call is still recognised), 107 and 108 are implemented; 104 is not. |
| **D-8** | Busy/idle: an agent is busy while an inbound exchange (`A1`–`A3`) or an outbound one (`B2`) is in flight; the state is re-evaluated as its own task. |
| **D-9** | `stop`, in the protocol's order: a `stop` during `start` leaves nothing armed; exchange tasks whose caller has gone are abandoned like any other; every step's failure is logged and the later steps still run; a handler that will not stop is left after `stop_grace`, the one exception to "nothing armed"; an exchange the stop answered is held inside the stop phase until its caller confirms it or one bound is spent, so the caller can read the answer before the files are archived. |
| **D-10** | `restart` is in-process with fresh state: registrations survive, a `disabled` flag set by `enable` does not; the object stays the same and `wait_stopped()` does not return; a `stop` during the delay outranks the restart at any phase and ends `wait_stopped()`; the delay is clamped at 60 s. |
| **D-11** | Header consistency: every file of one exchange carries the same header, `datetime` excepted; a mismatch is an ignored file with a reason. An ignored file is reported once per `(path, mtime)`, so a watcher wake-up cannot log the same file twice. |
| **D-12** | Result schema: a non-error response whose `result` breaks the peer's declared `result` schema ends as `ActioResultError` in the caller. An error response is the peer's answer, not our bug. |
| **D-13** | The confirmation guard: the requestor checks that the request still exists before writing its confirmation, because the receiver may have archived it. |
| **D-14** | Encrypted bodies: the core profile writes and reads plaintext envelopes. The protocol's encryption fields are accepted where the schema allows them and are never produced here. |
| **D-15** | Malformed files are reported, never raised and never silent. A rejection reason is one line of at most 200 characters, built from the path and the schema and never from the peer's value: `(root) missing 'rid', 'src'`, `/status must be one of 'inactive', 'idle'`, `/code must be of type integer`, otherwise `/src fails 'maxLength'`. A path segment can be a peer's key, so it is escaped. The wording is by context: an inbox file that is not JSON says `malformed JSON: <the parser's error>`, an unparsable publication in a scan and an unparsable diarium line say `not valid JSON`, flat. `unreadable: <error>` is ours, because the reference has no counterpart for it. |
| **D-16** | Housekeeping: removing a file or folder that is already gone is not an error; request attachments left in `forum` are swept once their exchange ends. |
| **D-17** | `nuntium`: any change under the root's `nuntium/` schedules one debounced full scan, so a peer that starts later is found without anyone calling `refresh()`. |
| **D-18** | Watching the destination (`B2`): an exchange subscribes to the destination's inbox on the agent's one watcher *before* writing its request, then loops — clear the wake flag, look for its own `<rid>.res.json`, then wait on the flag, the timer, the cancellation and its own task. Whoever owns the exchange when the caller is cancelled keeps that subscription, so the exchange can still finish in the background. `actio-go` learned this the hard way; the reference keeps a per-destination subscription instead. |
| **D-19** | Capability schemas: each capability gets its own validator with an empty registry, so a repeated `$id` is harmless and no external reference resolves. A capability whose own `check_schema` fails, or which uses an unknown keyword, is a registration error. |
| **D-20** | Schemas and types: no generator. `src/actio/schemas/` is a byte-identical copy of the published schemas, read through `importlib.resources`, and a test fails if the copy ever differs. The protocol types in `_protocol.py` are hand-written `TypedDict`s. |
| **D-21** | Sync providers: `none`, `delay[:<ms>]`, `fsync+delay[:<ms>]`, and `mutagen:<session>`, the last run through `asyncio.create_subprocess_exec` with the binary named by `ACTIO_MUTAGEN_BIN`. |
| **D-22** | Watch errors: no retry and no re-arm. If the agent's own inbox or `nuntium/` is lost, it logs `watch lost <dir>: <error>` and fails. |
| **D-23** | The watcher: one `watchdog` observer per agent, non-recursive. An event only wakes whoever subscribes to that folder: `subscribe(folder)` returns a `FolderWatch` with a `changed` event and a `lost` exception slot. A periodic audit tick catches events the observer never delivered. |
| **D-24** | Announcements: every announcement appends an `announce` diarium event (`event_data: null`) — step 3, `enable`, `ping`, the re-announce, and the shutdown announce — and the diarium line is written before the `nuntium` file that carries it. |

---

## Differences from `actio-ts`

These are the places where reading the reference will not predict this implementation. Everything
not listed here is "as `actio-ts`", and `CONFORMANCE.md` states each rule with the row that proves it.

- **The regex dialect and strict-mode limits.** Schema `pattern` keywords are evaluated by Python's
  `re`, not JavaScript's; a pattern using JS-only syntax is a registration error rather than a
  silently different match.
- **`wait_stopped()`** is a method on the agent. The reference has no equivalent: its agent resolves
  `stop()` and offers no way to await a stop that arrived from outside.
- **`async with`**: the agent is an async context manager, so `start()` and `stop()` bracket a block
  including the exception paths.
- **The decorator form**: `@agent.capability(spec)` works as a decorator as well as a call.
- **Durations are seconds.** Config values (`heartbeat_interval`, `confirm_timeout`,
  `cancel_wait_timeout`, `announce_delay`, `nuntium_debounce`, `stop_grace`) are floats in seconds;
  the environment variables are seconds too. The reference stores milliseconds.
- **The watcher's audit tick.** One `watchdog` observer per agent can miss an event, so a periodic
  audit re-checks the folders it should have seen. The reference leans on `fs.watch`.
- **The wording of rejection reasons** is `D-15`'s: one line, built from path and schema, never from
  a value. `unreadable: …` and the strict-UTF-8 case below have no counterpart in the reference, so
  those reason strings stay as they are here.
- **`ActioError` below 1000 raises `ValueError`** at the raise site. The protocol's refusal codes are
  100–108 and belong to the receiver, so a handler that names one has a bug; the reference does not
  check.
- **Strict UTF-8.** A file that is not valid UTF-8 is unparsable: a publication is ignored with the
  reference's reason, an inbox file with the inbox's. The reference substitutes U+FFFD and hands the
  mangled text to the parser.
- **A handler that will not stop is left.** After `stop_grace` the stop moves on and the task is
  abandoned; it is the one thing that can still be armed after `stop()` returns.
- **Two named thread pools:** asyncio's default executor, and `actio-io` (`_files.py`) for the file
  layer's reads and writes. The pool is the one piece of module state in the package.
- **Restart's delay is clamped** to 60 seconds whatever the command asked for.
- **`D-24`'s line before file:** the `announce` diarium entry is on disk before the `nuntium` signal
  that announces it.
- **Duplicate attachment basenames are refused** where the reference lets two attachments collide in
  `forum`.
- **A response attachment stays valid until the confirmation.** The exchange owns it until its
  confirmation arrives or times out; the reference sweeps it at the same moment it archives.
- **At `stop`, an answered exchange waits for its confirmation.** Up to `stop_grace`, the archive is
  held so the caller can read and confirm the answer (`D-9`); the reference archives as soon as the
  stop reaches that phase, which leaves a caller whose read was in flight with an archived exchange
  and a `dropped` answer it cannot confirm.

---

## Tests and conformance

```bash
uv run ruff format --check src tests examples
uv run ruff check src tests examples
uv run mypy src
uv run python -X dev -W error -m unittest discover -s tests
```

Interpreter noise is a failure: a run whose summary says `OK` but which prints `Exception ignored`,
`was never awaited`, `RuntimeWarning` or `ResourceWarning` anywhere in its output is not green.

`interop/` holds the cross-implementation suite: it runs this agent and the reference's example agent
against the same tree and compares what each one leaves on disk. It needs the reference checkout and
a Node runtime, and it runs once per milestone, not per commit:

```bash
TMPDIR=/tmp PYTHONPATH="$PWD/src:$PWD/interop" uv run python -X dev -W error -m unittest discover -s interop
```

`CONFORMANCE.md` is the matrix of protocol rules against this tree: every rule with the implementation
symbol that enforces it and the test row that proves it, `doc L…` for the protocol line, and
`no unit test` where no row reaches the rule. It is written from this repository; it is not a
translation of the reference's matrix, and its test names do not match theirs.
