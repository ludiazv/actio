"""The `actio` command: `ls`, `diarium`, `call`, `cmd`, `broadcast`, `validate`, and a global `--json`.

Same subcommands, flags, output and exit codes as the reference CLI `actio-ts/src/cli.ts`: a usage
error prints the reason and the `usage:` line to stderr and exits 1, never argparse's 2. `call` and
`cmd` are lane B only — the CLI is a vehicle, not an agent — and `validate` decides the kind through
`_validate.file_kind`, the one name inference in the package (*Announcement and Discovery*).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
from collections.abc import Coroutine, Sequence
from typing import Any, NoReturn, cast

from ._config import ActioConfig, load_config
from ._diarium import append
from ._discovery import scan
from ._files import make_dirs, read_text, write_json_atomic
from ._protocol import DiariumEntry, Spec, StatusState
from ._requestor import Requestor
from ._status import health
from ._sync import create_sync_provider
from ._types import ActioEvent, AgentFolders, LogEvent, RequestOutcome, SyncProvider
from ._util import format_iso
from ._validate import SchemaKind, file_kind, validate
from ._watch import Watcher

DEFAULT_TIMEOUT = 60

USAGE = (
    "usage: actio [--json] ls | diarium <agent> [-n N] | call <src-id> <dst> <capability> "
    "<json-params> [--timeout s (default 60)] | cmd <src-id> <dst> <name> [json-params] "
    '| broadcast "<text>" | validate <file> [--as <kind>]'
)

_KINDS = "actio spec status announce request cancellation response confirmation diarium".split()


class _Usage(Exception):
    """A usage error: the reason (when there is one) and the `usage:` line, on stderr, exit 1."""


def out(line: str) -> None:
    sys.stdout.write(f"{line}\n")


def err(line: str) -> None:
    sys.stderr.write(f"{line}\n")


def _usage_out(error: _Usage) -> None:
    if error.args:
        err(str(error.args[0]))
    err(USAGE)


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise _Usage(message)


def _sub_parser(sub: str) -> _Parser:
    p = _Parser(prog=f"actio {sub}", usage=USAGE)
    p.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    if sub == "diarium":
        p.add_argument("agent", nargs="?")
        p.add_argument("-n", metavar="N")
    elif sub in ("call", "cmd"):
        p.add_argument("src", nargs="?")
        p.add_argument("dst", nargs="?")
        p.add_argument("capability" if sub == "call" else "name", nargs="?")
        p.add_argument("json_params", nargs="?")
        if sub == "call":
            p.add_argument("--timeout", metavar="s")
    elif sub == "broadcast":
        p.add_argument("text", nargs="?")
    elif sub == "validate":
        p.add_argument("file", nargs="?")
        p.add_argument("--as", dest="as_kind", metavar="kind")
    return p


def _parse(args: list[str]) -> tuple[str, argparse.Namespace, bool]:
    top = _Parser(prog="actio", usage=USAGE)
    top.add_argument("--json", action="store_true", help="print JSON instead of text")
    top.add_argument("sub", nargs="?", help=argparse.SUPPRESS)
    top.add_argument("rest", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)
    ns = top.parse_args(args)
    if ns.sub is None or ns.sub not in _SUBS:
        raise _Usage() if ns.sub is None else _Usage(f'unknown subcommand "{ns.sub}"')
    sub_ns = _sub_parser(ns.sub).parse_args(list(ns.rest))
    return ns.sub, sub_ns, ns.json or sub_ns.json


def _number(flag: str, raw: str | None, default: float, rule: str, strict: bool = False) -> float:
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise _Usage(f'{flag} {rule}, got "{raw}"') from None
    if not math.isfinite(value) or (value <= 0 if strict else value < 0):
        raise _Usage(f'{flag} {rule}, got "{raw}"')
    return value


def _json(raw: str, what: str) -> Any:
    try:
        return json.loads(raw)
    except ValueError as e:
        raise _Usage(f"{what} is not valid JSON: {e}") from None


def _resolve() -> tuple[ActioConfig, SyncProvider]:
    try:
        config = load_config()
        sync = create_sync_provider(config.sync, spawn=_spawn, on_error=lambda e: err(f"sync: {e}"))
    except ValueError as e:
        raise RuntimeError(f"config: {e}") from None
    return config, sync


def _spawn(coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
    return asyncio.get_running_loop().create_task(coro, name=name)  # fan-out


def _log_line(event: ActioEvent) -> str:
    if isinstance(event, LogEvent):
        return f"log: {event.message}"
    return type(event).__name__.removesuffix("Event").lower()


class _Vehicle:
    """The `AgentContext` of `call`/`cmd`: a vehicle, not an agent — no spec, no status, no announce."""

    def __init__(self, config: ActioConfig, src: str, sync: SyncProvider, logs: list[str]) -> None:
        root, agent = config.root, os.path.join(config.root, src)
        self.config = config
        self.id = src
        self.folders = AgentFolders(
            root=root,
            agent=agent,
            inbox=os.path.join(agent, "inbox"),
            forum=os.path.join(root, "forum"),
            nuntium=os.path.join(root, "nuntium"),
        )
        self.sync = sync
        self._logs = logs

    def now(self) -> float:
        return self.config.clock()

    def spec(self) -> Spec:
        raise RuntimeError("the CLI vehicle has no spec: it is not a started agent")

    def state(self) -> StatusState:
        return "idle"

    def emit(self, event: ActioEvent) -> None:
        self._logs.append(_log_line(event))

    async def append_diarium(self, entry: DiariumEntry) -> None:
        await append(self.folders.agent, entry, self.sync)


class _Hooks:
    def __init__(self, logs: list[str]) -> None:
        self._logs = logs

    def on_watch_error(self, folder: str, error: BaseException) -> None:
        self._logs.append(f"watch error in {folder}: {error}")

    def on_state_change(self) -> None:
        pass


def _print_outcome(do_json: bool, outcome: RequestOutcome) -> None:
    if do_json:
        doc: dict[str, Any] = {"finalStatus": outcome.final_status}
        if outcome.response is not None:
            doc["response"] = outcome.response
        out(json.dumps(doc, indent=2))
        return
    out(f"finalStatus: {outcome.final_status}")
    res = outcome.response
    if res is None:
        out("response: none")
        return
    if res.get("error"):
        result = cast("dict[str, Any]", res.get("result") or {})
        out(f"error: {result.get('code')} {json.dumps(result.get('msg'))}")
    else:
        # Compact separators: the reference prints `JSON.stringify(res.result)`, which puts no space
        # after a colon, and §6 is the same output, not an equivalent one.
        out(f"result: {json.dumps(res.get('result'), separators=(',', ':'))}")
    if res.get("cancellation"):
        out("cancellation: true")


async def _send(
    config: ActioConfig,
    sync: SyncProvider,
    do_json: bool,
    mode: str,
    pos: list[str | None],
    timeout_raw: str | None,
) -> int:
    src, dst, name, raw_params = pos
    if src is None or dst is None or name is None or (mode == "call" and raw_params is None):
        if mode == "call":
            raise _Usage("call needs <src-id> <dst> <capability> <json-params> [--timeout s]")
        raise _Usage("cmd needs <src-id> <dst> <name> [json-params]")
    params = None if raw_params is None else _json(raw_params, "json-params")
    timeout = _number("--timeout", timeout_raw, DEFAULT_TIMEOUT, "must be > 0 seconds", True)
    agent = os.path.join(config.root, src)
    await make_dirs(agent)
    await make_dirs(os.path.join(agent, "inbox"))
    logs: list[str] = []
    watcher = await Watcher.open(_spawn)
    requestor = Requestor(_Vehicle(config, src, sync, logs), _Hooks(logs), watcher, _spawn)
    try:
        if mode == "cmd":
            out(f"rid: {await requestor.command(dst, name, params)}")
            return 0
        outcome = await requestor.request(dst, name, params, timeout=timeout)
        _print_outcome(do_json, outcome)
        return 0 if outcome.response is not None else 1
    finally:
        await requestor.stop()
        await watcher.close()
        for line in logs:
            err(f"{src}: {line}")


async def _ls(config: ActioConfig, do_json: bool) -> int:
    found = await scan(config.root)
    now = config.clock()
    agents: list[dict[str, Any]] = []
    for a in found.agents:
        status = a.status
        if status is None:
            continue  # the scan keeps only folders that published both files
        agents.append(
            {
                "id": a.spec["id"],
                "folder": a.folder,
                "status": status["status"],
                "datetime": status["datetime"],
                "health": health(status, now) or "unknown",
                "capabilities": [
                    {
                        "name": c["name"],
                        "disabled": (c.get("actio") or {}).get("disabled") is True,
                        "accepts_from": (c.get("actio") or {}).get("accepts_from") or [],
                    }
                    for c in a.spec["capabilities"]
                ],
            }
        )
    if do_json:
        out(
            json.dumps(
                {
                    "root": config.root,
                    "counts": {
                        "agents": len(agents),
                        "duplicates": len(found.duplicates),
                        "ignored": len(found.ignored),
                    },
                    "agents": agents,
                    "duplicates": found.duplicates,
                    "ignored": [{"path": i.path, "reason": i.reason} for i in found.ignored],
                },
                indent=2,
            )
        )
        return 0
    for row in agents:
        out(f"{row['id']}  {row['status']}  {row['health']}  {row['datetime']}  {row['folder']}")
        for cap in row["capabilities"]:
            hints = (" [disabled]" if cap["disabled"] else "") + (
                f" [accepts_from: {','.join(cap['accepts_from'])}]" if cap["accepts_from"] else ""
            )
            out(f"    {cap['name']}{hints}")
    for dup in found.duplicates:
        out(f"duplicate agent id: {dup}")
    for ign in found.ignored:
        out(f"ignored: {ign.path}: {ign.reason}")
    if not agents:
        out(f"no agents under {config.root}")
    return 0


async def _diarium(config: ActioConfig, do_json: bool, agent: str | None, n_raw: str | None) -> int:
    if agent is None:
        raise _Usage("diarium needs an agent id")
    n = _number("-n", n_raw, 20, "must be a number >= 0")
    if not n.is_integer() or n < 1:
        raise _Usage(f'-n must be a positive integer, got "{n_raw}"')
    path = os.path.join(config.root, agent, "diarium.jsonl")
    try:
        raw = await read_text(path)
    except (OSError, UnicodeDecodeError) as e:
        err(f'cannot read "{path}": {e}')
        return 1
    for line in [t for t in raw.split("\n") if t.strip()][-int(n) :]:
        if do_json:
            try:
                out(json.dumps(json.loads(line), indent=2))
                continue
            except ValueError:
                pass
        out(line)
    return 0


async def _broadcast(config: ActioConfig, sync: SyncProvider, text: str | None) -> int:
    if text is None:
        raise _Usage('broadcast needs "<text>"')
    now = config.clock()
    file = {"id": "broadcast", "datetime": format_iso(now), "ts": int(now), "nuntium": text}
    reason = validate("announce", file)
    if reason is not None:
        err(f"broadcast: {reason}")
        return 1
    target = os.path.join(config.root, "nuntium", "nuntium.json")
    await make_dirs(os.path.dirname(target))
    await write_json_atomic(target, file, config.root, sync)
    out(f"wrote {target}")
    return 0


def _validate_diarium(raw: str) -> int:
    good = 0
    for i, text in enumerate(raw.split("\n")):
        if not text.strip():
            continue
        try:
            doc = json.loads(text)
        except ValueError as e:
            err(f"diarium: line {i + 1} is not valid JSON: {e}")
            return 1
        reason = validate("diarium", doc)
        if reason is not None:
            err(f"diarium: line {i + 1}: {reason}")
            return 1
        good += 1
    out(f"OK diarium ({good} lines)")
    return 0


async def _validate_cmd(config: ActioConfig, file: str | None, as_kind: str | None) -> int:
    if file is None:
        raise _Usage("validate needs <file>")
    kind = cast("SchemaKind | None", as_kind if as_kind is not None else file_kind(file))
    if kind is None or kind not in _KINDS:
        shown = as_kind if as_kind is not None else os.path.basename(file)
        err(f'no validator for "{shown}": use --as ' + "|".join(_KINDS))
        return 1
    try:
        raw = await read_text(file)
    except (OSError, UnicodeDecodeError) as e:
        err(f'cannot read "{file}": {e}')
        return 1
    if kind == "diarium":
        return _validate_diarium(raw)
    try:
        doc = json.loads(raw)
    except ValueError as e:
        err(f"{file}: not valid JSON: {e}")
        return 1
    reason = validate(kind, doc)
    if reason is not None:
        err(f"{kind}: {reason}")
        return 1
    out(f"OK {kind}")
    return 0


async def _run(args: list[str]) -> int:
    sub, ns, do_json = _parse(args)
    config, sync = _resolve()
    if sub == "ls":
        return await _ls(config, do_json)
    if sub == "diarium":
        return await _diarium(config, do_json, ns.agent, ns.n)
    if sub in ("call", "cmd"):
        pos = [ns.src, ns.dst, ns.capability if sub == "call" else ns.name, ns.json_params]
        return await _send(config, sync, do_json, sub, pos, ns.timeout if sub == "call" else None)
    if sub == "broadcast":
        return await _broadcast(config, sync, ns.text)
    return await _validate_cmd(config, ns.file, ns.as_kind)


_SUBS = ("ls", "diarium", "call", "cmd", "broadcast", "validate")


def main(argv: Sequence[str] | None = None) -> int:
    """Run one CLI command and return its exit code; `python -m actio` and the console script share it."""
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        return asyncio.run(_run(args))
    except _Usage as e:
        _usage_out(e)
        return 1
    except Exception as e:  # the reference prints the reason and exits 1
        err(str(e))
        return 1
