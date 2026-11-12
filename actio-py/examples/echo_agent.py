"""The echo agent: the black-box conformance target — its behaviour is fixed.

The environment, the five capabilities and the `noop` command match `../actio-ts/examples/echo-agent.ts`
field by field, because the interop suite drives both through the same scenarios. It runs until
`stop` (a command) or SIGINT/SIGTERM (graceful), then exits 0.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from typing import Any

from actio import ActioAgent, ActioError


def _object(
    properties: dict[str, Any], required: list[str], strict: bool = False
) -> dict[str, Any]:
    """One object schema: the given properties, the required ones, nothing else when `strict`."""
    schema: dict[str, Any] = {"type": "object", "properties": properties, "required": required}
    if strict:
        schema["additionalProperties"] = False
    return schema


# The `{}` parameters of a capability that takes nothing.
NO_PARAMS: dict[str, Any] = {"type": "object", "properties": {}, "additionalProperties": False}

ECHO: dict[str, Any] = {
    "name": "echo",
    "description": "echoes the text it gets",
    "parameters": _object({"text": {"type": "string"}}, ["text"], strict=True),
    "actio": {"result": _object({"text": {"type": "string"}}, ["text"])},
}

SLEEP: dict[str, Any] = {
    "name": "sleep",
    "description": "waits the given seconds and reports them; honours cancellation",
    "parameters": _object({"seconds": {"type": "number"}}, ["seconds"], strict=True),
    "actio": {"result": _object({"slept": {"type": "number"}}, ["slept"]), "max_parallel": 1},
}

FAIL: dict[str, Any] = {
    "name": "fail",
    "description": "always fails with ActioError code 1000",
    "parameters": NO_PARAMS,
    "actio": {"result": True},
}

OFF: dict[str, Any] = {
    "name": "off",
    "description": "disabled in the spec",
    "parameters": NO_PARAMS,
    "actio": {"result": True, "disabled": True},
}

PRIVATE: dict[str, Any] = {
    "name": "private",
    "description": "callable only by nobody",
    "parameters": NO_PARAMS,
    "actio": {"result": True, "accepts_from": ["nobody"]},
}


async def _echo(params: dict[str, Any], ctx: Any) -> dict[str, Any]:
    return {"text": params["text"]}


async def _sleep(params: dict[str, Any], ctx: Any) -> dict[str, Any]:
    """Wakes the moment the request is cancelled and reports the whole seconds (A3)."""
    seconds = params["seconds"]
    wake = asyncio.ensure_future(asyncio.sleep(seconds))
    done = asyncio.ensure_future(ctx.cancelled.wait())
    try:
        await asyncio.wait({wake, done}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        wake.cancel()
        done.cancel()
    return {"slept": seconds}


async def _fail(params: Any, ctx: Any) -> None:
    raise ActioError(1000, "fail: requested failure")


async def _off(params: Any, ctx: Any) -> dict[str, Any]:
    return {}


async def _private(params: Any, ctx: Any) -> dict[str, Any]:
    return {}


async def _noop(params: Any, ctx: Any) -> None:
    return None


def main() -> int:
    agent_id = os.environ.get("ACTIO_AGENT_ID", "echo")
    commanders = [s for s in os.environ.get("ACTIO_COMMANDERS", "").split(",") if s]
    # An empty list must not be passed as `[]`: absent means the null namespace.
    namespaces = [s for s in os.environ.get("ACTIO_NAMESPACES", "").split(",") if s]
    agent = ActioAgent(
        id=agent_id,
        description="conformance target agent (echo)",
        accepts_commands_from=commanders,
        namespaces=namespaces if namespaces else None,
    )
    agent.capability(ECHO, _echo)
    agent.capability(SLEEP, _sleep)
    agent.capability(FAIL, _fail)
    agent.capability(OFF, _off)
    agent.capability(PRIVATE, _private)
    agent.on_command("noop", _noop)

    async def run() -> int:
        loop = asyncio.get_running_loop()
        code = 0
        stop_task: asyncio.Task[None] | None = None

        async def stop() -> None:
            nonlocal code
            try:
                await agent.stop()
            except Exception:
                code = 1

        def on_signal() -> None:
            nonlocal stop_task
            stop_task = loop.create_task(stop())

        # The library installs no handlers; the example does, before `start`, as the reference
        # registers `process.on` before `start`: SIGINT/SIGTERM is a graceful lane C, then exit 0.
        # A `stop` command reaches the same point on its own.
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, on_signal)
        await agent.start()
        await agent.wait_stopped()
        return code

    return asyncio.run(run())


if __name__ == "__main__":
    sys.exit(main())
