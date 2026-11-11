"""Lane B: requests and commands to other agents (*Workflows B and C*, D-6, D-12, D-18, D-22). WP6.

An exchange subscribes to the destination inbox on the agent's one watcher **before** it writes the
request, then loops: clear `changed`, look for its own `<rid>.res.json`, then wait on `changed`, the
timeout, the cancel flag and stop (D-18). It unsubscribes when the exchange ends; a self-call is no
special case.

- `timeout > 0`: at expiry, `B4` (write the cancellation), wait up to `cancel_wait_timeout`, then
  abandon. `timeout == 0`: wait forever unless `cancel` is set or the caller's task is cancelled (§4
  semantics 1) — and then the exchange still ends correctly in the background.
- A peer's error answer is not an exception: the outcome carries `response["error"] is True`.
- A response whose `result` fails the capability's `result` schema raises `ActioResultError` (D-12).
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any, Protocol

from ._protocol import AgentId, Rid
from ._types import AgentContext, RequestOutcome
from ._watch import Watcher


class RequestorHooks(Protocol):
    """What the requestor asks its agent to do; a test drives it with a fake."""

    def on_watch_error(self, folder: str, error: BaseException) -> None:
        """D-22: a destination inbox was lost; every outbound exchange there is abandoned."""
        ...

    def on_state_change(self) -> None:
        """A busy/idle-relevant change (D-8)."""
        ...


class Requestor:
    """Issues requests and commands for one agent, and ends every exchange it starts."""

    def __init__(self, ctx: AgentContext, hooks: RequestorHooks, watch: Watcher) -> None: ...

    async def request(
        self,
        dst: AgentId,
        name: str,
        params: Any = None,
        *,
        timeout: float = 0,
        attachments: Sequence[str] = (),
        cancel: asyncio.Event | None = None,
    ) -> RequestOutcome:
        """One exchange, end to end. Raises `OSError` for a write that failed, never for a peer's error."""
        raise NotImplementedError("WP6")

    async def command(self, dst: AgentId, name: str, params: Any = None) -> Rid:
        """Write the command and return its rid: a command is never answered (*Request and command*)."""
        raise NotImplementedError("WP6")

    async def stop(self, *, force: bool = False) -> None:
        """End what is outstanding, in D-9's order; nothing is left armed afterwards."""
        raise NotImplementedError("WP6")
