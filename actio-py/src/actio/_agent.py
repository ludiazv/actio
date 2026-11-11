"""`ActioAgent`: the lifecycle, lane C (its own capability registrations), the commands, and the API
plan §4 froze (*Agent Lifecycle*, *Agent's status*, *Namespaces*, D-8, D-9, D-10, D-17). WP7.

The signature below is the frozen one: keyword-only, and `namespaces=None` means the field is **absent**
from `spec.json`, not `null` inside it (D-22). Nothing of the environment is read until `start()`, so a
bad `ACTIO_SYNC` surfaces there (D-21).

An agent belongs to one event loop and is not thread-safe (plan §4 semantics 7). It installs no signal
handler and starts no event loop (semantics 8): the example does both.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Mapping, Sequence
from typing import Any, overload

from ._config import ActioConfig
from ._protocol import AgentId, Capability, Rid
from ._types import (
    ActioEvent,
    AgentContext,
    AgentView,
    CapabilityHandler,
    CommandHandler,
    DiariumRead,
    RequestOutcome,
)


class ActioAgent(AgentContext):
    """One agent in the shared tree: it serves its inbox and talks to the others."""

    def __init__(
        self,
        *,
        id: AgentId,
        description: str,
        accepts_commands_from: Sequence[AgentId],
        namespaces: Sequence[str | None] | None = None,
        depends: Sequence[AgentId] | None = None,
        wants: Sequence[AgentId] | None = None,
        info: dict[str, Any] | None = None,
        config: ActioConfig | None = None,
    ) -> None: ...

    @overload
    def capability(self, capability: Capability, handler: CapabilityHandler) -> None: ...
    @overload
    def capability(
        self, capability: Capability, handler: None = None
    ) -> Callable[[CapabilityHandler], CapabilityHandler]: ...
    def capability(
        self, capability: Capability, handler: CapabilityHandler | None = None
    ) -> Callable[[CapabilityHandler], CapabilityHandler] | None:
        """Register a capability: a call, or the decorator form. `ValueError` for a bad capability or a
        name already taken; `TypeError` for a handler that is not `async def`. The input is deep-copied,
        so the caller cannot alias what we publish (plan §5.4)."""
        raise NotImplementedError("WP7")

    @overload
    def on_command(self, name: str, handler: CommandHandler) -> None: ...
    @overload
    def on_command(
        self, name: str, handler: None = None
    ) -> Callable[[CommandHandler], CommandHandler]: ...
    def on_command(
        self, name: str, handler: CommandHandler | None = None
    ) -> Callable[[CommandHandler], CommandHandler] | None:
        """Register a custom command, in the same two forms."""
        raise NotImplementedError("WP7")

    def on_event(self, handler: Callable[[ActioEvent], None]) -> None:
        """Subscribe to the observability stream. Callbacks run synchronously on the loop and must
        return quickly; one that raises produces a single `LogEvent` (§4 semantics 5)."""
        raise NotImplementedError("WP7")

    async def start(self) -> None:
        """Folders, spec, status, diarium, inbox, announce, watchers, the first scan (steps 1-5)."""
        raise NotImplementedError("WP7")

    async def stop(self, *, force: bool = False) -> None:
        """D-9's order; after it nothing of ours is armed: no task, timer, watcher or thread."""
        raise NotImplementedError("WP7")

    async def wait_stopped(self) -> None:
        """Returns when the agent has stopped for good. A `restart` does not end it (D-10)."""
        raise NotImplementedError("WP7")

    async def __aenter__(self) -> ActioAgent:
        raise NotImplementedError("WP7")

    async def __aexit__(self, *exc: object) -> None:
        raise NotImplementedError("WP7")

    def agmen(self) -> Mapping[AgentId, AgentView]:
        """What the callability rules leave usable, read-only, keyed by agent id (§4)."""
        raise NotImplementedError("WP7")

    async def refresh(self) -> None:
        """A full discovery scan now; read the new view with `agmen()`."""
        raise NotImplementedError("WP7")

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
        """Lane B. Raises `ActioCallError` when the callability rules refuse it locally, before writing."""
        raise NotImplementedError("WP7")

    async def command(self, dst: AgentId, name: str, params: Any = None) -> Rid:
        """A command to a peer that accepts commands from us (rules 5.1 and 11)."""
        raise NotImplementedError("WP7")

    async def fail(self, message: str) -> None:
        """Status `error` plus an announce; there is no recovery on its own."""
        raise NotImplementedError("WP7")

    async def diarium(self) -> DiariumRead:
        """This agent's own diarium: the valid lines and the ignored ones with a reason."""
        raise NotImplementedError("WP7")

    async def log(self, message: str) -> None:
        """A diarium `log` event."""
        raise NotImplementedError("WP7")

    def _spawn(self, coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
        """The one task helper (§5.2): it keeps the strong reference and reports a crash as a `LogEvent`."""
        raise NotImplementedError("WP7")
