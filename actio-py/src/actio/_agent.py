"""`ActioAgent`: the lifecycle, lane C (its own capability registrations), the commands, and the public
API (*Workflow*, *Agent's status*, *Callability rules*, D-8, D-9, D-10).

The signature below is the frozen one: keyword-only, and `namespaces=None` means the field is **absent**
from `spec.json`, not `null` inside it (*Callability rules*). Nothing of the environment is read until `start()`,
so a bad `ACTIO_SYNC` surfaces there (D-21).

An agent belongs to one event loop and is not thread-safe. It installs no signal handler and starts no
event loop: the example does both.
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


class ActioAgent:
    """One agent in the shared tree: it serves its inbox and talks to the others.

    It subclasses nothing. Its engines (the receiver, the requestor, the status publisher) are handed
    `_ctx`, a private object that satisfies `AgentContext` structurally; making the agent itself the
    context would put `emit`, `sync`, `folders` and the rest on the public surface, and would make the
    class abstract to a consumer's `mypy`.
    """

    _ctx: AgentContext  # built by `start()`: the seam, never a public attribute

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
        so the caller cannot alias what we publish."""
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
        return quickly; one that raises produces a single `LogEvent`."""
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
        """What the callability rules leave usable, read-only, keyed by agent id."""
        raise NotImplementedError("WP7")

    async def refresh(self) -> Mapping[AgentId, AgentView]:
        """A full discovery scan now, and the new view: the same mapping `agmen()` then returns."""
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
        """Lane B. Raises `ActioCallError` when the callability rules refuse it locally, before writing.

        `timeout == 0` means wait until the answer or the cancel; the caller's own task being cancelled
        still ends the exchange correctly in the background.
        """
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
        """The one task helper, of the shape `Spawn` in `_types.py`: it keeps the strong reference and
        reports a crash as a `LogEvent`. Everything long-lived comes from here."""
        raise NotImplementedError("WP7")
