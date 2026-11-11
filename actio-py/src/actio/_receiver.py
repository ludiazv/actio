"""Lane A (inbound requests) and lane D (commands): the receiver of one agent (*Workflow*,
*Acceptance errors*, D-3, D-4, D-5, D-7, D-8, D-11, D-15).

The receiver owns the inbox watch and the per-`rid` state of the exchanges it has accepted. Nothing here
runs a thread: it watches one folder through the agent's `Watcher` (D-23) and looks again when woken.

- Startup recovery (D-4) re-arms exchanges from the files that are still in the inbox.
- A `rid` already archived in this run is ignored, not re-served (D-5).
- Every later file of an exchange must carry the same header, `datetime` aside (D-11).
- A handler never crashes the agent: an `Exception` becomes an error response with `msg` and no `code`,
  and `ActioError` keeps its code.
- Archive is two steps in this order (D-11…D-16): the `inreq` line, then the deletions.
- A file taken from a folder is classified by its **body key** with `_validate.envelope_kind`, never by
  its name: to a receiver `<id>.announce.json` can be a perfectly good `nuntium`, and the name says nothing
  about the body. `file_kind` is the command's rule, not this one.
- Commands: `stop`, `enable`, `restart`, `ping` are mandatory and never advertised (D0).
"""

from __future__ import annotations

from typing import Protocol

from ._protocol import AgentId, Spec
from ._types import AgentContext
from ._watch import Watcher


class ReceiverHooks(Protocol):
    """What the receiver asks its agent to do; a test drives the receiver with a fake."""

    def on_watch_error(self, folder: str, error: BaseException) -> None:
        """D-22: the inbox watch was lost. The agent logs it and fails itself."""
        ...

    def peer_spec(self, src: AgentId) -> Spec | None:
        """The requester's spec as the last scan found it, before the callability rules (D-7's 106)."""
        ...

    def on_state_change(self) -> None:
        """A busy/idle-relevant change (D-8): the agent re-publishes its status."""
        ...

    async def on_command_stop(self, force: bool) -> None: ...

    async def on_command_restart(self, force: bool, delay_seconds: float) -> None: ...

    async def on_command_enable(self, enabled: bool) -> None: ...

    async def on_command_ping(self) -> None: ...


class Receiver:
    """Serves one agent's inbox until `stop`; created at `start`, closed by `stop`."""

    def __init__(self, ctx: AgentContext, hooks: ReceiverHooks, watch: Watcher) -> None: ...

    async def start(self) -> None:
        """Recover pending exchanges (D-4), then watch the inbox and serve it."""
        raise NotImplementedError("WP5")

    async def stop(self, *, force: bool = False) -> None:
        """Stop serving. `force` abandons work still running (D-9, D-10)."""
        raise NotImplementedError("WP5")
