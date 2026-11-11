"""Discovery: the scan of the tree, and the callability rules (*Announcement and Discovery*, *Callability rules* 1-8).

One `scan` is one `to_thread` call in `_files.py`: it reads `actio.json`, every `spec.json` and every
`status.json`, and returns what it found, the duplicate ids and the ignored files with a reason. The
callability rules then decide what the caller may use: `agent.agmen()` is the view after those rules,
never the raw scan. Rule 4's namespace reading treats absent, `null` and `[]` as the null namespace, and
`"*"` shares with everyone (*Callability rules*).
"""

from __future__ import annotations

from ._protocol import Spec
from ._types import AgentView, DiscoveredAgent, ScanResult


async def scan(root: str) -> ScanResult:
    """Public: every agent folder under `root`, with duplicates and ignored files reported."""
    raise NotImplementedError("WP4")


def agent_view(self_spec: Spec, peer: DiscoveredAgent, now: float) -> AgentView | None:
    """The peer as the callability rules leave it usable, or None when they remove it."""
    raise NotImplementedError("WP4")
