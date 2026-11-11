"""Capability registrations to a valid `spec.json` (*Agent Spec*, D-22, D-10). WP7.

The builder writes only what the operator gave: an absent `namespaces` stays absent, because absent,
`null` and `[]` are three legal ways to say "the null namespace" and the builder must not invent a value
(D-22). Capabilities keep registration order and are copied, so a later change by the caller cannot
alter what was published. A built spec that fails validation is our bug (I-2): it raises, and nothing
reaches the tree. `security_support` is empty and `security_posture` is `trusted`: the core profile
trusts the shared tree and has no keys.
"""

from __future__ import annotations

from collections.abc import Sequence

from ._protocol import AgentId, Capability, Spec


def build_spec(
    *,
    id: AgentId,
    description: str,
    capabilities: Sequence[Capability],
    ts: float,
    accepts_commands_from: Sequence[AgentId] = ("*",),
    namespaces: Sequence[str | None] | None = None,
    depends: Sequence[AgentId] | None = None,
    wants: Sequence[AgentId] | None = None,
    info: dict[str, object] | None = None,
) -> Spec:
    """A spec that passes validation, or `ValueError` for a duplicated capability name."""
    raise NotImplementedError("WP7")
