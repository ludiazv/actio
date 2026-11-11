"""The spec builder: the agent's options and the registered capabilities into a valid `spec.json`
(*Agent Spec*).

The builder is the only place a spec object is born, so it is the only place its shape is decided:
the core profile's security fields, a fresh `datetime` from the injected clock, and the
capabilities in registration order. A failing build is our bug, and it must not reach the tree:
the builder self-validates and raises, as the reference does.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from ._protocol import AgentId, Capability, Spec
from ._status import ACTIO_VERSION
from ._util import format_iso
from ._validate import validate


def build_spec(
    *,
    id: AgentId,
    description: str,
    accepts_commands_from: Sequence[AgentId],
    capabilities: Sequence[Capability],
    now: float,
    namespaces: Sequence[str | None] | None = None,
    depends: Sequence[AgentId] | None = None,
    wants: Sequence[AgentId] | None = None,
    info: Mapping[str, Any] | None = None,
) -> Spec:
    """A `spec.json` that passes the schema; raises on a duplicate capability name or on a spec the
    schema rejects. `disabled` is never written here: the `enable` command (D2) is the only writer
    during a run, and a fresh start clears it (D-10). `security` stays absent: the core profile has
    no keys for it."""
    names: set[str] = set()
    for cap in capabilities:
        # Duplicates are ours to reject: the schema describes one capability, not the uniqueness of
        # a name across the list, and a duplicated name would make the agmen view silently lose one.
        if cap["name"] in names:
            raise ValueError(
                f'spec-builder: duplicate capability name "{cap["name"]}" in agent "{id}"'
            )
        names.add(cap["name"])
    spec: dict[str, Any] = {
        "id": id,
        "actio_version": ACTIO_VERSION,
        "datetime": format_iso(now),  # the injected clock, never the wall clock
        "description": description,
        # An empty list means nobody may command us (*Agent Spec*); the array is copied so a later
        # change to the options cannot alter what we published.
        "accepts_commands_from": list(accepts_commands_from),
        "capabilities": list(capabilities),  # registration order
        "security_support": [],  # core profile: no signatures, no encryption
        "security_posture": "trusted",  # core profile: trust the shared tree
    }
    # Optional fields appear only when given: absent, null and [] are three legal ways to say "the
    # null namespace" (*Namespaces*), so the builder must not invent a value the operator did not
    # write. `info` is free content: the schema only requires an object, and it is copied, like the
    # rest, so a later mutation of the options cannot alter what we publish.
    if namespaces is not None:
        spec["namespaces"] = list(namespaces)
    if depends is not None:
        spec["depends"] = list(depends)
    if wants is not None:
        spec["wants"] = list(wants)
    if info is not None:
        spec["info"] = dict(info)
    checked = cast(Spec, spec)
    reason = validate("spec", checked)
    if reason is not None:
        raise ValueError(f'spec-builder: invalid spec for agent "{id}": {reason}')
    return checked
