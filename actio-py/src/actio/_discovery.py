"""Discovery: the scan of the tree, and the callability rules (*Announcement and Discovery*, *Callability rules* 1-8).

`scan` reads through `_files.py` and nothing else: one folder listing, then one read per publication
file (`actio.json`, each `spec.json`, each `status.json`) — a scan of N agents is 1 + 2N + 1 hops, each
one a folder or file read. It returns what it found, the duplicate ids and the ignored files with a
reason. The callability rules then decide what the caller may use: `agent.agmen()` is the view after those rules,
never the raw scan. Rule 4's namespace reading treats absent, `null` and `[]` as the null namespace, and
`"*"` shares with everyone (*Callability rules*).

`DiscoveredAgent.status` is typed `Status | None` in the frozen `_types.py`, but the scan never produces
`None`: a folder without a `status.json` is not an agent (*Announcement and Discovery* step 2) and is
reported in `ignored` instead.
"""

from __future__ import annotations

import os
from typing import Any

from ._files import list_dir, loads, read_text, unreadable_reason
from ._protocol import AgentId, Capability, Spec
from ._status import health
from ._types import AgentView, DiscoveredAgent, IgnoredFile, ScanResult
from ._validate import malformed_reason, unique_capability_names, validate

# *Announcement and Discovery* step 1: the reserved folders are never agents.
_RESERVED: frozenset[str] = frozenset({"tools", "forum", "nuntium"})


async def _read_publication(path: str) -> tuple[str, Any, str]:
    """One publication file as `(kind, value, reason)`: `kind` is `missing`, `unreadable`,
    `unparsable` or `json`. A file that exists but cannot be read or parsed is reported, never
    raised and never silent (D-15)."""
    try:
        text = await read_text(path)
    except FileNotFoundError:
        return "missing", None, ""
    except UnicodeDecodeError as err:
        return "unparsable", None, malformed_reason(err)
    except OSError as err:
        # D-15: one line, with the error's text and never a value of the peer's; `_files` words it,
        # because only the reader knows what failed.
        return "unreadable", None, unreadable_reason(err)
    try:
        return "json", loads(text), ""
    except ValueError as err:
        return "unparsable", None, malformed_reason(err)


async def _scan(root: str) -> ScanResult:
    """The whole tree read: the one thing `scan` hands to the thread (*Files*: one switch)."""
    agents: list[DiscoveredAgent] = []
    duplicates: list[str] = []
    ignored: list[IgnoredFile] = []
    seen: set[AgentId] = set()

    # *Team metadata*: `actio.json` is optional and read-only for agents. The protocol asks that a
    # non-conforming one be ignored (*Strict format*); a conforming one has no place in the result.
    team_path = os.path.join(root, "actio.json")
    kind, team, reason = await _read_publication(team_path)
    if kind in ("unreadable", "unparsable"):
        ignored.append(IgnoredFile(path=team_path, reason=reason))
    elif kind == "json":
        team_err = validate("actio", team)
        if team_err is not None:
            ignored.append(IgnoredFile(path=team_path, reason=team_err))

    for name in sorted(
        await list_dir(root)
    ):  # a folder listing is a file call: it goes through _files
        if name.startswith(".") or name in _RESERVED:
            continue
        folder = os.path.join(root, name)
        if not os.path.isdir(folder):
            continue  # a file at the root (actio.json and the like) is not an agent folder
        spec_path = os.path.join(folder, "spec.json")
        status_path = os.path.join(folder, "status.json")
        spec_kind, spec_doc, spec_reason = await _read_publication(spec_path)
        status_kind, status_doc, status_reason = await _read_publication(status_path)
        if spec_kind == "missing" or status_kind == "missing":
            # step 2: a folder that has not published both files is not an agent
            for file, file_kind in ((spec_path, spec_kind), (status_path, status_kind)):
                if file_kind == "missing":
                    ignored.append(
                        IgnoredFile(path=file, reason=f"missing {os.path.basename(file)}")
                    )
            continue
        if spec_kind != "json":
            ignored.append(IgnoredFile(path=spec_path, reason=spec_reason))
            continue
        if status_kind != "json":
            ignored.append(IgnoredFile(path=status_path, reason=status_reason))
            continue
        spec_err = validate("spec", spec_doc)
        if spec_err is not None:
            ignored.append(IgnoredFile(path=spec_path, reason=spec_err))
            continue
        # *Schemas*: a spec that repeats a capability name is ignored like a schema failure
        repeated = unique_capability_names(spec_doc)
        if repeated is not None:
            ignored.append(
                IgnoredFile(path=spec_path, reason=f'duplicate capability name "{repeated}"')
            )
            continue
        status_err = validate("status", status_doc)
        if status_err is not None:
            ignored.append(IgnoredFile(path=status_path, reason=status_err))
            continue
        spec = spec_doc  # the schema said so: a Spec now
        if spec["id"] in seen:
            duplicates.append(folder)  # D-3: the first folder in lexical order keeps the id
            continue
        seen.add(spec["id"])
        agents.append(DiscoveredAgent(folder=folder, spec=spec, status=status_doc))
    return ScanResult(agents=agents, duplicates=duplicates, ignored=ignored)


async def scan(root: str) -> ScanResult:
    """Public: every agent folder under `root`, with duplicates and ignored files reported."""
    return await _scan(root)


def _capability_usable(self_id: AgentId, cap: Capability) -> bool:
    """Rule 5.2: usable unless `actio.disabled` is true, or `actio.accepts_from` is a non-empty list
    that does not contain us; absent or empty `accepts_from` means everybody."""
    if cap["actio"].get("disabled") is True:
        return False
    accepts_from = cap["actio"].get("accepts_from")
    return accepts_from is None or len(accepts_from) == 0 or self_id in accepts_from


def _namespaces(spec: Spec) -> tuple[bool, set[str | None]]:
    """(*Agent Spec*): absent, `null` or an empty list is the `null` namespace; a filled list does not
    include it unless `null` is listed explicitly; `"*"` (alone or combined) is every namespace."""
    listed = spec.get("namespaces")
    if listed is None or len(listed) == 0:
        return False, {None}
    names: set[str | None] = set()
    for ns in listed:
        if ns == "*":
            return True, names
        names.add(ns)
    return False, names


def _share_namespace(self_spec: Spec, peer_spec: Spec) -> bool:
    """Rule 2: two agents share a namespace when one of them belongs to all of them, or their lists meet."""
    mine_every, mine = _namespaces(self_spec)
    their_every, theirs = _namespaces(peer_spec)
    if mine_every or their_every:
        return True
    return bool(mine & theirs)


def agent_view(self_spec: Spec, peer: DiscoveredAgent, now: float) -> AgentView | None:
    """The peer as the callability rules leave it usable, or None when they remove it."""
    peer_spec = peer.spec
    status = peer.status
    if status is None:
        return None  # unreachable: the scan never produces it, and a view needs a status to rate
    # Rule 1: a disabled peer offers no capabilities; commands can still reach it, so it can be
    # enabled again. Only new requests are affected — accepted work continues.
    usable: list[Capability] = (
        []
        if peer_spec.get("disabled") is True
        else [cap for cap in peer_spec["capabilities"] if _capability_usable(self_spec["id"], cap)]
    )
    # Rule 2: no namespace in common — the spec is not considered at all.
    if not _share_namespace(self_spec, peer_spec):
        return None
    # Rules 3 and 4: the core profile implements no security feature, so a peer that requires any
    # posture but `trusted` needs something we do not have; the zero-trust issuer possession check
    # is unreachable for the same reason.
    if peer_spec["security_posture"] != "trusted":
        return None
    # Rule 5.1: an empty `accepts_commands_from` means nobody.
    commands = self_spec["id"] in peer_spec["accepts_commands_from"]
    # Rule 6: nothing usable — the agent is discarded.
    if not commands and not usable:
        return None
    # Rule 7: an inactive or error agent will not respond.
    if status["status"] in ("inactive", "error"):
        return None
    # Rule 8: health is a flag (degraded/zombie stay in the view), never a filter.
    return AgentView(
        id=peer_spec["id"],
        folder=peer.folder,
        spec=peer_spec,
        status=status,
        health=health(status, now),
        commands=commands,
        capabilities={cap["name"]: cap for cap in usable},
    )
