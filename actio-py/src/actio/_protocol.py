"""The protocol shapes as `TypedDict`s, hand-written from `schemas/` (D-20).

No generator and no marshalling layer: an envelope is a plain `dict`, and `json.load` of one *is* a value
of these types. `tests/test_protocol_drift.py` compares every one of them against the schema node it
mirrors — key sets, required keys, types, and every `enum` in the 11 schemas — and its mutation controls
prove that comparison bites, so a type here cannot drift from a schema unnoticed.

`from __future__ import annotations` is deliberately **not** used here: stringified annotations would
break the `NotRequired` bookkeeping the drift test reads (`__required_keys__` and `__annotations__`).

The names follow the schemas' snake_case, not the reference's camelCase (*Schemas*). Durations are seconds,
`ts` is a Unix epoch second (`common.schema.json#/$defs/ts`), and `datetime` is the UTC pattern of
`common.schema.json#/$defs/datetime`, which has **no** milliseconds field.
"""

from typing import Literal, NotRequired, TypedDict

# Leaf scalars. The schemas constrain them with patterns, not types; the pattern is in the comment, and
# the validator enforces the patterns, so here they are only strings.
Ts = int  # Unix epoch seconds, 64-bit, >= 0
IsoDateTime = str  # YYYY-MM-DDThh:mm:ssZ
Rid = str  # ULID, Crockford base32: ^[0-7][0-9A-HJKMNP-TV-Z]{25}$
AgentId = str  # ^[a-zA-Z0-9_-]{1,32}$
CapabilityName = str  # ^[a-zA-Z0-9_-]+$
Nonce = str  # 128-bit hex: ^[0-9a-fA-F]{32}$
ProtocolVersion = str  # "<major>.<minor>"
Duration = int | float  # seconds; `number` in the schemas, and asyncio works in float seconds
AttachmentPath = str  # relative to `forum`, never absolute

type StatusState = Literal["inactive", "idle", "busy", "error"]
type TaskState = Literal["pending", "running", "waiting", "completed", "cancelled"]
type SecurityFeature = Literal["sign", "encrypt", "issuer_sign"]
type SecurityPosture = Literal["trusted", "untrusted", "secret", "zero-trust"]
type HeaderType = Literal["cmd", "req"]
type InreqFinalStatus = Literal["cancelled", "completed", "confirmed", "dropped"]
type OutreqFinalStatus = Literal["confirmed", "cancelled", "abandoned", "sent"]

ResultSchema = dict[str, object] | bool  # an OpenAI JSON Schema, or `false` for "returns nothing"
AnyValue = object  # `parameters` and `result` have no schema type: any JSON value


class ActioMeta(TypedDict, total=False):
    """`.actio/actio.json`: team metadata, read-only for agents (*Team metadata*). All fields optional."""

    actio_version: ProtocolVersion
    description: str
    metadata: dict[str, object]


class Header(TypedDict):
    """The six public fields at the top of every envelope file — no nested `header` key (*Envelope lifecycle*).

    Every file of an exchange repeats these values, and only `datetime` changes (D-11). `type` is `cmd`
    for a command and `req` for a request and everything that belongs to one — a `cancellation`, a
    `response` and a `confirmation` are all `req`, so `type` does not say which envelope a file is: the
    **body key** does (*Requests*). A file name is a recommendation; only the `actio` command reads it.
    """

    rid: Rid
    src: AgentId
    dst: AgentId
    datetime: IsoDateTime
    type: HeaderType
    actio_version: ProtocolVersion


class RequestBody(TypedDict):
    """The `request` of a request file — a capability request (`req`) or a command (`cmd`)."""

    ts: Ts
    nonce: Nonce
    signature: str | None
    name: str
    parameters: AnyValue  # the parameters of the request or command; null when there are none
    timeout: Duration  # the requestor's timeout; 0 means no timeout
    attachments: list[AttachmentPath]  # can be empty, never null


class RequestFile(Header):
    """`<rid>.json` in the inbox of the receiver. `request` is a base64 string when encrypted (*Security*)."""

    request: RequestBody | str


class CancellationBody(TypedDict):
    """The `cancellation` of a cancellation file; commands cannot be cancelled (*Cancellation*)."""

    ts: Ts
    nonce: Nonce
    signature: str | None
    name: CapabilityName  # the same name as the request it cancels
    force: bool


class CancellationFile(Header):
    """`<rid>.can.json` in the inbox of the receiver; the header repeats the request's, bar `datetime`.

    The schema narrows `type` to the constant `"req"` here (through `requestHeader`); the type does not
    model that narrowing, because `if`/`then` refinements are not reflected in these types at all — the
    validator is what enforces it, as in the reference's own generated types.
    """

    cancellation: CancellationBody | str


class ResponseBody(TypedDict):
    """The `response` of a response file (*Response*).

    A cancellation response has `cancellation: true`, `error: false`, `result: null` and no attachments;
    `error: true` makes `result` an `ErrorResult`. Both are `if`/`then` in the schema, and the receiver
    test checks them (D-20).
    """

    ts: Ts
    nonce: Nonce
    signature: str | None
    error: bool
    cancellation: bool
    result: AnyValue  # the capability result, an `ErrorResult`, or null for a cancellation response
    attachments: list[AttachmentPath]
    work_time: NotRequired[Duration]  # seconds of actual work, without waiting


class ResponseFile(Header):
    """`<rid>.res.json` in the inbox of the *writer of the request* — the receiver's own inbox (*Response*)."""

    response: ResponseBody | str


class ConfirmationBody(TypedDict):
    """The `confirmation` of a confirmation file: receipt of a response that has been captured (*Response Confirmation*)."""

    ts: Ts
    nonce: Nonce
    signature: str | None


class ConfirmationFile(Header):
    """`<rid>.con.json`, written by the requestor after it moved the response out of its inbox."""

    confirmation: ConfirmationBody | str


class ErrorResult(TypedDict):
    """The `result` of an error response; the acceptance codes come from the protocol (*Acceptance errors*)."""

    msg: str
    code: NotRequired[int]
    details: NotRequired[str]


class SecurityAttribute(TypedDict):
    """One `security` entry of a spec: a key and its value, e.g. a public key (*Security features*)."""

    key: str
    value: str


class CapabilityActio(TypedDict):
    """The `actio` part of a capability: `result` is required; the rest has defaults (*Agent Spec*).

    `skill`, `tool`, `tool_name` and `sync` are harness hints: the protocol never reads them, and
    `_spec_builder` passes them through.
    """

    result: ResultSchema
    skill: NotRequired[str]
    tool: NotRequired[bool]  # a harness hint: run the capability as a tool call
    tool_name: NotRequired[str]  # the name the harness sees, when it differs
    sync: NotRequired[bool]
    accepts_from: NotRequired[list[AgentId]]  # absent or empty: every agent
    max_parallel: NotRequired[int]  # 0 means unbounded
    recommended_timeout: NotRequired[Duration]  # seconds
    disabled: NotRequired[bool]


class Capability(TypedDict):
    """An OpenAI tool definition plus `actio`; all four keys are required (*Agent Spec*)."""

    name: CapabilityName
    description: str
    parameters: dict[str, object]
    actio: CapabilityActio


class Spec(TypedDict):
    """`spec.json`: identity, capabilities and security features of an agent (*Agent Spec*)."""

    id: AgentId
    actio_version: ProtocolVersion
    datetime: IsoDateTime
    description: str
    accepts_commands_from: list[AgentId]
    capabilities: list[Capability]
    security_support: list[SecurityFeature]
    security_posture: SecurityPosture
    namespaces: NotRequired[list[str | None] | None]  # absent, null or [] means the null namespace
    depends: NotRequired[list[AgentId]]
    wants: NotRequired[list[AgentId]]
    disabled: NotRequired[bool]  # absent means false
    security: NotRequired[list[SecurityAttribute]]
    info: NotRequired[dict[str, object]]  # free content, never validated


class CapabilityStatusEntry(TypedDict):
    """One entry of `capability_status`: how many requests run and how many wait (*Agent's status*)."""

    name: CapabilityName
    running: int
    queue: int


class StatusTask(TypedDict):
    """One entry of `tasks`: an internal task, disclosed at the agent's discretion."""

    capability: CapabilityName
    status: TaskState
    name: NotRequired[str]
    title: NotRequired[str]
    progress: NotRequired[Duration]  # 0..100
    eta: NotRequired[str]  # hh:mm:ss


class Status(TypedDict):
    """`status.json`: written on every change and at least every heartbeat period (*Agent's status*, *Timers and thresholds*)."""

    id: AgentId
    status: StatusState
    datetime: IsoDateTime
    capability_status: list[CapabilityStatusEntry]
    msg: NotRequired[str]
    load: NotRequired[Duration]  # 0..100
    tasks: NotRequired[list[StatusTask]]
    contention: NotRequired[bool]


class Announce(TypedDict):
    """`nuntium/<id>.n.json` (and `nuntium/nuntium.json` for a broadcast): signal only (*Announcement and Discovery*)."""

    id: AgentId
    datetime: IsoDateTime
    ts: Ts
    nuntium: str


class ExchangeRecord(TypedDict):
    """The `event_data` of an `inreq` or `outreq` line: the envelopes, encrypted as they were (*Diarium*).

    `final_status` is `cancelled|completed|confirmed|dropped` for `inreq` and
    `confirmed|cancelled|abandoned|sent` for `outreq` — two `Literal`s, not one, because the sets differ.
    """

    final_status: str
    req: RequestFile | None
    can: CancellationFile | None
    res: ResponseFile | None
    con: ConfirmationFile | None


class DiariumEntry(TypedDict):
    """One line of `diarium.jsonl` (*Diarium*). `event_data` depends on `event`, and can be null."""

    id: AgentId
    datetime: IsoDateTime
    event: str
    event_data: AnyValue
