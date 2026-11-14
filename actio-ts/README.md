# actio-ts

A TypeScript client for the [Actio protocol](https://github.com/ludiazv/actio/blob/main/doc/actio_protocol.md)
v0.1, **core conformance profile**. Actio is file-based agent coordination: a team of agents shares one
`.actio` directory, each agent publishes what it can do (`spec.json`) and how it is doing (`status.json`),
and agents exchange JSON envelopes through each other's `inbox/`. There is no broker, no socket and no
server to run: a peer you cannot reach is a folder that has not been updated recently.

You get one library: `ActioAgent` (run an agent — receive, answer, publish, log), the calling side
(`request`, `command`), discovery (`agmen`, `scan`, `health`) and the protocol log reader
(`readDiarium`). How the package works inside is in [`internals.md`](internals.md).

## Before you use it

The protocol is public, and it is still a **draft**.

- **The wire format ships with this package.** The 11 JSON Schemas of the core profile are embedded
  verbatim, so installing it gives anyone everything needed to build an interoperable implementation — and
  published copies are permanent (the registry, mirrors, everyone's `node_modules`). Which exact revision
  is implemented is pinned by SHA-256 in [`CHANGELOG.md`](CHANGELOG.md).
- **There is no authentication.** The core profile is `security_posture: "trusted"` with `signature: null`:
  every envelope is unsigned and unauthenticated, and the cryptographic layer is *pending specification*
  in the protocol document. **Write access to the shared `.actio` root is peer authority** — anyone who can
  write there is a peer as far as the protocol is concerned. Do not put untrusted agents in one root, and do
  not share a root with a process that can be influenced by untrusted input.
- **Capability descriptions are untrusted text.** They exist to be read by an LLM planner; in this profile
  they carry no signature and no isolation.
- **Schemas are shapes, not the specification.** Ownership, lifecycle semantics and the callability rules
  are prose in the protocol document. This package implements them; [`CONFORMANCE.md`](CONFORMANCE.md)
  lists every protocol requirement with the code that satisfies it and the test that proves it.

## Install

```sh
npm install actio-ts
```

- **Node.js ≥ 22**, ESM only (`"type": "module"`). TypeScript declarations ship in the package. One runtime
  dependency.
- The agent needs **write access to the whole `.actio` root**, not only its own folder: writing into a
  peer's `inbox/` *is* how you call it.
- The storage must deliver **local change notifications** (a local disk, or a sync tool that writes
  locally). There is no polling fallback — see [Storage sync](#storage-sync).
- Every protocol type (`Spec`, `Status`, `Rid`, `RequestBody`, `ResponseBody`, `Exchange`, `DiariumLine`, …)
  is exported from the package entry: you never import from a path inside it.

## Two agents, one root

```ts
import { ActioAgent } from "actio-ts";

const root = "/shared/actio";   // or $ACTIO_DIR; default ~/.actio

const echo = new ActioAgent({
  id: "echo",                   // also its folder name, always
  description: "echoes text",
  acceptsCommandsFrom: ["alpha"],
  config: { root },
});

echo.capability(
  {
    name: "echo",
    description: "echoes the text it gets",
    parameters: { type: "object", properties: { text: { type: "string" } }, required: ["text"], additionalProperties: false },
    actio: { result: { type: "object", properties: { text: { type: "string" } }, required: ["text"] } },
  },
  async (params) => ({ text: (params as { text: string }).text }),
);
await echo.start();             // folders, spec.json, status.json, announce

const alpha = new ActioAgent({ id: "alpha", description: "requestor", acceptsCommandsFrom: [], config: { root } });
await alpha.start();
await alpha.agmen();            // both agents discovered (an agent sees itself too)

const outcome = await alpha.request("echo", "echo", { text: "hello" });
outcome.finalStatus;            // "confirmed"
outcome.response?.result;       // { text: "hello" }

await alpha.stop();
await echo.stop();
```

Two runnable examples are in the repository:
[`examples/echo-agent.ts`](https://github.com/ludiazv/actio/blob/main/actio-ts/examples/echo-agent.ts) (a
standalone agent process, the conformance target) and
[`examples/planner.ts`](https://github.com/ludiazv/actio/blob/main/actio-ts/examples/planner.ts) (a minimal
caller).

## Publishing a capability

`agent.capability(spec, handler)` registers what the agent can do. `parameters` and `result` are JSON
Schemas, and they are used on both sides:

- **at registration** they are compiled immediately — a schema Ajv rejects throws there, before the agent
  starts, not on the first call;
- **on the receiver**, incoming `parameters` are checked against them first (a request that does not fit is
  answered with an error, code 105, and the handler never runs);
- **on the caller**, the response `result` is checked against the schema the *receiver* published; a
  mismatch throws `ActioResultError` (`finalStatus` is still what the exchange said — the exchange
  completed, the contract did not).

The handler receives `(params, ctx)`, where `ctx` carries `rid`, `src`, an `AbortSignal` (a cancellation, or
your own `stop()`), the request's `attachments` as `forum`-relative paths, and
`ctx.publishAttachment(name, data)` to return a file. Return the result, or throw `ActioError(code, msg)`
for a **capability** error (`code ≥ 1000`); the peer receives it as an error response (`error: true`, the
`{ code, msg, details }` in `result`) and the exchange still confirms. Names must be unique per agent, and
they are also the names peers call you by.

To check a payload against a capability you discovered **before** calling it, use
`checkParams(capability, params)`: the same check the receiver runs, returning one error string or
`undefined`.

## Calling

```ts
const outcome = await alpha.request("echo", "echo", { text: "hello" }, {
  timeout: 60,          // seconds; 0 waits forever (until the peer answers or `signal` aborts)
  attachments: ["/tmp/report.pdf"],   // copied into `forum` under their basename
  signal: controller.signal,          // cancels: writes the cancellation envelope
});
```

`outcome.finalStatus` is `"confirmed"`, `"cancelled"` or `"abandoned"`; `response` is present whenever a
response was received and confirmed. A peer's failure is not an exception: it arrives as a response with
`error: true` and the `{ code?, msg, details? }` payload in `result` — an error **answer** is still an
answer. Two things throw instead:

- **`ActioCallError`** — the callability rules refused locally and *nothing was written*: unknown peer,
  outside your namespaces, peer disabled or not accepting you, `error`/`zombie` status…
- **`ActioResultError`** — the answer arrived but breaks the peer's published `result` schema.

`agent.command(dst, name, params?)` sends a command instead of a request and resolves with the envelope's
`rid`: commands are never answered and cannot be cancelled. The four built-ins are `ping` (republish),
`enable` (`{ enabled }`, which sets or clears `disabled` in `spec.json`), `stop` (`{ force }`) and `restart`
(`{ delay, force }`); a peer you do not list in `acceptsCommandsFrom` cannot send you any of them.

## Who can I talk to

- **`agent.agmen()`** — the only honest answer: peers that survived the callability rules (namespaces,
  posture, `disabled`, `accepts_from`, status, health), each with its `spec`, `status`, `health`, the
  capabilities it lets you use, and whether it accepts your commands. `request()` refuses anything outside
  this view. `agent.refresh()` runs a scan now; a `nuntium/` event schedules one (debounced) anyway.
- **`scan(root)`** — the read-only picture of a root for diagnostics and dashboards, with no agent started:
  every valid agent folder, duplicate ids, and the ignored files with a reason each. A folder without both
  `spec.json` and `status.json` is skipped silently, so it appears in neither list.
- **`health(status, nowMs)`** — rates a status at a moment: `healthy | online | degraded | zombie`
  (undefined for `inactive` and `error`).
- **`agent.on("event", handler)`** — every protocol event on both sides: `status`, `announce`, `inreq`,
  `outreq`, `ignored`, `log`.

## The log (diarium)

Every agent appends to its own `diarium.jsonl`: requests received, requests sent, status changes, ignored
files, `log` lines. `await agent.diarium()` (or `readDiarium(folder)` for any agent folder) returns
`{ lines, ignored }` — the valid lines in file order, and the ignored ones (1-based) with a reason each. A
missing file yields an empty result; a last line without a trailing newline is skipped, not reported (an
append may be in progress). `event_data` arrives as `unknown`, and the event name says what to cast it to:
`Exchange` after `inreq`/`outreq`, `StatusState` after `status`, `string` after `log`.
`await agent.log("…")` appends your own `log` line.

## The directory an agent maintains

```
<root>/                     (config.root; default ~/.actio, or $ACTIO_DIR)
  actio.json                optional team metadata, read-only for agents
  nuntium/<agent_id>.n.json announce signals (any write triggers a scan)
  nuntium/nuntium.json      broadcast signal, written by special tools only
  forum/                    attachments, addressed by paths relative to it
  <agent_id>/
    spec.json               published capabilities (schema-validated)
    status.json             inactive | idle | busy | error + per-capability state
    diarium.jsonl           the protocol log (append-only, schema-validated lines)
    inbox/<rid>…            the four envelope files of each exchange
```

Agents find each other by scanning the root whenever `nuntium/` changes, and apply the callability rules to
that view.

## Configuration (`config`)

All timers are per-agent, in **milliseconds** (the `ACTIO_*` env overrides are in **seconds**, per the
protocol's *Timers and thresholds*):

| Key | Default | Env override | Notes |
| --- | --- | --- | --- |
| `root` | `~/.actio` | `ACTIO_DIR` | the shared root |
| `sync` | `none` | `ACTIO_SYNC` | [storage sync](#storage-sync) |
| `heartbeatInterval` | 30 000 | `ACTIO_HEARTBEAT` | how often `status.json` is republished (the protocol requires at least every 90 s) |
| `confirmTimeout` | 120 000 | `ACTIO_CONFIRM_TIMEOUT` | the receiver archives a response on confirmation silence |
| `cancelWaitTimeout` | 60 000 | `ACTIO_CANCEL_WAIT` | after cancelling: wait for the response, then abandon |
| `announceDelay` | 100 | — | between the status write and the announce |
| `nuntiumDebounce` | 1 000 | — | `nuntium` events coalesce into one scan |
| `stopGrace` | 30 000 | — | `stop()` lets in-flight work finish first |
| `clock` | `Date.now` | — | injectable, for tests |

A bad value throws at `start()`, never at construction — so a misconfigured agent fails loudly instead of
registering half a team.

Other `ActioAgent` options: `namespaces` — the namespaces this agent **belongs to**: a peer can reach you
only if you share one, so absent, `null` and `[]` all mean the void namespace, `null` listed explicitly adds
it to a filled list, and `["*"]` is every namespace. Then `depends`, `wants`, and `info` (free metadata in
`spec.json`). Per capability, `actio.disabled` and `actio.accepts_from` narrow who may use it.

## Storage sync

The protocol asks an agent to "signal the storage layer" after a write. Pick with `ACTIO_SYNC` (or
`config.sync`):

| Value | Behavior | Use when |
| --- | --- | --- |
| `none` (default) | no hooks | a local file system with kernel change notifications |
| `delay[:<ms>]` (default 50) | a short pause after each write | watchers need events to land before the next step |
| `fsync+delay[:<ms>]` | fsync the file, and its folder after the rename | storage where data or renames may not be visible to other readers yet |
| `mutagen:<session>` | a debounced `mutagen sync flush <session>` | replicated setups, e.g. Docker ↔ host (`ACTIO_MUTAGEN_BIN` sets the binary) |

`<ms>` is 1–10 000. **One provider per process**: every agent in a process shares one `.actio` root and
therefore one storage behavior.

## Stopping

`stop()` is graceful: in-flight work gets `stopGrace` to finish (handlers may still issue calls), then the
remaining inbound exchanges are answered with code 300 and archived, outbound ones are abandoned, and the
agent publishes `inactive` and announces. `stop({ force: true })` skips only the grace wait, not the order.
`fail(message)` is terminal (`error` + announce). A stopped agent keeps its `diarium.jsonl` readable.

## Not in this version

- **No security at all**, by profile: no keys, no signing, no `issuer_sign`, no codes 200–299. An encrypted
  (base64) body received is ignored and logged, never answered.
- Code **104** (`version_not_supported`) is not implemented; **105–108** are.
- **No queueing**: at `max_parallel` a request is rejected (107), not held.
- **No CLI binary**: the package exports the library only.

## Conformance, testing, license

- [`CONFORMANCE.md`](CONFORMANCE.md): every MUST row of the protocol, with the code and the test.
- [`CHANGELOG.md`](CHANGELOG.md): what each release changed, and the schema digests of the protocol
  revision it implements.
- [`internals.md`](internals.md): how the package works, the decisions behind it, and how to run the tests.
- **License**: BSD 3-Clause — see `LICENSE`. It covers the code **and** the schemas embedded verbatim in it,
  which are part of the published artifact. Two gaps worth knowing for a protocol rather than a library:
  BSD-3-Clause has **no explicit patent grant**, and its third clause reserves nothing about the name
  **Actio** itself.
