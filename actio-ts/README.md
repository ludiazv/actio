# actio-ts

A simple TypeScript implementation of the [Actio protocol](../doc/actio_protocol.md) v0.1 — the **core conformance profile**: `security_support: []`, `security_posture: "trusted"`, `signature: null`. No cryptography.

Actio is a file-based agent coordination protocol: a team of agents (*agmen*) shares one `.actio` directory. Each agent publishes `spec.json` and `status.json`, announces in `nuntium/`, and exchanges request/response envelopes (`<rid>.json`, `.can.json`, `.res.json`, `.con.json`) through its `inbox/`. Everything is JSON files on a watchable file system — no broker, no sockets.

## Install & requirements

- **Node.js ≥ 22** (npm). `npm ci` installs the dependencies (runtime: only `ajv`; dev: `typescript`, `json-schema-to-typescript`, `tsx`, `@types/node`).
- The schemas drive the code: `src/generated/` (schemas + interfaces) is **generated** from `../doc/schemas/` and is git-ignored — every build/test/typecheck runs `npm run gen` first.

```sh
npm ci          # install
npm run gen     # regenerate src/generated from ../doc/schemas (deterministic)
npm run check   # gen + tsc --noEmit + the full test suite
npm run blackbox # the independent black-box run (echo agent as a child process, envelopes by hand)
```

`test/blackbox/blackbox.mjs` imports no actio-ts code: it writes the protocol files by hand and validates every file the agent produces with its own Ajv built from `../doc/schemas/`. Run it after any change to protocol behavior.

## Quick start: two agents in one process

```ts
import { ActioAgent } from "./src/agent.js";
import { loadConfig } from "./src/config.js";

const root = "/tmp/my-team";          // or $ACTIO_DIR; default ~/.actio

const beta = new ActioAgent({
  id: "echo",
  description: "echoes text",
  acceptsCommandsFrom: ["alpha"],
  config: { root, heartbeatInterval: 200, confirmTimeout: 1_000 },
});
beta.capability(
  {
    name: "echo",
    description: "echoes the text it gets",
    parameters: { type: "object", properties: { text: { type: "string" } }, required: ["text"], additionalProperties: false },
    actio: { result: { type: "object", properties: { text: { type: "string" } }, required: ["text"] } },
  },
  async (params, ctx) => ({ text: (params as { text: string }).text }),
);
await beta.start();

const alpha = new ActioAgent({ id: "alpha", description: "demo requestor", acceptsCommandsFrom: [], config: { root } });
await alpha.start();
await alpha.agmen();                  // both agents discovered (alpha sees itself too)

const outcome = await alpha.request("echo", "echo", { text: "hello" });
// outcome.finalStatus === "confirmed"; outcome.response.result === { text: "hello" }

await alpha.stop();
await beta.stop();
```

Running examples live in [`examples/`](examples): `echo-agent.ts` (the §2.9 conformance target, driven by `ACTIO_AGENT_ID` / `ACTIO_COMMANDERS` / `ACTIO_NAMESPACES` + the config env vars) and `planner.ts` (a minimal requestor).

```sh
npm run echo        # tsx examples/echo-agent.ts
npm run actio -- ls # the CLI (see below)
```

## The directory layout an agent maintains

```
<root>/.actio                 (config.root; default ~/.actio, $ACTIO_DIR)
  actio.json                  optional team metadata (read-only for agents)
  nuntium/<agent_id>.n.json   announce signals (any create/update triggers a full scan)
  nuntium/nuntium.json        broadcast signal (special tools only — the CLI writes it)
  forum/                      attachments (paths relative to forum)
  <agent_id>/
    spec.json                 published capabilities (schema-validated)
    status.json               inactive|idle|busy|error + capability_status
    diarium.jsonl             the protocol log (append-only, schema-validated lines)
    inbox/<rid>…              the four envelope files of each exchange
```

Agents discover each other by scanning the root on every `nuntium` event (debounced) and apply the callability rules (namespaces, posture, disabled, `accepts_from`, capability `accepts_from`, status, health) to build the *Agmen* view — `agent.agmen()`.

## Configuration (`ActioConfig`)

All timers come from one `ActioConfig` (durations in **milliseconds** internally; the env overrides are in **seconds**, per the protocol's *Timers and thresholds*):

| Key | Default | Env override | Notes |
| --- | --- | --- | --- |
| `root` | `~/.actio` | `ACTIO_DIR` | the shared `.actio` root |
| `sync` | `none` | `ACTIO_SYNC` | the storage-sync provider (below) |
| `heartbeatInterval` | 30 000 ms | `ACTIO_HEARTBEAT` (s) | status rewritten ≥ every 90 s (rule 3) |
| `confirmTimeout` | 120 000 ms | `ACTIO_CONFIRM_TIMEOUT` (s) | the receiver archives on silence |
| `cancelWaitTimeout` | 60 000 ms | `ACTIO_CANCEL_WAIT` (s) | after a cancellation, wait for the response, then abandon |
| `announceDelay` | 100 ms | — | the rule-2 delay between the status write and the announce |
| `nuntiumDebounce` | 1 000 ms | — | nuntium events coalesce into one scan |
| `stopGrace` | 30 000 ms | — | `force=false`: in-flight work finishes first |
| `clock` | `Date.now` | — | injectable for tests |

A bad env value throws at `start()` — never at construction (D-21).

## Sync providers (`ACTIO_SYNC`)

The protocol's "signal the storage layer" primitive — pick with `ACTIO_SYNC`:

| Value | Behavior | Use when |
| --- | --- | --- |
| `none` (default) | no hooks | local file systems with kernel notifications |
| `delay[:<ms>]` (default 50) | a short pause after every write | the watchers need the events to land before the next step |
| `fsync+delay[:<ms>]` | fsync the file (and the parent folder) before returning | storages where data or renames may not be visible to other readers yet |
| `mutagen:<session>` | a debounced (200 ms) `mutagen sync flush <session>` via `execFile` | replicated setups (e.g. Docker ↔ host); the binary is `ACTIO_MUTAGEN_BIN` (default `mutagen`) |

`<ms>` is an integer in 1–10 000. An unknown value is a configuration error at `start()`. **One provider per process** (documented choice I-10): all agents of a team share one `.actio` root and therefore one storage behavior; the last-started agent installs it.

**Requirement:** the storage must deliver local change notifications (local disks, or a sync tool that writes locally). No polling fallback exists (`watch.ts` is event-driven only). Network file systems without notifications are unsupported.

## The CLI (`npm run actio -- …` or `npx tsx src/cli.ts`)

```
actio [--json] ls                                  agents, status, health, capabilities
actio diarium <agent> [-n N]                       the last N diarium lines
actio call <src-id> <dst> <cap> <json-params> [--timeout s (default 60)]
                                                   a one-shot requestor (lane B vehicle)
actio cmd <src-id> <dst> <name> [json-params]      a one-shot command (prints the rid)
actio broadcast "<text>"                           writes nuntium/nuntium.json (a special tool)
actio validate <file> [--as spec|status|announce|request|cancellation|response|confirmation|diarium|actio]
                                                   schema-validate a file (diarium: every line)
```

`--timeout` must be **> 0** seconds (default 60; `0`/negative/non-numeric is a usage error) — a CLI process has no heartbeat, so it never sends `timeout: 0`. `call` exits 1 only when no response arrived (`abandoned`); an error answer from the peer is an answer and exits 0. `cmd` reports only the rid: a peer-side `dropped` is the peer's decision and is not reported back. Exit codes: 0 ok, 1 usage/IO/validation failure.

## API notes

- `scan(root)` is the read-only picture of the root (every valid agent folder, the duplicate ids, the ignored files with a reason) for diagnostics and dashboards; it needs no started agent. A folder without both `spec.json` and `status.json` is skipped silently (discovery rule 2), so it is in neither `agents` nor `ignored`. Only `agmen()` says who is callable, and `request()` still refuses a peer outside the view. The agent's private `discovered` map stays private: it holds specs only, and exists for code 106. `health(status, nowMs)` rates a status at a moment (healthy|online|degraded|zombie; undefined for inactive/error).
- `readDiarium(folder)` reads `<folder>/diarium.jsonl` into `{ lines, ignored }`: the valid lines in file order, and the ignored lines (1-based) with a reason each ("not valid JSON" or the first Ajv error). A missing file gives the empty result; a last line without a trailing newline is skipped and not reported (an append may be in progress). The schema checks `event_data` for each of the five protocol events, so every line returned has passed that check; the value arrives as `unknown`, and the event names say what to cast it to: `Exchange` after `inreq`/`outreq`, `StatusState` after `status`, `string` after `log`. An event name of an agent's own is not checked. `agent.diarium()` is `readDiarium` on the agent's own folder; both reject before the first `start()`, and both work after `stop()`.
- `agent.log(message)` appends a `log` event to the agent's own diarium (validated; the sync provider applies it). It emits no `ActioEvent`; there is no API to append any other event name — the mandatory events belong to the library.
- The protocol types (`Spec`, `Status`, `Rid`, `Header`, `RequestBody`, `ResponseBody`, `ErrorResult`, `DiariumLine`, `Exchange`, …) are all exported from the entry, so a consumer never needs a deep import.
- `agent.refresh()` runs a full discovery scan now (the same `rescan()` a nuntium event schedules) and returns the new Agmen view; a stopped agent returns its current view without scanning.
- `checkParams(capability, params)` → the first Ajv error of the capability's `parameters` schema against `params`, as `"<instancePath or (root)> <message>"`; `undefined` when valid, or when the schema does not compile (D-19: the checks are skipped and the peer decides).
- `agent.request(dst, name, params, options?)` → `{ finalStatus, response? }`; rejects with `ActioCallError` (nothing written) when the callability rules fail, and with `ActioResultError` when a non-error result breaks the peer's published `result` schema (D-12).
- `agent.on("event", handler)` receives **every protocol event** on both lanes: `status`, `announce`, `inreq`, `outreq`, `ignored` (D-15/D-14), `log`.
- `agent.capability(cap, handler)` deep-copies the registration (a later mutation of the caller's object changes nothing) and compiles the schemas eagerly (D-19: a non-strict schema throws at registration).
- `agent.stop({ force })` follows D-9: graceful (default) lets in-flight work finish within `stopGrace` (handlers may still issue requests), then aborts + 300 + archives; `force=true` aborts at once. A stopped agent is `inactive` and announces (C1). `fail(msg)` is terminal (`error` + announce; no recovery).
- A `restart` command archives the command, runs `stop` (honouring `force`), waits `delay`, then starts fresh in-process (the registrations survive; a `disabled` set by `enable` does not).

## Implementation choices

Where the protocol leaves room, the implementation decided — each choice is observable in behavior:

| Id | Choice |
| --- | --- |
| D-1 | the agent folder name is always the agent ID |
| D-2 | file names: the recommended names; **reads match by `rid` + body key, never by file name**; only `*.json` in an inbox |
| D-3 | duplicate agent IDs: the first folder in lexical order wins; the rest are logged and ignored |
| D-4 | pending envelopes at startup are processed as if they had just arrived (requests → A0 incl. 108; commands → D0; bare responses re-tracked with the confirmation timer restarted) |
| D-5 | duplicate rids are ignored (an in-memory archived set per run) |
| D-6 | requestor timeout: `> 0` → cancel at expiry, wait up to `cancelWaitTimeout` for the response, then abandon; `0` → wait forever (or until `signal`) |
| D-7 | optional A0 checks implemented: 105 (parameters), 106 (namespaces — against the requestor's spec as **discovered** by the last scan, before the callability filter; an unknown requestor skips 106), 107 (reject, no queue), 108 (expired); **104 not implemented** |
| D-8 | busy iff ≥ 1 inbound exchange (A1/A2/A3) or ≥ 1 outbound in B2; written immediately on change; the heartbeat republishes with a fresh `datetime`; the diarium `status` event only on change; the announce file only on active↔non-active changes, `enable`, `ping`, steps 3/C1 |
| D-9 | stop order: heartbeat/scan off → grace (handlers may still issue requests) → `receiver.stop()` (abort + 300 + archive) → `requestor.stop()` (abandon) → C1 → C2; a stop during `start()` leaves nothing armed |
| D-10 | restart: archive → stop (honouring `force`) → wait `delay` → start fresh in-process; a `disabled` set by `enable` does not survive |
| D-11 | a `.can/.res/.con` envelope whose header differs from the tracked request (except `datetime`) is ignored and logged; the same check applies to the responses the requestor captures |
| D-12 | a non-error result that breaks the peer's `result` schema ends the exchange as `ActioResultError` (the exchange itself is already confirmed on both sides); cancellation responses (`result: null`) are exempt |
| D-13 | before writing the confirmation, the requestor checks its request file still exists; if not: `outreq` with the final status, no confirmation |
| D-14 | encrypted (base64) bodies received by a core agent: ignored + logged, never answered (the requestor treats a captured base64 response as received-and-confirmed with `response: undefined`) |
| D-15 | malformed files: ignored, `log` diarium event `ignored <path>: <first ajv error>` once per (path, mtime) |
| D-16 | no housekeeping beyond the protocol (no stale-file cleanup, no diarium rotation) |
| D-17 | any create/update in `nuntium/` schedules one debounced full scan |
| D-18 | one watcher per destination inbox, reference-counted by outstanding rids; released exactly once per exchange |
| D-19 | capability schemas are data: compiled at runtime (own registrations eagerly — a non-strict schema throws; peers lazily, cached per (id, spec datetime, capability), a failure logged once and the checks skipped) |
| D-20 | `src/generated/` is git-ignored, rebuilt by `npm run gen` before every build/test |
| D-21 | a bad `ACTIO_SYNC`/env value is a configuration error at `start()` — never at construction |
| D-22 | a watch error on the own inbox or `nuntium` is fatal: `log` (`watch lost <dir>: <code>`) + `fail()`; a destination watch error abandons the requestor's exchanges there (the agent stays up) |

From the review history: **I-1** the mutagen `onError` hook; **I-2** every written file is self-validated before writing; **I-3** `CapabilityEntry.compiled` (D-19 registration) and `ReceiverHooks.peerSpec` (I-11); **I-4** D-12 validated in `agent.request()`; **I-5** `announce()` takes two folders; **I-6** exactly 100 s → `degraded`; **I-7** a captured base64 (encrypted) response body is treated as received: the exchange is confirmed and `request()` resolves `confirmed` with `response: undefined`; **I-8** no `dst` check at A0/D0 (the inbox is the address); **I-9** `namespaces` admits `null` items; **I-10** one sync provider per process; **I-11** the `peerSpec` hook. Deferred: a shared protocol-version constant (the version string is written where it is needed).

## Limitations (core profile)

- **No security**: `security_support: []`, posture `trusted`, `signature: null`. Encrypted (base64) bodies received are ignored and logged, never answered (D-14). Codes 200–299 and the `issuer_sign` posture are out of scope. 104 (`version_not_supported`) is not implemented.
- **No queueing**: at `max_parallel` a request is rejected with 107, not queued.
- **Custom command handlers must not hang**: commands cannot be cancelled; a hung handler stalls lane D only.
- **One sync provider per process** (I-10).
- **The storage must deliver local change notifications**: local disks, or a sync tool writing locally (mutagen). No polling fallback.
- **A stopped agent's last status**: during lane C an inbound exchange is archived without waiting for the client's confirmation — its final status is `completed` or `confirmed` depending on whether the confirmation had arrived when `receiver.stop()` ran.
- **Hand-written envelopes** (test/debug tooling) only need to match the schema shapes; the rids need not be decodable ULIDs (nothing decodes them).

## Regenerating the schemas

`npm run gen` re-embeds `../doc/schemas/*.schema.json` into `src/generated/schemas.ts` (unmodified, keyed by `$id`, with a SHA-256 header) and regenerates `src/generated/types.ts` via json-schema-to-typescript with fixed interface names. The generator compiles every schema with Ajv `strict: true` first — a schema that fails strict mode fails the build. The output is deterministic (byte-identical across runs); `src/generated/` is never hand-edited.
