# actio-ts — how it works inside

For anyone reading or changing this package's code: the module map, the lifecycle, the protocol machinery,
the decisions behind it, and how to run the tests and the release. If you are **using** the package, read
[`README.md`](README.md) instead; if you are checking it against the protocol, read
[`CONFORMANCE.md`](CONFORMANCE.md), which maps every MUST row to its code and its test.

## 1. The shape of the code

Sixteen source modules and two generated ones, about 2 700 lines outside `src/generated/`, and 21 test files.
Every statement below names the file it comes from: the code and its tests are the specification, and this
document is the map to them.

```
src/index.ts          the package entry: every public type and function, no deep imports needed
src/agent.ts          ActioAgent: lifecycle steps 1–6, lane C wiring, commands, the public API
src/receiver.ts       lanes A (requests) and D (commands): inbox dispatch, per-rid tracker, acceptance
src/requestor.ts      lane B: write, watch the destination inbox, cancellation, confirmation, abandon
src/status.ts         self-publication: spec.json, status.json, the announce file, the heartbeat, health
src/discovery.ts      the folder scan, the callability rules 1–8, the Agmen view
src/watch.ts          one event-driven folder watcher: fs.watch events trigger a folder scan
src/io.ts             atomic JSON writes, JSONL appends, small fs helpers, the sync write hooks
src/sync.ts           the sync providers: none, delay, fsync+delay, mutagen
src/diarium.ts        append + typed event builders + the read-only view of a diarium file
src/validate.ts       Ajv validators over the embedded schemas, the rules schemas cannot express
src/spec-builder.ts   options + registrations → a valid spec.json
src/config.ts         defaults, env reading, the injectable clock
src/types.ts          the API's own types: options, views, events, the three error classes
src/util.ts           rid/nonce/datetime helpers
src/cli.ts            the diagnostic CLI (not published as a `bin` — see §15)
src/generated/        the embedded schemas and the generated interfaces (§2)
```

Dependencies: **`ajv`** at runtime, and nothing else. `typescript`, `tsx`, `json-schema-to-typescript` and
`@types/node` are development only.

## 2. The schemas drive the code

`scripts/gen-schemas.ts` reads the protocol schemas from **`../doc/schemas/`** (outside this package) and
writes two files into `src/generated/`:

- **`schemas.ts`** — every schema embedded **verbatim**, keyed by `$id`, under a header carrying the
  SHA-256 of each source file. That file is the runtime validator input: nothing re-implements a schema by
  hand, so a bug in this package cannot make it "conform" to a private reading of the protocol.
- **`types.ts`** — the TypeScript interfaces, via json-schema-to-typescript with fixed names.

Before writing anything, the generator compiles every schema with **Ajv `strict: true`** — 33 schemas and
`$defs` — so a schema that Ajv's strict mode rejects breaks the build instead of breaking an agent at
runtime. The output is deterministic (byte-identical across runs).

`src/generated/` is **committed**, and `npm run gen:check` byte-compares it against a fresh generation
without writing (`npm run gen` is the explicit "I intend to regenerate"). It used to be git-ignored and
rebuilt by every build (decision **D-20**, now superseded): a published package cannot do that, because the
generator reads a directory outside the package and needs devDependencies that installs skip. `gen:check`
runs from `check`, `test`, `typecheck`, `build`'s gate and the tarball smoke — so the published schemas are
provably the current ones, and drift fails a build rather than shipping.

## 3. Start-up, in protocol order

`start()` resolves the config first — a bad `ACTIO_SYNC` or env value throws there, before the agent exists
in the tree (D-21) — and installs the process-wide sync provider (I-10: one per process, the last started
agent wins). Then the start sequence, in protocol order (the code comments carry the protocol's step
numbers):

1. `makeDir` for root, `nuntium/`, `forum/`, the agent folder, `inbox/`;
2. `spec.json` — built from the registrations and self-validated (`publishSpec`). A bad capability
   registration fails **here**, before anything lands in the tree, and `abortStart()` unwinds so a failed
   `start()` leaves nothing armed;
3. `status.json` → `idle`, plus its diarium `status` event;
4. the announce, `announceDelay` after the status write;
5. the **nuntium watcher** (every create/update schedules one debounced full scan, D-17), followed
   immediately by the first `rescan()` — the workflow's first discovery runs now, not after the debounce;
6. the heartbeat, and **last** the inbox watcher, whose initial scan delivers the pending envelopes of a
   previous run (D-4: requests → A0 including 108, commands → D0, bare responses re-tracked with the
   confirmation wait restarted). It is last on purpose: a pending `stop`/`restart` command then runs while
   everything is armed, so the teardown it triggers is complete.

Self-calls work because the receiver's inbox fan-out feeds the requestor: an agent calling itself watches
its own inbox too (B2).

## 4. Stop, restart, fail

`stop()` (D-9) is idempotent and ordered:

1. heartbeat and any pending scan off;
2. the grace wait — only for in-flight **work** (`stopGrace`, polled), and the requestor is deliberately
   **not** stopped yet, so a handler may still issue a request and receive its answer;
3. `receiver.stop()`: every accepted, unanswered request is answered with code **300**, and exchanges that
   already have a response are archived;
4. `requestor.stop()`: whatever is still in B2 is abandoned (`outreq` `abandoned`);
5. **C1**: `status.json` → `inactive`, then the announce (delayed as always), and `sync.flush()` before C1
   completes, so the last writes reach the shared tree. **C2** closes the watchers and drops the in-memory
   view and bookkeeping.

`force: true` skips the grace wait and nothing else. `restart` (D-10) archives the command, stops honouring
`force`, waits `delay` seconds, then starts fresh **in process**: the registrations survive, a `disabled`
set by `enable` does not. `fail(message)` is terminal: `error` + announce, no recovery path.

## 5. The lanes

**Lane A — inbound requests** (`receiver.ts`). The inbox scan classifies each `*.json` by **`rid` + body
key, never by file name** (D-2), tracks the exchange *before* A0 starts (so a concurrent cancellation
matches it), and runs A0 asynchronously so acceptance never blocks the dispatcher. `A0` is the protocol's
check table **in order**, answering with the first failure found: 100 (unknown capability), 101
(`disabled`), 102/103 (callability), 105 (parameters), 106 (namespaces — against the requestor's spec **as
discovered**, before the callability filter; an unknown requestor skips 106), 107 (at `max_parallel`:
reject, no queue), 108 (expired). **104 is not implemented.** Then A1 runs the handler with an
`AbortSignal`, A2 writes the response and waits for the confirmation under `confirmTimeout`, A3 handles a
cancellation (abort the work, answer the cancellation), A4 archives: the `inreq` line first, then the
envelope removals.

**Lane B — outbound** (`requestor.ts`). B1 copies attachments into `forum` first, then writes the request
(self-validated, I-2). B2 watches the destination inbox (D-18: one watcher per destination, reference
counted by outstanding rids, released exactly once per exchange) and reacts to a `<rid>.res.json` event;
the header of anything captured is compared to the request (D-11) and a mismatch is ignored and logged. B3
writes the confirmation, but only after checking that the request file still exists (D-13 — if the peer
archived us, the `outreq` line records the final status and no confirmation is written). B4 writes the
cancellation and waits `cancelWaitTimeout` for the cancellation response before abandoning (D-6;
`timeout: 0` waits forever, or until `signal`). B5 deletes the attachments it published.

**Lane D — commands.** `D0` decides synchronously whether this `src` may command us and whether the name is
known; a command name is never advertised. Accepted commands go to a queue drained **one at a time in rid
order**, so a slow custom handler only delays the commands behind it — lanes A and B keep running, but a
handler that never returns stalls lane D forever, because commands cannot be cancelled. The four built-ins (`stop`, `enable`,
`restart`, `ping`) archive **first** (D3) and then hand over to the agent lifecycle — which is what lets
`stop` tear down the agent that is executing it. `D4` is the drop path: only the envelope and a `log` line,
because a rejected command is the receiver's decision and is never answered. Commands are never cancelled
and never answered.

`busy` (D-8) is ≥ 1 inbound exchange in A1/A2/A3 or ≥ 1 outbound in B2, written immediately on change; the
heartbeat republishes it with a fresh `datetime`, the diarium `status` event fires only on change, and the
announce file only on active↔non-active changes plus `enable`, `ping` and the start/stop steps.

## 6. Files: how the package writes and reads

- **Writes are atomic and cross-folder.** `writeJsonAtomic` writes `.<name>.<nonce>.tmp` in the **writer's
  own agent folder**, fsyncs if the sync provider says so, then `rename`s the file into the peer's `inbox`.
  Two consequences, both protocol facts: the root must be **one file system**, and an agent needs write
  access to the whole root, not only its own folder.
- **Every protocol file is self-validated before it is written** (I-2): a file this package writes cannot be
  the reason a peer ignores it.
- **Appends** go through `appendJsonl`; the diarium validates its own line before appending (a bad line is
  a bug in this package, so it throws rather than corrupting the log).
- **Malformed input is survivable**: a file that fails validation is ignored, and the receiver logs
  `ignored <path>: <first ajv error>` once per `(path, mtime)` (D-15). Duplicate agent ids: the first folder
  in lexical order wins, the rest are logged and ignored (D-3). Duplicate rids are ignored (D-5).
- **Sync hooks** run after every write (delay, optional fsync of the file and the folder, mutagen flush).

## 7. Discovery and callability

`scan(root)` walks the root: the reserved names (`tools`, `forum`, `nuntium`) are never agent folders; a
folder must contain both `spec.json` and `status.json`, else it is skipped silently (discovery rule 2), so
it appears in neither `agents` nor `ignored`; both files are schema-validated, capability names must be
unique, and anything invalid is reported with a reason. `agent.agmen()` is that picture after the callability
rules 1–8 (namespaces, posture, `disabled`, `accepts_from`, per-capability `accepts_from`, status, health);
`request()` refuses anything outside it with `ActioCallError`, so a call never half-writes an envelope.
`agent.refresh()` runs the same scan `rescan()` does; a stopped agent returns its current view without
scanning. The agent's private `discovered` map holds specs only — it exists so the 106 check can see a peer
that the filter excluded.

Health is derived from the age of a peer's `datetime` (`status.ts`): exactly 100 s → `degraded` (I-6), then
`zombie`; `inactive` and `error` have no health.

## 8. Watches

`watch.ts` is deliberately minimal: an `fs.watch` event triggers a folder scan; there are no timers, no
polling and no re-arming — the watcher never decides for itself what to look at. That is why the storage
must deliver local change notifications, and why no network file system without them is supported. Failure
is asymmetric (D-22): losing the watch on the **own** inbox or on `nuntium` is fatal (`log` + `fail()`),
because a half-listening agent is worse than a dead one; losing a **destination** inbox abandons the
outbound exchanges there and the agent stays up.

## 9. Validation and the capability schemas

Two Ajv instances, both `{ strict: true, allErrors: false }`: the protocol one (the embedded schemas, lazy
singleton) and a second for **capability** schemas, so a peer's `$id`s can never collide with the protocol's.
Own registrations are compiled eagerly at `capability()` — a non-strict schema throws there, as does a
duplicate name — and the registration is `structuredClone`d, so mutating the caller's object afterwards
changes nothing: the published `spec.json` is a snapshot. A peer's
capability schemas are compiled lazily and cached per `(agent id, spec datetime, capability)`; a schema that
will not compile is logged once and its checks are **skipped** (D-19: the peer's published schema is data,
and a broken one is the peer's problem, not a reason to refuse the call). `checkParams(capability, params)`
is the same check exposed to callers, formatted as `"<instancePath or (root)> <message>"`.

The rules the schemas cannot express live in `validate.ts`: capability names unique per agent, announce
agent-id equals its writer, `rid` plus body key identify an envelope rather than its file name, and
`sameHeader` (D-11) compares an inbound envelope to the request it belongs to (`datetime` excepted).

## 10. Configuration

`config.ts` merges defaults, the `ACTIO_*` env overrides (**seconds**, per the protocol) and the explicit
`config` object (**milliseconds**), and validates the result; `clock` is injectable, which is how the tests
control time. Env names: `ACTIO_DIR`, `ACTIO_SYNC`, `ACTIO_HEARTBEAT`, `ACTIO_CONFIRM_TIMEOUT`,
`ACTIO_CANCEL_WAIT`, and `ACTIO_MUTAGEN_BIN` for the mutagen provider. Parsing of `ACTIO_SYNC` happens at
`start()` (D-21).

## 11. Sync providers

`sync.ts` implements exactly four: `none`, `delay[:ms]`, `fsync+delay[:ms]`, `mutagen:<session>` (a debounced
200 ms `mutagen sync flush`, 200 ms coalescing, `onError` surfaced as a `log` event — I-1). The provider is
process-wide (`io.setSyncProvider`), because one process shares one `.actio` root and therefore one storage
behavior (I-10).

## 12. Decisions and implementation rules

Where the protocol leaves room, this implementation decided; each choice is observable in behavior.
[`CONFORMANCE.md`](CONFORMANCE.md) maps them to the protocol rows.

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
| D-11 | a `.can`/`.res`/`.con` envelope whose header differs from the tracked request (except `datetime`) is ignored and logged; the same check applies to the responses the requestor captures |
| D-12 | a non-error result that breaks the peer's `result` schema ends the exchange as `ActioResultError` (the exchange itself is already confirmed on both sides); cancellation responses (`result: null`) are exempt |
| D-13 | before writing the confirmation, the requestor checks its request file still exists; if not: `outreq` with the final status, no confirmation |
| D-14 | encrypted (base64) bodies received by a core agent: ignored + logged, never answered (the requestor treats a captured base64 response as received-and-confirmed with `response: undefined`) |
| D-15 | malformed files: ignored, `log` diarium event `ignored <path>: <first ajv error>` once per (path, mtime) |
| D-16 | no housekeeping beyond the protocol (no stale-file cleanup, no diarium rotation) |
| D-17 | any create/update in `nuntium/` schedules one debounced full scan |
| D-18 | one watcher per destination inbox, reference-counted by outstanding rids; released exactly once per exchange |
| D-19 | capability schemas are data: compiled at runtime (own registrations eagerly — a non-strict schema throws; peers lazily, cached per (id, spec datetime, capability), a failure logged once and the checks skipped) |
| D-20 | *(superseded 2026-11-14)* `src/generated/` was git-ignored and rebuilt by `npm run gen` before every build/test. It is now **committed** and gated by `gen:check`: a published package cannot regenerate it (§2) |
| D-21 | a bad `ACTIO_SYNC`/env value is a configuration error at `start()` — never at construction |
| D-22 | a watch error on the own inbox or `nuntium` is fatal: `log` (`watch lost <dir>: <code>`) + `fail()`; a destination watch error abandons the requestor's exchanges there (the agent stays up) |

Implementation rules the code adds: **I-1** the mutagen `onError` hook; **I-2** every written file is
self-validated before writing; **I-3** `CapabilityEntry.compiled` (D-19 registration) and
`ReceiverHooks.peerSpec` (I-11); **I-4** D-12 validated in `agent.request()`; **I-5** `announce()` takes two
folders; **I-6** exactly 100 s → `degraded`; **I-7** a captured base64 (encrypted) response body is treated
as received: the exchange is confirmed and `request()` resolves `confirmed` with `response: undefined`;
**I-8** no `dst` check at A0/D0 (the inbox is the address); **I-9** `namespaces` admits `null` items;
**I-10** one sync provider per process; **I-11** the `peerSpec` hook. Deferred: a shared protocol-version
constant (the version string is written where it is needed).

Mechanical limits that follow from the above: no housekeeping of any kind (D-16); no polling fallback for
change notifications; one sync provider per process; during lane C an inbound exchange is archived without
waiting for the client's confirmation, so its final status is `completed` or `confirmed` depending on whether
the confirmation had arrived when `receiver.stop()` ran; hand-written envelopes (test/debug tooling) only
need to match the schema shapes — nothing decodes a `rid` as a ULID.

## 13. Tests

```
test/unit/      301 tests: every module, with a fake clock and a temp root; the CLI included
test/vectors/   111 protocol samples under <schema>/{valid,invalid}, one test() per schema folder —
                an unknown or missing folder fails the suite, so a new schema cannot slip in untested
test/e2e/        29 tests: multi-agent scenarios E1–E21 in one process, plus a child-process run
test/blackbox/  an independent run: 19 scenarios
```

```sh
npm ci
npm run check      # gen:check + tsc --noEmit + unit and e2e
npm run blackbox   # the black-box run
```

The black-box run is the one to trust for protocol behaviour: `test/blackbox/blackbox.mjs` **imports no
actio-ts code**, runs the example echo agent as a child process, writes the envelopes by hand, and validates
every file the agent produces with its own Ajv built from the schema sources. It takes an optional argument —
`npm run blackbox -- <path to another checkout>` — which is how it doubles as an interop probe against a
different implementation.

Two environment facts, because they cost time if you do not know them: the suite needs **real Node**, and it
spawns `process.execPath --import tsx …`, so a Bun `node` shim cannot run it (it also fails on `readdir`
ordering, which POSIX does not guarantee). In this monorepo the dev helper that pins the Node binary and the
dev `ACTIO_DIR` is `orchestra/env.sh` (gitignored, developer-box specific) — `bash orchestra/env-check.sh`
prints what it resolved. On CI the Node version comes from the workflow instead.

## 14. Packaging and publishing

- **Entry**: the compiled `dist/` (`exports` → `dist/index.js`, `types` → `dist/index.d.ts`). `build` starts
  with `rm -rf dist`, because a stale artifact left by a config change rides straight into a tarball. No
  source maps: the tarball carries code, types and source, not three copies of each. `src/` ships as source.
- **`files` is an allowlist** (`dist`, `src`, README, internals, CONFORMANCE, CHANGELOG, LICENSE). Without
  one, npm publishes anything not gitignored — the test tree and its protocol vectors, scripts, examples, and
  any scratch document.
- **The gate** is `npm run check:publish`: `gen:check` → typecheck → the 330 tests → the black-box run →
  `build` → `scripts/pack-smoke.mjs`. The smoke packs the tarball, checks the file list and that the manifest
  has no local dependency specifier, verifies that the schemas **embedded in the artifact** still match the
  sources by SHA-256, installs it into a temp directory with `--omit=dev`, and drives a real agent from
  **plain Node** — no tsx, no jiti, nothing from this repository on the import path. It is the check that
  catches "works in the repo, broken when installed".
- **One trap inside that gate**: a nested `npm` call inherits the parent's `npm_config_*`. Under
  `npm publish`, `prepublishOnly` → gate → `pack-smoke` would otherwise run `npm pack` in dry-run mode
  (a tarball name printed, no file written) and `npm install` would install nothing. `pack-smoke` therefore
  drives its children with a clean environment.
- **Publishing is a tag, not a command**: push `actio-ts/v<version>`, and
  `.github/workflows/publish-actio-ts.yml` runs the gate on Node 22 **and** 24 and publishes with
  provenance. The namespace matters — in this monorepo a repo-wide `v*` would imply a release of the
  protocol documents and the other implementations too; `protocol/v0.1` is the tag for the schemas. The
  workflow refuses if the tag does not match `package.json`, and it checks out the whole monorepo because
  `gen:check` reads `../doc/schemas`.
- **Choices behind this shape**: the entry is *compiled* rather than published as TypeScript — the Pi loader
  (jiti) and `tsx` would take a `.ts` entry happily, plain `node` would not, and a library should not assume
  the consumer runs one of our two loaders. The name is the bare `actio-ts` (unclaimed on the registry; the
  bare `actio` is not, so the CLI can never take it either — §15). The license is BSD-3-Clause, holder
  `ldiaz`, and it covers the embedded schemas, which is why they are part of the artifact and not a pointer.
- **How the credential works, and why it changes in January 2027.** npm revoked classic tokens (2025-12-09)
  and **removes direct publishing with bypass-2FA granular tokens in January 2027**: a long-lived
  `NPM_TOKEN` is a countdown, not a setup. The workflow therefore has both paths, and the migration is
  "delete the secret": with `NPM_TOKEN` set it publishes with a granular **Read and write (publish and
  stage)** token and an explicit `--provenance`; with the secret gone it publishes through **trusted
  publishing** — OIDC from a GitHub-hosted runner, `id-token: write`, no credential stored in the repository,
  provenance generated automatically. Configure it on the package page (Publishing from CI/CD → GitHub
  Actions: user `ludiazv`, repository `actio`, workflow filename `publish-actio-ts.yml`) and tick **`npm
  publish`** under Allowed actions — a configuration created after 2026-09-03 allows only `npm stage publish`
  until you say otherwise. Three constraints that bite: the package must **exist** before it can have a
  trusted publisher (so the first version still goes out on the token path), the connection is **immutable**
  (the workflow filename is a commitment — to change it, delete and recreate it), and a fresh configuration
  must complete a first successful publish **within 2 days** or it expires. Requirements: a **public**
  repository, **GitHub-hosted** runners (npm accepts no OIDC from self-hosted ones yet), npm CLI ≥ 11.5.1 and
  Node ≥ 22.14.
- **The other migration path, if OIDC is blocked** — a self-hosted runner, or simply wanting a human in the
  loop: a **stage-only** token and `npm stage publish` in CI; a maintainer then reviews with
  `npm stage list` / `npm stage view <id>` / `npm stage download <id>` and promotes with
  `npm stage approve <id> --otp <code>` or discards with `npm stage reject <id>`. A stage-only token that
  tries `npm publish` fails with `E_STAGE_REQUIRED`; it can still move dist-tags and deprecate versions, so it
  narrows the risk rather than bounding it. Related: since August 2026 no token can perform account-identity
  actions (create tokens, change 2FA, add maintainers) — those require an interactive 2FA challenge.
- `private` is already `false`, and `prepublishOnly` re-runs the gate for a hand-run `npm publish` too, so
  that path is possible but is not the way releases happen.
- **Cutting a release**: bump `version`, add a `## <version>` entry to `CHANGELOG.md` — with the schema
  digests if `doc/schemas` moved, since `gen:check` fails the gate if the two drift apart — then:

  ```sh
  ver=$(node -p "require('./package.json').version")
  git tag -a "actio-ts/v$ver" -m "actio-ts $ver" && git push origin "actio-ts/v$ver"
  ```

- **A release is permanent.** `npm unpublish` works inside 72 hours and only while nothing depends on the
  version; the way out of a bad release is `npm deprecate actio-ts@0.1.0 "… use 0.1.1"` followed by a patch.
  And while `0.x`, remember what consumers can pin: `^0.1.0` admits **only** `0.1.x`, so a breaking fix in the
  0.x line means telling people to re-pin.
- **After the first publish**, `pi-actio` switches from its `"actio-ts": "file:../actio-ts"` link to the
  published version — pinned exactly while 0.x — and its `npm run check` and `npm run smoke` re-run against
  the registry artifact rather than the workspace symlink.

## 15. The CLI

`src/cli.ts` is the diagnostic CLI, **not published as a `bin`** (an open decision, and the bare name `actio`
is taken on the registry — if it ever ships, it ships as `actio-ts`). From a checkout: `npm run actio -- …`.

```
actio [--json] ls                                  agents, status, health, capabilities
actio diarium <agent> [-n N]                       the last N diarium lines
actio call <src-id> <dst> <cap> <json-params> [--timeout s (default 60)]
                                                   a one-shot requestor (a lane B vehicle)
actio cmd <src-id> <dst> <name> [json-params]      a one-shot command (prints the rid)
actio broadcast "<text>"                           writes nuntium/nuntium.json (a special tool)
actio validate <file> [--as spec|status|announce|request|cancellation|response|confirmation|diarium|actio]
                                                   schema-validate a file (diarium: every line)
```

`--timeout` must be **> 0** seconds: a CLI process has no heartbeat, so it never sends `timeout: 0`. `call`
exits 1 only when no response arrived (`abandoned`); an error answer from the peer is an answer and exits 0.
`cmd` reports only the rid — a peer-side `dropped` is the peer's decision. Exit codes: 0 ok, 1
usage/IO/validation failure.

## 16. The rest of the project

The protocol prose and the schema sources live in `../doc` of this monorepo
(`actio_protocol.md`, `schemas/`); this package is the reference implementation of v0.1 core.
`../actio-go` is a port in the exact sense: it keeps these decision numbers by ID and lists its deviations,
which is why D-numbers are stable and worth citing in commits. The published package, however, must be
self-contained: nothing in `dist/` or in the shipped `src/` may read outside the package, which is the whole
reason for §2.
