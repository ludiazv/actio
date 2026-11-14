# pi-actio — how it works inside

For anyone reading or changing this extension's code: the module map, the lifecycle, what each Pi hook is
used for, the decisions behind the behaviour, and how to run the tests and the package. If you are **using**
the extension, read [`README.md`](README.md) instead. The protocol itself is not implemented here at all —
read [`actio-ts/internals.md`](https://github.com/ludiazv/actio/blob/master/actio-ts/internals.md) and
[the protocol](https://github.com/ludiazv/actio/blob/master/doc/actio_protocol.md).

Every statement below names the file it comes from. The code and its tests are the specification; this
document is the map to them.

## 1. The shape of the code

Eight source modules, about 1 070 lines, and four test files plus a fake-Pi harness.

```
src/index.ts     98  the extension entry: config, the tool, the command, the prompt service, the lifecycle
src/config.ts    113 one JSON file: its schema, defaults, the computed agent id (D-1…D-5)
src/team.ts      128 the ActioAgent wrapper: lifecycle, the team view without this agent, calls with history
src/tool.ts       57 the single `actio` tool: list-team, info, call — three commands, nothing else
src/format.ts     92 rendering of tool results: Markdown for humans, JSON for `json: true` (D-20)
src/prompt.ts    268 the prompt service: four capabilities, the queue, the compaction interplay
src/history.ts    67 in-memory records of what this session did and received, plus change listeners (D-19)
src/explorer.ts  248 the /actio overlay: the team, the outgoing calls, the received prompt requests
```

Imports from outside the package, in full:

| Specifier | Used by | Where it comes from at runtime |
| --- | --- | --- |
| `actio-ts` | `team.ts`, `prompt.ts`, `index.ts` | our one real dependency (`dependencies`) |
| `@earendil-works/pi-coding-agent` | `index.ts`, `team.ts`, `tool.ts`, `explorer.ts` | the host, aliased by Pi's loader |
| `@earendil-works/pi-tui`, `@earendil-works/pi-ai` | `explorer.ts`, `prompt.ts` | the host, aliased by Pi's loader |
| `typebox`, `typebox/value` | `config.ts`, `prompt.ts` | the host, aliased by Pi's loader |

Two rules follow from that table. The host-provided packages are declared in `peerDependencies` with `"*"`
and never in `dependencies`: Pi *"suppresses automatic peer installation"*, and a physical copy of them
*"can bypass Pi's extension module mapping in compiled ESM and create duplicate classes, registries, and
initialization work"*, which Pi reports as an extension warning. And this package holds **no protocol
logic**: it never reads or writes the actio root, it only calls the `actio-ts` package entry.

`test/faux.ts` is a 83-line fake of the Pi extension API: an `ExtensionAPI` that records registrations and an
`ExtensionContext` with the hooks the extension uses. Everything here is testable without a terminal.

## 2. Entry and lifecycle (`index.ts`)

The default export receives `ExtensionAPI` and does, in order:

1. `loadConfig()`; an invalid file applies D-2 and a `disabled` file returns — **nothing is registered**, not
   even the command (D-3).
2. Register the `actio` tool and the `/actio` command (registration is cheap; the agent is not started yet).
3. Build `Team`, `History`, and `PromptService` when `exposePrompt` is on; register the four prompt
   capabilities **before** `start()`.
4. `session_start`: bind the latest `ctx`, then `await team.start()`; a failure is notified and the tool then
   answers "actio agent is not running" (D-7).
5. `session_shutdown`: `team.stop({ force: true })` — quitting must never wait out actio-ts's 30 s grace
   period. In-flight inbound requests are answered `300`, outbound ones are abandoned (D-7).
6. `agent_settled` / `agent_end`: the prompt service's clock (D-14, D-16).

`ctx` is re-bound on every `session_start` because Pi hands a fresh context per session; the handlers close
over a mutable cell, not over the first context.

## 3. Configuration (`config.ts`)

One file: `join(getAgentDir(), "actio.json")` (D-1). The original requirements wrote
`${PI_CODING_AGENT_DIR}/angent/actio.json`; that is read as the agent dir itself, because
`PI_CODING_AGENT_DIR` *is* that directory.

- The shape is a TypeBox schema check, not a `Cast` swallow: an unknown key, a wrong type, or an id
  outside `^[a-zA-Z0-9_-]{1,32}$` throws an error that **names the path** (D-2). With
  `additionalProperties: false`, a typo in a key means a dead extension rather than a silent default — which
  is why the README spells all eight keys out.
- The config is **stricter than the protocol in one place**: `namespaces` is `string[]` here, while the
  protocol's `spec.schema.json` allows `null` items (`"*"` means all namespaces). Writing
  `"namespaces": [null]` in `actio.json` is a D-2 file, even though it would be a legal spec.
- A missing file, an empty file, or a partial file are all valid and mean defaults.
- `disabled: true` is the fully-off switch (D-3); it takes effect on `/reload`, like every config change,
  because the file is read as the extension loads.
- The default `agentId` (D-4): cwd basename, characters outside `[a-zA-Z0-9_-]` → `_`, cut to 27, plus `-`
  and 4 random hex characters, so it fits the protocol's 32-character limit. It is cached per process and cwd
  on `globalThis[Symbol.for("pi-actio.agentIds")]`, so `/reload`, `/new` and `/fork` keep it while two pi
  processes in one folder differ.
- `actioDir` (D-5): when present, a leading `~/` is expanded and the value becomes `config.root`; when
  absent, `root` is not passed at all, so actio-ts applies `$ACTIO_DIR` or `~/.actio`. Same for `namespaces`.

## 4. The team view (`team.ts`)

`Team` owns the `ActioAgent`. `acceptsCommandsFrom: []`: the pi agent accepts no protocol commands (D-6).
`agmen()` is filtered to drop our own id (D-8), so the model can never prompt its own session.

`list-team` calls `agent.refresh()` for a full scan (D-9); `info`, `call` and the Explorer read the current
view, which actio-ts keeps fresh on every nuntium event.

`startCall` runs only after D-10 passes, and each outcome of `agent.request` closes the history record
(D-19): `confirmed`; or `confirmed` with `{ code, error }` when `response.error === true`; or `cancelled` /
`abandoned`; or `failed` with the message. A `confirmed` record with `error` set is a peer's error answer and
is displayed as `error <code>`.

## 5. The tool (`tool.ts`, `format.ts`)

One tool, three commands. Before anything is written (D-10):

1. `target` matches `^([^/]+)/(.+)$`;
2. the agent is in the view;
3. the capability is in `view.capabilities`;
4. `checkParams(capability, params ?? null)` returns no error.

Each failure throws a tool error that names the next step (`use info <id>`). Then
`agent.request(id, name, params ?? null, { timeout: 0, signal })` — the signal is Pi's, so Escape cancels.

A peer's error answer is a **normal** tool result (D-11): it is the peer's answer. Only a local failure
throws. `format.ts` renders every result as Markdown, or as `JSON.stringify(value, null, 2)` of the same
shape when `json: true` (D-20). Everything the tool prints is a peer's answer rendered as text or JSON: nothing is invented here.

## 6. The prompt service (`prompt.ts`)

Four capabilities, each with `accepts_from` from `acceptPrompt`, omitted when empty (D-17 — absent and empty
both mean *every agent* in the protocol). Parameters and results are validated by the protocol; the shapes are
in the plan's table and in `test/prompt.test.ts`. `PID` is `{ type: "string", pattern: "^[0-9a-f]{8}$" }`.

- **Ids** (D-12): `randomBytes(4)` → 8 lower-case hex, redrawn on a collision with a live id.
- **States** (D-13): `queued`, `running`, `done`. `retrieve` of `done` returns the answer and **forgets** the
  id; `cancel` forgets it in any state; an unknown id in either is `ActioError(1000, "unknown prompt_id …")`.
- **When a prompt is sent** (D-14): busy means `!ctx.isIdle() || running.size > 0` (Pi reports idle for a
  moment after accepting a prompt, hence the second term). Not busy → send now. Busy and `asap` →
  `deliverAs: "steer"`. Busy and not `asap` → `queued`, FIFO. On `agent_settled`, every `running` prompt
  becomes `done` with the last `agent_end` answer (empty string after an abort), and the next queued prompt
  goes if idle. A steered prompt shares its run's answer, including a run the local user started: accepted.
- **Cancel** (D-15): forget the id and `ctx.abort()`, which ends the whole run, so another prompt riding it
  ends with the partial answer.
- **`compact`** (D-16, refined twice during M2): `ctx.compact({ onError })` and answer `null` whatever
  happens. While busy (`!ctx.isIdle() || running.size > 0 || compacting`) it sets `compactPending` and answers
  immediately; on `agent_settled` it starts the pending compaction *instead of* `sendNext()`. `compacting`
  counts as busy, so an `asap` prompt arriving mid-compaction is queued rather than steered — steering into a
  run that will not reach the model would silently lose it. The completion callback **runs once**: Pi also
  calls `onError` when `onComplete` throws, so it returns at once if the flag is already cleared, then calls
  `sendNext()` only when `!busy()`. A `compact` arriving *while compacting* answers `null` and sets nothing:
  the compaction in flight covers it.

Handlers never notify. Only the `index.ts` history subscription does (R1 A-1).

## 7. History (`history.ts`) and notifications (D-18)

In memory, per runtime: outgoing calls (including in-flight ones) and received prompt-service requests.
Lost on `/reload` — which is what *live sessions* means. No cap, on purpose. `onChange` is how the Explorer
re-renders without a timer.

`PromptRecord.outcome` is one of `prompt` → `running|queued`, `cancel` → `cancelled`, `retrieve` → the status
it returned, `compact` → `compact`, unknown id → `error 1000` (R1 A-3).

`reportActivity: true` notifies the pi user with `ctx.ui.notify(…, "info")`: `actio: <src> → <capability>
[<prompt_id>]` per received request, and `actio: <target> → <finalStatus or code>` once a call leaves
`in-flight`. A `retrieve` that polls a still-running prompt is recorded but silent. No protocol effect.

## 8. The Explorer (`explorer.ts`)

A Pi overlay with three tabs (`Team`, `Calls`, `Prompts`) and keys `tab`, `↑↓`, `r`, `esc`; it needs an
interactive terminal and says so otherwise. Two invariants, each pinned by a test:

- **Geometry.** `MIN_HEIGHT = 12` rows *including* both rules, the two header rows and the footer. A short
  tab is padded inside the frame; a long one grows it. Content rows are padded and truncated to
  `width - 4`, so the frame survives an arbitrarily narrow terminal.
- **One colour.** Every glyph of the frame — `╭╮╰╯ ├┤ │ ─` — is drawn with the theme's `border` token, the
  one pi's own dialogs use. `borderAccent` is content, not frame. A test drives the renderer with a theme
  that records every `fg(token, …)` call and fails if a frame glyph arrives with anything but `border`.

## 9. Decisions

The code cites these numbers in comments. They are recorded here rather than only in the plan so that the
package explains itself.

| Id | Decision |
| --- | --- |
| D-1 | The config file is `actio.json` in pi's agent dir; a missing file means all defaults |
| D-2 | An invalid config (bad JSON, schema violation, bad id) registers and starts nothing, and is notified once, naming the file and the first error |
| D-3 | `disabled: true` turns the extension fully off: no agent, no tool, no command, until `/reload` |
| D-4 | The default `agentId` is the cwd basename made safe, cut to 27, plus `-` and 4 hex characters, kept per process and cwd |
| D-5 | `actioDir` (with `~/` expanded) is the root; absent, actio-ts applies `$ACTIO_DIR` or `~/.actio` |
| D-6 | Identity: description `pi coding agent in <cwd>`, no protocol commands accepted, capabilities only from the prompt service |
| D-7 | The agent starts in `session_start` and stops in `session_shutdown` with `force`; a failed start is notified and the tool reports "not running" |
| D-8 | The team view excludes the agent itself, so the model can never prompt its own session |
| D-9 | `list-team` scans again; `info`, `call` and the Explorer use the view actio-ts keeps current |
| D-10 | A call is validated before anything is written: the target's form, the agent in the view, the capability, its parameters |
| D-11 | A peer's error answer is a normal tool result; only a local failure throws |
| D-12 | Prompt ids are 8 lower-case hex characters, redrawn on a collision with a live id |
| D-13 | States `queued`/`running`/`done`; `retrieve` of `done` and `cancel` forget the id; an unknown id is error 1000 |
| D-14 | A prompt is sent now when idle, steered when busy and `asap`, queued otherwise; it ends `done` on `agent_settled` with the run's last answer |
| D-15 | Cancelling a running prompt aborts the whole run |
| D-16 | `compact` answers `null` whatever happens; while busy it waits for the run to settle; mid-compaction `asap` prompts are queued, not steered |
| D-17 | `acceptPrompt` becomes each prompt capability's `accepts_from`, omitted when empty |
| D-18 | `reportActivity: true` notifies the pi user of each request received and each call that leaves in-flight; the handlers themselves never notify |
| D-19 | The Explorer's history is in memory, per runtime, uncapped |
| D-20 | `json: true` makes every tool result pretty-printed JSON, otherwise Markdown |
| D-21 | *Packaging, as first written:* `pi.extensions: ["./src/index.ts"]` and `"dependencies": { "actio-ts": "file:../actio-ts" }`. **Superseded for a published package** — a `file:` specifier in a published manifest installs fine and breaks everywhere else; see §11 |

## 10. Tests and development

`npm run check` is `tsc --noEmit` plus `node --import tsx --test "test/*.test.ts"`: **35 tests** — 9 config,
13 format, 9 prompt, 4 Explorer. There is no `npm test` script. `npm run smoke` (`test/smoke.ts`) drives the extension against a real
`ActioAgent` in a temporary actio root: register, discover, call, prompt.

- **The runner matters.** `node` on PATH here is a Bun shim, and Bun does not hand Node's quoted
  `test/*.test.ts` glob to the test runner. Use the real Node, or `bun test test/` with the glob expanded.
  Nothing in this package is Bun-specific; `npm run smoke` and the extension work under both.
- Keep `PI_CODING_AGENT_DIR` and `ACTIO_DIR` pointed at temporary directories while testing, or the agent
  joins a real team.
- To try the extension without installing it: `pi -ne -e <path>/pi-actio/src/index.ts`. Nothing needs
  installing for that — but `node_modules/actio-ts` must exist, which the monorepo does with a symlink
  (`node_modules/actio-ts -> ../../actio-ts`).
- `test/` is deliberately thin: the plan's principle is *"Do not develop extensive testing with hard gates or
  convoluted test harness"*. The tests pin the contracts that are easy to break by accident — config errors,
  rendering, the prompt state machine, the Explorer frame — and the fake Pi in `test/faux.ts` is the whole
  harness.

## 11. Packaging and publishing

- **The entry is TypeScript, and that is correct.** Pi loads extensions through jiti
  (`dist/core/extensions/loader.js`: *"Extension loader - loads TypeScript extension modules using jiti"*), so
  there is **no build step** and `src/` must be in the tarball. Published Pi packages do the same —
  `pi-mcp-adapter@7.0.1` ships `"types": "./index.ts"`.
- **`pi.extensions`** is already declared, so Pi finds the extension without the conventional `extensions/`
  directory.
- **`keywords: ["pi-package", …]`** is what makes the package eligible for the
  [Pi package gallery](https://pi.dev/packages). Every published Pi package carries it.
- **`CHANGELOG.md`** at the package root is what Pi shows under *"Extension Updates"* after an update, with
  headings like `## [0.1.0]` matched against `version`. `pi.changelogPath` points elsewhere; `""` disables it.
  Optional `pi.image` / `pi.video` add gallery previews.
- **A `files` allowlist is mandatory here.** Without `files`, npm falls back to `.gitignore`, and this
  directory's ignore list (`orchestra/`, `node_modules/`, `spec.md`) does **not** cover the two compaction
  notes sitting here: `compaction-extension.md` and `compaction-proposal.md` would ship to npm as package
  content. Allowlist: `src`, `README.md`, `internals.md`, `CHANGELOG.md`, `LICENSE`. The same fallback is what
  keeps the requirements document out of the tarball, which is why §9 transcribes the decisions instead of
  pointing at a file the reader will not have.
- **`publishConfig.access: "public"`** belongs in the manifest, not in a CI flag: provenance refuses a package
  the registry has never seen otherwise (*"Can't generate provenance for new or private package, you must
  set `access` to public"* — learned the hard way with `actio-ts@0.1.0`).
- **The `actio-ts` pin must become an exact registry version** (`"0.1.0"`, not `^` while 0.x). To develop
  against the monorepo copy afterwards, re-create the link by hand:
  `rm -rf node_modules/actio-ts && ln -s ../../actio-ts node_modules/actio-ts`.
- **From a checkout**, the dependency must exist before the extension runs: `cd actio-ts && npm ci` (its generated code is committed, so no `gen` step), then `cd ../pi-actio && npm install`. The generated code is committed precisely so that this step needs nothing but an install.
- **git installs cannot reach this directory.** Pi clones the *repository root*; there is no subdirectory
  package source. npm is therefore the only channel for `pi-actio` in this monorepo.
- **Tags and workflow.** Package-scoped tags, never a repo-wide `v*`: `pi-actio/v<version>`, publishing from a
  workflow named `publish-pi-actio.yml` — the filename is a permanent commitment, because a trusted publisher
  configuration is immutable. `actio-ts` has its own configuration pointing at `publish-actio-ts.yml`; npm
  allows at most 10 publishers per package. A trusted publisher can only be configured for a package that
  already exists, so the first release of each package goes through the token branch of the workflow. See
  [`actio-ts/internals.md`](https://github.com/ludiazv/actio/blob/master/actio-ts/internals.md) §14 for the full release procedure and the npm
  policies that shape it.
- **`pi install` mechanics worth knowing when testing:** personal installs go to
  `~/.pi/agent/settings.json` (`--local` writes `.pi/settings.json`, loaded only after project trust); Pi
  identifies a local package by its resolved absolute path, so `pi uninstall <path>` needs the path, not the
  name; Pi installs npm sources with `--legacy-peer-deps` (npm), `--omit=peer` (bun) or
  `--config.auto-install-peers=false` (pnpm), which is why the peers stay unbundled; a local path install does
  **not** install dependencies at all.

**Status: `pi-actio` is not published.** The checklist before a first release is: `private: false`, the exact
`actio-ts` pin, `keywords`, `files`, `publishConfig.access`, `repository`/`bugs`/`homepage`, a `LICENSE`
(the same BSD-3-Clause as `actio-ts`), a `CHANGELOG.md`, the README's npm install line, a pack-smoke
(`pi.extensions[0]` present in the tarball, no local specifiers, a temp-dir install resolving `actio-ts` and
`ajv`), and `publish-pi-actio.yml`.

## 12. Links

The URLs in this package name a branch. Four places in `actio-ts` and the ones here must change together, or
use `/blob/HEAD/…` and `/tree/HEAD/…` instead; the repository's default branch is `master`. The logo is one
file at the repository root (`actio-logo.svg`) and every README refers to it by URL, not by a copy.
