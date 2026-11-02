# pi-actio

A pi extension that makes a **pi coding agent a member of an Actio team**: it sees the team, calls other
agents' capabilities, and can let the team send prompts into the session.

All protocol behavior comes from [`actio-ts`](../actio-ts) through its package entry. This extension contains
no protocol logic, and it never reads or writes the actio root directly.

## What it adds

| | |
| --- | --- |
| one tool | `actio`, with three commands: `list-team`, `info`, `call` |
| one command | `/actio`, the Actio Explorer overlay |
| optional | the **prompt service**: the capabilities `prompt`, `cancel`, `retrieve`, `compact` |
| optional | **activity notifications** (`reportActivity`) |
| configuration | one JSON file, `actio.json`, read on load and on `/reload` |

Out of scope, on purpose: sending *commands* to other agents (`cmd`), attachments, a per-call timeout
parameter, history that survives `/reload`, settings UIs, and any protocol logic outside actio-ts.

## Install

`actio-ts` must be installed and its schemas generated, and `pi-actio` needs its own dependencies:

```bash
cd actio-ts    && npm ci && npm run gen
cd ../pi-actio && npm install
pi install <absolute path>/pi-actio
```

- **Node ≥ 22.** pi runs on whatever `node` is first on PATH. If that is a Bun shim, `npm test` and
  `npm run smoke` fail — Bun does not hand Node's test glob to the test runner — and `npm run check` stops at
  that point. The extension itself loads under both.
- Removing it: `pi uninstall pi-actio`.
- For development, nothing needs installing: `pi -ne -e <path>/pi-actio/src/index.ts`. Keep
  `PI_CODING_AGENT_DIR` and `ACTIO_DIR` pointed at temporary directories while testing, or the agent joins a
  real team.

## Configuration (`actio.json`)

Location: the pi agent directory — `$PI_CODING_AGENT_DIR/actio.json`, by default `~/.pi/agent/actio.json`.

**Every key is optional.** No file, or an empty object, means all defaults. A file that is not valid JSON, has
an unknown key, or has a wrong type or a bad `agentId` registers **nothing** and shows one notification naming
the file and the first error; fix it and `/reload`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `agentId` | `^[a-zA-Z0-9_-]{1,32}$` | see below | the id this agent joins the team with |
| `namespaces` | `string[]` | unset (actio-ts's default) | the namespaces this agent publishes |
| `actioDir` | `string` | unset → `$ACTIO_DIR` or `~/.actio` | the actio root folder; a leading `~` is expanded |
| `exposePrompt` | boolean | `false` | offer the prompt service (§The prompt service) |
| `acceptPrompt` | `string[]` of agent ids | `[]` | who may call the prompt service; `[]` means every agent on the team |
| `disabled` | boolean | `false` | `true`: the extension is fully off — no agent, no tool, no command |
| `json` | boolean | `false` | `true`: tool results are JSON instead of Markdown |
| `reportActivity` | boolean | `false` | notify the pi user about received requests and finished calls |

The default `agentId` is the current folder's name, with characters outside `[a-zA-Z0-9_-]` replaced by `_`,
cut to 27 characters, plus `-` and four random hex characters. It stays the same across `/reload`, `/new` and
`/fork` in one process, and two pi sessions in the same folder get different ids.

A configuration change takes effect on `/reload`, because the extension reads the file as it loads.

## The `actio` tool

The model uses it; parameters in italics are optional.

| Command | Does |
| --- | --- |
| `actio {command: "list-team"}` | a fresh scan of the team, one bullet per agent: id, description, capability names. `No visible agents.` when the team is empty |
| `actio {command: "info", id: "<agent>", capability: *"name"*}` | that agent's status, health and namespaces, and its capabilities with their parameters and result schemas. Without `capability`, all of them. An unknown name is an error that lists the real ones |
| `actio {command: "call", target: "<id>/<capability>", params: *{…}*}` | calls a peer and **waits** for the answer. Press Escape to cancel it |

Before a call is sent, the target is checked against the team view and the parameters against the published
schema, and the error says what to do next (`use info <id>`). A peer's **error answer** is a normal result —
it is the peer's answer — and is shown as `→ error <code>: <msg>`. Only a local failure (unknown agent,
unknown capability, bad parameters, no answer) is an error from the tool.

With `json: true` every result is `JSON.stringify(value, null, 2)` of the same shape.

## The prompt service

Set `exposePrompt: true`. The agent then publishes four capabilities. A peer calls `prompt`, gets a
`prompt_id` immediately, and polls it with `retrieve`.

| Capability | Parameters | Answers |
| --- | --- | --- |
| `prompt` | `{ msg_prompt, asap? }` | `{ prompt_id }` — 8 lower-case hex characters |
| `retrieve` | `{ prompt_id }` | `{ status: "queued" \| "running" \| "done", response }` — `response` is the run's final text |
| `cancel` | `{ prompt_id }` | `null` |
| `compact` | `{}` | `null`, always |

Behavior worth knowing:

- When the session is busy, a new prompt is **queued** and sent when the run settles. `asap: true` instead
  **steers** the running one, and a steered prompt shares that run's answer.
- Two prompts sent back to back while the session is idle: the second is queued. The extension keeps its own
  idea of "busy", because pi reports idle for a moment after accepting a prompt.
- `retrieve` of a `done` prompt returns the answer and **forgets** the id; a second `retrieve` of the same id
  is `ActioError 1000`. `cancel` forgets it in any state. An unknown id is the same error 1000.
- Cancelling a **running** prompt aborts the whole run, so another prompt riding the same run ends with the
  partial answer.
- A prompt may be answered by a run the local user started, and `prompt` cannot select a model or a mode.
- The pi agent accepts no protocol **commands** (`cmd`); the prompt service is the only thing it offers.
- `acceptPrompt` lists the agent ids allowed to call these capabilities. The default `[]` means every agent on
  the team — the pi agent still has no capability at all unless `exposePrompt` is on.

With `reportActivity: true` the pi user sees, as notifications: `actio: <src> → <capability> [<prompt_id>]`
for each received request, and `actio: <target> → confirmed | error <code> | cancelled | …` when a call the
agent made finishes. Polling `retrieve` while a prompt is queued or running makes no noise. These have no
protocol effect.

## `/actio` — the Actio Explorer

Opens an overlay with three tabs. It needs the interactive terminal; in another mode it reports that.

```
 Actio Explorer — <agentId> · <actio root> · prompt service: on|off
 [Team]  Calls  Prompts
 Team:    the agents (id, status/health, description); the selected agent's capabilities below
 Calls:   this agent's outgoing calls, newest first: <id>/<capability>  in-flight | confirmed <ms> ms | error <code> | cancelled | abandoned | failed: <msg>
 Prompts: received prompt-service requests, newest first: <time> <src> <capability> [<prompt_id>] <outcome>
 tab switch · ↑↓ select · r refresh · esc close
```

The list updates by itself when a call or a request arrives or finishes; `r` re-scans the team.
The history is **in memory, per session**: `/reload`, `/new` or `/fork` starts it empty again, which is what
"live sessions" means. It has no size cap.

## Implementation choices

- One config file, read at load; a `/reload` is enough to change anything.
- An invalid configuration registers nothing and says so once, rather than half-working.
- The agent starts when a session starts and stops when it ends, with no grace period on quit; in-flight
  inbound requests are answered 300 and outbound ones are abandoned.
- The team view excludes the agent itself, so the model can never prompt its own session.
- `list-team` scans again (that is what "live" means); `info`, `call` and the Explorer use the view the
  protocol keeps current.
- Everything the tool prints is a peer's answer rendered as text or JSON; nothing is invented locally.
- A prompt's answer is the last assistant text of the run, which may be empty after an abort.

## Limits

- One tool, three commands, one command, four capabilities: nothing else, on purpose.
- No per-call timeout parameter; a call waits for the peer and Escape cancels it.
- History is not persisted, and there is no way to ask about a session that is not running.
- The prompt service answers with the run's final text: no streaming, no mid-run output, no model choice.
- The Explorer cannot show the resolved actio root when `actioDir` is unset (actio-ts keeps it); set
  `actioDir` to see the path in the header.
- No protocol commands (`cmd`), no attachments, no retries.
