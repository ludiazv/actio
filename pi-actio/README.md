<p align="center">
  <img src="https://raw.githubusercontent.com/ludiazv/actio/HEAD/actio-logo.svg" alt="Actio" width="200">
</p>

# pi-actio

A [pi](https://pi.dev) extension that makes a **pi coding agent a member of an Actio team**: it sees the
team, calls other agents' capabilities, and can let the team send prompts into the session.

## What it adds

| | |
| --- | --- |
| one tool | `actio`, with three commands: `list-team`, `info`, `call` |
| one command | `/actio`, the Actio Explorer overlay |
| optional | the **prompt service**: the capabilities `prompt`, `cancel`, `retrieve`, `compact` |
| optional | **activity notifications** (`reportActivity`) |
| configuration | one JSON file, `actio.json`, read on load and on `/reload` |

Out of scope, on purpose: sending *commands* to other agents, attachments, a per-call timeout parameter,
history that survives `/reload`, settings UIs.

## Install

```bash
pi install npm:pi-actio@0.1.0
```

The npm source becomes available with the first published release. From a source checkout, install the
folder instead — `pi install <absolute path>/pi-actio` — and install the checkout's dependencies first
([internals.md](internals.md) §11).

- **Node ≥ 22.**
- Remove it with `pi uninstall <source>`. For a folder, pi matches an installed extension by its resolved
  path, so use the same path you installed from.
- Personal installs are recorded in `~/.pi/agent/settings.json`. An install is loaded at the next `pi` start
  or `/reload`.

## Configuration (`actio.json`)

Location: the pi agent directory — `$PI_CODING_AGENT_DIR/actio.json`, by default `~/.pi/agent/actio.json`.

**Every key is optional.** No file, or an empty object, means all defaults. A file that is not valid JSON, has
an unknown key, has a wrong type, or has a bad agent id (`agentId` or an `acceptPrompt` item) registers
**nothing** and shows one notification naming the file and the first error; fix it and `/reload`.

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

A configuration change takes effect on `/reload`.

## The `actio` tool

The model uses it; parameters in italics are optional.

| Command | Does |
| --- | --- |
| `actio {command: "list-team"}` | a fresh scan of the team, one bullet per agent: id, description, capability names. `No visible agents.` when the team is empty |
| `actio {command: "info", id: "<agent>", capability: *"name"*}` | that agent's status, health and namespaces, and its capabilities with their parameters and result schemas. Without `capability`, all of them. An unknown name is an error that lists the real ones |
| `actio {command: "call", target: "<id>/<capability>", params: *{…}*}` | calls a peer and **waits** for the answer. Press Escape to cancel it |

Before a call is sent, the target is checked against the team and the parameters against the published
schema, and the error says what to do next (`use info <id>`). A peer's **error answer** is a normal result —
it is the peer's answer — and is shown as `→ error <code>: <msg>`. Only a local failure (unknown agent,
unknown capability, bad parameters, no answer) is an error from the tool.

With `json: true` every result is JSON instead of Markdown.

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
- Two prompts sent back to back while the session is idle: the second is queued.
- `retrieve` of a `done` prompt returns the answer and **forgets** the id; a second `retrieve` of the same id
  is `ActioError 1000`. `cancel` forgets it in any state. An unknown id is the same error 1000.
- Cancelling a **running** prompt aborts the whole run, so another prompt riding the same run ends with the
  partial answer.
- A prompt may be answered by a run the local user started, and `prompt` cannot select a model or a mode.
- The pi agent accepts no protocol **commands**; the prompt service is the only thing it offers.
- `acceptPrompt` lists the agent ids allowed to call these capabilities. The default `[]` means every agent on
  the team — and the pi agent publishes no capability at all unless `exposePrompt` is on.

With `reportActivity: true` the pi user sees, as notifications: `actio: <src> → <capability> [<prompt_id>]`
for each received request, and `actio: <target> → confirmed | error <code> | cancelled | …` when a call the
agent made finishes. Polling `retrieve` while a prompt is queued or running makes no noise.

## `/actio` — the Actio Explorer

Opens an overlay with three tabs. It needs the interactive terminal; in another mode it reports that. The
window never collapses below twelve rows, and it follows pi's theme colours.

```
╭──────────────────────────────────────────────────────────────────────────────────────────╮
│ Actio Explorer — <agentId> · <actio root> · prompt service: on|off                      │
│ [Team]  Calls  Prompts                                                                 │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│ Team:    the agents (id, status/health, description); the selected agent's capabilities below
│ Calls:   this agent's outgoing calls, newest first: <id>/<capability>  in-flight | confirmed <ms> ms | error <code> | cancelled | abandoned | failed: <msg>
│ Prompts: received prompt-service requests, newest first: <time> <src> <capability> [<prompt_id>] <outcome>
│ tab switch · ↑↓ select · r refresh · esc close                                         │
╰──────────────────────────────────────────────────────────────────────────────────────────╯
```

The list updates by itself when a call or a request arrives or finishes; `r` re-scans the team. The history is
**in memory, per session**: `/reload`, `/new` or `/fork` starts it empty again, which is what "live sessions"
means.

## Limits

- One tool, three commands, one command, four capabilities: nothing else, on purpose.
- No per-call timeout parameter; a call waits for the peer and Escape cancels it.
- History is not persisted, and there is no way to ask about a session that is not running.
- The prompt service answers with the run's final text: no streaming, no mid-run output, no model choice.
- The Explorer cannot show the resolved actio root when `actioDir` is unset; set `actioDir` to see the path in
  the header.
- A compaction started **by the pi user** (`/compact`) is outside the extension's view: a prompt queued during
  it waits for the next run, and an `asap` prompt sent during it never reaches the model and is later reported
  `done` with the next run's answer. Every compaction requested through the prompt service is waited for and
  then run.
- No protocol commands, no attachments, no retries.

## Where the details are

- [`internals.md`](internals.md) — how it works, the decisions behind each behaviour, tests, packaging.
- [The Actio protocol](https://github.com/ludiazv/actio/blob/master/doc/actio_protocol.md) — the wire format,
  which this extension implements through [`actio-ts`](https://www.npmjs.com/package/actio-ts). The protocol
  is public but still a **draft**: its security layer is unspecified, and write access to the shared actio
  folder *is* peer authority. Do not point two teams' folders at each other.
