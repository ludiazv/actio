# Hora

An Actio agent that schedules calls to other agents' capabilities. Any agent in the same
Actio tree can ask Hora to arrange a timed call — a one-shot at a specific time, a
repeating interval, or a calendar (cron) schedule — by calling Hora's `set` capability
with a plain-language prompt. Hora translates the prompt into a schedule with a small
LLM, stores it in a JSON database, and fires the capability call when the time comes.

**Reading or changing the code?** `doc/internals.md` covers the structure, the
lifecycle, time-zone handling, the prompts and the database format.

## Building

Hora is one static Go binary, built with the Go toolchain 1.26 or newer (the module pins
`go 1.26.0`) and the pinned `actio-go` library next to it. Both static builds must pass:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o hora-amd64 .
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o hora-arm64 .
```

The result is a statically linked binary of about 21 MB; `file hora-amd64` says
`statically linked`. The IANA zone database is compiled into the binary, so no system
tzdata is needed.

## Running

```sh
hora -config <file>
```

- A missing `-config` (or any usage error) prints the usage to stderr and exits **2**.
- A config file that breaks a rule, or a start that fails at any step — including a
  database the loader refuses — prints the reason, naming the offending key or entry,
  to stderr and exits **1**.
- SIGINT and SIGTERM stop the agent gracefully, and Hora then exits **0**. A call still
  running 30 s after the signal is left behind (a line on stderr) and Hora still exits
  **0**; an agent that cannot be stopped within its own 30 s is a failed stop: Hora
  prints the stop's error and exits **1**.
- A signal that lands before the process reaches its own code cannot be caught: TERM
  the instant `exec` returns kills it with the default disposition.

## The configuration file

One JSON object. Unknown keys are an error, as is a second JSON value after the
object.

| Key | Required | Default | Rule |
| --- | --- | --- | --- |
| `agent_id` | yes | — | The Actio agent id. Its form is checked by the library. |
| `namespaces` | no | `[]` | An array of strings or `null`s (a `null` item is a nil namespace). An empty or absent list is nil. |
| `TZ` | no | `"UTC"` | The IANA zone, loaded with `time.LoadLocation`. A zone the library cannot load is a config error. |
| `coordinators` | no | `[]` | Agent ids that may schedule and manage entries for other agents. |
| `db` | yes | — | The path to the DB file. A relative path is resolved against **the config file's directory**. The directory must exist; the file is created on the first save. |
| `model` | yes | — | The chat model's name, sent to `url`. |
| `url` | yes | — | The OpenAI-compatible chat/completions server. |
| `env_apikey` | yes | — | The name of an environment variable holding the chat API key. When it is `""`, no key is sent. When it is not, the variable must be set and non-empty at start, or the start fails. |
| `decision_model`, `decision_url`, `decision_env_apikey` | no | — | The decision model, used to filter requests before the chat round. **All three keys present, or none**, or the config is an error — presence is the key's, not the value's. When present, `decision_model` and `decision_url` must be non-empty; `decision_env_apikey` follows the same key rule, so `""` means no key. |
| `decision_gate` | no | `"v1"` | The name of a registered decision gate. An unknown name is a config error, and so is the key without the decision triple. |

The resolved values (the loaded zone, the two API keys) are never read from the file.

## The capabilities

Hora publishes three capabilities. A caller's own timeout must allow for Hora's: `set`
publishes `recommended_timeout: 180` seconds, because a `set` can spend two chat
rounds plus a decision round.

### `set`

Parameters: `{"prompt": string (required), "dest_id": string (optional)}` — nothing
else is accepted.

The destination is `dest_id`, or the caller when it is absent; a non-coordinator may
only schedule for itself. Hora asks the decision model (when configured) and then the
chat model, and validates the answer against the destination's visible capabilities.
If the first answer fails validation, one more round runs, carrying the reasons.

Results:

- `{"result":"scheduled","sched_id","dest_id","dest_capability","capability_parameters","sched_expression"}`
- `{"result":"rejected","dest_id","rejection"}` — with `unauthorized`, `agent not
  found or no visible capabilities for the scheduler`, `not a scheduling request`, or
  `invalid answer: <reasons>`.

When `set` answers `scheduled`, the entry is already in the database file and already
in the scheduler.

### `list`

Parameters: none.

It returns the caller's entries, or every entry when the caller is a coordinator,
sorted by `datetime` then `sched_id`. Each item carries `sched_id`, `dest_id`,
`sched_type`, `dest_capability` and `sched_expression`. An empty result is `[]`.

### `unset`

Parameters: `{"sched_id": string (required, matching the id format)}` — nothing else.

It removes the entry when the caller is its owner or a coordinator, and answers
`{"result":"unset","sched_id"}`. A missing entry and a not-yours entry get the same
capability error, code **1001**, `unknown sched_id: <id>`: `unset` reveals no more than
`list` does. A call that is already running when its entry is unset is not cancelled.

## What Hora writes in the log

Every line below is a Hora diarium line, so a running agent can be followed — and
schedules audited — from the tree's log alone.

```
gate <name>: ok=<true|false> score=<0.000> type=<t> capability=<c>   # one per decision
gate <name>: failed: <err>                                          # a decision call that failed
fire <sched_id> <dest>/<cap>: <final status>                         # one per firing
     … error <code> <msg>   … failed: <err>                          # appended when it went wrong
skip <sched_id>: previous call still running                        # one per skipped firing
clean <sched_id>: one-shot time passed                              # at start, for expired entries
```

## When a schedule comes due

Hora calls the destination's capability with the stored parameters. The call's own
timeout is the **destination** capability's `recommended_timeout` when it is greater
than 0, and 300 s otherwise. While one call for a schedule is still running, its later
firings are skipped, and each skip gets a line. A `one-shot` is removed only after it
has run and been logged, so it is still listed during a slow call.

## The database file

A JSON array of entries, written as one compact line (the example below is
wrapped to fit), with the fields in this order:

```json
{"datetime":"2026-11-08T18:00:00Z","sched_id":"SCH-1a2b3c4d","sched_type":"cron",
 "sched_expression":"CRON_TZ=Europe/Madrid 0 0 13 * * 0","dest_id":"backup-agent",
 "dest_capability":"backup","capability_parameters":{"kind":"full"}}
```

- `datetime` is the creation time, in UTC, RFC 3339 with whole seconds.
- `sched_id` is `SCH-` plus 8 lowercase hex digits.
- `sched_type` is `one-shot`, `periodic` or `cron`, and `sched_expression` is in its
  stored form: an RFC 3339 **UTC** time, `@every <duration>` of whole seconds of at
  least 1s, or six cron fields that **must** carry a `CRON_TZ=<zone> ` or `TZ=<zone> `
  prefix — the file has no zone of its own, so a stored cron names its own.
- `dest_id` and `dest_capability` are non-empty. `capability_parameters` is an object;
  a `null` read from the file is taken as `{}`.

You may edit the file by hand. Saves are atomic — a whole new file is written and
renamed into place with mode `0600` — so a crash leaves the previous file, never a
partial one. At start, a missing file is an empty database, every `one-shot` whose time
has already passed is dropped (and logged), and any entry that breaks a rule above
**stops the start**, naming the entry's index and the reason, with the file left
untouched: Hora never silently discards what you wrote.

An entry belongs to its `dest_id`: only that agent, or a coordinator, sees it in `list`
or can remove it with `unset`.

## Limits

Hora is a high-level scheduler, not a precise timer. It does not support sub-second
scheduling, and while it can express second-level precision, the Actio protocol and
internal latency mean a call can arrive with some delay — use Hora for scheduling in
long-running workflows, not for time-critical tasks. If you need precise timing or
rapid triggering, use another solution.
