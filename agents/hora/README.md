# Hora

An Actio agent that schedules calls to other agents' capabilities. Any agent in the same
Actio tree can ask Hora to arrange a timed call — a one-shot at a specific time, a
repeating interval, or a calendar (cron) schedule — by calling Hora's `set` capability
with a plain-language prompt. Hora translates the prompt into a schedule with a small
LLM, stores it in a JSON database, and fires the capability call when the time comes.

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
- A config file that breaks a rule, or a start that fails at any step, prints the
  reason — which names the offending key — to stderr and exits **1**.
- SIGINT and SIGTERM stop the agent gracefully; Hora then exits **0**.

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

## The database

A JSON array of entries, in this field order:

```json
{"datetime":"2026-11-08T18:00:00Z","sched_id":"SCH-1a2b3c4d","sched_type":"cron",
 "sched_expression":"CRON_TZ=Europe/Madrid 0 0 13 * * 0","dest_id":"backup-agent",
 "dest_capability":"backup","capability_parameters":{"kind":"full"}}
```

- `datetime` is the creation time, in UTC, RFC 3339 with whole seconds.
- `sched_id` is `SCH-` plus 8 lowercase hex digits, generated from `crypto/rand`.
- `sched_type` is `one-shot`, `periodic` or `cron`, and `sched_expression` is in its
  stored form: an RFC 3339 UTC time, `@every <duration>` of whole seconds of at least
  1s, or six cron fields — a `cron` expression must carry a `CRON_TZ=<zone> ` or
  `TZ=<zone> ` prefix, because the database file has no zone of its own.
- `dest_id` and `dest_capability` are non-empty.
- `capability_parameters` is an object; a `null` read from the file is taken as `{}`.

The file is never written in place: a save writes the whole array to a temp file in
the same directory, fsyncs it, and renames it over the DB, with mode `0600`. An empty
database is written as `[]`, never `null`.

At start, a missing file is an empty database. A file that is not a JSON array, or any
entry that breaks a rule above, fails the start, naming the entry's index and the
reason; the file is left byte-for-byte as it was. Before that check, every `one-shot`
entry whose time is not after now is removed (and the DB is saved if anything was
removed): a past one-shot must not fail the start, and it is not fired.

An entry belongs to its `dest_id`: only that agent, or a coordinator, may see it in
`list` or remove it with `unset`.

## The capabilities

Hora publishes three capabilities.

### `set`

Parameters: `{"prompt": string (required), "dest_id": string (optional)}` — nothing
else is accepted.

It resolves the destination (the `dest_id`, or the caller when it is absent), refuses
with `unauthorized` when a non-coordinator schedules for another agent, reads the
destination's view, and, when a decision model is configured, asks it through the
configured gate. The chat model then extracts the schedule (one round, and a second
round carrying the previous reply and the failure reasons when the first reply fails
validation). A valid answer becomes an entry: saved to the DB, then added to the
runner, then answered.

Results:

- `{"result":"scheduled","sched_id","dest_id","dest_capability","capability_parameters","sched_expression"}`
- `{"result":"rejected","dest_id","rejection"}` — with `unauthorized`, `agent not
  found or no visible capabilities for the scheduler`, `not a scheduling request`, or
  `invalid answer: <reasons>`.

`set` publishes `recommended_timeout: 180` seconds: a chat round ran up to 18.6 s on
the measured server, a decision 2.3 s, and a set can take two rounds.

### `list`

Parameters: none.

It returns the caller's entries, or every entry when the caller is a coordinator,
sorted by `datetime` then `sched_id`. Each item carries `sched_id`, `dest_id`,
`sched_type`, `dest_capability` and `sched_expression`. An empty result is `[]`.

### `unset`

Parameters: `{"sched_id": string (required, matching the id format)}` — nothing else.

It removes the entry when the caller is its owner or a coordinator — first the DB
save, then the removal from the runner, then the answer `{"result":"unset","sched_id"}`.
For a missing entry and a not-yours entry alike it answers the capability error with
code **1001**, `unknown sched_id: <id>`: one answer for both, so `unset` reveals no
more than `list` does. A call that is already running when its entry is unset is not
cancelled.

## Firing

When a schedule comes due, Hora calls the destination's capability with the stored
parameters. The call's own timeout is the capability's `recommended_timeout` when it
is greater than 0, and 300 s otherwise. One line goes to Hora's diarium per firing:
`fire <sched_id> <dest>/<cap>: <final status>`, with ` error <code> <msg>` on an error
answer (a `-` where the answer carries no code) and ` failed: <err>` when the call
itself failed. While one call for a schedule is still running, its later firings are
skipped, and each skip gets one line: `skip <sched_id>: previous call still running`.
A `one-shot` is removed from the DB and the runner only after it has been executed and
logged.

## Limits

Hora is a high-level scheduler, not a precise timer. It does not support sub-second
scheduling, and while it can express second-level precision, the Actio protocol and
internal latency mean a call can arrive with some delay — use Hora for scheduling in
long-running workflows, not for time-critical tasks. If you need precise timing or
rapid triggering, use another solution.

Two more edges, measured:

- A call still running 30 s after the stop signal is left behind: Hora writes to
  stderr, stops the agent anyway, and exits 0 unless the agent stop itself fails.
- A signal that lands before the process reaches its own code cannot be caught: TERM
  the instant `exec` returns kills it with the default disposition, because no handler
  is installed yet.