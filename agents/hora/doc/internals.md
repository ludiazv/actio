# Hora — how it works inside

`README.md` is for someone installing or using Hora. This file is for someone
reading or changing it: the shape of the code, the pieces that meet on the normal
path, and the mechanisms behind what the README promises. It does not repeat the
configuration table or the capability schemas — those are the README's.

Every statement names the file it comes from. The code and its tests are the
specification; they are small enough to read, and this document is the map to them.

## 1. The files

Seven source files, about 1 100 lines, and eleven test files, about 4 000 lines.
One package, `main`, so nothing here is importable: two types are capitalised for
readability — `Config` and `Entry` — and nothing else is.

| File | What lives there |
| --- | --- |
| `main.go` | The process. `main` (the flag, the exit codes), `run` (the start and the stop), `fire`, the `inFlight` bookkeeping, and the `stopBound` each stop phase gets. |
| `set.go` | The agent's state (`hora`), `set`'s shell and pipeline, the validation of a model's answer, the extraction prompt, and the destination-capability context handed to both models. |
| `caps.go` | The three published capabilities and their JSON schemas, `publish`, `list`, `unset`, and `isCoordinator`. |
| `config.go` | The `Config` struct and `loadConfig`: strict decoding, the defaults, the decision-triple rule, the zone load, the two API keys. |
| `store.go` | `Entry` (one DB record) and `store`: load, check, clean, add, remove, get, all, the id generator, and the atomic save. |
| `sched.go` | `parseSchedule` — one function, three scheduling modes, one canonical stored form — and `runner`, the robfig wrapper. |
| `model.go` | `models`, the two model calls; the reply text; the `gate` type, the registry and `findGate`; gate `v1`'s state, questions and verdict. |
| `go.mod`, `go.sum` | The pins: `actio-go` by local `replace`, PiG (the `ai` package only), robfig's `cron`. Two indirect `golang.org/x/...` entries come from those. |
| `testdata/gate_corpus.json` | 18 labelled prompts — 10 that must be accepted, 8 that mostly must not — for measuring a gate. |
| `README.md` | The user's document: install, run, configure, use, limits. |
| `doc/internals.md` | This file. |
| `.gitignore` | The orchestration workspace and the script that starts it. Neither is part of the module. |

The tests split by how much of the system they let in:

| Test file | What it exercises |
| --- | --- |
| `config_test.go`, `store_test.go`, `sched_test.go`, `sched_runner_test.go`, `model_test.go`, `set_test.go`, `caps_test.go` | One file at a time, with fake model servers and a fixed clock. `set_test.go` uses a real in-process Actio agent so `Refresh` and the call views are real. |
| `main_test.go` | `run`, `fire` and the stop, in-process, against real in-process agents. |
| `e2e_test.go` | A whole tree: Hora plus caller plus destination, over the wire, with the answers canned. The last rows build the binary (`go build -o <temp>/bin/hora`) and run it as a child process, so the flag handling, the signals and the exit codes are real. |
| `live_test.go`, `gate_eval_test.go` | Build tag `live`. They need `HORA_LIVE_URL` and a real model server, and they **fail** rather than skip without it, so they never join a normal run by accident. |

## 2. The normal lifecycle

One `hora` struct (`set.go`) is the whole agent state: the config, the Actio agent,
the store, the runner, the models, a `now` function (the tests replace it), and the
in-flight bookkeeping. One process runs one `hora`, built once and never rebuilt.

### Start — `run`, in order

The order matters at three points, so it is worth spelling out:

1. **The signal handler goes in first**, before the config is read. A signal after
   this point runs the stop; a signal before the process reaches its own code cannot
   be caught at all.
2. `loadConfig`, then `openStore(cfg.DB, time.Now())` — which loads the entries,
   removes the one-shots whose time has already passed, and returns both the entries
   and what it cleaned. A broken DB stops the start here, file untouched.
3. `actio.New`, then `h.publish(a)`, which registers `list`, `unset` and `set`.
4. `h.run.add(e)` **for every entry, before `a.Start`**. The runner is not started
   yet, so nothing can fire, and a one-shot that comes due while the agent is
   starting is still a live entry. Adding it after the start let such an entry be
   re-parsed as "already past" and cleaned without ever firing.
5. `a.Start(ctx)`. A start whose context is cancelled before it returns is a failed
   start, like any other failure: the earlier steps are undone and `run` returns the
   error, so the process exits 1.
6. One log line per cleaned entry (`clean <sched_id>: one-shot time passed`), then
   `h.run.start()`.

### Waiting

`run` then blocks in a `select` with four stop sources: SIGINT/SIGTERM, the agent's
own done channel, and the caller's context (`main` passes `context.Background()`, so in
practice the first two). In the meantime nothing in Hora's own goroutines does any
work: robfig's Cron owns the timing, and it calls `h.fire(id)` in a job goroutine for
each firing.

### Firing — `h.fire`

1. Look the entry up; if it is gone (unset, or another firing already cleaned it),
   return.
2. Under the in-flight mutex: if the stop has begun, return without calling anything;
   if this id is already running, unlock and write `skip <sched_id>: previous call
   still running`; otherwise register this call's cancel function. The mutex is
   released before the request, and the entry is removed from the set on every exit
   path.
3. `Refresh`, read the destination capability's `recommended_timeout`, and use it if
   it is greater than zero, 300 s otherwise.
4. `a.Request(fireCtx, dest, capability, parameters, RequestOptions{Timeout: rt})`.
5. One diarium line: `fire <sched_id> <dest>/<cap>: <final status>`, with
   ` error <code> <msg>` appended for an error answer (`-` where the answer carries
   no code), or ` failed: <err>` when the call itself failed.
6. Only for a one-shot, and only after the line: remove it from the DB and the
   runner. Executed, logged, then cleaned — in that order.

Two rules hold everywhere in the file: **no lock is ever held across a model call, an
Actio call or `fire`** (the store's mutex covers only the map change and the file
write), and **the runner never drops or merges firings** — no chain, no
`SkipIfStillRunning`, no dedupe in `sched.go`. Skipping a schedule that is already
running is `fire`'s job, because it is the only place that knows what a call is.

### Stop

`cancelFiring` → `runner.stop(ctx)` → `a.Stop(ctx, {})`. Each of the last two gets
**its own fresh 30 s context** (`stopBound`), never the same one. The order and the
two bounds are what make the observable behaviour make sense:

- Cancelling first means no new request starts, and a running one gets its context
  cancelled; cancelling the call does not return from it — the response (or its
  absence) still arrives, so the fire line is written.
- `runner.stop` waits for running jobs, bounded. Past its bound Hora writes to stderr
  and goes on: the leftover call must not expire the agent's stop as well.
- `a.Stop` on a bound of its own is what lets the agent publish `inactive` and Hora
  exit 0 even when a destination ignored the cancellation and burned the runner's
  whole bound. If the agent itself cannot be stopped, `run` returns `stop: <err>` and
  the process exits 1 — that is a failed stop, not a graceful one.

A row in `main_test.go` exists for each of those two shapes, and they measure apart:
about 3.1 s and about 6.0 s at a 3 s bound.

### The pieces that meet there

| Type | File | Role |
| --- | --- | --- |
| `Config` | `config.go` | The config file's shape, plus three resolved fields (`Loc`, `APIKey`, `DecisionAPIKey`) that are never read from the file. |
| `Entry` | `store.go` | One DB record, and the JSON field order of the file. |
| `store` | `store.go` | The entries in memory, the mutex, the file. `openStore`, `loadEntries`, `checkEntry`, `add`, `remove`, `get`, `all`, `freshID`, `saveLocked`. |
| `runner` | `sched.go` | One robfig `Cron` in UTC plus `map[sched_id]cron.EntryID` under its own mutex. `add`, `remove`, `start`, `stop`. |
| `oneShot` | `sched.go` | A `cron.Schedule` that returns the time once, then the zero time — which robfig never fires again. |
| `models` | `model.go` | The two calls and their base URLs, keys and `Fetch` clients. `complete`, `decide`, `hasDecision`, `replyText`. |
| `gate`, `gateV1`, `gateVerdict`, `hints` | `model.go` | The decision step as data: three functions and a result. |
| `hora` | `set.go` | The agent state, and the `set` pipeline that ties all of the above together. |
| `inFlight` | `main.go` | `map[sched_id]context.CancelFunc` plus a `stopped` flag, under one mutex. |

## 3. Time zones

The config's `TZ` is loaded once with `time.LoadLocation` and kept as `cfg.Loc`. The
IANA database is compiled into the binary (`_ "time/tzdata"` in `main.go`), so a
static build needs nothing from the host.

What the zone touches, mode by mode:

- **one-shot.** Any RFC 3339 offset is accepted; the time is converted and **stored
  in UTC**. `oneShot.Next` compares instants, so the zone plays no further part and
  never reaches the file.
- **periodic.** No zone at all: `@every <duration>` is an interval, not a wall-clock
  time.
- **cron.** This is the only place the zone is data. If the expression carries no
  `CRON_TZ=<zone> ` or `TZ=<zone> ` prefix, `parseSchedule` writes
  `CRON_TZ=<config TZ> ` in front of it when storing. The entry therefore keeps its
  wall-clock meaning if the config's `TZ` changes later, or if the file is copied to a
  machine whose system zone is different.

Two consequences of that choice are enforced in both directions:

- A stored cron expression **must** carry a prefix. `loadEntries` rejects a
  prefix-less one as a start error, because the file has no zone of its own and the
  entry would be ambiguous.
- The runner's `Cron` is created with `cron.WithLocation(time.UTC)`, and the parser
  applies the prefix's zone. `0 0 13 * * 0` with `CRON_TZ=Europe/Madrid` fires at
  11:00 or 12:00 UTC, whichever that Sunday's offset is — DST included, for free.

The zone also reaches the models. The extraction prompt states the current time twice,
in UTC and in the config zone with the zone's name and offset, and says that a bare
cron is read in that zone; the decision state does not carry the clock at all.

What `TZ` does **not** do: it never rewrites existing entries, it is not re-applied to
a cron entry that already carries a zone, and it never lets a bare local time into the
file for a one-shot.

Rows that pin this: `TestParseSchedule`'s cron rows (bare, `CRON_TZ=`, `TZ=`),
`TestRunner/Madrid cron fires at its UTC instant`,
`TestLoadCronWithoutPrefixFailsAndFileUnchanged`, and
`TestSetTwoRounds/the config zone's name and local time in the prompt`.

## 4. The prompts, and how a request is rejected

`set` runs this pipeline (`set.go`), and the two model calls are the only parts that
guess anything:

```
resolve dest → authorize → read the destination's view
  → [decision model, if configured] → gate: accept / reject / no hints
  → round 1: extraction prompt → chat model
  → validate (every check, all reasons)
  → round 2 (a fresh request carrying the reasons) → validate
  → save → runner add → "scheduled"
```

### The decision call, and the rejection algorithm

The decision is not a chat call: it is PiG's typesafe system-one classifier
(`ai.ClassifyTypesafeSystemOne` to `<decision_url>/v1/systemone`), whose answers carry
probabilities. Everything about what it asks lives in one `gate` value; `decide` only
builds the state and the questions from the named gate, makes the call, and hands the
answers to the gate's `verdict` function — which is pure (no I/O, no clock), so every
gate is testable with canned answers and no server.

Gate `v1`, the only one shipped:

- **State** — three keys: `request` (the caller's prompt), `destination` (the
  capability context, described below) and `modes` (one line naming the three
  scheduling modes).
- **Question `valid`**, a bool: instructions `Is the request a valid scheduling
  request?`, criteria `it asks for something to happen at a time or repeatedly` /
  `it does not ask to schedule anything`.
- **Question `type`**, a choice of `one-shot`, `cron`, `periodic`.
- **Question `capability`**, a choice over the destination's visible capability names
  — asked **only when there are two or more**. With one capability that one is the
  hint without asking.

The verdict is arithmetic: `Score` is the probability of `valid`; **below 0.5
(`v1ValidCutOff`) the request is rejected**, and `set` answers
`not a scheduling request` — always that string, whatever the gate, so callers never
depend on a gate's internals. At or above it, the `type` and `capability` answers
become hints, added to the extraction prompt as one line. A failed decision call, or
an answer that is missing or of the wrong kind, is **not** a rejection: Hora logs
`gate <name>: failed: <err>` and goes on to round 1 with no hints, because the
decision model only filters. Every decision, rejected or not, writes one line:
`gate <name>: ok=<bool> score=<0.000> type=<t> capability=<c>`.

Over the 18-row corpus on the measured server, `v1` accepts 10/10 of the requests that
should pass (0.92–1.00), gets the mode right on all 10, rejects 6 of the 8 that should
fail, and passes two: a request naming a capability the destination does not have
(0.98, which validation then rejects) and `Do something every hour` (0.99, which fails
both chat rounds). The two criteria texts are part of the rule, not decoration: with
`asks to schedule a capability call …` instead, the spec's own one-shot scored 0.10 and
its periodic 0.29. Two questions were measured and dropped: `Can the capability be
inferred?` scored 0.002–0.03 even on good requests, and `can the parameters be
inferred?` 0.39–0.67.

### The extraction prompt, verbatim

`systemPrompt` in `set.go` builds the system message; the user message is the caller's
`prompt` and nothing else. Assembled, it reads:

```
Reply with JSON only: one object with exactly the keys sched_type, sched_expression, dest_capability, capability_parameters.
sched_type is one of: one-shot, cron, periodic.
one-shot: sched_expression is an RFC 3339 UTC time with whole seconds, after now, like 2026-12-01T09:30:00Z.
cron: sched_expression is six fields (sec min hour dom month dow), optionally prefixed CRON_TZ=<zone> ; a cron without a prefix is read in the config zone.
periodic: sched_expression is @every <duration>, whole seconds of at least 1s, like @every 30m.
Current time: 2026-11-09T16:30:00Z UTC; in Europe/Madrid: 2026-11-09T17:30:00 +0100 (CET).
destination backup-agent: backs the site up. capabilities: backup (a full or partial backup) parameters: {"type":"object",…}, status (last run) parameters: {…}
Decision hints: type periodic, capability backup.
```

The `Decision hints` line appears only when the gate gave a type or a capability. The
capability context is built by `destCapContext`: the destination's id and description,
then each visible capability's name, description and parameter schema, sorted by name.
"Visible" means after Actio's own callability filter, so the model is never shown a
capability Hora could not call.

Round 2 is **a fresh request, not a continued transcript**: the same system message and
one user message that carries the whole history itself.

```
Original prompt: <the caller's prompt>
Previous reply: <the text of the failed reply>
It failed validation: <the reasons, joined by "; ">
```

The call is `ai.CompleteSimple` to `<url>/v1/chat/completions` with
`MaxTokens: 8192`. That number is load-bearing: without it PiG sends
`max_completion_tokens: 1`, and a measured model spent 2 431 output tokens on one cron
answer, so anything smaller gets cut off mid-reply.

A reply's text is the `ai.TextContent` blocks joined; thinking blocks are ignored. It is
trimmed, and one surrounding fenced block (three backticks, optionally followed by
`json` on the opening line) is removed; the rest must decode as one JSON object.

`set` publishes its own `recommended_timeout: 180` seconds, and that number comes from
measurements on the model server: a chat round ran up to 18.6 s, a decision round 2.3 s,
and a `set` can take two chat rounds.

### Validation, and the four ways a `set` is rejected

`validateReply` runs **every** check and collects **every** reason — the first failure
does not short-circuit, because round 2 is more useful with all of them:

| Check | The reason it appends |
| --- | --- |
| The reply decodes as one JSON object | `the reply is not one JSON object: <err>` |
| `sched_type` is one of the three | `sched_type "x": not one-shot, cron or periodic` |
| The expression is valid for that type, with the config zone and Hora's clock (`parseSchedule`) | `sched_expression: <err>` |
| `dest_capability` is a visible capability of the destination | `dest_capability "x": not a visible capability of the destination` |
| `capability_parameters` is an object (`null` taken as `{}`) and passes `actio.CheckParams` | `capability_parameters: not an object`, or `capability_parameters: <err>` |

The four rejections a caller can see, and who produces them:

| `rejection` | From |
| --- | --- |
| `unauthorized` | `set`'s shell: a non-coordinator asking for another agent. |
| `agent not found or no visible capabilities for the scheduler` | `set`'s shell: the destination is not in the view, or has nothing callable. |
| `not a scheduling request` | the gate, whatever it is. |
| `invalid answer: <reasons joined by "; ">` | validation, after round 2 failed too. |

A model failure in either round (transport, or a stop reason other than `stop`) is a
reason of its own (`model error: <err>`), so it goes through round 2 and can end as an
`invalid answer` rejection. A DB save failure is never a rejection: it is the
capability error `1002 storage: <err>`, because answering `rejected` would hide an
entry that may in fact have been written.

One defensive line still exists in `set.go` after the save: if `runner.add` refuses the
entry that was just saved, Hora removes it from the DB and answers `invalid answer`.
In practice the DB refuses the same entry first (`store.add` runs the same parse with
the real clock), so the line is unreachable by construction and stays as defence. The
same is true of the `len(view.Capabilities) == 0` half of the step-3 rejection:
discovery drops a capability-less agent from the view entirely. Neither has a test, and
that is deliberate.

### Swapping the gate

A gate is one value of `type gate { name; state; questions; verdict }` in `model.go`,
and `var gates = []gate{gateV1}` is the registry; `decision_gate` picks a name and
`loadConfig` rejects an unknown one. Nothing outside the gate knows its question ids:
`set.go` only sees `gateVerdict{Gate, OK, Hints, Score}`. A gate defined in a `_test.go`
file can be selected by name and drives the whole pipeline with no change to non-test
code — that is how the pipeline's rows stay independent of `v1`.

To compare gates on the built agent: `testdata/gate_corpus.json` holds the labelled
prompts, and the harness in `gate_eval_test.go` (build tag `live`, needs
`HORA_LIVE_URL`) runs every row through the same decision path and prints one line per
row plus a summary — `SUMMARY gate=v1 good=10/10 rejects=6/8 typeRight=10/10
failed=0 meanLatency=…`. That line is why gates can be compared from the diarium of a
running agent, without a harness at all.

## 5. The DB file

The file is one **compact** JSON array — `json.Marshal`, so no newlines and no
indentation; in practice one very long line. Field order comes from the struct tags in
`store.go`, and a test pins it (`TestSaveModeAndFieldOrder`), because a human reads
this file.

```json
[{"datetime":"2026-11-08T18:00:00Z","sched_id":"SCH-1a2b3c4d","sched_type":"cron","sched_expression":"CRON_TZ=Europe/Madrid 0 0 13 * * 0","dest_id":"backup-agent","dest_capability":"backup","capability_parameters":{"kind":"full"}}]
```

- `datetime` is creation time, UTC, RFC 3339 with whole seconds. It is what `list`
  sorts by.
- `sched_id` is `SCH-` plus 8 lowercase hex digits from `crypto/rand`, retried until
  free. An entry that arrives without an id gets one; an entry with an id keeps it, so
  a hand-written id survives.
- `sched_type` and `sched_expression` are always in the **stored form**: an RFC 3339
  UTC time, `@every <whole seconds>`, or a zone-prefixed six-field cron. That is why
  the file is portable and why a prefix-less cron is an error rather than a default.
- `capability_parameters` is an object; a `null` read from the file is taken as `{}`.

### Saving

`saveLocked` never writes in place:

1. If the file exists, open it for writing first: a read-only DB fails there, before
   anything changes, with `db <path>: not writable`.
2. Write the whole array to `<path>.<unix-nanoseconds>.tmp` with `O_EXCL` and mode
   `0600`, `fsync`, close, then `rename` over the DB.

So a crash mid-save leaves the previous file intact, the file is never a half-written
array, and Hora's own file is never world-readable. An empty database is written as
`[]`, never `null`.

### Loading, and what hand-editing does

`openStore` reads the file once at start. A missing file is an empty database. The
rules, in order:

1. Before anything is checked, every `one-shot` whose time is **not after now** is
   dropped (a one-shot at exactly now counts as past), and the file is rewritten if
   anything went. A past one-shot must not fail a start, and it is not fired. A
   one-shot whose text is not RFC 3339 is not cleaned: that is a broken entry, not an
   old one.
2. The file must be a JSON array; each entry is then checked by `checkEntry`, the same
   rules the writer's output obeys — including a duplicate `sched_id`, and the
   cron-prefix rule.
3. Any failure returns an error naming the entry's index and the reason, and the file
   is left byte-for-byte as it was: Hora never silently drops data the user may have
   edited by hand.

That is the whole contract for editing the file by hand: you may edit it, and a broken
entry stops the next start with a message, rather than being ignored.

`store.add` refuses an entry that `loadEntries` would refuse, so the two paths cannot
disagree; and the write happens before the answer, always — both in `set` (save, then
runner add, then `scheduled`) and in `unset` (save, then runner remove, then the
answer), so an answer never arrives before the file agrees with it.

Rows that pin this: `TestSaveIsARename`, `TestSaveFailsOnReadOnlyDB`,
`TestSaveWritesEmptyArray`, `TestAddGeneratesIDAndSaves`, `TestAddKeepsAPresetID`,
`TestAddTakenIDConflicts`, and the load rows (`TestLoadNotAnArrayFailsFileUnchanged`,
`TestLoadBadFieldFailsFileUnchanged`, `TestLoadDuplicateIDFailsFileUnchanged`,
`TestLoadCronWithoutPrefixFailsAndFileUnchanged`, `TestCleanPastOneShotSavesAndStarts`,
`TestLoadOneShotFractionalSecondFails`, `TestAddRefusesAnEntryTheLoadRefuses`).

## 6. Running the tests

```sh
go build ./... && go vet ./... && go vet -tags live ./... && go test -race -count=1 ./...
go test -count=1 -v . | grep -c '^=== RUN'      # 189 rows today
GOMAXPROCS=1 taskset -c 0 go test -count=1 ./...                    # one CPU
go test -race -count=10 -run 'E2E|Runner|Lifecycle' ./...           # the stress set
```

The live suites stay out of that: they need `-tags live` **and** `HORA_LIVE_URL`, and
without it they fail rather than skip on purpose. Run one live copy at a time, and read
its numbers as measurements, not assertions: the gate harness asserts mechanics
(one line per row, a summary, no crash), never model accuracy.

The suite is not quick — about 70 s for the whole thing under `-race`, about 5½ min for
the stress set — because much of it is real clocks and real protocol choreography: the
runner rows use robfig's own timers, the end-to-end rows pay for discovery between real
in-process agents, and the child-process rows compile the binary first. Rows that drive
`run` and the stop shorten `stopBound` and restore it, which is why no lifecycle row
uses `t.Parallel`.
