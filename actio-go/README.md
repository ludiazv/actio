# actio-go

A Go implementation of the **Actio protocol v0.1 — core conformance profile** (`../doc/actio_protocol.md`,
schemas in `../doc/schemas`). It is a port in the exact sense: the TypeScript implementation in `../actio-ts`
is the reference, its decisions D-1…D-23 are kept by number, and where this code does something differently
the difference is in [Differences from `actio-ts`](#differences-from-actio-ts) and has a test.

Everything the protocol requires of an agent is here: the folder contract, the four envelope kinds, the
receiver's lanes, the requestor's wait, discovery and callability, the diarium, the heartbeat, the commands,
the sync providers, and the failure rules. The library has no network dependency: a "peer" is a folder, and a
running agent is a process that reads and writes files in one root.

```
actio-go/
  agent.go receiver.go requestor.go discovery.go watch.go   the runtime
  validate.go specbuilder.go status.go diarium.go sync.go    the contract and the publications
  config.go protocol.go types.go util.go io.go doc.go        configuration, vocabulary, primitives
  cmd/actio/            the CLI (the reference CLI's counterpart)
  examples/echo/        the echo agent: the conformance target
  internal/schema/      a byte-identical copy of ../doc/schemas, embedded (see D-20)
  e2e_a_test.go e2e_b_test.go   scenarios E1–E21, end to end
  interop_test.go       this implementation and actio-ts in one root (build tag `interop`)
  CONFORMANCE.md        every MUST row of the protocol, with its Go code and its test
  orchestra/            the process: plan, briefs, reports, budgets, probes (not part of the module)
```

## Install

The module path is `actio-go` (`go.mod:1`), and it needs **Go 1.26** or newer with the toolchain pinned:

```sh
go build ./...                      # the library, the CLI, the example
go build -o actio ./cmd/actio       # the CLI
go build -o echo ./examples/echo    # the echo agent
```

Direct dependencies, and nothing else is imported:

```
github.com/fsnotify/fsnotify            v1.10.1
github.com/santhosh-tekuri/jsonschema/v6 v6.0.3
```

The test environment is `orchestra/env.sh`: `GOTOOLCHAIN=local` (nothing may switch toolchains mid-build),
a writable `GOPATH`, `GOFLAGS` cleared, `CGO_ENABLED=1` (the race detector needs it). Everything below runs
under that file. The whole module is also green under the released **go1.26.10**:

```
$ GOTOOLCHAIN=go1.26.10 go test -race -count=1 ./...
ok  	actio-go	70.375s
ok  	actio-go/cmd/actio	5.165s
```

## Quick start: two agents in one root

`ACTIO_DIR` is the root every agent and every CLI call shares. Start two echo agents — the binary needs only
its id and the root — and drive them with the CLI, which is itself a requestor.

```sh
export ACTIO_DIR=/tmp/actio-demo
export ACTIO_SYNC=none
./echo &            # ACTIO_AGENT_ID=beta  (env, or --id if you run two copies)
ACTIO_AGENT_ID=alpha ./echo &
```

```
$ actio ls
alpha  idle  healthy  2026-11-07T19:11:45Z  /tmp/wp10-bb/alpha
    echo
    sleep
    fail
    off [disabled]
    private [accepts_from: nobody]
beta  idle  healthy  2026-11-07T19:11:45Z  /tmp/wp10-bb/beta
    echo
    sleep
    fail
    off [disabled]
    private [accepts_from: nobody]
$ echo $?
0

$ actio call alpha beta echo '{"text":"hi"}'
finalStatus: confirmed
result: {"text":"hi"}
$ echo $?
0

$ actio call alpha beta ghost '{}'          # unknown capability
finalStatus: confirmed
error: 101 "unknown_capability: ghost"
$ echo $?
0

$ actio cmd alpha beta noop '{}'            # a command: delivered, never answered
rid: 01M6VPSQ1FR3MBEEZRXJTNWXGB
$ echo $?
0

$ actio broadcast "hello from actio-go"
wrote /tmp/wp10-bb/nuntium/nuntium.json
$ echo $?
0

$ actio validate /tmp/wp10-bb/beta/spec.json
OK spec
$ echo $?
0

$ actio validate /tmp/wp10-bb/beta/diarium.jsonl
OK diarium (5 lines)
$ echo $?
0
```

The unknown-capability row exits **0**: the peer answered, and an answer — even an error answer — is an
outcome, not a CLI failure. Exit 1 from `call` means no response arrived at all. That is the reference
CLI's rule (`actio-ts/src/cli.ts:225`), not a Go invention.

## The API

`doc.go` carries the same text; `go doc actio-go` prints it. The surface is the reference's surface — every
export of `../actio-ts/src/index.ts` has a Go counterpart and a named test; the mapping, row by row, is
`orchestra/handover-api.md`.

Constructing and starting:

```go
cfg := actio.LoadConfig()                 // defaults < ACTIO_* < overrides
cfg.Root = "/tmp/actio-demo"
a, err := actio.New("alpha", cfg, actio.WithCommands(true), actio.WithRequestors("*"))
a.Capability("echo", paramsSchema, resultSchema, echoHandler)
a.OnCommand("noop", noopHandler)
a.OnEvent(func(e actio.Event) { ... })     // log, ignored, status, watch, failure
err = a.Start(ctx)
<-a.Done()
```

Serving is a handler: `func(ctx, params json.RawMessage, cap *actio.CapabilityContext) (any, error)`. The
`ctx` is the exchange's: it is cancelled when a cancellation arrives, when the requestor's timer expires, and
when the agent stops. A returned error becomes an ordinary error answer; a panicking handler is caught and
answered as a failure, never a crash (semantic 3 of §4).

Calling is `Request`, which builds and sends the envelope itself:

```go
out, err := a.Request(ctx, actio.RequestOptions{
    To: "beta", Capability: "echo", Params: json.RawMessage(`{"text":"hi"}`),
    Timeout: 30 * time.Second, Force: false,
})
```

`err` is non-nil only when the protocol exchange could not run (an uncallable peer, a bad capability name, the
local gate). A peer's error answer comes back inside `out` — `out.Response.Error`, `out.FinalStatus`
(`confirmed`, `cancelled`, `completed`, `failed`, `timed_out`, `dropped`) — and is never a Go error. That is
semantic 2, and a consumer program outside this module proves it: `orchestra/consumer/`.

The rest: `Agmen()` and `Refresh()` for the discovery view, `Fail()` and `Stop()`, `Diarium()` and
`ReadDiarium()`, `HealthOf()`, `Scan()` and the eight callability rules, `CheckParams()`, `Call` and
`SendCommand` and `Broadcast` for the CLI-shaped exchanges, `Validate` for the file contract, and the
vocabulary in `protocol.go` (`Code*`, `Final*`, `Event*`, `Kind*`, `Type*`, the file and folder names, the
`Env*` names).

`Config` holds the reference's keys with Go types: `Root`, `Sync`, `HeartbeatInterval`, `ConfirmTimeout`,
`CancelWaitTimeout`, `AnnounceDelay`, `NuntiumDebounce`, `StopGrace`, `Clock func() time.Time`. The
environment names, their units (**seconds**) and their defaults are the reference's:

| env | key | default |
|---|---|---|
| `ACTIO_DIR` | `Root` | `$HOME/.actio` |
| `ACTIO_SYNC` | `Sync` | `none` (`none`, `delay[:ms]`, `fsync+delay[:ms]`, `mutagen:<session>`) |
| `ACTIO_HEARTBEAT` | `HeartbeatInterval` | 30 s |
| `ACTIO_CONFIRM_TIMEOUT` | `ConfirmTimeout` | 120 s |
| `ACTIO_CANCEL_WAIT` | `CancelWaitTimeout` | 60 s |
| `ACTIO_MUTAGEN_BIN` | (the mutagen binary) | `mutagen` |
| — | `AnnounceDelay` / `NuntiumDebounce` / `StopGrace` | 100 ms / 1 s / 30 s |

## Implementation choices

The protocol leaves real decisions open; the reference resolved them as D-1…D-23, and this port keeps those
numbers so the matrices line up (`CONFORMANCE.md`). The rows that are *choices of this implementation*, with
their tests:

- **D-20, no code generator.** `internal/schema/` is a committed byte-identical copy of `../doc/schemas`,
  embedded with `go:embed`; `protocol.go`'s structs are hand-written. Two tests hold the copy: an embed drift
  test that fails if the bytes differ, and a reflection drift test that every schema property has a field with
  that JSON name and vice versa, required properties have no `omitempty`, optional booleans and numbers are
  pointers, and every protocol enum equals its Go constant. Schemas stay authoritative at run time.
- **D-23, one watcher per agent.** One `fsnotify.Watcher`, one goroutine, however many folders are watched
  (`inbox`, `nuntium`, each destination inbox). An event only wakes whoever watches that folder
  (`subscribe(dir) → changed(), lost()`, capacity 1, `changed` sent without blocking); the watcher does no
  file I/O and cannot block a consumer; one mutex guards the folder table and those sends. `scanFolder` is a
  plain function the folder's owner calls after a wake-up. An overflow is a look again, never a loss.
- **D-18, the destination is watched before the request is written**, then the loop looks for its own
  `<rid>.res.json`; there are no per-destination watchers, and a self-call is not a special case.
- **D-9, one run is one object.** An `agentState` is built completely, published through one
  `atomic.Pointer`, and never rewritten; what outlives a run lives in a shared part. The lifecycle has one
  gate (a channel of capacity 1), so a wait for it is bounded by the caller's context; every wait in `Stop`
  and in a failed `Start`'s cleanup is bounded the same way. A heartbeat tick publishes under the publication
  mutex and writes nothing once the state is `inactive`, so it cannot overwrite C1.
- **D-15, an unreadable inbox file is reported with the parser's words**: `malformed JSON: …`, as the
  reference does, and the tests assert the prefix.
- **D-22, a lost inbox is fatal to the agent, not to the loop.** The receiver's loop survives an inbox loss
  until stop, so `performStop` always runs: handlers are cancelled, exchanges settle, and a code-300 answer
  still goes out. A request whose answer cannot be written stays unsettled with one `log` line naming its rid
  — `dropped` belongs to commands alone (protocol, *Final status*: "a request is never dropped").
- **Four mutexes, not five.** The publication mutex, the `OnEvent` dispatch mutex, the outbound-exchange
  count mutex (`requestor.go`), the watcher's folder-table mutex (`watch.go`). No agent-lifecycle mutex: the
  lifecycle is a channel, and `onEvent` queues callbacks instead of holding a mutex across them.

The decisions named above are the ones that changed something. **Every other decision in the plan's table — D-2, D-3,
D-4, D-5, D-6, D-8, D-12, D-13, D-14, D-16, D-17, D-21 — is recorded there as *as in `actio-ts`***: the port copies
the reference's behaviour and nothing in this module re-decides it. The table is `orchestra/master_plan.md` §6 (the
plan is not in git; the handover says where the record lives).

## Differences from `actio-ts`

Each row is a place where this code and the reference behave differently on purpose, and each has a test or a
record. The protocol's own behaviour is not in this list.

| Difference | Why | Where it is pinned |
|---|---|---|
| **A stop command's bound: `StopGrace + 30 s`** (D-9). | The reference's command lane has **no** bound: `drainCommands` (`actio-ts/src/receiver.ts:269-284`) and the stop path after the grace (`actio-ts/src/agent.ts:259-292`) await a handler forever. A handler that never returns leaves a TS agent unable to stop; ours returns `ctx.Err()` and reports. Hardening, not a behaviour change for any conforming handler. | `TestAgentCommandStopBound`, `TestAgentStopBoundedWithStuckTick`, `TestAgentAbortStartBoundedByCtx` |
| **`restart` clamps its delay at 60 s, and a stop or a start cancels the pending restart** (D-10). | The reference sleeps for whatever `delay` says and nothing cancels the sleep, so `delay: 100000` parks an agent and a stop during it is ignored. The clamp (`maxRestartDelay`) and the cancellation are ours. | `TestAgentClampedRestartDelay`, `TestAgentRestartFractionalDelayIsHonoured`, `TestAgentStopDuringRestartDelayLeavesDown` |
| **A broken peer response is reported once per `(path, content hash)`, not `(path, mtime)`** (D-11). | A byte-identical rewrite is not new news. The key only limits repeated reports; nothing is skipped that was not already reported. | `TestRequestIgnoredRewriteReportedAgain`: one report through many looks, a second when the bytes change, still two when the same bytes are re-sent under a new mtime |
| **Capability schemas are RE2.** No lookahead, no backreferences; Ajv's other strict checks (`strictTypes`, `strictRequired`) are not ported (D-19). | Go's `regexp` cannot express those constructs. An own schema using one is a registration error; a peer's schema that fails to compile skips the parameter checks and logs once — the same path Ajv's unknown-keyword failure takes. | `TestCompileCapability`, `TestBadSchemaFailsAtCompile`, `TestNilCapabilitySchemasSkipChecks` |
| **Five exports the reference does not have:** `Call`, `SendCommand`, `Broadcast`, `Validate`, `KindOf`. | Go gives `cmd/actio` no way to reach package internals, and the reference's CLI reaches its own helpers. `KindOf` is the protocol's name→kind table in one place (R2b-3: two copies of that inference produced a bug that lived for five waves). | the parity table `orchestra/handover-api.md`, the CLI tests |
| **`validate` without `--as` decides by the name, as the reference does; our library export decides name-then-bytes.** | The CLI's rule is the reference's, including its last line — `if (base.endsWith(".json")) return "request"` (`actio-ts/src/cli.ts:312`) — so a valid request file named `copy.json` is `OK request` (exit 0) in both, and a valid **response** file under that name is refused by both (the reference refuses it too, because it guesses `request`). `Validate(path, "")`, which TS has no counterpart for, still settles a nameless file by its bytes. | `TestValidateResolvesNameThenBytes`, the seven-row `validate` table in `cmd/actio/main_test.go` |
| **A zero timer in an override means "not set" in Go and "zero" in the reference.** | Go's `Config` cannot distinguish an explicit zero from an unset field; the plan's §15 risk row accepts that because no key needs an explicit zero — nothing in `actio-ts` passes one either. The environment path agrees on both sides (`ACTIO_*` must be a positive number of seconds). | `TestConfigZeroOverrideMeansNotSet`, `TestConfigSetButEmptyTimerIsNotAnOverride` |
| **Local storage must deliver change notifications.** As in the reference: no polling fallback, no re-arm. This is tested on Linux; fsnotify's other backends differ (kqueue opens every file in a watched folder). | D-23 as written. | `TestWatchRealOverflowWakesEverySubscriber`, `TestWatchOverflowWakesEverySubscriber`, the loss tests |

## Testing

```sh
. orchestra/env.sh
bash orchestra/run-checks.sh                    # gofmt, vet, the suite, budgets, §9's greps
go test -race -count=1 ./...
GOMAXPROCS=1 go test -race -count=1 ./...       # the scheduler, one processor
go test -tags interop -race -count=1 -run TestInterop .
```

The e2e scenarios are E1–E21 of the protocol's own scenario list (`e2e_a_test.go`, `e2e_b_test.go`). The
`interop` build tag runs **two implementations in one root** — a TypeScript echo agent under `tsx` and this
one as a binary — and 25 rows: calls both ways, both CLIs listing both agents, each CLI validating every kind
of file the other writes (`spec`, `status`, the announce, the broadcast, the diarium, a command envelope), and
each rejecting garbage. It is behind a tag because it needs `actio-ts` checked out beside this module.

`orchestra/blackbox-go.sh` runs the reference's independent black-box suite
(`../actio-ts/test/blackbox/blackbox.mjs`) against our echo binary with one spawn line changed: envelopes
written by hand, every produced file validated by that suite's own Ajv. 19/19.

Two rules hold every test honest. **A test whose control passes does not count:** for each rule a test is
meant to hold, someone must break the rule in a scratch copy and name the test that fails. **No test may
depend on the scheduler:** every loop in a test has a named bound, and a wait must observe the condition the
assertion checks, not an earlier side effect of the same operation.

## Known limits

- Linux is the tested platform. fsnotify's other backends are not exercised here.
- The protocol's optional check 104 is not implemented, as in the reference (D-7); 105–108 are.
- `GOMAXPROCS=1` and `-race` are part of the standing gate, so a scheduler-sensitive bug is a build failure
  rather than a mystery.
