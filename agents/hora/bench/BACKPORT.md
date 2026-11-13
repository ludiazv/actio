# Backport analysis: bench (v2) → implementation

Status: **analysis only — no implementation changes made.**
Bench space: `bench/` (was `tmp/`), gitignored. Harness: `bench/ar` (`bench/main.go`),
candidate gate: `bench/model.go`, corpora: `bench/corpus_*.json`, logs: `bench/*.log`.

## 1. What the bench validated (final design "v2")

Pipeline: gate (d1-2b, System One) → extraction (2 rounds, conversation repair) → validate → store.

Gate (d1-2b), 30-case corpus:
- 30/30 verdicts, 14/14 types, 0 false rejections, avg 179 ms
- 14 valid accepted, 12 rejected by gate, 4 accepted by gate but rejected by extraction
- The deterministic named-capability check closes the 3 historic leaks
  (#12 "report generator", #17 "Do something every hour", #25 "haz algo cada hora")

Extraction, 30-case corpus:
- agents-a1-4b: 17/17 final, 14/14 types, 34m17s wall
- sanson/lfm3-1.7b: 17/17 final, 14/14 types, 7m59s wall (4.3x faster);
  4 cron rows fail round 1 and are repaired by round 2 (conversation context)
- Rejection path proven: "call echo every 500 milliseconds" fails both rounds
  on both models → rejected with reasons
- Prompt evolution measured (6-case A/B, then full runs):
  v0 shipped 42m56s → v1 (role+split+examples, no destination) 19m45s → v2 34m17s/7m59s
  (v2 runs include 4 extra hard rows and 2-round rejects)

## 2. Design summary (what gets ported)

Gate v1 (model.go):
1. State: single-field JSON `{"request": <markdown text>}` where the text is
   `User is requesting scheduling with this request:\n"<prompt>"\nAvailable capabilities are:\n- name: desc`
2. Questions: `valid` (bool), `type` (choice), `capability` (choice, only ≥2 caps) — unchanged.
3. Verdict:
   - P(valid) < 0.5 → reject (unchanged)
   - NEW: ≥2 caps and no capability name (case-insensitive substring) appears in the
     prompt → reject
   - hints: type from answer; capability = answer (≥2 caps) or the single cap name

Extraction (set.go + model.go):
1. 0 capabilities → pre-reject (already in production `set()`).
2. NEW prompt split:
   - system (static, cacheable): role line + JSON contract + 3 mode rules +
     capability rules + 3 one-line examples.
     - ≥2 caps: 4-key contract (sched_type, sched_expression, dest_capability, capability_parameters)
     - 1 cap: 3-key contract (no dest_capability)
   - user (per request): current time (UTC + config zone), structured capability
     list (`- name: desc. parameters: <schema>`), decision hints (type only for 1 cap),
     the request verbatim last.
   - The destination id/name is NOT in the prompt (removes the
     `dest_capability: "<dest>"` failure mode).
3. 1 cap: harness injects `dest_capability` into the reply before validation.
4. NEW `completeChat(system, msgs)`: round 2 continues the conversation:
   `[user1, assistant1 (model's own reply), user2 ("Your previous reply failed validation: <reasons>. Reply again…")]`
   (production today: fresh single-turn call with the reply quoted as text)
5. Validation: unchanged in production (already strict: parseSchedule + actio.CheckParams).
6. Reject after round 2 with the collected reasons (unchanged).

## 3. File-by-file changes

### model.go
- ADD `capInfo` struct `{Name, Description string}`.
- `gate` struct: change
  - `state func(prompt, capContext string) ai.JsonObject` → `state func(prompt string, caps []capInfo) ai.JsonObject`
  - `verdict func(a ai.ClassifierAnswers, capNames []string) (gateVerdict, error)` → `verdict func(a ai.ClassifierAnswers, prompt string, capNames []string) (gateVerdict, error)`
  - `questions` unchanged (names only).
- `decide`: signature `(ctx, prompt string, caps []capInfo)`; builds
  `capNames` internally for questions/verdict; state = `ai.JsonObject{"request": v1StateText(prompt, caps)}`.
- `v1State`: replace 3-field object state with the markdown-text state
  (single field). Keep the name `v1State` returning `ai.JsonObject`, with a
  `v1StateText` helper if preferred.
- `v1Questions`: no change.
- `v1Verdict`: add the named-capability check (needs the prompt — new param).
- `complete`: becomes a thin wrapper over NEW `completeChat(ctx, system, msgs []ai.Message)`
  (same CompleteSimple call, full message history).
- `v1ValidCutOff` (0.5): unchanged. `gateV1` registration: unchanged.
- `replyText`, `isJSONObject`, `stripAuth`, `newModels`, `hasDecision`: unchanged.

### set.go
- `scheduleFor`:
  - build `caps []capInfo` (sorted) from `view.Capabilities` (name + description);
    drop `capNames`/`capCtx` locals.
  - `h.models.decide(ctx, prompt, caps)` (new signature).
  - replace `system := h.systemPrompt(capCtx, hints)` + `runRound` with:
    - `system, user := h.extractPrompts(prompt, caps, hints)`
    - round 1 via `h.models.complete(ctx, system, user)`
    - round 2 via `h.models.completeChat(ctx, system, []ai.Message{user1, assistant1, user2})`
      with `user2 = "Your previous reply failed validation: <reasons>. Reply again with the corrected JSON object only."`
  - 1-cap injection: after each round's reply, if `len(caps) == 1`, set
    `dest_capability` to the single name before `validateReply` (JSON merge,
    as bench `injectDestCap`).
- REPLACE `systemPrompt(capCtx, hints)` with `extractSystem(caps []capInfo) string`
  (static: role + contract + rules + examples; 4-key or 3-key by cap count)
  and `extractUser(prompt string, caps []capInfo, hints hints) string`
  (time, capability list with schemas from `view.Capabilities[n].Parameters`,
  hints, request).
- REMOVE `destCapContext` (only consumer was the old prompt/state).
- `validateReply`: unchanged.
- `set()`: unchanged (0-cap pre-reject already present).
- Steps 8–9 (store/runner): unchanged.

### config.go
- No changes. `decision_gate: v1` stays; d1-2b already used by tests.

### testdata/gate_corpus.json
- Already: +8 Spanish rows (uncommitted change).
- Optional: +4 hard rows ("report generator", "Do something every hour",
  "haz algo cada hora" are already in the 26; the 4 bench-only rows are the
  past-date ones + "every 500 milliseconds"). For the GATE corpus they are
  `want: accept` (gate lets them through; extraction must reject — that is an
  e2e concern, not a gate corpus concern). Recommend adding them with
  `want: accept` + the expected type.

### Tests (will break, need updating)
- `model_test.go`: `v1State` (old 3-field object → new single-field text),
  `v1Verdict` (new prompt param + name-check cases), `decide` signature.
- `set_test.go`: `systemPrompt`/`destCapContext` removed → update prompt-shape
  assertions; round-2 now multi-message (mock/fake model must handle a second
  call with 3 messages); 1-cap injection.
- `gate_eval_test.go`: live eval — `decide` signature change; expectations
  should now include the 3 named-capability rejects.
- `e2e_test.go` / `live_test.go`: behavior-level; verify, likely no change.
- `config_test.go`, `store_test.go`, `sched*`: unaffected.

## 4. Decisions needed (not made)

1. Extraction model: keep `agents-a1-4b` (robust r1, 20–330s/call) or switch to
   `sanson/lfm3-1.7b` (same final quality, 4–57s/call, needs round 2 on ~4/14
   cron rows). This is a deployment/config change (`model` field), not code.
2. Past-date policy: round 2 currently reinterprets past one-shots into future
   times (3 of 4 bench cases get scheduled). Options:
   a. accept the reinterpretation (current bench behavior), or
   b. make `sched_expression: not after now` a terminal reason (skip round 2)
      → past dates are rejected. One-line rule in `scheduleFor`.

## 5. Rollout order (when approved)

1. `model.go`: capInfo, decide/gate signatures, v1State text, v1Verdict name check, completeChat.
2. `set.go`: extractSystem/extractUser, 1-cap injection, round-2 conversation, remove destCapContext.
3. Update model_test.go, set_test.go, gate_eval_test.go.
4. Extend testdata/gate_corpus.json (Spanish rows already added; +4 hard rows optional).
5. `go test ./...` (unit) → live eval (`gate_eval_test.go`) → e2e.
6. Deployment: pick extraction model (decision 1).

## 6. Risks / trade-offs (documented, accepted in bench)

- Named-capability substring check: a multi-cap request referring to a
  capability only by description (no name) is rejected. All 14 valid corpus
  rows name their capability; keep such rows in the corpus as it grows.
- Substring false positives possible (e.g. cap "scan" inside "rescan");
  accepted.
- Examples use capability names (scan/backup/echo); the contract line
  "must be exactly one of the capability names listed" mitigates copying;
  no observed copy-to-wrong-cap in 30-case runs.
- Static system prompt enables server-side prefix caching if the server
  supports it (bonus, no code needed).