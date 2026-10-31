# CONFORMANCE — actio-ts vs. the Actio protocol v0.1 (core profile)

Traceability matrix: one row per normative statement of *The protocol* (`doc/actio_protocol.md`), with the implementing file:function and the tests that exercise it. Rows outside the core profile are marked `n/a (security)`.

Conventions: **§** references point into `doc/actio_protocol.md`; "v" references point into the master plan's decisions (D-x). Tests: `unit:*` = `test/unit/<file>.test.ts`, `e2e:*` = `test/e2e/<file>.test.ts`, `vectors` = `test/vectors/` + `test/unit/vectors.test.ts`.

## 1. Structure, ownership, announcement

| # | Normative statement (section, quote) | Implementation | Tests |
| --- | --- | --- | --- |
| 1.1 | *Directory structure*: "One folder per agent … The owning agent writes spec.json, status.json and diarium.jsonl" | `src/agent.ts` `startUp()` (folders per `AgentFolders`); `src/io.ts` | `e2e:requests` (all scenarios read the real layout) |
| 1.2 | "`nuntium` : Mandatory announce folder" | `src/agent.ts` `startUp()` (created at start; `src/status.ts` `announce()` writes `nuntium/<id>.n.json`) | `unit:status` (`announce` tests), `e2e:commands` E10 (announce count) |
| 1.3 | "`forum`: Optional shared folder … Attachments are listed … by their path relative to `forum`" | `src/receiver.ts` `publishAttachment()` (B1 copies into `forum` before the request file); `src/requestor.ts` `beginOutbound()` | `e2e:robustness` E16, `unit:requestor` |
| 1.4 | "`tools`, `forum` and `nuntium` are reserved and cannot be used as agent folders" | `src/discovery.ts` `scan()` (RESERVED set, skipped before any stat) | `unit:discovery` (`scan ignores files and reserved folders…`) |
| 1.5 | "Each agent creates its own folder and its `inbox`" (announce steps 1/5) | `src/agent.ts` `startUp()` (steps 1–5 in protocol order) | `unit:agent` (`start` tests), `e2e:smoke` |
| 1.6 | "The file names are recommended but not enforced … identified by the `rid` field … the kind of file by its body key" (D-2) | `src/receiver.ts` `processFile()` (`envelopeKind` + rid matching); `src/validate.ts` `envelopeKind` | `unit:receiver` (D-2 matching), `unit:validate` (`envelopeKind`) |
| 1.7 | "Any agent creates the `.actio`, `nuntium` and `forum` folders if they do not exist when it needs them" | `src/agent.ts` `startUp()` (`makeDir` for all three) | `e2e:smoke` (cold start into an empty root) |
| 1.8 | "`actio.json` is an optional file … agents MAY read it and MUST NOT write it" | not written anywhere in `src/` (the scan skips the file); `src/discovery.ts` `scan()` (non-directories skipped) | `unit:discovery` (`actio.json` ignored) |

## 2. Announcement and Discovery

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 2.1 | Steps 1–6 in order: "folder … spec.json … status.json setting the status … a new line … diarium.jsonl … inbox … announce file" | `src/agent.ts` `startUp()` (the exact order: folders → spec → status+diarium event → announceDelay → announce) | `unit:agent` (`start`/restart tests), `e2e:smoke` (announce file) |
| 2.2 | Rule 2: "status MUST be written first, before the announce file is touched, with a minor delay/sync" | `src/agent.ts` (`reannounceAfterDelay` after `setStatus`); `src/status.ts` `announce()` | `unit:status` (ordering: status mtime < announce mtime), `e2e:commands` E12/E13 (C1 order) |
| 2.3 | "All agents … watch the folder `.actio/nuntium` and, whenever a file in it is created or modified … MUST proceed as follows [the scan]" | `src/agent.ts` (`watchDir` on nuntium + `scheduleScan()`, D-17 debounced) | `unit:agent` (`a broadcast signal in nuntium triggers a full rescan`), `e2e:robustness` E15 |
| 2.4 | Scan rule 1: "Scan all subdirectories … excluding the reserved tools, forum and nuntium folders" | `src/discovery.ts` `scan()` | `unit:discovery` (reserved names, `actio.json`, dot folders) |
| 2.5 | Scan rule 2: "inspect if status.json and spec.json files exist. If they don't, ignore the directory" | `src/discovery.ts` `scan()` (`readPublication` → missing = silent) | `unit:discovery` (missing = silent) |
| 2.6 | Scan rule 3: "If status.json or spec.json don't comply with their schemas … ignore the directory" | `src/discovery.ts` `scan()` (`validateSpec`/`validateStatus` → ignored + reported) | `unit:discovery` (schema failures reported), `e2e:robustness` E14 |
| 2.7 | Scan rule 4: "Process status.json and spec.json to discover the agent's capabilities and current status" | `src/discovery.ts` `scan()` (returns the parsed pair) | `unit:discovery` |
| 2.8 | "Any creation or update in the `nuntium` folder triggers a complete scan of all agent folders and updates the internal view" | `src/agent.ts` (`scheduleScan()` → `rescan()` → a fresh view) | `unit:agent` (the broadcast-triggered rescan), `e2e:robustness` E15 |
| 2.9 | The announce file: four mandatory fields (id, datetime, ts, nuntium) | `src/status.ts` `announce()` (builds + self-validates with `validateAnnounce`, I-2) | `unit:status`, `e2e:smoke` (the announce file validated by `actio validate`) |

## 3. Agent Spec

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 3.1 | "**ID** … up to 32 characters matching `[a-zA-Z0-9_-]+`. **It MUST be unique**" | `src/spec-builder.ts` `buildSpec()` (id from the options; the schema enforces the pattern; duplicates of capability names throw) | `unit:spec-builder`, `unit:discovery` (D-3) |
| 3.2 | Namespace rules (absent/null/empty → the null namespace; explicit `null`; `"*"`; the string `"null"` is ordinary) | `src/discovery.ts` `namespacesOf()`/`shareNamespace()`; `src/receiver.ts` `namespacesOf()` (A0 code 106) | `unit:discovery` (19 namespace rows × 2 directions), `unit:validate` (spec vectors), `e2e:requests` (the R2-4 106 scenario) |
| 3.3 | "Capabilities … The id MUST be unique for an agent and MUST follow the regex" | `src/spec-builder.ts` `buildSpec()` (duplicates throw); `src/validate.ts` (`validateSpec` + the schema) | `unit:spec-builder`, vectors (`spec`) |
| 3.4 | "`accepts_commands_from` … Mandatory. Only agents in this list can send commands" | `src/spec-builder.ts` (always written); `src/receiver.ts` `acceptCommand()` (fresh read, empty = nobody) | `unit:receiver` (D0 tests), `e2e:commands` E10/E11 |
| 3.5 | "security_support … security_posture … Mandatory" | `src/spec-builder.ts` (core profile: `[]` / `"trusted"`) | `unit:spec-builder` |
| 3.6 | "Info: Additional info … not subject to strict schema definition" | `src/spec-builder.ts` (carried as given) | `unit:spec-builder` |

## 4. Callability rules

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 4.1 | Rule 1: "If agent B's spec has `disabled=true` … Commands can still be sent" | `src/discovery.ts` `callable()` (rule 1: capabilities emptied, commands kept) | `unit:discovery` (rule 1 rows), `e2e:robustness` E15 |
| 4.2 | Rule 2: "If agents A and B do not share at least one namespace … not considered" | `src/discovery.ts` `shareNamespace()` | `unit:discovery` (19 namespace rows × 2) |
| 4.3 | Rules 3+4: the posture/issuer filter | `src/discovery.ts` `callable()` (core: only `trusted` survives) | `unit:discovery` (untrusted/secret/zero-trust discarded) |
| 4.4 | Rule 5.1: "If agent A's ID is in agent B's `accepts_commands_from` … `command=true`" | `src/discovery.ts` `callable()` | `unit:discovery` (5 rows incl. the empty list) |
| 4.5 | Rule 5.2: "Each capability … tested for `disabled=true` and `accepts_from`" | `src/discovery.ts` `capabilityUsable()` | `unit:discovery` (9 rows) |
| 4.6 | Rule 6: "If the previous filters show that no commands and no capabilities are usable … discard" | `src/discovery.ts` `callable()` | `unit:discovery` |
| 4.7 | Rule 7: "filters out agents with status `inactive` or `error`" | `src/discovery.ts` `callable()` | `unit:discovery` (rule 7 precedes rule 8) |
| 4.8 | Rule 8: "flags, but does not filter out … degraded … zombie" | `src/discovery.ts` `callable()` (`health()` from `status.ts` as a flag) | `unit:discovery` (7 ages + the unrateable case) |
| 4.9 | "An agent MAY also call itself" | `src/agent.ts` (the own folder is in the view); `src/requestor.ts` (`notifyFile` for self-calls) | `unit:agent`, `e2e:robustness` E18 |

## 5. Agent's status

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 5.1 | Rule 1: "update … status.json with a valid status string and `datetime` with the ISO 8601 date UTC" | `src/status.ts` `buildStatus()`/`publishStatus()` (`toIso`) | `unit:status` |
| 5.2 | Rule 2: "When the status changes between an active state and a non-active state … the announce file MUST be written, as a delayed write after updating status.json" | `src/agent.ts` `syncBusy()`/`reannounceAfterDelay()`; `src/status.ts` `announce()` | `unit:status` (ordering), `e2e:commands` E10 (announce count), `e2e:robustness` E21 |
| 5.3 | Rule 3: "update its status immediately after a state change and at least once every 90 seconds" | `src/status.ts` `startHeartbeat()`; `src/agent.ts` (`beat()` republishes with a fresh `datetime`) | `unit:status` (heartbeat tests), `unit:agent` (`idle→busy→idle`, the heartbeat test) |
| 5.4 | Rules 4–7: the health thresholds (60/100/600 s) | `src/status.ts` `health()` (the constants; I-6 at exactly 100 s) | `unit:status` (59/60/100/600/601 s + 13 whole-second ages) |
| 5.5 | Rule 8: "If status is `error` or `inactive`, the agent is not required to respond" | `src/status.ts` `health()` (undefined); `src/receiver.ts` `accept()` (code 100 for disabled; inactive peers are filtered by callability rule 7) | `unit:status`, `unit:discovery`, `e2e:requests` E3-100 |
| 5.6 | `capability_status` (mandatory in status.json): "one object per capability with the number of requests running and in queue" | `src/agent.ts` `statusFor()` (from `CapabilityEntry.running`; queue always 0, D-7/107); `src/status.ts` `buildStatus()` | `unit:agent` (`idle→busy→idle` asserts `capability_status.running === 1`) |

## 6. Requests, cancellation, response, confirmation

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 6.1 | "An exchange is made of up to four envelope files … Each file has a single writer" | `src/requestor.ts` (`beginOutbound`/`writeCancellation`/`complete`) and `src/receiver.ts` (`writeResponse`/`archive`) | `unit:requestor`, `unit:receiver`, `e2e:requests` E1–E9, vectors |
| 6.2 | "All files of an exchange share the same header as the original request; only `datetime` changes" | `src/receiver.ts` (`sameHeader` on can/res/con, D-11); `src/requestor.ts` (D-11 on the captured response — R1-6); `src/validate.ts` `sameHeader` | `unit:receiver`, `unit:requestor` (D-11 test), `unit:validate` |
| 6.3 | "The name of a cancellation is the same as the `name` in the request" | `src/validate.ts` `nameMatches`; `src/receiver.ts` (`applyCancellation`) | `unit:validate`, `unit:receiver`, vectors (`cancellation`) |
| 6.4 | The request body: "ts, nonce, signature, name, parameters, timeout, attachments" all mandatory | the schema (`request.schema.json`); `src/validate.ts` `validateRequest` | vectors (`request`), `unit:validate`, `unit:requestor` (I-2 self-validation) |
| 6.5 | "`parameters`: … If there are no parameters it MUST be null" | `src/validate.ts` (the requestRules if/then); `src/receiver.ts` `accept()` (105; `null ≡ {}`) | `unit:validate`, `unit:receiver`, vectors (`request`) |
| 6.6 | The cancellation body: "ts, nonce, signature, name, force" | the schema; `src/requestor.ts` `writeCancellation()` (self-validated, I-2) | vectors (`cancellation`), `unit:requestor` |
| 6.7 | The response body: "error … cancellation … result … attachments" + the cancellation-response rules (`error=false`, `result=null`, no attachments) | the schema (the if/then rules); `src/receiver.ts` `writeResponse()` (self-validated) | vectors (`response`), `unit:validate`, `e2e:requests` E5–E7 |
| 6.8 | "In case of error, the `result` field MUST follow … `error.schema.json`" | `src/receiver.ts` `run()` (ActioError → `{code, msg, details}`; others → `{msg}`); `src/validate.ts` `validateError` | `unit:receiver` (the A0/ActioError paths), vectors (`error`) |
| 6.9 | "Upon arrival of the response … the requestor confirms … by writing the confirmation file" | `src/requestor.ts` `complete()` (B3, with the D-13 guard) | `unit:requestor` (D-13 test), `e2e:requests` E1 |
| 6.10 | "The requestor SHOULD therefore check that the request still exists before writing the confirmation" | `src/requestor.ts` `complete()` (the D-13 `exists` guard) | `unit:requestor` (D-13), `e2e:requests` (the D-13 scenario) |
| 6.11 | Commands: "Commands (`type="cmd"`) only use the request file and cannot be cancelled" | `src/receiver.ts` (no response for cmd; `cancel()`/`giveUp` bail on cmd), `src/requestor.ts` (`cancel` on cmd = no-op) | `unit:receiver` (lane D), `unit:requestor`, `e2e:commands` E10–E11 |

## 7. Acceptance errors (A0/D0)

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 7.1 | "A rejected request is answered with `error=true` following the *request error schema* … with the `code` of the failed check" | `src/receiver.ts` `accept()` + `respond()` | `unit:receiver` (one test per code), `e2e:requests` E2/E3, `e2e:child-process` E2 |
| 7.2 | 100 `agent_disabled` ("the receiver's spec.json has `disabled=true`") — Required | `src/receiver.ts` `accept()` (fresh `spec().disabled`) | `unit:receiver`, `e2e:requests` E3-100, `e2e:commands` E10 (`enable(false)` then 100) |
| 7.3 | 101 `unknown_capability` — Required | `src/receiver.ts` `accept()` | `unit:receiver`, `e2e:requests` E2 (+ the direct-write 101), `e2e:child-process` E2 |
| 7.4 | 102 `capability_disabled` — Required | `src/receiver.ts` `accept()` | `unit:receiver`, `e2e:requests` E3-102 |
| 7.5 | 103 `requestor_not_allowed` — Required | `src/receiver.ts` `accept()` (`accepts_from`) | `unit:receiver`, `e2e:requests` E3-103 |
| 7.6 | 105 `invalid_parameters` (the capability `parameters` schema, D-19) | `src/receiver.ts` `accept()` (`entry.compiled.validateParams`) | `unit:receiver`, `e2e:requests` E3-105 |
| 7.7 | 106 `namespace_mismatch` — against the requestor's spec **as discovered**, before the callability filter (v11) | `src/agent.ts` (`discovered` map) + `ReceiverHooks.peerSpec` (I-11); `src/receiver.ts` `accept()` | `unit:receiver` (the 106 test), `unit:agent` (the R2-4 e2e in `requests.e2e.test.ts`) |
| 7.8 | 107 `capacity_exceeded` ("at `max_parallel` … reject, no queue") | `src/receiver.ts` `accept()` (`entry.running >= max_parallel`) | `unit:receiver`, `e2e:requests` E3-107 |
| 7.9 | 108 `request_expired` ("`ts + timeout < now`") | `src/receiver.ts` `accept()` | `unit:receiver`, `e2e:requests` E3-108 (+ the direct-write row), `e2e:robustness` E17 |
| 7.10 | 104 `version_not_supported` — Optional, **not implemented** (D-7) | absent by design | `unit:validate` (the version is informative; the vectors prove acceptance) |
| 7.11 | 300 `agent_stopping` ("an accepted request that ends without a result … the receiver stops") | `src/receiver.ts` `stop()` (code 300 for accepted-unanswered) | `unit:receiver` (the stop test), `e2e:commands` E12, `e2e:child-process` E12 |
| 7.12 | "Files that do not conform to the schemas are ignored … and are never answered" (D-15/D-14) | `src/receiver.ts` `processFile()`/`ignore()` | `unit:receiver` (D-15/D-14 tests), `e2e:robustness` E14, `e2e:child-process` E14 |

## 8. Commands

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 8.1 | D0: "src listed in `accepts_commands_from` … commands are processed even if the agent is disabled … `name` is a command the agent knows" | `src/receiver.ts` `acceptCommand()` | `unit:receiver` (D0 tests: listed/unlisted/disabled/unknown) |
| 8.2 | "stop {force:bool} | Stop the agent. … answered with error code 300" | `src/receiver.ts` (`onCommandStop` after the archive) + `src/agent.ts` `stop()` | `unit:receiver` (archive-before-hook), `unit:agent`, `e2e:commands` E12 |
| 8.3 | "enable {enabled:bool} … Sets `disabled` in `spec.json` accordingly and re-announces" | `src/agent.ts` `applyEnable()` (rebuilds + sets `disabled` on the built object; R2-1: it survives `ping`) | `unit:agent` (R2-1 test), `e2e:commands` E10, `e2e:requests` (the stale-view 100) |
| 8.4 | "restart {force:bool, delay} … start again as fresh … The command is archived before the agent shuts down, and the `delay` starts after the archive" | `src/receiver.ts` (archive first) + `src/agent.ts` `restart()` | `unit:receiver` (the order test), `unit:agent` (the restart test), `e2e:commands` E13 |
| 8.5 | "ping … Update spec and status and re-announce" | `src/agent.ts` `applyPing()` (rebuilds spec + status with fresh datetimes; R2-1: the enabled state survives) | `unit:agent`, `e2e:commands` E10 |
| 8.6 | "A command that is not known by the receiver is invalid and is dropped … without any response" (D4: "status.json, spec.json and the announce file are not touched") | `src/receiver.ts` `acceptCommand()`/`archiveCommand("dropped")` | `unit:receiver`, `e2e:commands` E11 (byte-identical status/spec/nuntium) |
| 8.7 | Custom commands ("any custom command of its own") | `src/agent.ts` `onCommand()`; `src/receiver.ts` (`commands` map, `CommandContext`) | `unit:receiver`, `unit:agent`, `e2e:commands` E10 (`noop`) |

## 9. Diarium

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 9.1 | "Each event … `{id, datetime, event, event_data}` … MUST be a single JSON object per line" | `src/diarium.ts` `append()` (validates with `validateDiarium` first, then `appendJsonl`) | `unit:diarium`, vectors (`diarium`) |
| 9.2 | The `status`/`announce`/`inreq`/`outreq`/`log` events and their `event_data` | `src/diarium.ts` (the five builders); `src/receiver.ts`/`src/requestor.ts`/`src/agent.ts` (the call sites) | `unit:diarium` (the builders' lines validate), `e2e:*` (the finals asserted per scenario) |
| 9.3 | The `outreq` timings: "just before writing the confirmation (B3) … For a command, right after writing the command file (B1) … When it gives up without a response (B2)" | `src/requestor.ts` (`complete`/`command`/`abandon`) | `unit:requestor` (`outreq` deep-equal to the envelopes), `e2e:requests` E4–E9 |
| 9.4 | "The envelopes are stored in `inreq` and `outreq` … as they were exchanged" | `src/receiver.ts` `archive()` / `src/requestor.ts` `exchangeData()` | `unit:requestor` (deep-equal), `unit:receiver` |

## 10. Timers and thresholds

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 10.1 | "Status heartbeat … at least every 90 s, and on every status change" | `src/agent.ts` (the heartbeat at `config.heartbeatInterval`; D-8 on change) | `unit:status`/`unit:agent` (the freshness tests), `e2e:robustness` E19 |
| 10.2 | The health thresholds 60/100/600 s (rules 4–7) | `src/status.ts` `health()` | `unit:status` (the boundary walk) |
| 10.3 | "Confirmation timeout … 120 s after the response is written" | `src/receiver.ts` (`confirmTimer` = `config.confirmTimeout`, default 120 000) | `unit:receiver`, `e2e:requests` E4 |
| 10.4 | "Request `timeout` … 0 = no timeout" (D-6) | `src/requestor.ts` `request()` (the timer, B4, the bounded wait, abandon) | `unit:requestor`, `e2e:requests` E8 |
| 10.5 | "Wait for a cancellation response … bounded by the requestor's timeout" | `src/requestor.ts` `writeCancellation()` + `cancelWaitTimeout` | `unit:requestor`, `e2e:requests` E8 |
| 10.6 | "`restart` `delay` … starts after the command is archived" | `src/receiver.ts` (the archive before the hook) + `src/agent.ts` `restart()` | `unit:receiver`, `unit:agent`, `e2e:commands` E13 |

## 11. Workflow

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 11.1 | B1: "Writes the request file `<rid>.json` … Attachments are copied to `forum`. status.json updated to busy … For `type="cmd"`, append an `outreq` event" | `src/requestor.ts` `beginOutbound()`/`command()`; `src/agent.ts` (D-8) | `unit:requestor`, `e2e:commands` E10, `e2e:requests` (the D-8 rows) |
| 11.2 | B2: "watch … the destination inbox … the watch is removed once the response is captured" (D-18) | `src/requestor.ts` `acquireWatcher()`/`releaseWatcher()` | `unit:requestor` (the refcount tests), `e2e:requests` (the two-concurrent test) |
| 11.3 | B3: "Just before writing the confirmation, append an `outreq` event … Then write `<rid>.con.json`" (D-13) | `src/requestor.ts` `complete()` | `unit:requestor` (the D-13 test + the deep-equal), `e2e:requests` E1 |
| 11.4 | B4: "Write `<rid>.can.json` … the request attachments are not deleted here" | `src/requestor.ts` `writeCancellation()` | `unit:requestor`, `e2e:requests` E8 |
| 11.5 | B5: "Delete only the request attachments in `forum`" | `src/requestor.ts` `deleteAttachments()` | `unit:requestor`, `e2e:robustness` E16 |
| 11.6 | A0: the acceptance checks (7.2–7.9) + "If a cancellation file for the request is already present, no work is started and a cancellation response is written" | `src/receiver.ts` `accept()`/`acceptAndRun()` | `unit:receiver`, `e2e:requests` E3/E5 |
| 11.7 | A1: "run handler with AbortSignal; status … busy" | `src/receiver.ts` `run()`; `src/agent.ts` (D-8) | `unit:receiver`, `e2e:requests` E6, `e2e:requests` (the D-8 rows) |
| 11.8 | A2: "Writes the response file … Waits for confirmation or confirmation timeout … If a cancellation arrives after the response was written, no second response is written" | `src/receiver.ts` `respond()`/`writeResponse()` | `unit:receiver`, `e2e:requests` E7 |
| 11.9 | A3: "A cancellation file appeared; the work is stopped as gracefully as possible" → the cancellation response | `src/receiver.ts` `run()` (the abort path) | `unit:receiver`, `e2e:requests` E6 |
| 11.10 | A4: "append an `inreq` event … then delete all files of the exchange … and its response attachments" | `src/receiver.ts` `archive()` (single-shot, P-2) | `unit:receiver`, `e2e:requests` (per scenario), `e2e:robustness` E16 |
| 11.11 | C1: "status.json MUST be updated to inactive and the announce file written; status first" | `src/agent.ts` `stop()` (`setStatus` → `reannounceAfterDelay`) | `unit:agent`, `e2e:commands` E12/E13, `e2e:smoke` |
| 11.12 | D0–D4 (the command lifecycle: accepted → executed → archived `completed`; rejected → archived `dropped`, nothing else touched) | `src/receiver.ts` `acceptCommand()`/`runCommand()`/`archiveCommand()` | `unit:receiver` (the lane-D tests), `e2e:commands` E10/E11 |
| 11.13 | Pending envelopes at startup (D-4) | `src/receiver.ts` (the watcher's initial scan; the deferral for can/res/con whose request is pending; the bare-res re-track) | `unit:receiver` (the D-4 tests), `e2e:robustness` E17 |
| 11.14 | A stop that arrives during `start()` leaves nothing armed; a pending restart leaves exactly one instance (D-9 v11) | `src/agent.ts` (`receiver.start()` last; the `stopping` guard) | `unit:agent` (the R2-3 tests), oracle probe P3 |

## 12. Sync, watch, and storage

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 12.1 | "`sync`: Signal the storage layer that files have changed" | `src/sync.ts` (the four providers) + `src/io.ts` (the write hooks) | `unit:sync`, `unit:io`, `e2e:robustness` E20 |
| 12.2 | "`watch_dir`: Watch a directory non-recursively … report new and modified files … keeping a per-file change marker" | `src/watch.ts` `watchDir()`/`diffMarkers()` (event-driven; no timers/polling — R1-11/§2.1) | `unit:watch` (13 scenarios), `unit:io` (the atomic-write loop) |
| 12.3 | "`write_json`: Write a json to file/object as atomically as possible" (the temp-then-rename example) | `src/io.ts` `writeJsonAtomic()` (temp `<tmpDir>/.<name>.<nonce>.tmp` → rename, with the sync hooks) | `unit:io` (the atomicity loop + the P-4 pin test) |
| 12.4 | "Errors … a bad `<ms>` … is a configuration error at `start()`" (D-21) | `src/sync.ts` `createSyncProvider()`; `src/agent.ts` `start()` | `unit:sync` (the parser/rejection tables), `unit:agent` (the bad-sync test) |
| 12.5 | E20: the four providers produce identical protocol outcomes | `e2e:robustness` E20 (the deep-equal across providers) | `e2e:robustness` E20 |

## 13. Schemas and strict format

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 13.1 | "Any file or object that does not conform to the protocol schemas is invalid and **ignored** by the receiver: it does not trigger any acceptance test, generates no response and produces no state change" | `src/receiver.ts` `processFile()` (validation first, D-15) | `unit:receiver`, `e2e:robustness` E14, `e2e:child-process` E14 |
| 13.2 | "validated with Ajv at runtime against the schemas embedded by the generator … Ajv strict" | `src/validate.ts` (the lazy strict singletons; no relaxations) | `unit:validate`, vectors |
| 13.3 | "Rules that the schemas cannot express": the same header (D-11), the cancellation `name` match, unique capability names | `src/validate.ts` (`sameHeader`/`nameMatches`/`uniqueCapabilityNames`) + the call sites | `unit:validate`, `unit:receiver`, `unit:requestor`, `unit:spec-builder` |
| 13.4 | "The `result` of a response that is not an error follows the capability `result` schema … the request `parameters` follow its `parameters` schema" (D-12/D-19) | `src/validate.ts` `compileCapability()`; `src/receiver.ts` `accept()` (105); `src/agent.ts` `checkResult()` (D-12) | `unit:validate` (compileCapability), `unit:receiver` (105), `unit:agent` (the D-12 test) |
| 13.5 | "All 11 schemas … as a whole (refs by `$id`)" embedded and strict-clean | `src/generated/schemas.ts` + `scripts/gen-schemas.ts` (the strict gate) | `npm run check` (the gen gate), vectors |

## 14. Security (out of scope for the core profile)

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 14.1 | *Security features* (sign / encrypt / issuer_sign, the postures untrusted/secret/zero-trust, codes 200–299) | `n/a (security)` — `security_support: []`, posture `trusted`; base64 bodies received are ignored + logged (D-14) | `e2e:robustness` E14 (the base64 body case), `unit:receiver` |
| 14.2 | 104 `version_not_supported` (optional) | `n/a` — not implemented (D-7; the version is informative) | `unit:validate` (the version vectors accept) |
| 14.3 | Queueing at `max_parallel` | `n/a` — rejected with 107, no queue (D-7) | `unit:receiver`, `e2e:requests` E3-107 |

## 15. Process rules the implementation adds (from the review history)

| # | Statement (issues.md) | Implementation | Tests |
| --- | --- | --- | --- |
| 15.1 | I-2: every file the library writes is self-validated before writing | `src/status.ts`, `src/diarium.ts`, `src/requestor.ts`, `src/receiver.ts` (the `mustValidate`/inline checks) | the unit suites of each writer |
| 15.2 | I-3/I-11: the capability registrations carry their compiled schemas (D-19); the 106 hook reads the DISCOVERED specs | `src/types.ts` (`CapabilityEntry.compiled`), `src/agent.ts` (`capability()`), `src/receiver.ts` (`accept()` 106) | `unit:agent` (the registration tests), `unit:receiver` (106) |
| 15.3 | I-10: one sync provider per process | `src/io.ts` `setSyncProvider()`; `src/agent.ts` `start()` | `unit:io` (the provider tests) |
| 15.4 | P-3/P-4/P-5/P-6 (the carry-over fixes) | `src/receiver.ts` (`dropDeferredFor`, `startArchive`, the awaited `stop()`), `src/io.ts` (the P-4 pin test) | `unit:receiver` (the P-3/P-5 tests), `unit:io` (the P-4 test) |
