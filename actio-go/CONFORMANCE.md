# CONFORMANCE — actio-go vs. the Actio protocol v0.1 (core profile)

Traceability matrix: one row per normative statement of *The protocol* (`doc/actio_protocol.md`), with the
implementing file:function and the tests that exercise it. The rows mirror `../actio-ts/CONFORMANCE.md`; the
*Implementation* and *Tests* columns are rewritten for the Go module. Rows outside the core profile are marked
`n/a (security)`.

Conventions: **§** references point into `doc/actio_protocol.md`; "v" references point into the master plan's
decisions (D-x). Tests: `unit:<Test>` = a test in the module's `*_test.go`, `e2e:<Test>` = a test in
`e2e_a_test.go`/`e2e_b_test.go`, `interop:<Test>` = a test in `interop_test.go` (build tag `interop`),
`vectors` = the schema conformance vectors.

## 1. Structure, ownership, announcement

| # | Normative statement (section, quote) | Implementation | Tests |
| --- | --- | --- | --- |
| 1.1 | *Directory structure*: "One folder per agent … The owning agent writes spec.json, status.json and diarium.jsonl" | `agent.go` `startUp()` (folders per `folders`); `io.go` | `e2e:TestE1NormalRequest` (all scenarios read the real layout) |
| 1.2 | "`nuntium` : Mandatory announce folder" | `agent.go` `startUp()` (created at start; `status.go` `publishAnnounce` writes `nuntium/<id>.n.json`) | `unit:TestStatusAnnounce`, `e2e:TestE10CommandsAccepted` (announce) |
| 1.3 | "`forum`: Optional shared folder … Attachments are listed … by their path relative to `forum`" | `requestor.go` `beginOutbound()` (B1 copies the request attachments into `forum`); `receiver.go` `publishAttachment` (the response side) | `e2e:TestE16AttachmentsCleaned`, `unit:TestRequestAttachments` |
| 1.4 | "`tools`, `forum` and `nuntium` are reserved and cannot be used as agent folders" | `discovery.go` `scan()` (RESERVED set, skipped before any stat) | `unit:TestScanReserved` |
| 1.5 | "Each agent creates its own folder and its `inbox`" (announce steps 1/5) | `agent.go` `startUp()` (steps 1–5 in protocol order) | `unit:TestAgentStart`, `e2e:TestE1NormalRequest` |
| 1.6 | "The file names are recommended but not enforced … identified by the `rid` field … the kind of file by its body key" (D-2) | `receiver.go` `processFile()` (`envelopeKind` + rid matching); `validate.go` `envelopeKind` | `unit:TestRecvEnvelopeKind`, `unit:TestValidateEnvelopeKind` |
| 1.7 | "Any agent creates the `.actio`, `nuntium` and `forum` folders if they do not exist when it needs them" | `agent.go` `startUp()` (`makeDir` for all) | `e2e:TestE1NormalRequest` (cold start into an empty root) |
| 1.8 | "`actio.json` is an optional file … agents MAY read it and MUST NOT write it" | not written anywhere in the module (the scan skips the file); `discovery.go` `scan()` (non-directories skipped) | `unit:TestScanSkipsActioJSON` |

## 2. Announcement and Discovery

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 2.1 | Steps 1–6 in order: "folder … spec.json … status.json setting the status … a new line … diarium.jsonl … inbox … announce file" | `agent.go` `startUp()` (the exact order: folders → spec → status+diarium event → announceDelay → announce) | `unit:TestAgentStart`, `e2e:TestE1NormalRequest` (announce file) |
| 2.2 | Rule 2: "status MUST be written first, before the announce file is touched, with a minor delay/sync" | `agent.go` (the re-announce after `setState`); `status.go` `publishAnnounce` | `unit:TestStatusAnnounceOrdering`, `e2e:TestE12StopWithRequestInFlight`/`TestE13RestartWithDelay` (C1 order) |
| 2.3 | "All agents … watch the folder `.actio/nuntium` and, whenever a file in it is created or modified … MUST proceed as follows [the scan]" | `agent.go` (watch on nuntium + `rescan()`, debounced) | `unit:TestAgentRescanOnBroadcast`, `e2e:TestE15DiscoveryCallability` |
| 2.4 | Scan rule 1: "Scan all subdirectories … excluding the reserved tools, forum and nuntium folders" | `discovery.go` `scan()` | `unit:TestScanReserved` |
| 2.5 | Scan rule 2: "inspect if status.json and spec.json files exist. If they don't, ignore the directory" | `discovery.go` `scan()` (`readPublication` → missing = silent) | `unit:TestScanMissingSilent` |
| 2.6 | Scan rule 3: "If status.json or spec.json don't comply with their schemas … ignore the directory" | `discovery.go` `scan()` (`ValidateSpec`/`ValidateStatus` → ignored + reported) | `unit:TestScanSchemaFailures`, `e2e:TestE14MalformedFilesIgnored` |
| 2.7 | Scan rule 4: "Process status.json and spec.json to discover the agent's capabilities and current status" | `discovery.go` `scan()` (returns the parsed pair) | `unit:TestScan` |
| 2.8 | "Any creation or update in the `nuntium` folder triggers a complete scan of all agent folders and updates the internal view" | `agent.go` (`rescan()` → a fresh view) | `unit:TestAgentRescanOnBroadcast`, `e2e:TestE15DiscoveryCallability` |
| 2.9 | The announce file: four mandatory fields (id, datetime, ts, nuntium) | `status.go` `publishAnnounce` (builds + self-validates with `ValidateAnnounce`, I-2) | `unit:TestStatusAnnounce`, `e2e:TestE1NormalRequest` (the announce file validated by `actio validate`) |

## 3. Agent Spec

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 3.1 | "**ID** … up to 32 characters matching `[a-zA-Z0-9_-]+`. **It MUST be unique**" | `specbuilder.go` `buildSpec()` (id from the options; the schema enforces the pattern; duplicate capability names error) | `unit:TestBuildSpec`, `unit:TestScanDuplicate` |
| 3.2 | Namespace rules (absent/null/empty → the null namespace; explicit `null`; `"*"`; the string `"null"` is ordinary) | `discovery.go` `namespacesOf()`/`shareNamespace()`; `receiver.go` `namespacesOf` (A0 code 106) | `unit:TestShareNamespace`, `vectors (spec)`, `e2e:TestE3AcceptanceRejections` (106) |
| 3.3 | "Capabilities … The id MUST be unique for an agent and MUST follow the regex" | `specbuilder.go` `buildSpec()` (our own spec: a duplicate errors) + `discovery.go` `scan()` (a peer's spec: rule 3 — ignored, reported with the name); `validate.go` `uniqueCapabilityNames` is the decision, `ValidateSpec` the schema side | `unit:TestBuildSpecDuplicate`, `unit:TestScanDuplicate`, `vectors (spec)` |
| 3.4 | "`accepts_commands_from` … Mandatory. Only agents in this list can send commands" | `specbuilder.go` (always written); `receiver.go` `acceptCommand` (fresh read, empty = nobody) | `unit:TestRecvCommandAcceptance`, `e2e:TestE10CommandsAccepted`/`TestE11CommandsDropped` |
| 3.5 | "security_support … security_posture … Mandatory" | `specbuilder.go` (core profile: `[]` / `"trusted"`) | `unit:TestBuildSpec` |
| 3.6 | "Info: Additional info … not subject to strict schema definition" | `specbuilder.go` (carried as given) | `unit:TestBuildSpec` |

## 4. Callability rules

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 4.1 | Rule 1: "If agent B's spec has `disabled=true` … Commands can still be sent" | `discovery.go` `callable()` (rule 1: capabilities emptied, commands kept) | `unit:TestCallableRule1`, `e2e:TestE15DiscoveryCallability` |
| 4.2 | Rule 2: "If agents A and B do not share at least one namespace … not considered" | `discovery.go` `shareNamespace()` | `unit:TestShareNamespace` |
| 4.3 | Rules 3+4: the posture/issuer filter | `discovery.go` `callable()` (core: only `trusted` survives) | `unit:TestCallableRule3` |
| 4.4 | Rule 5.1: "If agent A's ID is in agent B's `accepts_commands_from` … `command=true`" | `discovery.go` `callable()` | `unit:TestCallableRule5` |
| 4.5 | Rule 5.2: "Each capability … tested for `disabled=true` and `accepts_from`" | `discovery.go` `capabilityUsable()` | `unit:TestCallableRule5` |
| 4.6 | Rule 6: "If the previous filters show that no commands and no capabilities are usable … discard" | `discovery.go` `callable()` | `unit:TestCallableRule6` |
| 4.7 | Rule 7: "filters out agents with status `inactive` or `error`" | `discovery.go` `callable()` | `unit:TestCallableRule7` |
| 4.8 | Rule 8: "flags, but does not filter out … degraded … zombie" | `discovery.go` `callable()` (`HealthOf` from `status.go` as a flag) | `unit:TestHealthOf`, `e2e:TestE15DiscoveryCallability` |
| 4.9 | "An agent MAY also call itself" | `agent.go` (the own folder is in the view); `requestor.go` (self-calls) | `unit:TestAgentSelfCall`, `e2e:TestE18SelfCall` |

## 5. Agent's status

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 5.1 | Rule 1: "update … status.json with a valid status string and `datetime` with the ISO 8601 date UTC" | `status.go` `buildStatus()`/`publishStatusFileLocked` (`formatISO`) | `unit:TestStatusBuild` |
| 5.2 | Rule 2: "When the status changes between an active state and a non-active state … the announce file MUST be written, as a delayed write after updating status.json" | `agent.go` `setState`/re-announce; `status.go` `publishAnnounce` | `unit:TestStatusAnnounceOrdering`, `e2e:TestE10CommandsAccepted` (announce), `e2e:TestE21WatchLost` |
| 5.3 | Rule 3: "update its status immediately after a state change and at least once every 90 seconds" | `status.go` `startHeartbeat`; `agent.go` (the beat republishes with a fresh `datetime`) | `unit:TestStatusHeartbeat`, `e2e:TestE19HeartbeatKeepsStatusFresh` |
| 5.4 | Rules 4–7: the health thresholds (60/100/600 s) | `status.go` `HealthOf` (the constants) | `unit:TestHealthOf` |
| 5.5 | Rule 8: "If status is `error` or `inactive`, the agent is not required to respond" | `status.go` `HealthOf` (unknown); `receiver.go` `accept` (code 100 for disabled; inactive peers are filtered by callability rule 7) | `unit:TestHealthOf`, `unit:TestCallableRule7`, `e2e:TestE3AcceptanceRejections` (100) |
| 5.6 | `capability_status` (mandatory in status.json): "one object per capability with the number of requests running and in queue" | `agent.go` `statusForLocked` (from the running count; queue always 0, D-7/107); `status.go` `buildStatus` | `unit:TestAgentBusyStatus` |

## 6. Requests, cancellation, response, confirmation

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 6.1 | "An exchange is made of up to four envelope files … Each file has a single writer" | `requestor.go` (`beginOutbound`/`writeCancellation`/`complete`) and `receiver.go` (`writeResponse`/`archive`) | `unit:TestRequest`, `unit:TestRecv`, `e2e:TestE1NormalRequest`–`TestE9ReceiverInactiveAbandons`, `vectors` |
| 6.2 | "All files of an exchange share the same header as the original request; only `datetime` changes" | `receiver.go` (`sameHeader` on can/res/con, D-11); `requestor.go` (D-11 on the captured response too); `validate.go` `sameHeader` | `unit:TestRecvSameHeader`, `unit:TestRequestSameHeader`, `unit:TestValidateSameHeader` |
| 6.3 | "The name of a cancellation is the same as the `name` in the request" | `validate.go` `nameMatches`; `receiver.go` (`applyCancellation`) | `unit:TestValidateNameMatches`, `unit:TestRecvApplyCancellation`, `vectors (cancellation)` |
| 6.4 | The request body: "ts, nonce, signature, name, parameters, timeout, attachments" all mandatory | the schema (`request.schema.json`); `validate.go` `validateRequestFile` | `vectors (request)`, `unit:TestValidateRequest`, `unit:TestRequestSelfValidation` |
| 6.5 | "`parameters`: … If there are no parameters it MUST be null" | `validate.go` (the requestRules if/then); `receiver.go` `accept` (105; `null ≡ {}`) | `unit:TestValidateRequest`, `unit:TestRecvAccept`, `vectors (request)` |
| 6.6 | The cancellation body: "ts, nonce, signature, name, force" | the schema; `requestor.go` `writeCancellation` (self-validated, I-2) | `vectors (cancellation)`, `unit:TestRequestWriteCancellation` |
| 6.7 | The response body: "error … cancellation … result … attachments" + the cancellation-response rules (`error=false`, `result=null`, no attachments) | the schema (the if/then rules); `receiver.go` `writeResponse` (self-validated) | `vectors (response)`, `unit:TestValidateResponse`, `e2e:TestE5CancelledBeforeAcceptance`–`TestE7CancelledAfterResponse` |
| 6.8 | "In case of error, the `result` field MUST follow … `error.schema.json`" | `receiver.go` `run` (ActioError → `{code, msg, details}`; others → `{msg}`); `validate.go` `validateError` | `unit:TestRecvErrorPaths`, `vectors (error)` |
| 6.9 | "Upon arrival of the response … the requestor confirms … by writing the confirmation file" | `requestor.go` `complete` (B3, with the D-13 guard) | `unit:TestRequestConfirm`, `e2e:TestE1NormalRequest` |
| 6.10 | "The requestor SHOULD therefore check that the request still exists before writing the confirmation" | `requestor.go` `complete` (the D-13 `exists` guard) | `unit:TestRequestConfirmGuard`, `e2e:TestE1NormalRequest` |
| 6.11 | Commands: "Commands (`type="cmd"`) only use the request file and cannot be cancelled" | `receiver.go` (no response for cmd; `cancel`/`giveUp` bail on cmd), `requestor.go` (`cancel` on cmd = no-op) | `unit:TestRecvCommandLane`, `unit:TestRequest`, `e2e:TestE10CommandsAccepted`–`TestE11CommandsDropped` |

## 7. Acceptance errors (A0/D0)

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 7.1 | "A rejected request is answered with `error=true` following the *request error schema* … with the `code` of the failed check" | `receiver.go` `accept` + `respond` | `unit:TestRecvAccept` (one test per code), `e2e:TestE2UnknownCapability`/`TestE3AcceptanceRejections` |
| 7.2 | 100 `agent_disabled` ("the receiver's spec.json has `disabled=true`") — Required | `receiver.go` `accept` (fresh `spec().disabled`) | `unit:TestRecvAccept`, `e2e:TestE3AcceptanceRejections` (100) |
| 7.3 | 101 `unknown_capability` — Required | `receiver.go` `accept` | `unit:TestRecvAccept`, `e2e:TestE2UnknownCapability` |
| 7.4 | 102 `capability_disabled` — Required | `receiver.go` `accept` | `unit:TestRecvAccept`, `e2e:TestE3AcceptanceRejections` (102) |
| 7.5 | 103 `requestor_not_allowed` — Required | `receiver.go` `accept` (`accepts_from`) | `unit:TestRecvAccept`, `e2e:TestE3AcceptanceRejections` (103) |
| 7.6 | 105 `invalid_parameters` (the capability `parameters` schema, D-19) | `receiver.go` `accept` (`entry.compiled.validateParams`) | `unit:TestRecvAccept`, `e2e:TestE3AcceptanceRejections` (105) |
| 7.7 | 106 `namespace_mismatch` — against the requestor's spec **as discovered**, before the callability filter (D-7) | `agent.go` (the `discovered` view) + `receiver.go` `peerSpec` (I-11); `receiver.go` `accept` | `unit:TestRecvAccept` (106), `unit:TestAgentDiscoveredPeer`, `e2e:TestE3AcceptanceRejections` (106) |
| 7.8 | 107 `capacity_exceeded` ("at `max_parallel` … reject, no queue") | `receiver.go` `accept` (`entry.running >= max_parallel`) | `unit:TestRecvAccept`, `e2e:TestE3AcceptanceRejections` (107) |
| 7.9 | 108 `request_expired` ("`ts + timeout < now`") | `receiver.go` `accept` | `unit:TestRecvAccept`, `e2e:TestE3AcceptanceRejections` (108), `e2e:TestE17PendingEnvelopesAtStartup` (expired) |
| 7.10 | 104 `version_not_supported` — Optional, **not implemented** (D-7) | absent by design | `unit:TestValidate` (the version is informative; the vectors prove acceptance) |
| 7.11 | 300 `agent_stopping` ("an accepted request that ends without a result … the receiver stops") | `receiver.go` `stop` (code 300 for accepted-unanswered) | `unit:TestRecvStop`, `e2e:TestE12StopWithRequestInFlight` |
| 7.12 | "Files that do not conform to the schemas are ignored … and are never answered" (D-15/D-14) | `receiver.go` `processFile`/`ignore` | `unit:TestRecvIgnore`, `e2e:TestE14MalformedFilesIgnored` |

## 8. Commands

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 8.1 | D0: "src listed in `accepts_commands_from` … commands are processed even if the agent is disabled … `name` is a command the agent knows" | `receiver.go` `acceptCommand` | `unit:TestRecvCommandAcceptance` |
| 8.2 | "stop {force:bool} | Stop the agent. … answered with error code 300" | `receiver.go` (`onCommandStop` after the archive) + `agent.go` `stop` | `unit:TestRecvCommandStop`, `unit:TestAgentStop`, `e2e:TestE12StopWithRequestInFlight` |
| 8.3 | "enable {enabled:bool} … Sets `disabled` in `spec.json` accordingly and re-announces" | `agent.go` `cmdEnable` (sets `disabled` on the built spec; the state survives `ping`, D-10) | `unit:TestAgentEnable`, `e2e:TestE10CommandsAccepted` |
| 8.4 | "restart {force:bool, delay} … start again as fresh … The command is archived before the agent shuts down, and the `delay` starts after the archive" | `receiver.go` (archive first) + `agent.go` `cmdRestart` | `unit:TestRecvCommandRestart`, `unit:TestAgentRestart`, `e2e:TestE13RestartWithDelay` |
| 8.5 | "ping … Update spec and status and re-announce" | `agent.go` `cmdPing` (rebuilds spec + status with fresh datetimes; the enabled state survives, D-10) | `unit:TestAgentPing`, `e2e:TestE10CommandsAccepted` |
| 8.6 | "A command that is not known by the receiver is invalid and is dropped … without any response" (D4: "status.json, spec.json and the announce file are not touched") | `receiver.go` `acceptCommand`/`archiveCommand("dropped")` | `unit:TestRecvCommandDropped`, `e2e:TestE11CommandsDropped` (byte-identical status/spec/nuntium) |
| 8.7 | Custom commands ("any custom command of its own") | `agent.go` `OnCommand`; `receiver.go` (the `commands` map, `CommandContext`) | `unit:TestRecvCommandCustom`, `unit:TestAgentOnCommand`, `e2e:TestE10CommandsAccepted` (`noop`) |

## 9. Diarium

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 9.1 | "Each event … `{id, datetime, event, event_data}` … MUST be a single JSON object per line" | `diarium.go` `append` (validates with `ValidateDiarium` first, then appends) | `unit:TestDiariumAppend`, `vectors (diarium)` |
| 9.2 | The `status`/`announce`/`inreq`/`outreq`/`log` events and their `event_data` | `diarium.go` (the five builders); `receiver.go`/`requestor.go`/`agent.go` (the call sites) | `unit:TestDiariumBuilders`, `e2e:*` (the finals asserted per scenario) |
| 9.3 | The `outreq` timings: "just before writing the confirmation (B3) … For a command, right after writing the command file (B1) … When it gives up without a response (B2)" | `requestor.go` (`complete`/`command`/`abandon`) | `unit:TestRequestOutreq`, `e2e:TestE4ConfirmationTimeout`–`TestE9ReceiverInactiveAbandons` |
| 9.4 | "The envelopes are stored in `inreq` and `outreq` … as they were exchanged" | `receiver.go` `archive` / `requestor.go` `exchangeData` | `unit:TestRequestExchangeData`, `unit:TestRecvArchive` |

## 10. Timers and thresholds

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 10.1 | "Status heartbeat … at least every 90 s, and on every status change" | `agent.go` (the heartbeat at `config.heartbeatInterval`; D-8 on change) | `unit:TestStatusHeartbeat`, `e2e:TestE19HeartbeatKeepsStatusFresh` |
| 10.2 | The health thresholds 60/100/600 s (rules 4–7) | `status.go` `HealthOf` | `unit:TestHealthOf` |
| 10.3 | "Confirmation timeout … 120 s after the response is written" | `receiver.go` (the confirm timer = `config.confirmTimeout`, default 120 s) | `unit:TestRecvConfirmTimeout`, `e2e:TestE4ConfirmationTimeout` |
| 10.4 | "Request `timeout` … 0 = no timeout" (D-6) | `requestor.go` `request` (the timer, B4, the bounded wait, abandon) | `unit:TestRequestTimeout`, `e2e:TestE8RequestorTimeoutCancels` |
| 10.5 | "Wait for a cancellation response … bounded by the requestor's timeout" | `requestor.go` `writeCancellation` + the cancel wait | `unit:TestRequestCancelWait`, `e2e:TestE8RequestorTimeoutCancels` |
| 10.6 | "`restart` `delay` … starts after the command is archived" | `receiver.go` (the archive before the hook) + `agent.go` `cmdRestart` | `unit:TestRecvCommandRestart`, `unit:TestAgentRestart`, `e2e:TestE13RestartWithDelay` |

## 11. Workflow

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 11.1 | B1: "Writes the request file `<rid>.json` … Attachments are copied to `forum`. status.json updated to busy … For `type="cmd"`, append an `outreq` event" | `requestor.go` `beginOutbound`/`command`; `agent.go` (D-8) | `unit:TestRequestBeginOutbound`, `e2e:TestE10CommandsAccepted` |
| 11.2 | B2: "watch … the destination inbox … the watch is removed once the response is captured" (D-18) | `requestor.go` (the watch subscription) | `unit:TestRequestWatch`, `e2e:TestE1NormalRequest` (the concurrent rows) |
| 11.3 | B3: "Just before writing the confirmation, append an `outreq` event … Then write `<rid>.con.json`" (D-13) | `requestor.go` `complete` | `unit:TestRequestConfirm`, `e2e:TestE1NormalRequest` |
| 11.4 | B4: "Write `<rid>.can.json` … the request attachments are not deleted here" | `requestor.go` `writeCancellation` | `unit:TestRequestWriteCancellation`, `e2e:TestE8RequestorTimeoutCancels` |
| 11.5 | B5: "Delete only the request attachments in `forum`" | `requestor.go` `deleteAttachments` | `unit:TestRequestDeleteAttachments`, `e2e:TestE16AttachmentsCleaned` |
| 11.6 | A0: the acceptance checks (7.2–7.9) + "If a cancellation file for the request is already present, no work is started and a cancellation response is written" | `receiver.go` `accept`/`acceptAndRun` | `unit:TestRecvAccept`, `e2e:TestE3AcceptanceRejections`/`TestE5CancelledBeforeAcceptance` |
| 11.7 | A1: "run handler with AbortSignal; status … busy" | `receiver.go` `run`; `agent.go` (D-8) | `unit:TestRecvRun`, `e2e:TestE6CancelledDuringWork` |
| 11.8 | A2: "Writes the response file … Waits for confirmation or confirmation timeout … If a cancellation arrives after the response was written, no second response is written" | `receiver.go` `respond`/`writeResponse` | `unit:TestRecvRespond`, `e2e:TestE7CancelledAfterResponse` |
| 11.9 | A3: "A cancellation file appeared; the work is stopped as gracefully as possible" → the cancellation response | `receiver.go` `run` (the abort path) | `unit:TestRecvAbort`, `e2e:TestE6CancelledDuringWork` |
| 11.10 | A4: "append an `inreq` event … then delete all files of the exchange … and its response attachments" | `receiver.go` `archive` (single-shot per rid, D-5) | `unit:TestRecvArchive`, `e2e:*` (per scenario), `e2e:TestE16AttachmentsCleaned` |
| 11.11 | C1: "status.json MUST be updated to inactive and the announce file written; status first" | `agent.go` `stopInternal` (`setState` → re-announce) | `unit:TestAgentStop`, `e2e:TestE12StopWithRequestInFlight`/`TestE13RestartWithDelay` |
| 11.12 | D0–D4 (the command lifecycle: accepted → executed → archived `completed`; rejected → archived `dropped`, nothing else touched) | `receiver.go` `acceptCommand`/`runCommand`/`archiveCommand` | `unit:TestRecvCommandLane`, `e2e:TestE10CommandsAccepted`/`TestE11CommandsDropped` |
| 11.13 | Pending envelopes at startup (D-4) | `receiver.go` (the watcher's initial scan; the deferral for can/res/con whose request is pending; the bare-res re-track) | `unit:TestRecvPending`, `e2e:TestE17PendingEnvelopesAtStartup` |
| 11.14 | A stop that arrives during `start()` leaves nothing armed; a pending restart leaves exactly one instance (D-9) | `agent.go` (the receiver started last; the `stopping` guard / lifecycle gate) | `unit:TestAgentStopDuringStart`, `unit:TestAgentRestartOnce` |

## 12. Sync, watch, and storage

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 12.1 | "`sync`: Signal the storage layer that files have changed" | `sync.go` (the providers) + `io.go` (the write hooks) | `unit:TestSync`, `unit:TestIO`, `e2e:TestE20SyncProvidersIdentical` |
| 12.2 | "`watch_dir`: Watch a directory non-recursively … report new and modified files … keeping a per-file change marker" | `watch.go` (event-driven; no timers and no polling, §2.1) | `unit:TestWatch` (the scenarios), `unit:TestIO` (the atomic-write loop) |
| 12.3 | "`write_json`: Write a json to file/object as atomically as possible" (the temp-then-rename example) | `io.go` `writeBytesAtomic` (temp → rename, with the sync hooks; a failed rename unlinks the temp) | `unit:TestIOAtomic` |
| 12.4 | "Errors … a bad `<ms>` … is a configuration error at `start()`" (D-21) | `sync.go` `parseSyncProvider`; `agent.go` `start` | `unit:TestSyncParser`, `unit:TestAgentBadSync` |
| 12.5 | E20: the four providers produce identical protocol outcomes | `e2e:TestE20SyncProvidersIdentical` (the identical E1 outcome across providers) | `e2e:TestE20SyncProvidersIdentical` |

## 13. Schemas and strict format

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 13.1 | "Any file or object that does not conform to the protocol schemas is invalid and **ignored** by the receiver: it does not trigger any acceptance test, generates no response and produces no state change" | `receiver.go` `processFile` (validation first, D-15) | `unit:TestRecvIgnore`, `e2e:TestE14MalformedFilesIgnored` |
| 13.2 | "validated … at runtime against the schemas … strict" | `validate.go` (the strict validators; no relaxations) | `unit:TestValidate`, `vectors` |
| 13.3 | "Rules that the schemas cannot express": the same header (D-11), the cancellation `name` match, unique capability names | `validate.go` (`sameHeader`/`nameMatches`/`uniqueCapabilityNames`); the call sites: `receiver.go`/`requestor.go` (header and name), `discovery.go` `scan` (a peer's capability names), `specbuilder.go` `buildSpec` (our own) | `unit:TestValidate`, `unit:TestRecvSameHeader`, `unit:TestRequestSameHeader`, `unit:TestBuildSpecDuplicate`, `unit:TestScanDuplicate` |
| 13.4 | "The `result` of a response that is not an error follows the capability `result` schema … the request `parameters` follow its `parameters` schema" (D-12/D-19) | `validate.go` `compileCapability`; `receiver.go` `accept` (105); `agent.go` `checkResult` (D-12) | `unit:TestValidateCompileCapability`, `unit:TestRecvAccept` (105), `unit:TestAgentCheckResult` |
| 13.5 | "All 11 schemas … as a whole (refs by `$id`)" embedded and strict-clean | `validate.go` (the embedded schemas) | `unit:TestValidateSchemas`, `vectors` |

## 14. Security (out of scope for the core profile)

| # | Normative statement | Implementation | Tests |
| --- | --- | --- | --- |
| 14.1 | *Security features* (sign / encrypt / issuer_sign, the postures untrusted/secret/zero-trust, codes 200–299) | `n/a (security)` — `security_support: []`, posture `trusted`; base64 bodies received are ignored + logged (D-14) | `e2e:TestE14MalformedFilesIgnored` (the base64 body case), `unit:TestRecvIgnore` |
| 14.2 | 104 `version_not_supported` (optional) | `n/a` — not implemented (D-7; the version is informative) | `unit:TestValidate` (the version vectors accept) |
| 14.3 | Queueing at `max_parallel` | `n/a` — rejected with 107, no queue (D-7) | `unit:TestRecvAccept`, `e2e:TestE3AcceptanceRejections` (107) |

## 15. Process rules the implementation adds (from the review history)

| # | Statement (the I-x rows of `README.md`) | Implementation | Tests |
| --- | --- | --- | --- |
| 15.1 | I-2: every file the library writes is self-validated before writing | `status.go`, `diarium.go`, `requestor.go`, `receiver.go` (the inline `validate*` checks) | the unit suites of each writer |
| 15.2 | I-3/I-11: the capability registrations carry their compiled schemas (D-19); the 106 hook reads the DISCOVERED specs | `types.go` (the compiled schemas), `agent.go` (`Capability`), `receiver.go` (`accept` 106) | `unit:TestAgentCapability`, `unit:TestRecvAccept` (106) |
| 15.3 | I-10: one sync provider per process | `io.go` (the provider); `agent.go` `start` | `unit:TestSync` |
| 15.4 | the rules the review rounds settled, stated as the code keeps them: an ignored request file drops the envelopes deferred for its rid (D-15); a failed atomic rename leaves no temp file; `receiver.stop` resolves only once every archive has landed (D-9); the `confirmTimeout` timer is tracked, so `stop` awaits it | `receiver.go` (the deferred-drop, the awaited `stop`, the tracked confirm timer), `io.go` (`writeBytesAtomic`) | `unit:TestRecvDeferredDrop`, `unit:TestRecvStopAwaitsArchive`, `unit:TestIOAtomic` |

## 16. Interop (Go ↔ TypeScript)

| # | Statement | Implementation | Tests |
| --- | --- | --- | --- |
| 16.1 | The Go CLI and the TS CLI drive each other's echo target (echo, fail, private 103, sleep cancelled) over one shared root | `cmd/actio/main.go`, `examples/echo/main.go` (Go); `src/cli.ts`, `examples/echo-agent.ts` (TS) | `interop:TestInterop` (the cross-call rows, both directions) |
| 16.2 | Each CLI validates a file the other implementation wrote | `validate.go` (Go); `src/validate.ts` (TS) | `interop:TestInterop` (the validate rows) |
| 16.3 | Both `ls` list both agents with the same status and health | `discovery.go`/`status.go` (Go); `src/discovery.ts`/`src/status.ts` (TS) | `interop:TestInterop` (the `ls` rows) |
| 16.4 | Each reader reads the other's diarium with no ignored lines | `diarium.go` `ReadDiarium` (Go); `src/diarium.ts` (TS) | `interop:TestInterop` (the cross-diarium rows) |