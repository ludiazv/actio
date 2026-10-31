// E2E — robustness, discovery, sync and watch: scenarios E14–E21 (plan §7).
//
// Same shape as `requests.e2e.test.ts`, with its own copy of the harness (the brief asks for the
// patterns, not that file's internals): real temp `.actio` roots, `ActioAgent`s in-process, and
// `fastConfig()` timers. Every scenario with an exchange asserts the §7 recipe:
//   (a) the order of the files in the receiver inbox, with `recordInbox`,
//   (b) the inbox empty after the archive,
//   (c) every diarium line of every agent validates, plus the `inreq`/`outreq` final statuses,
//   (d) attachments cleaned per the *Envelope lifecycle*.
// Where a scenario deliberately leaves files or folders behind, that exception is asserted
// explicitly: E14's three ignored envelopes stay in the inbox untouched, E21's deleted folders are
// gone and stay gone.
//
// Scenario notes worth reading before the assertions:
// - E14: an ENCRYPTED envelope is protocol-VALID (`body` may be a Base64 string), so its absence
//   from the archive is D-14 (no decryption), not D-15; the reason in the report tells them apart.
//   The two folders with a broken `spec.json`/`status.json` are reported by the discovery scan, not
//   by the receiver, and a reported folder never becomes a peer.
// - E15: peers are publications on disk (`spec.json` + `status.json`), not running agents: the
//   callability rules read files only, so a peer needs no inbox. Every fixture is validated before
//   it is written, so a peer can be missing from `agmen()` only because of a callability rule. The
//   rescan comes from a broadcast file in `nuntium/` (D-17), which also covers the folders that
//   appeared with no announce at all.
// - E17: a cancellation pending BESIDE its request is matched by rid and the exchange ends
//   `cancelled`, but the recorder cannot prove that the `can` file was read first: both were already
//   there, and the watcher's first listing is `readdir` order, not lexical order.
// - E19: `toIso` has the protocol's SECOND resolution, so freshness is measured twice: the file's
//   mtime (exact: the heartbeat's cadence) and its `datetime` (allowing up to 1 s of truncation).
// - E20: `mutagen` runs through a fake binary on `ACTIO_MUTAGEN_BIN` (the pattern of
//   `test/unit/sync.test.ts`); its log proves the provider really executed the flushes.
// - E21: losing the watch of our OWN inbox is fatal (D-22: `error`, and the heartbeat never
//   recovers it); losing the watch of a DESTINATION folder in B2 only abandons that exchange.
import assert from "node:assert/strict";
import { chmod, mkdir, mkdtemp, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test, type TestContext } from "node:test";
import { ActioAgent } from "../../src/agent.js";
import * as io from "../../src/io.js";
import { nonce, sleep, toIso, ulid } from "../../src/util.js";
import {
  validateCancellation, validateDiarium, validateRequest, validateResponse, validateSpec, validateStatus,
} from "../../src/validate.js";
import { fastConfig, recordInbox, waitFor } from "../helpers/index.js";
import type { ActioConfig, ActioEvent, AgentOptions, CapabilityHandler } from "../../src/types.js";
import type {
  Announce,
  CancellationFile,
  Capability,
  DiariumLine,
  Exchange,
  RequestBody,
  RequestFile,
  ResponseBody,
  ResponseFile,
  Spec,
  Status,
} from "../../src/generated/types.js";

/** A stuck exchange or a missing watch event fails its test instead of hanging the suite. */
const opts = { timeout: 40_000 };

const OK_RESULT = { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"] } as const;

const ECHO: Capability = {
  name: "echo",
  description: "echoes the text it gets (Appendix A.1)",
  parameters: { type: "object", properties: { text: { type: "string" } }, required: ["text"], additionalProperties: false },
  actio: { result: { type: "object", properties: { text: { type: "string" } }, required: ["text"] } },
};

/** E16: reads its request attachments, publishes one of its own, then waits for the test. */
const GIVE: Capability = {
  name: "give",
  description: "reads the request attachments and publishes a response attachment",
  parameters: { type: "object", properties: {}, additionalProperties: false },
  actio: {
    result: {
      type: "object",
      properties: { saw: { type: "array", items: { type: "string" } }, published: { type: "string" } },
      required: ["saw", "published"],
    },
  },
};

/** E21: work that is still running when the destination folder goes away. */
const HOLD: Capability = {
  name: "hold",
  description: "waits until the test releases it",
  parameters: { type: "object", properties: {}, additionalProperties: false },
  actio: { result: { ...OK_RESULT } },
};

/** A capability of a fixture peer spec (E15): callability reads published specs, it never calls them. */
const peerCap = (name: string, over: Partial<NonNullable<Capability["actio"]>> = {}): Capability => ({
  name,
  description: `the ${name} capability of a fixture peer`,
  parameters: { type: "object", properties: {}, additionalProperties: false },
  actio: { result: { ...OK_RESULT }, ...over },
});

const echoHandler: CapabilityHandler = async (params) => ({ text: `echo: ${(params as { text: string }).text}` });

// --- the harness (the WP9-A/B patterns; copied, so each e2e file stands on its own) ---------------

interface Harness {
  root: string;
  agents: ActioAgent[];
}

function makeAgent(
  h: Harness,
  id: string,
  over: Partial<AgentOptions> = {},
  timers: Partial<ActioConfig> = {},
): { agent: ActioAgent; events: ActioEvent[] } {
  const events: ActioEvent[] = [];
  const agent = new ActioAgent({
    id,
    description: `${id} e2e robustness agent`,
    acceptsCommandsFrom: [],
    ...over,
    config: { ...fastConfig(h.root, Date.now), ...timers },
  });
  agent.on("event", (e) => events.push(e));
  h.agents.push(agent);
  return { agent, events };
}

async function harness(t: TestContext): Promise<Harness> {
  const root = await mkdtemp(path.join(os.tmpdir(), ".actio-e2e-"));
  const h: Harness = { root, agents: [] };
  t.after(async () => {
    // E21 deletes folders an agent may still have to archive into: a stop that fails during cleanup
    // is the scenario's own doing, and that scenario asserted its outcome already.
    for (const a of h.agents) await a.stop().catch(() => undefined);
    await rm(root, { recursive: true, force: true });
  });
  return h;
}

interface Pair {
  a: ActioAgent;
  b: ActioAgent;
  ea: ActioEvent[];
  eb: ActioEvent[];
  /** The forum listing the `give` handler took just before it waited for the test. */
  giveForum(): string[];
  releaseGive(): void;
  releaseHold(): void;
}

/** alpha (requestor side) and beta (receiver side); both serve echo, give and hold. */
async function pair(
  h: Harness,
  over: { a?: Partial<AgentOptions>; b?: Partial<AgentOptions>; timers?: Partial<ActioConfig> } = {},
): Promise<Pair> {
  let giveSnapshot: string[] = [];
  let releaseGive!: () => void;
  const giveGate = new Promise<void>((resolve) => {
    releaseGive = resolve;
  });
  let releaseHold!: () => void;
  const holdGate = new Promise<void>((resolve) => {
    releaseHold = resolve;
  });
  const giveHandler: CapabilityHandler = async (_params, ctx) => {
    const saw: string[] = [];
    for (const name of ctx.attachments) saw.push(await readFile(path.join(h.root, "forum", name), "utf8"));
    const published = await ctx.publishAttachment("out.bin", new TextEncoder().encode("the response attachment"));
    giveSnapshot = clean(await io.listDir(path.join(h.root, "forum")));
    await giveGate; // the test inspects the forum while the exchange is in flight
    return { saw, published };
  };
  const holdHandler: CapabilityHandler = async () => {
    await holdGate;
    return { ok: true };
  };
  const a = makeAgent(h, "alpha", { acceptsCommandsFrom: ["beta"], ...over.a }, over.timers);
  const b = makeAgent(h, "beta", { acceptsCommandsFrom: ["alpha"], ...over.b }, over.timers);
  for (const { agent } of [a, b]) {
    agent.capability(ECHO, echoHandler);
    agent.capability(GIVE, giveHandler);
    agent.capability(HOLD, holdHandler);
  }
  await b.agent.start(); // beta publishes first, so alpha's first discovery sees it
  await a.agent.start();
  await waitFor(() => a.agent.agmen().has("beta"), 6_000);
  return {
    a: a.agent, b: b.agent, ea: a.events, eb: b.events,
    giveForum: () => giveSnapshot, releaseGive, releaseHold,
  };
}

// --- the §7 recipe, on real files ----------------------------------------------------------------

/** Directory names without the atomic-write temp files (`.<name>.<nonce>.tmp`), in lexical order. */
const clean = (names: string[]): string[] => names.filter((n) => !n.startsWith(".")).sort();

const inboxNames = (root: string, id: string): Promise<string[]> => io.listDir(path.join(root, id, "inbox"));
const forumNames = (root: string): Promise<string[]> => io.listDir(path.join(root, "forum"));

async function statusOf(root: string, id: string): Promise<Status> {
  return JSON.parse(await readFile(path.join(root, id, "status.json"), "utf8")) as Status;
}

async function announceOf(root: string, id: string): Promise<Announce> {
  return JSON.parse(await readFile(path.join(root, "nuntium", `${id}.n.json`), "utf8")) as Announce;
}

async function diaries(root: string, id: string): Promise<DiariumLine[]> {
  const file = path.join(root, id, "diarium.jsonl");
  if (!(await io.exists(file))) return [];
  return (await readFile(file, "utf8"))
    .split("\n")
    .filter((l) => l !== "")
    .map((l) => JSON.parse(l) as DiariumLine);
}

/** (c): every line of every listed agent must pass `validateDiarium`. */
async function assertDiariaValid(root: string, ids: string[]): Promise<void> {
  for (const id of ids) {
    for (const line of await diaries(root, id)) {
      const checked = validateDiarium(line);
      assert.ok(checked.ok, `diarium line of ${id} does not validate: ${JSON.stringify(line)}`);
    }
  }
}

const exchangeOf = (line: DiariumLine): Exchange => line.event_data as unknown as Exchange;

const finalsOf = (lines: DiariumLine[], event: "inreq" | "outreq"): string[] =>
  lines.filter((l) => l.event === event).map((l) => exchangeOf(l).final_status);

// A core agent can only hold an unencrypted response body; the `Base64` alternative of the schema is
// the encrypted case (D-14), which the receiver ignores and never archives as an exchange.
const resBody = (ex: Exchange): ResponseBody | undefined =>
  ex.res === null ? undefined : (ex.res.response as ResponseBody);

const inreqOf = (lines: DiariumLine[], rid: string): Exchange | undefined => {
  const line = lines.find((l) => l.event === "inreq" && exchangeOf(l).req?.rid === rid);
  return line ? exchangeOf(line) : undefined;
};

/** The archived line of an exchange re-tracked from a bare response: `req` is null, `res` names the rid. */
const retrackedOf = (lines: DiariumLine[], rid: string): Exchange | undefined =>
  lines
    .filter((l) => l.event === "inreq")
    .map(exchangeOf)
    .find((ex) => ex.req === null && ex.res !== null && ex.res.rid === rid);

const diaryLogs = (lines: DiariumLine[]): string[] =>
  lines
    .filter((l) => l.event === "log")
    .map((l) => String((l.event_data as unknown as { message?: string }).message ?? l.event_data));

const diaryStatuses = (lines: DiariumLine[]): string[] =>
  lines.filter((l) => l.event === "status").map((l) => String(l.event_data));

const streamLogs = (events: ActioEvent[]): string[] => events.flatMap((e) => (e.event === "log" ? [e.message] : []));

const streamIgnored = (events: ActioEvent[]): { path: string; reason: string }[] =>
  events.flatMap((e) => (e.event === "ignored" ? [{ path: e.path, reason: e.reason }] : []));

const streamStatuses = (events: ActioEvent[]): string[] =>
  events.flatMap((e) => (e.event === "status" ? [e.state] : []));

/** The live view of the outbound exchanges (WP6-C-1): `"<rid>:<finalStatus>"`. */
const streamOutreq = (events: ActioEvent[]): string[] =>
  events.flatMap((e) => (e.event === "outreq" ? [`${e.rid}:${e.finalStatus}`] : []));

/** The ignored entries of a discovery scan, as `"<folder>/<file>"`, deduplicated and sorted. */
const whereIgnored = (events: ActioEvent[]): string[] => {
  const names = streamIgnored(events).map((e) => `${path.basename(path.dirname(e.path))}/${path.basename(e.path)}`);
  return [...new Set(names)].sort();
};

const exactly = (list: string[], what: string): string => {
  assert.equal(list.length, 1, `exactly one report for ${what}, got ${JSON.stringify(list)}`);
  return list[0]!;
};

type Rec = ReturnType<typeof recordInbox>;

const kindOf = (name: string): "req" | "res" | "can" | "con" => {
  if (name.endsWith(".res.json")) return "res";
  if (name.endsWith(".can.json")) return "can";
  if (name.endsWith(".con.json")) return "con";
  return "req";
};
const ridOf = (name: string): string => name.replace(/\.json$/, "").replace(/\.(res|can|con)$/, "");

function ridsOf(rec: Rec): string[] {
  return [...new Set(rec.events().map((e) => ridOf(e.name)))];
}

/** (a) helper: the APPEARANCE-only sequence of one rid, in `recordInbox` order (the WP9-A pattern:
 * disappearances can be lost to inotify coalescing, appearances cannot). */
function appearedSeq(rec: Rec, rid: string): string[] {
  return rec.events().filter((e) => e.kind === "appeared" && ridOf(e.name) === rid).map((e) => kindOf(e.name));
}

// --- fixtures the test writes itself: publications, a broadcast, envelope files -------------------

const peerSpec = (id: string, over: Partial<Spec> = {}): Spec => ({
  id,
  actio_version: "0.1",
  datetime: toIso(Date.now()),
  description: `${id}: a fixture peer of the discovery scenario`,
  accepts_commands_from: [],
  capabilities: [peerCap("open")],
  security_support: [],
  security_posture: "trusted",
  ...over,
});

const peerStatus = (id: string, state: Status["status"], datetime = toIso(Date.now())): Status => ({
  id,
  status: state,
  datetime,
  capability_status: [],
});

/** Writes a peer's two publications. A peer needs no inbox: the callability rules read files only. */
async function publishPeer(root: string, spec: Spec, status: Status): Promise<void> {
  const folder = path.join(root, spec.id);
  await mkdir(folder, { recursive: true });
  await io.writeJsonAtomic(path.join(folder, "spec.json"), spec, folder);
  await io.writeJsonAtomic(path.join(folder, "status.json"), status, folder);
}

/** The CLI's broadcast file: one write in `nuntium/` is what schedules the debounced scan (D-17). */
async function broadcast(root: string, text: string): Promise<void> {
  const now = Date.now();
  await io.makeDir(path.join(root, "nuntium"));
  await io.writeJsonAtomic(
    path.join(root, "nuntium", "nuntium.json"),
    { id: "broadcast", datetime: toIso(now), ts: Math.floor(now / 1000), nuntium: text } satisfies Announce,
    root,
  );
}

/** A file written atomically but NOT as JSON: the reader must see it whole, or the reason is a torn read. */
async function writeRawAtomic(target: string, content: string, tmpDir: string): Promise<void> {
  const tmp = path.join(tmpDir, `.${path.basename(target)}.${nonce()}.tmp`);
  await writeFile(tmp, content);
  await rename(tmp, target);
}

const tsNow = (): number => Math.floor(Date.now() / 1000);

function requestFile(
  rid: string,
  src: string,
  dst: string,
  name: string,
  params: unknown,
  timeoutSec: number,
  ts = tsNow(),
): RequestFile {
  return {
    rid,
    src,
    dst,
    datetime: toIso(Date.now()),
    type: "req",
    actio_version: "0.1",
    request: {
      ts,
      nonce: nonce(),
      signature: null,
      name,
      parameters: params as { [k: string]: unknown },
      timeout: timeoutSec,
      attachments: [],
    },
  };
}

function cancellationFile(req: RequestFile, name: string): CancellationFile {
  const now = Date.now();
  return {
    rid: req.rid,
    src: req.src,
    dst: req.dst,
    datetime: toIso(now),
    type: "req",
    actio_version: req.actio_version,
    cancellation: { ts: Math.floor(now / 1000), nonce: nonce(), signature: null, name, force: false },
  };
}

function responseFile(rid: string, src: string, dst: string, over: Partial<ResponseBody> = {}): ResponseFile {
  return {
    rid,
    src,
    dst,
    datetime: toIso(Date.now()),
    type: "req",
    actio_version: "0.1",
    response: {
      ts: tsNow(),
      nonce: nonce(),
      signature: null,
      error: false,
      cancellation: false,
      result: { text: "answered by a run that is gone" },
      attachments: [],
      ...over,
    },
  };
}

/** A base64 (encrypted) `type: "req"` envelope: schema-valid, unreadable for a core agent (D-14). */
function encryptedRequest(rid: string, src: string, dst: string): RequestFile {
  const body: RequestBody = {
    ts: tsNow(),
    nonce: nonce(),
    signature: null,
    name: "echo",
    parameters: { text: "encrypted" },
    timeout: 30,
    attachments: [],
  };
  return {
    rid,
    src,
    dst,
    datetime: toIso(Date.now()),
    type: "req",
    actio_version: "0.1",
    request: Buffer.from(JSON.stringify(body), "utf8").toString("base64") as unknown as RequestBody,
  };
}

// --- E14 — malformed files: ignored, never answered, reported once ---------------------------------

test("E14: malformed inbox files and broken publications: ignored, never answered", opts, async (t) => {
  const h = await harness(t);
  const p = await pair(h);
  const betaFolder = path.join(h.root, "beta");
  const inbox = path.join(betaFolder, "inbox");
  const rec = recordInbox(inbox);

  const ridBad = ulid();
  const ridSchema = ulid();
  const ridCrypt = ulid();
  const badFiles = [`${ridBad}.json`, `${ridCrypt}.json`, `${ridSchema}.json`].sort();
  await writeRawAtomic(path.join(inbox, `${ridBad}.json`), `{ this is not a json envelope`, betaFolder);

  const noNonce = requestFile(ridSchema, "alpha", "beta", "echo", { text: "no nonce" }, 30);
  delete (noNonce.request as unknown as { nonce?: string }).nonce;
  assert.ok(!validateRequest(noNonce).ok, "the schema fixture really is invalid");
  await io.writeJsonAtomic(path.join(inbox, `${ridSchema}.json`), noNonce, betaFolder);

  const encrypted = encryptedRequest(ridCrypt, "alpha", "beta");
  assert.ok(validateRequest(encrypted).ok, "an encrypted body IS protocol-valid: ignored for D-14, not D-15");
  await io.writeJsonAtomic(path.join(inbox, `${ridCrypt}.json`), encrypted, betaFolder);

  // the discovery side: a folder with an invalid spec.json, one with an invalid status.json
  const brokenSpecId = "broken-spec";
  const brokenStatusId = "broken-status";
  const brokenSpec = path.join(h.root, brokenSpecId);
  await mkdir(brokenSpec, { recursive: true });
  await writeRawAtomic(path.join(brokenSpec, "spec.json"), `{ "id": "broken-spec" }`, brokenSpec);
  await io.writeJsonAtomic(path.join(brokenSpec, "status.json"), peerStatus(brokenSpecId, "idle"), brokenSpec);
  const asleep = { ...peerStatus(brokenStatusId, "idle"), status: "asleep" } as unknown as Status;
  await publishPeer(h.root, peerSpec(brokenStatusId), asleep);

  // beta reports the three inbox files: once per path+mtime, in the diarium AND in the stream (D-15)
  const betaIgnores = async (): Promise<number> =>
    diaryLogs(await diaries(h.root, "beta")).filter((m) => m.includes("ignored")).length;
  await waitFor(async () => (await betaIgnores()) >= 3, 6_000);
  // alpha reports the two broken publications on the nuntium-triggered rescan (D-17)
  await broadcast(h.root, "E14: two folders of broken material appeared");
  await waitFor(() => new Set(streamIgnored(p.ea).map((e) => e.path)).size >= 2, 6_000);

  // the agent is unharmed: a normal request to the same inbox works
  const out = await p.a.request("beta", "echo", { text: "still here" });
  assert.equal(out.finalStatus, "confirmed");
  assert.deepEqual(out.response?.result, { text: "echo: still here" });
  await waitFor(() => inboxNames(h.root, "beta").then((n) => clean(n).length === 3), 6_000);
  rec.stop();

  // (a) each malformed envelope appeared once, and none of them ever got a response file
  const bad = [ridBad, ridSchema, ridCrypt];
  for (const rid of bad) {
    assert.deepEqual(appearedSeq(rec, rid), ["req"], `the malformed ${rid} was never answered`);
  }
  assert.equal(ridsOf(rec).filter((r) => !bad.includes(r)).length, 1, "only the follow-up exchange is left");
  // (b) exception, asserted: an ignored file stays in the inbox: not archived, not deleted
  assert.deepEqual(clean(await inboxNames(h.root, "beta")), badFiles);

  // (c) both diaria validate; the malformed material made report lines, never exchange lines
  const bLines = await diaries(h.root, "beta");
  const aLines = await diaries(h.root, "alpha");
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  const logs = diaryLogs(bLines);
  const forRid = (rid: string): string[] => logs.filter((m) => m.includes(`${rid}.json`));
  assert.equal(logs.filter((m) => m.includes("ignored")).length, 3, "one report per malformed file");
  assert.match(exactly(forRid(ridBad), ridBad), /ignored \S+\.json: malformed JSON/);
  assert.match(exactly(forRid(ridSchema), ridSchema), /ignored \S+\.json: .*nonce/);
  assert.match(exactly(forRid(ridCrypt), ridCrypt), /ignored \S+\.json: base64 body/);
  assert.deepEqual(finalsOf(bLines, "inreq"), ["confirmed"], "only the follow-up exchange reached A1");
  assert.deepEqual(finalsOf(aLines, "outreq"), ["confirmed"]);

  // the stream mirrors those three reports with their reasons (beta's own scan reports the broken
  // publications too, so filter to the inbox, where the malformed envelopes are)
  const ignoredBeta = streamIgnored(p.eb).filter((e) => e.path.startsWith(inbox));
  assert.equal(ignoredBeta.length, 3);
  assert.deepEqual(ignoredBeta.map((e) => path.basename(e.path)).sort(), badFiles);
  assert.ok(ignoredBeta.every((e) => e.reason.length > 0));
  // and the scan's reports name the offending file of each broken publication
  assert.deepEqual(whereIgnored(p.ea), [`${brokenSpecId}/spec.json`, `${brokenStatusId}/status.json`]);
  assert.ok(streamIgnored(p.ea).every((e) => e.reason.length > 0));
  // a reported folder never becomes a peer
  const adopted = p.a.agmen().has(brokenSpecId) || p.a.agmen().has(brokenStatusId);
  assert.ok(!adopted, "the scan reported, it did not adopt");
  // beta is idle, not error: D-22 is for lost watches, not for bad files
  assert.equal((await statusOf(h.root, "beta")).status, "idle");
  // (d) no attachments were involved anywhere
  assert.deepEqual(clean(await forumNames(h.root)), []);
});

// --- E15 — the Agmen view after the callability rules ---------------------------------------------

test("E15: agmen() matches the callability rules over eight fixture peers", opts, async (t) => {
  const h = await harness(t);
  const { agent: alpha, events } = makeAgent(h, "alpha");
  alpha.capability(ECHO, echoHandler);
  await alpha.start();
  assert.deepEqual([...alpha.agmen().keys()], ["alpha"], "before any peer exists the view holds only ourselves");

  const fresh = toIso(Date.now());
  const zombie = toIso(Date.now() - 2 * 60 * 60 * 1000); // two hours: far past the 600 s zombie line
  const idle = (id: string): Status => peerStatus(id, "idle", fresh);
  const privCaps = [peerCap("open"), peerCap("mine", { accepts_from: ["nobody"] })];
  const untrusted = peerSpec("peer-untrusted", {
    security_posture: "untrusted",
    security: [{ key: "issuer", value: "the issuer of this peer" }],
  });
  // Every fixture is protocol-valid, so a peer can only be absent from the view because of a rule.
  const peers: [Spec, Status][] = [
    [peerSpec("peer-ok"), idle("peer-ok")], // the control: nothing to discard
    [peerSpec("peer-ns", { namespaces: ["blue"] }), idle("peer-ns")], // rule 2: no namespace in common
    [peerSpec("peer-dis", { disabled: true, accepts_commands_from: ["alpha"] }), idle("peer-dis")], // 1
    [peerSpec("peer-priv", { capabilities: privCaps }), idle("peer-priv")], // rule 5.2
    [untrusted, idle("peer-untrusted")], // rules 3 and 4: the core profile is the `trusted` posture
    [peerSpec("peer-inactive"), peerStatus("peer-inactive", "inactive", fresh)], // rule 7
    [peerSpec("peer-zombie"), peerStatus("peer-zombie", "idle", zombie)], // rule 8
    [peerSpec("peer-none", { capabilities: [] }), idle("peer-none")], // rule 6: nothing usable
  ];
  for (const [spec, status] of peers) {
    // validate before publishing: an invalid fixture would be ignored by the scan, not filtered
    assert.ok(validateSpec(spec).ok, `${spec.id}: the fixture spec must be schema-valid`);
    assert.ok(validateStatus(status).ok, `${spec.id}: the fixture status must be schema-valid`);
    await publishPeer(h.root, spec, status);
  }
  // Nothing has rescanned since: `nuntiumDebounce` (50 ms) is the guard, and this check is immediate.
  assert.ok(!alpha.agmen().has("peer-ok"), "no scan has covered the new folders yet");

  await broadcast(h.root, "E15: eight peers appeared"); // D-17: any nuntium event schedules one scan
  await waitFor(() => alpha.agmen().has("peer-ok"), 6_000);

  const view = alpha.agmen();
  assert.deepEqual(
    [...view.keys()].sort(),
    ["alpha", "peer-dis", "peer-ok", "peer-priv", "peer-zombie"],
    "rule 2 (peer-ns), rules 3/4 (peer-untrusted), rule 6 (peer-none) and rule 7 (peer-inactive) discard them",
  );
  // rule 8: health is a flag, never a filter
  assert.equal(view.get("peer-ok")!.health, "healthy");
  assert.equal(view.get("peer-zombie")!.health, "zombie", "a zombie peer stays in the view, flagged");
  // rule 5.2: an `accepts_from` that does not list us filters the capability, not the peer
  assert.deepEqual([...view.get("peer-priv")!.capabilities.keys()], ["open"]);
  assert.equal(view.get("peer-priv")!.commands, false, "an empty accepts_commands_from means nobody (5.1)");
  // rule 1: a disabled peer loses every capability and keeps its commands
  assert.deepEqual([...view.get("peer-dis")!.capabilities.keys()], [], "rule 1");
  assert.equal(view.get("peer-dis")!.commands, true, "rule 5.1 still applies to a disabled peer");
  // the view carries the peer's own published objects
  assert.equal(view.get("peer-ok")!.spec.description, peerSpec("peer-ok").description);
  assert.equal(view.get("peer-ok")!.status.status, "idle");
  assert.ok(view.get("peer-zombie")!.folder.endsWith("peer-zombie"));
  // the same rescan ignored nothing in this root: every folder here is a valid publication
  assert.equal(streamIgnored(events).length, 0);
  await assertDiariaValid(h.root, ["alpha"]);
});

// --- E16 — request and response attachments (B1 → B5, publishAttachment → A4) ----------------------

test("E16: two request attachments and one response attachment: the forum ends empty", opts, async (t) => {
  const h = await harness(t);
  const p = await pair(h);
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));

  const sources = path.join(h.root, "sources");
  await mkdir(sources, { recursive: true });
  await writeFile(path.join(sources, "a1.txt"), "attachment one");
  await writeFile(path.join(sources, "a2.txt"), "attachment two");

  const attachments = [path.join(sources, "a1.txt"), path.join(sources, "a2.txt")];
  const outP = p.a.request("beta", "give", {}, { attachments });
  // the handler published its own attachment and still holds the exchange open
  await waitFor(() => p.giveForum().length > 0, 6_000);
  assert.deepEqual(p.giveForum(), ["a1.txt", "a2.txt", "out.bin"], "in flight, the forum holds all three");
  p.releaseGive();

  const out = await outP;
  assert.equal(out.finalStatus, "confirmed");
  assert.equal(out.response?.error, false);
  assert.deepEqual(out.response?.result, { saw: ["attachment one", "attachment two"], published: "out.bin" });
  assert.deepEqual(out.response?.attachments, ["out.bin"], "the response names the attachment it published");

  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 6_000);
  rec.stop();
  // (a) req → res → con in protocol order, one exchange only
  assert.equal(ridsOf(rec).length, 1, "exactly one exchange crossed the inbox");
  assert.deepEqual(appearedSeq(rec, ridsOf(rec)[0]!), ["req", "res", "con"]);
  // (b) both inboxes empty after the archive
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  assert.deepEqual(await inboxNames(h.root, "alpha"), []);
  // (d) *Envelope lifecycle*: B5 removed the request attachments, A4 removed the response one
  assert.deepEqual(clean(await forumNames(h.root)), [], "the forum is empty on both counts");

  // (c) both diaria validate, both sides confirmed, each naming its own attachments
  const aLines = await diaries(h.root, "alpha");
  const bLines = await diaries(h.root, "beta");
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  assert.deepEqual(finalsOf(aLines, "outreq"), ["confirmed"]);
  assert.deepEqual(finalsOf(bLines, "inreq"), ["confirmed"]);
  const outbound = exchangeOf(aLines.find((l) => l.event === "outreq")!);
  assert.deepEqual((outbound.req!.request as RequestBody).attachments, ["a1.txt", "a2.txt"]);
  assert.deepEqual(resBody(exchangeOf(bLines.find((l) => l.event === "inreq")!))?.attachments, ["out.bin"]);
  // the stream carries the same final statuses as the two diaria (WP6-C-1)
  const rid = ridsOf(rec)[0]!;
  assert.deepEqual(streamOutreq(p.ea), [`${rid}:confirmed`], "alpha: outreq confirmed");
  assert.ok(p.eb.some((e) => e.event === "inreq" && e.rid === rid), "beta: inreq on the stream too");
});

// --- E17 — pending envelopes at startup (D-4) -------------------------------------------------------

test("E17: pending envelopes at startup — cancelled, re-tracked response, expired request (108)", opts, async (t) => {
  const h = await harness(t);
  const betaFolder = path.join(h.root, "beta");
  const inbox = path.join(betaFolder, "inbox");
  await mkdir(inbox, { recursive: true });
  const rec = recordInbox(inbox); // armed before the agent starts: its initial scan IS this listing

  // (1) a request whose cancellation is pending too: A0 answers the cancellation, no work runs
  const ridA = ulid();
  const reqA = requestFile(ridA, "alpha", "beta", "echo", { text: "pending" }, 30);
  const canA = cancellationFile(reqA, "echo");
  assert.ok(validateRequest(reqA).ok && validateCancellation(canA).ok, "the D-4 fixtures are protocol files");
  // (2) a bare `.res.json` of a run that is gone: D-4 re-tracks it and restarts the confirmation wait
  const ridB = ulid();
  const resB = responseFile(ridB, "alpha", "beta", { result: { text: "answered before the crash" } });
  assert.ok(validateResponse(resB).ok, "the re-track fixture is a protocol response");
  // (3) an expired request: ts + timeout passed long before A0 ever looked
  const ridC = ulid();
  const reqC = requestFile(ridC, "alpha", "beta", "echo", { text: "expired" }, 5, tsNow() - 100);
  await io.writeJsonAtomic(path.join(inbox, `${ridA}.json`), reqA, betaFolder);
  await io.writeJsonAtomic(path.join(inbox, `${ridA}.can.json`), canA, betaFolder);
  await io.writeJsonAtomic(path.join(inbox, `${ridB}.res.json`), resB, betaFolder);
  await io.writeJsonAtomic(path.join(inbox, `${ridC}.json`), reqC, betaFolder);

  const { agent: beta } = makeAgent(h, "beta");
  beta.capability(ECHO, echoHandler);
  await beta.start(); // D-4: the initial scan processes them as if they had just arrived

  await waitFor(async () => (await diaries(h.root, "beta")).filter((l) => l.event === "inreq").length === 3, 10_000);
  // none of the three is confirmed (no requestor is alive), the confirmation timeout ends them all
  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 6_000);
  rec.stop();

  const lines = await diaries(h.root, "beta");
  // (1) cancellation before acceptance: cancelled, cancellation response, no confirmation
  const byA = inreqOf(lines, ridA)!;
  assert.equal(byA.final_status, "cancelled");
  assert.ok(byA.can !== null, "the pending cancellation was matched by rid (D-2)");
  assert.equal(resBody(byA)?.cancellation, true);
  assert.equal(resBody(byA)?.result, null);
  assert.equal(byA.con, null, "no requestor is alive to confirm");
  // (2) the re-tracked response: completed by the confirmation timeout, its request never present
  const byB = retrackedOf(lines, ridB);
  assert.ok(byB !== undefined, "the bare response was re-tracked, not ignored");
  assert.equal(byB.final_status, "completed");
  assert.equal(byB.req, null, "re-tracked from the response alone (D-4)");
  assert.deepEqual(resBody(byB)?.result, { text: "answered before the crash" });
  // (3) the expired request: A0 answers 108, and the archive needs no confirmation
  const byC = inreqOf(lines, ridC)!;
  assert.equal(byC.final_status, "completed");
  assert.equal(resBody(byC)?.error, true);
  assert.equal((resBody(byC)?.result as { code: number }).code, 108);
  // (c) every line validates
  await assertDiariaValid(h.root, ["beta"]);
  assert.deepEqual(finalsOf(lines, "inreq").sort(), ["cancelled", "completed", "completed"]);
  assert.deepEqual(finalsOf(lines, "outreq"), []);

  // (a) the inbox order: the watcher hands the pending files over in lexical order, but the recorder's
  // own first listing is `readdir` order, so the sequence of two files that were ALREADY there is not
  // fixed — only their presence is. The writes that follow are: each exchange ends with its response.
  assert.deepEqual(new Set(appearedSeq(rec, ridA)), new Set(["can", "req", "res"]), "cancelled, and answered");
  assert.deepEqual(appearedSeq(rec, ridB), ["res"], "only the response was pending for the re-track");
  assert.deepEqual(appearedSeq(rec, ridC), ["req", "res"], "the expired request was answered by us");
  assert.ok(!rec.events().some((e) => kindOf(e.name) === "con"), "nothing was confirmed: the requestors are gone");
  // (b) the inbox is empty after the three archives
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  // (d) no attachments were involved
  assert.deepEqual(clean(await forumNames(h.root)), []);
});

// --- E18 — the self-call ---------------------------------------------------------------------------

test("E18: an agent calling itself works like any other exchange", opts, async (t) => {
  const h = await harness(t);
  const { agent: alpha, events } = makeAgent(h, "alpha");
  alpha.capability(ECHO, echoHandler);
  await alpha.start();
  assert.ok(alpha.agmen().has("alpha"), "our own folder is in the view, so a self-call is callable");
  const rec = recordInbox(path.join(h.root, "alpha", "inbox"));

  const out = await alpha.request("alpha", "echo", { text: "myself" });
  assert.equal(out.finalStatus, "confirmed");
  assert.deepEqual(out.response?.result, { text: "echo: myself" });
  await waitFor(() => inboxNames(h.root, "alpha").then((n) => n.length === 0), 6_000);
  rec.stop();

  // (a) the same req → res → con sequence, in the agent's own inbox
  assert.equal(ridsOf(rec).length, 1, "one exchange: ours, to ourselves");
  assert.deepEqual(appearedSeq(rec, ridsOf(rec)[0]!), ["req", "res", "con"]);
  // (b) the inbox is empty after the archive
  assert.deepEqual(await inboxNames(h.root, "alpha"), []);
  // (c) the one diary holds BOTH sides of the exchange, and both are confirmed
  const lines = await diaries(h.root, "alpha");
  await assertDiariaValid(h.root, ["alpha"]);
  assert.deepEqual(finalsOf(lines, "outreq"), ["confirmed"]);
  assert.deepEqual(finalsOf(lines, "inreq"), ["confirmed"]);
  const outEx = exchangeOf(lines.find((l) => l.event === "outreq")!);
  const inEx = exchangeOf(lines.find((l) => l.event === "inreq")!);
  assert.equal(outEx.req!.rid, inEx.req!.rid);
  assert.equal(outEx.req!.src, "alpha");
  assert.equal(inEx.req!.dst, "alpha");
  // (d) no attachments
  assert.deepEqual(clean(await forumNames(h.root)), []);
  // D-8 saw both the inbound and the outbound side of the same exchange: one busy period, then idle
  assert.deepEqual(diaryStatuses(lines), ["idle", "busy", "idle"], "the self-call makes us busy once, not twice");
  assert.equal((await statusOf(h.root, "alpha")).status, "idle");
  // the live stream carries both sides too (WP6-C-1): the diarium is the durable record, not the only one
  const rid = ridsOf(rec)[0]!;
  assert.deepEqual(streamOutreq(events), [`${rid}:confirmed`], "outreq on the stream, as in the diarium");
  assert.ok(events.some((e) => e.event === "inreq" && e.rid === rid), "and inreq as well");
});

// --- E19 — the heartbeat keeps status.json fresh ----------------------------------------------------

test("E19: the heartbeat keeps status.json fresh; the diarium status event does not repeat", opts, async (t) => {
  const h = await harness(t);
  const HEARTBEAT = 250;
  const { agent: beta, events } = makeAgent(h, "beta", {}, { heartbeatInterval: HEARTBEAT });
  beta.capability(ECHO, echoHandler);
  await beta.start();

  const file = path.join(h.root, "beta", "status.json");
  const stampAges: number[] = [];
  const mtimeAges: number[] = [];
  const stamps = new Set<string>();
  const deadline = Date.now() + 2_000;
  while (Date.now() < deadline) {
    const s = JSON.parse(await readFile(file, "utf8")) as Status;
    const wall = Date.now();
    stamps.add(s.datetime);
    // `toIso` is second-resolved (the resolution of the protocol), so the datetime age carries up to
    // 1 s of truncation; the file's mtime is the exact measure of the heartbeat's cadence.
    stampAges.push(wall - Date.parse(s.datetime));
    mtimeAges.push(wall - (await stat(file)).mtimeMs);
    await sleep(100);
  }

  const worstStamp = Math.max(...stampAges);
  const worstMtime = Math.max(...mtimeAges);
  const republishes = stamps.size;
  t.diagnostic(
    `status.json: worst mtime age ${worstMtime} ms, worst datetime age ${worstStamp} ms, ` +
      `${republishes} distinct datetimes in 2 s`,
  );
  assert.ok(worstMtime <= HEARTBEAT + 500, `status.json went ${worstMtime} ms without a rewrite`);
  assert.ok(worstStamp <= HEARTBEAT + 1_500, `status.json datetime was ${worstStamp} ms old`);
  assert.ok(republishes >= 2, `the heartbeat republished status.json only ${republishes} times in 2 s`);
  assert.equal((await statusOf(h.root, "beta")).status, "idle", "the heartbeat rewrites the same state");
  assert.deepEqual(streamStatuses(events), ["idle"], "and the stream saw the same single change");

  // D-8: status.json is rewritten over and over; the diarium records a status event only on change
  const lines = await diaries(h.root, "beta");
  await assertDiariaValid(h.root, ["beta"]);
  assert.deepEqual(diaryStatuses(lines), ["idle"], "one status event, however many heartbeats ran");
});

// --- E20 — the four sync providers run the same protocol ------------------------------------------

/** The providers of §2.6, as `ActioConfig.sync` values. */
const PROVIDERS = ["none", "delay:20", "fsync+delay:20", "mutagen:robustness"];

interface Run {
  appearances: string[];
  inboxBeta: string[];
  inboxAlpha: string[];
  forum: string[];
  outreq: string[];
  inreq: string[];
  finalStatus: string;
  result: unknown;
  responseError: boolean | undefined;
}

test("E20: E1 runs identically with the four sync providers of §2.6", opts, async (t) => {
  const savedBin = process.env.ACTIO_MUTAGEN_BIN;
  t.after(() => {
    if (savedBin === undefined) delete process.env.ACTIO_MUTAGEN_BIN;
    else process.env.ACTIO_MUTAGEN_BIN = savedBin;
  });

  const runs: Run[] = [];
  let mutagenLog: string[] = [];
  for (const sync of PROVIDERS) {
    const h = await harness(t);
    if (sync.startsWith("mutagen:")) {
      // the fake binary of test/unit/sync.test.ts: it records the arguments of every execution
      const bin = path.join(h.root, "mutagen-fake");
      await writeFile(bin, `#!/bin/sh\necho "$@" >> "${path.join(h.root, "mutagen.log")}"\n`);
      await chmod(bin, 0o755);
      process.env.ACTIO_MUTAGEN_BIN = bin; // the provider reads the variable at run time
    }
    const p = await pair(h, { timers: { sync } });
    const rec = recordInbox(path.join(h.root, "beta", "inbox"));

    const out = await p.a.request("beta", "echo", { text: "provider" });
    await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 10_000);
    rec.stop();
    const [rid] = ridsOf(rec);
    runs.push({
      appearances: appearedSeq(rec, rid!),
      inboxBeta: clean(await inboxNames(h.root, "beta")),
      inboxAlpha: clean(await inboxNames(h.root, "alpha")),
      forum: clean(await forumNames(h.root)),
      outreq: finalsOf(await diaries(h.root, "alpha"), "outreq"),
      inreq: finalsOf(await diaries(h.root, "beta"), "inreq"),
      finalStatus: out.finalStatus,
      result: out.response?.result,
      responseError: out.response?.error,
    });
    await assertDiariaValid(h.root, ["alpha", "beta"]);
    for (const agent of h.agents) await agent.stop(); // C1 flushes the provider: the fake mutagen runs here
    if (sync.startsWith("mutagen:")) {
      const log = path.join(h.root, "mutagen.log");
      mutagenLog = (await readFile(log, "utf8")).trim().split("\n").filter((l) => l !== "");
    }
  }

  const baseline = runs[0]!;
  assert.deepEqual(baseline.appearances, ["req", "res", "con"], "the reference run (none)");
  assert.equal(baseline.finalStatus, "confirmed");
  assert.deepEqual(baseline.inreq, ["confirmed"]);
  assert.deepEqual(baseline.outreq, ["confirmed"]);
  assert.deepEqual(baseline.inboxBeta, []);
  assert.deepEqual(baseline.inboxAlpha, []);
  assert.deepEqual(baseline.forum, []);
  for (const [i, r] of runs.entries()) {
    assert.deepEqual(r, baseline, `the run with sync="${PROVIDERS[i]}" diverged from the none run`);
  }
  // the fake mutagen saw the debounced flushes of the exchange and the closing flush of C1
  t.diagnostic(`the fake mutagen ran ${mutagenLog.length} times: ${JSON.stringify([...new Set(mutagenLog)])}`);
  assert.ok(mutagenLog.length >= 2, `the mutagen provider ran ${mutagenLog.length} times`);
  assert.deepEqual([...new Set(mutagenLog)], ["sync flush robustness"], "only `mutagen sync flush <session>`");
});

// --- E21 — the watch is lost (D-22) -----------------------------------------------------------------

test("E21: own inbox deleted → error; a dst inbox deleted in B2 → abandoned", opts, async (t) => {
  const h = await harness(t);
  const p = await pair(h);

  // (1) beta's own inbox is gone: D-22 is fatal for the agent, and it never recovers on its own
  await rm(path.join(h.root, "beta", "inbox"), { recursive: true, force: true });
  await waitFor(() => statusOf(h.root, "beta").then((s) => s.status === "error"), 6_000);
  await waitFor(async () => (await announceOf(h.root, "beta")).nuntium.includes("failed"), 6_000);
  const betaStatusEvents = async (): Promise<string[]> => diaryStatuses(await diaries(h.root, "beta"));
  assert.ok(streamLogs(p.eb).some((m) => /^watch lost /.test(m)), "the watch loss is reported (D-22 text, R2-5)");
  assert.deepEqual(await betaStatusEvents(), ["idle", "error"], "exactly one status change: idle → error");
  assert.ok(streamStatuses(p.eb).includes("error"), "status error is observable");
  const failed = await announceOf(h.root, "beta");
  assert.equal(failed.id, "beta");
  assert.match(failed.nuntium, /^agent beta failed: watch lost /);
  await sleep(700); // two heartbeat intervals: the heartbeat keeps status.json, it never recovers the state
  const stillError = (await statusOf(h.root, "beta")).status;
  assert.equal(stillError, "error", "no recovery transition (spec note after the workflow)");
  assert.deepEqual(await betaStatusEvents(), ["idle", "error"], "the heartbeat did not add a status event");
  await assertDiariaValid(h.root, ["beta"]);

  // (2) gamma's inbox disappears while alpha waits in B2: the exchange is abandoned (D-6/D-22)
  const gamma = makeAgent(h, "gamma");
  let releaseGamma!: () => void;
  const gammaGate = new Promise<void>((resolve) => {
    releaseGamma = resolve;
  });
  gamma.agent.capability(HOLD, async () => {
    await gammaGate;
    return { ok: true };
  });
  await gamma.agent.start();
  await broadcast(h.root, "E21: gamma appeared");
  await waitFor(() => p.a.agmen().has("gamma"), 6_000);

  const outP = p.a.request("gamma", "hold", {}, { timeout: 0 }); // no timeout: the watch has to end it
  await waitFor(() => statusOf(h.root, "gamma").then((s) => s.status === "busy"), 6_000);
  assert.equal((await statusOf(h.root, "alpha")).status, "busy", "outbound B2 makes the requestor busy too (D-8)");
  await rm(path.join(h.root, "gamma", "inbox"), { recursive: true, force: true });

  const out = await outP;
  assert.equal(out.finalStatus, "abandoned", "the dst watch error abandons the exchanges waiting on that folder");
  assert.ok(streamLogs(p.ea).some((m) => /^watch lost /.test(m)), "the dst watch loss is logged (D-22 text)");
  await waitFor(() => statusOf(h.root, "alpha").then((s) => s.status === "idle"), 6_000);
  const aLines = await diaries(h.root, "alpha");
  assert.deepEqual(finalsOf(aLines, "outreq"), ["abandoned"]);
  assert.equal(streamOutreq(p.ea).length, 1, "the stream reports the abandoned exchange (WP6-C-1)");
  assert.match(streamOutreq(p.ea)[0]!, /:abandoned$/);
  assert.equal(exchangeOf(aLines.find((l) => l.event === "outreq")!).res, null, "no response was ever captured");
  // (b) alpha's own inbox never held anything of an exchange it could not finish
  assert.deepEqual(await inboxNames(h.root, "alpha"), []);
  // (d) no attachments were involved
  assert.deepEqual(clean(await forumNames(h.root)), []);
  await assertDiariaValid(h.root, ["alpha", "beta", "gamma"]);

  // gamma's work has to land somewhere writable, or its own archive (and the cleanup) cannot run
  await mkdir(path.join(h.root, "gamma", "inbox"), { recursive: true });
  releaseGamma();
});
