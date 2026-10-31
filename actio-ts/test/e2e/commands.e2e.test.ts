// E2E — command and lifecycle scenarios E10–E13 (plan §7): real temp .actio roots, two
// ActioAgents in-process (alpha = command issuer, beta = receiver-side) with fastConfig().
// Every scenario asserts the four §7 properties: (a) order of files in the receiver inbox with
// recordInbox, (b) inbox empty after archive, (c) every diarium line of both agents validates
// and the inreq/outreq final statuses, (d) attachments cleaned per the Envelope lifecycle.
//
// Notes on what the public API can express (documented in the report):
// - E11's unlisted-src drop (A.5): the requestor's local gate (ActioAgent.command, rule 5.1)
//   rejects command() to a peer whose accepts_commands_from does not list us BEFORE anything is
//   written, so the D0 drop for an unlisted src is exercised with an envelope written directly
//   into beta's inbox (the same trick as E3's 108).
// - E11's "no status/spec/nuntium change" is asserted BYTE-identical: the fastConfig heartbeat
//   (200 ms) rewrites status.json's datetime, so this scenario runs with a long heartbeatInterval.
// - E12: receiver.stop() writes the code-300 response and archives the exchange immediately
//   (R1-2: the archive completes before C1), so the res file's lifetime is milliseconds; (a)
//   asserts the req → res appearances, and the 300 body is asserted from the archived envelope
//   (the diarium) as well as from alpha's captured outcome.
// - E12's "status inactive" is asserted from the DIARIUM `status` event, the announce file AND the
//   final status.json content. The syncBusy/C1 race that used to leave status.json `idle` after the
//   stop is fixed (f2eb08d), and the R3 probes (oracle/R3-probes.test.ts: 10/10 stops with a request
//   in flight, 40/40 forced stops racing a 1 ms heartbeat) confirm it is gone.
import assert from "node:assert/strict";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test, type TestContext } from "node:test";
import { ActioAgent } from "../../src/agent.js";
import * as io from "../../src/io.js";
import { nonce, toIso, ulid } from "../../src/util.js";
import { validateDiarium } from "../../src/validate.js";
import { fastConfig, recordInbox, waitFor } from "../helpers/index.js";
import type { ActioConfig, ActioEvent, AgentOptions, CapabilityHandler } from "../../src/types.js";
import type { Announce, Capability, DiariumLine, Exchange, RequestFile, Status } from "../../src/generated/types.js";

/** A stuck exchange fails the test instead of hanging the suite. */
const opts = { timeout: 30_000 };

const ECHO: Capability = {
  name: "echo",
  description: "echoes the text it gets (Appendix A.1)",
  parameters: { type: "object", properties: { text: { type: "string" } }, required: ["text"], additionalProperties: false },
  actio: { result: { type: "object", properties: { text: { type: "string" } }, required: ["text"] } },
};

/** Abortable sleeper; E12 needs a request in flight when the stop arrives. */
const SLEEP: Capability = {
  name: "sleep",
  description: "waits, abortable via the handler signal",
  parameters: { type: "object", properties: { seconds: { type: "number" } }, required: ["seconds"], additionalProperties: false },
  actio: { result: { type: "object", properties: { slept: { type: "number" } }, required: ["slept"] } },
};

const echoHandler: CapabilityHandler = async (params) => ({ text: `echo: ${(params as { text: string }).text}` });

interface Harness {
  root: string;
  agents: ActioAgent[];
}

interface Pair {
  root: string;
  a: ActioAgent;
  b: ActioAgent;
  ea: ActioEvent[];
  eb: ActioEvent[];
}

/** One agent in the harness; the after-hook stops every agent BEFORE the root is removed
 * (the archives must not write into a deleted tree). */
function makeAgent(
  h: Harness,
  id: string,
  over: Partial<AgentOptions> = {},
  timers: Partial<ActioConfig> = {},
): { agent: ActioAgent; events: ActioEvent[] } {
  const events: ActioEvent[] = [];
  const a = new ActioAgent({
    id,
    description: `${id} e2e agent`,
    acceptsCommandsFrom: [],
    ...over,
    config: { ...fastConfig(h.root, Date.now), ...timers },
  });
  a.on("event", (e) => events.push(e));
  h.agents.push(a);
  return { agent: a, events };
}

async function harness(t: TestContext): Promise<Harness> {
  const root = await fsp.mkdtemp(path.join(os.tmpdir(), ".actio-e2e-"));
  const h: Harness = { root, agents: [] };
  t.after(async () => {
    for (const a of h.agents) await a.stop(); // a NOP when it never started or already stopped
    await fsp.rm(root, { recursive: true, force: true });
  });
  return h;
}

/** alpha and beta, both serving echo/sleep and a custom `noop` command, mutually commandable. */
async function pair(
  h: Harness,
  over: { a?: Partial<AgentOptions>; b?: Partial<AgentOptions>; timers?: Partial<ActioConfig> } = {},
): Promise<Pair> {
  const sleepHandler: CapabilityHandler = async (params, ctx) => {
    const seconds = (params as { seconds: number }).seconds;
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, seconds * 1000);
      ctx.signal.addEventListener("abort", () => {
        clearTimeout(timer);
        resolve();
      }, { once: true });
    });
    return { slept: seconds };
  };
  const register = (agent: ActioAgent): void => {
    agent.capability(ECHO, echoHandler);
    agent.capability(SLEEP, sleepHandler);
    agent.onCommand("noop", async () => undefined);
  };
  const a = makeAgent(h, "alpha", { acceptsCommandsFrom: ["beta"], ...over.a }, over.timers);
  const b = makeAgent(h, "beta", { acceptsCommandsFrom: ["alpha"], ...over.b }, over.timers);
  register(a.agent);
  register(b.agent);
  // Beta starts first: alpha's INITIAL scan (step 4) is the discovery that runs in time, and it
  // needs beta to be already published.
  await b.agent.start();
  await a.agent.start();
  await waitFor(() => a.agent.agmen().has("beta"), 6_000);
  return { root: h.root, a: a.agent, b: b.agent, ea: a.events, eb: b.events };
}

// --- the four §7 properties, all on REAL files --------------------------------------------------

const statusOf = (root: string, id: string): Promise<Status> =>
  fsp.readFile(path.join(root, id, "status.json"), "utf8").then((s) => JSON.parse(s) as Status);
const specDisabled = (root: string, id: string): Promise<boolean | undefined> =>
  fsp.readFile(path.join(root, id, "spec.json"), "utf8").then((s) => (JSON.parse(s) as { disabled?: boolean }).disabled);
const announceOf = (root: string, id: string): Promise<Announce> =>
  fsp.readFile(path.join(root, "nuntium", `${id}.n.json`), "utf8").then((s) => JSON.parse(s) as Announce);

const inboxNames = (root: string, id: string): Promise<string[]> => io.listDir(path.join(root, id, "inbox"));
const forumNames = (root: string): Promise<string[]> => io.listDir(path.join(root, "forum"));

/** (c) input: the raw diarium lines of one agent, from disk. */
async function diaries(root: string, id: string): Promise<DiariumLine[]> {
  const file = path.join(root, id, "diarium.jsonl");
  if (!(await io.exists(file))) return [];
  return (await fsp.readFile(file, "utf8"))
    .split("\n")
    .filter((l) => l !== "")
    .map((l) => JSON.parse(l) as DiariumLine);
}

/** (c): every line of every given agent must pass validateDiarium. */
async function assertDiariaValid(root: string, ids: string[]): Promise<void> {
  for (const id of ids) {
    for (const line of await diaries(root, id)) {
      const checked = validateDiarium(line);
      assert.ok(checked.ok, `diarium line of ${id} does not validate: ${JSON.stringify(line)}`);
    }
  }
}

/** The `final_status` of the `inreq`/`outreq` lines, in order. */
const finalsOf = (lines: DiariumLine[], event: "inreq" | "outreq"): string[] =>
  lines.filter((l) => l.event === event).map((l) => String((l.event_data as unknown as Exchange).final_status));

/** The `status` lines of a diarium, in order (the state strings). */
const statusEventsOf = (lines: DiariumLine[]): string[] =>
  lines.filter((l) => l.event === "status").map((l) => String(l.event_data));

/** The `announce` lines of a diarium, in order (one per announce, event_data null). */
const announceCount = (root: string, id: string): Promise<number> =>
  diaries(root, id).then((ls) => ls.filter((l) => l.event === "announce").length);

/** The archived exchange of a diarium line (the four envelopes + final status). */
const exchangeOf = (line: DiariumLine): Exchange => line.event_data as unknown as Exchange;

type Rec = ReturnType<typeof recordInbox>;

/** The kind of an inbox file name: req / res / can / con. */
const kindOf = (name: string): "req" | "res" | "can" | "con" => {
  if (name.endsWith(".res.json")) return "res";
  if (name.endsWith(".can.json")) return "can";
  if (name.endsWith(".con.json")) return "con";
  return "req";
};
const ridOf = (name: string): string => name.replace(/\.json$/, "").replace(/\.(res|can|con)$/, "");

/** (a) helper: the per-rid event sequence (kinds, in recordInbox order). */
function byRid(rec: Rec): Map<string, string[]> {
  const m = new Map<string, string[]>();
  for (const e of rec.events()) {
    const list = m.get(ridOf(e.name)) ?? [];
    list.push(kindOf(e.name));
    m.set(ridOf(e.name), list);
  }
  return m;
}

/** (a) helper: the APPEARANCE-only sequence of one rid, in recordInbox order. Disappearances are
 * asserted on disk ((b)): the recorder's last disappearance scan can be lost to inotify
 * coalescing, while appearances are stable (the files persist between scans). */
function appearedSeq(rec: Rec, rid: string): string[] {
  return rec.events().filter((e) => e.kind === "appeared" && ridOf(e.name) === rid).map((e) => kindOf(e.name));
}

// --- E10 — commands accepted: ping, enable false/true, custom noop ------------------------------

test("E10: commands accepted — ping, enable false/true, custom noop: completed, re-announce, spec toggled", opts, async (t) => {
  const h = await harness(t);
  const p = await pair(h);
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));
  await waitFor(() => announceCount(h.root, "beta").then((n) => n === 1), 6_000); // the start announce is settled

  // ping: D2 re-publishes spec and status and re-announces; D3 archives the command as completed.
  const ridPing = await p.a.command("beta", "ping");
  await waitFor(() => announceCount(h.root, "beta").then((n) => n === 2), 6_000);
  assert.equal((await announceOf(h.root, "beta")).nuntium, "agent beta is alive");

  // enable false: spec.json disabled true on disk + re-announce.
  const ridOff = await p.a.command("beta", "enable", { enabled: false });
  await waitFor(() => announceCount(h.root, "beta").then((n) => n === 3), 6_000);
  assert.equal(await specDisabled(h.root, "beta"), true, "spec.json carries disabled: true");
  assert.equal((await announceOf(h.root, "beta")).nuntium, "agent beta is disabled");

  // enable true: back.
  const ridOn = await p.a.command("beta", "enable", { enabled: true });
  await waitFor(() => announceCount(h.root, "beta").then((n) => n === 4), 6_000);
  assert.equal(await specDisabled(h.root, "beta"), false, "spec.json carries disabled: false");
  assert.equal((await announceOf(h.root, "beta")).nuntium, "agent beta is enabled");

  // a custom command: executed, archived as completed, no re-announce (D3 re-announces the
  // standard commands' state change; a custom command changes no protocol state).
  const ridNoop = await p.a.command("beta", "noop", { n: 1 });
  await waitFor(async () => finalsOf(await diaries(h.root, "beta"), "inreq").length === 4, 6_000);
  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 6_000);
  assert.equal(await announceCount(h.root, "beta"), 4, "the custom command did not re-announce");
  rec.stop();

  // (a) each command envelope appeared once (a command is a bare `.json`; D3 deletes it)
  const seqs = byRid(rec);
  assert.equal(seqs.size, 4, "four commands, four rids");
  for (const rid of [ridPing, ridOff, ridOn, ridNoop]) {
    assert.deepEqual(appearedSeq(rec, rid), ["req"], `rid ${rid}`);
  }
  // (b) the inboxes are empty after every archive
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  assert.deepEqual(await inboxNames(h.root, "alpha"), []);
  // (c) both diaria validate; the four commands are `completed`, the four sends are `sent`, and
  // a command envelope carries only `req` (commands are never answered, D3)
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "beta"), "inreq"), ["completed", "completed", "completed", "completed"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "alpha"), "outreq"), ["sent", "sent", "sent", "sent"]);
  for (const line of (await diaries(h.root, "beta")).filter((l) => l.event === "inreq")) {
    const ex = exchangeOf(line);
    assert.equal(ex.req?.type, "cmd");
    assert.equal(ex.can, null);
    assert.equal(ex.res, null);
    assert.equal(ex.con, null);
  }
  // (d) no attachments were involved, so the forum is empty
  assert.deepEqual(await forumNames(h.root), []);
});

// --- E11 — commands dropped (A.5): unknown name and unlisted src --------------------------------

test("E11: commands dropped — unknown name and unlisted src: dropped, no status/spec/nuntium change", opts, async (t) => {
  const h = await harness(t);
  // A long heartbeat: the 200 ms beat would rewrite status.json's datetime and break the
  // byte-identical assertion (D4: a dropped command touches no file).
  const p = await pair(h, { timers: { heartbeatInterval: 30_000 } });
  // gamma is not in beta's accepts_commands_from (["alpha"]): its stop is the A.5 drop.
  const gamma = makeAgent(h, "gamma", { acceptsCommandsFrom: [] });
  await gamma.agent.start();
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));
  await waitFor(() => announceCount(h.root, "beta").then((n) => n === 1), 6_000); // the start announce is settled
  const paths = {
    status: path.join(h.root, "beta", "status.json"),
    spec: path.join(h.root, "beta", "spec.json"),
    nuntium: path.join(h.root, "nuntium", "beta.n.json"),
  };
  const beforeStatus = await fsp.readFile(paths.status);
  const beforeSpec = await fsp.readFile(paths.spec);
  const beforeNuntium = await fsp.readFile(paths.nuntium);

  // 1: an unknown name from a LISTED src (alpha): the local gate passes (commands are never
  // advertised), the receiver's D0 drops it (D4).
  const ridUnknown = await p.a.command("beta", "frobnicate", { x: 1 });
  // 2: a `stop` from an UNLISTED src (A.5): gamma.command() would be rejected by its own local
  // gate (rule 5.1) before writing, so the envelope is written directly into beta's inbox —
  // only the receiver can judge which commands it accepts.
  const ridStop = ulid();
  const stopCmd: RequestFile = {
    rid: ridStop,
    src: "gamma",
    dst: "beta",
    datetime: toIso(Date.now()),
    type: "cmd",
    actio_version: "0.1",
    request: {
      ts: Math.floor(Date.now() / 1000),
      nonce: nonce(),
      signature: null,
      name: "stop",
      parameters: { force: false },
      timeout: 0,
      attachments: [],
    },
  };
  await io.writeJsonAtomic(path.join(h.root, "beta", "inbox", `${ridStop}.json`), stopCmd, path.join(h.root, "beta"));

  // both envelopes are archived (D4)
  await waitFor(async () => finalsOf(await diaries(h.root, "beta"), "inreq").length === 2, 6_000);
  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 6_000);
  rec.stop();

  // (a) each dropped envelope appeared once (a bare `.json`; D4 deletes it)
  const seqs = byRid(rec);
  assert.equal(seqs.size, 2, "two dropped commands, two rids");
  for (const rid of [ridUnknown, ridStop]) {
    assert.deepEqual(appearedSeq(rec, rid), ["req"], `rid ${rid}`);
  }
  // (b) the inboxes are empty
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  assert.deepEqual(await inboxNames(h.root, "alpha"), []);
  // (c) every diarium line validates; both commands are `dropped`, the send is `sent`, and a
  // dropped envelope carries only `req` (never answered)
  await assertDiariaValid(h.root, ["alpha", "beta", "gamma"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "beta"), "inreq"), ["dropped", "dropped"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "alpha"), "outreq"), ["sent"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "gamma"), "outreq"), []);
  for (const line of (await diaries(h.root, "beta")).filter((l) => l.event === "inreq")) {
    const ex = exchangeOf(line);
    assert.equal(ex.req?.type, "cmd");
    assert.equal(ex.res, null);
  }
  // D4: status.json, spec.json and the announce file are NOT touched — byte-identical
  const afterStatus = await fsp.readFile(paths.status);
  const afterSpec = await fsp.readFile(paths.spec);
  const afterNuntium = await fsp.readFile(paths.nuntium);
  assert.ok(beforeStatus.equals(afterStatus), "status.json is byte-identical (D4)");
  assert.ok(beforeSpec.equals(afterSpec), "spec.json is byte-identical (D4)");
  assert.ok(beforeNuntium.equals(afterNuntium), "the announce file is byte-identical (D4)");
  // the dropped stop was never executed: beta is still idle and running
  assert.equal((await statusOf(h.root, "beta")).status, "idle");
  // (d) no attachments were involved, so the forum is empty
  assert.deepEqual(await forumNames(h.root), []);
});

// --- E12 — stop with a request in flight: 300, inreq archived before C1 --------------------------

test("E12: stop with a request in flight — error 300, inreq archived before C1, status inactive", opts, async (t) => {
  const h = await harness(t);
  const p = await pair(h);
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));
  const outP = p.a.request("beta", "sleep", { seconds: 30 });
  // the handler is in flight (A1): beta is busy and the capability shows running 1
  await waitFor(
    () =>
      statusOf(h.root, "beta").then(
        (s) => s.capability_status.find((c) => c.name === "sleep")?.running === 1,
      ),
    6_000,
  );
  // alpha commands the stop: D3 archives the command FIRST, then C1 (lane C) runs.
  const ridStop = await p.a.command("beta", "stop", {});
  // C1: the `status inactive` diarium event FIRST, then (rule 2) the announce file with a delay.
  await waitFor(
    async () => (await diaries(h.root, "beta")).some((l) => l.event === "status" && String(l.event_data) === "inactive"),
    8_000,
  );
  await waitFor(
    () => announceOf(h.root, "beta").then((a) => a.nuntium === "agent beta is inactive"),
    8_000,
  );
  // The final status.json content: `inactive` (§7 E12). The syncBusy/C1 race that used to leave it
  // `idle` is gone (f2eb08d; the R3 probes in the file header).
  assert.equal((await statusOf(h.root, "beta")).status, "inactive");
  // alpha's in-flight request resolves with the code-300 error outcome (B2 captured the 300 res).
  const out = await outP;
  assert.equal(out.finalStatus, "confirmed", "an error response is also confirmed (the error is in res)");
  assert.equal(out.response?.error, true);
  assert.equal((out.response?.result as { code: number }).code, 300, "agent_stopping");
  // (b) the inbox is empty after every archive
  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 8_000);
  rec.stop();
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  assert.deepEqual(await inboxNames(h.root, "alpha"), []);

  // (a) the request's envelope: req → res (the 300 response); the stop command: req only
  const rids = [...byRid(rec).keys()];
  assert.equal(rids.length, 2, "one request, one command");
  const ridSleep = rids.find((r) => r !== ridStop)!;
  assert.deepEqual(appearedSeq(rec, ridSleep), ["req", "res"]);
  assert.deepEqual(appearedSeq(rec, ridStop), ["req"]);

  // (c) both diaria validate; the diarium ORDER: both inreq lines (the stop command at D3, the
  // request at A4) come BEFORE the `status inactive` event (C1) — "the archive is always
  // completed before shutting down"
  const bLines = await diaries(h.root, "beta");
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  assert.deepEqual(finalsOf(bLines, "inreq"), ["completed", "completed"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "alpha"), "outreq"), ["sent", "confirmed"]);
  const idxStop = bLines.findIndex((l) => l.event === "inreq" && exchangeOf(l).req?.rid === ridStop);
  const idxSleep = bLines.findIndex((l) => l.event === "inreq" && exchangeOf(l).req?.rid === ridSleep);
  const idxInactive = bLines.findIndex((l) => l.event === "status" && String(l.event_data) === "inactive");
  assert.ok(idxStop >= 0 && idxSleep >= 0 && idxInactive >= 0, "the stop inreq, the request inreq and the inactive status all exist");
  assert.ok(idxStop < idxInactive, "the stop command is archived (D3) before C1");
  assert.ok(idxSleep < idxInactive, "the request exchange is archived (A4) before C1");
  // the archived exchange carries the 300 response (cancellation false, no confirmation)
  const sleepEx = exchangeOf(bLines.find((l) => l.event === "inreq" && exchangeOf(l).req?.rid === ridSleep)!);
  const res300 = sleepEx.res === null ? undefined : (sleepEx.res as unknown as { response: { error: boolean; cancellation: boolean; result: { code: number } } }).response;
  assert.equal(res300?.error, true);
  assert.equal(res300?.cancellation, false);
  assert.equal(res300?.result.code, 300);
  assert.equal(sleepEx.con, null, "the stop archived before any confirmation was needed");
  // (d) no attachments were involved, so the forum is empty
  assert.deepEqual(await forumNames(h.root), []);
});

// --- E13 — restart with delay: archive, inactive, active announce after ≥ delay ------------------

test("E13: restart with delay — archive, inactive, then the active announce after ≥ the delay; works again", opts, async (t) => {
  const h = await harness(t);
  const p = await pair(h);
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));
  const rid = await p.a.command("beta", "restart", { force: false, delay: 2 });
  // D3: the command is archived BEFORE the shutdown; the delay starts after the archive.
  await waitFor(
    () => diaries(h.root, "beta").then((ls) => ls.some((l) => l.event === "inreq" && exchangeOf(l).req?.rid === rid)),
    6_000,
  );
  const archived = (await diaries(h.root, "beta")).find((l) => l.event === "inreq" && exchangeOf(l).req?.rid === rid)!;
  assert.equal(exchangeOf(archived).final_status, "completed", "D3 archived the command");
  const tArchive = Date.parse(archived.datetime); // seconds resolution: ≤ the true archive time
  // C1 in between: the status went inactive.
  await waitFor(() => statusOf(h.root, "beta").then((s) => s.status === "inactive"), 8_000);
  // the fresh start re-announces active — the third announce of the run (start, inactive, active).
  await waitFor(() => announceCount(h.root, "beta").then((n) => n === 3), 12_000);
  const tActive = Date.now(); // the diarium announce line is appended AFTER the file write
  const ann = await announceOf(h.root, "beta");
  assert.equal(ann.nuntium, "agent beta is active");
  assert.ok(
    tActive - tArchive >= 2_000 - 50,
    `the active announce comes ≥ the delay after the archive (observed ${tActive - tArchive} ms)`,
  );
  assert.equal((await statusOf(h.root, "beta")).status, "idle");
  // beta works again: the fresh announce re-discovers beta for alpha, and the echo flows.
  await waitFor(() => p.a.agmen().has("beta"), 6_000);
  const out = await p.a.request("beta", "echo", { text: "again" });
  assert.equal(out.finalStatus, "confirmed");
  assert.deepEqual(out.response?.result, { text: "echo: again" });
  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 8_000);
  rec.stop();

  // (a) the restart envelope appeared once (a bare `.json`; D3 deleted it); the follow-up echo
  // request shows the full protocol order req → res → con in the same inbox
  const seqs = byRid(rec);
  assert.equal(seqs.size, 2, "the restart command and the follow-up echo request");
  assert.deepEqual(appearedSeq(rec, rid), ["req"]);
  const ridEcho = [...seqs.keys()].find((r) => r !== rid)!;
  assert.deepEqual(appearedSeq(rec, ridEcho), ["req", "res", "con"]);
  // (b) the inboxes are empty
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  assert.deepEqual(await inboxNames(h.root, "alpha"), []);
  // (c) both diaria validate; the command is `completed` (the echo request: `confirmed`), the
  // statuses went idle → inactive → idle
  const bLines = await diaries(h.root, "beta");
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  assert.deepEqual(finalsOf(bLines, "inreq"), ["completed", "confirmed"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "alpha"), "outreq"), ["sent", "confirmed"]);
  assert.deepEqual(
    statusEventsOf(bLines),
    ["idle", "inactive", "idle", "busy", "idle"],
    "C1 then a fresh start, and the echo request makes it busy → idle again",
  );
  // (d) no attachments were involved, so the forum is empty
  assert.deepEqual(await forumNames(h.root), []);
});