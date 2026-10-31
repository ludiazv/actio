// E2E — request scenarios E1–E9 (plan §7): real temp .actio roots, two ActioAgents in-process
// (alpha = requestor-side, beta = receiver-side) with fastConfig(). Every scenario asserts the
// four §7 properties: (a) order of files in the receiver inbox with recordInbox, (b) inbox empty
// after archive (E9 leaves its files), (c) every diarium line of both agents validates and the
// inreq/outreq final statuses, (d) attachments cleaned per the Envelope lifecycle.
//
// Notes on what the public API can express (documented in the report):
// - 101/102/103/106 (E2 and the E3 sub-cases): the requestor's local callability gate
//   (ActioAgent.request, rules 1/2/5.2) rejects with ActioCallError BEFORE anything is written, so
//   the receiver-side 10x response is unreachable in e2e. The e2e assertion is the local rejection
//   plus zero disk side effects; the responses themselves are unit-tested (receiver.test.ts).
// - 100/105/107/108 produce real responses: 100 through a stale view (long nuntiumDebounce, the
//   same trick as agent.test.ts), 108 through a request file written directly into beta's inbox.
// - E7: the requestor cannot cancel after it captured a response (giveUp bails on ex.res !== null),
//   so the e2e variant stops the requestor before the response is written; the receiver side is
//   exactly A.4 (cancellation after the response, single response with cancellation=false).
// - E4/E9: the requestor side ends `abandoned` (it is gone before confirming / the receiver is
//   inactive); the brief's table fixes only the receiver status for E4 and the outreq status for E9.
import assert from "node:assert/strict";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test, type TestContext } from "node:test";
import { ActioAgent } from "../../src/agent.js";
import * as io from "../../src/io.js";
import { nonce, toIso, ulid } from "../../src/util.js";
import { ActioCallError } from "../../src/types.js";
import { validateDiarium } from "../../src/validate.js";
import { fastConfig, recordInbox, waitFor } from "../helpers/index.js";
import type { ActioConfig, ActioEvent, AgentOptions, CapabilityHandler } from "../../src/types.js";
import type { Capability, DiariumLine, Exchange, RequestFile, ResponseBody, Status } from "../../src/generated/types.js";

/** A stuck exchange fails the test instead of hanging the suite. */
const opts = { timeout: 30_000 };

const OK_RESULT = { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"] } as const;

const ECHO: Capability = {
  name: "echo",
  description: "echoes the text it gets (Appendix A.1)",
  parameters: { type: "object", properties: { text: { type: "string" } }, required: ["text"], additionalProperties: false },
  actio: { result: { type: "object", properties: { text: { type: "string" } }, required: ["text"] } },
};

/** Abortable sleeper; max_parallel 1 is what E3's 107 sub-case needs. */
const SLEEP: Capability = {
  name: "sleep",
  description: "waits, abortable via the handler signal",
  parameters: { type: "object", properties: { seconds: { type: "number" } }, required: ["seconds"], additionalProperties: false },
  actio: {
    max_parallel: 1,
    result: { type: "object", properties: { slept: { type: "number" } }, required: ["slept"] },
  },
};

/** Registered disabled from the start: E3's 102 sub-case (filtered out of the peer view, rule 5.2). */
const OFF: Capability = {
  name: "off",
  description: "always disabled",
  parameters: { type: "object", properties: {}, additionalProperties: false },
  actio: { result: { ...OK_RESULT }, disabled: true },
};

/** Only gamma may call it: E3's 103 sub-case (filtered out of the peer view, rule 5.2). */
const PRIVATE: Capability = {
  name: "private",
  description: "only gamma may call it",
  parameters: { type: "object", properties: {}, additionalProperties: false },
  actio: { result: { ...OK_RESULT }, accepts_from: ["gamma"] },
};

/** Strict parameters: E3's 105 sub-case. */
const STRICT: Capability = {
  name: "strict",
  description: "strict parameters",
  parameters: { type: "object", properties: { text: { type: "string" } }, required: ["text"], additionalProperties: false },
  actio: { result: { ...OK_RESULT } },
};

/** E7: the handler the test holds, so the response can be written on command. */
const HOLD: Capability = {
  name: "hold",
  description: "waits until the test releases it",
  parameters: { type: "object", properties: {}, additionalProperties: false },
  actio: { result: { ...OK_RESULT } },
};

const echoHandler: CapabilityHandler = async (params) => ({ text: `echo: ${(params as { text: string }).text}` });
const okHandler: CapabilityHandler = async () => ({ ok: true });

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
  /** One entry per `sleep` handler invocation, with whether the signal was aborted. */
  sleepCalls: { aborted: boolean }[];
  holdCalls: number;
  releaseHold(): void;
}

/** One agent in the harness; the after-hook stops every agent BEFORE the root is removed
 * (D-9's archives must not write into a deleted tree). */
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

/** alpha and beta, both serving the full capability set, mutually discovered. */
async function pair(
  h: Harness,
  over: { a?: Partial<AgentOptions>; b?: Partial<AgentOptions>; timers?: Partial<ActioConfig> } = {},
): Promise<Pair> {
  const sleepCalls: { aborted: boolean }[] = [];
  let holdCalls = 0;
  let releaseHold!: () => void;
  const holdGate = new Promise<void>((r) => {
    releaseHold = r;
  });
  const sleepHandler: CapabilityHandler = async (params, ctx) => {
    const seconds = (params as { seconds: number }).seconds;
    let aborted = false;
    await new Promise<void>((resolve) => {
      const t = setTimeout(resolve, seconds * 1000);
      ctx.signal.addEventListener("abort", () => {
        aborted = true;
        clearTimeout(t);
        resolve();
      }, { once: true });
    });
    sleepCalls.push({ aborted });
    return { slept: seconds };
  };
  const holdHandler: CapabilityHandler = async () => {
    holdCalls += 1;
    await holdGate;
    return { ok: true };
  };
  const register = (agent: ActioAgent): void => {
    agent.capability(ECHO, echoHandler);
    agent.capability(SLEEP, sleepHandler);
    agent.capability(OFF, okHandler);
    agent.capability(PRIVATE, okHandler);
    agent.capability(STRICT, okHandler);
    agent.capability(HOLD, holdHandler);
  };
  const a = makeAgent(h, "alpha", { acceptsCommandsFrom: ["beta"], ...over.a }, over.timers);
  const b = makeAgent(h, "beta", { acceptsCommandsFrom: ["alpha"], ...over.b }, over.timers);
  register(a.agent);
  register(b.agent);
  // Beta starts first: with a long `nuntiumDebounce` (E3/E5/E9) alpha's INITIAL scan (step 4) is
  // the only discovery that runs in time, and it needs beta to be already published. The reverse
  // direction (beta seeing alpha) is not required by any scenario: A0's 106 check treats a peer
  // it does not know as a pass.
  await b.agent.start();
  await a.agent.start();
  await waitFor(() => a.agent.agmen().has("beta"), 6_000);
  return { root: h.root, a: a.agent, b: b.agent, ea: a.events, eb: b.events, sleepCalls, get holdCalls() { return holdCalls; }, releaseHold };
}

// --- the four §7 properties, all on REAL files --------------------------------------------------

const statusOf = (root: string, id: string): Promise<Status> =>
  fsp.readFile(path.join(root, id, "status.json"), "utf8").then((s) => JSON.parse(s) as Status);
const specDisabled = (root: string, id: string): Promise<boolean | undefined> =>
  fsp.readFile(path.join(root, id, "spec.json"), "utf8").then((s) => (JSON.parse(s) as { disabled?: boolean }).disabled);

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

/** The archived exchange of a diarium line (the four envelopes + final status). */
const exchangeOf = (line: DiariumLine): Exchange => line.event_data as unknown as Exchange;

const resBody = (ex: Exchange): ResponseBody | undefined =>
  ex.res !== null ? (ex.res as { response: ResponseBody }).response : undefined;

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

// --- E1 — normal request (Appendix A.1): confirmed / confirmed -----------------------------------

test("E1: a normal request — confirmed both sides, inbox order and empty after archive", opts, async (t) => {
  const h = await harness(t);
  const p = await pair(h);
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));
  const out = await p.a.request("beta", "echo", { text: "hi" });
  assert.equal(out.finalStatus, "confirmed");
  assert.equal(out.response?.error, false);
  assert.deepEqual(out.response?.result, { text: "echo: hi" });
  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 6_000);
  rec.stop();
  // (a) the receiver inbox saw exactly req → res → con, in protocol order
  const entries = [...byRid(rec).entries()];
  assert.equal(entries.length, 1, "one exchange, one rid");
  const [rid] = entries[0];
  assert.equal(rid.length, 26, "one exchange, one rid");
  assert.deepEqual(appearedSeq(rec, rid), ["req", "res", "con"], "appearances in protocol order");
  // (b) the inboxes are empty after the archive (every file removed from disk)
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  assert.deepEqual(await inboxNames(h.root, "alpha"), []);
  // (c) both diaria validate; the final statuses are confirmed / confirmed
  const aLines = await diaries(h.root, "alpha");
  const bLines = await diaries(h.root, "beta");
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  assert.deepEqual(finalsOf(aLines, "outreq"), ["confirmed"]);
  assert.deepEqual(finalsOf(bLines, "inreq"), ["confirmed"]);
  // (d) no attachments were involved, so the forum is empty
  assert.deepEqual(await forumNames(h.root), []);
});

// --- E2 — unknown capability: the local gate rejects, nothing is written -------------------------

test("E2: an unknown capability is rejected locally (callability gate) and nothing is written", opts, async (t) => {
  const h = await harness(t);
  const p = await pair(h);
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));
  await assert.rejects(
    () => p.a.request("beta", "nope", {}),
    (e: unknown) => e instanceof ActioCallError && /capability is not usable/.test(e.message),
  );
  rec.stop();
  // (a) the receiver inbox never saw a file
  assert.deepEqual(rec.events(), []);
  // (b) the inboxes are empty
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  assert.deepEqual(await inboxNames(h.root, "alpha"), []);
  // (c) both diaria validate and carry no exchange lines
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "alpha"), "outreq"), []);
  assert.deepEqual(finalsOf(await diaries(h.root, "beta"), "inreq"), []);
  // (d) the forum is empty
  assert.deepEqual(await forumNames(h.root), []);
});

// --- E3 — the seven acceptance codes ------------------------------------------------------------

test("E3: requests rejected 100, 102, 103, 105, 106, 107, 108 — matching code each", opts, async (t) => {
  const h = await harness(t);
  // A long debounce keeps alpha's view of beta stale after `enable(false)`, so the 100 request
  // really reaches beta's A0 (the same trick as agent.test.ts).
  const p = await pair(h, { timers: { nuntiumDebounce: 30_000 } });
  // 106 needs a requestor that shares no namespace with beta (distinct namespaces via AgentOptions).
  const delta = makeAgent(h, "delta", { namespaces: ["blue"], acceptsCommandsFrom: [] }, { nuntiumDebounce: 30_000 });
  await delta.agent.start();
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));

  // 100 agent_disabled: beta is disabled via the `enable` command; alpha's stale view lets the
  // request through the local gate, and beta's A0 (fresh in-memory spec) answers 100.
  await p.a.command("beta", "enable", { enabled: false });
  await waitFor(() => specDisabled(h.root, "beta").then((d) => d === true), 6_000);
  const r100 = await p.a.request("beta", "echo", { text: "x" });
  assert.equal(r100.finalStatus, "confirmed");
  assert.equal(r100.response?.error, true);
  assert.equal((r100.response?.result as { code: number }).code, 100, "agent_disabled");
  await p.a.command("beta", "enable", { enabled: true });
  await waitFor(() => specDisabled(h.root, "beta").then((d) => d === false), 6_000);

  // 101 unknown capability / 102 disabled capability / 103 requestor not allowed: the requestor's
  // local callability gate (rules 1/5.2) rejects before anything is written.
  await assert.rejects(() => p.a.request("beta", "nope", {}), (e: unknown) => e instanceof ActioCallError);
  await assert.rejects(() => p.a.request("beta", "off", {}), (e: unknown) => e instanceof ActioCallError);
  await assert.rejects(() => p.a.request("beta", "private", {}), (e: unknown) => e instanceof ActioCallError);
  // 106 namespace_mismatch: a requestor with a disjoint namespace never sees beta in its agmen.
  await assert.rejects(
    () => delta.agent.request("beta", "echo", { text: "x" }),
    (e: unknown) => e instanceof ActioCallError && /peer is not callable/.test(e.message),
  );

  // 105 invalid_parameters: parameters violating the capability schema.
  const r105 = await p.a.request("beta", "strict", { n: 1 });
  assert.equal(r105.finalStatus, "confirmed");
  assert.equal((r105.response?.result as { code: number }).code, 105, "invalid_parameters");

  // 107 capacity_exceeded: two parallel sleeps at max_parallel 1.
  const first = p.a.request("beta", "sleep", { seconds: 2 });
  await waitFor(
    () =>
      statusOf(h.root, "beta").then(
        (s) => s.capability_status.find((c) => c.name === "sleep")?.running === 1,
      ),
    6_000,
  );
  const second = await p.a.request("beta", "sleep", { seconds: 2 });
  assert.equal(second.finalStatus, "confirmed");
  assert.equal((second.response?.result as { code: number }).code, 107, "capacity_exceeded");
  const firstOut = await first;
  assert.equal(firstOut.finalStatus, "confirmed");
  assert.deepEqual(firstOut.response?.result, { slept: 2 });

  // 108 request_expired: a request whose timeout passed before acceptance, written directly into
  // beta's inbox (ts + timeout < now).
  const rid108 = ulid();
  const nowSec = Math.floor(Date.now() / 1000);
  const req108: RequestFile = {
    rid: rid108,
    src: "alpha",
    dst: "beta",
    datetime: toIso(Date.now()),
    type: "req",
    actio_version: "0.1",
    request: {
      ts: nowSec - 100,
      nonce: nonce(),
      signature: null,
      name: "echo",
      parameters: { text: "x" },
      timeout: 5,
      attachments: [],
    },
  };
  await io.writeJsonAtomic(path.join(h.root, "beta", "inbox", `${rid108}.json`), req108, path.join(h.root, "beta"));
  await waitFor(() => inboxNames(h.root, "beta").then((n) => !n.includes(`${rid108}.json`)), 8_000);
  rec.stop();

  // (b) the inbox is empty after every archive
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  assert.deepEqual(await inboxNames(h.root, "alpha"), []);

  // (a) per-rid inbox sequences: a command is req only; 108 is req+res (no confirmation — no
  // requestor); every answered request is req → res → con.
  const bLines = await diaries(h.root, "beta");
  const codeOf = (rid: string): number | null => {
    const line = bLines.find((l) => l.event === "inreq" && exchangeOf(l).req?.rid === rid);
    if (line === undefined) return null;
    const body = resBody(exchangeOf(line));
    if (body === undefined || body.error !== true) return null;
    return (body.result as { code: number }).code;
  };
  const seqs = byRid(rec);
  assert.equal(seqs.size, 7, "two commands, four answered requests, one 108 request");
  for (const [rid] of seqs) {
    const appearances = appearedSeq(rec, rid);
    const expected = rid === rid108 ? ["req", "res"] : appearances.includes("con") ? ["req", "res", "con"] : ["req"];
    assert.deepEqual(appearances, expected, `rid ${rid}: ${appearances.join(">")}`);
  }
  // the 108 rid really carries code 108, and the commands' rids are the two `enable`s
  assert.equal(codeOf(rid108), 108);

  // (c) both (three) diaria validate; the final statuses:
  // beta inreq: the two commands `completed`, 100/105/107×2 `confirmed`, 108 `completed` (no con)
  // alpha outreq: the two commands `sent`, four `confirmed`
  await assertDiariaValid(h.root, ["alpha", "beta", "delta"]);
  assert.deepEqual(finalsOf(bLines, "inreq").sort(), ["completed", "completed", "completed", "confirmed", "confirmed", "confirmed", "confirmed"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "alpha"), "outreq").sort(), ["confirmed", "confirmed", "confirmed", "confirmed", "sent", "sent"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "delta"), "outreq"), []);

  // (d) no attachments in any of these scenarios: the forum is empty
  assert.deepEqual(await forumNames(h.root), []);
});

// --- E4 — confirmation timeout: the requestor is gone before confirming --------------------------

test("E4: confirmation timeout — the receiver archives as `completed`", opts, async (t) => {
  const h = await harness(t);
  const p = await pair(h);
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));
  const outP = p.a.request("beta", "sleep", { seconds: 0.3 });
  // the request file is in beta's inbox (a bare `.json`, not a res/can/con)
  await waitFor(
    () =>
      inboxNames(h.root, "beta").then(
        (n) => n.some((x) => x.endsWith(".json") && !x.endsWith(".res.json") && !x.endsWith(".can.json") && !x.endsWith(".con.json")),
      ),
    6_000,
  );
  // the requestor stops BEFORE confirming: its exchange is abandoned, no confirmation ever comes.
  await p.a.stop();
  const out = await outP;
  assert.equal(out.finalStatus, "abandoned", "the requestor is gone: D-6 abandon");
  // fastConfig's confirmTimeout (1 s) fires on the receiver side: `completed`, then the archive.
  // A4 appends the inreq event BEFORE deleting the files, so the archive is complete when the
  // inbox is empty.
  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 8_000);
  rec.stop();
  // (a) req and res appeared, in order; no con ever came
  const [rid4] = [...byRid(rec).keys()];
  assert.deepEqual(appearedSeq(rec, rid4), ["req", "res"]);
  // (b) the inbox is empty after the archive
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  // (c) both diaria validate; receiver `completed`, requestor `abandoned`
  const bLines = await diaries(h.root, "beta");
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  assert.deepEqual(finalsOf(bLines, "inreq"), ["completed"]);
  assert.equal(resBody(exchangeOf(bLines.find((l) => l.event === "inreq")!))?.error, false);
  assert.deepEqual(finalsOf(await diaries(h.root, "alpha"), "outreq"), ["abandoned"]);
  // (d) the forum is empty
  assert.deepEqual(await forumNames(h.root), []);
});

// --- E5 — cancelled before acceptance (A.3): cancelled both, cancellation response ---------------

test("E5: cancelled before acceptance — cancelled both sides, cancellation response, no work", opts, async (t) => {
  const h = await harness(t);
  // A long debounce keeps alpha's view of beta usable after the stop; a long cancelWait so the
  // cancellation response (arriving after beta restarts) is captured before the abandon fallback.
  const p = await pair(h, { timers: { nuntiumDebounce: 30_000, cancelWaitTimeout: 3_000 } });
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));
  await p.b.stop(); // beta is inactive: the request file lands in a quiet inbox
  const outP = p.a.request("beta", "sleep", { seconds: 30 }, { timeout: 1 });
  // at t=1 s the requestor's timeout writes the cancellation (B4)
  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.some((x) => x.endsWith(".can.json"))), 6_000);
  await p.b.start(); // the initial scan delivers `.can.json` before `.json` (lexical)
  const out = await outP;
  assert.equal(out.finalStatus, "cancelled");
  assert.equal(out.response?.cancellation, true, "the A.3 cancellation response");
  assert.equal(out.response?.result, null);
  assert.equal(p.sleepCalls.length, 0, "no work was started (cancellation before acceptance)");
  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 8_000);
  rec.stop();
  // (a) req → can → res → con, in protocol order
  const [rid5] = [...byRid(rec).keys()];
  assert.deepEqual(appearedSeq(rec, rid5), ["req", "can", "res", "con"]);
  // (b) the inbox is empty
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  // (c) both diaria validate; cancelled / cancelled
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "beta"), "inreq"), ["cancelled"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "alpha"), "outreq"), ["cancelled"]);
  // (d) the forum is empty
  assert.deepEqual(await forumNames(h.root), []);
});

// --- E6 — cancelled during work (sleep): the handler signal is aborted ---------------------------

test("E6: cancelled during work — the handler signal aborts, cancelled both sides", opts, async (t) => {
  const h = await harness(t);
  const p = await pair(h);
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));
  const out = await p.a.request("beta", "sleep", { seconds: 30 }, { timeout: 1 });
  assert.equal(out.finalStatus, "cancelled");
  assert.equal(out.response?.cancellation, true);
  assert.equal(p.sleepCalls.length, 1, "the handler WAS started");
  assert.equal(p.sleepCalls[0].aborted, true, "A3: the handler's signal was aborted");
  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 8_000);
  rec.stop();
  // (a) req → can → res → con, in protocol order
  const [rid6] = [...byRid(rec).keys()];
  assert.deepEqual(appearedSeq(rec, rid6), ["req", "can", "res", "con"]);
  // (b) the inbox is empty
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  // (c) both diaria validate; cancelled / cancelled
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "beta"), "inreq"), ["cancelled"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "alpha"), "outreq"), ["cancelled"]);
  // (d) the forum is empty
  assert.deepEqual(await forumNames(h.root), []);
});

// --- E7 — cancelled after the response (A.4): single response with cancellation=false ------------

test("E7: cancelled after the response — no second response, receiver `cancelled`", opts, async (t) => {
  const h = await harness(t);
  const p = await pair(h);
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));
  const outP = p.a.request("beta", "hold", {});
  // the handler is running and the response is NOT written yet
  await waitFor(() => statusOf(h.root, "beta").then((s) => s.status === "busy"), 6_000);
  // the requestor stops BEFORE the response: it abandons (a requestor cannot cancel after it
  // captured a response — giveUp bails on ex.res !== null), so no confirmation will ever come.
  await p.a.stop();
  const out = await outP;
  assert.equal(out.finalStatus, "abandoned");
  // now the response is written …
  p.releaseHold();
  const rid = await waitForRidOfFirstRes(h.root);
  // … and the cancellation arrives AFTER it (A.4).
  const can = {
    rid,
    src: "alpha",
    dst: "beta",
    datetime: toIso(Date.now()),
    type: "req" as const,
    actio_version: "0.1",
    cancellation: { ts: Math.floor(Date.now() / 1000), nonce: nonce(), signature: null, name: "hold", force: false },
  };
  await io.writeJsonAtomic(path.join(h.root, "beta", "inbox", `${rid}.can.json`), can, path.join(h.root, "beta"));
  // no confirmation ever arrives: the confirmTimeout archives the exchange as `cancelled`.
  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 8_000);
  rec.stop();
  // (a) req, res and can appeared, in that order; the res appeared EXACTLY ONCE (no second response)
  const [rid7] = [...byRid(rec).keys()];
  const seq7 = appearedSeq(rec, rid7);
  assert.deepEqual(seq7, ["req", "res", "can"]);
  assert.equal(seq7.filter((x) => x === "res").length, 1, "the response file was written once");
  // (b) the inbox is empty
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  // (c) both diaria validate; the SINGLE response carried cancellation=false and the result
  const bLines = await diaries(h.root, "beta");
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  assert.deepEqual(finalsOf(bLines, "inreq"), ["cancelled"]);
  const body = resBody(exchangeOf(bLines.find((l) => l.event === "inreq")!));
  assert.equal(body?.cancellation, false, "the single response is the work response, not a cancellation");
  assert.equal(body?.error, false);
  assert.deepEqual(body?.result, { ok: true });
  assert.equal(p.holdCalls, 1);
  assert.deepEqual(finalsOf(await diaries(h.root, "alpha"), "outreq"), ["abandoned"]);
  // (d) the forum is empty
  assert.deepEqual(await forumNames(h.root), []);
});

/** Polls until exactly one `*.res.json` exists in beta's inbox; returns its rid. */
async function waitForRidOfFirstRes(root: string): Promise<string> {
  await waitFor(() => inboxNames(root, "beta").then((n) => n.filter((x) => x.endsWith(".res.json")).length === 1), 6_000);
  const [name] = await inboxNames(root, "beta").then((n) => n.filter((x) => x.endsWith(".res.json")));
  return name.slice(0, -".res.json".length);
}

// --- E8 — requestor timeout → cancel → response: cancelled ----------------------------------------

test("E8: requestor timeout → cancel → cancellation response — cancelled both sides", opts, async (t) => {
  const h = await harness(t);
  const p = await pair(h);
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));
  // the requestor's own timeout (D-6) writes the cancellation; the receiver answers it.
  const out = await p.a.request("beta", "sleep", { seconds: 30 }, { timeout: 1 });
  assert.equal(out.finalStatus, "cancelled");
  assert.equal(out.response?.cancellation, true);
  assert.equal(out.response?.result, null);
  await waitFor(() => inboxNames(h.root, "beta").then((n) => n.length === 0), 8_000);
  rec.stop();
  // (a) the full B4→B2→B3 sequence in the receiver inbox, in protocol order
  const [rid8] = [...byRid(rec).keys()];
  assert.deepEqual(appearedSeq(rec, rid8), ["req", "can", "res", "con"]);
  // (b) the inbox is empty
  assert.deepEqual(await inboxNames(h.root, "beta"), []);
  // (c) both diaria validate; cancelled / cancelled
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "beta"), "inreq"), ["cancelled"]);
  assert.deepEqual(finalsOf(await diaries(h.root, "alpha"), "outreq"), ["cancelled"]);
  // (d) the forum is empty
  assert.deepEqual(await forumNames(h.root), []);
});

// --- E9 — receiver inactive → abandon: outreq abandoned, attachments removed, files stay ----------

test("E9: receiver inactive — outreq `abandoned`, attachments removed, files stay in the inbox", opts, async (t) => {
  const h = await harness(t);
  // A long debounce keeps alpha's view of beta usable after the stop; a short cancelWait keeps
  // the D-6 give-up (timeout → cancel → bounded wait → abandon) fast.
  const p = await pair(h, { timers: { nuntiumDebounce: 30_000, cancelWaitTimeout: 300 } });
  const rec = recordInbox(path.join(h.root, "beta", "inbox"));
  const att = path.join(h.root, "alpha", "att.txt");
  await fsp.writeFile(att, "attachment payload");
  await p.b.stop(); // beta is inactive: it will never answer
  const out = await p.a.request("beta", "echo", { text: "x" }, { timeout: 1, attachments: [att] });
  assert.equal(out.finalStatus, "abandoned");
  // (d) the request attachment was copied into the forum and removed again at the abandon (D-6)
  assert.deepEqual(await forumNames(h.root), [], "the attachment is cleaned even on abandon");
  // (c) both diaria validate; the outreq is `abandoned` and its envelope carried the attachment
  const aLines = await diaries(h.root, "alpha");
  await assertDiariaValid(h.root, ["alpha", "beta"]);
  assert.deepEqual(finalsOf(aLines, "outreq"), ["abandoned"]);
  const ex = exchangeOf(aLines.find((l) => l.event === "outreq")!);
  assert.deepEqual((ex.req?.request as { attachments: string[] }).attachments, ["att.txt"]);
  assert.equal(ex.res, null, "no response was ever received");
  assert.deepEqual(finalsOf(await diaries(h.root, "beta"), "inreq"), [], "beta never processed the request");
  // (b, the exception) the envelope files REMAIN in beta's inbox for its next start
  const names = (await inboxNames(h.root, "beta")).sort();
  assert.deepEqual(names, [`${ex.req!.rid}.can.json`, `${ex.req!.rid}.json`]);
  rec.stop();
  // (a) req and can appeared, in order, and (per (b) below) neither was ever removed
  const [rid9] = [...byRid(rec).keys()];
  assert.deepEqual(appearedSeq(rec, rid9), ["req", "can"]);
});