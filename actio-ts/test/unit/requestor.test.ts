// Tests of the requestor (WP6-A-1, lane B). A scripted fake receiver writes `.res.json` files
// by hand into the destination inbox; the diarium `outreq` content is asserted against the
// actual envelope files.
import assert from "node:assert/strict";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test, type TestContext } from "node:test";
import { append } from "../../src/diarium.js";
import { Requestor, type RequestorHooks } from "../../src/requestor.js";
import { sleep } from "../../src/util.js";
import type { ActioConfig, ActioEvent, AgentContext, DiariumLine } from "../../src/types.js";
import { fakeClock, fastConfig, makeRoot, waitFor } from "../helpers/index.js";

const NOP_SYNC = { fsync: false, delayMs: 0, notify(): void {}, flush: async () => {}, close: async () => {} };

type OutreqData = { final_status: string; req: unknown; can: unknown; res: unknown; con: unknown };

interface Fixture {
  root: string;
  ctx: AgentContext;
  requestor: Requestor;
  diarium: string;
  alphaInbox: string;
  alphaForum: string;
  betaInbox: string;
  gammaInbox: string;
  watchErrors: Array<[string, Error]>;
  stateChanges: number[];
  emitted: ActioEvent[];
}

/** Builds one agent's folders + context under `root`; `overrides` shrink the test timers. */
async function makeAgent(t: TestContext, root: string, id: string, clock: () => number, overrides?: Partial<ActioConfig>, onEmit?: (e: ActioEvent) => void): Promise<AgentContext> {
  const cfg = fastConfig(root, clock, { cancelWaitTimeout: 200, ...overrides });
  const folders = {
    root,
    agent: path.join(root, id, "agent"),
    inbox: path.join(root, id, "inbox"),
    forum: path.join(root, id, "forum"),
    nuntium: path.join(root, id, "nuntium"),
  };
  for (const dir of Object.values(folders)) await fsp.mkdir(dir, { recursive: true });
  const ctx: AgentContext = {
    config: cfg,
    id,
    folders,
    sync: NOP_SYNC,
    now: () => cfg.clock(),
    spec: (() => {
      throw new Error("spec() is not used by the requestor tests");
    }) as AgentContext["spec"],
    state: () => "idle",
    emit: (e) => onEmit?.(e),
    appendDiarium: (e) => append(folders.agent, e),
  };
  return ctx;
}

/** Alpha (requestor) + beta/gamma (fake receivers) under one temp root. */
async function makeFixture(t: TestContext): Promise<Fixture> {
  const root = await makeRoot(t);
  const clock = fakeClock();
  const emitted: ActioEvent[] = [];
  const ctx = await makeAgent(t, root, "alpha", clock, undefined, (e) => emitted.push(e));
  await makeAgent(t, root, "beta", clock);
  await makeAgent(t, root, "gamma", clock);
  const watchErrors: Array<[string, Error]> = [];
  const stateChanges: number[] = [];
  const hooks: RequestorHooks = {
    onWatchError: (dir, err) => watchErrors.push([dir, err]),
    onStateChange: () => stateChanges.push(1),
  };
  const requestor = new Requestor(ctx, hooks);
  t.after(() => requestor.stop());
  return {
    root,
    ctx,
    requestor,
    diarium: path.join(root, "alpha", "agent", "diarium.jsonl"),
    alphaInbox: path.join(root, "alpha", "inbox"),
    alphaForum: path.join(root, "alpha", "forum"),
    betaInbox: path.join(root, "beta", "inbox"),
    gammaInbox: path.join(root, "gamma", "inbox"),
    watchErrors,
    stateChanges,
    emitted,
  };
}

/** The fake receiver: writes `<rid>.res.json` in `inbox` with the request header AS-IS (the real
 * receiver copies the tracked header into the response, src/dst not swapped). */
async function writeResponse(inbox: string, rid: string, body: Record<string, unknown>): Promise<void> {
  const reqFile = JSON.parse(await fsp.readFile(path.join(inbox, `${rid}.json`), "utf8"));
  const { request: _request, ...reqHeader } = reqFile;
  const res = { ...reqHeader, datetime: "2026-10-24T10:01:10Z", type: "req", response: body };
  await fsp.writeFile(path.join(inbox, `${rid}.res.json`), JSON.stringify(res));
}

/** A request file name (not a can/res/con envelope). */
const isReqFile = (n: string): boolean => n.endsWith(".json") && !n.includes(".can.") && !n.includes(".res.") && !n.includes(".con.");

const resBody = (over: Record<string, unknown> = {}): Record<string, unknown> => ({
  ts: 1_792_836_070,
  nonce: "a688e91bc1a01ba05c68c4ec37aa9dfe",
  signature: null,
  error: false,
  cancellation: false,
  result: "ok",
  attachments: [],
  ...over,
});

/** The rid of the request file in `inbox` (the first `.json` that is not a can/res/con). */
async function ridOf(inbox: string): Promise<string> {
  const names = await fsp.readdir(inbox);
  const req = names.find((n) => n.endsWith(".json") && !n.includes(".can.") && !n.includes(".res.") && !n.includes(".con."));
  assert.ok(req, `no request file in ${inbox}: ${names.join(", ")}`);
  return req!.slice(0, -".json".length);
}

async function diariumLines(p: string): Promise<DiariumLine[]> {
  try {
    const raw = await fsp.readFile(p, "utf8");
    return raw.split("\n").filter(Boolean).map((l) => JSON.parse(l) as DiariumLine);
  } catch {
    return [];
  }
}

async function outreqData(diarium: string, rid: string): Promise<OutreqData | undefined> {
  const lines = await diariumLines(diarium);
  const line = lines.find((l) => l.event === "outreq" && (l.event_data as OutreqData).req !== null && ((l.event_data as OutreqData).req as { rid?: string }).rid === rid);
  return line ? (line.event_data as OutreqData) : undefined;
}

async function makeAttachment(t: TestContext): Promise<string> {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "actio-attach-"));
  t.after(() => fsp.rm(dir, { recursive: true, force: true }));
  const p = path.join(dir, "input.txt");
  await fsp.writeFile(p, "attachment payload");
  return p;
}

test("command: writes the cmd file and diariums outreq sent (req only)", async (t) => {
  const fx = await makeFixture(t);
  const rid = await fx.requestor.command("beta", "stop", { force: false });
  assert.match(rid, /^[0-7][0-9A-HJKMNP-TV-Z]{25}$/);
  const reqFile = JSON.parse(await fsp.readFile(path.join(fx.betaInbox, `${rid}.json`), "utf8"));
  assert.equal(reqFile.type, "cmd");
  assert.equal(reqFile.src, "alpha");
  assert.equal(reqFile.dst, "beta");
  assert.equal(reqFile.actio_version, "0.1");
  assert.deepEqual(reqFile.request, {
    ts: Math.floor(1_700_000_000_000 / 1000),
    nonce: reqFile.request.nonce,
    signature: null,
    name: "stop",
    parameters: { force: false },
    timeout: 0,
    attachments: [],
  });
  assert.equal(reqFile.request.nonce.length, 32);
  const data = await outreqData(fx.diarium, rid);
  assert.ok(data, "diarium has an outreq line for the command");
  assert.equal(data!.final_status, "sent");
  assert.deepEqual(data!.req, reqFile);
  assert.equal(data!.can, null);
  assert.equal(data!.res, null);
  assert.equal(data!.con, null);
  assert.ok(fx.emitted.some((e) => e.event === "outreq" && e.rid === rid && e.finalStatus === "sent"), "the stream carries outreq sent");
  assert.equal(fx.requestor.outboundCount(), 0);
});

test("request: confirmed — response, confirmation, outreq matches the files, B5 cleanup", async (t) => {
  const fx = await makeFixture(t);
  const attach = await makeAttachment(t);
  const p = fx.requestor.request("beta", "summarize", { text: "hello" }, { attachments: [attach] });
  // B1: the attachment is in the forum before the request completes
  await waitFor(() => fsp.readdir(fx.betaInbox).then((n) => n.some((x) => x.endsWith(".json"))), 2_000);
  assert.equal(fx.requestor.outboundCount(), 1);
  const rid = await ridOf(fx.betaInbox);
  await waitFor(() => fsp.access(path.join(fx.alphaForum, "input.txt")).then(() => true).catch(() => false), 2_000);
  // the fake receiver answers
  await writeResponse(fx.betaInbox, rid, resBody());
  const outcome = await p;
  assert.equal(outcome.finalStatus, "confirmed");
  assert.equal(outcome.response?.result, "ok");
  assert.equal(fx.requestor.outboundCount(), 0);
  // B3: the confirmation file exists and the diarium matches the envelopes exactly
  const data = await outreqData(fx.diarium, rid);
  assert.ok(data);
  assert.equal(data!.final_status, "confirmed");
  assert.deepEqual(data!.req, JSON.parse(await fsp.readFile(path.join(fx.betaInbox, `${rid}.json`), "utf8")));
  assert.deepEqual(data!.res, JSON.parse(await fsp.readFile(path.join(fx.betaInbox, `${rid}.res.json`), "utf8")));
  assert.deepEqual(data!.con, JSON.parse(await fsp.readFile(path.join(fx.betaInbox, `${rid}.con.json`), "utf8")));
  assert.equal(data!.can, null);
  assert.ok(fx.emitted.some((e) => e.event === "outreq" && e.rid === rid && e.finalStatus === "confirmed"), "the stream carries outreq confirmed");
  // B5: the request attachment is deleted from the forum
  await assert.rejects(fsp.access(path.join(fx.alphaForum, "input.txt")));
  assert.ok(fx.stateChanges.length >= 2, `onStateChange on 0<->nonzero transitions (got ${fx.stateChanges.length})`);
});

test("R1-1: two concurrent requests to the same dst, staggered responses, both confirmed", async (t) => {
  const fx = await makeFixture(t);
  const p1 = fx.requestor.request("beta", "summarize", { text: "a" }, {});
  const p2 = fx.requestor.request("beta", "summarize", { text: "b" }, {});
  await waitFor(async () => (await fsp.readdir(fx.betaInbox)).filter(isReqFile).length === 2, 2_000);
  const rids = (await fsp.readdir(fx.betaInbox)).filter(isReqFile).map((n) => n.slice(0, -".json".length)).sort();
  // the responses arrive at different times: the second only after the first completes
  await writeResponse(fx.betaInbox, rids[0], resBody());
  const o1 = await Promise.race([p1, p2]);
  assert.equal(o1.finalStatus, "confirmed");
  assert.equal(fx.requestor.outboundCount(), 1, "the second exchange is still in B2 — the watcher must stay open");
  await writeResponse(fx.betaInbox, rids[1], resBody());
  const [oa, ob] = await Promise.all([p1, p2]);
  assert.equal(oa.finalStatus, "confirmed");
  assert.equal(ob.finalStatus, "confirmed");
  assert.equal(fx.requestor.outboundCount(), 0);
});

test("R1-1: the dst watcher is closed after the last exchange — deleting the folder fires no watch error", async (t) => {
  const fx = await makeFixture(t);
  const p = fx.requestor.request("beta", "summarize", { text: "a" }, {});
  await waitFor(() => fsp.readdir(fx.betaInbox).then((n) => n.some(isReqFile)), 2_000);
  const rid = await ridOf(fx.betaInbox);
  await writeResponse(fx.betaInbox, rid, resBody());
  assert.equal((await p).finalStatus, "confirmed");
  // a still-open watcher would notice the deletion within milliseconds (the D-22 test proves it)
  await sleep(200);
  await fsp.rm(fx.betaInbox, { recursive: true });
  await sleep(400);
  assert.equal(fx.watchErrors.length, 0, "the watcher was released with the last exchange (D-18)");
});

test("R1-6: a response with a mismatched header is ignored and logged once (D-11); the exchange stays in B2", async (t) => {
  const fx = await makeFixture(t);
  const p = fx.requestor.request("beta", "summarize", { text: "a" }, {});
  await waitFor(() => fsp.readdir(fx.betaInbox).then((n) => n.some(isReqFile)), 2_000);
  const rid = await ridOf(fx.betaInbox);
  // a schema-valid response whose header differs from the tracked request (src is not the requester)
  const reqFile = JSON.parse(await fsp.readFile(path.join(fx.betaInbox, `${rid}.json`), "utf8"));
  const { request: _request, ...reqHeader } = reqFile;
  await fsp.writeFile(
    path.join(fx.betaInbox, `${rid}.res.json`),
    JSON.stringify({ ...reqHeader, src: "gamma", datetime: "2026-10-24T10:01:10Z", type: "req", response: resBody() }),
  );
  // the mismatch is logged once (deduplicated per (path, mtime)) — emit + diarium `log` line
  await waitFor(async () => (await diariumLines(fx.diarium)).filter((l) => l.event === "log").length === 1, 2_000);
  const logEmits = fx.emitted.filter((e) => e.event === "log" && e.message.includes("header mismatch (D-11)"));
  assert.equal(logEmits.length, 1);
  const logLine = (await diariumLines(fx.diarium)).find((l) => l.event === "log")!;
  assert.match(logLine.event_data as unknown as string, /ignored .*header mismatch \(D-11\)/);
  // the exchange stays in B2 (timers keep running) and is not resolved
  assert.equal(fx.requestor.outboundCount(), 1);
  // the correct response completes the exchange
  await writeResponse(fx.betaInbox, rid, resBody());
  assert.equal((await p).finalStatus, "confirmed");
  // a second identical bad file (same path, new mtime) would log again; the same file does not
  const logs = (await diariumLines(fx.diarium)).filter((l) => l.event === "log");
  assert.equal(logs.length, 1);
});

test("request: cancelled before the response — timeout writes can, cancellation response, single res", async (t) => {
  const fx = await makeFixture(t);
  const p = fx.requestor.request("beta", "summarize", { text: "hello" }, { timeout: 0.15 });
  await waitFor(() => fsp.readdir(fx.betaInbox).then((n) => n.some((x) => x.endsWith(".can.json"))), 2_000);
  const rid = await ridOf(fx.betaInbox);
  const canFile = JSON.parse(await fsp.readFile(path.join(fx.betaInbox, `${rid}.can.json`), "utf8"));
  assert.equal(canFile.cancellation.name, "summarize");
  assert.equal(canFile.cancellation.force, false);
  // the fake receiver sees the cancellation and answers it
  await writeResponse(fx.betaInbox, rid, resBody({ cancellation: true, result: null }));
  const outcome = await p;
  assert.equal(outcome.finalStatus, "cancelled");
  assert.equal(outcome.response?.cancellation, true);
  // exactly one response file, a confirmation, and a diarium line matching the files
  const names = await fsp.readdir(fx.betaInbox);
  assert.deepEqual(names.filter((n) => n.endsWith(".res.json")), [`${rid}.res.json`]);
  const data = await outreqData(fx.diarium, rid);
  assert.ok(data);
  assert.equal(data!.final_status, "cancelled");
  assert.deepEqual(data!.can, canFile);
  assert.deepEqual(data!.res, JSON.parse(await fsp.readFile(path.join(fx.betaInbox, `${rid}.res.json`), "utf8")));
  assert.deepEqual(data!.con, JSON.parse(await fsp.readFile(path.join(fx.betaInbox, `${rid}.con.json`), "utf8")));
});

test("request: cancelled after the response — can written after the res file, still cancelled", async (t) => {
  const fx = await makeFixture(t);
  const p = fx.requestor.request("beta", "summarize", { text: "hello" }, { timeout: 5 });
  await sleep(100);
  const rid = await ridOf(fx.betaInbox);
  // the response file lands, but invisible to the watcher (dot file) until after the cancellation
  const hidden = path.join(fx.betaInbox, `.hidden-${rid}.res.json`);
  await writeResponse(fx.betaInbox, rid, resBody());
  await fsp.rename(path.join(fx.betaInbox, `${rid}.res.json`), hidden);
  await fx.requestor.cancel(rid);
  await fsp.rename(hidden, path.join(fx.betaInbox, `${rid}.res.json`));
  const outcome = await p;
  assert.equal(outcome.finalStatus, "cancelled");
  assert.equal(outcome.response?.result, "ok");
  const names = await fsp.readdir(fx.betaInbox);
  assert.deepEqual(names.filter((n) => n.endsWith(".res.json")), [`${rid}.res.json`]);
  assert.ok(names.includes(`${rid}.can.json`));
  assert.ok(names.includes(`${rid}.con.json`));
  const data = await outreqData(fx.diarium, rid);
  assert.equal(data?.final_status, "cancelled");
});

test("request: abandoned — timeout then no response within cancelWaitTimeout; attachments deleted", async (t) => {
  const fx = await makeFixture(t);
  const attach = await makeAttachment(t);
  const p = fx.requestor.request("beta", "summarize", { text: "hello" }, { timeout: 0.1, attachments: [attach] });
  const outcome = await p;
  assert.equal(outcome.finalStatus, "abandoned");
  assert.equal(outcome.response, undefined);
  const rid = await ridOf(fx.betaInbox);
  const names = await fsp.readdir(fx.betaInbox);
  assert.ok(names.includes(`${rid}.can.json`), "the timeout wrote the cancellation");
  assert.ok(!names.some((n) => n.endsWith(".res.json")));
  assert.ok(!names.some((n) => n.endsWith(".con.json")), "no confirmation for an abandoned exchange");
  await assert.rejects(fsp.access(path.join(fx.alphaForum, "input.txt")), "the request attachments are deleted");
  const data = await outreqData(fx.diarium, rid);
  assert.ok(data);
  assert.equal(data!.final_status, "abandoned");
  assert.deepEqual(data!.req, JSON.parse(await fsp.readFile(path.join(fx.betaInbox, `${rid}.json`), "utf8")));
  assert.deepEqual(data!.can, JSON.parse(await fsp.readFile(path.join(fx.betaInbox, `${rid}.can.json`), "utf8")));
  assert.equal(data!.res, null);
  assert.equal(data!.con, null);
  assert.equal(fx.requestor.outboundCount(), 0);
});

test("request: D-13 — receiver archived before confirmation, no .con.json written", async (t) => {
  const fx = await makeFixture(t);
  const p = fx.requestor.request("beta", "summarize", { text: "hello" }, {});
  await waitFor(() => fsp.readdir(fx.betaInbox).then((n) => n.some((x) => x.endsWith(".json") && !x.endsWith(".res.json"))), 2_000);
  const rid = await ridOf(fx.betaInbox);
  // the receiver archived the exchange (deleted the request file) before answering
  const reqFile = JSON.parse(await fsp.readFile(path.join(fx.betaInbox, `${rid}.json`), "utf8"));
  const { request: _request, ...reqHeader } = reqFile;
  await fsp.rm(path.join(fx.betaInbox, `${rid}.json`));
  await fsp.writeFile(
    path.join(fx.betaInbox, `${rid}.res.json`),
    JSON.stringify({ ...reqHeader, datetime: "2026-10-24T10:01:10Z", type: "req", response: resBody() }),
  );
  const outcome = await p;
  assert.equal(outcome.finalStatus, "confirmed");
  assert.equal(outcome.response?.result, "ok");
  const names = await fsp.readdir(fx.betaInbox);
  assert.ok(!names.some((n) => n.endsWith(".con.json")), "the confirmation is skipped (D-13)");
  const data = await outreqData(fx.diarium, rid);
  assert.equal(data?.final_status, "confirmed");
  assert.equal(data?.con, null);
  assert.deepEqual(data?.res, JSON.parse(await fsp.readFile(path.join(fx.betaInbox, `${rid}.res.json`), "utf8")));
});

test("request: D-22 — deleting the dst inbox abandons the exchange and surfaces the watch error", async (t) => {
  const fx = await makeFixture(t);
  const p = fx.requestor.request("beta", "summarize", { text: "hello" }, {});
  await waitFor(() => fsp.readdir(fx.betaInbox).then((n) => n.some((x) => x.endsWith(".json"))), 2_000);
  await fsp.rm(fx.betaInbox, { recursive: true });
  const outcome = await p;
  assert.equal(outcome.finalStatus, "abandoned");
  assert.equal(fx.watchErrors.length, 1);
  assert.equal(fx.watchErrors[0][0], fx.betaInbox);
  assert.ok(fx.watchErrors[0][1] instanceof Error);
  const data = (await diariumLines(fx.diarium)).find((l) => l.event === "outreq") as DiariumLine | undefined;
  assert.ok(data);
  assert.equal((data.event_data as OutreqData).final_status, "abandoned");
});

test("request: signal abort triggers B4, then abandon when no cancellation response", async (t) => {
  const fx = await makeFixture(t);
  const ac = new AbortController();
  const p = fx.requestor.request("beta", "summarize", { text: "hello" }, { signal: ac.signal });
  await waitFor(() => fsp.readdir(fx.betaInbox).then((n) => n.some((x) => x.endsWith(".json"))), 2_000);
  await sleep(50);
  ac.abort();
  const outcome = await p;
  assert.equal(outcome.finalStatus, "abandoned");
  const names = await fsp.readdir(fx.betaInbox);
  assert.ok(names.some((n) => n.endsWith(".can.json")), "the abort wrote the cancellation");
  assert.ok(!names.some((n) => n.endsWith(".con.json")));
});

test("stop: abandons every outstanding exchange and releases the watchers", async (t) => {
  const fx = await makeFixture(t);
  const p1 = fx.requestor.request("beta", "summarize", { text: "a" }, {});
  const p2 = fx.requestor.request("gamma", "summarize", { text: "b" }, {});
  await waitFor(
    async () => {
      const b = await fsp.readdir(fx.betaInbox);
      const g = await fsp.readdir(fx.gammaInbox);
      return b.some((x) => x.endsWith(".json")) && g.some((x) => x.endsWith(".json"));
    },
    2_000,
  );
  assert.equal(fx.requestor.outboundCount(), 2);
  await fx.requestor.stop();
  const [o1, o2] = await Promise.all([p1, p2]);
  assert.equal(o1.finalStatus, "abandoned");
  assert.equal(o2.finalStatus, "abandoned");
  assert.equal(fx.requestor.outboundCount(), 0);
  const lines = (await diariumLines(fx.diarium)).filter((l) => l.event === "outreq");
  assert.equal(lines.length, 2);
  for (const l of lines) assert.equal((l.event_data as OutreqData).final_status, "abandoned");
});

test("notifyFile: a self-call response fed through the own inbox is captured", async (t) => {
  const fx = await makeFixture(t);
  const p = fx.requestor.request("alpha", "summarize", { text: "self" }, {});
  await waitFor(() => fsp.readdir(fx.alphaInbox).then((n) => n.some((x) => x.endsWith(".json"))), 2_000);
  const [ridFile] = await fsp.readdir(fx.alphaInbox);
  const rid = ridFile.slice(0, -".json".length);
  // the receiver side (not under test) would feed the own-inbox event; simulate it
  await writeResponse(fx.alphaInbox, rid, resBody());
  fx.requestor.notifyFile(fx.alphaInbox, `${rid}.res.json`);
  const outcome = await p;
  assert.equal(outcome.finalStatus, "confirmed");
  assert.equal(outcome.response?.result, "ok");
  assert.ok(await fsp.access(path.join(fx.alphaInbox, `${rid}.con.json`)).then(() => true).catch(() => false));
});

test("missing dst folder: request and command reject locally, nothing is written", async (t) => {
  const fx = await makeFixture(t);
  await assert.rejects(() => fx.requestor.command("ghost", "stop", {}));
  await assert.rejects(() => fx.requestor.request("ghost", "summarize", {}, {}));
  await assert.rejects(fsp.access(path.join(fx.root, "ghost")));
});