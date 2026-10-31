// Tests of the agent (WP7): the lifecycle spine (steps 1–5, lane C), the D-8 status policy, the
// discovery wiring (D-3, D-17), the callability gate of request()/command(), the built-in commands'
// lifecycle side (D2, D-10), D-22, D-12 and the D-19 peer cache, and the observability stream.
// Every agent of a test is stopped before its folder is removed: after-hooks run in order, so one
// hook does both, in that order.
import assert from "node:assert/strict";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test, type TestContext } from "node:test";
import { ActioAgent } from "../../src/agent.js";
import * as io from "../../src/io.js";
import { buildSpec } from "../../src/spec-builder.js";
import { buildStatus } from "../../src/status.js";
import { sleep, toIso } from "../../src/util.js";
import { ActioCallError, ActioResultError } from "../../src/types.js";
import { fastConfig, waitFor } from "../helpers/index.js";
import type { ActioConfig, ActioEvent, AgentOptions, CapabilityHandler, CommandContext } from "../../src/types.js";
import type { Announce, Capability, DiariumLine, Spec, Status } from "../../src/generated/types.js";

/** Each test gets a timeout: a stuck exchange fails the test instead of hanging the suite. */
const opts = { timeout: 20_000 };

const STRICT_PARAMS = { type: "object", properties: {}, additionalProperties: false };

const ECHO: Capability = {
  name: "echo",
  description: "echoes the text it gets",
  parameters: {
    type: "object",
    properties: { text: { type: "string" } },
    required: ["text"],
    additionalProperties: false,
  },
  actio: { result: { type: "object", properties: { text: { type: "string" } }, required: ["text"] } },
};

/** Its result schema demands `{slept}`; the cancellation response of a cancelled exchange carries null. */
const SLOW: Capability = {
  name: "slow",
  description: "waits, and reports how long",
  parameters: {
    type: "object",
    properties: { seconds: { type: "number" } },
    required: ["seconds"],
    additionalProperties: false,
  },
  actio: { result: { type: "object", properties: { slept: { type: "number" } }, required: ["slept"] } },
};

/** The handler of `bad` answers something its own `actio.result` schema rejects (D-12). */
const BAD: Capability = {
  name: "bad",
  description: "answers out of its own schema",
  parameters: STRICT_PARAMS as Capability["parameters"],
  actio: { result: { type: "object", properties: { n: { type: "number" } }, required: ["n"] } },
};

const echoHandler: CapabilityHandler = async (params) => ({ text: `echo: ${(params as { text: string }).text}` });

const slowHandler: CapabilityHandler = async (params) => {
  const seconds = (params as { seconds: number }).seconds;
  await sleep(seconds * 1000);
  return { slept: seconds };
};

const badHandler: CapabilityHandler = async () => ({ oops: 1 });

interface Harness {
  root: string;
  agents: ActioAgent[];
}

async function harness(t: TestContext): Promise<Harness> {
  const root = await fsp.mkdtemp(path.join(os.tmpdir(), ".actio-test-"));
  const h: Harness = { root, agents: [] };
  t.after(async () => {
    for (const a of h.agents) await a.stop(); // a NOP when it never started or already stopped
    await fsp.rm(root, { recursive: true, force: true });
  });
  return h;
}

function agent(
  h: Harness,
  id: string,
  over: Partial<AgentOptions> = {},
  timers: Partial<ActioConfig> = {},
): { agent: ActioAgent; events: ActioEvent[] } {
  const events: ActioEvent[] = [];
  const a = new ActioAgent({
    id,
    description: `${id} test agent`,
    acceptsCommandsFrom: [],
    ...over,
    config: { ...fastConfig(h.root, Date.now), ...timers },
  });
  a.on("event", (e) => events.push(e));
  h.agents.push(a);
  return { agent: a, events };
}

/** Two agents in one process, mutually commandable, both serving `echo`, mutually discovered. */
async function pair(
  h: Harness,
  before?: (alpha: ActioAgent, beta: ActioAgent) => void,
  over: { a?: Partial<AgentOptions>; b?: Partial<AgentOptions>; timers?: Partial<ActioConfig> } = {},
): Promise<{ a: ActioAgent; b: ActioAgent; ea: ActioEvent[]; eb: ActioEvent[] }> {
  const a = agent(h, "alpha", { acceptsCommandsFrom: ["beta"], ...over.a }, over.timers);
  const b = agent(h, "beta", { acceptsCommandsFrom: ["alpha"], ...over.b }, over.timers);
  a.agent.capability(ECHO, echoHandler);
  b.agent.capability(ECHO, echoHandler);
  before?.(a.agent, b.agent);
  await a.agent.start();
  await b.agent.start();
  await waitFor(() => a.agent.agmen().has("beta") && b.agent.agmen().has("alpha"), 6_000);
  return { a: a.agent, b: b.agent, ea: a.events, eb: b.events };
}

async function json<T>(file: string): Promise<T> {
  return JSON.parse(await fsp.readFile(file, "utf8")) as T;
}

const statusOf = (root: string, id: string): Promise<Status> => json(path.join(root, id, "status.json"));
const specOf = (root: string, id: string): Promise<Spec> => json(path.join(root, id, "spec.json"));
const announceOf = (root: string, id: string): Promise<Announce> =>
  json(path.join(root, "nuntium", `${id}.n.json`));
const inboxOf = (root: string, id: string): Promise<string[]> => io.listDir(path.join(root, id, "inbox"));

async function diaryOf(root: string, id: string): Promise<DiariumLine[]> {
  const raw = await fsp.readFile(path.join(root, id, "diarium.jsonl"), "utf8");
  return raw
    .split("\n")
    .filter((l) => l !== "")
    .map((l) => JSON.parse(l) as DiariumLine);
}

/** The `status` events of a diarium, in order. */
const statusEvents = (lines: DiariumLine[]): string[] =>
  lines.filter((l) => l.event === "status").map((l) => String(l.event_data));

/** The `final_status` of the `inreq` (or `outreq`) lines of a diarium, in order. */
const finalsOf = (lines: DiariumLine[], event: "inreq" | "outreq"): string[] =>
  lines.filter((l) => l.event === event).map((l) => String((l.event_data as { final_status?: string }).final_status));

const mtimeOf = (file: string): Promise<number> => fsp.stat(file).then((s) => s.mtimeMs);

/** The broadcast signal of `nuntium/nuntium.json`: one rescan for every agent (D-17). */
async function broadcast(root: string, text = "rescan please"): Promise<void> {
  const now = Date.now();
  const file: Announce = { id: "ops", datetime: toIso(now), ts: Math.floor(now / 1000), nuntium: text };
  await io.writeJsonAtomic(path.join(root, "nuntium", "nuntium.json"), file, root);
}

test("two agents in one process discover each other and themselves", opts, async (t) => {
  const h = await harness(t);
  const a = agent(h, "alpha", { acceptsCommandsFrom: ["beta"] });
  assert.deepEqual([...a.agent.agmen().keys()], [], "the agmen is empty before start()");
  const b = agent(h, "beta", { acceptsCommandsFrom: ["alpha"] });
  a.agent.capability(ECHO, echoHandler);
  b.agent.capability(ECHO, echoHandler);
  await a.agent.start();
  await b.agent.start();
  // The first agent learns about the second one through its announce file (D-17 debounce).
  await waitFor(() => a.agent.agmen().has("beta") && b.agent.agmen().has("alpha"), 6_000);
  for (const [who, self, other] of [
    [a.agent, "alpha", "beta"],
    [b.agent, "beta", "alpha"],
  ] as const) {
    const view = who.agmen();
    assert.deepEqual([...view.keys()].sort(), ["alpha", "beta"], `${self} sees both`);
    const peer = view.get(other)!;
    assert.equal(peer.id, other);
    assert.equal(peer.folder, path.join(h.root, other));
    assert.equal(peer.health, "healthy", "a fresh status is healthy (rule 8)");
    assert.deepEqual([...peer.capabilities.keys()], ["echo"]);
    assert.equal(peer.commands, true, "the peer lists us in accepts_commands_from (rule 5.1)");
  }
});

test("an echo request flows end to end and A4 leaves nothing behind", opts, async (t) => {
  const h = await harness(t);
  const { a, eb } = await pair(h);
  const out = await a.request("beta", "echo", { text: "hi" });
  assert.equal(out.finalStatus, "confirmed");
  assert.equal(out.response?.error, false);
  assert.deepEqual(out.response?.result, { text: "echo: hi" });
  // B3 completes the requestor's side; A4's archive on the peer side lands a moment later.
  await waitFor(async () => finalsOf(await diaryOf(h.root, "beta"), "inreq").length === 1, 6_000);
  assert.deepEqual(await inboxOf(h.root, "beta"), [], "A4 removed every envelope of the exchange");
  assert.deepEqual(await inboxOf(h.root, "alpha"), []);
  assert.deepEqual(finalsOf(await diaryOf(h.root, "beta"), "inreq"), ["confirmed"]);
  assert.deepEqual(finalsOf(await diaryOf(h.root, "alpha"), "outreq"), ["confirmed"]);
  assert.ok(eb.some((e) => e.event === "inreq" && e.finalStatus === "confirmed"), "the stream carries A4");
});

test("an agent can call itself: its own folder is in the agmen", opts, async (t) => {
  const h = await harness(t);
  const { a } = await pair(h);
  assert.ok(a.agmen().has("alpha"), "self is callable, so self-calls are possible");
  const out = await a.request("alpha", "echo", { text: "me" });
  assert.deepEqual(out.response?.result, { text: "echo: me" });
  await waitFor(async () => finalsOf(await diaryOf(h.root, "alpha"), "inreq").length === 1, 6_000);
  assert.deepEqual(await inboxOf(h.root, "alpha"), [], "the own-inbox watcher fed B2 for the response");
});

test("idle → busy → idle is observable on disk during a slow capability (D-8)", opts, async (t) => {
  const h = await harness(t);
  const { a } = await pair(h, (_alpha, beta) => beta.capability(SLOW, slowHandler));
  const ongoing = a.request("beta", "slow", { seconds: 0.5 });
  await waitFor(
    async () => (await statusOf(h.root, "beta")).status === "busy" && (await statusOf(h.root, "alpha")).status === "busy",
    6_000,
  );
  const serving = await statusOf(h.root, "beta");
  assert.equal(serving.capability_status.find((c) => c.name === "slow")?.running, 1, "capability_status is live");
  assert.equal((await ongoing).finalStatus, "confirmed");
  await waitFor(async () => (await statusOf(h.root, "beta")).status === "idle", 6_000);
  await waitFor(async () => (await statusOf(h.root, "alpha")).status === "idle", 6_000);
  assert.equal((await statusOf(h.root, "beta")).status, "idle");
  assert.equal((await statusOf(h.root, "alpha")).status, "idle");
  assert.deepEqual(statusEvents(await diaryOf(h.root, "beta")), ["idle", "busy", "idle"]);
  assert.deepEqual(
    statusEvents(await diaryOf(h.root, "alpha")),
    ["idle", "busy", "idle"],
    "an outbound exchange in B2 makes the requestor busy too (D-8)",
  );
});

test("the heartbeat rewrites the same status with a fresh datetime and no diarium event (rule 3, D-8)", opts, async (t) => {
  const h = await harness(t);
  const { a } = await pair(h, undefined, { timers: { heartbeatInterval: 100 } });
  const statusFile = path.join(h.root, "alpha", "status.json");
  const before = await mtimeOf(statusFile);
  await sleep(350); // at least two beats
  assert.ok((await mtimeOf(statusFile)) > before, "status.json was rewritten");
  assert.equal((await statusOf(h.root, "alpha")).status, "idle", "the state itself did not change");
  assert.deepEqual(statusEvents(await diaryOf(h.root, "alpha")), ["idle"], "one status event for one change");
});

test("stop(): status inactive first and the announce after it; a second stop() is a no-op", opts, async (t) => {
  const h = await harness(t);
  const { a, ea } = await pair(h);
  await a.stop();
  assert.equal((await statusOf(h.root, "alpha")).status, "inactive");
  assert.equal((await announceOf(h.root, "alpha")).nuntium, "agent alpha is inactive");
  const statusMtime = await mtimeOf(path.join(h.root, "alpha", "status.json"));
  const announceMtime = await mtimeOf(path.join(h.root, "nuntium", "alpha.n.json"));
  assert.ok(announceMtime >= statusMtime, "rule 2: the announce file comes after the status write");
  const lines = await diaryOf(h.root, "alpha");
  assert.ok(
    String(lines.filter((l) => l.event === "status").at(-1)!.event_data) === "inactive",
    "the last diarium status event is the C1 one",
  );
  assert.ok(lines.at(-1)!.event === "announce", "and the announce event follows it");
  assert.equal(ea.filter((e) => e.event === "status" && e.state === "inactive").length, 1);
  assert.equal(a.agmen().size, 0, "C2 drops the in-memory view");
  const before = await mtimeOf(path.join(h.root, "alpha", "status.json"));
  await a.stop(); // idempotent
  await a.stop();
  assert.equal(await mtimeOf(path.join(h.root, "alpha", "status.json")), before, "a second stop() writes nothing");
});

test("restart (D-10): archive, stop, delay, and a fresh start that announces active again", opts, async (t) => {
  const h = await harness(t);
  const { a } = await pair(h);
  await a.command("beta", "restart", { force: false, delay: 0.1 });
  await waitFor(async () => statusEvents(await diaryOf(h.root, "beta")).join(">") === "idle>inactive>idle", 8_000);
  await waitFor(async () => (await announceOf(h.root, "beta")).nuntium === "agent beta is active", 8_000);
  assert.deepEqual(statusEvents(await diaryOf(h.root, "beta")), ["idle", "inactive", "idle"], "C1 then a fresh start");
  assert.deepEqual(finalsOf(await diaryOf(h.root, "beta"), "inreq"), ["completed"], "D3 archived the command");
  assert.deepEqual(await inboxOf(h.root, "beta"), [], "the restart envelope is gone");
  assert.equal(
    (await diaryOf(h.root, "beta")).filter((l) => l.event === "announce").length,
    3,
    "active, inactive, active",
  );
  await waitFor(() => a.agmen().has("beta"), 6_000); // the new announce triggered our rescan (D-17)
  const out = await a.request("beta", "echo", { text: "again" });
  assert.deepEqual(out.response?.result, { text: "echo: again" }, "the agent works again after the restart");
});

test("a deleted own inbox is a watch error: log event, status error, announce (D-22)", opts, async (t) => {
  const h = await harness(t);
  const { a, ea } = await pair(h);
  const inbox = path.join(h.root, "alpha", "inbox");
  await fsp.rm(inbox, { recursive: true, force: true });
  await waitFor(async () => (await statusOf(h.root, "alpha")).status === "error", 6_000);
  const logs = ea.filter((e) => e.event === "log").map((e) => e.message);
  assert.ok(logs.some((m) => m.includes("watch error") && m.includes(inbox)), JSON.stringify(logs));
  assert.match((await announceOf(h.root, "alpha")).nuntium, /^agent alpha failed: watch error in /);
  assert.ok(statusEvents(await diaryOf(h.root, "alpha")).includes("error"));
  await sleep(300); // beats and possible scans must not undo the error state
  assert.equal((await statusOf(h.root, "alpha")).status, "error", "error has no recovery transition");
});

test("fail(): status error plus announce, and the agent stays in error until stopped", opts, async (t) => {
  const h = await harness(t);
  const { a, ea } = await pair(h, undefined, { timers: { heartbeatInterval: 100 } });
  await a.fail("the disk is gone");
  assert.equal((await statusOf(h.root, "alpha")).status, "error");
  assert.equal((await announceOf(h.root, "alpha")).nuntium, "agent alpha failed: the disk is gone");
  assert.ok(ea.some((e) => e.event === "status" && e.state === "error"));
  await a.fail("again"); // one announce only: the state did not change
  assert.equal((await diaryOf(h.root, "alpha")).filter((l) => l.event === "announce").length, 2, "active, then failed");
  await sleep(300); // beats: nothing leaves `error` on its own
  assert.equal((await statusOf(h.root, "alpha")).status, "error");
  await a.stop();
  assert.equal((await statusOf(h.root, "alpha")).status, "inactive");
  assert.deepEqual(statusEvents(await diaryOf(h.root, "alpha")), ["idle", "error", "inactive"]);
});

test("enable(false)/enable(true): spec.disabled and the re-announce; a disabled peer answers 100", opts, async (t) => {
  const h = await harness(t);
  // Beta is up first, so alpha discovers it in its own initial scan (step 4); the long debounce then
  // keeps alpha's view of the peer as it is, so the request really reaches the peer's A0.
  const timers = { nuntiumDebounce: 10_000 };
  const b = agent(h, "beta", { acceptsCommandsFrom: ["alpha"] }, timers);
  b.agent.capability(ECHO, echoHandler);
  await b.agent.start();
  const a = agent(h, "alpha", { acceptsCommandsFrom: ["beta"] }, timers);
  a.agent.capability(ECHO, echoHandler);
  await a.agent.start();
  assert.ok(a.agent.agmen().has("beta"), "the initial scan discovers the peer");
  await a.agent.command("beta", "enable", { enabled: false });
  await waitFor(async () => (await specOf(h.root, "beta")).disabled === true, 6_000);
  assert.equal((await announceOf(h.root, "beta")).nuntium, "agent beta is disabled");
  assert.deepEqual((await specOf(h.root, "beta")).capabilities.map((c) => c.name), ["echo"], "the rest stands");
  const rejected = await a.agent.request("beta", "echo", { text: "x" });
  assert.equal(rejected.response?.error, true);
  assert.equal((rejected.response?.result as { code: number }).code, 100, "A0: agent_disabled");
  await a.agent.command("beta", "enable", { enabled: true });
  await waitFor(async () => (await specOf(h.root, "beta")).disabled === false, 6_000);
  assert.equal((await announceOf(h.root, "beta")).nuntium, "agent beta is enabled");
  const accepted = await a.agent.request("beta", "echo", { text: "again" });
  assert.equal(accepted.response?.error, false);
  assert.deepEqual(accepted.response?.result, { text: "echo: again" });
  await waitFor(
    async () => finalsOf(await diaryOf(h.root, "beta"), "inreq").length === 4,
    6_000,
  );
  assert.deepEqual(
    finalsOf(await diaryOf(h.root, "beta"), "inreq"),
    ["completed", "confirmed", "completed", "confirmed"],
    "two commands archived, two exchanges answered (the disabled one with code 100)",
  );
});

test("ping re-publishes spec and status with fresh datetimes and re-announces", opts, async (t) => {
  const h = await harness(t);
  const { a } = await pair(h);
  const specFile = path.join(h.root, "beta", "spec.json");
  const statusFile = path.join(h.root, "beta", "status.json");
  const before = { spec: await mtimeOf(specFile), status: await mtimeOf(statusFile) };
  await a.command("beta", "ping");
  await waitFor(async () => (await announceOf(h.root, "beta")).nuntium === "agent beta is alive", 6_000);
  assert.ok((await mtimeOf(specFile)) > before.spec, "D2: spec.json rewritten");
  assert.ok((await mtimeOf(statusFile)) >= before.status, "and status.json too");
  assert.equal((await statusOf(h.root, "beta")).status, "idle", "ping does not change the state");
  assert.deepEqual(statusEvents(await diaryOf(h.root, "beta")), ["idle"], "no diarium status event for ping");
  assert.deepEqual(finalsOf(await diaryOf(h.root, "beta"), "inreq"), ["completed"], "D3 archived the ping");
});

test("a custom command reaches the registered handler with its parameters and its context", opts, async (t) => {
  const h = await harness(t);
  const seen: { params: unknown; ctx: CommandContext }[] = [];
  const { a } = await pair(h, (_alpha, beta) => {
    beta.onCommand("flush-cache", async (params, ctx) => {
      seen.push({ params, ctx });
    });
  });
  const rid = await a.command("beta", "flush-cache", { why: "test" });
  await waitFor(() => seen.length === 1, 6_000);
  assert.deepEqual(seen[0].params, { why: "test" });
  assert.equal(seen[0].ctx.src, "alpha");
  assert.equal(seen[0].ctx.rid, rid);
  assert.deepEqual(await inboxOf(h.root, "beta"), [], "a command is never answered, only archived");
  assert.deepEqual(finalsOf(await diaryOf(h.root, "beta"), "inreq"), ["completed"]);
});

test("request() to an unknown peer or capability rejects with ActioCallError and writes nothing", opts, async (t) => {
  const h = await harness(t);
  const { a } = await pair(h);
  await assert.rejects(() => a.request("ghost", "echo", { text: "x" }), ActioCallError);
  await assert.rejects(
    () => a.request("beta", "nope", {}),
    (err: unknown) => err instanceof ActioCallError && /capability is not usable/.test(err.message),
  );
  assert.equal(await io.exists(path.join(h.root, "ghost")), false, "no folder is invented for an unknown peer");
  assert.deepEqual(await inboxOf(h.root, "beta"), []);
  assert.deepEqual(await inboxOf(h.root, "alpha"), []);
  assert.deepEqual(finalsOf(await diaryOf(h.root, "alpha"), "outreq"), [], "nothing written, so nothing archived");
});

test("a namespace mismatch keeps the peer out of the agmen, and the call is rejected locally", opts, async (t) => {
  const h = await harness(t);
  const a = agent(h, "alpha", { namespaces: ["red"] });
  const b = agent(h, "beta", { namespaces: ["blue"] });
  a.agent.capability(ECHO, echoHandler);
  b.agent.capability(ECHO, echoHandler);
  await a.agent.start();
  await b.agent.start();
  await sleep(400); // past the nuntium debounce: no discovery may sneak in
  assert.deepEqual([...a.agent.agmen().keys()], ["alpha"], "only self survives rule 2");
  assert.deepEqual([...b.agent.agmen().keys()], ["beta"]);
  await assert.rejects(() => a.agent.request("beta", "echo", { text: "x" }), ActioCallError);
  assert.deepEqual(await inboxOf(h.root, "beta"), []);
  assert.deepEqual(finalsOf(await diaryOf(h.root, "alpha"), "outreq"), []);
});

test("command() to a peer that does not list us rejects with ActioCallError and writes nothing", opts, async (t) => {
  const h = await harness(t);
  const { a } = await pair(h, undefined, { b: { acceptsCommandsFrom: [] } });
  assert.equal(a.agmen().get("beta")?.commands, false, "rule 5.1: an empty list means nobody");
  await assert.rejects(() => a.command("beta", "ping"), ActioCallError);
  assert.deepEqual(await inboxOf(h.root, "beta"), []);
  assert.deepEqual(finalsOf(await diaryOf(h.root, "alpha"), "outreq"), [], "`outreq` sent is only for a written command");
});

test("D-12: a result outside the peer's result schema ends the exchange and rejects with ActioResultError", opts, async (t) => {
  const h = await harness(t);
  const { a } = await pair(h, (_alpha, beta) => beta.capability(BAD, badHandler));
  assert.ok(a.agmen().get("beta")!.capabilities.has("bad"));
  let caught: unknown;
  try {
    await a.request("beta", "bad", {});
  } catch (err) {
    caught = err;
  }
  assert.ok(caught instanceof ActioResultError, `expected ActioResultError, got ${String(caught)}`);
  assert.deepEqual(caught.response.result, { oops: 1 }, "the error carries the response");
  // A4 writes the response before archiving: wait for the archive, then look at it.
  await waitFor(async () => finalsOf(await diaryOf(h.root, "beta"), "inreq").length === 1, 6_000);
  assert.deepEqual(finalsOf(await diaryOf(h.root, "beta"), "inreq"), ["confirmed"], "the exchange still ended");
  assert.deepEqual(finalsOf(await diaryOf(h.root, "alpha"), "outreq"), ["confirmed"], "and was confirmed (B3)");
});

test("D-19: an uncompilable peer capability is logged once and its checks are skipped", opts, async (t) => {
  const h = await harness(t);
  const { a, ea } = await pair(h, (_alpha, beta) => beta.capability(BAD, badHandler));
  const skipped = (): number => ea.filter((e) => e.event === "log" && e.message.includes("checks skipped")).length;
  // A peer may publish a spec whose inner schema strict mode refuses (the protocol only requires an object).
  const spec = await specOf(h.root, "beta");
  const peerCap = spec.capabilities.find((c) => c.name === "bad")!;
  peerCap.actio.result = { type: "object", thisKeywordDoesNotExist: true };
  spec.datetime = toIso(Date.now() + 1_000); // a new spec version: the D-19 cache key changes with it
  await io.writeJsonAtomic(path.join(h.root, "beta", "spec.json"), spec, path.join(h.root, "beta"));
  await broadcast(h.root);
  await waitFor(() => a.agmen().get("beta")?.spec.datetime === spec.datetime, 6_000);
  const out = await a.request("beta", "bad", {});
  assert.equal(out.finalStatus, "confirmed", "the checks are skipped, so the answer is accepted");
  assert.equal(skipped(), 1, "one log event for the (peer, spec datetime, capability) triple");
  await a.request("beta", "bad", {});
  assert.equal(skipped(), 1, "the cached failure is not logged again");
});

test("a cancelled exchange is not a D-12 violation: its response carries result null", opts, async (t) => {
  const h = await harness(t);
  const { a } = await pair(h, (_alpha, beta) => beta.capability(SLOW, slowHandler));
  const controller = new AbortController();
  const ongoing = a.request("beta", "slow", { seconds: 0.5 }, { signal: controller.signal });
  await sleep(150);
  controller.abort();
  const out = await ongoing; // must resolve: `result: null` is protocol for a cancellation response
  assert.equal(out.finalStatus, "cancelled");
  assert.equal(out.response?.cancellation, true);
  assert.equal(out.response?.result, null);
});

test("a non-conforming inbox file is ignored and reported on the event stream (D-15)", opts, async (t) => {
  const h = await harness(t);
  const { a, ea } = await pair(h);
  const junk = path.join(h.root, "alpha", "inbox", "junk.json");
  await fsp.writeFile(junk, "{ not json");
  await waitFor(() => ea.some((e) => e.event === "ignored"), 6_000);
  const first = ea.find((e) => e.event === "ignored")!;
  assert.ok(first.event === "ignored" && first.path.endsWith("junk.json"));
  assert.ok(first.event === "ignored" && first.reason.length > 0);
  assert.equal(await fsp.readFile(junk, "utf8"), "{ not json", "an ignored file stays where it is");
  assert.deepEqual(finalsOf(await diaryOf(h.root, "alpha"), "inreq"), [], "and is never archived");
});

test("a duplicate capability, a shadowed built-in command, and a non-strict schema are registration errors", opts, async (t) => {
  const h = await harness(t);
  const { agent: a } = agent(h, "alpha");
  a.capability(ECHO, echoHandler);
  assert.throws(() => a.capability(ECHO, echoHandler), /capability "echo" is already registered/);
  const broken: Capability = {
    name: "broken",
    description: "schema strict mode refuses",
    parameters: { type: "object", thisKeywordDoesNotExist: true } as Capability["parameters"],
    actio: { result: true },
  };
  assert.throws(() => a.capability(broken, echoHandler), /capability "broken" has a schema strict mode rejects/);
  assert.throws(() => a.onCommand("stop", async () => undefined), /"stop" is a built-in command/);
  a.onCommand("x", async () => undefined);
  assert.throws(() => a.onCommand("x", async () => undefined), /command "x" is already registered/);
});

test("start() twice throws; a bad sync value fails at start() and leaves nothing armed", opts, async (t) => {
  const h = await harness(t);
  const { agent: a } = agent(h, "alpha");
  a.capability(ECHO, echoHandler);
  await a.start();
  await assert.rejects(() => a.start(), /start\(\) while already started/);
  const broken = agent(h, "broken", {}, { sync: "mutagen" }); // `mutagen` needs a session
  await assert.rejects(() => broken.agent.start(), /unknown sync provider "mutagen"/);
  assert.equal(broken.agent.agmen().size, 0);
  assert.equal(await io.exists(path.join(h.root, "broken")), false, "a failed start writes nothing into the tree");
  await broken.agent.stop(); // a NOP: the failed start is not "running"
  assert.deepEqual((await io.listDir(h.root)).sort(), ["alpha", "forum", "nuntium"]);
});

test("a broadcast signal in nuntium triggers a full rescan (D-17)", opts, async (t) => {
  const h = await harness(t);
  const { a } = await pair(h);
  // A folder published without any announce file: only a scan can bring it into the agmen.
  const folder = path.join(h.root, "gamma");
  await io.makeDir(folder);
  const now = Date.now();
  const spec = buildSpec({ id: "gamma", description: "gamma test agent", acceptsCommandsFrom: [] }, [ECHO], now);
  await io.writeJsonAtomic(path.join(folder, "spec.json"), spec, folder);
  await io.writeJsonAtomic(path.join(folder, "status.json"), buildStatus("gamma", "idle", now), folder);
  assert.equal(a.agmen().has("gamma"), false, "no announce, no discovery");
  await sleep(300); // past any pending debounce: still nothing
  assert.equal(a.agmen().has("gamma"), false, "discovery is event-driven, never a poll");
  await broadcast(h.root);
  await waitFor(() => a.agmen().has("gamma"), 6_000);
  assert.equal(a.agmen().get("gamma")?.health, "healthy");
  assert.deepEqual([...(a.agmen().get("gamma")?.capabilities.keys()) ?? []], ["echo"]);
});
