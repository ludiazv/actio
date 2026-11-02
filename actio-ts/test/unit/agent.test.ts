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
import { nonce, sleep, toIso, ulid } from "../../src/util.js";
import { ActioCallError, ActioResultError } from "../../src/types.js";
import { fastConfig, waitFor } from "../helpers/index.js";
import type { ActioConfig, ActioEvent, AgentOptions, CapabilityHandler, CommandContext } from "../../src/types.js";
import type { Announce, Capability, DiariumLine, RequestFile, Spec, Status }
  from "../../src/generated/types.js";

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
  assert.ok(logs.some((m) => m === `watch lost ${inbox}: ENOENT`), JSON.stringify(logs)); // D-22 text
  assert.match((await announceOf(h.root, "alpha")).nuntium, /^agent alpha failed: watch lost /);
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

test("R3-3: the library copies caller objects — a later mutation changes neither the spec nor A0", opts, async (t) => {
  const h = await harness(t);
  // Caller-owned objects: the capability and the options' namespaces array.
  const cap: Capability = structuredClone(ECHO);
  const ns = ["green"];
  const { agent: a } = agent(h, "alpha", { acceptsCommandsFrom: ["beta"], namespaces: ns });
  const { agent: b } = agent(h, "beta", { acceptsCommandsFrom: ["alpha"], namespaces: ["green"] });
  a.capability(cap, echoHandler);
  b.capability(ECHO, echoHandler);
  await a.start();
  await b.start();
  await waitFor(() => b.agmen().has("alpha"), 6_000);
  // The published spec carries the original values.
  const specBefore = await specOf(h.root, "alpha");
  assert.deepEqual(specBefore.namespaces, ["green"]);
  assert.equal(specBefore.capabilities.find((c) => c.name === "echo")?.actio?.disabled, undefined);
  // The caller mutates its own objects after capability()/start(): with R3-3 neither the agent's
  // registration nor its options are aliased, so the mutation must reach nowhere.
  cap.actio!.disabled = true;
  ns.push("blue");
  // A `ping` (a real protocol action) rebuilds and republishes alpha's spec from its own state.
  // The spec's `datetime` has second resolution, so the sync point is the command's archive
  // (D3 runs after D2's republish).
  await b.command("alpha", "ping");
  await waitFor(async () => finalsOf(await diaryOf(h.root, "alpha"), "inreq").length === 1, 6_000);
  const specAfter = await specOf(h.root, "alpha");
  assert.deepEqual(specAfter.namespaces, ["green"], "the published spec ignores the caller's mutation");
  assert.equal(
    specAfter.capabilities.find((c) => c.name === "echo")?.actio?.disabled,
    undefined,
    "the registered (cloned) capability is untouched",
  );
  // A0's decisions are unchanged too: alpha's echo is still accepted (not 102) and answers per the
  // original schema.
  const out = await b.request("alpha", "echo", { text: "x" });
  assert.equal(out.finalStatus, "confirmed");
  assert.deepEqual(out.response?.result, { text: "echo: x" });
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

test("refresh() sees a peer that started after us, where the nuntium debounce would not", opts, async (t) => {
  const h = await harness(t);
  const timers = { nuntiumDebounce: 60_000 }; // no nuntium-driven scan can land during the test
  const a = agent(h, "alpha", { acceptsCommandsFrom: ["beta"] }, timers);
  a.agent.capability(ECHO, echoHandler);
  await a.agent.start();
  assert.equal(a.agent.agmen().has("beta"), false, "beta has not started: not in the view");
  const b = agent(h, "beta", { acceptsCommandsFrom: ["alpha"] }, timers);
  b.agent.capability(ECHO, echoHandler);
  await b.agent.start(); // its announce schedules a scan 60 s out: it cannot land in time
  await sleep(300); // well past any other pending debounce: no scan may have sneaked in
  assert.equal(a.agent.agmen().has("beta"), false, "the 60 s debounce hides beta");
  const view = await a.agent.refresh();
  assert.ok(view.has("beta"), "refresh() scans now and sees beta");
  assert.equal(view, a.agent.agmen(), "the new view, the same map as agmen()");
  assert.equal(a.agent.agmen().get("beta")?.health, "healthy");
  assert.deepEqual([...(a.agent.agmen().get("beta")?.capabilities.keys()) ?? []], ["echo"]);
});

// --- the R2 regressions (WP7-C-1): one test per finding; the oracle's probes are separate -----------

/** A request envelope as a peer would write it: the local gate of `request()` refuses to produce one. */
function requestFile(
  rid: string,
  src: string,
  dst: string,
  name: string,
  parameters: { [k: string]: unknown },
): RequestFile {
  const now = Date.now();
  return {
    rid,
    src,
    dst,
    datetime: toIso(now),
    type: "req",
    actio_version: "0.1",
    request: {
      ts: Math.floor(now / 1000),
      nonce: nonce(),
      signature: null,
      name,
      parameters,
      timeout: 0, // 0 = no timeout: nothing here has to expire
      attachments: [],
    },
  };
}

/** Gamma (the delegate) and alpha (a handler that delegates to gamma); both started. */
async function delegating(
  h: Harness,
  timers: Partial<ActioConfig>,
  onInner: (finalStatus: string) => void,
): Promise<{ a: ActioAgent; g: ActioAgent }> {
  const g = agent(h, "gamma", { acceptsCommandsFrom: [] }, {
    ...timers, stopGrace: 200, confirmTimeout: 30_000, // its own timeout must not compete with the stop
  });
  g.agent.capability(SLOW, slowHandler);
  await g.agent.start();
  const a = agent(h, "alpha", { acceptsCommandsFrom: [] }, timers);
  const RELAY: Capability = { ...SLOW, name: "relay" };
  a.agent.capability(RELAY, async (params) => {
    const inner = await a.agent.request("gamma", "slow", params); // the delegation (planner.ts does this)
    onInner(inner.finalStatus);
    if (inner.finalStatus !== "confirmed") throw new Error(`relay: no result (${inner.finalStatus})`);
    return inner.response!.result;
  });
  await a.agent.start();
  return { a: a.agent, g: g.agent };
}

test("R2-1: enable(false) survives the heartbeat and ping; only a fresh start clears it (D-10)", opts, async (t) => {
  const h = await harness(t);
  const timers = { nuntiumDebounce: 10_000, heartbeatInterval: 100 };
  const b = agent(h, "beta", { acceptsCommandsFrom: ["alpha"] }, timers);
  b.agent.capability(ECHO, echoHandler);
  await b.agent.start();
  const a = agent(h, "alpha", { acceptsCommandsFrom: ["beta"] }, timers); // its view of beta stays as it is
  a.agent.capability(ECHO, echoHandler);
  await a.agent.start();
  await a.agent.command("beta", "enable", { enabled: false });
  await waitFor(async () => (await specOf(h.root, "beta")).disabled === true, 6_000);
  const statusFile = path.join(h.root, "beta", "status.json");
  const beaten = await mtimeOf(statusFile);
  await sleep(400); // four beats: they rewrite status.json, they do not touch the enabled state
  assert.ok((await mtimeOf(statusFile)) > beaten, "the heartbeat ran");
  assert.equal((await specOf(h.root, "beta")).disabled, true, "a beat does not re-enable the agent");
  await a.agent.command("beta", "ping"); // the rebuild that used to drop `disabled`: probe P1
  await waitFor(async () => (await announceOf(h.root, "beta")).nuntium === "agent beta is alive", 6_000);
  assert.equal((await specOf(h.root, "beta")).disabled, true, "ping must not change the enabled state");
  const rejected = await a.agent.request("beta", "echo", { text: "x" });
  assert.equal((rejected.response?.result as { code: number }).code, 100, "and A0 still answers 100");
  await a.agent.command("beta", "restart", { force: false, delay: 0 });
  await waitFor(async () => (await announceOf(h.root, "beta")).nuntium === "agent beta is active", 8_000);
  assert.equal((await specOf(h.root, "beta")).disabled, undefined, "D-10: a restart rebuilds from the options");
  const again = await a.agent.request("beta", "echo", { text: "fresh" });
  assert.deepEqual(again.response?.result, { text: "echo: fresh" }, "the restarted agent serves again");
});

test("R2-2: a graceful stop lets a delegating handler finish inside stopGrace (D-9 order)", opts, async (t) => {
  const h = await harness(t);
  const timers = { nuntiumDebounce: 10_000, stopGrace: 3_000 };
  let innerFinal = "";
  const { a } = await delegating(h, timers, (s) => (innerFinal = s));
  const c = agent(h, "client", { acceptsCommandsFrom: [] }, timers);
  await c.agent.start();
  await waitFor(() => c.agent.agmen().has("alpha") && a.agmen().has("gamma"), 6_000);
  const pending = c.agent.request("alpha", "relay", { seconds: 0.4 }, { timeout: 0 });
  await waitFor(async () => (await statusOf(h.root, "alpha")).status === "busy", 6_000);
  await a.stop(); // force=false: the grace (3 s) is longer than the work (0.4 s)
  const out = await pending;
  assert.equal(out.response?.error, false, "the in-flight work finished inside the grace (probe P2)");
  assert.deepEqual(out.response?.result, { slept: 0.4 }, "the client got the result, not code 300");
  assert.equal(innerFinal, "confirmed", "and its inner request got its answer, the requestor stayed alive");
  assert.deepEqual(finalsOf(await diaryOf(h.root, "alpha"), "outreq"), ["confirmed"], "alpha confirmed itself");
  // The client's confirmation of the relay exchange cannot be awaited: lane C archives the inbound
  // exchange as soon as the work is done, so whichever envelope existed at that moment decides it (D-13).
  const inbound = finalsOf(await diaryOf(h.root, "alpha"), "inreq");
  assert.equal(inbound.length, 1, "one archive for one exchange (D-5)");
  assert.ok(inbound[0] === "confirmed" || inbound[0] === "completed", `the archive happened, ending: ${inbound[0]}`);
  // gamma is served by the confirmation of alpha's requestor, which stayed alive for the whole grace: the
  // line takes two watcher hops to appear, so it is waited for, then pinned.
  await waitFor(async () => (await finalsOf(await diaryOf(h.root, "gamma"), "inreq")).length === 1, 6_000);
  assert.deepEqual(finalsOf(await diaryOf(h.root, "gamma"), "inreq"), ["confirmed"], "gamma served it");
});

test("R2-2: force=true skips the wait: the client gets 300, the inner request is abandoned (D-9)", opts, async (t) => {
  const h = await harness(t);
  const timers = { nuntiumDebounce: 10_000, stopGrace: 3_000 };
  let innerFinal = "";
  const { a } = await delegating(h, timers, (s) => (innerFinal = s));
  const c = agent(h, "client", { acceptsCommandsFrom: [] }, timers);
  await c.agent.start();
  await waitFor(() => c.agent.agmen().has("alpha") && a.agmen().has("gamma"), 6_000);
  const pending = c.agent.request("alpha", "relay", { seconds: 3 }, { timeout: 0 }); // outlives the stop
  await waitFor(async () => (await statusOf(h.root, "alpha")).status === "busy", 6_000);
  await a.stop({ force: true }); // D-9: skip the wait; the abandon stays last
  const out = await pending;
  assert.equal(out.response?.error, true, "D-9: an accepted, unanswered request gets an error response");
  assert.equal((out.response?.result as { code: number }).code, 300, "agent_stopping");
  await waitFor(async () => finalsOf(await diaryOf(h.root, "alpha"), "outreq").join() === "abandoned", 6_000);
  assert.equal(innerFinal, "abandoned", "the handler's own request ended through the abandon path (D-6)");
  assert.deepEqual(finalsOf(await diaryOf(h.root, "alpha"), "inreq"), ["completed"], "answered with 300");
  assert.equal((await statusOf(h.root, "alpha")).status, "inactive");
});

test("R2-3: a stop command pending at start ends with the agent inactive and nothing armed", opts, async (t) => {
  const h = await harness(t);
  const timers = { heartbeatInterval: 100, nuntiumDebounce: 10_000 };
  // beta starts first so alpha's own initial scan (row 4) discovers it: the long debounce would
  // otherwise hide it for the whole test (D-17).
  const b = agent(h, "beta", { acceptsCommandsFrom: ["alpha"] }, timers);
  const a = agent(h, "alpha", { acceptsCommandsFrom: [] }, timers);
  await b.agent.start();
  await a.agent.start();
  await waitFor(() => a.agent.agmen().has("beta"), 6_000);
  await b.agent.stop();
  await a.agent.command("beta", "stop", { force: false }); // it waits in beta's inbox for the next start (D-4)
  assert.equal((await inboxOf(h.root, "beta")).length, 1, "one pending envelope");
  await b.agent.start(); // its initial scan delivers the pending stop: probe P3
  await waitFor(async () => (await statusOf(h.root, "beta")).status === "inactive", 6_000);
  assert.equal((await announceOf(h.root, "beta")).nuntium, "agent beta is inactive", "C1 of the pending stop");
  const file = path.join(h.root, "beta", "status.json");
  const written = await mtimeOf(file);
  await sleep(500); // five heartbeat intervals: nothing may rewrite it
  assert.equal(await mtimeOf(file), written, "no heartbeat and no watcher survived the stop (R2-3)");
  assert.deepEqual(await inboxOf(h.root, "beta"), [], "the command was archived (D3)");
  assert.equal(b.agent.agmen().size, 0, "C2 dropped the view");
  // idle+inactive from the explicit stop() above, idle from this start(), inactive from the pending one.
  assert.deepEqual(
    statusEvents(await diaryOf(h.root, "beta")),
    ["idle", "inactive", "idle", "inactive"],
    "the pending stop wrote C1 of its own",
  );
  await b.agent.start(); // and the agent is startable again
  await waitFor(async () => (await statusOf(h.root, "beta")).status === "idle", 6_000);
});

test("R2-3: a restart command pending at start leaves exactly one instance running", opts, async (t) => {
  const h = await harness(t);
  const timers = { heartbeatInterval: 150, nuntiumDebounce: 10_000 };
  // beta starts first so alpha's own initial scan (row 4) discovers it: the long debounce would
  // otherwise hide it for the whole test (D-17).
  const b = agent(h, "beta", { acceptsCommandsFrom: ["alpha"] }, timers);
  const a = agent(h, "alpha", { acceptsCommandsFrom: [] }, timers);
  await b.agent.start();
  await a.agent.start();
  await waitFor(() => a.agent.agmen().has("beta"), 6_000);
  await b.agent.stop();
  await a.agent.command("beta", "restart", { force: true, delay: 0 });
  await b.agent.start(); // the pending restart runs inside start(): it must not arm a second set
  await waitFor(async () => (await statusOf(h.root, "beta")).status === "idle", 8_000);
  // One heartbeat or two? Count the rewrites of one window: the interval gives the cadence, and a
  // second armed heartbeat would roughly double it. (Gaps cannot be used: the restart's own writes
  // land next to each other.)
  const file = path.join(h.root, "beta", "status.json");
  const interval = timers.heartbeatInterval!;
  const window = 6 * interval;
  const before = await mtimeOf(file);
  const seen = new Set<number>();
  const until = Date.now() + window;
  while (Date.now() < until) {
    await sleep(20);
    const m = await mtimeOf(file);
    if (m !== before) seen.add(m);
  }
  const rewrites = seen.size;
  assert.ok(rewrites >= 4, `the heartbeat rewrote status.json ${rewrites} times in ${window} ms`);
  assert.ok(
    rewrites <= Math.round(window / (0.6 * interval)),
    `two heartbeats armed: ${rewrites} rewrites in ${window} ms, cadence ${interval} ms`,
  );
  await b.agent.stop();
  const stopped = await mtimeOf(file);
  await sleep(500);
  assert.equal(await mtimeOf(file), stopped, "and after the final stop() nothing is rewritten");
});

test("R2-4: a discovered peer with no shared namespace is answered 106 (A0, D-7)", opts, async (t) => {
  const h = await harness(t);
  const timers = { nuntiumDebounce: 10_000 };
  const d = agent(h, "delta", { namespaces: ["blue"], acceptsCommandsFrom: [] }, timers);
  await d.agent.start();
  const b = agent(h, "beta", { namespaces: ["red"], acceptsCommandsFrom: [] }, timers);
  b.agent.capability(ECHO, echoHandler);
  await b.agent.start();
  assert.equal(b.agent.agmen().has("delta"), false, "callability rule 2 keeps it out of the Agmen view");
  // delta's own agent cannot send this: request() refuses a peer that is not in the view. The envelope
  // arrives the way any peer's would, and beta's A0 judges it from the spec its scan DISCOVERED: probe P5.
  const rid = ulid();
  const target = path.join(h.root, "beta", "inbox", `${rid}.json`);
  const tmp = path.join(h.root, "delta");
  await io.writeJsonAtomic(target, requestFile(rid, "delta", "beta", "echo", { text: "x" }), tmp);
  const resFile = path.join(h.root, "beta", "inbox", `${rid}.res.json`);
  await waitFor(async () => (await io.exists(resFile)), 6_000);
  const res = await json<{ rid: string; response: { error: boolean; result: unknown } }>(resFile);
  assert.equal(res.rid, rid, "the response header carries the request header (D-11)");
  assert.equal(res.response.error, true);
  assert.equal((res.response.result as { code: number }).code, 106, "namespace_mismatch, not an echo");
  assert.equal(b.events.filter((e) => e.event === "ignored").length, 0, "a well-formed envelope: not ignored");
  // no requestor is alive for this rid, so it archives when the confirmation times out (D-13).
  await waitFor(async () => finalsOf(await diaryOf(h.root, "beta"), "inreq").join() === "completed", 8_000);
});

test("R2-6: a bad environment value throws at start(), never at construction (D-21)", opts, async (t) => {
  const h = await harness(t);
  process.env["ACTIO_HEARTBEAT"] = "abc";
  try {
    const a = new ActioAgent({
      id: "alpha",
      description: "the constructor must not read the environment",
      acceptsCommandsFrom: [],
      config: fastConfig(h.root, Date.now),
    });
    h.agents.push(a); // the after-hook stops it: a NOP unless the start() below ever succeeds
    await assert.rejects(() => a.start(), /ACTIO_HEARTBEAT/);
    await assert.rejects(() => a.start(), /ACTIO_HEARTBEAT/, "a failed start() left it not running");
    assert.deepEqual(await io.listDir(h.root), [], "and it created nothing");
  } finally {
    delete process.env["ACTIO_HEARTBEAT"];
  }
});
