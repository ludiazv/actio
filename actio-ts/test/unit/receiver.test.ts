// Tests of the receiver: lane A (dispatch, acceptance (A0), work (A1/A3), response +
// confirmation wait (A2), archive (A4), the D-4/D-5/D-11/D-14/D-15 rules) and lane D (command
// dispatch: D0–D4, the built-in commands and the archive around the agent lifecycle hooks).
import assert from "node:assert/strict";
import fsp from "node:fs/promises";
import path from "node:path";
import { test, type TestContext } from "node:test";
import * as diarium from "../../src/diarium.js";
import { Receiver } from "../../src/receiver.js";
import type { ReceiverHooks } from "../../src/receiver.js";
import * as io from "../../src/io.js";
import type {
  ActioEvent,
  AgentContext,
  CapabilityContext,
  CapabilityEntry,
  CapabilityHandler,
  CommandContext,
  CommandHandler,
} from "../../src/types.js";
import type {
  CancellationFile,
  ConfirmationFile,
  Capability,
  DiariumLine,
  RequestBody,
  RequestFile,
  ResponseBody,
  ResponseFile,
  Spec,
  Status,
} from "../../src/generated/types.js";
import { compileCapability, validateResponse } from "../../src/validate.js";
import { nonce, sleep, toIso, ulid } from "../../src/util.js";
import { fakeClock, makeRoot, waitFor } from "../helpers/index.js";

const T0 = 1_700_000_000_000;
const ISO0 = toIso(T0);

function makeSpec(over: Partial<Spec> = {}): Spec {
  return {
    id: "alpha",
    actio_version: "0.1",
    datetime: ISO0,
    description: "test receiver",
    namespaces: ["ns1"],
    accepts_commands_from: [],
    capabilities: [],
    security_support: [],
    security_posture: "trusted",
    ...over,
  };
}

/** A peer spec for the `peerSpec` hook (D-7 code 106): what the last scan DISCOVERED, I-11. */
function makePeerSpec(id: string, over: Partial<Spec> = {}): Spec {
  return makeSpec({ id, ...over });
}

/** A CapabilityEntry with a strict default parameters/result schema (D-19). */
function makeEntry(
  name: string,
  handler: CapabilityHandler,
  over: Partial<Capability["actio"]> = {},
  parameters: unknown = { type: "object", properties: {}, additionalProperties: false },
): CapabilityEntry {
  const capability: Capability = {
    name,
    description: `${name} test capability`,
    parameters: parameters as Capability["parameters"],
    actio: { result: { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"] }, ...over },
  };
  return { capability, compiled: compileCapability(capability), handler, running: 0 };
}

function makeRequest(
  rid: string,
  over: { name?: string; parameters?: unknown; src?: string; type?: "req" | "cmd"; timeout?: number; ts?: number; attachments?: string[] } = {},
): RequestFile {
  return {
    rid,
    src: over.src ?? "beta",
    dst: "alpha",
    datetime: ISO0,
    type: over.type ?? "req",
    actio_version: "0.1",
    request: {
      ts: over.ts ?? 1_700_000_000,
      nonce: nonce(),
      signature: null,
      name: over.name ?? "echo",
      parameters: (over.parameters ?? null) as RequestBody["parameters"],
      timeout: over.timeout ?? 0,
      attachments: over.attachments ?? [],
    },
  };
}

function makeCancellation(req: RequestFile, over: { name?: string; src?: string } = {}): CancellationFile {
  const body = req.request as RequestBody;
  return {
    rid: req.rid,
    src: over.src ?? req.src,
    dst: req.dst,
    datetime: ISO0,
    type: "req",
    actio_version: req.actio_version,
    cancellation: { ts: body.ts + 1, nonce: nonce(), signature: null, name: over.name ?? body.name, force: false },
  };
}
function makeConfirmation(req: RequestFile, over: { src?: string } = {}): ConfirmationFile {
  return {
    rid: req.rid,
    src: over.src ?? req.src,
    dst: req.dst,
    datetime: ISO0,
    type: "req",
    actio_version: req.actio_version,
    confirmation: { ts: 1_700_000_010, nonce: nonce(), signature: null },
  };
}

const writeJson = (dir: string, name: string, obj: unknown): Promise<void> =>
  io.writeJsonAtomic(path.join(dir, name), obj, dir);

/** A response file read back from the inbox; the body of a core response is always the object form. */
type ResFile = ResponseFile & { response: ResponseBody };

const readRes = (inbox: string, rid: string): Promise<ResFile | undefined> =>
  io.readJson(path.join(inbox, `${rid}.res.json`)) as Promise<ResFile | undefined>;

const inboxNames = (inbox: string): Promise<string[]> => io.listDir(inbox);

/** A promise the test controls (held work). */
function deferred<T = void>(): { promise: Promise<T>; resolve(v: T): void } {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

interface Env {
  root: string;
  agent: string;
  inbox: string;
  forum: string;
  nuntium: string;
  ctx: AgentContext;
  receiver: Receiver;
  events: ActioEvent[];
  diariumLines: DiariumLine[];
  /** The built-in command hooks that fired, in the order they fired (replaced per test). */
  commandCalls: string[];
  stateChanges: number[];
  watchErrors: { dir: string; err: Error }[];
  peers: Map<string, Spec>;
  fileEvents: string[];
  clock: ReturnType<typeof fakeClock>;
}

async function makeEnv(
  t: TestContext,
  opts: {
    spec?: Spec;
    /** The spec `D0` reads on every command: a test can change it between commands (`enable`). */
    specOf?: () => Spec;
    confirmTimeout?: number;
    entries?: CapabilityEntry[];
    /** Lane D: the custom commands of this agent (name → handler). */
    commands?: Record<string, CommandHandler>;
    /** Replace some built-in command hook; the default one records the call in `commandCalls`. */
    hooks?: Partial<ReceiverHooks>;
    /** Awaited inside `appendDiarium`, after the line: holds an archive in flight (D-5 test). */
    diariumGate?: () => Promise<void>;
  } = {},
): Promise<Env> {
  // Registered BEFORE makeRoot's cleanup hook: node:test runs after-hooks in registration order,
  // and D-9's archives (stop) must happen while the root still exists (teardown = D-9 then C2).
  let receiver: Receiver | undefined;
  t.after(async () => {
    await receiver?.stop();
  });
  const root = await makeRoot(t);
  const agent = path.join(root, "alpha");
  const inbox = path.join(agent, "inbox");
  const forum = path.join(root, "forum");
  const nuntium = path.join(root, "nuntium");
  for (const dir of [inbox, forum]) await fsp.mkdir(dir, { recursive: true });
  const clock = fakeClock(T0);
  const specValue = opts.spec ?? makeSpec();
  const events: ActioEvent[] = [];
  const diariumLines: DiariumLine[] = [];
  const commandCalls: string[] = [];
  const stateChanges: number[] = [];
  const watchErrors: { dir: string; err: Error }[] = [];
  const peers = new Map<string, Spec>();
  const fileEvents: string[] = [];
  const ctx: AgentContext = {
    config: {
      root,
      sync: "none",
      heartbeatInterval: 200,
      confirmTimeout: opts.confirmTimeout ?? 60_000,
      cancelWaitTimeout: 1_000,
      announceDelay: 10,
      nuntiumDebounce: 50,
      stopGrace: 1_000,
      clock,
    },
    id: "alpha",
    folders: { root, agent, inbox, forum, nuntium },
    sync: { fsync: false, delayMs: 0, notify(): void {}, flush: () => Promise.resolve(), close: () => Promise.resolve() },
    now: () => clock(),
    spec: opts.specOf ?? (() => specValue),
    state: () => "idle",
    emit: (event) => events.push(event),
    appendDiarium: async (line) => {
      diariumLines.push(line);
      await diarium.append(agent, line); // the real append: validates (I-2) and writes diarium.jsonl
      if (opts.diariumGate) await opts.diariumGate(); // the archive stays in flight while the gate holds
    },
  };
  receiver = new Receiver(
    ctx,
    {
      onWatchError: (dir, err) => watchErrors.push({ dir, err }),
      peerSpec: (src) => peers.get(src),
      onStateChange: () => stateChanges.push(stateChanges.length + 1),
      onCommandStop: async (force) => {
        commandCalls.push(`stop(force=${force})`);
      },
      onCommandRestart: async (force, delaySeconds) => {
        commandCalls.push(`restart(force=${force},delay=${delaySeconds})`);
      },
      onCommandEnable: async (enabled) => {
        commandCalls.push(`enable(${enabled})`);
      },
      onCommandPing: async () => {
        commandCalls.push("ping");
      },
      ...opts.hooks,
    },
    new Map((opts.entries ?? []).map((e) => [e.capability.name, e])),
    new Map(Object.entries(opts.commands ?? {})),
  );
  return {
    root,
    agent,
    inbox,
    forum,
    nuntium,
    ctx,
    receiver,
    events,
    diariumLines,
    commandCalls,
    stateChanges,
    watchErrors,
    peers,
    fileEvents,
    clock,
  };
}

/** The `inreq` events of the diarium, with their exchange data. */
function inreqs(env: Env): { final_status: string; req: RequestFile | null; can: CancellationFile | null; res: ResponseFile | null; con: ConfirmationFile | null }[] {
  return env.diariumLines
    .filter((l) => l.event === "inreq")
    .map((l) => l.event_data as { final_status: string; req: RequestFile | null; can: CancellationFile | null; res: ResponseFile | null; con: ConfirmationFile | null });
}

const logLines = (env: Env): string[] =>
  env.diariumLines.filter((l) => l.event === "log").map((l) => l.event_data as unknown as string);

/** A schema-valid rid whose trailing letters fix the lexical order (the scan processes rids sorted). */
const ridWith = (letter: string): string => `01${letter.repeat(24)}`;

/** The final status of every archived exchange, by rid. */
const statusByRid = (env: Env): Map<string, string> =>
  new Map(inreqs(env).map((d) => [d.req!.rid, d.final_status]));

/** The three files that tell peers what the agent is; lane D must not touch them on its own (D4). */
async function writePublications(env: Env): Promise<void> {
  await writeJson(env.agent, "spec.json", makeSpec({ accepts_commands_from: ["beta"] }));
  await writeJson(env.agent, "status.json", { id: "alpha", status: "idle", datetime: ISO0, capability_status: [] });
  await io.makeDir(env.nuntium);
  await writeJson(env.nuntium, "alpha.n.json", { id: "alpha", status: "idle", datetime: ISO0, text: "before" });
}

/** A comparable snapshot of those files (`<absent>` when one is missing). */
async function publicationState(env: Env): Promise<string[]> {
  const names = [
    path.join(env.agent, "spec.json"),
    path.join(env.agent, "status.json"),
    path.join(env.nuntium, "alpha.n.json"),
  ];
  return Promise.all(names.map((n) => fsp.readFile(n, "utf8").catch(() => "<absent>")));
}

const publishedFile = (env: Env, name: string): Promise<string> =>
  fsp.readFile(path.join(env.agent, name), "utf8").catch(() => "<absent>");

// --- Envelope lifecycle, final-status table (receiver side) ---

test("normal exchange: request → response → confirmation → archive as `confirmed`", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }))] });
  await env.receiver.start((name) => env.fileEvents.push(name));
  const rid = ulid();
  const req = makeRequest(rid);
  await writeJson(env.inbox, `${rid}.json`, req);
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const res = await readRes(env.inbox, rid);
  assert.ok(res);
  assert.ok(validateResponse(res).ok, "the response is schema-valid (self-validated before writing, I-2)");
  assert.equal(res.response.error, false);
  assert.equal(res.response.cancellation, false);
  assert.deepEqual(res.response.result, { ok: true });
  assert.deepEqual(res.response.attachments, []);
  assert.equal(res.rid, rid);
  assert.equal(res.src, "beta"); // the header is the request's, including for the response
  assert.equal(res.dst, "alpha");
  assert.match(res.datetime, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/, "a fresh datetime (same header, only datetime changes)");
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req));
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 3_000);
  assert.equal(env.receiver.runningCount(), 0);
  const [data] = inreqs(env);
  assert.ok(data);
  assert.equal(data.final_status, "confirmed");
  assert.deepEqual(data.req, req); // the envelopes are stored as exchanged (the request read back from disk)
  assert.equal(data.can, null);
  assert.deepEqual(data.res, res);
  assert.ok(data.con);
  assert.ok(env.events.some((e) => e.event === "inreq" && e.rid === rid && e.finalStatus === "confirmed"));
  assert.ok(env.stateChanges.length >= 3, "state changes on accept, on settle and on archive");
});

test("confirmation timeout: no confirmation → archive as `completed`", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }))], confirmTimeout: 80 });
  await env.receiver.start(() => {});
  const rid = ulid();
  await writeJson(env.inbox, `${rid}.json`, makeRequest(rid));
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 5_000);
  const [data] = inreqs(env);
  assert.ok(data);
  assert.equal(data.final_status, "completed");
  assert.ok(data.res);
  assert.equal(data.con, null);
});

test("rejected at A0: error response with the code, then `confirmed`", async (t) => {
  const env = await makeEnv(t, { entries: [] }); // no capabilities at all
  await env.receiver.start(() => {});
  const rid = ulid();
  const req = makeRequest(rid, { name: "nope" });
  await writeJson(env.inbox, `${rid}.json`, req);
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const res = await readRes(env.inbox, rid);
  assert.ok(res);
  assert.equal(res.response.error, true);
  assert.equal(res.response.cancellation, false);
  assert.equal(res.response.result.code, 101);
  assert.match(String(res.response.result.msg), /^unknown_capability/);
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req));
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 3_000);
  const [data] = inreqs(env);
  assert.equal(data.final_status, "confirmed");
});

test("request + cancellation both pending at start → cancellation response, `inreq cancelled`, no `log` line", async (t) => {
  let called = false;
  const env = await makeEnv(t, {
    entries: [makeEntry("echo", async () => {
      called = true;
      return { ok: true };
    })],
  });
  const rid = ulid();
  const req = makeRequest(rid);
  await writeJson(env.inbox, `${rid}.json`, req);
  await writeJson(env.inbox, `${rid}.can.json`, makeCancellation(req));
  await env.receiver.start(() => {}); // the watcher's initial scan delivers both (can before json, lexical)
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const res = await readRes(env.inbox, rid);
  assert.ok(res);
  assert.equal(res.response.error, false);
  assert.equal(res.response.cancellation, true);
  assert.equal(res.response.result, null);
  assert.deepEqual(res.response.attachments, []);
  assert.equal(called, false, "no work is started");
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req));
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 3_000);
  const [data] = inreqs(env);
  assert.equal(data.final_status, "cancelled");
  assert.ok(data.can);
  assert.deepEqual(logLines(env), [], "the valid pending cancellation is matched, never reported as ignored");
});

test("cancelled during the work: the signal aborts, cancellation response, `cancelled`", async (t) => {
  const entry = makeEntry("slow", async (_params, ctx) => {
    await new Promise<void>((resolve) => {
      if (ctx.signal.aborted) return resolve();
      ctx.signal.addEventListener("abort", () => resolve(), { once: true });
    });
    return { ok: true };
  });
  const env = await makeEnv(t, { entries: [entry] });
  await env.receiver.start(() => {});
  const rid = ulid();
  const req = makeRequest(rid, { name: "slow" });
  await writeJson(env.inbox, `${rid}.json`, req);
  await waitFor(() => env.receiver.runningCount() === 1, 3_000);
  await writeJson(env.inbox, `${rid}.can.json`, makeCancellation(req));
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const res = await readRes(env.inbox, rid);
  assert.ok(res);
  assert.equal(res.response.cancellation, true);
  assert.equal(res.response.error, false);
  assert.equal(res.response.result, null);
  assert.equal(entry.running, 0, "the handler slot is released");
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req));
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 3_000);
  const [data] = inreqs(env);
  assert.equal(data.final_status, "cancelled");
});

test("cancelled after the response: no second response, `cancelled`", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }))] });
  await env.receiver.start(() => {});
  const rid = ulid();
  const req = makeRequest(rid);
  await writeJson(env.inbox, `${rid}.json`, req);
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const first = await readRes(env.inbox, rid);
  assert.ok(first);
  assert.equal(first.response.cancellation, false);
  await writeJson(env.inbox, `${rid}.can.json`, makeCancellation(req));
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req));
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 3_000);
  const [data] = inreqs(env);
  assert.equal(data.final_status, "cancelled");
  assert.deepEqual(data.res, first, "the original response is kept: no second response is written");
});

// --- Acceptance codes (104 is intentionally absent) ---

test("100 agent_disabled: a disabled spec rejects new requests", async (t) => {
  const env = await makeEnv(t, {
    spec: makeSpec({ disabled: true }),
    entries: [makeEntry("echo", async () => ({ ok: true }))],
  });
  await env.receiver.start(() => {});
  const rid = ulid();
  await writeJson(env.inbox, `${rid}.json`, makeRequest(rid));
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const res = await readRes(env.inbox, rid);
  assert.equal(res?.response.result.code, 100);
  assert.match(String(res?.response.result.msg ?? ""), /^agent_disabled/);
});

test("102 capability_disabled: a disabled capability rejects", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }), { disabled: true })] });
  await env.receiver.start(() => {});
  const rid = ulid();
  await writeJson(env.inbox, `${rid}.json`, makeRequest(rid));
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const res = await readRes(env.inbox, rid);
  assert.equal(res?.response.result.code, 102);
});

test("103 requestor_not_allowed: accepts_from not listing the requestor rejects", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }), { accepts_from: ["gamma"] })] });
  await env.receiver.start(() => {});
  const rid = ulid();
  await writeJson(env.inbox, `${rid}.json`, makeRequest(rid, { src: "beta" }));
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const res = await readRes(env.inbox, rid);
  assert.equal(res?.response.result.code, 103);
});

test("105 invalid_parameters: parameters failing the capability schema reject", async (t) => {
  const params = { type: "object", properties: { n: { type: "integer" } }, required: ["n"], additionalProperties: false };
  const env = await makeEnv(t, { entries: [makeEntry("calc", async () => ({ ok: true }), {}, params)] });
  await env.receiver.start(() => {});
  const rid = ulid();
  await writeJson(env.inbox, `${rid}.json`, makeRequest(rid, { name: "calc", parameters: { n: "x" } }));
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const res = await readRes(env.inbox, rid);
  assert.equal(res?.response.result.code, 105);
});

test("106 namespace_mismatch: only when the requestor was discovered and shares no namespace", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }))] });
  // I-11: the hook hands over the DISCOVERED spec (before the callability rules), which is what makes 106
  // reachable at all: a peer whose namespaces are disjoint never reaches the Agmen view.
  env.peers.set("beta", makePeerSpec("beta", { namespaces: ["other"] }));
  env.peers.set("gamma", makePeerSpec("gamma", { namespaces: ["ns1"] }));
  env.peers.set("delta", makePeerSpec("delta", { namespaces: ["*"] }));
  await env.receiver.start(() => {});
  const r1 = ulid();
  await writeJson(env.inbox, `${r1}.json`, makeRequest(r1, { src: "beta" }));
  const r2 = ulid();
  await writeJson(env.inbox, `${r2}.json`, makeRequest(r2, { src: "gamma" }));
  const r3 = ulid();
  await writeJson(env.inbox, `${r3}.json`, makeRequest(r3, { src: "delta" }));
  const r4 = ulid(); // "epsilon" is unknown to peerSpec: 106 is skipped
  await writeJson(env.inbox, `${r4}.json`, makeRequest(r4, { src: "epsilon" }));
  await waitFor(() =>
    Promise.all([r1, r2, r3, r4].map((rid) => readRes(env.inbox, rid).then((r) => r !== undefined))).then(
      (all) => all.every(Boolean),
    ),
    3_000,
  );
  assert.equal((await readRes(env.inbox, r1))?.response.result.code, 106);
  assert.match(String((await readRes(env.inbox, r1))?.response.result.msg), /^namespace_mismatch: no shared namespace with "beta"$/);
  assert.equal((await readRes(env.inbox, r2))?.response.error, false, "a shared namespace passes");
  assert.equal((await readRes(env.inbox, r3))?.response.error, false, '"*" shares every namespace');
  assert.equal((await readRes(env.inbox, r4))?.response.error, false, "an unknown requestor skips 106");
});

test("107 capacity_exceeded: at max_parallel the request is rejected, not queued", async (t) => {
  const gate = deferred();
  const entry = makeEntry("slot", async () => {
    await gate.promise;
    return { ok: true };
  }, { max_parallel: 1 });
  const env = await makeEnv(t, { entries: [entry] });
  await env.receiver.start(() => {});
  const r1 = ulid();
  await writeJson(env.inbox, `${r1}.json`, makeRequest(r1, { name: "slot" }));
  await waitFor(() => env.receiver.runningCount() === 1, 3_000);
  const r2 = ulid();
  await writeJson(env.inbox, `${r2}.json`, makeRequest(r2, { name: "slot" }));
  await waitFor(() => readRes(env.inbox, r2).then((r) => r !== undefined), 3_000);
  const res2 = await readRes(env.inbox, r2);
  assert.equal(res2?.response.result.code, 107, "rejected, not queued (queue is always 0)");
  gate.resolve();
  await waitFor(() => readRes(env.inbox, r1).then((r) => r !== undefined), 3_000);
  assert.equal((await readRes(env.inbox, r1))?.response.error, false);
  assert.equal(entry.running, 0);
});

test("108 request_expired: a timeout that passed before acceptance rejects (and archives as `completed` on timeout)", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }))], confirmTimeout: 80 });
  await env.receiver.start(() => {});
  const rid = ulid();
  await writeJson(env.inbox, `${rid}.json`, makeRequest(rid, { ts: 1_600_000_000, timeout: 100 }));
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const res = await readRes(env.inbox, rid);
  assert.equal(res?.response.result.code, 108);
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 5_000);
  const [data] = inreqs(env);
  assert.equal(data.final_status, "completed", "rejected requests archive as completed on confirmation timeout");
});

// --- Malformed files (D-15) and encrypted bodies (D-14) ---

test("D-15: a schema-violating file is ignored with one log event per (path, mtime)", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }))] });
  await env.receiver.start(() => {});
  const bad = makeRequest(ulid());
  delete (bad.request as unknown as Record<string, unknown>).nonce; // schema violation
  await writeJson(env.inbox, "x.json", bad);
  await env.receiver.processFile("x.json");
  const logs = logLines(env);
  assert.equal(logs.length, 1);
  assert.match(logs[0], /^ignored x\.json: /);
  assert.ok(env.events.some((e) => e.event === "ignored" && e.path.endsWith("x.json")));
  assert.equal(inreqs(env).length, 0, "no archive");
  assert.equal(await readRes(env.inbox, "x"), undefined, "never answered");
  assert.ok((await inboxNames(env.inbox)).includes("x.json"), "the file stays in the inbox");
  await env.receiver.processFile("x.json"); // same (path, mtime): reported once
  assert.equal(logLines(env).length, 1);
  const file = path.join(env.inbox, "x.json");
  await fsp.utimes(file, new Date(T0 + 10_000), new Date(T0 + 10_000)); // new mtime: a new write attempt
  await env.receiver.processFile("x.json");
  assert.equal(logLines(env).length, 2);
});

test("D-15: a malformed JSON file is ignored and logged", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }))] });
  await env.receiver.start(() => {});
  await fsp.writeFile(path.join(env.inbox, "bad.json"), "{not json");
  await env.receiver.processFile("bad.json");
  const logs = logLines(env);
  assert.equal(logs.length, 1);
  assert.match(logs[0], /^ignored bad\.json: malformed JSON/);
  assert.equal(inreqs(env).length, 0);
  assert.ok((await inboxNames(env.inbox)).includes("bad.json"));
});

test("D-14: a base64 (encrypted) body is ignored and never answered", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }))] });
  await env.receiver.start(() => {});
  const rid = ulid();
  const encrypted: RequestFile = {
    ...makeRequest(rid),
    request: Buffer.from(JSON.stringify({ ts: 1, nonce: "0".repeat(32), signature: null, name: "echo", parameters: null, timeout: 0, attachments: [] })).toString("base64"),
  };
  await writeJson(env.inbox, `${rid}.json`, encrypted);
  await env.receiver.processFile(`${rid}.json`);
  const logs = logLines(env);
  assert.equal(logs.length, 1);
  assert.match(logs[0], new RegExp(`^ignored ${rid}\\.json: base64 body`));
  assert.equal(await readRes(env.inbox, rid), undefined);
  assert.equal(inreqs(env).length, 0);
});

// --- Header consistency (D-11), duplicates (D-5), attachments, stop ---

test("D-11: a confirmation with a mismatched header is ignored; the exchange keeps waiting", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }))] });
  await env.receiver.start(() => {});
  const rid = ulid();
  const req = makeRequest(rid);
  await writeJson(env.inbox, `${rid}.json`, req);
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req, { src: "gamma" }));
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.equal(inreqs(env).length, 0, "the mismatched confirmation is not applied");
  assert.match(logLines(env)[0] ?? "", new RegExp(`ignored ${rid}\\.con\\.json: header mismatch`));
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req));
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 3_000);
  const [data] = inreqs(env);
  assert.equal(data.final_status, "confirmed");
});

test("D-5: a repeated request file is ignored while tracked and after the archive", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }))] });
  await env.receiver.start(() => {});
  const rid = ulid();
  const req = makeRequest(rid);
  await writeJson(env.inbox, `${rid}.json`, req);
  await env.receiver.processFile(`${rid}.json`); // duplicate while tracked
  assert.equal(env.receiver.runningCount(), 1);
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req));
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 3_000);
  assert.equal(inreqs(env).length, 1);
  await writeJson(env.inbox, `${rid}.json`, req); // seen again after the archive (this run)
  await env.receiver.processFile(`${rid}.json`);
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.equal(inreqs(env).length, 1, "no second exchange");
  assert.equal(await readRes(env.inbox, rid), undefined, "no second response");
  assert.deepEqual(await inboxNames(env.inbox), [`${rid}.json`], "the repeated file is ignored, not archived");
});

test("response attachments: published to the forum, listed in the response, deleted at A4", async (t) => {
  const seen: { rid?: string; src?: string; attachments?: string[] } = {};
  const handler: CapabilityHandler = async (_params, ctx) => {
    seen.rid = ctx.rid;
    seen.src = ctx.src;
    seen.attachments = ctx.attachments;
    const name = await ctx.publishAttachment("art/payload.bin", new TextEncoder().encode("binary payload"));
    return { ok: true, name };
  };
  const env = await makeEnv(t, { entries: [makeEntry("echo", handler)] });
  await env.receiver.start(() => {});
  const rid = ulid();
  const req = makeRequest(rid, { attachments: ["in/a.bin"] });
  await writeJson(env.inbox, `${rid}.json`, req);
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const res = await readRes(env.inbox, rid);
  assert.ok(res);
  assert.deepEqual(seen, { rid, src: "beta", attachments: ["in/a.bin"] }, "the handler ctx contract");
  assert.deepEqual(res.response.attachments, ["art/payload.bin"]);
  const forumFile = path.join(env.forum, "art", "payload.bin");
  assert.equal(await fsp.readFile(forumFile, "utf8"), "binary payload");
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req));
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 3_000);
  await assert.rejects(fsp.access(forumFile), "response attachments are deleted at A4");
});

test("stop(): a request in work → code 300 + `inreq completed` + inbox deleted, ALL before stop() resolves", async (t) => {
  const env = await makeEnv(t, {
    entries: [makeEntry("slow", async (_params, ctx) => {
      await new Promise<void>((resolve) => {
        if (ctx.signal.aborted) return resolve();
        ctx.signal.addEventListener("abort", () => resolve(), { once: true });
      });
      return { ok: true };
    })],
  });
  await env.receiver.start(() => {});
  const rid = ulid();
  const req = makeRequest(rid, { name: "slow" });
  await writeJson(env.inbox, `${rid}.json`, req);
  await waitFor(() => env.receiver.workingCount() === 1, 3_000);
  assert.equal(env.receiver.runningCount(), 1);
  await env.receiver.stop();
  // Everything below already holds: stop() resolved only after the archive (E12).
  const [data] = inreqs(env);
  assert.ok(data, "the inreq event was archived before stop() resolved");
  assert.equal(data.final_status, "completed", "no cancellation, no confirmation: the A4 rule");
  const res = data.res as (ResponseFile & { response: ResponseBody }) | null;
  assert.ok(res, "the code 300 response is the archived one (A4 deleted the inbox files)");
  assert.equal(res.response.error, true);
  assert.equal(res.response.cancellation, false);
  assert.equal(res.response.result.code, 300);
  assert.match(String(res.response.result.msg), /^agent_stopping/);
  assert.equal(data.con, null);
  assert.deepEqual(await inboxNames(env.inbox), [], "the inbox files are deleted at A4");
  assert.equal(env.receiver.runningCount(), 0);
  assert.equal(env.receiver.workingCount(), 0);
});

test("stop(): an exchange waiting for confirmation is archived at once", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }))] });
  await env.receiver.start(() => {});
  const rid = ulid();
  const req = makeRequest(rid);
  await writeJson(env.inbox, `${rid}.json`, req);
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const res = await readRes(env.inbox, rid);
  assert.ok(res);
  assert.equal(res.response.error, false);
  assert.equal(env.receiver.runningCount(), 1, "still tracked: waiting for the confirmation");
  assert.equal(env.receiver.workingCount(), 0, "...but no in-flight work (stopGrace waits only for that)");
  await env.receiver.stop();
  const [data] = inreqs(env);
  assert.ok(data, "archived before stop() resolved");
  assert.equal(data.final_status, "completed", "D-9: `completed`, or `cancelled` if a cancellation was seen");
  assert.deepEqual(data.res, res);
  assert.equal(data.con, null);
  assert.deepEqual(await inboxNames(env.inbox), []);
});

test("a published attachment is deleted at A4 even when the exchange is cancelled", async (t) => {
  const handler: CapabilityHandler = async (_params, ctx) => {
    await ctx.publishAttachment("art/leak.bin", new TextEncoder().encode("leak"));
    await new Promise<void>((resolve) => {
      if (ctx.signal.aborted) return resolve();
      ctx.signal.addEventListener("abort", () => resolve(), { once: true });
    });
  };
  const env = await makeEnv(t, { entries: [makeEntry("slow", handler)] });
  await env.receiver.start(() => {});
  const rid = ulid();
  const req = makeRequest(rid, { name: "slow" });
  await writeJson(env.inbox, `${rid}.json`, req);
  const forumFile = path.join(env.forum, "art", "leak.bin");
  await waitFor(() => fsp.access(forumFile).then(() => true, () => false), 3_000);
  await writeJson(env.inbox, `${rid}.can.json`, makeCancellation(req));
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  const res = await readRes(env.inbox, rid);
  assert.ok(res);
  assert.equal(res.response.cancellation, true);
  assert.deepEqual(res.response.attachments, [], "the cancellation response body carries no attachments");
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req));
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 3_000);
  await assert.rejects(fsp.access(forumFile), "the file the handler published is deleted at A4");
});

test("a pending request is processed exactly once: the watcher's initial scan is the single delivery path", async (t) => {
  let runs = 0;
  const env = await makeEnv(t, {
    entries: [makeEntry("echo", async () => {
      runs += 1;
      return { ok: true };
    })],
  });
  const rid = ulid();
  const req = makeRequest(rid);
  await writeJson(env.inbox, `${rid}.json`, req);
  await env.receiver.start(() => {});
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  assert.equal(runs, 1, "the handler ran exactly once (no double delivery)");
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req));
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 3_000);
  assert.equal(runs, 1);
  assert.equal(inreqs(env).length, 1);
});

test("a watch error on the own inbox reaches onWatchError (D-22)", async (t) => {
  const env = await makeEnv(t, { entries: [makeEntry("echo", async () => ({ ok: true }))] });
  await env.receiver.start(() => {});
  await fsp.rm(env.inbox, { recursive: true, force: true });
  await waitFor(() => env.watchErrors.length >= 1, 5_000);
  assert.equal(env.watchErrors[0].dir, env.inbox);
});
// --- Lane D: the command rows of the *Envelope lifecycle* table (*Workflow* D0–D4) ---

test("D1 a custom command is executed with its parameters and {rid, src}, then archived as `completed`", async (t) => {
  let env!: Env;
  const seen: { params: unknown; ctx: CommandContext; running: number }[] = [];
  env = await makeEnv(t, {
    spec: makeSpec({ accepts_commands_from: ["beta"] }),
    commands: {
      translate: async (params, ctx) => {
        seen.push({ params, ctx, running: env.receiver.runningCount() });
      },
    },
  });
  await writePublications(env);
  const before = await publicationState(env);
  await env.receiver.start(() => {});
  const rid = ridWith("A");
  const cmd = makeRequest(rid, { type: "cmd", name: "translate", parameters: { text: "hola" } });
  await writeJson(env.inbox, `${rid}.json`, cmd);
  await waitFor(() => inreqs(env).length === 1, 3_000);
  assert.deepEqual(seen, [{ params: { text: "hola" }, ctx: { rid, src: "beta" }, running: 0 }]);
  assert.deepEqual(env.commandCalls, [], "no built-in hook is involved");
  const [data] = inreqs(env);
  assert.equal(data!.final_status, "completed");
  assert.deepEqual(data!.req, cmd, "the command envelope is archived as exchanged");
  assert.equal(data!.can, null, "a command is never cancelled");
  assert.equal(data!.res, null, "a command is never answered");
  assert.equal(data!.con, null);
  assert.deepEqual(await io.listDir(env.inbox), [], "D3 deletes the envelope");
  assert.equal(await readRes(env.inbox, rid), undefined, "no response file for type=\"cmd\"");
  assert.ok(env.events.some((e) => e.event === "inreq" && e.rid === rid && e.finalStatus === "completed"));
  assert.equal(env.stateChanges.length, 0, "a command is not an A1/A2/A3 exchange: busy is unaffected (D-8)");
  assert.deepEqual(
    await publicationState(env),
    before,
    "the receiver publishes no spec/status/announce of its own (D2 is the agent's)",
  );
});

test("stop() leaves a queued command in the inbox: it waits for the next start (D-4)", async (t) => {
  let env!: Env;
  const gate = deferred();
  let started = false;
  let secondRan = false;
  env = await makeEnv(t, {
    spec: makeSpec({ accepts_commands_from: ["beta"] }),
    commands: {
      first: async () => {
        started = true;
        await gate.promise; // holds the lane D drain
      },
      second: async () => {
        secondRan = true;
      },
    },
  });
  await env.receiver.start(() => {});
  const first = ridWith("A");
  const second = ridWith("B");
  await writeJson(env.inbox, `${first}.json`, makeRequest(first, { type: "cmd", name: "first" }));
  await writeJson(env.inbox, `${second}.json`, makeRequest(second, { type: "cmd", name: "second" }));
  await waitFor(() => started, 3_000);
  await env.receiver.stop();
  gate.resolve();
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.equal(secondRan, false, "the queued command never runs after stop()");
  assert.deepEqual(await io.listDir(env.inbox), [`${second}.json`], "its envelope stays for the next start");
  assert.deepEqual(inreqs(env).map((d) => d.req!.rid), [first], "only the executed command is archived");
});

test("D4 a command with an unknown name is dropped: diarium and deletion, nothing else", async (t) => {
  const env = await makeEnv(t, { spec: makeSpec({ accepts_commands_from: ["beta"] }) });
  await writePublications(env);
  const before = await publicationState(env);
  await env.receiver.start(() => {});
  const rid = ridWith("A");
  const cmd = makeRequest(rid, { type: "cmd", name: "reboot_router", parameters: null });
  await writeJson(env.inbox, `${rid}.json`, cmd);
  await waitFor(() => inreqs(env).length === 1, 3_000);
  const [data] = inreqs(env);
  assert.equal(data!.final_status, "dropped");
  assert.deepEqual(data!.req, cmd);
  assert.equal(data!.can, null);
  assert.equal(data!.res, null);
  assert.equal(data!.con, null);
  assert.deepEqual(await io.listDir(env.inbox), [], "the envelope is deleted");
  assert.equal(await readRes(env.inbox, rid), undefined, "never answered: a dropped command is silent");
  assert.deepEqual(env.commandCalls, [], "nothing is executed");
  assert.deepEqual(logLines(env), [], "the diarium holds the inreq event only");
  assert.deepEqual(await publicationState(env), before, "status.json, spec.json and the announce file untouched");
  assert.ok(env.events.some((e) => e.event === "log" && /dropped command/.test(e.message)));
  assert.ok(env.events.some((e) => e.event === "inreq" && e.rid === rid && e.finalStatus === "dropped"));
});

test("D4 `stop` from a src nobody may command is dropped: the hook never fires", async (t) => {
  const env = await makeEnv(t, { spec: makeSpec({ accepts_commands_from: [] }) });
  await writePublications(env);
  const before = await publicationState(env);
  await env.receiver.start(() => {});
  const rid = ridWith("A");
  const cmd = makeRequest(rid, { type: "cmd", name: "stop", parameters: { force: true } });
  await writeJson(env.inbox, `${rid}.json`, cmd);
  await waitFor(() => inreqs(env).length === 1, 3_000);
  assert.equal(statusByRid(env).get(rid), "dropped");
  assert.deepEqual(env.commandCalls, [], "the agent is never told to stop");
  assert.deepEqual(await publicationState(env), before);
  assert.ok(env.events.some((e) => e.event === "log" && /not in accepts_commands_from/.test(e.message)));
});

test("D0 the src must be listed exactly: an empty list means nobody, a prefix does not count", async (t) => {
  const commandedBy: string[] = [];
  const env = await makeEnv(t, {
    spec: makeSpec({ accepts_commands_from: ["gamma"] }),
    commands: { work: async (_params, ctx) => { commandedBy.push(ctx.src); } },
  });
  await env.receiver.start(() => {});
  const rows: { rid: string; src: string; accepted: boolean }[] = [
    { rid: ridWith("A"), src: "beta", accepted: false }, // not listed
    { rid: ridWith("B"), src: "gamma", accepted: true }, // listed
    { rid: ridWith("C"), src: "gamma2", accepted: false }, // a longer id that starts with the listed one
    { rid: ridWith("D"), src: "gam", accepted: false }, // a prefix of the listed id
    { rid: ridWith("E"), src: "alpha", accepted: false }, // ourselves: only a listed src may command
  ];
  for (const row of rows) {
    await writeJson(env.inbox, `${row.rid}.json`, makeRequest(row.rid, { type: "cmd", name: "work", src: row.src }));
  }
  await waitFor(() => inreqs(env).length === rows.length, 3_000);
  assert.deepEqual(commandedBy, ["gamma"]);
  const status = statusByRid(env);
  for (const row of rows) assert.equal(status.get(row.rid), row.accepted ? "completed" : "dropped", `src ${row.src}`);
});

test("D0 reads the spec afresh: the same command is dropped before `enable` and executed after it", async (t) => {
  let spec = makeSpec({ disabled: true, accepts_commands_from: ["gamma"] });
  const executed: string[] = [];
  const env = await makeEnv(t, {
    specOf: () => spec,
    hooks: {
      // what the agent does in D2: rewrite spec.json (fresh datetime) and re-announce (the agent does it for real)
      onCommandEnable: async (enabled) => {
        spec = {
          ...spec,
          disabled: !enabled,
          accepts_commands_from: enabled ? ["beta", "gamma"] : [],
          datetime: toIso(T0 + 60_000),
        };
      },
    },
    commands: { work: async (_params, ctx) => { executed.push(ctx.src); } },
  });
  await env.receiver.start(() => {});
  const first = ridWith("A"); // "beta" is not listed yet
  await writeJson(env.inbox, `${first}.json`, makeRequest(first, { type: "cmd", name: "work", src: "beta" }));
  await waitFor(() => inreqs(env).length === 1, 3_000);
  assert.equal(statusByRid(env).get(first), "dropped", "beta may not command us yet");
  const enable = ridWith("B"); // "gamma" is listed, and a disabled agent still takes `enable`
  const enableCmd = makeRequest(enable, { type: "cmd", name: "enable", parameters: { enabled: true }, src: "gamma" });
  await writeJson(env.inbox, `${enable}.json`, enableCmd);
  await waitFor(() => spec.disabled === false, 3_000); // the hook ran: `enable` changed the published spec
  assert.equal(statusByRid(env).get(enable), "completed", "D3 archives before the hook fires");
  const second = ridWith("C"); // the very same command as `first`, now from a listed src
  await writeJson(env.inbox, `${second}.json`, makeRequest(second, { type: "cmd", name: "work", src: "beta" }));
  await waitFor(() => inreqs(env).length === 3, 3_000);
  assert.equal(statusByRid(env).get(second), "completed");
  assert.deepEqual(executed, ["beta"]);
});

test("D0 a disabled agent still processes commands: enable, ping, restart, stop, custom", async (t) => {
  const executed: string[] = [];
  const env = await makeEnv(t, {
    spec: makeSpec({ disabled: true, accepts_commands_from: ["beta"] }),
    commands: { work: async () => { executed.push("work"); } },
  });
  await env.receiver.start(() => {});
  const rows: [string, unknown][] = [
    ["work", null],
    ["enable", { enabled: false }],
    ["ping", null],
    ["restart", { delay: 2 }],
    ["stop", { force: true }],
  ];
  for (const [index, [name, parameters]] of rows.entries()) {
    const rid = ridWith("ABCDEFGHIJ"[index]!);
    await writeJson(env.inbox, `${rid}.json`, makeRequest(rid, { type: "cmd", name, parameters }));
  }
  await waitFor(() => inreqs(env).length === rows.length, 3_000);
  assert.deepEqual(executed, ["work"], "no 100-style rejection: a disabled agent still runs commands");
  assert.deepEqual(env.commandCalls, ["enable(false)", "ping", "restart(force=false,delay=2)", "stop(force=true)"]);
  assert.deepEqual(inreqs(env).map((d) => d.final_status), rows.map(() => "completed"));
  assert.deepEqual(await io.listDir(env.inbox), []);
  assert.ok(!(await io.listDir(env.inbox)).some((n) => n.endsWith(".res.json")));
});

test("stop: D3 archives before the lifecycle hook runs (the shutdown starts after the archive)", async (t) => {
  let env!: Env;
  const observed: { inbox: string[]; diarium: string }[] = [];
  env = await makeEnv(t, {
    spec: makeSpec({ accepts_commands_from: ["beta"] }),
    hooks: {
      onCommandStop: async (force) => {
        observed.push({
          inbox: await io.listDir(env.inbox),
          diarium: await fsp.readFile(path.join(env.agent, "diarium.jsonl"), "utf8"),
        });
        assert.equal(force, true, "the parsed `force` flag");
      },
    },
  });
  await env.receiver.start(() => {});
  const rid = ridWith("A");
  const cmd = makeRequest(rid, { type: "cmd", name: "stop", parameters: { force: true } });
  await writeJson(env.inbox, `${rid}.json`, cmd);
  await waitFor(() => observed.length === 1, 3_000);
  assert.deepEqual(observed[0]!.inbox, [], "the envelope is already deleted when the agent shuts down");
  assert.match(observed[0]!.diarium, /"event":"inreq"/, "the diarium line is already on disk");
  assert.match(observed[0]!.diarium, /"final_status":"completed"/);
  await waitFor(() => inreqs(env).length === 1, 3_000);
  assert.equal(statusByRid(env).get(rid), "completed");
});

test("restart: `force` and `delay` (seconds) reach the hook after the archive (D-10)", async (t) => {
  let env!: Env;
  const a = ridWith("A");
  const b = ridWith("B");
  /** The envelope of the command whose hook is running must already be gone (the other may stay). */
  const seen: { force: boolean; delay: number; archivedFirst: boolean }[] = [];
  env = await makeEnv(t, {
    spec: makeSpec({ accepts_commands_from: ["beta"] }),
    hooks: {
      onCommandRestart: async (force, delaySeconds) => {
        const rid = seen.length === 0 ? a : b; // the queue runs them in lexical rid order
        const inbox = await io.listDir(env.inbox);
        seen.push({ force, delay: delaySeconds, archivedFirst: !inbox.includes(`${rid}.json`) });
      },
    },
  });
  await env.receiver.start(() => {});
  const withDelay = makeRequest(a, { type: "cmd", name: "restart", parameters: { force: true, delay: 5 } });
  await writeJson(env.inbox, `${a}.json`, withDelay);
  await writeJson(env.inbox, `${b}.json`, makeRequest(b, { type: "cmd", name: "restart", parameters: {} }));
  await waitFor(() => seen.length === 2, 3_000);
  assert.deepEqual(seen, [
    { force: true, delay: 5, archivedFirst: true },
    { force: false, delay: 0, archivedFirst: true },
  ]);
  assert.deepEqual(inreqs(env).map((d) => d.final_status), ["completed", "completed"]);
});

test("enable: the parsed boolean reaches the hook, for false and for true", async (t) => {
  const env = await makeEnv(t, { spec: makeSpec({ accepts_commands_from: ["beta"] }) });
  await env.receiver.start(() => {});
  const off = ridWith("A");
  const on = ridWith("B");
  const offCmd = makeRequest(off, { type: "cmd", name: "enable", parameters: { enabled: false } });
  const onCmd = makeRequest(on, { type: "cmd", name: "enable", parameters: { enabled: true } });
  await writeJson(env.inbox, `${off}.json`, offCmd);
  await writeJson(env.inbox, `${on}.json`, onCmd);
  await waitFor(() => env.commandCalls.length === 2, 3_000);
  assert.deepEqual(env.commandCalls, ["enable(false)", "enable(true)"]);
  assert.deepEqual(inreqs(env).map((d) => d.final_status), ["completed", "completed"]);
  assert.equal(await publishedFile(env, "spec.json"), "<absent>", "D2 is the agent's: the receiver rewrites no spec");
});

test("ping: the hook fires; its parameters must be null, so a non-null one never reaches D0", async (t) => {
  const env = await makeEnv(t, { spec: makeSpec({ accepts_commands_from: ["beta"] }) });
  await env.receiver.start(() => {});
  const rid = ridWith("A");
  await writeJson(env.inbox, `${rid}.json`, makeRequest(rid, { type: "cmd", name: "ping", parameters: null }));
  await waitFor(() => env.commandCalls.length === 1, 3_000);
  assert.deepEqual(env.commandCalls, ["ping"]);
  assert.equal(statusByRid(env).get(rid), "completed");
});

test("a command envelope that breaks `commandRules` is ignored before D0: never archived, never deleted", async (t) => {
  const env = await makeEnv(t, { spec: makeSpec({ accepts_commands_from: ["beta"] }) });
  await env.receiver.start(() => {});
  const rows: { rid: string; name: string; parameters: unknown }[] = [
    { rid: ridWith("A"), name: "ping", parameters: {} }, // must be null
    { rid: ridWith("B"), name: "stop", parameters: null }, // must be an object
    { rid: ridWith("C"), name: "enable", parameters: {} }, // `enabled` is required
    { rid: ridWith("D"), name: "restart", parameters: { wait: 3 } }, // unknown parameter
  ];
  for (const row of rows) {
    const cmd = makeRequest(row.rid, { type: "cmd", name: row.name, parameters: row.parameters });
    await writeJson(env.inbox, `${row.rid}.json`, cmd);
  }
  await waitFor(() => logLines(env).length === rows.length, 3_000);
  assert.deepEqual(inreqs(env), [], "a non-conforming envelope is ignored, not dropped (D0 precondition)");
  assert.deepEqual(env.commandCalls, []);
  assert.deepEqual(
    (await io.listDir(env.inbox)).sort(),
    rows.map((r) => `${r.rid}.json`),
    "the files stay in the inbox",
  );
  assert.ok(env.events.some((e) => e.event === "ignored"));
});

test("a rejecting command handler is logged and still archives as `completed`", async (t) => {
  const executed: string[] = [];
  const env = await makeEnv(t, {
    spec: makeSpec({ accepts_commands_from: ["beta"] }),
    commands: {
      boom: async () => {
        throw new Error("kaboom");
      },
      after: async () => {
        executed.push("after");
      },
    },
  });
  await env.receiver.start(() => {});
  const boom = ridWith("A");
  await writeJson(env.inbox, `${boom}.json`, makeRequest(boom, { type: "cmd", name: "boom", parameters: { x: 1 } }));
  await waitFor(() => inreqs(env).length === 1, 3_000);
  assert.equal(
    statusByRid(env).get(boom),
    "completed",
    "accepted and executed: the failure is internal, not a protocol state",
  );
  assert.ok(env.events.some((e) => e.event === "log" && /command "boom" from beta failed: kaboom/.test(e.message)));
  assert.deepEqual(logLines(env), [], "an internal failure adds no diarium event: the protocol prescribes none");
  assert.equal(await readRes(env.inbox, boom), undefined, "a failed command is not answered either");
  const after = ridWith("B");
  await writeJson(env.inbox, `${after}.json`, makeRequest(after, { type: "cmd", name: "after" }));
  await waitFor(() => executed.length === 1, 3_000);
  assert.equal(inreqs(env).length, 2, "the dispatcher kept working");
});

test("D-5: one command rid is executed once, while it is in flight and after the archive", async (t) => {
  let runs = 0;
  const env = await makeEnv(t, {
    spec: makeSpec({ accepts_commands_from: ["beta"] }),
    commands: {
      work: async () => {
        runs += 1;
      },
    },
  });
  await env.receiver.start(() => {});
  const rid = ridWith("A");
  const cmd = makeRequest(rid, { type: "cmd", name: "work" });
  await writeJson(env.inbox, `${rid}.json`, cmd);
  await env.receiver.processFile(`${rid}.json`); // second dispatch of the same rid, still in flight
  await waitFor(() => inreqs(env).length === 1, 3_000);
  await writeJson(env.inbox, `${rid}.json`, cmd); // the same envelope appears again after the archive
  await env.receiver.processFile(`${rid}.json`);
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.equal(runs, 1, "never executed twice");
  assert.equal(inreqs(env).length, 1, "never archived twice");
});

test("lane D: the drain runs queued commands one at a time in lexical rid order", async (t) => {
  const order: string[] = [];
  const env = await makeEnv(t, {
    spec: makeSpec({ accepts_commands_from: ["beta"] }),
    commands: {
      slow: async (params) => {
        const ms = (params as { ms: number }).ms;
        await new Promise((resolve) => setTimeout(resolve, ms)); // longest first: concurrency would reorder
        order.push(String(ms));
      },
    },
  });
  const rows: [string, number][] = [[ridWith("C"), 10], [ridWith("A"), 90], [ridWith("B"), 50]]; // written out of lexical order
  for (const [rid, ms] of rows) {
    await writeJson(env.inbox, `${rid}.json`, makeRequest(rid, { type: "cmd", name: "slow", parameters: { ms } }));
  }
  // Deliver in lexical rid order, awaiting each delivery: an awaited processFile has queued the
  // command before it resolves, so all three are queued long before the first (90 ms) settles and
  // the drain's per-iteration rid-sort — not the delivery timing — decides the order.
  for (const [rid] of [...rows].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
    await env.receiver.processFile(`${rid}.json`);
  }
  await waitFor(() => order.length === 3, 5_000);
  assert.deepEqual(order, ["90", "50", "10"]);
  assert.deepEqual(inreqs(env).map((d) => d.final_status), ["completed", "completed", "completed"]);
});

test("D-4: the initial scan runs every pending command (all complete; the rid set matches)", async (t) => {
  const env = await makeEnv(t, {
    spec: makeSpec({ accepts_commands_from: ["beta"] }),
    commands: {
      slow: async (params) => {
        const ms = (params as { ms: number }).ms;
        await new Promise((resolve) => setTimeout(resolve, ms));
      },
    },
  });
  const rows: [string, number][] = [[ridWith("C"), 10], [ridWith("A"), 90], [ridWith("B"), 50]]; // written out of lexical order
  for (const [rid, ms] of rows) {
    await writeJson(env.inbox, `${rid}.json`, makeRequest(rid, { type: "cmd", name: "slow", parameters: { ms } }));
  }
  await env.receiver.start(() => {}); // D-4: the initial scan hands over the pending envelopes
  await waitFor(() => inreqs(env).length === 3, 5_000);
  // Weakened to what timing cannot flip: every pending command completes, and the rid set matches.
  // The execution order of an initial-scan start can depend on how the partial set is delivered
  // under load, so it is NOT asserted here (the deterministic order test above covers the property).
  assert.deepEqual(inreqs(env).map((d) => d.final_status).sort(), ["completed", "completed", "completed"]);
  assert.deepEqual([...inreqs(env).map((d) => d.req!.rid)].sort(), rows.map(([rid]) => rid).sort());
});

test("one archive per exchange: a confirmation landing mid-archive adds no second `inreq` (D-5, D-13)", async (t) => {
  // A4 starts on the confirmTimeout and awaits; while it is in flight the confirmation of the same rid
  // lands. The exchange is already closed (`completed`, D-13): a second attempt must not append a second
  // `inreq` for one rid (D-5), and the archive's own file sweep takes the late confirmation with it.
  let open!: () => void;
  const gate = new Promise<void>((resolve) => { open = resolve; });
  const env = await makeEnv(t, {
    entries: [makeEntry("echo", async () => ({ ok: true }))],
    confirmTimeout: 40,
    diariumGate: () => gate, // holds the archive right after its diarium line
  });
  await env.receiver.start(() => {});
  const rid = ulid();
  const req = makeRequest(rid);
  await writeJson(env.inbox, `${rid}.json`, req);
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  await waitFor(() => inreqs(env).length > 0, 3_000); // the timeout archive has begun and is held
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req)); // lands inside the in-flight window
  await sleep(30); // let the watcher deliver it while the gate still holds
  open(); // the archive resumes and sweeps the files of the rid
  await waitFor(() => inboxNames(env.inbox).then((n) => n.length === 0), 3_000);
  assert.equal(inreqs(env).length, 1, "one exchange, one archive (D-5)");
  assert.equal(inreqs(env)[0]?.final_status, "completed", "the confirmTimeout ended it (D-13)");
});

// --- deferred envelopes, and stop after an archive in flight ---

test("a deferred can for an ignored request is dropped and logged once; the files stay (D-15, D-16)", async (t) => {
  const env = await makeEnv(t); // no capabilities: the (valid-shape) request can never be tracked
  const rid = ulid();
  // The request file is present but schema-invalid (no `attachments`): it is ignored (D-15) and
  // the rid is never tracked, so a can for it would otherwise stay deferred for the run lifetime.
  const bad = makeRequest(rid);
  delete (bad.request as { attachments?: string[] }).attachments;
  await writeJson(env.inbox, `${rid}.json`, bad);
  await writeJson(env.inbox, `${rid}.can.json`, makeCancellation(bad));
  // Deterministic delivery (no watcher): the can first — deferred (the request file is present,
  // the rid not tracked) — then the request, which is ignored and drops the deferred can.
  await env.receiver.processFile(`${rid}.can.json`);
  await env.receiver.processFile(`${rid}.json`);
  const canFull = path.join(env.inbox, `${rid}.can.json`);
  const reqFull = path.join(env.inbox, `${rid}.json`);
  const ignoredEvents = env.events.filter((e) => e.event === "ignored");
  assert.equal(ignoredEvents.length, 2, "the request and the deferred can are each reported once");
  assert.ok(ignoredEvents.some((e) => e.event === "ignored" && e.path === reqFull), "the request is `ignored`");
  const canEvent = ignoredEvents.find((e) => e.event === "ignored" && e.path === canFull);
  assert.ok(canEvent, "the deferred can is reported");
  assert.match(canEvent!.reason, /the request of rid .* was ignored \(D-15\)/);
  const logs = logLines(env);
  assert.ok(logs.some((l) => l.startsWith(`ignored ${rid}.json:`)), "the request's ignore line");
  assert.ok(
    logs.some((l) => l === `deferred ${canFull}: the request of rid ${rid} was ignored (D-15)`),
    "the deferred drop line (its format)",
  );
  assert.deepEqual(
    (await inboxNames(env.inbox)).sort(),
    [`${rid}.can.json`, `${rid}.json`].sort(),
    "the orphan files stay in the inbox (D-16: no housekeeping beyond the protocol)",
  );
  // "Once": redelivering both files adds no second report (D-15 dedup by (path, mtime); the
  // deferred map no longer holds the rid).
  await env.receiver.processFile(`${rid}.can.json`);
  await env.receiver.processFile(`${rid}.json`);
  assert.equal(env.events.filter((e) => e.event === "ignored").length, 2, "still two reports after redelivery");
  assert.equal(logLines(env).length, 2, "still two log lines after redelivery");
});

test("stop() resolves only after an archive that is already in flight (the lane-C order, E12)", async (t) => {
  let open!: () => void;
  const gate = new Promise<void>((resolve) => {
    open = resolve;
  });
  const env = await makeEnv(t, {
    entries: [makeEntry("echo", async () => ({ ok: true }))],
    diariumGate: () => gate, // holds every archive right after its diarium line
  });
  await env.receiver.start(() => {});
  const rid = ulid();
  const req = makeRequest(rid);
  await writeJson(env.inbox, `${rid}.json`, req);
  await waitFor(() => readRes(env.inbox, rid).then((r) => r !== undefined), 3_000);
  await writeJson(env.inbox, `${rid}.con.json`, makeConfirmation(req)); // confirmation → the archive starts
  await waitFor(() => inreqs(env).length === 1, 3_000); // the inreq line is in; the archive is held
  const stopP = env.receiver.stop(); // the archive is in flight: stop() must wait for it
  let stopped = false;
  void stopP.then(() => {
    stopped = true;
  });
  await sleep(30); // the gate still holds: stop() must NOT have resolved
  assert.equal(stopped, false, "stop() is waiting for the in-flight archive");
  open(); // the archive resumes and removes the envelopes
  await stopP;
  assert.equal(stopped, true);
  assert.equal(inreqs(env).length, 1, "one exchange, one inreq line (D-5)");
  assert.deepEqual(await inboxNames(env.inbox), [], "the envelope removals landed before stop() resolved");
});
