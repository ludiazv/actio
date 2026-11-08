// E2E — the child-process run, in the black-box style. `examples/echo-agent.ts` runs as a REAL child process on a scratch
// `ACTIO_DIR`; every envelope of every scenario is hand-written into its inbox — no library requestor,
// no CLI — and every file the child produces is validated against `../doc/schemas/` with a FRESH
// `Ajv2020` assembled in this file: not `src/validate.ts`, and not the embedded generated schemas.
// This file imports no runtime module from `src/`: the envelope shapes are transcribed from the schema
// files and Appendix A of the protocol, so a mistake in the implementation's own types cannot hide here.
//
// Scenarios (a hand-driven set of the E table):
//   E1  normal request (A.1)            E2  A0 acceptance errors (A.2)      E5  cancelled (A.3)
//   E12 `stop` with a request in flight E14 malformed files in the inbox
//   E4  (added) no confirmation ever arrives — with no library requestor there is nobody to confirm,
//       which is the confirmation-timeout path, and it is reachable ONLY by hand-writing envelopes.
//
// The four checks, in the child-process reading:
//   (a) the order of files in the receiver inbox, with `recordInbox` (the helper watches the directory
//       from our side, so it works across processes); asserted where the writes are separated in time
//       (E1, E12) and as a presence set where they are not (E5: the req and the can are written back to
//       back, so a single directory scan can see both and readdir order would decide the "order");
//   (b) the inbox is empty after the archive — with the malformed files of E14 asserted as the exception;
//   (c) every diarium line validates, and the `inreq` final statuses;
//   (d) attachments: these scenarios use none, so the forum is asserted empty (E1).
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test, type TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import { Ajv2020 } from "ajv/dist/2020.js";
import { recordInbox, waitFor, type InboxEvent } from "../helpers/index.js";

const projectRoot = path.resolve(fileURLToPath(import.meta.url), "../../..");

/** The child's agent id (`ACTIO_AGENT_ID`) and the id our hand-written envelopes come from. */
const ECHO = "echo";
const HAND = "hand"; // no folder, never discovered: A0 must still serve it (and never answer 106)
const VERSION = "0.1"; // the protocol version string

const opts = { timeout: 120_000 };

// ---------------------------------------------------------------- the envelope shapes, by hand
// Every shape below is the schema file transcribed: `common.schema.json#/$defs/header` plus the body of
// `request`/`cancellation`/`confirmation`. Nothing is imported from `src/generated/types.ts`.

type Body = Record<string, unknown>;
type Envelope = Record<string, unknown>;

/** ISO 8601 UTC to seconds, the `datetime` pattern of `common.schema.json`. */
const nowIso = (): string => new Date().toISOString().slice(0, 19) + "Z";
/** Unix epoch seconds, the `ts` of the bodies. */
const nowTs = (): number => Math.floor(Date.now() / 1000);
/** A 128-bit nonce in hex, the `nonce` pattern. */
const handNonce = (): string => randomBytes(16).toString("hex");

/**
 * A hand-written ULID-shaped rid: 26 Crockford base32 characters whose first character is 0-7 (the `rid`
 * pattern). The time half comes from the wall clock, the tail from the sequence number, so the rids of a
 * run are distinct and increase.
 */
function handRid(seq: number): string {
  const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
  let n = BigInt(Date.now()) * 256n + BigInt(seq);
  let tail = "";
  for (let i = 0; i < 25; i++) {
    tail = crockford[Number(n % 32n)] + tail;
    n /= 32n;
  }
  return `0${tail}`;
}

/** A `type: "req"` request of a capability (`request.schema.json`, body of `$defs/body`). */
function handRequest(rid: string, name: string, parameters: unknown): Envelope {
  const body: Body = {
    ts: nowTs(),
    nonce: handNonce(),
    signature: null, // posture `trusted`, core profile: no signature
    name,
    parameters,
    timeout: 0, // 0 = no requestor timeout: the child can never answer 108 for staleness
    attachments: [],
  };
  return { rid, src: HAND, dst: ECHO, datetime: nowIso(), type: "req", actio_version: VERSION, request: body };
}

/** A `type: "cmd"` command envelope; `name` is the command, `parameters` its (schema-shaped) arguments. */
function handCommand(rid: string, name: string, parameters: unknown): Envelope {
  return { ...handRequest(rid, name, parameters), type: "cmd" };
}

/** `<rid>.can.json`: the cancellation body (`name` must repeat the request's capability). */
function handCancel(rid: string, name: string, force: boolean): Envelope {
  const body: Body = { ts: nowTs(), nonce: handNonce(), signature: null, name, force };
  return { rid, src: HAND, dst: ECHO, datetime: nowIso(), type: "req", actio_version: VERSION, cancellation: body };
}

/** `<rid>.con.json`: the confirmation body, the only envelope with a body of nothing but ts/nonce/signature. */
function handConfirm(rid: string): Envelope {
  const body: Body = { ts: nowTs(), nonce: handNonce(), signature: null };
  return { rid, src: HAND, dst: ECHO, datetime: nowIso(), type: "req", actio_version: VERSION, confirmation: body };
}

// ---------------------------------------------------------------- files read and written by hand
const exists = (file: string): Promise<boolean> => fsp.access(file).then(() => true).catch(() => false);

async function readJson(file: string): Promise<unknown> {
  return JSON.parse(await fsp.readFile(file, "utf8"));
}

/**
 * Writes an envelope the way an agent does: a complete file renamed into place. The temporary name starts
 * with "." so the child's inbox scanner never sees a partial envelope (it ignores dot entries).
 */
async function writeEnvelope(dir: string, name: string, value: unknown): Promise<void> {
  const tmp = path.join(dir, `.${name}.${process.pid}.tmp`);
  await fsp.writeFile(tmp, JSON.stringify(value));
  await fsp.rename(tmp, path.join(dir, name));
}

/** The non-dot entries of a directory (`[]` when the directory does not exist). */
async function entries(dir: string): Promise<string[]> {
  const names = await fsp.readdir(dir).catch(() => [] as string[]);
  return names.filter((n) => !n.startsWith("."));
}

// ---------------------------------------------------------------- what the child leaves behind
/** A response file as the child writes it (`response.schema.json`). */
interface ResFile {
  rid: string;
  src: string;
  dst: string;
  datetime: string;
  type: string;
  actio_version: string;
  response: {
    ts: number;
    nonce: string;
    signature: string | null;
    error: boolean;
    cancellation: boolean;
    result: unknown;
    attachments: unknown[];
    work_time?: number;
  };
}

/** A diarium line (`diarium.schema.json`). */
interface DiaryLine {
  id: string;
  datetime: string;
  event: string;
  event_data: unknown;
}

/** The `event_data` of an `inreq` line: the exchange as archived (`diarium.schema.json#/$defs/exchange`). */
interface Archived {
  final_status: string;
  req: { rid: string; type: string } | null;
  can: unknown;
  res: ResFile | null;
  con: unknown;
}

const resultOf = (res: ResFile | null): { code?: number; msg?: string } =>
  (res?.response.result ?? {}) as { code?: number; msg?: string };

// ---------------------------------------------------------------- the independent validator
const schemaDir = path.resolve(projectRoot, "..", "doc", "schemas");

/** The contract schema files, read once per process (a data-file read, not a module import). */
let schemaDocs: Promise<unknown[]> | undefined;
function readSchemaDocs(): Promise<unknown[]> {
  schemaDocs ??= (async () => {
    const names = (await fsp.readdir(schemaDir)).filter((n) => n.endsWith(".json")).sort();
    assert.equal(names.length, 11, `all contract schemas of ../doc/schemas: ${names.join(", ")}`);
    const docs: unknown[] = [];
    for (const n of names) docs.push(JSON.parse(await fsp.readFile(path.join(schemaDir, n), "utf8")) as unknown);
    return docs;
  })();
  return schemaDocs;
}

/**
 * The reason why `value` breaks `<kind>.schema.json` of `../doc/schemas/`, or `undefined` when it validates.
 * A FRESH `Ajv2020` per call: nothing from `src/validate.ts`, no embedded schema, no shared instance.
 */
async function docRejection(kind: string, value: unknown): Promise<string | undefined> {
  const ajv = new Ajv2020({ strict: true, allErrors: true }); // the same strictness, from the other side
  for (const doc of await readSchemaDocs()) ajv.addSchema(doc as object);
  const check = ajv.getSchema(`${kind}.schema.json`);
  assert.ok(check, `doc/schemas/${kind}.schema.json has no root schema`);
  return check(value) ? undefined : ajv.errorsText(check.errors);
}

async function assertValidDoc(kind: string, value: unknown, what: string): Promise<void> {
  const why = await docRejection(kind, value);
  assert.equal(why, undefined, `${what} must validate against doc/schemas/${kind}.schema.json: ${why}`);
}

// ---------------------------------------------------------------- the child process
interface Child {
  proc: ChildProcess;
  root: string;
  agent: string;
  inbox: string;
  log: string[];
}

/** Resolves with the child's exit code, or -1 when it is still alive after `ms`. */
function exitCodeOf(child: ChildProcess, ms: number): Promise<number> {
  return new Promise((resolve) => {
    const finish = (code: number | null, signal: NodeJS.Signals | null): void =>
      resolve(code !== null ? code : signal === null ? 0 : -1);
    if (child.exitCode !== null || child.signalCode !== null) return finish(child.exitCode, child.signalCode);
    const timer = setTimeout(() => resolve(-1), ms);
    child.once("exit", (code, signal) => {
      clearTimeout(timer);
      finish(code, signal);
    });
  });
}

/**
 * Spawns `examples/echo-agent.ts` as a child process on its own temp root with fast protocol timers
 * (seconds, as the environment gives them), and reaps it by its own PID in the cleanup hook: only the
 * children this file spawned, never a pattern and never a /proc walk .
 */
async function spawnEcho(t: TestContext, env: Record<string, string> = {}): Promise<Child> {
  const root = await fsp.mkdtemp(path.join(os.tmpdir(), ".actio-child-"));
  const proc = spawn(process.execPath, ["--import", "tsx", "examples/echo-agent.ts"], {
    cwd: projectRoot,
    env: {
      ...process.env,
      ACTIO_DIR: root,
      ACTIO_SYNC: "none",
      ACTIO_HEARTBEAT: "0.2",
      ACTIO_CONFIRM_TIMEOUT: "5",
      ACTIO_CANCEL_WAIT: "5",
      ACTIO_AGENT_ID: ECHO,
      ACTIO_COMMANDERS: HAND, // our hand-written `stop` command must be accepted (D0)
      ...env,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const log: string[] = [];
  proc.stdout?.on("data", (d) => log.push(String(d)));
  proc.stderr?.on("data", (d) => log.push(String(d)));
  t.after(async () => {
    if (proc.exitCode === null && proc.signalCode === null) proc.kill("SIGKILL");
    await exitCodeOf(proc, 5_000);
    await fsp.rm(root, { recursive: true, force: true });
  });
  const child: Child = { proc, root, agent: path.join(root, ECHO), inbox: path.join(root, ECHO, "inbox"), log };
  // Started = the announce exists: publication (spec, status) and the inbox folder precede it (steps 1–6).
  await waitFor(() => exists(path.join(root, "nuntium", `${ECHO}.n.json`)), 20_000);
  return child;
}

/** The (a) recorder, armed on the child's inbox from our side. */
function watchInbox(
  t: TestContext,
  dir: string,
): {
  seq: (rid: string) => string[];
  names: (rid: string) => string[];
  events: () => InboxEvent[];
  kindOf: (name: string) => string;
  ridOf: (name: string) => string;
} {
  const rec = recordInbox(dir);
  t.after(() => rec.stop());
  const ridOf = (name: string): string => name.replace(/\.json$/, "").replace(/\.(res|can|con)$/, "");
  const kindOf = (name: string): string =>
    name === `${ridOf(name)}.json` ? "req" : name.replace(/^(.*)\.(res|can|con)\.json$/, "$2");
  const events = (): InboxEvent[] => rec.events();
  return {
    seq: (rid) =>
      events()
        .filter((e) => ridOf(e.name) === rid && e.kind === "appeared")
        .map((e) => kindOf(e.name)),
    names: (rid) => events().filter((e) => ridOf(e.name) === rid).map((e) => `${kindOf(e.name)}:${e.kind}`),
    events,
    kindOf,
    ridOf,
  };
}

/** Every diarium line of the child, parsed, and each one validated against the contract. */
async function diaryOf(child: Child): Promise<DiaryLine[]> {
  const text = await fsp.readFile(path.join(child.agent, "diarium.jsonl"), "utf8");
  const lines = text.split("\n").filter((l) => l.trim() !== "").map((l) => JSON.parse(l) as DiaryLine);
  for (const line of lines) await assertValidDoc("diarium", line, `the diarium line "${line.event}"`);
  return lines;
}

/** The `inreq` lines that archived the exchange of `rid` (more than one would break D-5). */
const inreqsOf = (lines: DiaryLine[], rid: string): Archived[] =>
  lines
    .filter((l) => l.event === "inreq" && (l.event_data as Archived)?.req?.rid === rid)
    .map((l) => l.event_data as Archived);

const statusOf = (child: Child): Promise<string> =>
  readJson(path.join(child.agent, "status.json")).then((s) => (s as { status: string }).status);

const announceOf = (child: Child): Promise<{ nuntium: string }> =>
  readJson(path.join(child.root, "nuntium", `${ECHO}.n.json`)).then((a) => a as { nuntium: string });

// ---------------------------------------------------------------- the scenarios
test("E1 (hand-written): a normal echo request, Appendix A.1, `confirmed` on both sides", opts, async (t) => {
  const child = await spawnEcho(t);
  const seq = watchInbox(t, child.inbox);
  const rid = handRid(1);

  // The child's publications, validated by the independent validator before anything is exchanged.
  const spec = await readJson(path.join(child.agent, "spec.json"));
  await assertValidDoc("spec", spec, "the child's spec.json");
  assert.equal((spec as { id: string; capabilities: { name: string }[] }).id, ECHO);
  assert.deepEqual(
    (spec as { capabilities: { name: string }[] }).capabilities.map((c) => c.name),
    ["echo", "sleep", "fail", "off", "private"],
    "the fixed capability set",
  );
  await assertValidDoc("status", await readJson(path.join(child.agent, "status.json")), "the child's status.json");
  await assertValidDoc("announce", await readJson(path.join(child.root, "nuntium", `${ECHO}.n.json`)), "the announce");
  assert.equal((await announceOf(child)).nuntium, "agent echo is active");

  // The request itself is validated against the contract before it is written: a reply with an
  // acceptance code would then be the implementation's fault, not the envelope's.
  const req = handRequest(rid, "echo", { text: "by hand" });
  await assertValidDoc("request", req, "our hand-written request");
  assert.equal(await statusOf(child), "idle", "idle before the exchange (D-8)");
  await writeEnvelope(child.inbox, `${rid}.json`, req);

  // The response the child produced, on disk, schema-valid, echoing the header of the request.
  const resFile = path.join(child.inbox, `${rid}.res.json`);
  await waitFor(() => exists(resFile), 20_000);
  const res = (await readJson(resFile)) as ResFile;
  await assertValidDoc("response", res, "the child's response file");
  assert.equal(res.rid, rid);
  assert.deepEqual(
    { src: res.src, dst: res.dst, type: res.type, actio_version: res.actio_version },
    { src: HAND, dst: ECHO, type: "req", actio_version: VERSION },
    "the header is shared by every file of the exchange (only `datetime` changes)",
  );
  assert.equal(res.response.error, false);
  assert.equal(res.response.cancellation, false);
  assert.equal(res.response.signature, null, "posture `trusted`: nothing is signed");
  assert.deepEqual(res.response.result, { text: "by hand" });

  // Our confirmation closes it; the archive then empties the inbox.
  await writeEnvelope(child.inbox, `${rid}.con.json`, handConfirm(rid));
  await waitFor(() => entries(child.inbox).then((n) => n.length === 0), 20_000);
  assert.deepEqual(seq.seq(rid), ["req", "res", "con"], "the order of the files in the inbox");

  const lines = await diaryOf(child);
  const ex = inreqsOf(lines, rid)[0];
  assert.ok(ex, "one `inreq` line for the exchange");
  assert.equal(ex.final_status, "confirmed", "the confirmation arrived before the timeout (D-13)");
  assert.ok(ex.req && ex.res && ex.con, "the archive keeps all three envelopes");
  assert.equal(lines.filter((l) => l.event === "inreq").length, 1, "one exchange, one archive (D-5)");
  await waitFor(() => statusOf(child).then((s) => s === "idle"), 20_000);
  assert.equal((await announceOf(child)).nuntium, "agent echo is active", "busy/idle is not announced (D-8)");
  assert.deepEqual(await entries(path.join(child.root, "forum")), [], "no attachments, the forum stays empty");
});

test("E2 (hand-written): A0 answers the acceptance codes 101, 102, 103", opts, async (t) => {
  const child = await spawnEcho(t);
  // The echo agent is fixed: `off` is capability-level disabled and `private` accepts only
  // `nobody`, so all three unusability codes are reachable against the conformance target.
  const probes = [
    { name: "nope", code: 101, msg: /^unknown_capability/, rid: handRid(11) },
    { name: "off", code: 102, msg: /^capability_disabled/, rid: handRid(12) },
    { name: "private", code: 103, msg: /^requestor_not_allowed/, rid: handRid(13) },
  ];
  for (const probe of probes) {
    const req = handRequest(probe.rid, probe.name, {});
    await assertValidDoc("request", req, `our hand-written request for "${probe.name}"`);
    await writeEnvelope(child.inbox, `${probe.rid}.json`, req);
    const resFile = path.join(child.inbox, `${probe.rid}.res.json`);
    await waitFor(() => exists(resFile), 20_000);
    const res = (await readJson(resFile)) as ResFile;
    await assertValidDoc("response", res, `the ${probe.code} response file`);
    assert.equal(res.response.error, true, "an acceptance error is an error response");
    assert.equal(res.response.cancellation, false, "and not a cancellation");
    assert.equal(resultOf(res).code, probe.code, `${probe.name}: ${JSON.stringify(res.response.result)}`);
    assert.match(String(resultOf(res).msg), probe.msg, "the message names the code (D-15 quality)");
    await assertValidDoc("error", res.response.result, `the error result of code ${probe.code}`);
    // Rejected at A0 is still a full exchange: confirmed, then archived like any other.
    await writeEnvelope(child.inbox, `${probe.rid}.con.json`, handConfirm(probe.rid));
    await waitFor(() => entries(child.inbox).then((n) => n.length === 0), 20_000);
  }
  const lines = await diaryOf(child);
  for (const probe of probes) {
    const archived = inreqsOf(lines, probe.rid);
    assert.equal(archived.length, 1, `one \`inreq\` line for the rejected "${probe.name}" request (D-5)`);
    assert.equal(archived[0].final_status, "confirmed", "a rejected request is confirmed like any request");
    assert.equal(resultOf(archived[0].res).code, probe.code, "the archive keeps the answer it gave");
    assert.ok(archived[0].con, "and the confirmation we wrote");
  }
  assert.equal(lines.filter((l) => l.event === "inreq").length, probes.length, "one archive per exchange (D-5)");
});

test("E5 (hand-written): request and cancellation back to back → `cancelled`", opts, async (t) => {
  const child = await spawnEcho(t);
  const rid = handRid(21);
  const req = handRequest(rid, "sleep", { seconds: 10 });
  await assertValidDoc("request", req, "our hand-written `sleep` request");
  await writeEnvelope(child.inbox, `${rid}.json`, req);
  await writeEnvelope(child.inbox, `${rid}.can.json`, handCancel(rid, "sleep", false));
  const canFile = await readJson(path.join(child.inbox, `${rid}.can.json`));
  await assertValidDoc("cancellation", canFile, "our hand-written cancellation");

  const resFile = path.join(child.inbox, `${rid}.res.json`);
  await waitFor(() => exists(resFile), 20_000);
  const res = (await readJson(resFile)) as ResFile;
  await assertValidDoc("response", res, "the cancellation response");
  assert.equal(res.response.cancellation, true, "A2 acknowledges the cancellation");
  assert.equal(res.response.error, false, "a cancellation is not an error");
  assert.equal(res.response.result, null, "a cancellation response carries no result");
  assert.deepEqual(res.response.attachments, [], "and no attachments");
  assert.ok(await exists(path.join(child.inbox, `${rid}.can.json`)), "the cancellation envelope is still there");

  await writeEnvelope(child.inbox, `${rid}.con.json`, handConfirm(rid));
  await waitFor(() => entries(child.inbox).then((n) => n.length === 0), 20_000);
  const lines = await diaryOf(child);
  const ex = inreqsOf(lines, rid)[0];
  assert.ok(ex, "one `inreq` line");
  assert.equal(ex.final_status, "cancelled", "a seen cancellation decides the final status (D-13)");
  assert.ok(ex.can, "the cancellation is part of the archived exchange");
  assert.equal(lines.filter((l) => l.event === "inreq").length, 1, "one exchange, one archive (D-5)");
  // A.3: the cancellation belongs to the exchange, and the 10 s of work was aborted rather than
  // awaited: the answer the child gave carries no `{ slept: 10 }` result of a completed sleep.
  assert.ok(ex.res, "the cancellation response is archived");
  assert.notDeepEqual(ex.res?.response.result, { slept: 10 }, "the work did not run to completion");
});

test("E4 (hand-written, extra): nobody confirms → the confirmTimeout archives `completed`", opts, async (t) => {
  // Only a hand-written exchange can do this: a library requestor always confirms (D-13).
  const child = await spawnEcho(t, { ACTIO_CONFIRM_TIMEOUT: "0.5" });
  const rid = handRid(41);
  await writeEnvelope(child.inbox, `${rid}.json`, handRequest(rid, "echo", { text: "unconfirmed" }));
  const resFile = path.join(child.inbox, `${rid}.res.json`);
  await waitFor(() => exists(resFile), 20_000);
  await assertValidDoc("response", await readJson(resFile), "the response of the unconfirmed exchange");
  // We write no confirmation: the child's own D-13 timer has to end the exchange.
  await waitFor(() => entries(child.inbox).then((n) => n.length === 0), 20_000);
  const lines = await diaryOf(child);
  const ex = inreqsOf(lines, rid)[0];
  assert.ok(ex, "one `inreq` line");
  assert.equal(ex.final_status, "completed", "no confirmation within the timeout (D-13)");
  assert.equal(ex.con, null, "and the archive records that");
});

test("E12 (hand-written): `stop` with a request in flight → 300, archive, inactive, exit 0", opts, async (t) => {
  const child = await spawnEcho(t);
  const seq = watchInbox(t, child.inbox);
  const workRid = handRid(31);
  const cmdRid = handRid(32);
  await writeEnvelope(child.inbox, `${workRid}.json`, handRequest(workRid, "sleep", { seconds: 10 }));
  await waitFor(() => statusOf(child).then((s) => s === "busy"), 20_000); // accepted, working (A1)

  const cmd = handCommand(cmdRid, "stop", { force: true });
  await assertValidDoc("request", cmd, "our hand-written `stop` command");
  await writeEnvelope(child.inbox, `${cmdRid}.json`, cmd);

  // The agent stops itself and exits 0 on its own: every watcher is closed by lane C.
  const code = await exitCodeOf(child.proc, 25_000);
  assert.equal(code, 0, `exit 0 after the stop command (child log: ${child.log.join("")})`);

  // Commands are never answered: no response file for the command rid, and the inbox is empty.
  assert.equal(await exists(path.join(child.inbox, `${cmdRid}.res.json`)), false, "a command gets no response");
  assert.deepEqual(await entries(child.inbox), [], "lane C archived everything (D-9)");
  // (a): the whole inbox timeline of this scenario, in the order the files appeared — the work is
  // answered only because the command made the agent stop, so the answer must come last.
  const label = (name: string): string => {
    const who = seq.ridOf(name) === workRid ? "work" : seq.ridOf(name) === cmdRid ? "cmd" : "other";
    return `${seq.kindOf(name)}(${who})`;
  };
  const appeared = seq.events().filter((e) => e.kind === "appeared").map((e) => label(e.name));
  assert.deepEqual(appeared, ["req(work)", "req(cmd)", "res(work)"], "request, then command, then the 300 answer");
  assert.equal(
    seq.events().filter((e) => e.kind === "disappeared").length,
    3,
    "all three envelopes were archived away",
  );

  const lines = await diaryOf(child);
  const work = inreqsOf(lines, workRid)[0];
  assert.ok(work, "the in-flight request is archived");
  assert.equal(work.final_status, "completed", "answered with 300, so nothing was left to confirm");
  assert.equal(work.res?.response.error, true, "the answer is an error response");
  assert.equal(resultOf(work.res).code, 300, `agent_stopping: ${JSON.stringify(work.res?.response.result)}`);
  assert.match(String(resultOf(work.res).msg), /^agent_stopping/);
  await assertValidDoc("response", work.res, "the archived 300 response");
  await assertValidDoc("request", work.req, "the archived request of the interrupted work");
  const stopCmd = inreqsOf(lines, cmdRid)[0];
  assert.ok(stopCmd, "the command is archived too (D3)");
  assert.equal(stopCmd.final_status, "completed");
  assert.equal(stopCmd.req?.type, "cmd", "archived as a command envelope");
  assert.equal(stopCmd.res, null, "with no response in the archive");
  assert.equal(inreqsOf(lines, workRid).length, 1, "one exchange, one archive (D-5)");

  // E12's order: the archive precedes C1 — `inreq` comes before the `status` event of inactive.
  const indexOfInreq = lines.findIndex((l) => l.event === "inreq" && (l.event_data as Archived)?.req?.rid === workRid);
  const indexOfInactive = lines.findIndex((l) => l.event === "status" && l.event_data === "inactive");
  assert.ok(indexOfInreq >= 0 && indexOfInactive >= 0, "both events are in the diarium");
  assert.ok(indexOfInreq < indexOfInactive, "`inreq` archived before C1 wrote `inactive`");
  assert.equal(await statusOf(child), "inactive");
  await assertValidDoc("status", await readJson(path.join(child.agent, "status.json")), "the inactive status");
  assert.equal((await announceOf(child)).nuntium, "agent echo is inactive");
  const announceFile = await readJson(path.join(child.root, "nuntium", `${ECHO}.n.json`));
  await assertValidDoc("announce", announceFile, "the inactive announce");
});

test("E14 (hand-written): bad JSON and a schema-violating envelope stay in the inbox, unanswered", opts, async (t) => {
  const child = await spawnEcho(t);
  const badJsonRid = handRid(51);
  const badSchemaRid = handRid(52);
  const brokenJson = `{"rid": "${badJsonRid}", "request": {"name": "echo"`; // truncated: not parsable
  const brokenSchema = handRequest(badSchemaRid, "echo", { text: "x" });
  delete (brokenSchema.request as Body).signature; // a required body field is missing
  const why = await docRejection("request", brokenSchema);
  assert.ok(why !== undefined && why.includes("signature"), `the planted envelope must break the contract: ${why}`);
  await fsp.writeFile(path.join(child.inbox, `${badJsonRid}.json`), brokenJson);
  await writeEnvelope(child.inbox, `${badSchemaRid}.json`, brokenSchema);

  // Neither is ever answered (D-15: reported, never answered).
  await assert.rejects(
    waitFor(
      () =>
        exists(path.join(child.inbox, `${badJsonRid}.res.json`)).then((a) =>
          exists(path.join(child.inbox, `${badSchemaRid}.res.json`)).then((b) => a || b),
        ),
      3_000,
    ),
    /waitFor: timeout/,
    "no response for a malformed envelope",
  );
  // Both stay where they are, byte for byte.
  assert.equal(await fsp.readFile(path.join(child.inbox, `${badJsonRid}.json`), "utf8"), brokenJson);
  assert.deepEqual(Object.keys((await readJson(path.join(child.inbox, `${badSchemaRid}.json`))) as Body), [
    "rid",
    "src",
    "dst",
    "datetime",
    "type",
    "actio_version",
    "request",
  ]);
  const before = (await entries(child.inbox)).sort();
  assert.deepEqual(before, [`${badJsonRid}.json`, `${badSchemaRid}.json`].sort(), "the two files are the leftovers");

  // No exchange is created for them, and the agent is unharmed: a valid exchange still works.
  const goodRid = handRid(53);
  await writeEnvelope(child.inbox, `${goodRid}.json`, handRequest(goodRid, "echo", { text: "still here" }));
  await waitFor(() => exists(path.join(child.inbox, `${goodRid}.res.json`)), 20_000);
  await writeEnvelope(child.inbox, `${goodRid}.con.json`, handConfirm(goodRid));
  await waitFor(() => entries(child.inbox).then((n) => n.length === 2), 20_000);
  const lines = await diaryOf(child);
  // D-15 reports each of them once, and the diarium carries that report (the `log` event of E14).
  const logs = lines.filter((l) => l.event === "log").map((l) => String(l.event_data));
  assert.ok(
    logs.some((m) => m.startsWith(`ignored ${badJsonRid}.json: malformed JSON`)),
    `the unparsable file is reported: ${JSON.stringify(logs)}`,
  );
  assert.ok(
    logs.some((m) => m.startsWith(`ignored ${badSchemaRid}.json:`) && m.includes("signature")),
    `the schema violation is reported with its reason: ${JSON.stringify(logs)}`,
  );
  const strays = inreqsOf(lines, badJsonRid).length + inreqsOf(lines, badSchemaRid).length;
  assert.equal(strays, 0, "nothing is archived for a malformed envelope");
  assert.equal(lines.filter((l) => l.event === "inreq").length, 1, "only the valid exchange reached A4");
  assert.equal(inreqsOf(lines, goodRid)[0]?.final_status, "confirmed", "the valid exchange is archived normally");
  assert.equal(await statusOf(child), "idle", "the agent is unharmed");
});
