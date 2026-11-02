// Independent black-box run: the echo agent runs as a child process, the envelopes are written BY HAND
// (no actio-ts code is imported), and every file it produces is validated with a fresh Ajv2020 built
// from doc/schemas. Usage: npm run blackbox [-- <path to an actio-ts checkout>] (default: this one).
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";

const TS = path.resolve(process.argv[2] ?? path.join(import.meta.dirname, "..", ".."));
const SCHEMAS = path.resolve(TS, "..", "doc", "schemas");
const require = createRequire(path.join(TS, "package.json"));
const { Ajv2020 } = require("ajv/dist/2020.js");

// ---------- independent validator ----------
const ajv = new Ajv2020({ strict: true, allErrors: true });
for (const f of fs.readdirSync(SCHEMAS)) ajv.addSchema(JSON.parse(fs.readFileSync(path.join(SCHEMAS, f), "utf8")));
function check(kind, obj, what) {
  const v = ajv.getSchema(`${kind}.schema.json`);
  if (!v(obj)) throw new Error(`${what} violates ${kind}.schema.json: ${ajv.errorsText(v.errors)}`);
}

// ---------- hand-made envelopes ----------
const B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
let lastMs = 0, seq = 0;
function ulid() {
  let ms = Date.now();
  if (ms <= lastMs) ms = lastMs; lastMs = ms;
  let t = "";
  for (let i = 9; i >= 0; i--) { t = B32[ms % 32] + t; ms = Math.floor(ms / 32); }
  let r = ""; const rb = randomBytes(16);
  for (let i = 0; i < 16; i++) r += B32[rb[i] % 32];
  seq++;
  return t + r;
}
const nonce = () => randomBytes(16).toString("hex");
const iso = (ms = Date.now()) => new Date(ms).toISOString().replace(/\.\d{3}Z$/, "Z");
const nowS = () => Math.floor(Date.now() / 1000);
const header = (rid, src, dst, type = "req") => ({ rid, src, dst, datetime: iso(), type, actio_version: "0.1" });
const reqFile = (rid, src, dst, name, parameters, { type = "req", timeout = 0, ts = nowS() } = {}) => ({
  ...header(rid, src, dst, type),
  request: { ts, nonce: nonce(), signature: null, name, parameters, timeout, attachments: [] },
});
const canFile = (req) => ({ ...strip(req), datetime: iso(), cancellation: { ts: nowS(), nonce: nonce(), signature: null, name: req.request.name, force: false } });
const conFile = (req) => ({ ...strip(req), datetime: iso(), confirmation: { ts: nowS(), nonce: nonce(), signature: null } });
function strip(f) { const { rid, src, dst, datetime, type, actio_version } = f; return { rid, src, dst, datetime, type, actio_version }; }

// ---------- world ----------
let ROOT, SCRATCH;
const inbox = (id) => path.join(ROOT, id, "inbox");
async function put(file, obj) { // atomic, written by hand: temp on the same FS, then rename
  const tmp = path.join(SCRATCH, `.${path.basename(file)}.${nonce()}.tmp`);
  await fsp.writeFile(tmp, typeof obj === "string" ? obj : JSON.stringify(obj));
  await fsp.rename(tmp, file);
}
const readJ = async (f) => JSON.parse(await fsp.readFile(f, "utf8"));
const exists = (f) => fsp.access(f).then(() => true, () => false);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(pred, ms, what) {
  const end = Date.now() + ms;
  for (;;) { try { if (await pred()) return; } catch {} if (Date.now() > end) throw new Error(`timeout: ${what}`); await sleep(20); }
}
async function diary(id) {
  const raw = await fsp.readFile(path.join(ROOT, id, "diarium.jsonl"), "utf8");
  return raw.split("\n").filter((l) => l !== "").map((l, i) => { const o = JSON.parse(l); check("diarium", o, `${id} diarium line ${i + 1}`); return o; });
}
const inreqOf = async (id, rid) => (await diary(id)).find((l) => l.event === "inreq" && l.event_data?.req?.rid === rid);
async function validatePublication(id) {
  check("spec", await readJ(path.join(ROOT, id, "spec.json")), `${id}/spec.json`);
  check("status", await readJ(path.join(ROOT, id, "status.json")), `${id}/status.json`);
  check("announce", await readJ(path.join(ROOT, "nuntium", `${id}.n.json`)), `nuntium/${id}.n.json`);
}
async function captureRes(id, rid, ms = 6000) {
  const f = path.join(inbox(id), `${rid}.res.json`);
  await waitFor(() => exists(f), ms, `${rid}.res.json`);
  const res = await readJ(f);
  check("response", res, `${rid}.res.json`);
  return res;
}
function sameHeader(a, b, what) {
  for (const k of ["rid", "src", "dst", "type", "actio_version"]) if (a[k] !== b[k]) throw new Error(`${what}: header ${k} differs`);
}
async function emptyInbox(id, ms = 6000) { await waitFor(async () => (await fsp.readdir(inbox(id))).length === 0, ms, `${id} inbox empty`); }
const eq = (a, b, what) => { if (JSON.stringify(a) !== JSON.stringify(b)) throw new Error(`${what}: expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`); };

function startAgent(env) {
  const child = spawn(process.execPath, ["--import", "tsx", "examples/echo-agent.ts"], {
    cwd: TS, stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, ACTIO_DIR: ROOT, ACTIO_SYNC: "none", ACTIO_HEARTBEAT: "1", ACTIO_CONFIRM_TIMEOUT: "2", ACTIO_CANCEL_WAIT: "2", ...env },
  });
  child.log = []; child.stdout.on("data", (d) => child.log.push(String(d))); child.stderr.on("data", (d) => child.log.push(String(d)));
  child.exited = new Promise((r) => child.once("exit", (code, sig) => r(code ?? sig)));
  return child;
}
async function upAgent(id, env = {}) {
  const c = startAgent({ ACTIO_AGENT_ID: id, ...env });
  await waitFor(() => exists(path.join(ROOT, "nuntium", `${id}.n.json`)), 20000, `${id} announce`);
  await waitFor(async () => (await readJ(path.join(ROOT, id, "status.json"))).status === "idle", 5000, `${id} idle`);
  return c;
}

/** One exchange by hand: write the request, capture + validate the response, confirm, wait for A4. */
async function exchange(id, name, params, opts = {}) {
  const rid = ulid();
  const req = reqFile(rid, opts.src ?? "blackbox", id, name, params, opts);
  check("request", req, "hand-written request");
  await put(path.join(inbox(id), `${rid}.json`), req);
  const res = await captureRes(id, rid);
  sameHeader(res, req, "res vs req");
  if (opts.confirm !== false) {
    if (await exists(path.join(inbox(id), `${rid}.json`))) await put(path.join(inbox(id), `${rid}.con.json`), conFile(req));
  }
  return { rid, req, res };
}

// ---------- scenarios ----------
const results = [];
async function scenario(name, fn) {
  const t0 = Date.now();
  try { const note = await fn(); results.push(["PASS", name, note ?? "", Date.now() - t0]); }
  catch (e) { results.push(["FAIL", name, e.message, Date.now() - t0]); }
}

async function main() {
  ROOT = await fsp.mkdtemp(path.join(os.tmpdir(), ".actio-blackbox-"));
  SCRATCH = path.join(ROOT, ".blackbox-tmp"); await fsp.mkdir(SCRATCH);
  const echo = await upAgent("echo", { ACTIO_COMMANDERS: "blackbox" });
  await validatePublication("echo");

  await scenario("E1 normal request → confirmed", async () => {
    const { rid, req, res } = await exchange("echo", "echo", { text: "bb" });
    eq(res.response.error, false, "error"); eq(res.response.result, { text: "bb" }, "result");
    await emptyInbox("echo");
    const l = await inreqOf("echo", rid);
    eq(l?.event_data.final_status, "confirmed", "inreq final_status");
    eq(l.event_data.req, req, "archived req is the exchanged one");
  });
  await scenario("E2 unknown capability → 101, confirmed", async () => {
    const { rid, res } = await exchange("echo", "ghost", {});
    eq([res.response.error, res.response.result.code], [true, 101], "error/code"); check("error", res.response.result, "101 result");
    await emptyInbox("echo"); eq((await inreqOf("echo", rid)).event_data.final_status, "confirmed", "inreq");
  });
  await scenario("E3 rejections 102/103/105/108 (+107, 100 below)", async () => {
    const codes = {};
    for (const [label, name, params, o] of [
      ["102", "off", {}, {}], ["103", "private", {}, {}], ["105", "echo", { wrong: 1 }, {}],
      ["108", "echo", { text: "x" }, { ts: nowS() - 100, timeout: 5 }],
    ]) { const { res } = await exchange("echo", name, params, o); codes[label] = res.response.result?.code; check("error", res.response.result, label); }
    eq(codes, { 102: 102, 103: 103, 105: 105, 108: 108 }, "codes");
    await emptyInbox("echo");
  });
  await scenario("E3 107 capacity_exceeded (sleep max_parallel 1)", async () => {
    const r1 = ulid(), q1 = reqFile(r1, "blackbox", "echo", "sleep", { seconds: 1.5 });
    await put(path.join(inbox("echo"), `${r1}.json`), q1);
    await waitFor(async () => (await readJ(path.join(ROOT, "echo", "status.json"))).capability_status.find((c) => c.name === "sleep").running === 1, 4000, "sleep running");
    const { res } = await exchange("echo", "sleep", { seconds: 1 });
    eq(res.response.result.code, 107, "second sleep");
    const res1 = await captureRes("echo", r1); eq(res1.response.result, { slept: 1.5 }, "first sleep result");
    await put(path.join(inbox("echo"), `${r1}.con.json`), conFile(q1));
    await emptyInbox("echo");
  });
  await scenario("E3 100 agent_disabled + E10 enable false/true (spec toggled, re-announce)", async () => {
    const annBefore = await readJ(path.join(ROOT, "nuntium", "echo.n.json"));
    const c1 = ulid();
    await put(path.join(inbox("echo"), `${c1}.json`), reqFile(c1, "blackbox", "echo", "enable", { enabled: false }, { type: "cmd" }));
    await waitFor(async () => (await readJ(path.join(ROOT, "echo", "spec.json"))).disabled === true, 5000, "spec disabled");
    await waitFor(async () => (await readJ(path.join(ROOT, "nuntium", "echo.n.json"))).nuntium !== annBefore.nuntium, 5000, "re-announce");
    await validatePublication("echo");
    const { res } = await exchange("echo", "echo", { text: "x" });
    eq(res.response.result.code, 100, "disabled → 100");
    const c2 = ulid();
    await put(path.join(inbox("echo"), `${c2}.json`), reqFile(c2, "blackbox", "echo", "enable", { enabled: true }, { type: "cmd" }));
    await waitFor(async () => (await readJ(path.join(ROOT, "echo", "spec.json"))).disabled === false, 5000, "spec enabled");
    await emptyInbox("echo");
    eq((await inreqOf("echo", c1)).event_data.final_status, "completed", "enable cmd archived");
    const { res: ok } = await exchange("echo", "echo", { text: "back" }); eq(ok.response.error, false, "works again");
    await emptyInbox("echo");
  });
  await scenario("E4 no confirmation → receiver archives completed after confirmTimeout", async () => {
    const { rid } = await exchange("echo", "echo", { text: "x" }, { confirm: false });
    await emptyInbox("echo", 6000);
    eq((await inreqOf("echo", rid)).event_data.final_status, "completed", "inreq");
  });
  await scenario("E5 cancelled before acceptance → cancellation response, cancelled", async () => {
    const rid = ulid(), req = reqFile(rid, "blackbox", "echo", "sleep", { seconds: 3 });
    await put(path.join(inbox("echo"), `${rid}.can.json`), canFile(req));
    await put(path.join(inbox("echo"), `${rid}.json`), req);
    const res = await captureRes("echo", rid, 2500);
    eq([res.response.error, res.response.cancellation, res.response.result], [false, true, null], "cancellation response");
    await put(path.join(inbox("echo"), `${rid}.con.json`), conFile(req));
    await emptyInbox("echo"); eq((await inreqOf("echo", rid)).event_data.final_status, "cancelled", "inreq");
  });
  await scenario("E6/E8 cancelled during work (sleep 5) → aborted quickly, cancelled", async () => {
    const rid = ulid(), req = reqFile(rid, "blackbox", "echo", "sleep", { seconds: 5 }, { timeout: 1 });
    await put(path.join(inbox("echo"), `${rid}.json`), req);
    await waitFor(async () => (await readJ(path.join(ROOT, "echo", "status.json"))).status === "busy", 3000, "busy");
    await sleep(1000); // the requestor's own timeout (1 s) fired: B4
    const t = Date.now();
    await put(path.join(inbox("echo"), `${rid}.can.json`), canFile(req));
    const res = await captureRes("echo", rid, 3000);
    eq([res.response.cancellation, res.response.error, res.response.result], [true, false, null], "cancellation response");
    if (Date.now() - t > 1500) throw new Error("the handler was not aborted promptly");
    await put(path.join(inbox("echo"), `${rid}.con.json`), conFile(req));
    await emptyInbox("echo"); eq((await inreqOf("echo", rid)).event_data.final_status, "cancelled", "inreq");
  });
  await scenario("E7 cancelled after response → single response (cancellation=false), cancelled", async () => {
    const { rid, req, res } = await exchange("echo", "echo", { text: "x" }, { confirm: false });
    await put(path.join(inbox("echo"), `${rid}.can.json`), canFile(req));
    await sleep(400);
    const again = await readJ(path.join(inbox("echo"), `${rid}.res.json`));
    eq(again, res, "the response is not rewritten");
    eq(res.response.cancellation, false, "cancellation flag");
    await put(path.join(inbox("echo"), `${rid}.con.json`), conFile(req));
    await emptyInbox("echo"); eq((await inreqOf("echo", rid)).event_data.final_status, "cancelled", "inreq");
  });
  await scenario("E10 ping and custom noop → completed, ping re-announces", async () => {
    await sleep(1100); // second-resolution datetimes
    const specBefore = await readJ(path.join(ROOT, "echo", "spec.json"));
    const annBefore = await fsp.stat(path.join(ROOT, "nuntium", "echo.n.json"));
    const p = ulid(), n = ulid();
    await put(path.join(inbox("echo"), `${p}.json`), reqFile(p, "blackbox", "echo", "ping", null, { type: "cmd" }));
    await put(path.join(inbox("echo"), `${n}.json`), reqFile(n, "blackbox", "echo", "noop", { a: 1 }, { type: "cmd" }));
    await emptyInbox("echo");
    await waitFor(async () => (await fsp.stat(path.join(ROOT, "nuntium", "echo.n.json"))).mtimeMs > annBefore.mtimeMs, 3000, "ping re-announce");
    const specAfter = await readJ(path.join(ROOT, "echo", "spec.json"));
    if (specAfter.datetime <= specBefore.datetime) throw new Error("ping did not refresh spec.datetime");
    eq([(await inreqOf("echo", p)).event_data.final_status, (await inreqOf("echo", n)).event_data.final_status], ["completed", "completed"], "inreq");
    await validatePublication("echo");
  });
  await scenario("E11 unknown command + unlisted src → dropped, nothing else changes", async () => {
    await sleep(300);
    const spec = await fsp.readFile(path.join(ROOT, "echo", "spec.json"), "utf8");
    const ann = await fsp.readFile(path.join(ROOT, "nuntium", "echo.n.json"), "utf8");
    const statusEvents = (await diary("echo")).filter((l) => l.event === "status").length;
    const u = ulid(), s = ulid();
    await put(path.join(inbox("echo"), `${u}.json`), reqFile(u, "blackbox", "echo", "bogus", null, { type: "cmd" }));
    await put(path.join(inbox("echo"), `${s}.json`), reqFile(s, "stranger", "echo", "noop", null, { type: "cmd" }));
    await emptyInbox("echo");
    eq([(await inreqOf("echo", u)).event_data.final_status, (await inreqOf("echo", s)).event_data.final_status], ["dropped", "dropped"], "inreq");
    eq(await fsp.readFile(path.join(ROOT, "echo", "spec.json"), "utf8"), spec, "spec unchanged");
    eq(await fsp.readFile(path.join(ROOT, "nuntium", "echo.n.json"), "utf8"), ann, "announce unchanged");
    eq((await diary("echo")).filter((l) => l.event === "status").length, statusEvents, "no status event");
    eq((await readJ(path.join(ROOT, "echo", "status.json"))).status, "idle", "status");
  });
  await scenario("E14 malformed: bad JSON, schema violation, encrypted body → ignored, unanswered, logged", async () => {
    const r1 = ulid(), r2 = ulid(), r3 = ulid();
    await put(path.join(inbox("echo"), `${r1}.json`), "{ not json");
    const bad = reqFile(r2, "blackbox", "echo", "echo", { text: "x" }); delete bad.request.nonce;
    await put(path.join(inbox("echo"), `${r2}.json`), bad);
    const enc = { ...header(r3, "blackbox", "echo"), request: Buffer.from("secret").toString("base64") };
    check("request", enc, "encrypted request (schema-valid)");
    await put(path.join(inbox("echo"), `${r3}.json`), enc);
    await sleep(1500);
    const names = (await fsp.readdir(inbox("echo"))).sort();
    eq(names, [`${r1}.json`, `${r2}.json`, `${r3}.json`].sort(), "the three files stay, no .res");
    const logs = (await diary("echo")).filter((l) => l.event === "log").map((l) => String(l.event_data));
    for (const r of [r1, r2, r3]) if (!logs.some((m) => m.includes(r))) throw new Error(`no log event for ${r}`);
    for (const r of [r1, r2, r3]) await fsp.rm(path.join(inbox("echo"), `${r}.json`));
    return `log lines: ${logs.filter((m) => [r1, r2, r3].some((r) => m.includes(r))).length}`;
  });
  await scenario("produced files of the run validate (spec/status/announce/diarium)", async () => {
    await validatePublication("echo"); return `${(await diary("echo")).length} diarium lines valid`;
  });

  await scenario("E12 stop with a request in flight → 300, archived before C1, inactive, exit 0", async () => {
    const rid = ulid(), req = reqFile(rid, "blackbox", "echo", "sleep", { seconds: 60 });
    await put(path.join(inbox("echo"), `${rid}.json`), req);
    await waitFor(async () => (await readJ(path.join(ROOT, "echo", "status.json"))).status === "busy", 3000, "busy");
    const s = ulid();
    await put(path.join(inbox("echo"), `${s}.json`), reqFile(s, "blackbox", "echo", "stop", { force: true }, { type: "cmd" }));
    const code = await Promise.race([echo.exited, sleep(15000).then(() => "alive")]);
    eq(code, 0, `exit code (log: ${echo.log.join("").slice(-300)})`);
    const lines = await diary("echo");
    const iReq = lines.findIndex((l) => l.event === "inreq" && l.event_data.req?.rid === rid);
    const iStop = lines.findIndex((l) => l.event === "inreq" && l.event_data.req?.rid === s);
    const iInactive = lines.findIndex((l) => l.event === "status" && l.event_data === "inactive");
    if (!(iReq >= 0 && iStop >= 0 && iInactive > iReq && iInactive > iStop)) throw new Error(`order req=${iReq} stop=${iStop} inactive=${iInactive}`);
    eq(lines[iReq].event_data.res.response.result.code, 300, "300 response archived");
    eq((await readJ(path.join(ROOT, "echo", "status.json"))).status, "inactive", "status.json");
    eq((await readJ(path.join(ROOT, "nuntium", "echo.n.json"))).nuntium.includes("inactive"), true, "announce inactive");
    await validatePublication("echo");
    if ((await fsp.readdir(inbox("echo"))).length !== 0) throw new Error("inbox not empty");
  });
  await scenario("E9 receiver inactive → the request stays unanswered in the inbox", async () => {
    const rid = ulid();
    await put(path.join(inbox("echo"), `${rid}.json`), reqFile(rid, "blackbox", "echo", "echo", { text: "x" }));
    await sleep(1500);
    eq(await fsp.readdir(inbox("echo")), [`${rid}.json`], "file stays, no response");
    eq((await readJ(path.join(ROOT, "echo", "status.json"))).status, "inactive", "status");
  });
  await scenario("E17 pending request at start is processed (D-4) — the E9 file", async () => {
    const c = await upAgent("echo", { ACTIO_COMMANDERS: "blackbox" });
    const [name] = (await fsp.readdir(inbox("echo"))).filter((n) => !n.includes(".res") && !n.includes(".con"));
    const rid = name.replace(".json", "");
    const res = await captureRes("echo", rid);
    eq(res.response.result, { text: "x" }, "answered after the restart of the process");
    const req = await readJ(path.join(inbox("echo"), `${rid}.json`));
    await put(path.join(inbox("echo"), `${rid}.con.json`), conFile(req));
    await emptyInbox("echo");
    globalThis.echo2 = c;
  });
  await scenario("E13 restart with delay 1 → archive, inactive, active again after ≥ delay", async () => {
    const r = ulid();
    await put(path.join(inbox("echo"), `${r}.json`), reqFile(r, "blackbox", "echo", "restart", { force: false, delay: 1 }, { type: "cmd" }));
    await waitFor(async () => (await readJ(path.join(ROOT, "echo", "status.json"))).status === "inactive", 5000, "inactive");
    const tInactive = Date.now();
    await waitFor(async () => (await readJ(path.join(ROOT, "echo", "status.json"))).status === "idle", 8000, "idle again");
    const gap = Date.now() - tInactive;
    const lines = await diary("echo");
    const iArch = lines.findIndex((l) => l.event === "inreq" && l.event_data.req?.rid === r);
    const iIn = lines.findLastIndex((l) => l.event === "status" && l.event_data === "inactive");
    if (!(iArch >= 0 && iArch < iIn)) throw new Error(`restart archived before C1? arch=${iArch} inactive=${iIn}`);
    if (gap < 900) throw new Error(`active again only ${gap} ms after inactive`);
    eq((await readJ(path.join(ROOT, "nuntium", "echo.n.json"))).nuntium.includes("active"), true, "active announce");
    const { res } = await exchange("echo", "echo", { text: "after" }); eq(res.response.result, { text: "after" }, "works");
    await emptyInbox("echo");
    return `inactive→idle gap ${gap} ms`;
  });
  globalThis.echo2.kill("SIGTERM");
  const sigCode = await Promise.race([globalThis.echo2.exited, sleep(10000).then(() => "alive")]);
  results.push([sigCode === 0 ? "PASS" : "FAIL", "SIGTERM → graceful lane C, exit 0", `exit ${sigCode}`, 0]);

  await scenario("E3 106 namespace_mismatch (receiver red, discovered requestor blue)", async () => {
    // a hand-written, schema-valid publication for the requestor "blackbox" (namespace blue)
    await fsp.mkdir(path.join(ROOT, "blackbox", "inbox"), { recursive: true });
    const spec = { id: "blackbox", actio_version: "0.1", datetime: iso(), description: "hand-written requestor", namespaces: ["blue"],
      accepts_commands_from: [], capabilities: [], security_support: [], security_posture: "trusted" };
    const status = { id: "blackbox", status: "idle", datetime: iso(), capability_status: [] };
    check("spec", spec, "blackbox spec"); check("status", status, "blackbox status");
    await put(path.join(ROOT, "blackbox", "spec.json"), spec); await put(path.join(ROOT, "blackbox", "status.json"), status);
    const c = await upAgent("red", { ACTIO_NAMESPACES: "red" }); // its initial scan discovers "blackbox"
    const { res } = await exchange("red", "echo", { text: "x" });
    eq(res.response.result?.code, 106, "106");
    await emptyInbox("red");
    c.kill("SIGTERM"); eq(await c.exited, 0, "red exits 0");
  });

  await fsp.rm(ROOT, { recursive: true, force: true });
  for (const [v, n, note, ms] of results) console.log(`${v}  ${n}${note ? `  — ${note}` : ""}${ms ? `  (${ms} ms)` : ""}`);
  const failed = results.filter((r) => r[0] === "FAIL").length;
  console.log(`\n${results.length - failed}/${results.length} passed`);
  process.exit(failed ? 1 : 0);
}
main().catch((e) => { console.error(e); process.exit(2); });
