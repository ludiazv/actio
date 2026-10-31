// Tests of status/spec/announce publication, health derivation and the heartbeat (WP3-A).
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { writeFile } from "node:fs/promises";
import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { test, type TestContext } from "node:test";
import * as diarium from "../../src/diarium.js";
import * as io from "../../src/io.js";
import { announce, buildStatus, health, publishSpec, publishStatus, startHeartbeat } from "../../src/status.js";
import { sleep, toIso } from "../../src/util.js";
import { validateAnnounce, validateDiarium, validateSpec, validateStatus } from "../../src/validate.js";
import { fakeClock, makeRoot, waitFor } from "../helpers/index.js";
import type { Announce, Capability, Spec, Status } from "../../src/generated/types.js";

const ID = "alpha";
const T0 = 1_700_000_000_000; // 2023-11-14T22:13:20Z
const ISO = "2023-11-14T22:13:20Z";
const projectRoot = path.resolve(fileURLToPath(import.meta.url), "../../.."); // for running a child with tsx
const execFileAsync = promisify(execFile);

const capability: Capability = {
  name: "translate",
  description: "Translates text",
  parameters: { type: "object" },
  actio: { result: true },
};

function specOf(over: Partial<Spec> = {}): Spec {
  return {
    id: ID,
    actio_version: "0.1",
    datetime: ISO,
    description: "A translation agent",
    accepts_commands_from: ["ops"],
    capabilities: [capability],
    security_support: [],
    security_posture: "trusted",
    ...over,
  };
}

const statusOf = (state: Status["status"], datetime: string): Status => ({
  id: ID,
  status: state,
  datetime,
  capability_status: [],
});

/** Error texts of a failed validation, for assertion messages. */
function errorText(r: { ok: boolean; errors?: string[] }): string {
  return r.ok ? "(accepted)" : (r.errors ?? []).join(" | ");
}

/** The JSON objects of a diarium file, one per line. */
async function readLines(file: string): Promise<unknown[]> {
  return (await readFile(file, "utf8"))
    .split("\n")
    .filter((l) => l !== "")
    .map((l) => JSON.parse(l));
}

const mtimeMs = async (file: string): Promise<number> => (await stat(file)).mtimeMs;

// --- buildStatus ---

test("buildStatus produces a status that passes validateStatus", () => {
  for (const state of ["inactive", "idle", "busy", "error"] as const) {
    const status = buildStatus(ID, state, T0);
    assert.ok(validateStatus(status).ok, errorText(validateStatus(status)));
    assert.deepEqual(status, { id: ID, status: state, datetime: ISO, capability_status: [] });
  }
});

test("buildStatus stamps the second-resolution datetime from nowMs", () => {
  assert.equal(buildStatus(ID, "idle", 1_700_000_000_999).datetime, ISO);
  assert.equal(buildStatus(ID, "idle", 1_700_000_001_000).datetime, "2023-11-14T22:13:21Z");
});

test("buildStatus keeps the capability_status entries given, and defaults to []", () => {
  const entries = [
    { name: "translate", running: 2, queue: 1 },
    { name: "summarize", running: 0, queue: 0 },
  ];
  assert.deepEqual(buildStatus(ID, "busy", T0, entries).capability_status, entries);
  assert.deepEqual(buildStatus(ID, "busy", T0, []).capability_status, []);
  assert.deepEqual(buildStatus(ID, "busy", T0, undefined).capability_status, []);
  assert.ok(validateStatus(buildStatus(ID, "busy", T0, entries)).ok);
});

// --- publishSpec / publishStatus ---

test("publishSpec writes spec.json atomically and it passes validateSpec", async (t) => {
  const root = await makeRoot(t);
  const spec = specOf();
  await publishSpec(root, spec);
  assert.deepEqual(await io.readJson(path.join(root, "spec.json")), spec);
  const written = JSON.parse(await readFile(path.join(root, "spec.json"), "utf8"));
  assert.equal(await readFile(path.join(root, "spec.json"), "utf8"), JSON.stringify(spec)); // no trailing whitespace
  assert.ok(validateSpec(written).ok, errorText(validateSpec(written)));
  assert.deepEqual(await io.listDir(root), ["spec.json"]); // the temp file was renamed away
});

test("publishStatus writes status.json atomically and rewrites it in place", async (t) => {
  const root = await makeRoot(t);
  await publishStatus(root, buildStatus(ID, "idle", T0));
  assert.deepEqual(await io.readJson(path.join(root, "status.json")), buildStatus(ID, "idle", T0));
  await publishStatus(root, buildStatus(ID, "busy", T0 + 1000)); // the heartbeat or a change: same file
  assert.deepEqual(await io.listDir(root), ["status.json"]); // one file, no temp leftovers
  const onDisk = (await io.readJson(path.join(root, "status.json"))) as Status;
  assert.deepEqual(onDisk, buildStatus(ID, "busy", T0 + 1000));
  assert.ok(validateStatus(onDisk).ok, errorText(validateStatus(onDisk)));
});

test("publishSpec and publishStatus throw on an invalid object and write nothing", async (t) => {
  const root = await makeRoot(t);
  const badSpec = { ...specOf(), actio_version: "0" } as unknown as Spec; // actio_version pattern
  const badState = { id: ID, status: "sleeping", datetime: ISO, capability_status: [] } as unknown as Status; // enum
  const badEntries = {
    id: ID,
    status: "idle",
    datetime: ISO,
    capability_status: [{ name: "translate", running: 1 }],
  } as unknown as Status;
  await assert.rejects(() => publishSpec(root, badSpec), /status\.ts: invalid spec\.json/);
  await assert.rejects(() => publishStatus(root, badState), /status\.ts: invalid status\.json/);
  await assert.rejects(() => publishStatus(root, badEntries), /required property 'queue'/);
  assert.deepEqual(await io.listDir(root), []); // not even a temp file
});

test("publishStatus creates the agent folder when it is missing (tmpDir is the folder)", async (t) => {
  const root = path.join(await makeRoot(t), ID);
  await publishStatus(root, buildStatus(ID, "idle", T0));
  assert.ok(validateStatus((await io.readJson(path.join(root, "status.json"))) as Status).ok);
});

// --- announce (I-5: the agent folder and the shared nuntium folder are different folders) ---

/** A temp root standing for `.actio`: agent folders live directly at the root, `nuntium/` is shared. */
async function makeLayout(t: TestContext): Promise<{ root: string; agent: string; nuntium: string }> {
  const root = await makeRoot(t);
  const agent = path.join(root, ID);
  await io.makeDir(agent);
  return { root, agent, nuntium: path.join(root, "nuntium") };
}

const announceFile = (nuntium: string): string => path.join(nuntium, `${ID}.n.json`);

test("announce writes <nuntium>/<id>.n.json with the four fields and validates it", async (t) => {
  const { agent, nuntium } = await makeLayout(t);
  await announce(agent, nuntium, ID, "agent is active", T0);
  const file = (await io.readJson(announceFile(nuntium))) as Announce;
  assert.deepEqual(file, { id: ID, datetime: ISO, ts: 1_700_000_000, nuntium: "agent is active" });
  assert.ok(validateAnnounce(file).ok, errorText(validateAnnounce(file)));
  assert.equal(file.ts, Math.floor(T0 / 1000)); // ts is Unix epoch seconds
  assert.deepEqual(await readLines(path.join(agent, "diarium.jsonl")), [diarium.announceEvent(ID, T0)]);
});

test("announce creates the nuntium folder and leaves no temp file in the agent folder", async (t) => {
  const { agent, nuntium } = await makeLayout(t);
  assert.equal(await io.exists(nuntium), false); // announce creates it
  await announce(agent, nuntium, ID, "first", T0);
  assert.deepEqual(await io.listDir(nuntium), ["alpha.n.json"]); // the only content of the shared folder
  assert.deepEqual(await io.listDir(agent), ["diarium.jsonl"]); // tmpDir = the agent folder (§2.5), no leftovers
});

test("announce writes the signal to the nuntium folder only, never into the agent folder", async (t) => {
  const { agent, nuntium } = await makeLayout(t);
  await publishStatus(agent, buildStatus(ID, "idle", T0));
  await announce(agent, nuntium, ID, "first", T0);
  assert.equal(await io.exists(announceFile(nuntium)), true); // where the peers' nuntium watcher looks
  assert.equal(await io.exists(path.join(agent, `${ID}.n.json`)), false); // not in the agent folder
  assert.equal(await io.exists(path.join(agent, "nuntium")), false); // no per-agent nuntium folder
  assert.equal(await io.exists(path.join(agent, "nuntium", `${ID}.n.json`)), false);
  assert.equal(await io.exists(path.join(agent, "diarium.jsonl")), true); // the event went to the agent folder
  assert.deepEqual(await io.listDir(agent), ["diarium.jsonl", "status.json"]); // nothing else at all
});

test("announce appends the diarium announce event after the announce file", async (t) => {
  const { agent, nuntium } = await makeLayout(t);
  await announce(agent, nuntium, ID, "agent is active", T0);
  await announce(agent, nuntium, ID, "agent is inactive", T0 + 5000);
  const lines = await readLines(path.join(agent, "diarium.jsonl")); // the event is per agent
  assert.deepEqual(lines, [diarium.announceEvent(ID, T0), diarium.announceEvent(ID, T0 + 5000)]);
  for (const line of lines) assert.ok(validateDiarium(line).ok, errorText(validateDiarium(line)));
  const announceMtime = await mtimeMs(announceFile(nuntium));
  const diariumMtime = await mtimeMs(path.join(agent, "diarium.jsonl"));
  assert.ok(announceMtime <= diariumMtime, "the event records the completed announce, never before it");
});

test("announce rewrites the same announce file on every announcement", async (t) => {
  const { agent, nuntium } = await makeLayout(t);
  await announce(agent, nuntium, ID, "agent is active", T0);
  await announce(agent, nuntium, ID, "spec updated", T0 + 60_000);
  assert.deepEqual(await io.listDir(nuntium), ["alpha.n.json"]); // one file per agent, rewritten
  assert.deepEqual(await io.readJson(announceFile(nuntium)), {
    id: ID,
    datetime: "2023-11-14T22:14:20Z",
    ts: 1_700_000_060,
    nuntium: "spec updated",
  });
  assert.deepEqual(await readLines(path.join(agent, "diarium.jsonl")), [
    diarium.announceEvent(ID, T0),
    diarium.announceEvent(ID, T0 + 60_000),
  ]);
});

test("announce throws on an invalid announce document and writes nothing", async (t) => {
  const { agent, nuntium } = await makeLayout(t);
  const invalidThrown = /status\.ts: invalid announce file/; // self-validation before any write (I-2)
  await assert.rejects(() => announce(agent, nuntium, "bad id", "x", T0), invalidThrown); // agentId pattern
  await assert.rejects(() => announce(agent, nuntium, ID, "x", -5000), invalidThrown); // negative ts
  assert.equal(await io.exists(nuntium), false); // the nuntium folder was not even created
  assert.deepEqual(await io.listDir(agent), []); // and no diarium line
});

test("rule 2 ordering: status.json is written before the announce file", async (t) => {
  const { agent, nuntium } = await makeLayout(t);
  const clock = fakeClock(T0);
  await publishStatus(agent, buildStatus(ID, "idle", clock())); // status first, in the agent folder
  await sleep(50); // announceDelay, applied by the caller
  await announce(agent, nuntium, ID, "agent is active", clock()); // then the announce file, in nuntium
  const statusMtime = await mtimeMs(path.join(agent, "status.json"));
  const announceMtime = await mtimeMs(announceFile(nuntium));
  assert.ok(statusMtime < announceMtime, `status ${statusMtime} must precede announce ${announceMtime}`);
  assert.ok(validateStatus((await io.readJson(path.join(agent, "status.json"))) as Status).ok);
  assert.ok(validateAnnounce((await io.readJson(announceFile(nuntium))) as Announce).ok);
});

// --- health (*Agent's status* rules 4–8) ---

test("health follows the 60/100/600 s thresholds at their boundaries", () => {
  const now = T0 + 10 * 60_000; // a fixed reader clock; the age is now − datetime
  // A protocol `datetime` has second resolution, so the ages a reader can compute are whole seconds.
  const cases: [number, "healthy" | "online" | "degraded" | "zombie"][] = [
    [0, "healthy"], // just written
    [1_000, "healthy"],
    [59_000, "healthy"], // rule 4: updated less than 60 s ago
    [60_000, "online"], // rule 4 needs LESS than 60 s, so rule 5 (60–100 s) starts at exactly 60 s
    [90_000, "online"], // rule 3: a ≤ 90 s heartbeat is only ever healthy or online
    [99_000, "online"],
    [100_000, "degraded"], // rule 6: 100–600 s
    [300_000, "degraded"],
    [599_000, "degraded"],
    [600_000, "degraded"], // rule 7 needs MORE than 600 s
    [601_000, "zombie"],
    [700_000, "zombie"],
    [3_600_000, "zombie"],
  ];
  for (const [ageMs, expected] of cases) {
    const datetime = toIso(now - ageMs);
    assert.equal(health(statusOf("idle", datetime), now), expected, `idle at age ${ageMs} ms`);
    assert.equal(health(statusOf("busy", datetime), now), expected, `busy at age ${ageMs} ms`);
  }
});

test("health thresholds are applied on the reader's millisecond clock", () => {
  const datetime = toIso(T0); // an update exactly at T0
  assert.equal(health(statusOf("idle", datetime), T0 + 59_999), "healthy");
  assert.equal(health(statusOf("idle", datetime), T0 + 60_000), "online"); // "less than 60 s" is not met
  assert.equal(health(statusOf("idle", datetime), T0 + 99_999), "online");
  assert.equal(health(statusOf("idle", datetime), T0 + 100_000), "degraded");
  assert.equal(health(statusOf("idle", datetime), T0 + 600_000), "degraded");
  assert.equal(health(statusOf("idle", datetime), T0 + 600_001), "zombie");
});

test("health walks through the thresholds as the fake clock advances (59/60/100/600/601 s)", () => {
  const clock = fakeClock(T0);
  const status = buildStatus(ID, "idle", clock()); // published at T0
  assert.equal(health(status, clock()), "healthy"); // just written
  clock.advance(59_000);
  assert.equal(health(status, clock()), "healthy", "59 s"); // rule 4
  clock.advance(1_000); // exactly 60 s
  assert.equal(health(status, clock()), "online", "60 s"); // rule 5
  clock.advance(39_000); // 99 s
  assert.equal(health(status, clock()), "online", "99 s");
  clock.advance(1_000); // exactly 100 s
  assert.equal(health(status, clock()), "degraded", "100 s"); // rule 6
  clock.advance(500_000); // 600 s
  assert.equal(health(status, clock()), "degraded", "600 s");
  clock.advance(1_000); // 601 s
  assert.equal(health(status, clock()), "zombie", "601 s"); // rule 7
});

test("health is undefined for inactive and error at any age (rule 8)", () => {
  const now = T0 + 10 * 60_000;
  for (const state of ["inactive", "error"] as const) {
    for (const ageMs of [0, 59_000, 100_000, 700_000]) {
      assert.equal(health(statusOf(state, toIso(now - ageMs)), now), undefined, `${state} at age ${ageMs}`);
    }
  }
});

test("health is undefined when the datetime is not in the protocol format", () => {
  for (const datetime of ["2023-11-14T22:13:20.000Z", "2023-11-14 22:13:20", "yesterday", "", "2023-13-14T22:13:20Z"]) {
    assert.equal(health(statusOf("idle", datetime), T0), undefined, datetime);
  }
});

test("health of a status with a datetime in the future is healthy (reader clock skew)", () => {
  assert.equal(health(statusOf("busy", toIso(T0 + 600_000)), T0), "healthy");
});

// --- heartbeat ---

test("the heartbeat rewrites status.json with a fresh datetime and stays healthy", async (t) => {
  const root = await makeRoot(t);
  const clock = fakeClock(T0);
  const published: string[] = []; // the datetime of every tick whose write completed
  const beat = startHeartbeat(200, async () => {
    clock.advance(1000); // the fake clock only moves when the test moves it
    const status = buildStatus(ID, "idle", clock());
    await publishStatus(root, status);
    published.push(status.datetime);
  });
  t.after(() => beat.stop());

  await waitFor(() => published.length >= 4, 3000); // 4 × 200 ms, well inside 3 s
  beat.stop();
  await sleep(60); // let a tick that was already in flight finish its write
  assert.deepEqual(published, [...new Set(published)]); // every tick rewrote with a fresh datetime
  const onDisk = JSON.parse(await readFile(path.join(root, "status.json"), "utf8")) as Status;
  assert.equal(onDisk.datetime, published[published.length - 1]); // the last completed write is on disk
  assert.ok(validateStatus(onDisk).ok, errorText(validateStatus(onDisk)));
  assert.equal(health(onDisk, clock()), "healthy"); // plan WP3: never older than the 90 s heartbeat bound
});

test("the heartbeat advances status.json' mtime and stop() halts it", async (t) => {
  const root = await makeRoot(t);
  const clock = fakeClock(T0);
  let ticks = 0;
  let completed = 0;
  const beat = startHeartbeat(100, async () => {
    ticks++;
    clock.advance(100);
    await publishStatus(root, buildStatus(ID, "idle", clock()));
    completed++; // the write of this tick is on disk
  });
  await waitFor(() => completed >= 3, 2000); // no write in flight once this holds
  beat.stop();
  const stoppedAt = ticks;
  const stoppedMtime = await mtimeMs(path.join(root, "status.json"));
  await sleep(400); // more than three intervals
  assert.equal(ticks, stoppedAt, "no tick may run after stop()");
  assert.equal(await mtimeMs(path.join(root, "status.json")), stoppedMtime, "status.json untouched after stop()");
  beat.stop(); // idempotent
});

test("the heartbeat awaits a slow tick before scheduling the next (no overlap)", async (t) => {
  const root = await makeRoot(t);
  let running = 0;
  let overlaps = 0;
  let completed = 0;
  const beat = startHeartbeat(20, async () => {
    running++;
    if (running > 1) overlaps++; // a scheduled tick must never start while another runs
    await sleep(80); // a tick slower than the interval
    await publishStatus(root, buildStatus(ID, "busy", T0 + completed * 1000));
    running--;
    completed++;
  });
  t.after(() => beat.stop());
  await waitFor(() => completed >= 3, 3000);
  assert.equal(overlaps, 0, `ticks overlapped ${overlaps} times`);
  assert.equal(running, 0);
  assert.ok(validateStatus((await io.readJson(path.join(root, "status.json"))) as Status).ok);
});

test("stop() from inside a tick in flight schedules no further tick", async () => {
  let ticks = 0;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => (release = resolve));
  const beat = startHeartbeat(20, async () => {
    ticks++;
    if (ticks === 1) beat.stop(); // stopped from inside the first (still running) tick
    await gate;
  });
  try {
    await sleep(60); // the first tick is in flight
    release();
    await sleep(120); // several intervals
    assert.equal(ticks, 1);
  } finally {
    beat.stop();
  }
});

// A rejecting tick must not be swallowed: `startHeartbeat` has no error channel (the frozen stub has
// only `stop()`), so the rejection surfaces to the process. It is observed in a child process, where
// the crashing rejection cannot be confused with a failure of this test file.
test("a rejecting tick surfaces to the process instead of being swallowed", async (t) => {
  const script = path.join(await makeRoot(t), "rejecting-tick.ts");
  const statusModule = path.resolve(fileURLToPath(import.meta.url), "../../../src/status.js");
  await writeFile(
    script,
    `import { startHeartbeat } from ${JSON.stringify(statusModule)};
let ticks = 0;
process.on("unhandledRejection", (err) => {
  console.log("SEEN", String(err));
  setTimeout(() => console.log("TICKS", ticks), 80); // several intervals later
});
startHeartbeat(10, async () => {
  ticks++;
  throw new Error("tick failed");
});
`,
  );
  const nodeArgs = ["--import", "tsx", script];
  const { stdout } = await execFileAsync(process.execPath, nodeArgs, { cwd: projectRoot, timeout: 15_000 });
  assert.match(stdout, /SEEN Error: tick failed/); // the caller (WP7) can install its own error policy
  assert.match(stdout, /TICKS 1/); // the chain stops: no further tick after a rejection
});

test("the first tick comes after one interval, not immediately (the caller publishes first)", async () => {
  let ticks = 0;
  const beat = startHeartbeat(1000, () => {
    ticks++;
  });
  try {
    await sleep(150);
    assert.equal(ticks, 0);
  } finally {
    beat.stop();
  }
});
