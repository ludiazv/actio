// Tests of the `actio` CLI: the six subcommands and their output, the exit codes, the
// temporary lane-B vehicle of `call`/`cmd`, and the direct-run entry point of `tsx src/cli.ts`.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test, type TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import { ActioAgent } from "../../src/agent.js";
import { main } from "../../src/cli.js";
import * as io from "../../src/io.js";
import { toIso } from "../../src/util.js";
import { validateAnnounce } from "../../src/validate.js";
import { fastConfig, waitFor } from "../helpers/index.js";
import type { CapabilityHandler, CommandHandler } from "../../src/types.js";
import type { Capability, CapabilityActio, DiariumLine, Spec, Status } from "../../src/generated/types.js";

const projectRoot = path.resolve(fileURLToPath(import.meta.url), "../../.."); // to run the CLI as a child
const ISO = "2023-11-14T22:13:20Z";
const RID_LINE = /^rid: [0-9A-HJKMNP-TV-Z]{26}$/; // the one line `cmd` prints

/** A stuck exchange fails its test instead of hanging the file. */
const opts = { timeout: 20_000 };

/** A test root plus the agents started in it; the agents stop before the root disappears. */
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

interface Run {
  code: number;
  out: string; // stdout, lines joined
  err: string; // stderr, lines joined
}

/** Runs the CLI in-process: its root and sync provider come from the environment, like any CLI. */
async function cli(root: string, argv: string[], env: Record<string, string> = {}): Promise<Run> {
  const names = ["ACTIO_DIR", "ACTIO_SYNC", "ACTIO_CONFIRM_TIMEOUT", "ACTIO_CANCEL_WAIT"];
  const saved: Record<string, string | undefined> = {};
  for (const n of names) saved[n] = process.env[n];
  process.env["ACTIO_DIR"] = root;
  process.env["ACTIO_SYNC"] = "none";
  for (const [k, v] of Object.entries(env)) process.env[k] = v;
  const out: string[] = [];
  const err: string[] = [];
  try {
    const code = await main(argv, { out: (l) => out.push(l), err: (l) => err.push(l) });
    return { code, out: out.join("\n"), err: err.join("\n") };
  } finally {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
}

function cap(name: string, actio: Partial<CapabilityActio> = {}): Capability {
  return { name, description: `does ${name}`, parameters: { type: "object" }, actio: { result: true, ...actio } };
}

function specOf(id: string, over: Partial<Spec> = {}): Spec {
  return {
    id,
    actio_version: "0.1",
    datetime: ISO,
    description: "a test agent",
    accepts_commands_from: ["alpha"],
    capabilities: [cap("work"), cap("private", { accepts_from: ["someone"] }), cap("off", { disabled: true })],
    security_support: [],
    security_posture: "trusted",
    ...over,
  };
}

const statusOf = (id: string, state: string, datetime = ISO): Status => ({
  id,
  status: state as Status["status"],
  datetime,
  capability_status: [],
});

/** A published agent folder as discovery sees it; the CLI of `ls` only reads it. */
async function publish(root: string, folder: string, spec: Spec, status: Status): Promise<void> {
  await io.makeDir(path.join(root, folder));
  await fsp.writeFile(path.join(root, folder, "spec.json"), JSON.stringify(spec));
  await fsp.writeFile(path.join(root, folder, "status.json"), JSON.stringify(status));
}

async function json<T>(file: string): Promise<T> {
  return JSON.parse(await fsp.readFile(file, "utf8")) as T;
}

async function diaryOf(root: string, id: string): Promise<DiariumLine[]> {
  const raw = await fsp.readFile(path.join(root, id, "diarium.jsonl"), "utf8");
  return raw
    .split("\n")
    .filter((l) => l !== "")
    .map((l) => JSON.parse(l) as DiariumLine);
}

const finalOf = (line: DiariumLine): string => String((line.event_data as { final_status?: string }).final_status);
const reqOf = (line: DiariumLine): { rid: string; type: string; request: { name: string } } =>
  (line.event_data as { req: { rid: string; type: string; request: { name: string } } }).req;

/** The plain envelopes of an inbox (`<rid>.json`): not res/can/con, not a dot file. */
async function envelopes(inbox: string): Promise<string[]> {
  return (await io.listDir(inbox)).filter((n) => n.endsWith(".json") && !n.slice(0, -".json".length).includes("."));
}

/** Waits for the CLI's request envelope in the dst inbox and answers it the way a peer would. */
async function answer(root: string, dst: string, body: Record<string, unknown> = {}): Promise<string> {
  const inbox = path.join(root, dst, "inbox");
  await waitFor(async () => (await envelopes(inbox)).length === 1, 5_000);
  const [name] = await envelopes(inbox);
  const req = await json<Record<string, unknown>>(path.join(inbox, name!));
  const rid = name!.slice(0, -".json".length);
  await io.writeJsonAtomic(
    path.join(inbox, `${rid}.res.json`),
    {
      rid: req["rid"],
      src: req["src"],
      dst: req["dst"],
      type: req["type"],
      actio_version: req["actio_version"],
      datetime: toIso(Date.now()), // the header may differ from the request only in `datetime`
      response: {
        ts: Math.floor(Date.now() / 1000),
        nonce: "0123456789abcdef0123456789abcdef", // the schema wants a 128-bit hex nonce
        signature: null,
        error: false,
        cancellation: false,
        result: { text: "echo: hi" },
        attachments: [],
        ...body,
      },
    },
    root,
  );
  return rid;
}

/** A peer agent in the same tree, serving `echo` and a custom `noop` command. */
async function peerOf(h: Harness, acceptsCommandsFrom: string[]): Promise<ActioAgent> {
  const peer = new ActioAgent({ id: "beta", description: "the peer", acceptsCommandsFrom, config: fastConfig(h.root, Date.now) });
  peer.capability(cap("echo"), async (params) => ({ text: `echo: ${(params as { text: string }).text}` }));
  h.agents.push(peer);
  await peer.start();
  return peer;
}

// --- ls ---

test("ls: an agent in the tree, with its status, health, and the capability hints", async (t) => {
  const h = await harness(t);
  await publish(h.root, "beta", specOf("beta"), statusOf("beta", "idle", toIso(Date.now() - 150_000))); // 150 s: degraded
  const r = await cli(h.root, ["ls"]);
  assert.equal(r.code, 0);
  const [id, state, health, , folder] = r.out.split("\n")[0]!.split("  ");
  assert.deepEqual([id, state, health, folder], ["beta", "idle", "degraded", path.join(h.root, "beta")]);
  assert.match(r.out, /^ {4}work$/m);
  assert.match(r.out, /^ {4}private \[accepts_from: someone\]$/m);
  assert.match(r.out, /^ {4}off \[disabled\]$/m);
});

test("ls: health comes from status.ts — fresh is healthy, an old age is zombie", async (t) => {
  const h = await harness(t);
  await publish(h.root, "beta", specOf("beta"), statusOf("beta", "idle", toIso(Date.now())));
  await publish(h.root, "gamma", specOf("gamma"), statusOf("gamma", "busy", ISO)); // 2023: far too old
  const r = await cli(h.root, ["ls"]);
  assert.match(r.out, /^beta {2}idle {2}healthy {2}/m);
  assert.match(r.out, /^gamma {2}busy {2}zombie {2}/m);
});

test("ls --json: the agents plus the duplicates and ignores of the scan report", async (t) => {
  const h = await harness(t);
  await publish(h.root, "a-beta", specOf("beta"), statusOf("beta", "idle", toIso(Date.now()))); // one spec ID, two folders
  await publish(h.root, "z-beta", specOf("beta"), statusOf("beta", "busy"));
  await io.makeDir(path.join(h.root, "gamma"));
  await fsp.writeFile(path.join(h.root, "gamma", "spec.json"), "{ not json");
  await fsp.writeFile(path.join(h.root, "gamma", "status.json"), JSON.stringify(statusOf("gamma", "idle")));
  const r = await cli(h.root, ["ls", "--json"]);
  assert.equal(r.code, 0);
  const report = JSON.parse(r.out) as {
    root: string;
    counts: { agents: number; duplicates: number; ignored: number };
    agents: { id: string; folder: string; status: string; health?: string; capabilities: { name: string; disabled: boolean; accepts_from: string[] }[] }[];
    duplicates: string[];
    ignored: { path: string; reason: string }[];
  };
  assert.equal(report.root, h.root);
  assert.deepEqual(report.counts, { agents: 1, duplicates: 1, ignored: 1 });
  assert.equal(report.agents[0]!.folder, path.join(h.root, "a-beta")); // D-3: the lexical first keeps the ID
  assert.equal(report.duplicates[0], path.join(h.root, "z-beta"));
  assert.equal(report.agents[0]!.health, "healthy");
  assert.match(report.ignored[0]!.reason, /not valid JSON/);
  assert.deepEqual(report.agents[0]!.capabilities.map((c) => [c.name, c.disabled, c.accepts_from]), [
    ["work", false, []],
    ["private", false, ["someone"]],
    ["off", true, []],
  ]);
});

test("ls: an empty tree says so; a missing root is an error, not an empty view", async (t) => {
  const h = await harness(t);
  const empty = await cli(h.root, ["ls"]);
  assert.equal(empty.code, 0);
  assert.match(empty.out, /no agents under/);
  const missing = await cli(path.join(h.root, "no-such-root"), ["ls"]);
  assert.equal(missing.code, 1);
  assert.match(missing.err, /ENOENT/);
});

// --- diarium ---

test("diarium: the last N lines, 20 by default, one JSON per line, indented with --json", async (t) => {
  const h = await harness(t);
  await io.makeDir(path.join(h.root, "beta"));
  const lines = Array.from({ length: 25 }, (_, i) => JSON.stringify({ id: "beta", datetime: ISO, event: "log", event_data: `line ${i + 1}` }));
  await fsp.writeFile(path.join(h.root, "beta", "diarium.jsonl"), `${lines.join("\n")}\n`);
  const three = await cli(h.root, ["diarium", "beta", "-n", "3"]);
  assert.equal(three.code, 0);
  assert.deepEqual(three.out.split("\n"), lines.slice(-3));
  const all = await cli(h.root, ["diarium", "beta"]);
  assert.deepEqual(all.out.split("\n"), lines.slice(-20)); // the default is the last 20
  const pretty = await cli(h.root, ["diarium", "beta", "--json", "-n", "1"]);
  assert.match(pretty.out, /^\{\n {2}"id": "beta",\n.*\n\}$/s);
  assert.equal(pretty.out.split("\n").length, 6); // one object, indented
  assert.match(pretty.out, /"event_data": "line 25"/);
  const noFile = await cli(h.root, ["diarium", "nobody"]);
  assert.equal(noFile.code, 1);
  assert.match(noFile.err, /cannot read/);
  for (const argv of [["diarium"], ["diarium", "beta", "-n"], ["diarium", "beta", "-n", "0"], ["diarium", "beta", "-n", "-3"]]) {
    assert.equal((await cli(h.root, argv)).code, 1, `${argv.join(" ")} must fail`);
  }
  assert.match((await cli(h.root, ["diarium", "beta", "-n"])).err, /-n needs a value/);
  assert.match((await cli(h.root, ["diarium", "beta", "-n", "0"])).err, /positive integer/);
});

// --- call ---

test("call: a scripted dst inbox answers the CLI, the vehicle confirms (B3)", opts, async (t) => {
  const h = await harness(t);
  await io.makeDir(path.join(h.root, "beta", "inbox"));
  const run = cli(h.root, ["call", "alpha", "beta", "echo", '{"text":"hi"}']);
  const rid = await answer(h.root, "beta", { result: { text: "echo: hi" } });
  const r = await run;
  assert.equal(r.code, 0, r.err);
  assert.match(r.out, /finalStatus: confirmed/);
  assert.match(r.out, /result: \{"text":"echo: hi"\}/);
  assert.ok(await io.exists(path.join(h.root, "beta", "inbox", `${rid}.con.json`)), "the vehicle wrote the confirmation");
  assert.deepEqual((await io.listDir(path.join(h.root, "beta", "inbox"))).sort(), [`${rid}.con.json`, `${rid}.json`, `${rid}.res.json`]);
});

test("call: the vehicle is a lane-B vehicle only: folders, one outreq, no spec, status or announce", opts, async (t) => {
  const h = await harness(t);
  await io.makeDir(path.join(h.root, "beta", "inbox"));
  const run = cli(h.root, ["call", "alpha", "beta", "echo", '{"text":"hi"}']);
  await answer(h.root, "beta");
  assert.equal((await run).code, 0);
  assert.ok(await io.exists(path.join(h.root, "alpha", "inbox")), "the vehicle created its folder and inbox");
  for (const absent of ["spec.json", "status.json"]) {
    assert.equal(await io.exists(path.join(h.root, "alpha", absent)), false, `the vehicle never publishes ${absent}`);
  }
  assert.equal(await io.exists(path.join(h.root, "nuntium", "alpha.n.json")), false, "and it never announces");
  assert.deepEqual((await diaryOf(h.root, "alpha")).map((l) => [l.event, finalOf(l)]), [["outreq", "confirmed"]]);
});

test("call: a started agent answers the CLI, and its own folder stays clean", opts, async (t) => {
  const h = await harness(t);
  const peer = await peerOf(h, ["alpha"]);
  const r = await cli(h.root, ["call", "alpha", "beta", "echo", '{"text":"hi"}']);
  assert.equal(r.code, 0, r.err);
  assert.match(r.out, /finalStatus: confirmed/);
  assert.match(r.out, /result: \{"text":"echo: hi"\}/);
  const inbox = path.join(h.root, "beta", "inbox");
  await waitFor(async () => (await io.listDir(inbox)).length === 0, 6_000); // A4: the peer cleaned up
  await waitFor(async () => (await json<Status>(path.join(h.root, "beta", "status.json"))).status === "idle", 6_000);
  assert.equal(await io.exists(path.join(h.root, "alpha", "spec.json")), false);
  void peer;
});

test("call --json: the outcome comes out as one JSON object", opts, async (t) => {
  const h = await harness(t);
  await io.makeDir(path.join(h.root, "beta", "inbox"));
  const run = cli(h.root, ["call", "--json", "alpha", "beta", "echo", '{"text":"hi"}']);
  await answer(h.root, "beta", { work_time: 3 });
  const r = await run;
  assert.equal(r.code, 0, r.err);
  const outcome = JSON.parse(r.out) as { finalStatus: string; response: { result: { text: string }; work_time: number } };
  assert.equal(outcome.finalStatus, "confirmed");
  assert.equal(outcome.response.result.text, "echo: hi");
  assert.equal(outcome.response.work_time, 3);
});

test("call: an error answer is an outcome, not a CLI failure (exit 0, code and msg printed)", opts, async (t) => {
  const h = await harness(t);
  await io.makeDir(path.join(h.root, "beta", "inbox"));
  const run = cli(h.root, ["call", "alpha", "beta", "echo", "{}"]);
  await answer(h.root, "beta", { error: true, result: { code: 100, msg: "agent_disabled" } });
  const r = await run;
  assert.equal(r.code, 0);
  assert.match(r.out, /finalStatus: confirmed/);
  assert.match(r.out, /error: 100 "agent_disabled"/);
});

test("call: an unanswered request is cancelled then abandoned, and the CLI exits 1", opts, async (t) => {
  const h = await harness(t);
  const inbox = path.join(h.root, "beta", "inbox");
  await io.makeDir(inbox);
  const r = await cli(h.root, ["call", "alpha", "beta", "echo", "{}", "--timeout", "1"], { ACTIO_CANCEL_WAIT: "1" });
  assert.equal(r.code, 1);
  assert.match(r.out, /finalStatus: abandoned/);
  assert.match(r.out, /response: none/);
  assert.ok((await io.listDir(inbox)).some((n) => n.endsWith(".can.json")), "B4 wrote the cancellation");
  assert.deepEqual((await diaryOf(h.root, "alpha")).map(finalOf), ["abandoned"]);
});

/** The CLI always sends a timeout > 0; 0, a negative and a non-number are usage errors. */
test("call --timeout: omitted means 60 in the envelope; 0, -5 and soon are usage errors", opts, async (t) => {
  const h = await harness(t);
  const inbox = path.join(h.root, "beta", "inbox");
  await io.makeDir(inbox);
  const run = cli(h.root, ["call", "alpha", "beta", "echo", '{"text":"hi"}']); // no --timeout at all
  const rid = await answer(h.root, "beta"); // answered at once: the default would otherwise wait 60 s
  const r = await run;
  assert.equal(r.code, 0, r.err);
  const sent = await json<{ request: { timeout: number } }>(path.join(inbox, `${rid}.json`));
  assert.equal(sent.request.timeout, 60, "the envelope carries the CLI default (60), never 0");
  for (const bad of ["0", "-5", "soon"]) {
    const b = await cli(h.root, ["call", "alpha", "beta", "echo", "{}", "--timeout", bad]);
    assert.equal(b.code, 1, `--timeout ${bad} is a usage error`);
    assert.match(b.err, /--timeout must be > 0 seconds/);
    assert.match(b.err, /usage: actio/);
  }
  assert.deepEqual((await diaryOf(h.root, "alpha")).map(finalOf), ["confirmed"], "one exchange only");
});

test("call: a missing dst folder is a local rejection: exit 1, nothing written there", async (t) => {
  const h = await harness(t);
  const r = await cli(h.root, ["call", "alpha", "noghost", "echo", "{}"]);
  assert.equal(r.code, 1);
  assert.match(r.err, /ENOENT/);
  assert.equal(await io.exists(path.join(h.root, "noghost")), false);
});

test("call --json: an unparsable <json-params> is a usage error, not a crash", async (t) => {
  const h = await harness(t);
  for (const argv of [["call", "alpha", "beta"], ["call", "alpha", "beta", "echo", "{not json}"], ["call", "alpha", "beta", "echo", "{}", "--timeout", "soon"]]) {
    const r = await cli(h.root, argv);
    assert.equal(r.code, 1, `${argv.join(" ")} must fail`);
    assert.match(r.err, /usage: actio/);
  }
  assert.match((await cli(h.root, ["call", "alpha", "beta", "echo", "{not json}"])).err, /is not valid JSON/);
});

// --- cmd ---

test("cmd: prints the rid, and the peer agent archives the command it received", opts, async (t) => {
  const h = await harness(t);
  let seen: unknown;
  const noop: CommandHandler = async (params) => {
    seen = params;
  };
  const peer = await peerOf(h, ["alpha"]);
  peer.onCommand("noop", noop);
  const r = await cli(h.root, ["cmd", "alpha", "beta", "noop", '{"a":1}']);
  assert.equal(r.code, 0, r.err);
  assert.match(r.out, RID_LINE);
  const rid = r.out.slice("rid: ".length).trim();
  await waitFor(async () => (await diaryOf(h.root, "beta")).some((l) => l.event === "inreq" && finalOf(l) === "completed"), 6_000);
  assert.deepEqual(seen, { a: 1 });
  const archived = (await diaryOf(h.root, "beta")).find((l) => l.event === "inreq")!;
  assert.equal(reqOf(archived).rid, rid);
  assert.equal(reqOf(archived).type, "cmd");
  assert.equal(reqOf(archived).request.name, "noop");
  assert.deepEqual(await io.listDir(path.join(h.root, "beta", "inbox")), [], "the peer consumed the envelope");
});

test("cmd: an unlisted commander is written and dropped by the peer (D4), a missing peer fails locally", opts, async (t) => {
  const h = await harness(t);
  const peer = await peerOf(h, []); // accepts_commands_from: [] means nobody
  const r = await cli(h.root, ["cmd", "alpha", "beta", "noop"]);
  assert.equal(r.code, 0, "the CLI wrote it: D0 is the peer's decision");
  await waitFor(async () => (await diaryOf(h.root, "beta")).some((l) => l.event === "inreq" && finalOf(l) === "dropped"), 6_000);
  const missing = await cli(h.root, ["cmd", "alpha", "noghost", "noop"]);
  assert.equal(missing.code, 1);
  assert.match((await cli(h.root, ["cmd", "alpha", "beta"])).err, /cmd needs <src-id> <dst> <name>/);
});

// --- broadcast ---

test("broadcast: a schema-valid nuntium/nuntium.json that `validate` reads as an announce", async (t) => {
  const h = await harness(t);
  const r = await cli(h.root, ["broadcast", "rescan the tree"]);
  assert.equal(r.code, 0, r.err);
  const file = path.join(h.root, "nuntium", "nuntium.json");
  assert.equal(r.out, `wrote ${file}`);
  const obj = await json<{ id: string; nuntium: string; datetime: string; ts: number }>(file);
  assert.equal(obj.id, "broadcast"); // a fixed non-agent id: the file is a signal, not an announcement
  assert.equal(obj.nuntium, "rescan the tree");
  assert.ok(validateAnnounce(obj).ok, "the broadcast conforms to the announce schema");
  const v = await cli(h.root, ["validate", file]); // the name decides the kind
  assert.equal(v.code, 0);
  assert.equal(v.out, "OK announce");
  assert.equal((await cli(h.root, ["broadcast"])).code, 1);
});

test("broadcast: the signal is one file that is rewritten, never accumulated", async (t) => {
  const h = await harness(t);
  await cli(h.root, ["broadcast", "first"]);
  await cli(h.root, ["broadcast", "second"]);
  assert.deepEqual(await io.listDir(path.join(h.root, "nuntium")), ["nuntium.json"]);
  assert.equal((await json<{ nuntium: string }>(path.join(h.root, "nuntium", "nuntium.json"))).nuntium, "second");
});

// --- validate ---

test("validate: OK on a conforming file; the first Ajv error and exit 1 on a broken one", async (t) => {
  const h = await harness(t);
  await publish(h.root, "beta", specOf("beta"), statusOf("beta", "idle"));
  const good = await cli(h.root, ["validate", path.join(h.root, "beta", "status.json")]);
  assert.equal(good.code, 0);
  assert.equal(good.out, "OK status");
  assert.equal((await cli(h.root, ["validate", path.join(h.root, "beta", "spec.json")])).out, "OK spec");
  await fsp.writeFile(path.join(h.root, "beta", "status.json"), JSON.stringify(statusOf("beta", "sleeping")));
  const bad = await cli(h.root, ["validate", path.join(h.root, "beta", "status.json")]);
  assert.equal(bad.code, 1);
  assert.match(bad.err, /^status: data\/status must be equal to one of the allowed values$/);
  assert.match((await cli(h.root, ["validate", path.join(h.root, "beta", "nothing.json")])).err, /cannot read/);
  assert.equal((await cli(h.root, ["validate"])).code, 1);
});

test("validate --as: an explicit kind wins over the name; an unknown kind is refused", async (t) => {
  const h = await harness(t);
  const file = path.join(h.root, "signal.json");
  await fsp.writeFile(file, JSON.stringify({ id: "broadcast", datetime: ISO, ts: 1_700_000_000, nuntium: "hi" }));
  assert.equal((await cli(h.root, ["validate", file])).code, 1, "as a request name, it is not one");
  const forced = await cli(h.root, ["validate", file, "--as", "announce"]);
  assert.equal(forced.code, 0);
  assert.equal(forced.out, "OK announce");
  const unknown = await cli(h.root, ["validate", file, "--as", "envelope"]);
  assert.equal(unknown.code, 1);
  assert.match(unknown.err, /no validator for "envelope": use --as actio\|spec\|status/);
});

test("validate: a diarium is checked line by line, and the first bad line is reported by number", async (t) => {
  const h = await harness(t);
  const file = path.join(h.root, "beta", "diarium.jsonl");
  await io.makeDir(path.join(h.root, "beta"));
  const line = (event: string, event_data: unknown): string => JSON.stringify({ id: "beta", datetime: ISO, event, event_data });
  const bad = `${["one", "two"].map((m) => line("log", m)).join("\n")}\n${line("status", "sleeping")}\n${line("log", "four")}\n`;
  await fsp.writeFile(file, bad);
  const r = await cli(h.root, ["validate", file]);
  assert.equal(r.code, 1);
  assert.match(r.err, /^diarium: line 3: data\/event_data must be equal to one of the allowed values$/);
  await fsp.writeFile(file, `${["one", "two", "three"].map((m) => line("log", m)).join("\n")}\n`);
  const good = await cli(h.root, ["validate", file]);
  assert.equal(good.code, 0);
  assert.equal(good.out, "OK diarium (3 lines)");
  await fsp.writeFile(file, '{"id": "beta", datetime:}\n');
  assert.match((await cli(h.root, ["validate", file])).err, /diarium: line 1 is not valid JSON/);
});

test("validate: the exchange file names of an inbox decide the kind", opts, async (t) => {
  const h = await harness(t);
  await io.makeDir(path.join(h.root, "beta", "inbox"));
  const run = cli(h.root, ["call", "alpha", "beta", "echo", '{"text":"hi"}']);
  const rid = await answer(h.root, "beta");
  await run;
  const inbox = path.join(h.root, "beta", "inbox");
  for (const [name, kind] of [
    [`${rid}.json`, "request"],
    [`${rid}.res.json`, "response"],
    [`${rid}.con.json`, "confirmation"],
  ] as const) {
    const v = await cli(h.root, ["validate", path.join(inbox, name)]);
    assert.equal(v.code, 0, `${name}: ${v.err}`);
    assert.equal(v.out, `OK ${kind}`);
  }
});

// --- usage, config, and the direct entry ---

test("usage: no subcommand, an unknown subcommand, and a flag without its value exit 1", async (t) => {
  const h = await harness(t);
  const none = await cli(h.root, []);
  assert.equal(none.code, 1);
  assert.match(none.err, /^usage: actio \[--json\] ls \| diarium/);
  const unknown = await cli(h.root, ["nope", "--json"]);
  assert.equal(unknown.code, 1);
  assert.match(unknown.err, /unknown subcommand "nope"/);
  assert.match(unknown.err, /usage: actio/);
  assert.match((await cli(h.root, ["diarium", "beta", "-n"])).err, /-n needs a value/);
});

test("config: a bad ACTIO_SYNC is reported at once, before any subcommand runs", async (t) => {
  const h = await harness(t);
  const r = await cli(h.root, ["ls"], { ACTIO_SYNC: "rsync" });
  assert.equal(r.code, 1);
  assert.match(r.err, /^config: /);
});

test("the direct entry: `tsx src/cli.ts` prints on stdout and sets the exit code", async (t) => {
  const h = await harness(t);
  const run = (args: string[]) =>
    spawnSync(process.execPath, ["--import", "tsx", "src/cli.ts", ...args], {
      cwd: projectRoot,
      env: { ...process.env, ACTIO_DIR: h.root, ACTIO_SYNC: "none" },
      encoding: "utf8",
    });
  const ok = run(["broadcast", "from the entry point"]);
  assert.equal(ok.status, 0, ok.stderr);
  assert.match(ok.stdout, /wrote .{0,200}nuntium\/nuntium\.json/);
  assert.equal((await json<{ nuntium: string }>(path.join(h.root, "nuntium", "nuntium.json"))).nuntium, "from the entry point");
  const broken = run(["validate", path.join(h.root, "beta", "spec.json")]);
  assert.equal(broken.status, 1);
  assert.match(broken.stderr, /cannot read/);
  assert.equal(run(["nope"]).status, 1);
});
