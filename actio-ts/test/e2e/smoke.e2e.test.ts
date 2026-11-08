// Smoke test: spawn `examples/echo-agent.ts` as a child process with fast
// timers, run the `actio` CLI against it from a cold start, and verify the full loop: announce,
// `call` for `echo` / `fail` / `sleep`+cancel, `validate` on the produced files, and
// `cmd … stop` → the agent exits 0 on its own. Every child is spawned here and reaped here
// (kill logic scoped to these PIDs only; no /proc walking).
import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test, type TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import { waitFor } from "../helpers/index.js";

const projectRoot = path.resolve(fileURLToPath(import.meta.url), "../../..");

/** The env of the smoke world: a per-test temp root and fast protocol timers (seconds). */
function actioEnv(root: string): NodeJS.ProcessEnv {
  return {
    ...process.env,
    ACTIO_DIR: root,
    ACTIO_SYNC: "none",
    ACTIO_HEARTBEAT: "1",
    ACTIO_CONFIRM_TIMEOUT: "5",
    ACTIO_CANCEL_WAIT: "5",
  };
}

/** One CLI invocation as a child process (the real `tsx src/cli.ts` entry; spawnSync reaps it). */
function runCli(root: string, args: string[]): { code: number; out: string; err: string } {
  const res = spawnSync(process.execPath, ["--import", "tsx", "src/cli.ts", ...args], {
    cwd: projectRoot,
    env: actioEnv(root),
    encoding: "utf8",
    timeout: 30_000, // generous: this test is about the cold-start loop, not speed
  });
  return { code: res.status ?? -1, out: res.stdout ?? "", err: res.stderr ?? "" };
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

test("smoke: echo-agent + the actio CLI, end to end from a cold start", { timeout: 120_000 }, async (t) => {
  const root = await fsp.mkdtemp(path.join(os.tmpdir(), ".actio-smoke-"));
  let agent: ChildProcess;
  t.after(async () => {
    // Reap accounting for exactly the children this test spawned: the CLI runs are spawnSync
    // (already reaped); the agent is killed only if it is somehow still alive, by its own PID.
    if (agent.exitCode === null && agent.signalCode === null) agent.kill("SIGKILL");
    await exitCodeOf(agent, 5_000);
    await fsp.rm(root, { recursive: true, force: true });
  });

  // a. cold start: the echo agent, with fast timers; `smoke` (the CLI vehicle id) may command it
  agent = spawn(process.execPath, ["--import", "tsx", "examples/echo-agent.ts"], {
    cwd: projectRoot,
    env: { ...actioEnv(root), ACTIO_AGENT_ID: "echo", ACTIO_COMMANDERS: "smoke" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const agentLog: string[] = [];
  agent.stdout?.on("data", (d) => agentLog.push(String(d)));
  agent.stderr?.on("data", (d) => agentLog.push(String(d)));

  // b. the announce file appears (nuntium/echo.n.json)
  await waitFor(
    () => fsp.access(path.join(root, "nuntium", "echo.n.json")).then(() => true).catch(() => false),
    15_000,
  );

  // c. call `echo`: confirmed, and the text comes back
  const echo = runCli(root, ["call", "smoke", "echo", "echo", '{"text":"smoke"}']);
  assert.equal(echo.code, 0, `echo call: exit 0 (out: ${echo.out}; err: ${echo.err})`);
  assert.match(echo.out, /finalStatus: confirmed/);
  assert.match(echo.out, /result: \{"text":"smoke"\}/);

  // d. call `fail`: an error answer is still an answer — exit 0, code 1000 on the stream
  const fail = runCli(root, ["call", "smoke", "echo", "fail", "{}"]);
  assert.equal(fail.code, 0, `fail call: exit 0 (out: ${fail.out}; err: ${fail.err})`);
  assert.match(fail.out, /finalStatus: confirmed/);
  assert.match(fail.out, /error: 1000 "fail: requested failure"/);

  // e. call `sleep` with a 1 s CLI timeout: the receiver aborts the work (A3) and answers with a
  //    cancellation response, so the exchange ends `cancelled` WITH a response → exit 0.
  //    (The abandon path / exit 1 is unreachable here: the echo agent
  //    answers cancellations in milliseconds and ACTIO_CANCEL_WAIT must be a positive number of
  //    seconds. The abandon path itself is covered by the requestor unit tests.)
  const sleep = runCli(root, ["call", "smoke", "echo", "sleep", '{"seconds":5}', "--timeout", "1"]);
  assert.equal(sleep.code, 0, `sleep+cancel: exit 0 (out: ${sleep.out}; err: ${sleep.err})`);
  assert.match(sleep.out, /finalStatus: cancelled/);
  assert.match(sleep.out, /cancellation: true/);

  // f. validate the produced files: spec, status, the announce envelope, the diarium
  for (const [file, kind] of [
    [path.join(root, "echo", "spec.json"), "spec"],
    [path.join(root, "echo", "status.json"), "status"],
    [path.join(root, "nuntium", "echo.n.json"), "announce"],
    [path.join(root, "echo", "diarium.jsonl"), "diarium"],
  ] as const) {
    const v = runCli(root, ["validate", file]);
    assert.equal(v.code, 0, `validate ${path.basename(file)}: exit 0 (err: ${v.err})`);
    assert.match(v.out, new RegExp(`^OK ${kind}`), `validate ${path.basename(file)}: OK ${kind}`);
  }
  // (After a confirmed exchange the receiver has archived the request/response/confirmation
  // envelopes into the diarium, so the live envelopes left on disk are the announce + diarium.)

  // g. the stop command: the CLI only writes the envelope (rid on stdout, exit 0) …
  const stop = runCli(root, ["cmd", "smoke", "echo", "stop", "{}"]);
  assert.equal(stop.code, 0, `cmd stop: exit 0 (out: ${stop.out}; err: ${stop.err})`);
  assert.match(stop.out, /^rid: [0-9A-HJKMNP-TV-Z]{26}$/m);

  // … and the agent stops itself and exits 0 on its own (no signal from us)
  const code = await exitCodeOf(agent, 15_000);
  assert.equal(code, 0, `the agent exited 0 on its own after the stop command (log: ${agentLog.join("")})`);
});