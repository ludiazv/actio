// End-to-end smoke run, run with `npm run smoke`.
// One line per step, exit 0 when all steps pass, 1 otherwise. No LLM call: the faux model.
// Every directory is temporary, so nothing of the user's pi or actio root is touched.
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { ActioAgent, type AgentId, type AgentView, type RequestOutcome } from "actio-ts";

const PKG = fileURLToPath(new URL("..", import.meta.url));
const ROOT = mkdtempSync(join(tmpdir(), "smoke-root-"));
const AGENT_DIR = mkdtempSync(join(tmpdir(), "smoke-agent-"));
const CWD = mkdtempSync(join(tmpdir(), "smoke-cwd-"));

// 1. A temp agent dir whose actio.json exposes the prompt service (D-1, D-17: acceptPrompt empty
//    means `accepts_from` is omitted, so every agent may call it).
writeFileSync(join(AGENT_DIR, "actio.json"), JSON.stringify({ agentId: "pi1", exposePrompt: true }));

// 3. pi itself, loaded with the faux provider and this extension. stdin stays open.
const child = spawn(
  "pi",
  ["--mode", "rpc", "--no-session", "-ne", "-e", join(PKG, "test/faux.ts"), "-e", join(PKG, "src/index.ts"),
    "--provider", "faux", "--model", "echo"],
  { cwd: CWD, env: { ...process.env, PI_CODING_AGENT_DIR: AGENT_DIR, ACTIO_DIR: ROOT }, stdio: ["pipe", "pipe", "pipe"] },
);
child.stdout.resume();
child.stderr.resume();

// 2. The in-process peer with one capability, `echo`.
let echoRan = 0;
const tester = new ActioAgent({ id: "tester", description: "smoke peer", acceptsCommandsFrom: [], config: { root: ROOT } });
tester.capability(
  { name: "echo", description: "Echo the text back", parameters: { type: "object", properties: { text: { type: "string" } }, required: ["text"], additionalProperties: false }, actio: { result: { type: "object" } } },
  async (params) => { echoRan++; return { echoed: (params as { text: string }).text }; },
);
await tester.start();

let failed = 0;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const step = (name: string, ok: boolean, detail = "") => {
  // One line per step: a peer's answer can be a multi-line Markdown block.
  console.log(`${ok ? "ok  " : "FAIL"}  ${name}${detail === "" ? "" : `  ·  ${detail.replace(/\s+/g, " ")}`}`);
  if (!ok) failed++;
};
const body = (o: RequestOutcome) => o.response?.result as Record<string, unknown> | undefined;

// Wait for a prompt_id to reach `done`, then read its answer (D-13, D-14).
async function answerOf(id: string): Promise<string> {
  for (let i = 0; i < 100; i++) {
    const r = body(await tester.request("pi1", "retrieve", { prompt_id: id }));
    if (r?.status === "done") return String(r.response ?? "");
    await sleep(200);
  }
  return "<timeout>";
}
async function askPi1(msg: string): Promise<{ id: string; answer: string }> {
  const id = String(body(await tester.request("pi1", "prompt", { msg_prompt: msg }))?.prompt_id ?? "");
  return { id, answer: await answerOf(id) };
}

try {
  // pi1 must show up in the team view with the four prompt capabilities.
  let pi1: AgentView | undefined;
  for (let i = 0; i < 60 && pi1 === undefined; i++) {
    const view: Map<AgentId, AgentView> = await tester.refresh();
    pi1 = view.get("pi1");
    if (pi1 === undefined) await sleep(250);
  }
  const caps = (pi1?.spec.capabilities ?? []).map((c) => c.name).sort().join(" ");
  step("setup: pi1 is in the team view, exposePrompt published its capabilities",
    pi1 !== undefined && caps === "cancel compact prompt retrieve", `capabilities: ${caps || "none"}`);

  // The smoke steps.
  const first = await askPi1("hello");
  step("prompt + retrieve: the answer is the faux echo of the prompt", first.answer === "echo: hello", `answer: ${first.answer}`);

  const again = await tester.request("pi1", "retrieve", { prompt_id: first.id });
  step("retrieve of a forgotten id is error 1000 (D-13)",
    again.finalStatus === "confirmed" && again.response?.error === true && body(again)?.code === 1000,
    `code: ${String(body(again)?.code)}`);

  // The faux model answers with a tool call, so pi runs the `actio` tool, which calls tester/echo.
  const viaTool = await askPi1('toolcall actio {"command":"call","target":"tester/echo","params":{"text":"hi"}}');
  step("a prompt whose reply is an actio tool call runs tester's echo handler",
    echoRan > 0 && viaTool.answer.includes("tester/echo") && viaTool.answer.includes("hi"),
    `echo calls: ${echoRan} · answer: ${viaTool.answer.slice(0, 60)}`);

  // A slow reply keeps the run busy, so the next prompt is queued and can be cancelled (D-14, D-15).
  const slow = await tester.request("pi1", "prompt", { msg_prompt: "slow one" });
  const slowId = String(body(slow)?.prompt_id ?? "");
  const queued = await tester.request("pi1", "prompt", { msg_prompt: "queued one" });
  const queuedId = String(body(queued)?.prompt_id ?? "");
  const state = body(await tester.request("pi1", "retrieve", { prompt_id: queuedId }))?.status;
  step("while a run is in flight the next prompt is queued", state === "queued", `status: ${String(state)}`);
  const cancel = await tester.request("pi1", "cancel", { prompt_id: queuedId });
  const gone = await tester.request("pi1", "retrieve", { prompt_id: queuedId });
  step("cancel of a queued prompt forgets it (D-15)",
    cancel.finalStatus === "confirmed" && cancel.response?.error !== true && body(gone)?.code === 1000,
    `retrieve after cancel: ${JSON.stringify(body(gone))}`);
  const slowAnswer = await answerOf(slowId);
  const cancelledIsGone = (await tester.request("pi1", "retrieve", { prompt_id: queuedId })).response?.error === true;
  step("the running prompt still finished, and the cancelled sibling stayed forgotten",
    slowAnswer.startsWith("echo:") && cancelledIsGone, `answer: ${slowAnswer.slice(0, 40)}`);

  // compact always answers null (D-16).
  const compact = await tester.request("pi1", "compact", {});
  step("compact answers null, whatever the session does (D-16)",
    compact.finalStatus === "confirmed" && compact.response?.error !== true,
    `finalStatus: ${compact.finalStatus} · result: ${JSON.stringify(body(compact))}`);
} catch (err) {
  step("smoke run aborted", false, err instanceof Error ? err.message : String(err));
} finally {
  await tester.stop({ force: true });
  child.stdin.end();
  if (!child.killed) await new Promise<void>((r) => { child.once("close", () => r()); setTimeout(r, 8000); });
  // Keep the folders only when something failed, so a green run leaves nothing in /tmp.
  if (failed === 0) {
    for (const dir of [ROOT, AGENT_DIR, CWD]) rmSync(dir, { recursive: true, force: true });
    step("teardown: temp folders removed", true);
  } else {
    step(`teardown: temp folders kept for inspection: ${ROOT} · ${AGENT_DIR} · ${CWD}`, true);
  }
}
console.log(failed === 0 ? "SMOKE OK" : `SMOKE FAILED (${failed})`);
process.exit(failed === 0 ? 0 : 1);
