// Echo agent: the black-box conformance target — its behavior is FIXED.
//
// Env:
//   ACTIO_AGENT_ID    agent id (default "echo"); its folder is <root>/<id>
//   ACTIO_COMMANDERS  comma list → acceptsCommandsFrom (default empty)
//   ACTIO_NAMESPACES  comma list (absent by default)
//   plus the config env overrides: ACTIO_DIR, ACTIO_SYNC, ACTIO_HEARTBEAT,
//   ACTIO_CONFIRM_TIMEOUT, ACTIO_CANCEL_WAIT (read by loadConfig at start()).
//
// Runs until `stop` (a command) or SIGINT/SIGTERM (graceful lane C); exits 0.
import { ActioAgent } from "../src/agent.js";
import { ActioError } from "../src/types.js";
import type { Capability } from "../src/generated/types.js";

/** The `{}` parameters of a capability that takes nothing. */
const NO_PARAMS = { type: "object", properties: {}, additionalProperties: false };

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

const SLEEP: Capability = {
  name: "sleep",
  description: "waits the given seconds and reports them; honours cancellation",
  parameters: {
    type: "object",
    properties: { seconds: { type: "number" } },
    required: ["seconds"],
    additionalProperties: false,
  },
  actio: { result: { type: "object", properties: { slept: { type: "number" } }, required: ["slept"] }, max_parallel: 1 },
};

const FAIL: Capability = {
  name: "fail",
  description: "always fails with ActioError code 1000",
  parameters: NO_PARAMS,
  actio: { result: true },
};

const OFF: Capability = {
  name: "off",
  description: "disabled in the spec",
  parameters: NO_PARAMS,
  actio: { result: true, disabled: true },
};

const PRIVATE: Capability = {
  name: "private",
  description: "callable only by nobody",
  parameters: NO_PARAMS,
  actio: { result: true, accepts_from: ["nobody"] },
};

/** Waits `ms` milliseconds, resolving early when `signal` aborts (A3 cancellation). */
function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const onAbort = () => {
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

const id = process.env["ACTIO_AGENT_ID"] ?? "echo";
const commanders = (process.env["ACTIO_COMMANDERS"] ?? "").split(",").filter((s) => s !== "");
const namespaces = (process.env["ACTIO_NAMESPACES"] ?? "").split(",").filter((s) => s !== "");

const agent = new ActioAgent({
  id,
  description: "conformance target agent (echo)",
  acceptsCommandsFrom: commanders,
  ...(namespaces.length > 0 ? { namespaces } : {}),
});

agent.capability(ECHO, async (params) => ({ text: (params as { text: string }).text }));
agent.capability(SLEEP, async (params, ctx) => {
  const seconds = (params as { seconds: number }).seconds;
  await abortableSleep(seconds * 1000, ctx.signal);
  return { slept: seconds };
});
agent.capability(FAIL, async () => {
  throw new ActioError(1000, "fail: requested failure");
});
agent.capability(OFF, async () => ({}));
agent.capability(PRIVATE, async () => ({}));
agent.onCommand("noop", async () => undefined);

await agent.start();

// The library never installs signal handlers; the example does: SIGINT/SIGTERM → graceful
// lane C, then exit 0. (A `stop` command stops the agent too; with every watcher closed,
// the process then exits 0 on its own.)
const stop = () => {
  void agent
    .stop()
    .then(() => process.exit(0))
    .catch(() => process.exit(1));
};
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
