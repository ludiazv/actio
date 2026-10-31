// Planner (Plan §2.9): a small requestor demo — an ActioAgent with no capabilities that, when
// run, sends ONE `echo` request to ACTIO_TARGET (default "echo") and prints the outcome.
//
// Env:
//   ACTIO_AGENT_ID  this agent's id (default "planner")
//   ACTIO_TARGET    the destination agent (default "echo")
//   plus the config env overrides of §2.4 (ACTIO_DIR, ACTIO_SYNC, …).
import { ActioAgent } from "../src/agent.js";
import { ActioCallError, ActioResultError } from "../src/types.js";

const id = process.env["ACTIO_AGENT_ID"] ?? "planner";
const target = process.env["ACTIO_TARGET"] ?? "echo";

const agent = new ActioAgent({
  id,
  description: "requestor demo (planner)",
  acceptsCommandsFrom: [],
});

await agent.start();
let failure: string | undefined;
try {
  const outcome = await agent.request(target, "echo", { text: "hello from planner" }, { timeout: 30 });
  console.log(`finalStatus: ${outcome.finalStatus}`);
  console.log(`result:      ${JSON.stringify(outcome.response?.result ?? null)}`);
} catch (err) {
  if (err instanceof ActioCallError || err instanceof ActioResultError) {
    failure = err.message; // a protocol-level rejection: report it and exit 1
  } else {
    throw err; // a real bug: crash loudly
  }
}
await agent.stop();
if (failure !== undefined) {
  console.log(`call failed: ${failure}`);
  process.exit(1);
}
