// The extension entry: configuration, wiring of the tool, the command, the prompt service and
// the session lifecycle. No resources are started here; they belong to `session_start` (D-7).
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { loadConfig } from "./config.js";
import { openExplorer } from "./explorer.js";
import { History, type CallRecord } from "./history.js";
import { PromptService } from "./prompt.js";
import { Team } from "./team.js";
import { actioTool } from "./tool.js";

export default function (pi: ExtensionAPI): void {
  // 1. Configuration decides whether anything is registered at all (D-2, D-3).
  let config;
  try {
    config = loadConfig();
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    // D-2: register nothing; a session_start handler notifies once.
    pi.on("session_start", (_event, ctx) => {
      if (ctx.hasUI) ctx.ui.notify(`actio: ${message}`, "info");
    });
    return;
  }
  if (config.disabled) return;

  const cwd = process.cwd();

  // 2. The history, the team, and (when exposed) the prompt service on the agent.
  const history = new History();
  const team = new Team(config, cwd, history);
  let prompt: PromptService | undefined;
  if (config.exposePrompt) {
    prompt = new PromptService(pi, history);
    prompt.register(team.agent, config.acceptPrompt);
  }

  // 3. and 4. The tool and the /actio command.
  pi.registerTool(actioTool(team, config));
  pi.registerCommand("actio", {
    description: "Open the actio team explorer",
    handler: (_args, ctx) => openExplorer(ctx, team, history, config.exposePrompt),
  });

  // The latest context, kept from session_start and agent_settled (D-18, §3.2).
  let latest: ExtensionContext | undefined;

  // 5. The activity subscription is the only notifier (D-18, A-1).
  if (config.reportActivity) {
    history.onChange((event) => {
      const ctx = latest;
      if (ctx === undefined || !ctx.hasUI) return;
      if (event.kind === "call") {
        if (event.record.state === "in-flight") return; // notify only when it is no longer in-flight
        ctx.ui.notify(callText(event.record), "info");
        return;
      }
      // A polling retrieve (queued/running) is recorded but not notified (A-1).
      if (event.record.capability === "retrieve" && (event.record.outcome === "queued" || event.record.outcome === "running")) {
        return;
      }
      const id = event.record.promptId === undefined ? "" : ` [${event.record.promptId}]`;
      ctx.ui.notify(`actio: ${event.record.src} → ${event.record.capability}${id}`, "info");
    });
  }

  // 6. The agent starts in session_start, in every mode (D-7).
  pi.on("session_start", async (_event, ctx) => {
    latest = ctx;
    prompt?.bind(ctx);
    try {
      await team.start();
    } catch (err) {
      // D-7: a failed start is notified; the tool answers "actio agent is not running".
      if (ctx.hasUI) ctx.ui.notify(`actio: agent start failed: ${err instanceof Error ? err.message : String(err)}`, "info");
    }
  });
  pi.on("agent_settled", (_event, ctx) => {
    latest = ctx;
  });

  // 7. Release in session_shutdown, idempotently (pi awaits the handler).
  pi.on("session_shutdown", async () => {
    await team.stop();
  });
}

/** The D-18 notification text for a finished call record (A-2). */
function callText(record: CallRecord): string {
  switch (record.state) {
    case "confirmed":
      return record.error === undefined
        ? `actio: ${record.target} → confirmed`
        : `actio: ${record.target} → error${record.code === undefined ? "" : ` ${record.code}`}`;
    case "failed":
      return `actio: ${record.target} → failed: ${record.error ?? ""}`;
    default:
      return `actio: ${record.target} → ${record.state}`;
  }
}