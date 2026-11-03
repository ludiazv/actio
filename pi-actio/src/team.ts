// The ActioAgent wrapper: lifecycle, the team view without this agent, and calls with history
// (D-6…D-11). Protocol behavior comes only from the actio-ts package entry.
import { ActioAgent, checkParams, type AgentOptions, type AgentView, type RequestOutcome } from "actio-ts";
import { expandHome, type Config } from "./config.js";
import type { CallRecord, History } from "./history.js";

export class Team {
  /** Capabilities are registered on it before `start()`. */
  readonly agent: ActioAgent;
  readonly id: string;
  /** `config.actioDir` with `~/` expanded, else undefined (D-5). */
  readonly root: string | undefined;

  private history: History;
  private agentRunning = false;
  /** Set while a `start()` is in flight, so a `stop()` can await it (V-4). */
  private startPromise: Promise<void> | undefined;

  constructor(config: Config, cwd: string, history: History) {
    this.history = history;
    this.id = config.agentId;
    this.root = config.actioDir === undefined ? undefined : expandHome(config.actioDir);
    // D-5, D-6: namespaces only when present; no protocol commands; identity from the cwd.
    const options: AgentOptions = {
      id: this.id,
      description: `pi coding agent in ${cwd}`,
      acceptsCommandsFrom: [],
    };
    if (config.namespaces !== undefined) options.namespaces = config.namespaces;
    if (this.root !== undefined) options.config = { root: this.root };
    this.agent = new ActioAgent(options);
  }

  start(): Promise<void> {
    if (this.agentRunning) return Promise.resolve();
    // A `start()` called twice returns the same promise; the field clears when it settles.
    this.startPromise ??= this.agent
      .start()
      .then(() => {
        this.agentRunning = true;
      })
      .finally(() => {
        this.startPromise = undefined;
      });
    return this.startPromise;
  }

  /** `stop({ force: true })`: quitting never waits out the grace period (D-7). */
  async stop(): Promise<void> {
    const start = this.startPromise;
    this.startPromise = undefined;
    if (start !== undefined) {
      try {
        await start;
      } catch {
        // Only its rejection is swallowed: the start caller reports it (D-7), not `stop()`.
      }
    }
    if (!this.agentRunning) return;
    this.agentRunning = false;
    await this.agent.stop({ force: true });
  }

  running(): boolean {
    return this.agentRunning;
  }

  /** The current view, without this agent, sorted by id (D-8). */
  view(): AgentView[] {
    return Team.minusSelf(this.agent.agmen(), this.id);
  }

  /** A full scan, then the same view (D-9). */
  async refresh(): Promise<AgentView[]> {
    const views = await this.agent.refresh();
    return Team.minusSelf(views, this.id);
  }

  /** Validated call (`<id>/<capability>`), recorded in the history (D-10, D-11). */
  async call(target: string, params: unknown, signal?: AbortSignal): Promise<RequestOutcome> {
    const match = /^([^/]+)\/(.+)$/.exec(target);
    if (match === null) {
      throw new Error(`invalid target "${target}": expected <id>/<capability> (use list-team)`);
    }
    const id = match[1];
    const name = match[2];
    const view = this.view().find((v) => v.id === id);
    if (view === undefined) {
      throw new Error(`agent "${id}" is not in the team view (use list-team)`);
    }
    const capability = view.capabilities.get(name);
    if (capability === undefined) {
      throw new Error(`capability "${name}" not found on "${id}" (use \`info ${id}\`)`);
    }
    const invalid = checkParams(capability, params ?? null);
    if (invalid !== undefined) {
      throw new Error(`invalid params for ${id}/${name}: ${invalid} (use \`info ${id}\`)`);
    }
    // D-10 passed: the call is real, so its record opens now (D-19).
    const record = this.history.startCall(`${id}/${name}`);
    try {
      const outcome = await this.agent.request(id, name, params ?? null, { timeout: 0, signal });
      // A peer's error answer is a confirmed record whose `error` is set (D-19): the payload is
      // the protocol's error result in `response.result` (`code` optional, `msg` required);
      // the guard covers a malformed peer.
      if (outcome.finalStatus === "confirmed" && outcome.response?.error === true) {
        const result = outcome.response.result;
        this.history.endCall(record, "confirmed", {
          code: typeof result?.code === "number" ? result.code : undefined,
          error: typeof result?.msg === "string" ? result.msg : "",
        });
      } else {
        this.history.endCall(record, outcome.finalStatus);
      }
      return outcome;
    } catch (err) {
      this.history.endCall(record, "failed", { error: err instanceof Error ? err.message : String(err) });
      throw err;
    }
  }

  /** The Agmen view minus this agent, sorted by id (D-8). */
  private static minusSelf(views: Map<string, AgentView>, self: string): AgentView[] {
    const out: AgentView[] = [];
    for (const view of views.values()) if (view.id !== self) out.push(view);
    out.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    return out;
  }
}