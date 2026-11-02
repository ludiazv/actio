// The ActioAgent wrapper: lifecycle, the team view without this agent, and calls with history
// (D-6…D-11). Protocol behavior comes only from the actio-ts package entry.
import type { ActioAgent, AgentView, RequestOutcome } from "actio-ts";
import type { Config } from "./config.js";
import type { History } from "./history.js";

export class Team {
  /** Capabilities are registered on it before `start()`. */
  readonly agent: ActioAgent;
  readonly id: string;
  /** `config.actioDir` with `~/` expanded, else undefined (D-5). */
  readonly root: string | undefined;

  constructor(config: Config, cwd: string, history: History) {
    void config;
    void cwd;
    void history;
    this.agent = undefined as unknown as ActioAgent;
    this.id = "";
    this.root = undefined;
  }

  start(): Promise<void> {
    throw new Error("not implemented: Team.start");
  }

  stop(): Promise<void> {
    throw new Error("not implemented: Team.stop");
  }

  running(): boolean {
    throw new Error("not implemented: Team.running");
  }

  /** The current view, without this agent, sorted by id (D-8). */
  view(): AgentView[] {
    throw new Error("not implemented: Team.view");
  }

  /** A full scan, then the same view (D-9). */
  async refresh(): Promise<AgentView[]> {
    throw new Error("not implemented: Team.refresh");
  }

  /** Validated call (`<id>/<capability>`), recorded in the history (D-10, D-11). */
  async call(target: string, params: unknown, signal?: AbortSignal): Promise<RequestOutcome> {
    void target;
    void params;
    void signal;
    throw new Error("not implemented: Team.call");
  }
}
