// The prompt service: four capabilities offered to the team (D-12…D-17), the queue, and the
// run state. Registered on the actio agent before it starts.
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { ActioAgent } from "actio-ts";
import type { History } from "./history.js";

/** The part of the extension API the queue needs; `on` is used for `agent_end`/`agent_settled`. */
export interface PiSurface {
  sendUserMessage: ExtensionAPI["sendUserMessage"];
  on: ExtensionAPI["on"];
}

export class PromptService {
  constructor(pi: PiSurface, history: History) {
    void pi;
    void history;
  }

  /** Registers the four capabilities; `acceptFrom` becomes each `actio.accepts_from` (D-17). */
  register(agent: ActioAgent, acceptFrom: string[]): void {
    void agent;
    void acceptFrom;
    throw new Error("not implemented: PromptService.register");
  }

  /** Keeps the latest context: `isIdle`, `abort`, `compact` (D-14…D-16). */
  bind(ctx: ExtensionContext): void {
    void ctx;
    throw new Error("not implemented: PromptService.bind");
  }
}
