// Requestor (WP6): lane B — write request/command, watch the destination inbox (D-18),
// cancellation (B4), confirmation (B3 with the D-13 guard), attachment cleanup (B5), abandon (D-6).
import type { AgentContext, AgentId, RequestOptions, RequestOutcome } from "./types.js";

/** Requestor callbacks into the ActioAgent. */
export interface RequestorHooks {
  /** Watch error on a destination inbox (D-22): every outbound exchange there is abandoned. */
  onWatchError(dir: string, err: Error): void;
  /** A busy/idle-relevant exchange state changed (D-8). */
  onStateChange(): void;
}

/** The requestor side of one agent: issues requests and commands to other agents. */
export class Requestor {
  constructor(private readonly ctx: AgentContext, private readonly hooks: RequestorHooks) {
    throw new Error("not implemented");
  }

  /** Feeds one file event of the OWN inbox (self-calls share the receiver's watcher). */
  notifyFile(folder: string, name: string): void {
    throw new Error("not implemented");
  }

  /** Lane B for `type="req"`: write, wait (B2), cancel on timeout/signal (B4), confirm (B3). */
  request(dst: AgentId, name: string, params: unknown, options: RequestOptions): Promise<RequestOutcome> {
    throw new Error("not implemented");
  }

  /** Lane B for `type="cmd"`: write the command; `outreq` "sent" right after (B1). */
  command(dst: AgentId, name: string, params: unknown): Promise<string> {
    throw new Error("not implemented");
  }

  /** Explicit cancellation of an ongoing outbound request (writes `<rid>.can.json`, B4). */
  cancel(rid: string): Promise<void> {
    throw new Error("not implemented");
  }

  /** Abandons every outbound exchange in B2 (agent stop, D-9). */
  stop(): Promise<void> {
    throw new Error("not implemented");
  }

  /** Number of outbound exchanges waiting for a response in B2 (for busy, D-8). */
  outboundCount(): number {
    throw new Error("not implemented");
  }
}
