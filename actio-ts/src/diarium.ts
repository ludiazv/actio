// Diarium (WP3): append + typed event builders. append() validates with validateDiarium before
// writing (a bug, not a peer, produced the event → throw).
import type { DiariumLine, Exchange } from "./generated/types.js";
import type { AgentId, InreqFinalStatus, OutreqFinalStatus, StatusState } from "./types.js";

/** Appends one event line to `<folder>/diarium.jsonl` after validating it. */
export function append(folder: string, event: DiariumLine): Promise<void> {
  throw new Error("not implemented");
}

/** `status` event: change of the agent's status. */
export function statusEvent(id: AgentId, nowMs: number, state: StatusState): DiariumLine {
  throw new Error("not implemented");
}

/** `announce` event: the agent performed an announcement (event_data null). */
export function announceEvent(id: AgentId, nowMs: number): DiariumLine {
  throw new Error("not implemented");
}

/** `inreq` event: an archived incoming exchange with its final status and four envelopes. */
export function inreqEvent(id: AgentId, nowMs: number, finalStatus: InreqFinalStatus, envelopes: Exchange): DiariumLine {
  throw new Error("not implemented");
}

/** `outreq` event: an outgoing exchange with its final status and four envelopes. */
export function outreqEvent(id: AgentId, nowMs: number, finalStatus: OutreqFinalStatus, envelopes: Exchange): DiariumLine {
  throw new Error("not implemented");
}

/** `log` event: any message (e.g. ignored files, D-15). */
export function logEvent(id: AgentId, nowMs: number, message: string): DiariumLine {
  throw new Error("not implemented");
}
