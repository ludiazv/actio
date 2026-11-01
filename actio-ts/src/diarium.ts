// Diarium: append + typed event builders. append() validates with validateDiarium before
// writing (a bug, not a peer, produced the event → throw).
import path from "node:path";
import { appendJsonl } from "./io.js";
import { toIso } from "./util.js";
import { validateDiarium } from "./validate.js";
import type { DiariumLine, Exchange } from "./generated/types.js";
import type { AgentId, InreqFinalStatus, OutreqFinalStatus, StatusState } from "./types.js";

/** Appends one event line to `<folder>/diarium.jsonl` after validating it. */
export async function append(folder: string, event: DiariumLine): Promise<void> {
  const checked = validateDiarium(event);
  if (!checked.ok) throw new Error(`diarium.ts: invalid "${event.event}" event: ${checked.errors.join("; ")}`);
  await appendJsonl(path.join(folder, "diarium.jsonl"), event);
}

/** One line: id, `datetime` from `nowMs`, the event name and its `event_data`. The generated
 * `DiariumLine.event_data` is an index-signature object, but the protocol allows any JSON value
 * (string, null, exchange) → cast at the boundary, and `append` validates before writing (I-2). */
function line(id: AgentId, nowMs: number, event: string, eventData: unknown): DiariumLine {
  return { id, datetime: toIso(nowMs), event, event_data: eventData } as DiariumLine;
}

/** `event_data` of `inreq`/`outreq`: the four envelopes as exchanged, with this final status. */
function exchangeData(finalStatus: string, envelopes: Exchange): Exchange {
  return { ...envelopes, final_status: finalStatus };
}

/** `status` event: change of the agent's status. */
export function statusEvent(id: AgentId, nowMs: number, state: StatusState): DiariumLine {
  return line(id, nowMs, "status", state);
}

/** `announce` event: the agent performed an announcement (event_data null). */
export function announceEvent(id: AgentId, nowMs: number): DiariumLine {
  return line(id, nowMs, "announce", null);
}

/** `inreq` event: an archived incoming exchange with its final status and four envelopes. */
export function inreqEvent(id: AgentId, nowMs: number, finalStatus: InreqFinalStatus, envelopes: Exchange): DiariumLine {
  return line(id, nowMs, "inreq", exchangeData(finalStatus, envelopes));
}

/** `outreq` event: an outgoing exchange with its final status and four envelopes. */
export function outreqEvent(id: AgentId, nowMs: number, finalStatus: OutreqFinalStatus, envelopes: Exchange): DiariumLine {
  return line(id, nowMs, "outreq", exchangeData(finalStatus, envelopes));
}

/** `log` event: any message (e.g. ignored files, D-15). */
export function logEvent(id: AgentId, nowMs: number, message: string): DiariumLine {
  return line(id, nowMs, "log", message);
}
