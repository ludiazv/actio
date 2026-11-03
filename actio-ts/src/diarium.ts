// Diarium: append + typed event builders + the read-only view of a diarium file.
// append() validates with validateDiarium before writing (a bug, not a peer, produced the event → throw).
import fsp from "node:fs/promises";
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

/** The read-only view of a diarium file: the valid lines, and the ignored lines with a reason each. */
export interface DiariumRead {
  lines: DiariumLine[];                         // the valid lines, in file order
  ignored: { line: number; reason: string }[];  // 1-based; "not valid JSON" or the first Ajv error
}

/** Reads `<folder>/diarium.jsonl` (an agent folder as `AgentView.folder` gives it, never `root/<id>`).
 * A missing file gives the empty result; any other read error rejects. Blank lines are skipped, and a
 * last line without a trailing newline is skipped and not reported: an append may be in progress.
 * Each line goes through `JSON.parse` and then `validateDiarium`; `event_data` is not narrowed (the
 * schema does not enforce it per event, and another agent's file is untrusted) — consumers narrow on
 * `event`. There are no filter options: filtering is the caller's job, and protocol `datetime`
 * strings compare correctly as plain strings. */
export async function read(folder: string): Promise<DiariumRead> {
  let raw: string;
  try {
    raw = await fsp.readFile(path.join(folder, "diarium.jsonl"), "utf8");
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return { lines: [], ignored: [] };
    throw err;
  }
  const lines: DiariumLine[] = [];
  const ignored: DiariumRead["ignored"] = [];
  const parts = raw.split("\n");
  const endsWithNewline = raw.endsWith("\n");
  for (let i = 0; i < parts.length; i++) {
    const text = parts[i];
    if (i === parts.length - 1 && !endsWithNewline) continue; // the partial last line: an append in progress
    if (text.trim() === "") continue; // blank lines carry no event
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      ignored.push({ line: i + 1, reason: "not valid JSON" });
      continue;
    }
    const checked = validateDiarium(parsed);
    if (checked.ok) lines.push(checked.value);
    else ignored.push({ line: i + 1, reason: checked.errors[0] });
  }
  return { lines, ignored };
}
