// Self-publication: spec.json/status.json publication, the announce file, the heartbeat
// timer and the health derivation (*Agent's status*).
import path from "node:path";
import * as diarium from "./diarium.js";
import { makeDir, writeJsonAtomic } from "./io.js";
import { fromIso, toIso } from "./util.js";
import { validateAnnounce, validateSpec, validateStatus } from "./validate.js";
import type { Announce, Spec, Status } from "./generated/types.js";
import type { Validated } from "./validate.js";
import type { AgentId, Health, StatusState } from "./types.js";

// Health thresholds (*Agent's status* rules 4–7, *Timers and thresholds*): protocol constants, not
// config (plan §2.4). A status state whose age a peer can rate; the other two have no health (8).
const HEALTHY_MS = 60_000; // rule 4: updated less than 60 s ago
const ONLINE_MS = 100_000; // rule 5: 60–100 s (rule 3's ≤ 90 s heartbeat keeps peers out of degraded)
const DEGRADED_MS = 600_000; // rule 6: 100–600 s; rule 7: zombie only after MORE than 600 s

/** One entry of the mandatory `capability_status` list of status.json. */
export type CapabilityStatusEntry = Status["capability_status"][number];

/** Self-validation before a write: we produced the object, so a rejection is our bug (I-2) → throw. */
function mustValidate<T>(what: string, validated: Validated<T>): T {
  if (!validated.ok) throw new Error(`status.ts: invalid ${what}: ${validated.errors.join("; ")}`);
  return validated.value;
}

/** Builds a valid status object (id, state, fresh datetime, capability_status). */
export function buildStatus(id: AgentId, state: StatusState, nowMs: number, capabilityStatus?: CapabilityStatusEntry[]): Status {
  return { id, status: state, datetime: toIso(nowMs), capability_status: capabilityStatus ?? [] };
}

/** Writes spec.json (atomic; the caller publishes status before re-announcing). */
export async function publishSpec(folder: string, spec: Spec): Promise<void> {
  const validated = mustValidate("spec.json", validateSpec(spec));
  await writeJsonAtomic(path.join(folder, "spec.json"), validated, folder); // tmpDir = own agent folder (§2.5)
}

/** Writes status.json (atomic; immediate on change, and on every heartbeat tick). */
export async function publishStatus(folder: string, status: Status): Promise<void> {
  const validated = mustValidate("status.json", validateStatus(status));
  await writeJsonAtomic(path.join(folder, "status.json"), validated, folder);
}

/** Writes `<nuntiumDir>/<id>.n.json` (the shared signal folder at the `.actio` root) and appends the
 * diarium announce event to `<agentFolder>/diarium.jsonl` (I-5: two folders, one call). The caller
 * applies the announceDelay between its status write and this call (*Agent's status* rule 2). */
export async function announce(agentFolder: string, nuntiumDir: string, id: AgentId, text: string, nowMs: number): Promise<void> {
  const file: Announce = { id, datetime: toIso(nowMs), ts: Math.floor(nowMs / 1000), nuntium: text };
  const validated = mustValidate(`announce file for "${id}"`, validateAnnounce(file));
  await makeDir(nuntiumDir); // idempotent; writeJsonAtomic only creates its tmpDir
  await writeJsonAtomic(path.join(nuntiumDir, `${id}.n.json`), validated, agentFolder);
  await diarium.append(agentFolder, diarium.announceEvent(id, nowMs)); // records the completed announce
}

/** Health of a status at `nowMs`: healthy|online|degraded|zombie; undefined for inactive/error. */
export function health(status: Status, nowMs: number): Health {
  if (status.status !== "idle" && status.status !== "busy") return undefined; // rule 8
  const updated = fromIso(status.datetime);
  if (updated === undefined) return undefined; // unparsable datetime: the reader cannot rate it
  const ageMs = nowMs - updated; // a datetime in the future is the freshest possible
  if (ageMs < HEALTHY_MS) return "healthy";
  if (ageMs < ONLINE_MS) return "online";
  if (ageMs <= DEGRADED_MS) return "degraded";
  return "zombie";
}

/** Runs `tick` every `intervalMs` (heartbeat); `stop()` cancels the timer. */
export function startHeartbeat(intervalMs: number, tick: () => Promise<void> | void): { stop(): void } {
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;

  // The next tick is scheduled only after the previous one resolves: no overlap, and the freshness
  // interval is measured from the end of a write, never during it. A rejecting tick is not caught:
  // it surfaces to the caller (whose error policy it is) and the chain stops.
  const step = async (): Promise<void> => {
    await tick();
    if (stopped) return;
    timer = setTimeout(step, intervalMs);
  };
  timer = setTimeout(step, intervalMs); // the caller publishes the first status itself

  return {
    stop: () => {
      stopped = true;
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
    },
  };
}
