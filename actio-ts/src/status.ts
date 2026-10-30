// Self-publication (WP3): spec.json/status.json publication, the announce file, the heartbeat
// timer and the health derivation (*Agent's status*).
import type { Spec, Status } from "./generated/types.js";
import type { AgentId, Health, StatusState } from "./types.js";

/** One entry of the mandatory `capability_status` list of status.json. */
export type CapabilityStatusEntry = Status["capability_status"][number];

/** Builds a valid status object (id, state, fresh datetime, capability_status). */
export function buildStatus(id: AgentId, state: StatusState, nowMs: number, capabilityStatus?: CapabilityStatusEntry[]): Status {
  throw new Error("not implemented");
}

/** Writes spec.json (atomic; the caller publishes status before re-announcing). */
export function publishSpec(folder: string, spec: Spec): Promise<void> {
  throw new Error("not implemented");
}

/** Writes status.json (atomic; immediate on change, and on every heartbeat tick). */
export function publishStatus(folder: string, status: Status): Promise<void> {
  throw new Error("not implemented");
}

/** Writes `nuntium/<id>.n.json` and appends the diarium announce event. The caller applies the
 * announceDelay between its status write and this call (*Agent's status* rule 2). */
export function announce(folder: string, id: AgentId, text: string, nowMs: number): Promise<void> {
  throw new Error("not implemented");
}

/** Health of a status at `nowMs`: healthy|online|degraded|zombie; undefined for inactive/error. */
export function health(status: Status, nowMs: number): Health {
  throw new Error("not implemented");
}

/** Runs `tick` every `intervalMs` (heartbeat); `stop()` cancels the timer. */
export function startHeartbeat(intervalMs: number, tick: () => Promise<void> | void): { stop(): void } {
  throw new Error("not implemented");
}
