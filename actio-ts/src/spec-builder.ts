// Spec builder (WP7): turns the agent options and the registered capabilities into a valid spec.json.
import type { Capability, Spec } from "./generated/types.js";
import type { AgentOptions } from "./types.js";

/**
 * Builds a spec.json that passes validateSpec: core profile security fields, fresh datetime,
 * capabilities in registration order; throws on duplicate capability names.
 */
export function buildSpec(options: AgentOptions, capabilities: Capability[], nowMs: number): Spec {
  throw new Error("not implemented");
}
