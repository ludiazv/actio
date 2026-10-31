// Spec builder (WP7): turns the agent options and the registered capabilities into a valid spec.json.
import { toIso } from "./util.js";
import { validateSpec } from "./validate.js";
import type { Capability, Spec } from "./generated/types.js";
import type { AgentOptions } from "./types.js";

/**
 * Builds a spec.json that passes validateSpec: core profile security fields, fresh datetime,
 * capabilities in registration order; throws on duplicate capability names.
 */
export function buildSpec(options: AgentOptions, capabilities: Capability[], nowMs: number): Spec {
  // Duplicates are ours to reject: the schema describes one capability, not the uniqueness of a name
  // across the list, and a duplicated name would make the Agmen view (WP4) silently lose one.
  const names = new Set<string>();
  for (const cap of capabilities) {
    if (names.has(cap.name)) {
      throw new Error(`spec-builder: duplicate capability name "${cap.name}" in agent "${options.id}"`);
    }
    names.add(cap.name);
  }

  const spec: Spec = {
    id: options.id,
    actio_version: "0.1", // *Version*: the only version this implementation speaks
    datetime: toIso(nowMs), // "Last time the spec was updated": the injected clock, never Date.now()
    description: options.description,
    // An empty list means nobody may command us (*Agent Spec*); the array is copied so a later change
    // to the options cannot alter what we published.
    accepts_commands_from: [...options.acceptsCommandsFrom],
    capabilities: [...capabilities], // registration order, the objects as given (WP4 reads their `actio`)
    security_support: [], // core profile: no signatures, no encryption
    security_posture: "trusted", // core profile: trust the shared tree (D-15, no issuer path)
  };

  // Optional fields appear only when given: absent, null and [] are three legal ways to say "the null
  // namespace" (*Namespaces*), so the builder must not invent a value the operator did not write.
  if (options.namespaces !== undefined) spec.namespaces = options.namespaces;
  if (options.depends !== undefined) spec.depends = [...options.depends];
  if (options.wants !== undefined) spec.wants = [...options.wants];
  // `info` is free content: the schema only requires an object (the self-validation below rejects the
  // rest), it is not validated field by field, and it is carried by reference, like the capabilities.
  if (options.info !== undefined) spec.info = options.info as Spec["info"];
  // `disabled` is never written here: it lives in spec.json, the `enable` command (D2) is what rewrites
  // it during a run, and a fresh start clears it again (D-10). `security` stays absent too: core has no keys.

  const checked = validateSpec(spec); // I-2: a failing build is our bug, and it must not reach the tree
  if (!checked.ok) {
    throw new Error(`spec-builder: invalid spec for agent "${options.id}": ${checked.errors.join("; ")}`);
  }
  return spec;
}
