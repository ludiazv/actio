// Discovery and callability: folder scan, the callability rules 1–8, and the Agmen view of one peer.
import fsp from "node:fs/promises";
import path from "node:path";
import * as io from "./io.js";
import { health } from "./status.js";
import { uniqueCapabilityNames, validateSpec, validateStatus } from "./validate.js";
import type { Capability, Spec, Status } from "./generated/types.js";
import type { AgentId, AgentView } from "./types.js";

/** Folder names that are never agent folders (*Announcement and Discovery* scan rule 1). */
const RESERVED = new Set(["tools", "forum", "nuntium"]);

/** A valid agent folder found by the scan (rules 1–4 of *Announcement and Discovery*). */
export interface DiscoveredAgent {
  folder: string;
  spec: Spec;
  status: Status;
}

/** What the scan reports besides the valid agents (the caller turns these into log events). */
export interface ScanResult {
  agents: DiscoveredAgent[];
  /** Folders skipped because their ID was already seen (D-3, first in lexical order wins). */
  duplicates: string[];
  /** Files/folders ignored for schema or structure reasons (rule 3), with a reason each. */
  ignored: { path: string; reason: string }[];
}

/** True for a directory (a link to one counts); a vanished entry or a file is not. */
async function isDirectory(target: string): Promise<boolean> {
  try {
    return (await fsp.stat(target)).isDirectory();
  } catch {
    return false; // the folder disappeared between listing and stat: nothing to discover
  }
}

/** One of the two publication files: `readJson` reports a missing file and an unparsable one the
 * same way, while the scan ignores the first silently (rule 2) and reports the second (rule 3). */
type Publication = { kind: "missing" } | { kind: "unparsable" } | { kind: "json"; value: unknown };

async function readPublication(file: string): Promise<Publication> {
  const value = await io.readJson(file);
  if (value !== undefined) return { kind: "json", value };
  return (await io.exists(file)) ? { kind: "unparsable" } : { kind: "missing" };
}

/** First Ajv text of a rejection; a rejection always has one, so the fallback never surfaces. */
const firstError = (errors: string[]): string => errors[0] ?? "does not validate";

/**
 * The namespaces an agent belongs to (*Agent Spec*): absent, `null` or an empty list is the `null`
 * namespace; a filled list does NOT include it unless `null` is listed explicitly; `"*"` (alone or
 * combined with other values) is every namespace.
 */
function namespacesOf(spec: Spec): { every: boolean; names: Set<string | null> } {
  const listed = spec.namespaces;
  if (listed === undefined || listed === null || listed.length === 0) return { every: false, names: new Set([null]) };
  const names = new Set<string | null>();
  for (const ns of listed) {
    if (ns === "*") return { every: true, names }; // redundant with the rest, but valid
    names.add(ns);
  }
  return { every: false, names };
}

/** Rule 2: two agents share a namespace when one of them belongs to all of them, or their lists meet. */
function shareNamespace(self: Spec, peer: Spec): boolean {
  const mine = namespacesOf(self);
  const theirs = namespacesOf(peer);
  if (mine.every || theirs.every) return true;
  for (const ns of mine.names) if (theirs.names.has(ns)) return true;
  return false;
}

/** Rule 5.2: a capability is usable unless `actio.disabled` is true, or `actio.accepts_from` excludes
 * us; an absent or empty `accepts_from` means everybody. Both live in the capability's `actio` object. */
function capabilityUsable(self: AgentId, capability: Capability): boolean {
  if (capability.actio.disabled === true) return false;
  const acceptsFrom = capability.actio.accepts_from;
  return acceptsFrom === undefined || acceptsFrom.length === 0 || acceptsFrom.includes(self);
}

/** Scans the agent folders of the root (reserved names skipped; invalid material reported). */
export async function scan(root: string): Promise<ScanResult> {
  const names = (await io.listDir(root)).sort(); // lexical: it decides which duplicate wins (D-3)
  const agents: DiscoveredAgent[] = [];
  const duplicates: string[] = [];
  const ignored: { path: string; reason: string }[] = [];
  const seen = new Set<AgentId>();
  for (const name of names) {
    if (name.startsWith(".") || RESERVED.has(name)) continue; // hidden folders are never agents
    const folder = path.join(root, name);
    if (!(await isDirectory(folder))) continue; // `actio.json` and the like are not agent folders
    const specFile = path.join(folder, "spec.json");
    const statusFile = path.join(folder, "status.json");
    const specRead = await readPublication(specFile);
    const statusRead = await readPublication(statusFile);
    if (specRead.kind === "missing" || statusRead.kind === "missing") continue; // rule 2: no publication
    if (specRead.kind === "unparsable") {
      ignored.push({ path: specFile, reason: "not valid JSON" });
      continue;
    }
    if (statusRead.kind === "unparsable") {
      ignored.push({ path: statusFile, reason: "not valid JSON" });
      continue;
    }
    const spec = validateSpec(specRead.value); // rule 3: a non-conforming pair is ignored, and said so
    if (!spec.ok) {
      ignored.push({ path: specFile, reason: firstError(spec.errors) });
      continue;
    }
    // Rule 3 covers the rules the schemas cannot express: a spec that repeats a capability name is
    // ignored like a schema failure, and reported with the name it repeats (*Schemas* rules).
    const repeated = uniqueCapabilityNames(spec.value);
    if (repeated !== undefined) {
      ignored.push({ path: specFile, reason: `duplicate capability name "${repeated}"` });
      continue;
    }
    const status = validateStatus(statusRead.value);
    if (!status.ok) {
      ignored.push({ path: statusFile, reason: firstError(status.errors) });
      continue;
    }
    if (seen.has(spec.value.id)) {
      duplicates.push(folder); // D-3: the first folder in lexical order keeps the ID
      continue;
    }
    seen.add(spec.value.id);
    agents.push({ folder, spec: spec.value, status: status.value }); // rule 4: the pair is the view
  }
  return { agents, duplicates, ignored };
}

/** Applies the callability rules 1–8 for `self` looking at `peer`; undefined = discarded. */
export function callable(self: Spec, peer: DiscoveredAgent, nowMs: number): AgentView | undefined {
  const peerSpec = peer.spec;
  // Rule 1: a disabled peer offers no capabilities; commands still reach it, so it can be enabled
  // again. Only new requests are affected — accepted work continues (the receiver decides that).
  const usable = peerSpec.disabled === true ? [] : peerSpec.capabilities.filter((c) => capabilityUsable(self.id, c));
  // Rule 2: no namespace in common → the spec is not considered at all.
  if (!shareNamespace(self, peerSpec)) return undefined;
  // Rules 3 and 4: this implementation supports the core profile only — the `trusted` posture. A peer
  // requiring any other posture needs security features we do not have, and the zero-trust issuer
  // possession check is unreachable for the same reason.
  if (peerSpec.security_posture !== "trusted") return undefined;
  // Rule 5.1: an empty `accepts_commands_from` means nobody.
  const commands = peerSpec.accepts_commands_from.includes(self.id);
  // Rule 6: nothing usable → the agent is discarded.
  if (!commands && usable.length === 0) return undefined;
  // Rule 7: an inactive or error agent will not respond.
  if (peer.status.status === "inactive" || peer.status.status === "error") return undefined;
  // Rule 8: health is a flag (degraded/zombie stay in the view), never a filter.
  return {
    id: peerSpec.id,
    folder: peer.folder,
    spec: peerSpec,
    status: peer.status,
    health: health(peer.status, nowMs),
    commands,
    capabilities: new Map(usable.map((c) => [c.name, c])),
  };
}
