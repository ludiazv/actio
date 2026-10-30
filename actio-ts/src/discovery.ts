// Discovery and callability (WP4): folder scan, the callability rules 1–8, and the Agmen view.
import type { Spec, Status } from "./generated/types.js";
import type { AgentId, AgentView } from "./types.js";

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

/** Scans the agent folders of the root (reserved names skipped; invalid material reported). */
export function scan(root: string): Promise<ScanResult> {
  throw new Error("not implemented");
}

/** Applies the callability rules 1–8 for `self` looking at `peer`; undefined = discarded. */
export function callable(self: Spec, peer: DiscoveredAgent, nowMs: number): AgentView | undefined {
  throw new Error("not implemented");
}

/** Builds the whole Agmen view: scan + callability for every discovered agent (self included). */
export function buildAgmen(root: string, self: Spec, nowMs: number): Promise<Map<AgentId, AgentView>> {
  throw new Error("not implemented");
}
