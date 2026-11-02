// Rendering of tool results: Markdown for humans, JSON for `json: true` (D-20).
import type { AgentView, RequestOutcome } from "actio-ts";

/** `list-team` output (D-8: the view never contains this agent). */
export function formatTeam(views: AgentView[], json: boolean): string {
  void views;
  void json;
  throw new Error("not implemented: formatTeam");
}

/** `info <id> [<capability>]` output; throws on an unknown capability. */
export function formatInfo(view: AgentView, capability: string | undefined, json: boolean): string {
  void view;
  void capability;
  void json;
  throw new Error("not implemented: formatInfo");
}

/** `call <id>/<capability>` output; a peer error answer is a normal result (D-11). */
export function formatCall(target: string, outcome: RequestOutcome, json: boolean): string {
  void target;
  void outcome;
  void json;
  throw new Error("not implemented: formatCall");
}
