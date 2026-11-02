// The /actio overlay: the team, the outgoing calls, the received prompt requests (D-19).
import type { ExtensionCommandContext } from "@earendil-works/pi-coding-agent";
import type { History } from "./history.js";
import type { Team } from "./team.js";

export function openExplorer(
  ctx: ExtensionCommandContext,
  team: Team,
  history: History,
  promptOn: boolean,
): Promise<void> {
  void ctx;
  void team;
  void history;
  void promptOn;
  throw new Error("not implemented: openExplorer");
}
