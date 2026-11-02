// The single `actio` tool: list-team, info, call (three commands, nothing else).
import type { ToolDefinition } from "@earendil-works/pi-coding-agent";
import type { Config } from "./config.js";
import type { Team } from "./team.js";

export function actioTool(team: Team, config: Config): ToolDefinition {
  void team;
  void config;
  throw new Error("not implemented: actioTool");
}
