// The single `actio` tool: list-team, info, call (three commands, nothing else).
import { StringEnum } from "@earendil-works/pi-ai";
import type { ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import type { Config } from "./config.js";
import { formatCall, formatInfo, formatTeam } from "./format.js";
import type { Team } from "./team.js";

const parameters = Type.Object({
  command: StringEnum(["list-team", "info", "call"] as const),
  id: Type.Optional(Type.String({ description: "info: the agent id" })),
  capability: Type.Optional(Type.String({ description: "info: one capability name (default: all)" })),
  target: Type.Optional(Type.String({ description: "call: <agent id>/<capability name>" })),
  params: Type.Optional(Type.Unknown({ description: "call: the capability parameters; they must match its parameters schema (see info)" })),
});

/** The parameter values of the `actio` tool, as the schema above shapes them. */
interface ActioParams {
  command: "list-team" | "info" | "call";
  id?: string;
  capability?: string;
  target?: string;
  params?: unknown;
}

export function actioTool(team: Team, config: Config): ToolDefinition {
  return {
    name: "actio",
    label: "Actio",
    description:
      "Work with the other agents of your Actio team: list the team, read an agent's capabilities, and call one of its capabilities.",
    promptSnippet: "actio: list-team, info and call for the other agents of your Actio team",
    promptGuidelines: [
      "First `list-team`, then `info <id>` to read a capability's parameters, then `call`.",
      "Calls block until the peer answers, and Escape cancels.",
      "The prompt service of other pi agents returns a `prompt_id`, to be polled with `retrieve`.",
    ],
    parameters,
    async execute(_toolCallId, params, signal) {
      if (!team.running()) throw new Error("the actio agent is not running");
      const p = params as ActioParams;
      if (p.command === "list-team") {
        const views = await team.refresh();
        return { content: [{ type: "text", text: formatTeam(views, config.json) }], details: undefined };
      }
      if (p.command === "info") {
        if (p.id === undefined) throw new Error('info needs "id"');
        const view = team.view().find((v) => v.id === p.id);
        if (view === undefined) throw new Error(`no visible agent "${p.id}" (use list-team)`);
        return { content: [{ type: "text", text: formatInfo(view, p.capability, config.json) }], details: undefined };
      }
      // p.command === "call"
      if (p.target === undefined) throw new Error('call needs "target"');
      const outcome = await team.call(p.target, p.params, signal);
      return { content: [{ type: "text", text: formatCall(p.target, outcome, config.json) }], details: undefined };
    },
  };
}