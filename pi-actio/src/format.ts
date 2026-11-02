// Rendering of tool results: Markdown for humans, JSON for `json: true` (D-20).
import type { AgentView, Capability, RequestOutcome } from "actio-ts";

/** The capability names of a view, sorted. */
function capabilityNames(view: AgentView): string[] {
  return [...view.capabilities.keys()].sort();
}

/** The quoted, comma-joined capability names (no capabilities: empty string). */
function quoted(names: string[]): string {
  return names.map((n) => `"${n}"`).join(", ");
}

/** The spec namespaces as words; `null` is the void namespace; absent: "(none)". */
function namespacesText(namespaces: (string | null)[] | null | undefined): string {
  if (namespaces === undefined || namespaces === null) return "(none)";
  return namespaces.map((n) => (n === null ? "void" : n)).join(", ");
}

/** `list-team` output (D-8: the view never contains this agent). */
export function formatTeam(views: AgentView[], json: boolean): string {
  const sorted = [...views].sort((a, b) => a.id.localeCompare(b.id));
  if (json) {
    return JSON.stringify(
      sorted.map((v) => ({ id: v.id, description: v.spec.description, capabilities: capabilityNames(v) })),
      null,
      2,
    );
  }
  if (sorted.length === 0) return "No visible agents.";
  return sorted
    .map((v) => `- **${v.id}** — ${v.spec.description} — capabilities: ${quoted(capabilityNames(v))}`)
    .join("\n");
}

/** One `## <capability>` section: description, `max_parallel` when set, Parameters and Result. */
function capabilitySection(cap: Capability): string {
  const parts: string[] = [`## ${cap.name}`, cap.description];
  if (cap.actio.max_parallel !== undefined) parts.push(`max_parallel: ${cap.actio.max_parallel}`);
  parts.push(`Parameters\n\`\`\`json\n${JSON.stringify(cap.parameters, null, 2)}\n\`\`\``);
  parts.push(`Result\n\`\`\`json\n${JSON.stringify(cap.actio.result, null, 2)}\n\`\`\``);
  return parts.join("\n");
}

/** `info <id> [<capability>]` output; throws on an unknown capability. */
export function formatInfo(view: AgentView, capability: string | undefined, json: boolean): string {
  const names = capabilityNames(view);
  if (capability !== undefined && !view.capabilities.has(capability)) {
    throw new Error(`unknown capability "${capability}" for agent "${view.id}"; available: ${quoted(names)}`);
  }
  const caps = (capability === undefined ? names : [capability]).map((n) => view.capabilities.get(n)!);
  if (json) {
    return JSON.stringify(
      {
        id: view.id,
        description: view.spec.description,
        status: view.status.status,
        health: view.health,
        namespaces: view.spec.namespaces,
        capabilities: caps,
      },
      null,
      2,
    );
  }
  const health = view.health ?? "unknown";
  const head = `# ${view.id}\n${view.spec.description}\nStatus: ${view.status.status} · Health: ${health} · Namespaces: ${namespacesText(view.spec.namespaces)}`;
  return `${head}\n\n${caps.map(capabilitySection).join("\n\n")}`;
}

/** The `result` of a peer error answer (code, msg, details). */
function errorResult(response: { result: { [k: string]: unknown } }): { code?: number; msg?: string; details?: string } {
  return response.result as { code?: number; msg?: string; details?: string };
}

/** `call <id>/<capability>` output; a peer error answer is a normal result (D-11). */
export function formatCall(target: string, outcome: RequestOutcome, json: boolean): string {
  if (json) {
    return JSON.stringify({ target, finalStatus: outcome.finalStatus, response: outcome.response }, null, 2);
  }
  if (outcome.finalStatus === "cancelled") return `**${target}** → cancelled`;
  if (outcome.finalStatus === "abandoned") return `**${target}** → abandoned (no response)`;
  // confirmed
  const response = outcome.response;
  if (response === undefined) return `**${target}** → confirmed (encrypted response, not readable)`;
  if (response.error) {
    const err = errorResult(response);
    const head = `**${target}** → ${err.code !== undefined ? `error ${err.code}: ` : "error: "}${err.msg}`;
    return err.details !== undefined ? `${head}\n\ndetails:\n${err.details}` : head;
  }
  return `**${target}** → confirmed\n\n\`\`\`json\n${JSON.stringify(response.result, null, 2)}\n\`\`\``;
}