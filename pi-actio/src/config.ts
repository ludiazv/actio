// One JSON file: its schema, defaults, and the computed agent id (D-1…D-5).
// A missing file means all defaults; an invalid file means "register nothing" (D-2).
import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { basename, join } from "node:path";
import { Type } from "typebox";
import { Value } from "typebox/value";

export interface Config {
  agentId: string;
  namespaces?: string[];
  actioDir?: string;
  exposePrompt: boolean;
  acceptPrompt: string[];
  disabled: boolean;
  json: boolean;
  reportActivity: boolean;
}

/** The TypeBox schema of the file; every key optional, no extras (D-1…D-5). */
const Schema = Type.Object(
  {
    agentId: Type.Optional(Type.String({ pattern: "^[a-zA-Z0-9_-]{1,32}$" })),
    namespaces: Type.Optional(Type.Array(Type.String())),
    actioDir: Type.Optional(Type.String()),
    exposePrompt: Type.Optional(Type.Boolean()),
    acceptPrompt: Type.Optional(Type.Array(Type.String({ pattern: "^[a-zA-Z0-9_-]{1,32}$" }))),
    disabled: Type.Optional(Type.Boolean()),
    json: Type.Optional(Type.Boolean()),
    reportActivity: Type.Optional(Type.Boolean()),
  },
  { additionalProperties: false },
);

/** The agent directory pi reads extensions from: `$PI_CODING_AGENT_DIR` or `~/.pi/agent`. */
function agentDir(): string {
  return process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent");
}

/** Absolute path of the configuration file (D-1). */
export function configPath(): string {
  return join(agentDir(), "actio.json");
}

/** Expands a leading `~` or `~/` to the home directory (D-5). */
export function expandHome(dir: string): string {
  if (dir === "~") return homedir();
  if (dir.startsWith("~/")) return join(homedir(), dir.slice(2));
  return dir;
}

/** `<cwd basename>-<4 hex>`, protocol-safe, kept per process and cwd (D-4). */
export function defaultAgentId(cwd: string): string {
  const key = Symbol.for("pi-actio.agentIds");
  const g = globalThis as Record<symbol, Map<string, string> | undefined>;
  const store: Map<string, string> = g[key] ?? new Map();
  g[key] = store;
  const kept = store.get(cwd);
  if (kept !== undefined) return kept;
  const base = basename(cwd).replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 27) || "pi";
  const id = `${base}-${randomBytes(2).toString("hex")}`;
  store.set(cwd, id);
  return id;
}

/** Reads and validates the file; throws `Error("<path>: <first error>")` (D-2). */
export function loadConfig(file?: string, cwd?: string): Config {
  const path = file ?? configPath();
  const dir = cwd ?? process.cwd();
  let raw: string;
  try {
    raw = readFileSync(path, "utf8");
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") {
      // D-1: a missing file means all defaults.
      return {
        agentId: defaultAgentId(dir),
        exposePrompt: false,
        acceptPrompt: [],
        disabled: false,
        json: false,
        reportActivity: false,
      };
    }
    throw err;
  }
  let data: unknown;
  try {
    data = JSON.parse(raw);
  } catch (err) {
    throw new Error(`${path}: ${(err as Error).message}`);
  }
  if (!Value.Check(Schema, data)) {
    const first = Value.Errors(Schema, data)[0];
    throw new Error(`${path}: ${first.instancePath} ${first.message}`);
  }
  const d = data as {
    agentId?: string;
    namespaces?: string[];
    actioDir?: string;
    exposePrompt?: boolean;
    acceptPrompt?: string[];
    disabled?: boolean;
    json?: boolean;
    reportActivity?: boolean;
  };
  return {
    agentId: d.agentId ?? defaultAgentId(dir),
    namespaces: d.namespaces,
    actioDir: d.actioDir,
    exposePrompt: d.exposePrompt ?? false,
    acceptPrompt: d.acceptPrompt ?? [],
    disabled: d.disabled ?? false,
    json: d.json ?? false,
    reportActivity: d.reportActivity ?? false,
  };
}