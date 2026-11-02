// One JSON file: its schema, defaults, and the computed agent id (D-1…D-5).
// A missing file means all defaults; an invalid file means "register nothing" (D-2).

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

/** Absolute path of the configuration file (D-1). */
export function configPath(): string {
  throw new Error("not implemented: configPath");
}

/** Reads and validates the file; throws `Error("<path>: <first error>")` (D-2). */
export function loadConfig(file?: string, cwd?: string): Config {
  void file;
  void cwd;
  throw new Error("not implemented: loadConfig");
}

/** `<cwd basename>-<4 hex>`, protocol-safe (D-4). */
export function defaultAgentId(cwd: string): string {
  void cwd;
  throw new Error("not implemented: defaultAgentId");
}
