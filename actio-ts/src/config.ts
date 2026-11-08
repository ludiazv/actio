// ActioConfig: defaults, env reading and the injectable clock. Durations are milliseconds
// internally; the env overrides of the protocol timers are in seconds (spec *Timers and thresholds*).
import type { ActioConfig } from "./types.js";

/** Protocol defaults (root and clock are always resolved, never defaulted here). */
export const defaultConfig: Omit<ActioConfig, "root" | "clock"> = {
  sync: "none",
  heartbeatInterval: 30_000,
  confirmTimeout: 120_000,
  cancelWaitTimeout: 60_000,
  announceDelay: 100,
  nuntiumDebounce: 1_000,
  stopGrace: 30_000,
};

/** Reads one positive duration env var in seconds, returns milliseconds; throws when unparsable. */
function envSeconds(name: string): number | undefined {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return undefined;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) throw new Error(`${name} must be a positive number of seconds, got "${raw}"`);
  return Math.round(value * 1000);
}

/** Builds the effective config: defaults < environment < overrides. Throws on bad env values. */
export function loadConfig(overrides?: Partial<ActioConfig>): ActioConfig {
  const root = process.env["ACTIO_DIR"] ?? `${process.env["HOME"] ?? ""}/.actio`;
  const sync = process.env["ACTIO_SYNC"] ?? defaultConfig.sync;
  const fromEnv: Partial<ActioConfig> = {
    root,
    sync,
    heartbeatInterval: envSeconds("ACTIO_HEARTBEAT"),
    confirmTimeout: envSeconds("ACTIO_CONFIRM_TIMEOUT"),
    cancelWaitTimeout: envSeconds("ACTIO_CANCEL_WAIT"),
  };
  const merged: ActioConfig = {
    ...defaultConfig,
    ...Object.fromEntries(Object.entries(fromEnv).filter(([, v]) => v !== undefined)),
    ...overrides,
    clock: overrides?.clock ?? Date.now,
  } as ActioConfig;
  if (typeof merged.root !== "string" || merged.root === "") throw new Error("config.root must be a non-empty string");
  if (typeof merged.sync !== "string") throw new Error("config.sync must be a string");
  for (const key of ["heartbeatInterval", "confirmTimeout", "cancelWaitTimeout", "announceDelay", "nuntiumDebounce", "stopGrace"] as const) {
    if (!(Number.isFinite(merged[key]) && merged[key] >= 0)) throw new Error(`config.${key} must be a number >= 0 (ms)`);
  }
  if (typeof merged.clock !== "function") throw new Error("config.clock must be a function");
  return merged;
}
