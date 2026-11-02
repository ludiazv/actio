// Public API barrel of actio-ts (§2.7). Internal modules are reached via deep imports.
export { ActioAgent } from "./agent.js";
export { ActioCallError, ActioError, ActioResultError } from "./types.js";
export { loadConfig } from "./config.js";
export { checkParams } from "./validate.js";
export type {
  ActioConfig,
  ActioEvent,
  AgentFolders,
  AgentId,
  AgentOptions,
  AgentView,
  CapabilityContext,
  CapabilityEntry,
  CapabilityHandler,
  CommandContext,
  CommandHandler,
  Health,
  InreqFinalStatus,
  OutreqFinalStatus,
  RequestOptions,
  RequestOutcome,
  StatusState,
  SyncProvider,
} from "./types.js";
export type { Capability, ResponseBody } from "./generated/types.js";
