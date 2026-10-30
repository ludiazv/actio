// Public API barrel of actio-ts (§2.7). Internal modules are reached via deep imports.
export { ActioAgent } from "./agent.js";
export { ActioCallError, ActioError, ActioResultError } from "./types.js";
export { loadConfig } from "./config.js";
export type {
  ActioConfig,
  ActioEvent,
  AgentFolders,
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
