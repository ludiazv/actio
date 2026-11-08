// Public API and internal types of actio-ts. Protocol shapes come from the generated types.
import type { CompiledCapability } from "./validate.js";
import type {
  AgentId,
  Capability,
  DiariumLine,
  ResponseBody,
  Rid,
  Spec,
  Status,
} from "./generated/types.js";

export * from "./generated/types.js";

/** Health derived from a peer's status age (*Agent's status* rules 4–8); undefined for inactive/error. */
export type Health = "healthy" | "online" | "degraded" | "zombie" | undefined;

/** The four mandatory status states, read off the generated status schema (*Agent's status* rule 1). */
export type StatusState = Status["status"];

/** Final status of an incoming exchange (receiver side, diarium `inreq`). */
export type InreqFinalStatus = "cancelled" | "completed" | "confirmed" | "dropped";

/** Final status of an outgoing exchange (requestor side, diarium `outreq`). */
export type OutreqFinalStatus = "confirmed" | "cancelled" | "abandoned" | "sent";

/** A peer agent as seen after the callability rules (*Callability rules* 1–8). */
export interface AgentView {
  id: AgentId;
  /** Absolute path of the peer's agent folder. */
  folder: string;
  spec: Spec;
  status: Status;
  health: Health;
  /** True if the peer's `accepts_commands_from` lists us (rule 5.1). */
  commands: boolean;
  /** Capabilities usable by us after the disabled/`accepts_from` filter (rule 5.2). */
  capabilities: Map<string, Capability>;
}

/** Observability event: every protocol event, including ignored files. */
export type ActioEvent =
  | { event: "status"; state: StatusState }
  | { event: "announce"; text: string }
  | { event: "inreq"; rid: Rid; finalStatus: InreqFinalStatus }
  | { event: "outreq"; rid: Rid; finalStatus: OutreqFinalStatus }
  | { event: "log"; message: string }
  | { event: "ignored"; path: string; reason: string };

/** Capability error thrown by a handler; `code >= 1000` per *Acceptance errors*. */
export class ActioError extends Error {
  readonly code: number;
  readonly details?: unknown;
  constructor(code: number, message: string, details?: unknown) {
    super(message);
    this.name = "ActioError";
    this.code = code;
    this.details = details;
  }
}

/** Local rejection of `request()`/`command()` by the callability rules; nothing is written. */
export class ActioCallError extends Error {
  readonly dst: AgentId;
  readonly callName: string;
  readonly reason: string;
  constructor(dst: AgentId, callName: string, reason: string) {
    super(`cannot call ${dst}/${callName}: ${reason}`);
    this.name = "ActioCallError";
    this.dst = dst;
    this.callName = callName;
    this.reason = reason;
  }
}

/** The exchange ended, but the response `result` failed the capability `result` schema (D-12). */
export class ActioResultError extends Error {
  readonly response: ResponseBody;
  constructor(message: string, response: ResponseBody) {
    super(message);
    this.name = "ActioResultError";
    this.response = response;
  }
}

/** Storage synchronization provider. */
export interface SyncProvider {
  /** io.ts: fsync the written file before the rename and its parent folder after it. */
  fsync: boolean;
  /** io.ts: await this delay after each write, before returning (0 = none). */
  delayMs: number;
  notify(): void;
  flush(): Promise<void>;
  close(): Promise<void>;
}

/** All timers of one agent, in milliseconds; tests inject a fake `clock`. */
export interface ActioConfig {
  root: string;
  /** Raw `ACTIO_SYNC` value; parsed into a SyncProvider at `start()` (D-21). */
  sync: string;
  heartbeatInterval: number;
  confirmTimeout: number;
  cancelWaitTimeout: number;
  announceDelay: number;
  nuntiumDebounce: number;
  stopGrace: number;
  clock: () => number;
}

/** Options of `new ActioAgent(...)`. */
export interface AgentOptions {
  id: AgentId;
  description: string;
  /** Absent = not set in spec.json; null = explicit null; [] = empty; ["*"] = all namespaces.
   * Items may be strings or the JSON literal null (the void namespace) — widened in I-9. */
  namespaces?: (string | null)[] | null;
  depends?: string[];
  wants?: string[];
  acceptsCommandsFrom: string[];
  info?: Record<string, unknown>;
  config?: Partial<ActioConfig>;
}

/** Options of `agent.request(...)`. */
export interface RequestOptions {
  /** Requestor timeout in seconds; 0 = wait forever unless `signal` aborts (D-6). */
  timeout?: number;
  /** Filesystem paths of files to copy into `forum` under their basename (B1). */
  attachments?: string[];
  signal?: AbortSignal;
}

/** Result of `agent.request(...)`; `response` is present whenever one was received and confirmed. */
export interface RequestOutcome {
  finalStatus: "confirmed" | "cancelled" | "abandoned";
  response?: ResponseBody;
}

/** Context passed to a capability handler. */
export interface CapabilityContext {
  rid: Rid;
  src: AgentId;
  signal: AbortSignal;
  /** Request attachments, as paths relative to `forum`. */
  attachments: string[];
  /** Writes `data` into `forum` and returns its forum-relative name for the response. */
  publishAttachment(name: string, data: Uint8Array): Promise<string>;
}

/** Context passed to a command handler; commands cannot be cancelled and are never answered. */
export interface CommandContext {
  rid: Rid;
  src: AgentId;
}

export type CapabilityHandler = (params: unknown, ctx: CapabilityContext) => Promise<unknown>;
export type CommandHandler = (params: unknown, ctx: CommandContext) => Promise<void>;

/** A registered capability plus its live state (receiver bookkeeping for `capability_status`).
 * `compiled` holds the D-19 validators the agent compiled at `capability()` registration. */
export interface CapabilityEntry {
  capability: Capability;
  compiled: CompiledCapability;
  handler: CapabilityHandler;
  running: number;
}

/** The folders of one agent inside the `.actio` root (all absolute). */
export interface AgentFolders {
  root: string;
  agent: string;
  inbox: string;
  forum: string;
  nuntium: string;
}

/** Internal wiring shared by the protocol modules; created by ActioAgent (no DI container). */
export interface AgentContext {
  config: ActioConfig;
  id: AgentId;
  folders: AgentFolders;
  sync: SyncProvider;
  now(): number;
  /** The currently published spec. */
  spec(): Spec;
  /** The current status state. */
  state(): StatusState;
  /** Observability event stream (`agent.on("event", ...)`). */
  emit(event: ActioEvent): void;
  /** Appends a validated diarium line (throws on invalid events: a bug, not a peer, produced it). */
  appendDiarium(event: DiariumLine): Promise<void>;
}
