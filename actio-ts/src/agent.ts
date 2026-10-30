// ActioAgent (WP7): lifecycle steps 1–5, lane C wiring, commands, and the public API of §2.7.
import type {
  ActioEvent,
  AgentId,
  AgentOptions,
  AgentView,
  CapabilityHandler,
  CommandContext,
  CommandHandler,
  RequestOutcome,
  RequestOptions,
} from "./types.js";
import type { Capability } from "./generated/types.js";

/** A team member: publishes spec/status, serves requests and commands, issues requests. */
export class ActioAgent {
  constructor(options: AgentOptions) {
    void options;
    throw new Error("not implemented");
  }

  /** Registers a capability and its handler; throws on a duplicate name or a non-strict schema (D-19). */
  capability(capability: Capability, handler: CapabilityHandler): void {
    throw new Error("not implemented");
  }

  /** Registers a custom command handler; stop/enable/restart/ping are built in. */
  onCommand(name: string, handler: (params: unknown, ctx: CommandContext) => Promise<void>): void {
    throw new Error("not implemented");
  }

  /** Subscribes to the observability event stream (every protocol event, incl. ignored files). */
  on(event: "event", handler: (e: ActioEvent) => void): void {
    throw new Error("not implemented");
  }

  /** Lifecycle steps 1–4, then the event loop (5); validates config (sync provider, D-21). */
  start(): Promise<void> {
    throw new Error("not implemented");
  }

  /** The current Agmen view (Map<AgentId, AgentView> after the callability rules). */
  agmen(): Map<AgentId, AgentView> {
    throw new Error("not implemented");
  }

  /** Issues a request (lane B); rejects locally with ActioCallError if not callable. */
  request(dst: AgentId, name: string, params: unknown, options?: RequestOptions): Promise<RequestOutcome> {
    throw new Error("not implemented");
  }

  /** Issues a command (lane B, type "cmd"); returns the rid; `outreq` "sent". */
  command(dst: AgentId, name: string, params?: unknown): Promise<string> {
    throw new Error("not implemented");
  }

  /** Status "error" + announce (no recovery; spec note after the workflow). */
  fail(message: string): Promise<void> {
    throw new Error("not implemented");
  }

  /** Lane C (D-9): in-flight handling, error 300 for unanswered accepted requests, C1 → C2. */
  stop(options?: { force?: boolean }): Promise<void> {
    throw new Error("not implemented");
  }
}
