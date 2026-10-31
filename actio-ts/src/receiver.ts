// Receiver (WP5): lanes A (requests) and D (commands) — inbox dispatch, per-rid exchange
// tracker, acceptance checks, work with AbortSignal, response, confirmation wait, archive.
import type { AgentContext, AgentId, AgentView, CapabilityEntry, CommandHandler } from "./types.js";

/** Lifecycle callbacks the receiver hands over to the ActioAgent (wired in WP7). */
export interface ReceiverHooks {
  /** Watch error on the own inbox (D-22): the agent logs and fails itself. */
  onWatchError(dir: string, err: Error): void;
  /** The Agmen view of a peer (D-7 code 106); undefined when the requestor is unknown. */
  peerOf(src: AgentId): AgentView | undefined;
  /** A busy/idle-relevant exchange state changed (D-8). */
  onStateChange(): void;
  /** Built-in commands that change the lifecycle; the receiver archives first (D3). */
  onCommandStop(force: boolean): Promise<void>;
  onCommandRestart(force: boolean, delaySeconds: number): Promise<void>;
  onCommandEnable(enabled: boolean): Promise<void>;
  onCommandPing(): Promise<void>;
}

/** The receiver of one agent: serves inbound requests and commands from its own inbox. */
export class Receiver {
  constructor(
    private readonly ctx: AgentContext,
    private readonly hooks: ReceiverHooks,
    private readonly capabilities: Map<string, CapabilityEntry>,
    private readonly commands: Map<string, CommandHandler>,
  ) {
    throw new Error("not implemented");
  }

  /** Arms the inbox watcher and processes pending envelopes (D-4); every file event → `onFile`. */
  start(onFile: (name: string) => void): Promise<void> {
    throw new Error("not implemented");
  }

  /** Dispatches one inbox file (schema validation first, D-15; header consistency, D-11). */
  processFile(name: string): Promise<void> {
    throw new Error("not implemented");
  }

  /** Closes the watcher, aborts in-flight work per D-9 (stop semantics are the agent's). */
  stop(): Promise<void> {
    throw new Error("not implemented");
  }

  /** Number of inbound exchanges in A1/A2/A3 (for busy, D-8) and capability_status. */
  runningCount(): number {
    throw new Error("not implemented");
  }
}
