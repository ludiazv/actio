// In-memory records of what this session did and received, plus change listeners (D-19).
// Oldest first; lost when the runtime is replaced.

export type CallState = "in-flight" | "confirmed" | "cancelled" | "abandoned" | "failed";

export interface CallRecord {
  target: string;
  startedAt: number;
  endedAt?: number;
  state: CallState;
  code?: number;
  error?: string;
}

export interface PromptRecord {
  at: number;
  src: string;
  capability: string;
  promptId?: string;
  outcome: string;
}

export type HistoryEvent =
  | { kind: "call"; record: CallRecord }
  | { kind: "prompt"; record: PromptRecord };

export class History {
  readonly calls: CallRecord[] = [];
  readonly prompts: PromptRecord[] = [];

  /** Appends an `in-flight` record and emits it. */
  startCall(target: string): CallRecord {
    void target;
    throw new Error("not implemented: History.startCall");
  }

  /** Moves a record to its final state and emits it. */
  endCall(record: CallRecord, state: CallState, extra?: { code?: number; error?: string }): void {
    void record;
    void state;
    void extra;
    throw new Error("not implemented: History.endCall");
  }

  /** Appends a received prompt-service request and emits it. */
  addPrompt(record: Omit<PromptRecord, "at">): void {
    void record;
    throw new Error("not implemented: History.addPrompt");
  }

  /** Subscribes to events; returns the unsubscribe function. */
  onChange(listener: (e: HistoryEvent) => void): () => void {
    void listener;
    throw new Error("not implemented: History.onChange");
  }
}
