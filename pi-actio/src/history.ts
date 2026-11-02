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

  private listeners = new Set<(e: HistoryEvent) => void>();

  /** Appends an `in-flight` record and emits it. */
  startCall(target: string): CallRecord {
    const record: CallRecord = { target, startedAt: Date.now(), state: "in-flight" };
    this.calls.push(record);
    this.emit({ kind: "call", record });
    return record;
  }

  /** Moves a record to its final state and emits it. */
  endCall(record: CallRecord, state: CallState, extra?: { code?: number; error?: string }): void {
    record.state = state;
    record.endedAt = Date.now();
    if (extra?.code !== undefined) record.code = extra.code;
    if (extra?.error !== undefined) record.error = extra.error;
    this.emit({ kind: "call", record });
  }

  /** Appends a received prompt-service request and emits it. */
  addPrompt(record: Omit<PromptRecord, "at">): void {
    const full: PromptRecord = { ...record, at: Date.now() };
    this.prompts.push(full);
    this.emit({ kind: "prompt", record: full });
  }

  /** Subscribes to events; returns the unsubscribe function. */
  onChange(listener: (e: HistoryEvent) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private emit(event: HistoryEvent): void {
    for (const listener of [...this.listeners]) listener(event);
  }
}