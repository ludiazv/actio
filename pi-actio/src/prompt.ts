// The prompt service: four capabilities offered to the team (D-12…D-17), the queue, and the
// run state. Registered on the actio agent before it starts.
import { randomBytes } from "node:crypto";
import type { AgentEndEvent, ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { ActioError, type ActioAgent, type Capability, type CapabilityContext } from "actio-ts";
import type { History } from "./history.js";

/** The part of the extension API the queue needs; `on` is used for `agent_end`/`agent_settled`. */
export interface PiSurface {
  sendUserMessage: ExtensionAPI["sendUserMessage"];
  on: ExtensionAPI["on"];
}

/** One live prompt: its state, its text while queued/running, its answer when done. */
interface PromptEntry {
  status: "queued" | "running" | "done";
  msg?: string;
  answer?: string;
}

/** The `prompt_id` schema (D-12): 8 lower-case hex characters. */
const PID = { type: "string", pattern: "^[0-9a-f]{8}$" };

export class PromptService {
  private pi: PiSurface;
  private history: History;
  private ctx: ExtensionContext | undefined;
  private entries = new Map<string, PromptEntry>();
  private queue: string[] = [];
  private running = new Set<string>();
  private lastAnswer = "";
  // D-16: a compaction is in flight, or was requested while busy and is deferred.
  private compacting = false;
  private compactPending = false;

  constructor(pi: PiSurface, history: History) {
    this.pi = pi;
    this.history = history;
    // `agent_end` can fire more than once per settle (retries), so keep the last answer.
    pi.on("agent_end", (event) => {
      this.lastAnswer = PromptService.answerOf(event.messages);
    });
    pi.on("agent_settled", (_event, ctx) => {
      // D-14: every running prompt becomes done with the last agent_end answer
      // (an empty string after an abort).
      for (const id of [...this.running]) {
        const entry = this.entries.get(id);
        if (entry !== undefined) {
          entry.status = "done";
          entry.answer = this.lastAnswer;
        }
      }
      this.running.clear();
      // Then, if idle: a deferred compaction starts before the queue (D-16).
      if (ctx.isIdle()) {
        if (this.compactPending) {
          this.compactPending = false;
          this.startCompaction();
        } else {
          this.sendNext();
        }
      }
    });
  }

  /** Registers the four capabilities; `acceptFrom` becomes each `actio.accepts_from` (D-17). */
  register(agent: ActioAgent, acceptFrom: string[]): void {
    const accepts = acceptFrom.length > 0 ? { accepts_from: acceptFrom } : {};
    const prompt: Capability = {
      name: "prompt",
      description: "Send a prompt to this pi agent; returns a prompt_id to poll with retrieve.",
      parameters: {
        type: "object",
        properties: {
          msg_prompt: { type: "string", minLength: 1 },
          asap: { type: "boolean" },
        },
        required: ["msg_prompt"],
        additionalProperties: false,
      },
      actio: {
        result: {
          type: "object",
          properties: { prompt_id: PID },
          required: ["prompt_id"],
          additionalProperties: false,
        },
        ...accepts,
      },
    };
    const cancel: Capability = {
      name: "cancel",
      description: "Cancel a queued or running prompt by prompt_id.",
      parameters: {
        type: "object",
        properties: { prompt_id: PID },
        required: ["prompt_id"],
        additionalProperties: false,
      },
      actio: { result: { type: "null" }, ...accepts },
    };
    const retrieve: Capability = {
      name: "retrieve",
      description: "Poll a prompt_id: queued, running, or done (with the answer).",
      parameters: {
        type: "object",
        properties: { prompt_id: PID },
        required: ["prompt_id"],
        additionalProperties: false,
      },
      actio: {
        result: {
          type: "object",
          properties: {
            status: { enum: ["queued", "running", "done"] },
            response: { type: "string" },
          },
          required: ["status"],
          additionalProperties: false,
        },
        ...accepts,
      },
    };
    const compact: Capability = {
      name: "compact",
      description: "Compact this agent's session context.",
      parameters: { type: "object", additionalProperties: false },
      actio: { result: { type: "null" }, ...accepts },
    };
    agent.capability(prompt, (params, ctx) => this.onPrompt(params, ctx));
    agent.capability(cancel, (params, ctx) => this.onCancel(params, ctx));
    agent.capability(retrieve, (params, ctx) => this.onRetrieve(params, ctx));
    agent.capability(compact, (_params, ctx) => this.onCompact(ctx));
  }

  /** Keeps the latest context: `isIdle`, `abort`, `compact` (D-14…D-16). */
  bind(ctx: ExtensionContext): void {
    this.ctx = ctx;
  }

  // -- capability handlers ----------------------------------------------------

  /** D-12 id, D-14 dispatch; returns `{ prompt_id }`. */
  private async onPrompt(params: unknown, ctx: CapabilityContext): Promise<unknown> {
    const p = params as { msg_prompt: string; asap?: boolean };
    const id = this.newId();
    // D-14/D-16: nothing is sent while busy without `deliverAs`, and an `asap` prompt steers
    // only when no compaction is in flight.
    const busy = this.busy();
    let outcome: string;
    if (!busy) {
      this.send(id, p.msg_prompt);
      outcome = "running";
    } else if (p.asap === true && !this.compacting) {
      this.send(id, p.msg_prompt, { deliverAs: "steer" });
      outcome = "running";
    } else {
      this.entries.set(id, { status: "queued", msg: p.msg_prompt });
      this.queue.push(id);
      outcome = "queued";
    }
    this.history.addPrompt({ src: ctx.src, capability: "prompt", promptId: id, outcome });
    return { prompt_id: id };
  }

  /** D-13/D-15: forgets the id in any state; a running cancel aborts the run. */
  private async onCancel(params: unknown, ctx: CapabilityContext): Promise<unknown> {
    const id = (params as { prompt_id: string }).prompt_id;
    if (this.entries.get(id) === undefined) {
      this.history.addPrompt({ src: ctx.src, capability: "cancel", promptId: id, outcome: "error 1000" });
      throw new ActioError(1000, `unknown prompt_id ${id}`);
    }
    this.entries.delete(id);
    this.queue = this.queue.filter((queued) => queued !== id);
    if (this.running.delete(id)) this.ctx?.abort();
    this.history.addPrompt({ src: ctx.src, capability: "cancel", promptId: id, outcome: "cancelled" });
    return null;
  }

  /** D-13: `done` returns the answer and forgets; `queued`/`running` report the state. */
  private async onRetrieve(params: unknown, ctx: CapabilityContext): Promise<unknown> {
    const id = (params as { prompt_id: string }).prompt_id;
    const entry = this.entries.get(id);
    if (entry === undefined) {
      this.history.addPrompt({ src: ctx.src, capability: "retrieve", promptId: id, outcome: "error 1000" });
      throw new ActioError(1000, `unknown prompt_id ${id}`);
    }
    if (entry.status === "done") {
      this.entries.delete(id);
      this.history.addPrompt({ src: ctx.src, capability: "retrieve", promptId: id, outcome: "done" });
      return { status: "done", response: entry.answer ?? "" };
    }
    // A poll: recorded, and the index.ts history subscriber decides it is not notified.
    this.history.addPrompt({ src: ctx.src, capability: "retrieve", promptId: id, outcome: entry.status });
    return { status: entry.status };
  }

  /** D-16: always success; a small session fails with "Nothing to compact". */
  private async onCompact(ctx: CapabilityContext): Promise<unknown> {
    // pi's compact() aborts the run in progress and never settles: while busy the
    // compaction is deferred to the next settle; its callbacks resume the queue.
    // During a compaction the one in flight already covers the request. Deferring it
    // would run a second compaction after some unrelated later settle, maybe the user's turn.
    if (this.compacting) {
      /* the compaction in flight covers it */
    } else if (this.busy()) this.compactPending = true;
    else this.startCompaction();
    this.history.addPrompt({ src: ctx.src, capability: "compact", outcome: "compact" });
    return null;
  }

  // -- run state ---------------------------------------------------------------

  /** D-12: 8 lower-case hex characters, redrawn on a collision with a live id. */
  private newId(): string {
    for (;;) {
      const id = randomBytes(4).toString("hex");
      if (!this.entries.has(id)) return id;
    }
  }

  /** Sends a prompt (plain, or steered) and marks it running. */
  private send(id: string, msg: string, options?: { deliverAs: "steer" }): void {
    this.entries.set(id, { status: "running", msg });
    this.running.add(id);
    this.pi.sendUserMessage(msg, options);
  }

  /** Sends the head of the FIFO queue (called from `agent_settled` when idle). */
  private sendNext(): void {
    const id = this.queue.shift();
    if (id === undefined) return;
    const entry = this.entries.get(id);
    if (entry === undefined || entry.msg === undefined) return;
    this.send(id, entry.msg);
  }

  /** D-14/D-16: busy = !isIdle, a prompt running, or a compaction in flight. */
  private busy(): boolean {
    return this.ctx === undefined || !this.ctx.isIdle() || this.running.size > 0 || this.compacting;
  }

  /** D-16: starts the compaction; one callback clears the flag and, if not busy, runs the queue. */
  private startCompaction(): void {
    if (this.ctx === undefined) return;
    this.compacting = true;
    const after = (): void => {
      // pi runs `onError` even when `onComplete` throws, so the callback must fire once. And it
      // checks busy(), not isIdle(): a plain send while a prompt is running breaks D-14.
      if (!this.compacting) return;
      this.compacting = false;
      if (!this.busy()) this.sendNext();
    };
    this.ctx.compact({ onComplete: after, onError: after });
  }

  /** The last assistant text of an `agent_end` ("" when there is none). */
  private static answerOf(messages: AgentEndEvent["messages"]): string {
    for (let i = messages.length - 1; i >= 0; i--) {
      const message = messages[i];
      if (message.role !== "assistant") continue;
      return message.content
        .filter((part) => part.type === "text")
        .map((part) => part.text)
        .join("");
    }
    return "";
  }
}