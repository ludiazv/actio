// PromptService against a stub PiSurface and a stub ctx (D-12…D-17); no real pi, no timers.
import assert from "node:assert/strict";
import { test } from "node:test";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { ActioError, type ActioAgent, type Capability, type CapabilityContext, type CapabilityHandler } from "actio-ts";
import { History } from "../src/history.js";
import { PromptService, type PiSurface } from "../src/prompt.js";

/** A stub ctx: an `isIdle` flag, and `abort`/`compact` spies. */
function makeCtx() {
  const state = { idle: true, aborted: 0, compacts: 0 };
  const ctx = {
    isIdle: () => state.idle,
    abort: () => {
      state.aborted += 1;
    },
    // Simulates a small session: the compaction fails, so `onError` fires.
    compact: (options?: { onError?: (error: Error) => void }) => {
      state.compacts += 1;
      options?.onError?.(new Error("Nothing to compact"));
    },
  };
  return { state, ctx: ctx as unknown as ExtensionContext };
}

/** A stub PiSurface; `agent_end` and `agent_settled` are emitted by hand. */
function makePi() {
  const sent: { text: string; deliverAs?: string }[] = [];
  type Handler = (e: never, ctx: ExtensionContext) => void;
  const handlers: { agent_end?: Handler; agent_settled?: Handler } = {};
  const surface = {
    sendUserMessage(text: string, options?: { deliverAs?: "steer" | "followUp" }) {
      sent.push({ text, deliverAs: options?.deliverAs });
    },
    on(event: "agent_end" | "agent_settled", handler: Handler) {
      handlers[event] = handler;
      return () => {};
    },
  } as unknown as PiSurface;
  const anyCtx = {} as unknown as ExtensionContext;
  return {
    sent,
    surface,
    end(messages: unknown[]) {
      handlers.agent_end?.({ type: "agent_end", messages } as never, anyCtx);
    },
    settled(aborted: boolean, ctx: ExtensionContext) {
      handlers.agent_settled?.({ type: "agent_settled", aborted } as never, ctx);
    },
  };
}

const assistant = (text: string) => ({ role: "assistant", content: [{ type: "text", text }] });
const capCtx = (src: string) =>
  ({ rid: "rid-1", src, signal: new AbortController().signal, attachments: [] }) as unknown as CapabilityContext;

/** A PromptService registered on a stub agent; `call` invokes a capability handler. */
function makeService() {
  const pi = makePi();
  const { state, ctx } = makeCtx();
  const history = new History();
  const service = new PromptService(pi.surface, history);
  service.bind(ctx);
  const registered = new Map<string, { cap: Capability; handler: CapabilityHandler }>();
  const agent = {
    capability(cap: Capability, handler: CapabilityHandler) {
      registered.set(cap.name, { cap, handler });
    },
  } as unknown as ActioAgent;
  service.register(agent, []);
  const call = (name: string, params: unknown) => registered.get(name)!.handler(params, capCtx("peer-1"));
  return { pi, state, ctx, history, call };
}

test("idle prompt runs, settles done, retrieve returns the answer and forgets", async () => {
  const h = makeService();
  const { prompt_id } = (await h.call("prompt", { msg_prompt: "hello" })) as { prompt_id: string };
  assert.match(prompt_id, /^[0-9a-f]{8}$/);
  assert.deepEqual(h.pi.sent, [{ text: "hello", deliverAs: undefined }]);
  assert.deepEqual(await h.call("retrieve", { prompt_id }), { status: "running" });
  h.pi.end([{ role: "user", content: "hello" }, assistant("echo: hello")]);
  h.pi.settled(false, h.ctx);
  assert.deepEqual(await h.call("retrieve", { prompt_id }), { status: "done", response: "echo: hello" });
  await assert.rejects(h.call("retrieve", { prompt_id }), (e: unknown) => e instanceof ActioError && e.code === 1000);
  assert.deepEqual(
    h.history.prompts.map((r) => [r.capability, r.promptId, r.outcome]),
    [
      ["prompt", prompt_id, "running"],
      ["retrieve", prompt_id, "running"],
      ["retrieve", prompt_id, "done"],
      ["retrieve", prompt_id, "error 1000"],
    ],
  );
});

test("busy prompt is queued and sent on settle", async () => {
  const h = makeService();
  h.state.idle = false;
  const { prompt_id } = (await h.call("prompt", { msg_prompt: "later" })) as { prompt_id: string };
  assert.deepEqual(h.pi.sent, []);
  assert.deepEqual(await h.call("retrieve", { prompt_id }), { status: "queued" });
  h.state.idle = true;
  h.pi.settled(false, h.ctx);
  assert.deepEqual(h.pi.sent, [{ text: "later", deliverAs: undefined }]);
  assert.deepEqual(await h.call("retrieve", { prompt_id }), { status: "running" });
});

test("asap while busy steers", async () => {
  const h = makeService();
  h.state.idle = false;
  const { prompt_id } = (await h.call("prompt", { msg_prompt: "now", asap: true })) as { prompt_id: string };
  assert.deepEqual(h.pi.sent, [{ text: "now", deliverAs: "steer" }]);
  assert.deepEqual(await h.call("retrieve", { prompt_id }), { status: "running" });
});

test("two back-to-back prompts while idle: the second is queued", async () => {
  const h = makeService();
  await h.call("prompt", { msg_prompt: "one" });
  const { prompt_id } = (await h.call("prompt", { msg_prompt: "two" })) as { prompt_id: string };
  assert.deepEqual(h.pi.sent, [{ text: "one", deliverAs: undefined }]);
  assert.deepEqual(await h.call("retrieve", { prompt_id }), { status: "queued" });
  h.pi.settled(false, h.ctx); // closes the first, then sends the second
  assert.deepEqual(h.pi.sent, [
    { text: "one", deliverAs: undefined },
    { text: "two", deliverAs: undefined },
  ]);
});

test("cancel a queued prompt forgets it", async () => {
  const h = makeService();
  h.state.idle = false;
  const { prompt_id } = (await h.call("prompt", { msg_prompt: "nope" })) as { prompt_id: string };
  assert.equal(await h.call("cancel", { prompt_id }), null);
  await assert.rejects(h.call("retrieve", { prompt_id }), /unknown prompt_id/);
  h.state.idle = true;
  h.pi.settled(false, h.ctx);
  assert.deepEqual(h.pi.sent, []);
  assert.deepEqual(h.history.prompts[1], {
    at: h.history.prompts[1].at,
    src: "peer-1",
    capability: "cancel",
    promptId: prompt_id,
    outcome: "cancelled",
  });
});

test("cancel a running prompt aborts the run", async () => {
  const h = makeService();
  const { prompt_id } = (await h.call("prompt", { msg_prompt: "stop" })) as { prompt_id: string };
  assert.equal(await h.call("cancel", { prompt_id }), null);
  assert.equal(h.state.aborted, 1);
  await assert.rejects(h.call("cancel", { prompt_id }), /unknown prompt_id/);
});

test("unknown prompt_id is ActioError 1000 (cancel and retrieve)", async () => {
  const h = makeService();
  await assert.rejects(h.call("cancel", { prompt_id: "deadbeef" }), (e: unknown) => {
    return e instanceof ActioError && e.code === 1000 && e.message === "unknown prompt_id deadbeef";
  });
  await assert.rejects(h.call("retrieve", { prompt_id: "deadbeef" }), (e: unknown) => {
    return e instanceof ActioError && e.code === 1000 && e.message === "unknown prompt_id deadbeef";
  });
});

test("compact returns null even when onError fires", async () => {
  const h = makeService();
  assert.equal(await h.call("compact", {}), null);
  assert.equal(h.state.compacts, 1);
  assert.equal(h.history.prompts.at(-1)?.outcome, "compact");
});