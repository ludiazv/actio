// Shared test helpers: temp .actio roots, fake clock, fast config, inbox recorder, waitFor.
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { TestContext } from "node:test";
import type { ActioConfig } from "../../src/types.js";

/** Creates a temp `.actio` root and registers its recursive cleanup on the test context. */
export async function makeRoot(t: TestContext): Promise<string> {
  const root = await fsp.mkdtemp(path.join(os.tmpdir(), ".actio-test-"));
  t.after(() => fsp.rm(root, { recursive: true, force: true }));
  return root;
}

/** Fake clock for tests: callable (`clock()`), moved only by advance/set (never the wall clock). */
export function fakeClock(startMs = 1_700_000_000_000): { (): number; advance(ms: number): void; set(ms: number): void } {
  let current = startMs;
  const clock = () => current;
  return Object.assign(clock, {
    advance: (ms: number) => {
      current += ms;
    },
    set: (ms: number) => {
      current = ms;
    },
  });
}

/** Config with shrunk timers for tests; `overrides` win over the fast defaults. */
export function fastConfig(root: string, clock: () => number, overrides?: Partial<ActioConfig>): ActioConfig {
  return {
    root,
    sync: "none",
    heartbeatInterval: 200,
    confirmTimeout: 1_000,
    cancelWaitTimeout: 1_000,
    announceDelay: 10,
    nuntiumDebounce: 50,
    stopGrace: 1_000,
    clock,
    ...overrides,
  };
}

export interface InboxEvent {
  name: string;
  kind: "appeared" | "disappeared";
}

/**
 * Records the order in which files appear in and disappear from `dir` (snapshots). Armed with an
 * initial readdir (present files count as appeared); deliberately self-contained (own fs.watch +
 * readdir diff), independent of src/watch.ts, so e2e assertions measure the inbox independently.
 */
export function recordInbox(dir: string): { events(): InboxEvent[]; stop(): void } {
  const events: InboxEvent[] = [];
  let known = new Set<string>();
  let closed = false;
  let scanning = false;
  let dirty = false;

  const scan = async (): Promise<void> => {
    if (scanning) {
      dirty = true;
      return;
    }
    scanning = true;
    try {
      const names = new Set((await fsp.readdir(dir)).filter((n) => !n.startsWith(".")));
      for (const name of names) {
        if (!known.has(name)) {
          known.add(name);
          events.push({ name, kind: "appeared" });
        }
      }
      for (const name of [...known]) {
        if (!names.has(name)) {
          known.delete(name);
          events.push({ name, kind: "disappeared" });
        }
      }
    } catch {
      // folder gone: stop recording (the protocol test asserts what happened before)
      known = new Set();
    } finally {
      scanning = false;
      if (dirty && !closed) {
        dirty = false;
        void scan();
      }
    }
  };

  const watcher = fs.watch(dir, { persistent: false }, () => {
    if (!closed) void scan();
  });
  watcher.on("error", () => {
    closed = true;
  });
  void scan();

  return {
    events: () => [...events],
    stop: () => {
      closed = true;
      watcher.close();
    },
  };
}

/** Polls `predicate` until it returns true; throws `waitFor: timeout` after `timeoutMs`. */
export async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs: number, stepMs = 20): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() > deadline) throw new Error(`waitFor: timeout after ${timeoutMs} ms`);
    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }
}
