// Tests of the shared test helpers themselves (WP0-C).
import assert from "node:assert/strict";
import { test } from "node:test";
import { fakeClock, fastConfig, makeRoot, recordInbox, waitFor } from "../helpers/index.js";

test("makeRoot creates a temp .actio root", async (t) => {
  const root = await makeRoot(t);
  assert.match(root, /\.actio-test-/);
  const st = await (await import("node:fs/promises")).stat(root);
  assert.ok(st.isDirectory());
});

test("fakeClock advances and can be set", () => {
  const clock = fakeClock(1000);
  assert.equal(clock(), 1000);
  clock.advance(500);
  assert.equal(clock(), 1500);
  clock.set(7);
  assert.equal(clock(), 7);
});

test("fastConfig applies fast timers and overrides", () => {
  const clock = fakeClock();
  const cfg = fastConfig("/tmp/x", clock);
  assert.equal(cfg.root, "/tmp/x");
  assert.equal(cfg.clock, clock);
  assert.equal(cfg.confirmTimeout, 1000);
  assert.equal(cfg.heartbeatInterval, 200);
  assert.equal(cfg.cancelWaitTimeout, 1000);
  assert.equal(cfg.announceDelay, 10);
  assert.equal(cfg.nuntiumDebounce, 50);
  assert.equal(cfg.stopGrace, 1000);
  assert.equal(cfg.sync, "none");
  const cfg2 = fastConfig("/tmp/y", clock, { confirmTimeout: 5 });
  assert.equal(cfg2.confirmTimeout, 5);
});

test("waitFor resolves when the predicate turns true", async () => {
  let ok = false;
  setTimeout(() => {
    ok = true;
  }, 50);
  await waitFor(() => ok, 2_000);
});

test("waitFor throws on timeout", async () => {
  await assert.rejects(waitFor(() => false, 80, 10), /waitFor: timeout/);
});

test("recordInbox records appear/disappear in order", async (t) => {
  const fsp = await import("node:fs/promises");
  const root = await makeRoot(t);
  const dir = `${root}/inbox`;
  await fsp.mkdir(dir, { recursive: true });
  const recorder = recordInbox(dir);
  t.after(() => recorder.stop());
  await fsp.writeFile(`${dir}/a.json`, JSON.stringify({ a: 1 }));
  await waitFor(() => recorder.events().length >= 1, 2_000);
  await fsp.rm(`${dir}/a.json`);
  await waitFor(() => recorder.events().length >= 2, 2_000);
  const events = recorder.events();
  assert.deepEqual(
    events.map((e) => [e.name, e.kind]),
    [
      ["a.json", "appeared"],
      ["a.json", "disappeared"],
    ],
  );
});
