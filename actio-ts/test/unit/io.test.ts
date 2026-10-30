import assert from "node:assert/strict";
import { open, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { test } from "node:test";
import type { TestContext } from "node:test";
import * as io from "../../src/io.js";
import { createSyncProvider } from "../../src/sync.js";
import { makeRoot, waitFor } from "../helpers/index.js";

test("writeJsonAtomic + readJson round-trip, no trailing whitespace, no temp leftovers", async (t) => {
  const root = await makeRoot(t);
  const target = path.join(root, "a.json");
  const tmpDir = path.join(root, "tmp");
  const obj = { a: 1, s: "x" };
  await io.writeJsonAtomic(target, obj, tmpDir);
  assert.deepEqual(await io.readJson(target), obj);
  assert.equal(await readFile(target, "utf8"), JSON.stringify(obj)); // no trailing newline
  assert.deepEqual(await io.listDir(tmpDir), []); // temp file renamed away
});

test("readJson returns undefined on missing or unparsable files", async (t) => {
  const root = await makeRoot(t);
  assert.equal(await io.readJson(path.join(root, "nope.json")), undefined);
  const bad = path.join(root, "bad.json");
  await writeFile(bad, "{not json");
  assert.equal(await io.readJson(bad), undefined);
});

test("appendJsonl appends one object per line", async (t) => {
  const root = await makeRoot(t);
  const target = path.join(root, "events.jsonl");
  await io.appendJsonl(target, { n: 1 });
  await io.appendJsonl(target, { n: 2 });
  assert.equal(await readFile(target, "utf8"), '{"n":1}\n{"n":2}\n');
});

test("copyFile copies content via temp file and rename", async (t) => {
  const root = await makeRoot(t);
  const src = path.join(root, "src.bin");
  const dst = path.join(root, "sub", "dst.bin");
  await io.makeDir(path.join(root, "sub"));
  await writeFile(src, "payload");
  await io.copyFile(src, dst, path.join(root, "tmp"));
  assert.equal(await readFile(dst, "utf8"), "payload");
  assert.deepEqual(await io.listDir(path.join(root, "tmp")), []);
});

test("makeDir, exists, listDir, remove", async (t) => {
  const root = await makeRoot(t);
  const dir = path.join(root, "d", "e");
  await io.makeDir(dir);
  await io.makeDir(dir); // idempotent
  assert.equal(await io.exists(dir), true);
  await writeFile(path.join(dir, "x.txt"), "x");
  assert.deepEqual(await io.listDir(dir), ["x.txt"]);
  await io.remove(dir);
  await io.remove(dir); // missing: ignored
  assert.equal(await io.exists(dir), false);
});

test("delay provider: file is complete before the delay; resolves not earlier than delayMs", async (t) => {
  const root = await makeRoot(t);
  io.setSyncProvider(createSyncProvider("delay:30"));
  t.after(() => io.setSyncProvider(createSyncProvider("none")));
  const target = path.join(root, "d.json");
  const obj = { v: 1, pad: "y".repeat(500) };
  const writeP = io.writeJsonAtomic(target, obj, path.join(root, "tmp"));
  let visibleAt = 0;
  await waitFor(
    async () => {
      const parsed = (await io.readJson(target)) as { v?: number; pad?: string } | undefined;
      if (parsed?.v === 1 && parsed?.pad === obj.pad) {
        visibleAt = Date.now();
        return true;
      }
      return false;
    },
    2000,
    2,
  );
  const resolvedAt = await writeP.then(() => Date.now());
  assert.ok(
    resolvedAt - visibleAt >= 27,
    `resolved only ${resolvedAt - visibleAt}ms after the file was visible (expected ~30ms delay)`,
  );
});

/**
 * Spies on FileHandle.prototype.sync (Node 24 does not export the FileHandle class as a value,
 * so the prototype is taken from a real, briefly-opened handle). Restored after the test.
 */
async function withSyncSpy(t: TestContext, root: string, onSync: () => void | Promise<void>): Promise<void> {
  const probe = await open(path.join(root, ".probe"), "w");
  const proto = Object.getPrototypeOf(probe) as { sync(): Promise<void> };
  await probe.close();
  const originalSync = proto.sync;
  t.mock.method(proto, "sync", async function (this: { sync(): Promise<void> }) {
    await onSync();
    return originalSync.call(this);
  });
}

test("fsync+delay: temp file fsynced before the rename, parent folder after it", async (t) => {
  const root = await makeRoot(t);
  io.setSyncProvider(createSyncProvider("fsync+delay:5"));
  t.after(() => io.setSyncProvider(createSyncProvider("none")));
  const dst = path.join(root, "sub", "f.json");
  await io.makeDir(path.join(root, "sub"));
  const states: { dstExists: boolean }[] = [];
  await withSyncSpy(t, root, async () => {
    states.push({ dstExists: await io.exists(dst) });
  });
  await io.writeJsonAtomic(dst, { a: 1 }, path.join(root, "tmp"));
  assert.equal(states.length, 2, "exactly two fsyncs: temp file and parent folder");
  assert.equal(states[0].dstExists, false, "temp file fsync happens before the rename");
  assert.equal(states[1].dstExists, true, "parent folder fsync happens after the rename");
});

test("none and delay providers never fsync", async (t) => {
  const root = await makeRoot(t);
  t.after(() => io.setSyncProvider(createSyncProvider("none")));
  let calls = 0;
  await withSyncSpy(t, root, () => {
    calls++;
  });
  for (const value of ["none", "delay:5"]) {
    io.setSyncProvider(createSyncProvider(value));
    await io.writeJsonAtomic(path.join(root, `${value.replace(":", "_")}.json`), { a: 1 }, path.join(root, "tmp"));
  }
  assert.equal(calls, 0);
});

test("atomic write never exposes a partial file to a concurrent reader", async (t) => {
  const root = await makeRoot(t);
  const target = path.join(root, "data.json");
  let stop = false;
  const reader = (async () => {
    while (!stop) {
      try {
        const raw = await readFile(target, "utf8");
        const parsed = JSON.parse(raw) as { n?: unknown; pad?: unknown };
        assert.equal(typeof parsed.n, "number");
        assert.equal(typeof parsed.pad, "string");
      } catch (err) {
        if (err instanceof assert.AssertionError) throw err;
        if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err; // partial JSON => failure
      }
    }
  })();
  for (let i = 0; i < 30; i++) {
    await io.writeJsonAtomic(target, { n: i, pad: "x".repeat(100 + (i % 4) * 50) }, path.join(root, "tmp"));
  }
  stop = true;
  await reader;
});