import assert from "node:assert/strict";
import { chmod, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { test } from "node:test";
import { createSyncProvider } from "../../src/sync.js";
import { makeRoot } from "../helpers/index.js";
import { sleep } from "../../src/util.js";

test("createSyncProvider parses the provider table", () => {
  const expect = (value: string, fsync: boolean, delayMs: number): void => {
    const p = createSyncProvider(value);
    assert.equal(p.fsync, fsync, value);
    assert.equal(p.delayMs, delayMs, value);
  };
  expect("none", false, 0);
  expect("delay", false, 50);
  expect("delay:20", false, 20);
  expect("fsync+delay", true, 50);
  expect("fsync+delay:20", true, 20);
  expect("mutagen:s1", false, 0);
});

test("createSyncProvider rejects unknown values and bad <ms>", () => {
  for (const value of [
    "delay:0",
    "delay:abc",
    "delay:20000",
    "fsync",
    "foo",
    "delay:",
    "delay:-5",
    "delay:5.5",
    "fsync+delay:0",
    "mutagen:",
    "mutagen",
    "none ",
  ]) {
    assert.throws(() => createSyncProvider(value), Error, `should reject ${JSON.stringify(value)}`);
  }
});

async function logLines(log: string): Promise<string[]> {
  try {
    return (await readFile(log, "utf8")).trim().split("\n");
  } catch {
    return [];
  }
}

test("mutagen: notify() debounces a burst into one execution of `sync flush <session>`", async (t) => {
  const root = await makeRoot(t);
  const log = path.join(root, "mutagen.log");
  const bin = path.join(root, "mutagen-fake");
  await writeFile(bin, `#!/bin/sh\necho "$@" >> "${log}"\nsleep 0.1\n`);
  await chmod(bin, 0o755);
  const saved = process.env.ACTIO_MUTAGEN_BIN;
  process.env.ACTIO_MUTAGEN_BIN = bin;
  t.after(() => {
    if (saved === undefined) delete process.env.ACTIO_MUTAGEN_BIN;
    else process.env.ACTIO_MUTAGEN_BIN = saved;
  });

  const p = createSyncProvider("mutagen:s1");
  for (let i = 0; i < 5; i++) p.notify();
  await sleep(500);
  assert.deepEqual(await logLines(log), ["sync flush s1"]);
  await p.close();
});

test("mutagen: flush() runs a sync at once, awaits it, and runs even without a pending notify", async (t) => {
  const root = await makeRoot(t);
  const log = path.join(root, "mutagen.log");
  const bin = path.join(root, "mutagen-fake");
  await writeFile(bin, `#!/bin/sh\necho "$@" >> "${log}"\n`);
  await chmod(bin, 0o755);
  const saved = process.env.ACTIO_MUTAGEN_BIN;
  process.env.ACTIO_MUTAGEN_BIN = bin;
  t.after(() => {
    if (saved === undefined) delete process.env.ACTIO_MUTAGEN_BIN;
    else process.env.ACTIO_MUTAGEN_BIN = saved;
  });

  const p = createSyncProvider("mutagen:s1");
  p.notify();
  await p.flush(); // cancels the pending debounce, runs now, and awaits
  assert.deepEqual(await logLines(log), ["sync flush s1"]);
  await p.flush(); // no pending notify: flush still runs
  assert.equal((await logLines(log)).length, 2);
  await p.close();
});

test("mutagen: close() cancels a pending debounced run", async (t) => {
  const root = await makeRoot(t);
  const log = path.join(root, "mutagen.log");
  const bin = path.join(root, "mutagen-fake");
  await writeFile(bin, `#!/bin/sh\necho "$@" >> "${log}"\n`);
  await chmod(bin, 0o755);
  const saved = process.env.ACTIO_MUTAGEN_BIN;
  process.env.ACTIO_MUTAGEN_BIN = bin;
  t.after(() => {
    if (saved === undefined) delete process.env.ACTIO_MUTAGEN_BIN;
    else process.env.ACTIO_MUTAGEN_BIN = saved;
  });

  const p = createSyncProvider("mutagen:s1");
  p.notify();
  await p.close();
  await sleep(400); // well past the 200 ms debounce window
  assert.deepEqual(await logLines(log), []);
});

test("mutagen: a failing binary calls onError once per failure and never throws", async (t) => {
  const root = await makeRoot(t);
  const bin = path.join(root, "mutagen-fake");
  await writeFile(bin, "#!/bin/sh\nexit 1\n");
  await chmod(bin, 0o755);
  const saved = process.env.ACTIO_MUTAGEN_BIN;
  process.env.ACTIO_MUTAGEN_BIN = bin;
  t.after(() => {
    if (saved === undefined) delete process.env.ACTIO_MUTAGEN_BIN;
    else process.env.ACTIO_MUTAGEN_BIN = saved;
  });

  const errors: Error[] = [];
  const p = createSyncProvider("mutagen:s1", (err) => errors.push(err));
  p.notify(); // fire-and-forget: must not throw
  await sleep(400); // debounce fires, the run fails
  assert.equal(errors.length, 1);
  assert.ok(errors[0] instanceof Error);
  await p.flush(); // fails again; resolves without throwing
  assert.equal(errors.length, 2);
  await p.close();
});