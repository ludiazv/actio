import assert from "node:assert/strict";
import { chmod, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { test, type TestContext } from "node:test";
import { createSyncProvider } from "../../src/sync.js";
import { makeRoot, waitFor } from "../helpers/index.js";
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

// --- R1-4: coalescing behind a run that is already in flight -------------------------------------

/**
 * A slow fake mutagen: it logs `run` when the flush starts and `end` when it finishes, and stays
 * alive for `sleepSec`. The gap between the two lines is the window in which a run is in flight.
 */
async function slowFake(t: TestContext, sleepSec = 0.3, exitCode = 0): Promise<string> {
  const root = await makeRoot(t);
  const log = path.join(root, "mutagen.log");
  const bin = path.join(root, "mutagen-fake");
  await writeFile(bin, `#!/bin/sh\necho run >> "${log}"\nsleep ${sleepSec}\necho end >> "${log}"\nexit ${exitCode}\n`);
  await chmod(bin, 0o755);
  const saved = process.env.ACTIO_MUTAGEN_BIN;
  process.env.ACTIO_MUTAGEN_BIN = bin;
  t.after(() => {
    if (saved === undefined) delete process.env.ACTIO_MUTAGEN_BIN;
    else process.env.ACTIO_MUTAGEN_BIN = saved;
  });
  return log;
}

const count = (lines: string[], word: string): number => lines.filter((l) => l === word).length;

/** Blocks until the fake has written `n` `run` (or `end`) lines; runs are serialized, so log order is time order. */
const waitRuns = (log: string, n: number, word = "run"): Promise<void> =>
  waitFor(async () => count(await logLines(log), word) >= n, 8_000, 10);

test("mutagen: notify()s arriving during a run coalesce into exactly one more run (R1-4)", async (t) => {
  const log = await slowFake(t);
  const p = createSyncProvider("mutagen:s1");
  p.notify();
  await waitRuns(log, 1); // run 1 has started; the fake keeps it alive for 300 ms
  p.notify();
  p.notify();
  p.notify(); // all three land while run 1 is in flight
  await waitRuns(log, 2, "end");
  await sleep(500); // past the debounce window and the follow-up: nothing else may show up
  assert.deepEqual(
    await logLines(log),
    ["run", "end", "run", "end"],
    "two executions: the in-flight run plus one coalesced follow-up, started after the first ended",
  );
  await p.close();
});

test("mutagen: flush() during a run resolves only after a run that started after the call (R1-4)", async (t) => {
  const log = await slowFake(t);
  const p = createSyncProvider("mutagen:s1");
  p.notify();
  await waitRuns(log, 1);
  const runsAtCallTime = count(await logLines(log), "run"); // 1, and that run is the in-flight one
  let resolved = false;
  const f = p.flush().then(() => {
    resolved = true;
  });
  await sleep(60); // run 1 may well finish inside this window
  assert.equal(resolved, false, "flush() must not resolve with the run that was already going");
  await f;
  assert.equal(count(await logLines(log), "run"), runsAtCallTime + 1, "a new run started after the call");
  assert.deepEqual(await logLines(log), ["run", "end", "run", "end"], "and it is the one flush() awaited");
  await sleep(500);
  assert.equal(count(await logLines(log), "run"), 2, "one follow-up for one mid-run flush");
  await p.close();
});

test("mutagen: notify()s during the follow-up run coalesce the same way (R1-4)", async (t) => {
  const log = await slowFake(t);
  const p = createSyncProvider("mutagen:s1");
  p.notify();
  await waitRuns(log, 1);
  p.notify(); // mid-run: one follow-up promised
  await waitRuns(log, 2); // that follow-up is itself in flight now
  p.notify();
  p.notify(); // mid-follow-up: promised exactly the same way
  await waitRuns(log, 3, "end");
  await sleep(500);
  assert.deepEqual(await logLines(log), ["run", "end", "run", "end", "run", "end"], "three, never four");
  await p.close();
});

test("mutagen: close() cancels the debounce but never drops a promised follow-up; close() is idempotent", async (t) => {
  const log = await slowFake(t);
  const p = createSyncProvider("mutagen:s1");
  p.notify();
  await waitRuns(log, 1);
  const f = p.flush(); // promised a follow-up mid-run: close() must not swallow it
  await p.close();
  await p.close(); // idempotent
  await f;
  assert.deepEqual(await logLines(log), ["run", "end", "run", "end"]);
  await sleep(500);
  assert.equal(count(await logLines(log), "run"), 2);
});

test("mutagen: a failing binary still reports onError once per run, coalesced runs included (I-1)", async (t) => {
  const log = await slowFake(t, 0.3, 1); // every run fails
  const errors: Error[] = [];
  const p = createSyncProvider("mutagen:s1", (err) => errors.push(err));
  p.notify();
  await waitRuns(log, 1);
  p.notify(); // promised follow-up
  await p.flush(); // mid-run flush joins it; must resolve, never throw
  await sleep(400);
  assert.equal(count(await logLines(log), "run"), 2, "one in-flight run, one coalesced follow-up");
  assert.equal(errors.length, 2, "exactly one error per failed run");
  assert.ok(errors.every((e) => e instanceof Error));
  await p.close();
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