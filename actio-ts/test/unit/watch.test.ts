// Tests of the folder watcher and the pure marker diff (WP1-C).
import assert from "node:assert/strict";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { test, type TestContext } from "node:test";
import type { DirEntry, Markers } from "../../src/watch.js";
import { diffMarkers, watchDir } from "../../src/watch.js";
import { makeRoot, waitFor } from "../helpers/index.js";

const entry = (name: string, mtimeMs: number, size: number, ino: number): DirEntry => ({ name, mtimeMs, size, ino });

test("diffMarkers reports a new name", () => {
  const { changed, next } = diffMarkers({}, [entry("a.json", 10, 2, 1)]);
  assert.deepEqual(changed, ["a.json"]);
  assert.deepEqual(next, { "a.json": "10:2:1" });
});

test("diffMarkers reports a change by mtimeMs only", () => {
  const { changed, next } = diffMarkers({ "a.json": "10:2:1" }, [entry("a.json", 11, 2, 1)]);
  assert.deepEqual(changed, ["a.json"]);
  assert.deepEqual(next, { "a.json": "11:2:1" });
});

test("diffMarkers reports a change by size only", () => {
  const { changed, next } = diffMarkers({ "a.json": "10:2:1" }, [entry("a.json", 10, 3, 1)]);
  assert.deepEqual(changed, ["a.json"]);
  assert.deepEqual(next, { "a.json": "10:3:1" });
});

test("diffMarkers reports a change by ino only (rename over an existing file)", () => {
  const { changed, next } = diffMarkers({ "a.json": "10:2:1" }, [entry("a.json", 10, 2, 9)]);
  assert.deepEqual(changed, ["a.json"]);
  assert.deepEqual(next, { "a.json": "10:2:9" });
});

test("diffMarkers reports nothing when the marker is equal", () => {
  const prev: Markers = { "a.json": "10:2:1", "b.json": "7:1:5" };
  const { changed, next } = diffMarkers(prev, [entry("a.json", 10, 2, 1), entry("b.json", 7, 1, 5)]);
  assert.deepEqual(changed, []);
  assert.deepEqual(next, prev);
});

test("diffMarkers drops the marker of a disappeared name", () => {
  const prev: Markers = { "a.json": "10:2:1", "gone.json": "7:1:5" };
  const { changed, next } = diffMarkers(prev, [entry("a.json", 10, 2, 1)]);
  assert.deepEqual(changed, []);
  assert.deepEqual(next, { "a.json": "10:2:1" });
});

test("diffMarkers treats a deleted then reappeared file as new", () => {
  const afterDelete = diffMarkers({ "a.json": "10:2:1" }, []);
  assert.deepEqual(afterDelete.changed, []);
  assert.deepEqual(afterDelete.next, {});
  const { changed, next } = diffMarkers(afterDelete.next, [entry("a.json", 10, 2, 1)]);
  assert.deepEqual(changed, ["a.json"]);
  assert.deepEqual(next, { "a.json": "10:2:1" });
});

test("diffMarkers returns changed names in lexical order", () => {
  const { changed } = diffMarkers({}, [entry("c.json", 1, 1, 1), entry("a.json", 1, 1, 2), entry("b.json", 1, 1, 3)]);
  assert.deepEqual(changed, ["a.json", "b.json", "c.json"]);
});

test("diffMarkers does not modify prev", () => {
  const prev: Markers = { "a.json": "10:2:1" };
  diffMarkers(prev, [entry("a.json", 11, 2, 1), entry("b.json", 1, 1, 2)]);
  assert.deepEqual(prev, { "a.json": "10:2:1" });
});

/** watchDir with its callbacks recorded; the handle is closed when the test ends. */
function watch(t: TestContext, dir: string): { files: string[]; errors: Error[]; close(): void } {
  const files: string[] = [];
  const errors: Error[] = [];
  const handle = watchDir(
    dir,
    (name) => files.push(name),
    (err) => errors.push(err),
  );
  t.after(() => handle.close());
  return { files, errors, close: handle.close };
}

/** A watched folder inside a fresh temp root: `[root, dir]` (`root` itself is not watched). */
async function makeWatched(t: TestContext, name = "inbox"): Promise<[string, string]> {
  const root = await makeRoot(t);
  const dir = path.join(root, name);
  await fsp.mkdir(dir);
  return [root, dir];
}

/** Asserts absence: no further callback arrives for 300 ms (so `waitFor` must time out). */
async function staysQuiet(files: Error[] | string[]): Promise<void> {
  const before = files.length;
  await assert.rejects(waitFor(() => files.length > before, 300, 20), /waitFor: timeout/);
}

test("watchDir reports the files already present once, in lexical order", async (t) => {
  const [, dir] = await makeWatched(t);
  fs.writeFileSync(path.join(dir, "b.json"), "1");
  fs.writeFileSync(path.join(dir, "a.json"), "2");
  const w = watch(t, dir);
  await waitFor(() => w.files.length >= 2, 3_000);
  fs.writeFileSync(path.join(dir, "c.json"), "3");
  await waitFor(() => w.files.length >= 3, 3_000);
  assert.deepEqual(w.files, ["a.json", "b.json", "c.json"]);
  await staysQuiet(w.files);
  assert.deepEqual(w.errors, []);
});

test("watchDir reports a file renamed into the folder", async (t) => {
  const [root, dir] = await makeWatched(t);
  const outside = path.join(root, "outside.json");
  fs.writeFileSync(outside, "payload");
  const w = watch(t, dir);
  await staysQuiet(w.files); // a file outside the watched folder is invisible
  await fsp.rename(outside, path.join(dir, "moved.json"));
  await waitFor(() => w.files.length >= 1, 3_000);
  assert.deepEqual(w.files, ["moved.json"]);
  await staysQuiet(w.files);
});

test("watchDir reports a rename over an existing file (only the ino differs)", async (t) => {
  const [root, dir] = await makeWatched(t);
  const target = path.join(dir, "a.json");
  const fixed = new Date(1_700_000_000_000);
  fs.writeFileSync(target, "abcd");
  await fsp.utimes(target, fixed, fixed);
  const w = watch(t, dir);
  await waitFor(() => w.files.length >= 1, 3_000);

  const incoming = path.join(root, "incoming.json"); // outside the watched folder: no extra events
  fs.writeFileSync(incoming, "abcd");
  await fsp.utimes(incoming, fixed, fixed);
  const before = await fsp.stat(target);
  const src = await fsp.stat(incoming);
  assert.equal(src.size, before.size);
  assert.equal(src.mtimeMs, before.mtimeMs);
  assert.notEqual(src.ino, before.ino);

  await fsp.rename(incoming, target);
  const after = await fsp.stat(target);
  assert.equal(after.size, before.size);
  assert.equal(after.mtimeMs, before.mtimeMs);
  assert.notEqual(after.ino, before.ino); // the ino is the only thing that changed

  await waitFor(() => w.files.length >= 2, 3_000);
  assert.deepEqual(w.files, ["a.json", "a.json"]);
  await staysQuiet(w.files);
});

test("watchDir reports an in-place rewrite", async (t) => {
  const [, dir] = await makeWatched(t);
  const file = path.join(dir, "r.json");
  fs.writeFileSync(file, "one");
  const w = watch(t, dir);
  await waitFor(() => w.files.length >= 1, 3_000);
  fs.writeFileSync(file, "one rewritten");
  await waitFor(() => w.files.length >= 2, 3_000);
  assert.deepEqual(w.files, ["r.json", "r.json"]);
  await staysQuiet(w.files);
  assert.deepEqual(w.errors, []);
});

test("watchDir reports a burst of 1 000 writes, each name exactly once", async (t) => {
  const [, dir] = await makeWatched(t);
  const names = Array.from({ length: 1_000 }, (_, i) => `f${String(i).padStart(4, "0")}.json`);
  const w = watch(t, dir);
  for (const name of names) fs.writeFileSync(path.join(dir, name), "x");
  await waitFor(() => w.files.length >= 1_000, 15_000);
  assert.deepEqual([...w.files].sort(), [...names].sort());
  assert.equal(new Set(w.files).size, 1_000);
  assert.deepEqual(w.errors, []);
});

test("watchDir does not report again a file whose marker did not change", async (t) => {
  const [, dir] = await makeWatched(t);
  fs.writeFileSync(path.join(dir, "a.json"), "aaa");
  const w = watch(t, dir);
  await waitFor(() => w.files.length >= 1, 3_000);
  fs.writeFileSync(path.join(dir, "unrelated.json"), "bbb");
  await waitFor(() => w.files.length >= 2, 3_000);
  assert.deepEqual(w.files, ["a.json", "unrelated.json"]);
  await staysQuiet(w.files);
});

test("watchDir ignores dot files and folders", async (t) => {
  const [, dir] = await makeWatched(t);
  fs.writeFileSync(path.join(dir, ".hidden.json"), "x");
  fs.mkdirSync(path.join(dir, "sub"));
  fs.writeFileSync(path.join(dir, "sub", "deep.json"), "x");
  const w = watch(t, dir);
  await staysQuiet(w.files);
  fs.writeFileSync(path.join(dir, "ok.json"), "y");
  await waitFor(() => w.files.length >= 1, 3_000);
  assert.deepEqual(w.files, ["ok.json"]);
  await staysQuiet(w.files);
});

test("deleting the watched folder calls onError once and onFile never again", async (t) => {
  const [, dir] = await makeWatched(t);
  fs.writeFileSync(path.join(dir, "a.json"), "x");
  const w = watch(t, dir);
  await waitFor(() => w.files.length >= 1, 3_000);
  await fsp.rm(dir, { recursive: true, force: true });
  await waitFor(() => w.errors.length >= 1, 5_000);
  fs.mkdirSync(dir);
  fs.writeFileSync(path.join(dir, "late.json"), "x");
  await staysQuiet(w.files);
  assert.equal(w.errors.length, 1);
  assert.equal((w.errors[0] as NodeJS.ErrnoException).code, "ENOENT");
  assert.deepEqual(w.files, ["a.json"]);
});

test("watchDir on a missing folder reports onError asynchronously and never throws", async (t) => {
  const [root] = await makeWatched(t);
  const w = watch(t, path.join(root, "gone"));
  assert.deepEqual(w.files, []);
  assert.deepEqual(w.errors, []); // reported asynchronously, not while arming
  await waitFor(() => w.errors.length >= 1, 3_000);
  assert.equal((w.errors[0] as NodeJS.ErrnoException).code, "ENOENT");
  w.close();
  w.close(); // idempotent
  assert.equal(w.errors.length, 1);
});

test("watchDir never runs two scans concurrently during a burst", async (t) => {
  const [, dir] = await makeWatched(t);
  const realReaddir = fsp.readdir;
  let active = 0;
  let maxActive = 0;
  let scans = 0;
  t.mock.method(fsp, "readdir", async (p: Parameters<typeof fsp.readdir>[0]) => {
    active += 1;
    maxActive = Math.max(maxActive, active);
    scans += 1;
    try {
      return await realReaddir(p);
    } finally {
      active -= 1;
    }
  });
  const w = watch(t, dir);
  for (let i = 0; i < 200; i++) fs.writeFileSync(path.join(dir, `b${String(i).padStart(3, "0")}.json`), "x");
  await waitFor(() => w.files.length >= 200, 15_000);
  assert.equal(maxActive, 1); // readdir never called while a previous scan was still running
  assert.ok(scans >= 2, `expected more than one scan, got ${scans}`);
  assert.equal(new Set(w.files).size, 200);
});

test("no callback fires after close()", async (t) => {
  const [, dir] = await makeWatched(t);
  fs.writeFileSync(path.join(dir, "a.json"), "x");
  const w = watch(t, dir);
  await waitFor(() => w.files.length >= 1, 3_000);
  w.close();
  w.close(); // idempotent
  fs.writeFileSync(path.join(dir, "late.json"), "x");
  await staysQuiet(w.files);
  assert.deepEqual(w.files, ["a.json"]);
  assert.deepEqual(w.errors, []);
});

test("close() from inside an onFile callback drops the rest of the batch", async (t) => {
  const [, dir] = await makeWatched(t);
  for (const name of ["a.json", "b.json", "c.json"]) fs.writeFileSync(path.join(dir, name), "x");
  const files: string[] = [];
  let handle: { close(): void } | undefined;
  handle = watchDir(
    dir,
    (name) => {
      files.push(name);
      handle!.close(); // stop in the middle of the initial batch
    },
    () => assert.fail("no error expected"),
  );
  t.after(() => handle!.close());
  await waitFor(() => files.length >= 1, 3_000);
  await staysQuiet(files);
  assert.equal(files.length, 1); // the batch was cut at close()
  assert.deepEqual(files, ["a.json"]);
});

test("close() drops the results of a scan still in flight", async (t) => {
  const [, dir] = await makeWatched(t);
  fs.writeFileSync(path.join(dir, "a.json"), "x");
  const realReaddir = fsp.readdir;
  let entered = 0;
  t.mock.method(fsp, "readdir", async (p: Parameters<typeof fsp.readdir>[0]) => {
    entered += 1;
    await new Promise((resolve) => setTimeout(resolve, 150));
    return realReaddir(p);
  });
  const w = watch(t, dir);
  await waitFor(() => entered >= 1, 3_000);
  w.close(); // the initial scan is still awaiting readdir
  await staysQuiet(w.files);
  assert.deepEqual(w.files, []);
  assert.deepEqual(w.errors, []);
});
