// File primitives (§2.5): atomic JSON writes, JSONL appends, small fs helpers. Every write of a
// protocol file goes through writeJsonAtomic or appendJsonl and applies the sync provider's hooks.
import { access, copyFile as fsCopyFile, mkdir, open, readdir, readFile, rename, rm } from "node:fs/promises";
import path from "node:path";
import { createSyncProvider } from "./sync.js";
import { nonce, sleep } from "./util.js";
import type { SyncProvider } from "./types.js";

let provider: SyncProvider = createSyncProvider("none");

/** Installs the process-wide sync provider used by every write hook (default: none). */
export function setSyncProvider(p: SyncProvider): void {
  provider = p;
}

/** fsync a directory; some platforms do not allow it, so errors are ignored (§2.6). */
async function fsyncDir(dir: string): Promise<void> {
  const dh = await open(dir, "r");
  try {
    await dh.sync();
  } catch {
    // not supported on some platforms
  } finally {
    await dh.close();
  }
}

/** Post-write hooks of §2.6 (after the rename, or after the append for JSONL). */
async function postWriteHooks(target: string): Promise<void> {
  if (provider.fsync) await fsyncDir(path.dirname(target));
  provider.notify();
  if (provider.delayMs > 0) await sleep(provider.delayMs);
}

/** Writes `obj` as JSON atomically: temp file `<tmpDir>/.<basename>.<nonce>.tmp`, then rename. */
export async function writeJsonAtomic(target: string, obj: unknown, tmpDir: string): Promise<void> {
  const data = JSON.stringify(obj);
  await makeDir(tmpDir);
  const tmp = path.join(tmpDir, `.${path.basename(target)}.${nonce()}.tmp`);
  const fh = await open(tmp, "w");
  try {
    await fh.writeFile(data);
    if (provider.fsync) await fh.sync();
  } finally {
    await fh.close();
  }
  try {
    await rename(tmp, target);
  } catch (err) {
    await rm(tmp, { force: true }).catch(() => {});
    throw err;
  }
  await postWriteHooks(target);
}

/** Reads a JSON file; undefined when it is missing or unparsable. */
export async function readJson(target: string): Promise<unknown> {
  try {
    return JSON.parse(await readFile(target, "utf8"));
  } catch {
    return undefined;
  }
}

/** Appends one JSON object as a single line (JSONL, no trailing whitespace). */
export async function appendJsonl(target: string, obj: unknown): Promise<void> {
  const fh = await open(target, "a");
  try {
    await fh.writeFile(`${JSON.stringify(obj)}\n`);
    if (provider.fsync) await fh.sync();
  } finally {
    await fh.close();
  }
  await postWriteHooks(target);
}

/** Lists the names of the entries of a directory. */
export async function listDir(target: string): Promise<string[]> {
  return readdir(target);
}

/** True when the path exists (file or folder). */
export async function exists(target: string): Promise<boolean> {
  try {
    await access(target);
    return true;
  } catch {
    return false;
  }
}

/** Creates the folder and its parents; idempotent. */
export async function makeDir(target: string): Promise<void> {
  await mkdir(target, { recursive: true });
}

/** Removes a file or folder; ignores a missing path. */
export async function remove(target: string): Promise<void> {
  await rm(target, { recursive: true, force: true });
}

/** Copies a file (attachments) via a temp file and rename, with the sync provider's hooks. */
export async function copyFile(src: string, dst: string, tmpDir: string): Promise<void> {
  await makeDir(tmpDir);
  const tmp = path.join(tmpDir, `.${path.basename(dst)}.${nonce()}.tmp`);
  await fsCopyFile(src, tmp);
  if (provider.fsync) {
    const fh = await open(tmp, "r");
    try {
      await fh.sync();
    } finally {
      await fh.close();
    }
  }
  try {
    await rename(tmp, dst);
  } catch (err) {
    await rm(tmp, { force: true }).catch(() => {});
    throw err;
  }
  await postWriteHooks(dst);
}