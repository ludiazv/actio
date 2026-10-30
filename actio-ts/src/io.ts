// File primitives (§2.5): atomic JSON writes, JSONL appends, small fs helpers. Every write of a
// protocol file goes through writeJsonAtomic or appendJsonl and applies the sync provider's hooks.
import type { SyncProvider } from "./types.js";

/** Installs the process-wide sync provider used by every write hook (default: none). */
export function setSyncProvider(provider: SyncProvider): void {
  throw new Error("not implemented");
}

/** Writes `obj` as JSON atomically: temp file `<tmpDir>/.<basename>.<nonce>.tmp`, then rename. */
export function writeJsonAtomic(path: string, obj: unknown, tmpDir: string): Promise<void> {
  throw new Error("not implemented");
}

/** Reads a JSON file; undefined when it is missing or unparsable. */
export function readJson(path: string): Promise<unknown> {
  throw new Error("not implemented");
}

/** Appends one JSON object as a single line (JSONL, no trailing whitespace). */
export function appendJsonl(path: string, obj: unknown): Promise<void> {
  throw new Error("not implemented");
}

/** Lists the names of the entries of a directory. */
export function listDir(path: string): Promise<string[]> {
  throw new Error("not implemented");
}

/** True when the path exists (file or folder). */
export function exists(path: string): Promise<boolean> {
  throw new Error("not implemented");
}

/** Creates the folder and its parents; idempotent. */
export function makeDir(path: string): Promise<void> {
  throw new Error("not implemented");
}

/** Removes a file or folder; ignores a missing path. */
export function remove(path: string): Promise<void> {
  throw new Error("not implemented");
}

/** Copies a file (attachments) via a temp file and rename, with the sync provider's hooks. */
export function copyFile(src: string, dst: string, tmpDir: string): Promise<void> {
  throw new Error("not implemented");
}
