// Event-driven folder watcher (§2.1, WP1): fs.watch events trigger a folder scan; no timers,
// no polling, no re-arming. Exact contract in the master plan (WP1).
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

export interface DirEntry {
  name: string;
  mtimeMs: number;
  size: number;
  ino: number;
}

/** Per-file change marker `mtimeMs:size:ino`. */
export type Markers = Record<string, string>;

/** The change marker of one file: `mtimeMs:size:ino`. */
function markerOf(entry: DirEntry): string {
  return `${entry.mtimeMs}:${entry.size}:${entry.ino}`;
}

/**
 * Pure marker comparison: names whose marker is new or different are reported as changed (in
 * lexical order); disappeared names lose their marker; `next` carries the markers to keep.
 */
export function diffMarkers(prev: Markers, entries: DirEntry[]): { changed: string[]; next: Markers } {
  const next: Markers = {};
  const changed: string[] = [];
  for (const entry of entries) {
    const marker = markerOf(entry);
    next[entry.name] = marker;
    if (!Object.hasOwn(prev, entry.name) || prev[entry.name] !== marker) changed.push(entry.name);
  }
  changed.sort();
  return { changed, next };
}

/**
 * Watches `dir` non-recursively: arms fs.watch first, then runs one initial scan (present files are
 * reported once as new). Every event triggers a serialized scan; at most one scan runs at a time.
 * A failing scan (e.g. folder deleted) or a watcher error closes the watcher and calls `onError`
 * once; `close()` is idempotent and stops all callbacks.
 */
export function watchDir(
  dir: string,
  onFile: (name: string) => void,
  onError: (err: Error) => void,
): { close(): void } {
  let markers: Markers = {};
  let watcher: fs.FSWatcher | null = null;
  let dead = false;
  let scanning = false;
  let dirty = false;

  const stopWatching = (): void => {
    const w = watcher;
    watcher = null;
    w?.close();
  };

  /** Death (D-22 policy is the caller's): close the watcher, report once, never call onFile again. */
  const die = (err: Error): void => {
    if (dead) return;
    dead = true;
    stopWatching();
    onError(err);
  };

  /** One readdir + stat of every non-dot regular file; a readdir failure rejects (→ death). */
  const readEntries = async (): Promise<DirEntry[]> => {
    const names = await fsp.readdir(dir);
    const entries: DirEntry[] = [];
    for (const name of names) {
      if (name.startsWith(".")) continue;
      try {
        const st = await fsp.stat(path.join(dir, name));
        if (st.isFile()) entries.push({ name, mtimeMs: st.mtimeMs, size: st.size, ino: st.ino });
      } catch {
        // vanished between readdir and stat: skipped silently, a later scan sees the truth
      }
    }
    return entries;
  };

  const scan = async (): Promise<void> => {
    scanning = true;
    let entries: DirEntry[];
    try {
      entries = await readEntries();
    } catch (err) {
      scanning = false;
      die(err instanceof Error ? err : new Error(String(err)));
      return;
    }
    const diff = diffMarkers(markers, entries);
    markers = diff.next;
    try {
      for (const name of diff.changed) {
        if (dead) break; // close() or death from inside a callback drops the rest of the batch
        onFile(name);
      }
    } finally {
      // exactly one more scan when events arrived while this one ran (bursts cost at most two)
      scanning = false;
      if (dirty && !dead) {
        dirty = false;
        void scan();
      }
    }
  };

  /** Every event means "something may have changed": ignore its type and name, scan again. */
  const requestScan = (): void => {
    if (dead) return;
    if (scanning) dirty = true;
    else void scan();
  };

  // Arm before scanning, so nothing written between arming and the first scan is lost.
  try {
    watcher = fs.watch(dir, { persistent: false, recursive: false }, () => requestScan());
  } catch (err) {
    // watchDir never throws: the watcher is dead, the error is reported asynchronously (not a timer).
    const e = err instanceof Error ? err : new Error(String(err));
    queueMicrotask(() => onError(e));
    return { close(): void {} };
  }
  watcher.on("error", (err: Error) => die(err));

  void scan(); // initial scan: files already present are reported once as new

  return {
    close(): void {
      if (dead) return;
      dead = true;
      stopWatching();
    },
  };
}
