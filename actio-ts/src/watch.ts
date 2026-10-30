// Event-driven folder watcher (§2.1, WP1): fs.watch events trigger a folder scan; no timers,
// no polling, no re-arming. Exact contract in the master plan (WP1).
export interface DirEntry {
  name: string;
  mtimeMs: number;
  size: number;
  ino: number;
}

/** Per-file change marker `mtimeMs:size:ino`. */
export type Markers = Record<string, string>;

/**
 * Pure marker comparison: names whose marker is new or different are reported as changed (in
 * lexical order); disappeared names lose their marker; `next` carries the markers to keep.
 */
export function diffMarkers(prev: Markers, entries: DirEntry[]): { changed: string[]; next: Markers } {
  throw new Error("not implemented");
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
  throw new Error("not implemented");
}
