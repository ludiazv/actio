// Sync providers (§2.6): none, delay, fsync+delay, mutagen. Exactly these four; parsed from
// the ACTIO_SYNC value selected in config.sync. Write hooks are applied by io.ts.
import type { SyncProvider } from "./types.js";

/**
 * Parses a `ACTIO_SYNC` value: `none` | `delay[:<ms>]` | `fsync+delay[:<ms>]` | `mutagen:<session>`.
 * Unknown values or a bad `<ms>` (not an integer in 1–10 000) throw (configuration error at start()).
 * The mutagen binary path comes from `ACTIO_MUTAGEN_BIN` (default `mutagen`). Mutagen run failures
 * never throw into the protocol logic: they are reported through the optional `onError` hook
 * (issues.md I-1), which the agent wires to its log event.
 */
export function createSyncProvider(value: string, onError?: (err: Error) => void): SyncProvider {
  throw new Error("not implemented");
}
