// Sync providers (§2.6): none, delay, fsync+delay, mutagen. Exactly these four; parsed from
// the ACTIO_SYNC value selected in config.sync. Write hooks are applied by io.ts.
import type { SyncProvider } from "./types.js";

/**
 * Parses a `ACTIO_SYNC` value: `none` | `delay[:<ms>]` | `fsync+delay[:<ms>]` | `mutagen:<session>`.
 * Unknown values or a bad `<ms>` (not an integer in 1–10 000) throw (configuration error at start()).
 * The mutagen binary path comes from `ACTIO_MUTAGEN_BIN` (default `mutagen`).
 */
export function createSyncProvider(value: string): SyncProvider {
  throw new Error("not implemented");
}
