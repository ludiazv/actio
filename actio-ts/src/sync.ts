// Sync providers (§2.6): none, delay, fsync+delay, mutagen. Exactly these four; parsed from
// the ACTIO_SYNC value selected in config.sync. Write hooks are applied by io.ts.
import { execFile } from "node:child_process";
import type { SyncProvider } from "./types.js";

const MIN_DELAY_MS = 1;
const MAX_DELAY_MS = 10_000;
const DEFAULT_DELAY_MS = 50;
const MUTAGEN_DEBOUNCE_MS = 200;

function nopProvider(fsync: boolean, delayMs: number): SyncProvider {
  return {
    fsync,
    delayMs,
    notify(): void {},
    flush(): Promise<void> {
      return Promise.resolve();
    },
    close(): Promise<void> {
      return Promise.resolve();
    },
  };
}

/** `<ms>` must be an integer in 1–10 000; omitted means the default 50. */
function parseDelayMs(value: string, raw: string | undefined): number {
  if (raw === undefined) return DEFAULT_DELAY_MS;
  if (!/^\d+$/.test(raw)) {
    throw new Error(
      `ACTIO_SYNC: invalid delay ${JSON.stringify(value)}: <ms> must be an integer between ` +
        `${MIN_DELAY_MS} and ${MAX_DELAY_MS}`,
    );
  }
  const ms = Number(raw);
  if (ms < MIN_DELAY_MS || ms > MAX_DELAY_MS) {
    throw new Error(
      `ACTIO_SYNC: invalid delay ${JSON.stringify(value)}: <ms> must be an integer between ` +
        `${MIN_DELAY_MS} and ${MAX_DELAY_MS}`,
    );
  }
  return ms;
}

/**
 * Debounced `mutagen sync flush <session>` provider. `notify()` collapses into one scheduled run
 * (200 ms); `flush()` runs one immediately; `close()` cancels a pending run. The binary path is
 * read from `ACTIO_MUTAGEN_BIN` (default `mutagen`) at run time. Failures (spawn error or
 * non-zero exit) call `onError` once per failed run and never throw.
 */
function mutagenProvider(session: string, onError?: (err: Error) => void): SyncProvider {
  let timer: NodeJS.Timeout | null = null;
  let running: Promise<void> | null = null;

  const run = (): Promise<void> => {
    if (running) return running;
    const bin = process.env.ACTIO_MUTAGEN_BIN || "mutagen";
    const p = new Promise<void>((resolve) => {
      execFile(bin, ["sync", "flush", session], (err) => {
        if (err) {
          const e = err instanceof Error ? err : new Error(String(err));
          try {
            onError?.(e);
          } catch {
            // a failing onError must not break the protocol
          }
        }
        resolve();
      });
    });
    running = p;
    void p.then(() => {
      if (running === p) running = null;
    });
    return p;
  };

  return {
    fsync: false,
    delayMs: 0,
    notify(): void {
      if (timer) return;
      timer = setTimeout(() => {
        timer = null;
        void run();
      }, MUTAGEN_DEBOUNCE_MS);
    },
    flush(): Promise<void> {
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      return run();
    },
    close(): Promise<void> {
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      return Promise.resolve();
    },
  };
}

/**
 * Parses a `ACTIO_SYNC` value: `none` | `delay[:<ms>]` | `fsync+delay[:<ms>]` | `mutagen:<session>`.
 * Unknown values or a bad `<ms>` (not an integer in 1–10 000) throw (configuration error at start()).
 * The mutagen binary path comes from `ACTIO_MUTAGEN_BIN` (default `mutagen`). Mutagen run failures
 * never throw into the protocol logic: they are reported through the optional `onError` hook
 * (issues.md I-1), which the agent wires to its log event.
 */
export function createSyncProvider(value: string, onError?: (err: Error) => void): SyncProvider {
  if (value === "none") return nopProvider(false, 0);
  const delay = /^(delay|fsync\+delay)(?::(\d+))?$/.exec(value);
  if (delay) return nopProvider(delay[1] === "fsync+delay", parseDelayMs(value, delay[2]));
  const mutagen = /^mutagen:(.+)$/.exec(value);
  if (mutagen) return mutagenProvider(mutagen[1], onError);
  throw new Error(
    `ACTIO_SYNC: unknown sync provider ${JSON.stringify(value)} (expected none, delay[:<ms>], ` +
      `fsync+delay[:<ms>] or mutagen:<session>)`,
  );
}