// Small pure utilities: time, ULID, nonce, sleep (§2.2).
import crypto from "node:crypto";

/** Milliseconds → `YYYY-MM-DDThh:mm:ssZ` (seconds resolution, UTC). */
export function toIso(ms: number): string {
  throw new Error("not implemented");
}

/** `YYYY-MM-DDThh:mm:ssZ` → milliseconds, or undefined when the string is not in that exact format. */
export function fromIso(iso: string): number | undefined {
  throw new Error("not implemented");
}

/** ULID in Crockford base32 (`^[0-7][0-9A-HJKMNP-TV-Z]{25}$`), 48-bit ms time + 80-bit random. */
export function ulid(clock?: () => number): string {
  throw new Error("not implemented");
}

/** 128-bit random nonce as 32 hex characters. */
export function nonce(): string {
  throw new Error("not implemented");
}

/** Resolves after `ms`; rejects when `signal` aborts. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  throw new Error("not implemented");
}
