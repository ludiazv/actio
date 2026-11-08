// Small pure utilities: time, ULID, nonce, sleep.
import crypto from "node:crypto";

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/** Milliseconds → `YYYY-MM-DDThh:mm:ssZ` (seconds resolution, UTC). */
export function toIso(ms: number): string {
  const d = new Date(Math.floor(ms / 1000) * 1000);
  return d.toISOString().replace(/\.\d{3}Z$/, "Z");
}

/** `YYYY-MM-DDThh:mm:ssZ` → milliseconds, or undefined when the string is not in that exact format. */
export function fromIso(iso: string): number | undefined {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(iso)) return undefined;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return undefined;
  if (toIso(d.getTime()) !== iso) return undefined;
  return d.getTime();
}

/** ULID in Crockford base32 (`^[0-7][0-9A-HJKMNP-TV-Z]{25}$`), 48-bit ms time + 80-bit random. */
export function ulid(clock?: () => number): string {
  const ts = BigInt(clock?.() ?? Date.now()) & 0xffffffffffffn;
  let out = CROCKFORD[Number(ts >> 45n) & 0x07];
  for (let shift = 40n; shift >= 0n; shift -= 5n) {
    out += CROCKFORD[Number(ts >> shift) & 0x1f];
  }

  const rand = crypto.randomBytes(10); // 80 bits
  let bitBuffer = 0;
  let bitCount = 0;
  for (let i = 0; i < rand.length; i++) {
    bitBuffer = (bitBuffer << 8) | rand[i];
    bitCount += 8;
    while (bitCount >= 5) {
      bitCount -= 5;
      out += CROCKFORD[(bitBuffer >> bitCount) & 0x1f];
    }
  }

  return out;
}

/** 128-bit random nonce as 32 hex characters. */
export function nonce(): string {
  return crypto.randomBytes(16).toString("hex");
}

/** Resolves after `ms`; rejects when `signal` aborts. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      const err = new Error("AbortError");
      err.name = "AbortError";
      reject(err);
      return;
    }
    const timer = setTimeout(resolve, ms);
    if (!signal) return;
    const onAbort = () => {
      clearTimeout(timer);
      const err = new Error("AbortError");
      err.name = "AbortError";
      reject(err);
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}
