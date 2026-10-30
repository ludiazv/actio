import assert from "node:assert/strict";
import { test } from "node:test";
import { fakeClock } from "../helpers/index.js";
import { fromIso, nonce, sleep, toIso, ulid } from "../../src/util.js";

test("toIso formats milliseconds to YYYY-MM-DDThh:mm:ssZ", () => {
  assert.equal(toIso(0), "1970-01-01T00:00:00Z");
  assert.equal(toIso(999), "1970-01-01T00:00:00Z");
  assert.equal(toIso(1000), "1970-01-01T00:00:01Z");
  assert.equal(toIso(1_700_000_000_000), "2023-11-14T22:13:20Z");
});

test("fromIso round-trips toIso and rejects bad input", () => {
  const ms = 1_700_000_000_000;
  const iso = toIso(ms);
  assert.equal(fromIso(iso), ms);
  assert.equal(fromIso("1970-01-01T00:00:00Z"), 0);
  assert.equal(fromIso("nope"), undefined);
  assert.equal(fromIso("2023-11-14T22:13:20.000Z"), undefined);
  assert.equal(fromIso("2023-11-14T22:13:20+00:00"), undefined);
  assert.equal(fromIso("2023-13-01T00:00:00Z"), undefined); // invalid month
  assert.equal(fromIso("2023-02-30T00:00:00Z"), undefined); // invalid day
});

test("ulid matches regex and encodes clock", () => {
  const clock = fakeClock(1_700_000_000_000);
  const id = ulid(clock);
  assert.match(id, /^[0-7][0-9A-HJKMNP-TV-Z]{25}$/);
  assert.equal(id.length, 26);

  const id2 = ulid(clock);
  assert.notEqual(id, id2);
});

test("ulid time part encodes injected clock value", () => {
  const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
  const decodeTime = (id: string): number => {
    let ts = 0;
    for (let i = 0; i < 10; i++) ts = ts * 32 + CROCKFORD.indexOf(id[i]);
    return ts;
  };
  for (const ms of [0, 1, 1_700_000_000_000, 0x4123456789ab, 0xffffffffffff]) {
    const id = ulid(fakeClock(ms));
    assert.match(id, /^[0-7][0-9A-HJKMNP-TV-Z]{25}$/);
    assert.equal(decodeTime(id), ms);
  }
});

test("nonce is 32 lowercase hex chars", () => {
  const n = nonce();
  assert.match(n, /^[0-9a-f]{32}$/);
  const n2 = nonce();
  assert.notEqual(n, n2);
});

test("sleep resolves after delay", async () => {
  const start = Date.now();
  await sleep(60);
  const elapsed = Date.now() - start;
  assert.ok(elapsed >= 50, `expected at least ~60ms, got ${elapsed}ms`);
});

test("sleep rejects on abort", async () => {
  const controller = new AbortController();
  const p = sleep(500, controller.signal);
  controller.abort();
  await assert.rejects(p, (err: Error) => err.name === "AbortError");
});

test("sleep rejects with already-aborted signal", async () => {
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(sleep(500, controller.signal), (err: Error) => err.name === "AbortError");
});
