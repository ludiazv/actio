// Tests of the diarium append and the five typed event builders.
import assert from "node:assert/strict";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { test } from "node:test";
import * as diarium from "../../src/diarium.js";
import { validateDiarium } from "../../src/validate.js";
import { makeRoot } from "../helpers/index.js";
import type {
  CancellationFile,
  ConfirmationFile,
  DiariumLine,
  Exchange,
  Header,
  RequestFile,
  ResponseFile,
} from "../../src/generated/types.js";
import type { Validated } from "../../src/validate.js";

// --- fixtures: a complete, valid exchange, so the inreq/outreq envelopes really validate ---

const ID = "alpha";
const T0 = 1_700_000_000_000; // 2023-11-14T22:13:20Z
const ISO = "2023-11-14T22:13:20Z";
const RID = "01J4ZZQ8H9T3WVXBH0M5K2C8P0";
const NONCE = "0123456789abcdef0123456789abcdef";
const TS = 1_700_000_000;

function header(over: Partial<Header> = {}): Header {
  return { rid: RID, src: "beta", dst: ID, datetime: ISO, type: "req", actio_version: "0.1", ...over };
}

const REQ_FILE: RequestFile = {
  ...header(),
  request: {
    ts: TS,
    nonce: NONCE,
    signature: null,
    name: "translate",
    parameters: { text: "hi" },
    timeout: 30,
    attachments: [],
  },
};
// Cancellation, response and confirmation files carry a RequestHeader: their `type` must be "req".
const REQ_HEADER = { rid: RID, src: "beta", dst: ID, datetime: ISO, type: "req", actio_version: "0.1" } as const;
const CAN_FILE: CancellationFile = {
  ...REQ_HEADER,
  cancellation: { ts: TS, nonce: NONCE, signature: null, name: "translate", force: false },
};
const RES_FILE: ResponseFile = {
  ...REQ_HEADER,
  response: {
    ts: TS,
    nonce: NONCE,
    signature: null,
    error: false,
    cancellation: false,
    result: { text: "ho" },
    attachments: [],
  },
};
const CON_FILE: ConfirmationFile = { ...REQ_HEADER, confirmation: { ts: TS, nonce: NONCE, signature: null } };

/** An exchange whose envelopes are all present; `final_status` is a placeholder on purpose. */
const EXCHANGE: Exchange = { final_status: "placeholder", req: REQ_FILE, can: CAN_FILE, res: RES_FILE, con: CON_FILE };

/** Only `req` is filled: a command just written (B1) or an exchange that got no further. */
const REQ_ONLY: Exchange = { final_status: "placeholder", req: REQ_FILE, can: null, res: null, con: null };

const diariumFile = (root: string): string => path.join(root, "diarium.jsonl");

/** `append` throws with this prefix when `validateDiarium` rejects the event. */
const INVALID_THROWN = /diarium\.ts: invalid/;

/** The JSON objects of a diarium file, one per line. */
async function readLines(file: string): Promise<unknown[]> {
  return (await readFile(file, "utf8"))
    .split("\n")
    .filter((l) => l !== "")
    .map((l) => JSON.parse(l));
}

/** Error text of a failed validation, for assertion messages. */
function errorText(r: Validated<unknown>): string {
  return r.ok ? "(accepted)" : r.errors.join(" | ");
}

test("the five builders produce lines that pass validateDiarium", () => {
  const built = [
    diarium.statusEvent(ID, T0, "idle"),
    diarium.statusEvent(ID, T0, "busy"),
    diarium.statusEvent(ID, T0, "inactive"),
    diarium.statusEvent(ID, T0, "error"),
    diarium.announceEvent(ID, T0),
    diarium.logEvent(ID, T0, "watcher died"),
    diarium.inreqEvent(ID, T0, "confirmed", EXCHANGE),
    diarium.inreqEvent(ID, T0, "dropped", REQ_ONLY),
    diarium.outreqEvent(ID, T0, "sent", REQ_ONLY),
    diarium.outreqEvent(ID, T0, "abandoned", { ...REQ_ONLY, res: null }),
  ];
  for (const line of built) assert.ok(validateDiarium(line).ok, errorText(validateDiarium(line)));
});

test("every builder stamps id, the datetime of nowMs and the event name", () => {
  const cases: [DiariumLine, string][] = [
    [diarium.statusEvent(ID, T0, "idle"), "status"],
    [diarium.announceEvent(ID, T0), "announce"],
    [diarium.logEvent(ID, T0, "hello"), "log"],
    [diarium.inreqEvent(ID, T0, "completed", REQ_ONLY), "inreq"],
    [diarium.outreqEvent(ID, T0, "confirmed", EXCHANGE), "outreq"],
  ];
  for (const [line, event] of cases) {
    assert.equal(line.id, ID);
    assert.equal(line.datetime, ISO);
    assert.equal(line.event, event);
    assert.deepEqual(Object.keys(line).sort(), ["datetime", "event", "event_data", "id"]); // no extra field
  }
});

test("the datetime is the second-resolution ISO of nowMs (toIso)", () => {
  assert.equal(diarium.logEvent(ID, 1_700_000_000_999, "x").datetime, ISO); // floors within the second
  assert.equal(diarium.logEvent(ID, 1_700_000_001_000, "x").datetime, "2023-11-14T22:13:21Z");
  assert.equal(diarium.statusEvent(ID, 0, "idle").datetime, "1970-01-01T00:00:00Z");
});

test("status, announce and log carry the event_data the protocol defines", () => {
  for (const state of ["inactive", "idle", "busy", "error"] as const) {
    assert.deepEqual(diarium.statusEvent(ID, T0, state), { id: ID, datetime: ISO, event: "status", event_data: state });
  }
  assert.deepEqual(diarium.announceEvent(ID, T0), { id: ID, datetime: ISO, event: "announce", event_data: null });
  assert.deepEqual(diarium.logEvent(ID, T0, "a \"quoted\" msg"), {
    id: ID,
    datetime: ISO,
    event: "log",
    event_data: "a \"quoted\" msg",
  });
  assert.equal("event_data" in diarium.announceEvent(ID, T0), true); // event_data is required, null included
});

test("inreqEvent and outreqEvent stamp the final status over the envelopes", () => {
  const inreq = diarium.inreqEvent(ID, T0, "confirmed", EXCHANGE);
  assert.deepEqual(inreq.event_data, { ...EXCHANGE, final_status: "confirmed" });
  const outreq = diarium.outreqEvent(ID, T0, "sent", REQ_ONLY);
  assert.deepEqual(outreq.event_data, { ...REQ_ONLY, final_status: "sent" });
  // the envelopes are stored as exchanged, in the same order and identity
  assert.equal(inreq.event_data.req, REQ_FILE);
  assert.deepEqual(inreq.event_data.con, CON_FILE);
  assert.deepEqual(outreq.event_data.can, null);
  // the caller's Exchange is not modified by the builder
  assert.equal(EXCHANGE.final_status, "placeholder");
  assert.equal(REQ_ONLY.final_status, "placeholder");
  for (const status of ["cancelled", "completed", "confirmed", "dropped"] as const) {
    assert.equal(diarium.inreqEvent(ID, T0, status, EXCHANGE).event_data.final_status, status);
  }
  for (const status of ["confirmed", "cancelled", "abandoned", "sent"] as const) {
    assert.equal(diarium.outreqEvent(ID, T0, status, EXCHANGE).event_data.final_status, status);
  }
});

test("append writes <folder>/diarium.jsonl with one validated object per line", async (t) => {
  const root = await makeRoot(t);
  const events = [
    diarium.statusEvent(ID, T0, "idle"),
    diarium.announceEvent(ID, T0),
    diarium.inreqEvent(ID, T0, "confirmed", EXCHANGE),
    diarium.logEvent(ID, T0 + 1000, "done"),
  ];
  for (const event of events) await diarium.append(root, event);

  const written = await readLines(diariumFile(root));
  assert.deepEqual(written, events); // read back exactly what was built
  for (const line of written) assert.ok(validateDiarium(line).ok, errorText(validateDiarium(line)));
  assert.equal(
    await readFile(diariumFile(root), "utf8"),
    events.map((e) => `${JSON.stringify(e)}\n`).join(""), // single line each, no blank line
  );
});

test("append creates the file on the first event and appends thereafter", async (t) => {
  const root = await makeRoot(t);
  assert.equal(await readLines(diariumFile(root)).catch(() => "missing"), "missing");
  await diarium.append(root, diarium.logEvent(ID, T0, "first"));
  assert.deepEqual((await readLines(diariumFile(root))).length, 1);
  await diarium.append(root, diarium.logEvent(ID, T0, "second"));
  assert.deepEqual((await readLines(diariumFile(root))).length, 2);
});

test("append throws on an invalid event and writes nothing", async (t) => {
  const root = await makeRoot(t);
  const invalid: Record<string, unknown>[] = [
    { id: ID, datetime: ISO, event: "status", event_data: "sleeping" }, // not one of the four states
    { id: ID, datetime: ISO, event: "status" }, // event_data is required
    { id: ID, datetime: ISO, event: "announce", event_data: "hello" }, // announce implies null
    { id: ID, datetime: ISO, event: "log", event_data: 5 }, // log implies a string
    { id: ID, datetime: ISO, event: "", event_data: null }, // event minLength
    { id: "bad id", datetime: ISO, event: "log", event_data: "x" }, // agentId pattern
    { id: ID, datetime: "2023-11-14T22:13:20", event: "log", event_data: "x" }, // datetime pattern
    { id: ID, datetime: ISO, event: "log", event_data: "x", extra: 1 }, // additionalProperties
    { id: ID, datetime: ISO, event: "inreq", event_data: { ...REQ_ONLY, final_status: "sent" } }, // not an inreq status
    { id: ID, datetime: ISO, event: "outreq", event_data: { ...REQ_ONLY, final_status: "dropped" } }, // not outreq
    { id: ID, datetime: ISO, event: "inreq", event_data: { ...REQ_ONLY, can: {} } }, // envelope not a cancellation file
    { id: ID, datetime: ISO, event: "inreq", event_data: { final_status: "confirmed" } }, // envelopes missing
  ];
  for (const event of invalid) {
    await assert.rejects(() => diarium.append(root, event as DiariumLine), INVALID_THROWN, JSON.stringify(event));
  }
  // the Ajv reason is part of the message
  await assert.rejects(() => diarium.append(root, invalid[0] as DiariumLine), /allowed values/);
  await assert.rejects(() => diarium.append(root, invalid[1] as DiariumLine), /required property 'event_data'/);
  assert.equal(await readLines(diariumFile(root)).catch(() => "missing"), "missing"); // nothing was written
});

test("append validates, it does not restrict the events: custom events are allowed", async (t) => {
  const root = await makeRoot(t);
  const custom: DiariumLine = { id: ID, datetime: ISO, event: "my-agent-event", event_data: { anything: [1, 2] } };
  await diarium.append(root, custom);
  assert.deepEqual(await readLines(diariumFile(root)), [custom]);
});

test("append propagates a write failure (missing folder) instead of swallowing it", async (t) => {
  const root = path.join(await makeRoot(t), "not-created");
  await assert.rejects(() => diarium.append(root, diarium.logEvent(ID, T0, "x")), { code: "ENOENT" });
});

// --- read(): the valid lines in file order, the ignored lines with a reason each ---

test("read: a mixed file gives the valid lines and the ignored lines with a reason", async (t) => {
  const folder = path.join(await makeRoot(t), ID);
  await mkdir(folder);
  const valid1 = JSON.stringify({ id: ID, datetime: ISO, event: "log", event_data: "one" });
  const invalid = JSON.stringify({ id: ID, datetime: "not-a-date", event: "log", event_data: "x" });
  const valid2 = JSON.stringify({ id: ID, datetime: ISO, event: "log", event_data: "two" });
  const partial = `{"id":"${ID}","datetime":"${ISO}","event":"log","event_data":"cut`;
  await writeFile(diariumFile(folder), `${valid1}\nnot json\n${invalid}\n${valid2}\n${partial}`);
  const read = await diarium.read(folder);
  assert.deepEqual(read.lines.map((l) => l.event_data), ["one", "two"], "the valid lines, in file order");
  assert.equal(read.ignored.length, 2, "the partial last line is not reported (an append may be in progress)");
  assert.deepEqual(read.ignored[0], { line: 2, reason: "not valid JSON" });
  assert.equal(read.ignored[1].line, 3);
  assert.match(read.ignored[1].reason, /datetime/, "the first Ajv error of the schema-invalid line");
});

test("read: a missing file gives the empty result", async (t) => {
  const root = await makeRoot(t);
  assert.deepEqual(await diarium.read(path.join(root, "never-created")), { lines: [], ignored: [] });
});
