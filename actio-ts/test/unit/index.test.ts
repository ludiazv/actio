// Tests of the public API barrel: the entry re-exports what the plan names, so a consumer of the
// `actio-ts` package (which may not deep-import `src/…`) reaches every public API through it.
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActioAgent, checkParams, health, readDiarium, scan } from "../../src/index.js";
import type { Capability, DiariumLine, DiscoveredAgent, Spec, Status } from "../../src/index.js";

test("the entry exports checkParams as a function and the agent class", () => {
  assert.equal(typeof checkParams, "function");
  assert.equal(typeof ActioAgent, "function");
});

test("the entry exports scan, health and readDiarium as functions", () => {
  assert.equal(typeof scan, "function");
  assert.equal(typeof health, "function");
  assert.equal(typeof readDiarium, "function");
});

test("the protocol types are nameable through the entry (type-only import)", () => {
  const line: DiariumLine = { id: "alpha", datetime: "2023-11-14T22:13:20Z", event: "log", event_data: { msg: "hi" } };
  const spec: Spec | null = null;
  const status: Status | null = null;
  const discovered: DiscoveredAgent | null = null;
  assert.equal(line.event, "log");
  assert.equal(spec, null);
  assert.equal(status, null);
  assert.equal(discovered, null);
});

const cap: Capability = {
  name: "entry-cap",
  description: "",
  parameters: {
    type: "object",
    properties: { text: { type: "string" } },
    required: ["text"],
    additionalProperties: false,
  },
  actio: { result: true },
};

test("checkParams reached through the entry: a string for invalid params, undefined for valid", () => {
  const bad = checkParams(cap, {});
  assert.equal(typeof bad, "string", "an invalid-params case returns the error text");
  assert.equal(checkParams(cap, { text: "hello" }), undefined, "a valid case returns undefined");
});
