// Tests of the public API barrel: the entry re-exports what the plan names, so a consumer of the
// `actio-ts` package (which may not deep-import `src/…`) reaches every public API through it.
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActioAgent, checkParams } from "../../src/index.js";
import type { Capability } from "../../src/generated/types.js";

test("the entry exports checkParams as a function and the agent class", () => {
  assert.equal(typeof checkParams, "function");
  assert.equal(typeof ActioAgent, "function");
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
