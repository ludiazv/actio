import assert from "node:assert/strict";
import { test } from "node:test";
import { parseParams } from "../src/tool.js";

test("parseParams passes objects and absent params through", () => {
  assert.deepEqual(parseParams({ a: 1 }), { a: 1 });
  assert.equal(parseParams(undefined), undefined);
});

test("parseParams reads a JSON string, as some models send it", () => {
  assert.deepEqual(parseParams('{"msg_prompt": "hi", "asap": true}'), { msg_prompt: "hi", asap: true });
});

test("parseParams refuses text that is not JSON", () => {
  assert.throws(() => parseParams("calculate 12+12"), /params must be a JSON object/);
});
