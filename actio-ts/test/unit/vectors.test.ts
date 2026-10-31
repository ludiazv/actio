// Conformance vector runner (WP2-B-1): walks test/vectors/<schema>/{valid,invalid} and asserts
// each file against the matching validate.ts validator. One test() per schema folder; an unknown
// folder (or a missing one) fails the suite, so new schemas cannot slip in untested.
import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { test } from "node:test";
import {
  compileCapability,
  validateActio,
  validateAnnounce,
  validateCancellation,
  validateConfirmation,
  validateDiarium,
  validateError,
  validateRequest,
  validateResponse,
  validateSpec,
  validateStatus,
} from "../../src/validate.js";
import type { Validated } from "../../src/validate.js";

const VALIDATORS: Record<string, (x: unknown) => Validated<unknown>> = {
  actio: validateActio,
  announce: validateAnnounce,
  cancellation: validateCancellation,
  confirmation: validateConfirmation,
  diarium: validateDiarium,
  error: validateError,
  request: validateRequest,
  response: validateResponse,
  spec: validateSpec,
  status: validateStatus,
};

const VECTORS_DIR = path.join(import.meta.dirname, "..", "vectors");

test("vectors: the folder contains exactly the known schema folders", async () => {
  const folders = (await readdir(VECTORS_DIR, { withFileTypes: true }))
    .filter((e) => e.isDirectory())
    .map((e) => e.name)
    .sort();
  for (const name of folders) assert.ok(name in VALIDATORS, `unknown vector folder: ${name}`);
  for (const name of Object.keys(VALIDATORS)) assert.ok(folders.includes(name), `missing vector folder: ${name}`);
});

for (const schema of Object.keys(VALIDATORS)) {
  test(`vectors: ${schema}`, async () => {
    const validate = VALIDATORS[schema];
    for (const kind of ["valid", "invalid"] as const) {
      const kindDir = path.join(VECTORS_DIR, schema, kind);
      const files = (await readdir(kindDir))
        .filter((f) => f.endsWith(".json"))
        .sort();
      const min = kind === "valid" ? 3 : 5;
      assert.ok(files.length >= min, `${schema}/${kind}: ${files.length} files, need at least ${min}`);
      for (const file of files) {
        const obj = JSON.parse(await readFile(path.join(kindDir, file), "utf8"));
        const res = validate(obj);
        if (res.ok) {
          assert.equal(kind, "valid", `${schema}/${kind}/${file}: expected invalid, validator said ok`);
        } else {
          assert.equal(kind, "invalid", `${schema}/${kind}/${file}: expected valid, validator said: ${res.errors.join("; ")}`);
          assert.ok(res.errors.length > 0, `${schema}/${kind}/${file}: invalid files must yield non-empty errors`);
        }
      }
    }
  });
}

test("compileCapability: a non-strict capability schema is rejected at own registration", () => {
  const base = {
    name: "cap",
    description: "d",
    parameters: { type: "object" as const },
  };
  // a well-formed capability compiles (positive control)
  const ok = compileCapability({ ...base, actio: { result: { type: "object", properties: { a: { type: "string" } }, required: ["a"] } } });
  assert.equal(ok.validateParams({ a: "x" }), true);
  // an unknown keyword is rejected by the strict compile (own registration must throw)
  const bad = { ...base, actio: { result: { type: "object", propeties: { a: { type: "string" } } } } };
  assert.throws(() => compileCapability(bad), /strict/i);
});