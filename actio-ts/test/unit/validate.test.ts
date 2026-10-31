// Tests of the runtime validators, the non-schema rules and capability compilation (WP2-A).
import assert from "node:assert/strict";
import { test } from "node:test";
import type { Validated } from "../../src/validate.js";
import {
  compileCapability,
  envelopeKind,
  nameMatches,
  sameHeader,
  uniqueCapabilityNames,
  validateActio,
  validateAnnounce,
  validateCancellation,
  validateCancellationBody,
  validateConfirmation,
  validateConfirmationBody,
  validateDiarium,
  validateError,
  validateRequest,
  validateRequestBody,
  validateResponse,
  validateResponseBody,
  validateSpec,
  validateStatus,
  type CompiledCapability,
} from "../../src/validate.js";
import type { Capability, CancellationBody, Header, RequestBody, Spec } from "../../src/generated/types.js";

// --- fixtures (hand-written; the vector suite of WP2-B covers every schema branch) ---

const RID = "01J4ZZQ8H9T3WVXBH0M5K2C8P0";
const NONCE = "0123456789abcdef0123456789abcdef";
const ISO = "2026-01-01T00:00:00Z";
const TS = 1_767_225_600;
const CRYPT = "QUJDRA=="; // an encrypted body: base64, never decrypted in these tests

function header(over: Partial<Header> = {}): Header {
  return { rid: RID, src: "alpha", dst: "beta", datetime: ISO, type: "req", actio_version: "0.1", ...over };
}

/** Body builders take loose overrides: invalid fixtures are documents the generated types forbid. */
function reqBody(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    ts: TS,
    nonce: NONCE,
    signature: null,
    name: "translate",
    parameters: { text: "hello" },
    timeout: 30,
    attachments: [],
    ...over,
  };
}

function canBody(over: Record<string, unknown> = {}): Record<string, unknown> {
  return { ts: TS, nonce: NONCE, signature: null, name: "translate", force: false, ...over };
}

function resBody(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    ts: TS,
    nonce: NONCE,
    signature: null,
    error: false,
    cancellation: false,
    result: { text: "hi" },
    attachments: [],
    ...over,
  };
}

function exchange(final: string, over: Record<string, unknown> = {}): Record<string, unknown> {
  return { final_status: final, req: { ...header(), request: reqBody() }, can: null, res: null, con: null, ...over };
}

/** An envelope file of the given kind, with a body and optional header overrides. */
function envelopeOf(kind: string, body: unknown, over: Partial<Header> = {}): Record<string, unknown> {
  return { ...header(over), [kind]: body };
}

/** A type "cmd" request envelope: the command name and parameters under test. */
function cmdOf(name: string, parameters: unknown): Record<string, unknown> {
  return envelopeOf("request", reqBody({ name, parameters }), { type: "cmd" });
}

/** One diarium line for the given event and event_data. */
function line(event: string, event_data: unknown): Record<string, unknown> {
  return { id: "alpha", datetime: ISO, event, event_data };
}

const specCapability: Capability = {
  name: "translate",
  description: "Translates text",
  parameters: {
    type: "object",
    properties: { text: { type: "string" } },
    required: ["text"],
    additionalProperties: false,
  },
  actio: { result: { type: "object", properties: { text: { type: "string" } }, required: ["text"] } },
};

function specOf(over: Partial<Spec> = {}): Spec {
  return {
    id: "alpha",
    actio_version: "0.1",
    datetime: ISO,
    description: "A translation agent",
    accepts_commands_from: ["ops"],
    capabilities: [specCapability],
    security_support: [],
    security_posture: "trusted",
    ...over,
  };
}

/** Typed bodies for the rule functions, which take the generated types, not unknown. */
function reqNamed(name: string): RequestBody {
  return { ts: TS, nonce: NONCE, signature: null, name, parameters: {}, timeout: 0, attachments: [] };
}

function canNamed(name: string): CancellationBody {
  return { ts: TS, nonce: NONCE, signature: null, name, force: false };
}

/** Error texts of a failed validation, for assertion messages. */
function errorText(r: Validated<unknown>): string {
  return r.ok ? "(accepted)" : r.errors.join(" | ");
}

/** One validator with at least one genuinely valid and one genuinely invalid document. */
interface Case {
  label: string;
  validate: (x: unknown) => Validated<unknown>;
  valid: unknown[];
  invalid: unknown[];
}

const cases: Case[] = [
  {
    label: "actio",
    validate: validateActio,
    valid: [
      {},
      { actio_version: "0.1" },
      { actio_version: "0.1", description: "the team", metadata: { anything: [1, 2] } },
    ],
    invalid: [{ team: "x" }, { actio_version: "1" }, { description: 3 }],
  },
  {
    label: "spec",
    validate: validateSpec,
    valid: [
      specOf(),
      specOf({ namespaces: ["*", null] }),
      specOf({ depends: ["beta"], info: { note: 1 }, disabled: true }),
    ],
    invalid: [
      { ...specOf(), security_posture: undefined }, // required
      { ...specOf(), security_support: ["encrypts"] }, // enum
      { ...specOf(), capabilities: [{ ...specCapability, extra: 1 }] }, // additionalProperties
      { ...specOf(), id: "not an id!" }, // agentId pattern
    ],
  },
  {
    label: "status",
    validate: validateStatus,
    valid: [
      { id: "alpha", status: "idle", datetime: ISO, capability_status: [{ name: "translate", running: 0, queue: 0 }] },
      { id: "alpha", status: "busy", datetime: ISO, capability_status: [], msg: "working", load: 50, contention: true },
      {
        id: "alpha",
        status: "error",
        datetime: ISO,
        capability_status: [],
        tasks: [{ capability: "translate", status: "running", progress: 10, eta: "01:02:03" }],
      },
    ],
    invalid: [
      { id: "alpha", status: "sleeping", datetime: ISO, capability_status: [] }, // enum
      { id: "alpha", status: "idle", datetime: ISO, capability_status: [], load: 101 }, // maximum
      {
        id: "alpha",
        status: "idle",
        datetime: ISO,
        capability_status: [{ name: "translate", running: 0 }], // queue missing
      },
      { id: "alpha", status: "idle", datetime: ISO, capability_status: [], unknown: 1 }, // additionalProperties
    ],
  },
  {
    label: "announce",
    validate: validateAnnounce,
    valid: [
      { id: "alpha", datetime: ISO, ts: TS, nuntium: "agent is active" },
      { id: "alpha-2", datetime: ISO, ts: 0, nuntium: "" },
      { id: "alpha_2", datetime: "2026-06-30T23:59:59Z", ts: TS, nuntium: "back online" },
    ],
    invalid: [
      { id: "alpha", datetime: ISO, ts: TS }, // required nuntium
      { id: "alpha", datetime: ISO, ts: TS, nuntium: "x", extra: 1 }, // additionalProperties
      { id: "alpha", datetime: "2026-01-01 00:00:00", ts: TS, nuntium: "x" }, // datetime pattern
      { id: "", datetime: ISO, ts: TS, nuntium: "x" }, // agentId minLength
    ],
  },
  {
    label: "request",
    validate: validateRequest,
    valid: [
      envelopeOf("request", reqBody()),
      envelopeOf("request", CRYPT), // encrypted body
      cmdOf("enable", { enabled: true }),
      cmdOf("stop", {}),
      cmdOf("stop", { force: true }),
      cmdOf("ping", null),
      cmdOf("restart", { force: true, delay: 5 }),
      cmdOf("custom", { any: [1, 2] }), // a custom command is free-form
    ],
    invalid: [
      envelopeOf("request", reqBody({ name: "bad name" })), // capabilityName pattern (requestRules)
      envelopeOf("request", reqBody({ parameters: "text" })), // req parameters: object or null
      cmdOf("enable", {}), // enable requires enabled
      cmdOf("enable", { enabled: "yes" }), // enable: boolean
      cmdOf("stop", null), // stop: parameters is an object
      cmdOf("stop", { bogus: 1 }), // stop: no other field
      cmdOf("ping", {}), // ping: parameters must be null
      cmdOf("restart", { delay: -1 }), // duration minimum
      { ...header(), request: reqBody(), extra: 1 }, // unevaluatedProperties
      { ...header() }, // no body at all
      { ...header({ rid: "bad" }), request: reqBody() }, // rid pattern
    ],
  },
  {
    label: "cancellation",
    validate: validateCancellation,
    valid: [
      envelopeOf("cancellation", canBody()),
      envelopeOf("cancellation", canBody({ force: true })),
      envelopeOf("cancellation", CRYPT), // encrypted body
    ],
    invalid: [
      envelopeOf("cancellation", canBody(), { type: "cmd" }), // header type must be "req"
      envelopeOf("cancellation", canBody({ force: "yes" })), // force must be boolean
      envelopeOf("cancellation", canBody({ name: "bad name" })), // capabilityName pattern
      envelopeOf("cancellation", canBody({ extra: 1 })), // additionalProperties
      envelopeOf("response", canBody()), // wrong body key
    ],
  },
  {
    label: "response",
    validate: validateResponse,
    valid: [
      envelopeOf("response", resBody()),
      envelopeOf("response", resBody({ cancellation: true, result: null })), // cancellation ack
      envelopeOf("response", resBody({ error: true, result: { code: 100, msg: "unknown capability" } })),
      envelopeOf("response", resBody({ work_time: 1.5, attachments: ["out.bin"] })),
      envelopeOf("response", CRYPT), // encrypted body
    ],
    invalid: [
      envelopeOf("response", resBody({ error: true, cancellation: true, result: null })), // ack implies error false
      envelopeOf("response", resBody({ cancellation: true, result: { text: "hi" } })), // ack implies result null
      envelopeOf("response", resBody({ cancellation: true, result: null, attachments: ["a"] })), // ack: no attachments
      envelopeOf("response", resBody({ error: true, result: {} })), // error result needs msg
      envelopeOf("response", resBody({ attachments: ["/abs"] })), // attachments are relative
      envelopeOf("response", resBody(), { type: "cmd" }), // type must be "req"
    ],
  },
  {
    label: "confirmation",
    validate: validateConfirmation,
    valid: [
      envelopeOf("confirmation", { ts: TS, nonce: NONCE, signature: null }),
      envelopeOf("confirmation", { ts: TS, nonce: NONCE, signature: CRYPT }),
      envelopeOf("confirmation", CRYPT), // encrypted body
    ],
    invalid: [
      envelopeOf("confirmation", { ts: TS, nonce: NONCE }), // required signature
      envelopeOf("confirmation", { ts: TS, nonce: NONCE, signature: null, name: "translate" }), // additionalProperties
      envelopeOf("confirmation", { ts: TS, nonce: "tooshort", signature: null }), // nonce pattern
    ],
  },
  {
    label: "error result",
    validate: validateError,
    valid: [{ msg: "boom" }, { code: 100, msg: "boom" }, { code: -3, msg: "boom", details: "because" }],
    invalid: [{}, { msg: 3 }, { msg: "x", extra: 1 }, { code: "100", msg: "x" }],
  },
  {
    label: "diarium",
    validate: validateDiarium,
    valid: [
      line("status", "idle"),
      line("announce", null),
      line("log", "watcher died"),
      line("inreq", exchange("completed")),
      line("outreq", exchange("sent")),
      line("custom", { anything: true }), // implementation-defined event
    ],
    invalid: [
      line("status", "sleeping"), // status enum
      line("announce", "hello"), // announce implies null
      line("log", 5), // log implies a string
      line("inreq", exchange("sent")), // not an inreq final status
      line("outreq", { final_status: "confirmed", req: null, can: null, res: null }), // con required
      { id: "alpha", datetime: ISO, event: "status" }, // event_data required
      line("", null), // event minLength
    ],
  },
  {
    label: "request body",
    validate: validateRequestBody,
    valid: [
      reqBody(),
      reqBody({ name: "custom", parameters: null, timeout: 0, attachments: ["a/b.bin"] }),
      reqBody({ signature: CRYPT, name: "a-b_C.9", parameters: { nested: { deep: [1] } } }),
    ],
    invalid: [
      { ...reqBody(), timeout: undefined }, // required
      { ...reqBody(), attachments: ["/abs"] }, // relative paths only
      { ...reqBody(), nonce: "xyz" }, // nonce pattern
      { ts: TS }, // most fields missing
    ],
  },
  {
    label: "cancellation body",
    validate: validateCancellationBody,
    valid: [canBody(), canBody({ force: true }), canBody({ name: "a-b_C" })],
    invalid: [{ ...canBody(), force: undefined }, { ...canBody(), name: "bad name" }, { ...canBody(), parameters: {} }],
  },
  {
    label: "response body",
    validate: validateResponseBody,
    valid: [
      resBody(),
      resBody({ cancellation: true, result: null }),
      resBody({ error: true, result: { msg: "boom" }, work_time: 0 }),
    ],
    invalid: [
      resBody({ error: true, cancellation: true, result: null }),
      resBody({ work_time: -1 }), // duration minimum
      { ts: TS, nonce: NONCE, signature: null, error: false, result: {}, attachments: [] }, // cancellation required
    ],
  },
  {
    label: "confirmation body",
    validate: validateConfirmationBody,
    valid: [
      { ts: TS, nonce: NONCE, signature: null },
      { ts: TS, nonce: NONCE, signature: CRYPT },
      { ts: 0, nonce: NONCE.toUpperCase(), signature: null }, // hex is case-insensitive
    ],
    invalid: [
      { ts: TS, nonce: NONCE }, // required signature
      { ts: TS, nonce: NONCE, signature: null, extra: 1 }, // additionalProperties
      { ts: `${TS}`, nonce: NONCE, signature: null }, // ts must be an integer
    ],
  },
];

for (const c of cases) {
  test(`validate ${c.label}: accepts valid and rejects invalid documents`, () => {
    assert.ok(c.valid.length >= 1 && c.invalid.length >= 1);
    for (const doc of c.valid) {
      const r = c.validate(doc);
      assert.equal(r.ok, true, `${c.label}: expected valid, got: ${errorText(r)}`);
    }
    for (const doc of c.invalid) {
      const r = c.validate(doc);
      assert.equal(r.ok, false, `${c.label}: expected invalid, accepted: ${JSON.stringify(doc)}`);
      if (!r.ok) {
        assert.ok(r.errors.length >= 1, `${c.label}: rejected without an error text`);
        assert.ok(r.errors.every((e) => typeof e === "string" && e.length > 0), `${c.label}: non-string error text`);
      }
    }
  });
}

test("a validator narrows to the generated type and keeps the same object", () => {
  const spec = specOf();
  const r = validateSpec(spec);
  if (!r.ok) assert.fail(`valid spec rejected: ${errorText(r)}`);
  assert.equal(r.value.id, "alpha");
  assert.equal(r.value.capabilities[0].name, "translate");
  assert.equal(r.value, spec); // no copy, no default filling: the input is the value
});

test("errors[0] is the most specific Ajv error (D-15 message)", () => {
  const r = validateRequest(cmdOf("enable", {}));
  if (r.ok) assert.fail("expected the enable command to be rejected");
  assert.match(r.errors[0], /required property 'enabled'/);
});

test("a validator rejects a non-object without throwing", () => {
  for (const x of [null, undefined, 7, "text", []]) {
    assert.equal(validateSpec(x).ok, false);
    assert.equal(validateRequestBody(x).ok, false);
    assert.equal(validateActio(x).ok, false);
  }
});

test("envelopeKind reads the body key", () => {
  assert.equal(envelopeKind({ ...header(), request: reqBody() }), "request");
  assert.equal(envelopeKind({ ...header(), cancellation: canBody() }), "cancellation");
  assert.equal(envelopeKind({ ...header(), response: resBody() }), "response");
  assert.equal(envelopeKind({ ...header(), confirmation: { ts: TS, nonce: NONCE, signature: null } }), "confirmation");
  assert.equal(envelopeKind({ request: CRYPT }), "request"); // an encrypted body is still a request
});

test("envelopeKind returns undefined for anything else", () => {
  const others: unknown[] = [
    undefined,
    null,
    0,
    "",
    "request",
    [],
    [{ request: {} }],
    {},
    { datetime: ISO },
    { request: {}, response: {} }, // two body keys: not one envelope
    { body: {} },
  ];
  for (const x of others) assert.equal(envelopeKind(x), undefined, `expected undefined for ${JSON.stringify(x)}`);
});

test("sameHeader ignores datetime only", () => {
  assert.ok(sameHeader(header(), header()));
  assert.ok(sameHeader(header(), header({ datetime: "2026-01-02T10:00:00Z" })));
  for (const bad of [
    header({ rid: "01J4ZZQ8H9T3WVXBH0M5K2C8P1" }),
    header({ src: "gamma" }),
    header({ dst: "gamma" }),
    header({ type: "cmd" }),
    header({ actio_version: "0.2" }),
  ]) {
    assert.ok(!sameHeader(header(), bad), `expected a difference for ${JSON.stringify(bad)}`);
  }
});

test("nameMatches compares the capability name", () => {
  assert.ok(nameMatches(reqNamed("translate"), canNamed("translate")));
  assert.ok(!nameMatches(reqNamed("translate"), canNamed("summarize")));
});

test("uniqueCapabilityNames is about names, which the schema cannot express", () => {
  assert.ok(uniqueCapabilityNames(specOf()));
  assert.ok(uniqueCapabilityNames(specOf({ capabilities: [] })));
  const dup = specOf({ capabilities: [specCapability, { ...specCapability, description: "same name" }] });
  assert.ok(!uniqueCapabilityNames(dup));
  assert.ok(validateSpec(dup).ok); // schema-valid and rule-invalid: the rule is the code's job
});

// --- capability schemas (D-19) ---

/** A capability with arbitrary (possibly non-strict) schemas, which the generated type forbids. */
function capability(parameters: unknown, result: unknown): Capability {
  return { name: "peer-cap", description: "", parameters, actio: { result } } as unknown as Capability;
}

test("compileCapability validates params and result against the capability schemas", () => {
  const compiled = compileCapability(
    capability(
      { type: "object", properties: { text: { type: "string" } }, required: ["text"], additionalProperties: false },
      { type: "object", properties: { text: { type: "string" } }, required: ["text"] },
    ),
  );
  assert.equal(compiled.validateParams({ text: "hello" }), true);
  assert.equal(compiled.validateParams({}), false);
  assert.equal(compiled.validateParams({ text: 3 }), false);
  assert.equal(compiled.validateParams("text"), false);
  assert.equal(compiled.validateResult({ text: "hi" }), true);
  assert.equal(compiled.validateResult({}), false);
  assert.equal(compiled.validateResult(null), false);
});

test("compileCapability keeps the params and result checks independent", () => {
  const compiled = compileCapability(
    capability({ type: "object", required: ["a"], properties: { a: { type: "string" } } }, { type: "number" }),
  );
  assert.equal(compiled.validateParams({}), false); // a failing params check must not spoil the next result check
  assert.equal(compiled.validateResult(7), true);
  assert.equal(compiled.validateParams({ a: "x" }), true);
  assert.equal(compiled.validateResult("7"), false);
});

test("compileCapability accepts an empty parameters schema and boolean result schemas", () => {
  const anyParams = compileCapability(capability({}, { type: "boolean" }));
  assert.equal(anyParams.validateParams({ whatever: [1, 2, 3] }), true);
  assert.equal(anyParams.validateResult(true), true);
  assert.equal(anyParams.validateResult("x"), false);
  assert.equal(compileCapability(capability({}, true)).validateResult({ anything: 1 }), true);
  assert.equal(compileCapability(capability({}, false)).validateResult({ anything: 1 }), false);
  assert.equal(compileCapability(capability({}, false)).validateResult(null), false);
});

test("compileCapability throws on a schema that strict mode rejects", () => {
  const noType = { properties: { a: { type: "string" } } }; // strictTypes: properties without a type
  const typo = { type: "object", propeties: {} }; // unknown keyword
  const draft07 = { $schema: "http://json-schema.org/draft-07/schema#" }; // unknown meta-schema
  assert.throws(() => compileCapability(capability(noType, true)), /strict/);
  assert.throws(() => compileCapability(capability({ type: "object" }, noType)), /strict/);
  assert.throws(() => compileCapability(capability(typo, true)), /strict mode: unknown keyword/);
  assert.throws(() => compileCapability(capability(draft07, true)), /draft-07/);
  const before = compileCapability(capability({ type: "object" }, true)); // the failure must not break later compiles
  assert.equal(before.validateParams({}), true);
});

test("the protocol Ajv instance and the capability instance are separate", () => {
  // a peer schema published under an id the protocol already uses, with the opposite verdicts
  const compiled: CompiledCapability = compileCapability(capability({ $id: "error.schema.json", type: "string" }, true));
  assert.equal(compiled.validateParams("boom"), true); // the capability instance took the peer schema
  assert.equal(compiled.validateParams({ msg: "boom" }), false);
  assert.ok(validateError({ msg: "boom" }).ok); // the protocol schema still decides for validateError
  assert.equal(validateError("boom").ok, false);
});

test("capability schemas of different capabilities do not interfere", () => {
  const a = compileCapability(capability({ type: "object", required: ["a"], properties: { a: {} } }, true));
  const b = compileCapability(capability({ type: "object", required: ["b"], properties: { b: {} } }, true));
  assert.equal(a.validateParams({ a: 1 }), true);
  assert.equal(b.validateParams({ a: 1 }), false);
  assert.equal(b.validateParams({ b: 1 }), true);
});
