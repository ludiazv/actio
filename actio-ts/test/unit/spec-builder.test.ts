// Tests of the spec builder: agent options + registered capabilities to a schema-valid spec (WP7-B).
import assert from "node:assert/strict";
import { test } from "node:test";
import { buildSpec } from "../../src/spec-builder.js";
import { toIso } from "../../src/util.js";
import { uniqueCapabilityNames, validateSpec } from "../../src/validate.js";
import type { Capability, Spec } from "../../src/generated/types.js";
import type { AgentOptions } from "../../src/types.js";

const ID = "alpha";
const NOW = 1_764_000_000_000; // one fixed instant: the builder only ever sees the clock it is handed

const REQUIRED_FIELDS = [
  "accepts_commands_from",
  "actio_version",
  "capabilities",
  "datetime",
  "description",
  "id",
  "security_posture",
  "security_support",
];

function options(overrides: Partial<AgentOptions> = {}): AgentOptions {
  return { id: ID, description: "translates text", acceptsCommandsFrom: ["beta"], ...overrides };
}

/** A registered capability: what WP7 hands over after `actio.register()`. */
function cap(name: string, actio: Partial<Capability["actio"]> = {}): Capability {
  return { name, description: `does ${name}`, parameters: { type: "object" }, actio: { result: true, ...actio } };
}

function build(opts: Partial<AgentOptions> = {}, capabilities: Capability[] = [], nowMs = NOW): Spec {
  return buildSpec(options(opts), capabilities, nowMs);
}

/** A build that must fail: returns the message and asserts it matches `why`. */
function buildFails(opts: Partial<AgentOptions>, capabilities: Capability[], why: RegExp): string {
  try {
    buildSpec(options(opts), capabilities, NOW);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    assert.match(message, why);
    return message;
  }
  return assert.fail(`the build of ${JSON.stringify(Object.keys(opts))} should have thrown`);
}

/** Every shape the builder produces has to survive the same validator the receiver will run later. */
function mustValidate(spec: Spec): void {
  const checked = validateSpec(spec);
  if (!checked.ok) assert.fail(`the builder produced an invalid spec: ${checked.errors.join("; ")}`);
  assert.equal(uniqueCapabilityNames(spec), true, "the builder's belt check must agree");
}

test("a minimal agent builds exactly the required fields", () => {
  const spec = build({}, []);
  mustValidate(spec);
  assert.deepEqual(Object.keys(spec).sort(), REQUIRED_FIELDS, "no optional field is invented");
  assert.equal(spec.id, ID);
  assert.equal(spec.actio_version, "0.1");
  assert.equal(spec.description, "translates text");
  assert.deepEqual(spec.accepts_commands_from, ["beta"]);
  assert.deepEqual(spec.capabilities, [], "an agent may advertise nothing at all");
});

test("the security fields are exactly the core profile, and nothing else is added", () => {
  const spec = build({}, [cap("translate")]);
  mustValidate(spec);
  assert.deepEqual(spec.security_support, [], "core profile: no signature and encryption features");
  assert.equal(spec.security_posture, "trusted", "core profile: the shared tree is trusted");
  assert.ok(!("security" in spec), "no keys, no issuer, no certificate to advertise");
  assert.ok(!("disabled" in spec), "the state lives in status.json (D-6); only `enable` writes it here");
});

test("datetime comes from the injected clock, at the protocol's second resolution", () => {
  assert.equal(build({}, [], 0).datetime, "1970-01-01T00:00:00Z");
  assert.equal(build({}, [], NOW).datetime, toIso(NOW));
  assert.equal(build({}, [], NOW).datetime, "2025-11-24T16:00:00Z");
  assert.notEqual(build({}, [], NOW).datetime, build({}, [], NOW + 1_000).datetime);
  assert.equal(build({}, [], NOW).datetime, build({}, [], NOW + 1).datetime, "ms are not part of the format");
});

test("namespaces: absent stays absent, an explicit null survives, lists are carried as given", () => {
  assert.ok(!("namespaces" in build()), "absent means the null namespace: do not write anything");
  const explicitNull = build({ namespaces: null });
  mustValidate(explicitNull);
  assert.ok("namespaces" in explicitNull, "null is a value the operator chose, not an absence");
  assert.equal(explicitNull.namespaces, null);
  // `null` may also appear *inside* the list (*Namespaces*); `AgentOptions` cannot express that, so the
  // builder must at least carry it through untouched for whoever hands it over (WP7-A-1 may widen it).
  for (const namespaces of [[], ["*"], ["alpha"], ["alpha", "beta"], ["null"], ["alpha", null] as string[]]) {
    const spec = build({ namespaces });
    mustValidate(spec);
    assert.deepEqual(spec.namespaces, namespaces, `"${JSON.stringify(namespaces)}" must survive untouched`);
  }
});

test("depends and wants appear only when given, and carry the listed IDs", () => {
  const minimal = build({}, [cap("translate")]);
  assert.ok(!("depends" in minimal) && !("wants" in minimal), "both default to empty: absent is legal");
  const spec = build({ depends: ["beta", "gamma"], wants: ["delta"] });
  mustValidate(spec);
  assert.deepEqual(spec.depends, ["beta", "gamma"]);
  assert.deepEqual(spec.wants, ["delta"]);
  const empty = build({ depends: [], wants: [] });
  mustValidate(empty);
  assert.deepEqual(empty.depends, []);
  assert.deepEqual(empty.wants, []);
});

test("info carries free content; info that is not an object fails the build", () => {
  const info = { owner: "team-a", tags: ["nlp", "batch"], limits: { per_day: 100 }, nested: { deep: [1, "2", null] } };
  const spec = build({ info });
  mustValidate(spec);
  assert.deepEqual(spec.info, info, "the builder neither filters nor flattens free content");
  for (const bad of ["just text", 7, true, [1, 2], null]) {
    buildFails({ info: bad as unknown as Record<string, unknown> }, [], /invalid spec/);
  }
});

test("duplicate capability names are rejected, and the message names the duplicate", () => {
  const three = [cap("translate"), cap("summarize"), cap("translate")];
  const message = buildFails({}, three, /duplicate capability name "translate"/);
  assert.ok(message.includes(ID), "the operator must know which agent is misconfigured");
  // different descriptions do not make two capabilities different, and the position does not matter
  buildFails({}, [{ ...cap("search"), description: "one" }, { ...cap("search"), description: "two" }], /"search"/);
  buildFails({}, [cap("a"), cap("b"), cap("c"), cap("b")], /duplicate capability name "b"/);
  buildFails({}, [cap("x"), cap("x"), cap("x")], /duplicate/);
  assert.equal(uniqueCapabilityNames(build({}, [cap("a"), cap("b"), cap("c")])), true, "unique names are fine");
});

test("capabilities are embedded in registration order, as the same objects", () => {
  const a = cap("translate");
  const b = cap("summarize", { tool: true, tool_name: "ffmpeg", skill: "video" });
  const c = cap("search", { accepts_from: ["beta"], max_parallel: 2, recommended_timeout: 30 });
  const spec = build({}, [a, b, c]);
  mustValidate(spec);
  assert.deepEqual(spec.capabilities.map((x) => x.name), ["translate", "summarize", "search"]);
  assert.equal(spec.capabilities[0], a, "the builder embeds the registered objects, it does not copy them");
  assert.equal(spec.capabilities[1], b);
  assert.equal(spec.capabilities[2], c);
  assert.deepEqual(spec.capabilities[2].actio, {
    result: true,
    accepts_from: ["beta"],
    max_parallel: 2,
    recommended_timeout: 30,
  });
});

test("the build output passes validateSpec on every shape we produce", () => {
  const many = Array.from({ length: 12 }, (_, i) => cap(`cap${i}`, { max_parallel: i }));
  const shapes: [Partial<AgentOptions>, Capability[]][] = [
    [{}, []],
    [{ acceptsCommandsFrom: [] }, []], // an empty list means nobody may command us
    [{ acceptsCommandsFrom: ["beta", "gamma", "delta", ID] }, many], // commanding ourselves is legal
    [{ namespaces: ["*"] }, [cap("a"), cap("b")]],
    [{ namespaces: null, depends: [], wants: [], info: {} }, []],
    [
      { description: "x".repeat(500), depends: Array.from({ length: 20 }, (_, i) => `agent-${i}`) },
      [cap("a", { disabled: true }), cap("b", { accepts_from: [] }), cap("c", { sync: false })],
    ],
  ];
  for (const [opts, capabilities] of shapes) mustValidate(build(opts, capabilities));
});

test("a malformed capability fails the build with the schema reason, before anything is published", () => {
  const noDescription = { name: "x", parameters: {}, actio: { result: true } } as unknown as Capability;
  const noActio = { name: "x", description: "d", parameters: {} } as unknown as Capability;
  const noResult = { name: "x", description: "d", parameters: {}, actio: {} } as unknown as Capability;
  buildFails({}, [noDescription], /invalid spec[\s\S]*description/);
  buildFails({}, [noActio], /invalid spec/);
  buildFails({}, [noResult], /invalid spec/);
  buildFails({}, [cap("x", { max_parallel: -1 }), cap("y")], /max_parallel must be >= 0/);
  buildFails({}, [cap("x", { sync: "yes" } as unknown as Partial<Capability["actio"]>)], /boolean/);
  buildFails({}, [{ ...cap("x"), extra: 1 } as unknown as Capability], /must NOT have additional properties/);
  buildFails({}, [cap("bad name")], /pattern/); // capabilityName pattern
  buildFails({}, [{ ...cap("x"), parameters: null } as unknown as Capability], /object/);
  // the good case proves the failure is about the shape, not about the capability being unusual
  mustValidate(build({}, [cap("x", { result: false, disabled: true, max_parallel: 0 })]));
});

test("a bad agent option set fails the build instead of reaching the tree", () => {
  for (const bad of ["", "bad id", "dotted.id", "a".repeat(33)]) {
    buildFails({ id: bad }, [], /invalid spec/);
  }
  buildFails({ description: 7 as unknown as string }, [], /invalid spec/);
  buildFails({ description: undefined as unknown as string }, [], /required property 'description'/);
  buildFails({ id: undefined as unknown as string }, [], /required property 'id'/);
  buildFails({ acceptsCommandsFrom: ["bad id"] }, [], /accepts_commands_from\/0 must match pattern/);
  buildFails({ namespaces: ["", "alpha"] }, [], /namespaces\/0 must NOT have fewer than 1 characters/);
  mustValidate(build({ acceptsCommandsFrom: [ID, ID] })); // commanding ourselves, and repeats, are legal
});

test("the builder copies the option arrays but embeds the capability objects", () => {
  const opts = options({ acceptsCommandsFrom: ["beta"], depends: ["gamma"], wants: ["delta"] });
  const capabilities = [cap("translate")];
  const spec = buildSpec(opts, capabilities, NOW);
  opts.acceptsCommandsFrom.push("epsilon"); // the operator keeps working on their own arrays
  opts.depends!.push("zeta");
  opts.wants!.push("eta");
  capabilities.push(cap("summarize"));
  assert.deepEqual(spec.accepts_commands_from, ["beta"], "a later option change cannot alter the spec");
  assert.deepEqual(spec.depends, ["gamma"]);
  assert.deepEqual(spec.wants, ["delta"]);
  assert.deepEqual(spec.capabilities.map((x) => x.name), ["translate"]);
  mustValidate(spec);
  capabilities[0].description = "changed in place";
  assert.equal(spec.capabilities[0].description, "changed in place", "capabilities are shared by identity");
});

test("the same inputs build the same spec", () => {
  const opts = { namespaces: ["*"], depends: ["beta"], info: { note: "x" } };
  const capabilities = [cap("a"), cap("b", { accepts_from: ["beta"] })];
  assert.deepEqual(build(opts, capabilities), build(opts, capabilities));
});
