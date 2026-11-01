// Tests of the discovery scan and the callability rules 1–8 (the Agmen view of one peer).
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import path from "node:path";
import { test } from "node:test";
import * as io from "../../src/io.js";
import { callable, scan } from "../../src/discovery.js";
import type { DiscoveredAgent, ScanResult } from "../../src/discovery.js";
import { toIso } from "../../src/util.js";
import { validateSpec, validateStatus } from "../../src/validate.js";
import { makeRoot } from "../helpers/index.js";
import type { Capability, CapabilityActio, Spec, Status } from "../../src/generated/types.js";

const SELF = "alpha"; // our own agent ID
const PEER = "beta"; // the peer under examination
const T0 = 1_700_000_000_000; // 2023-11-14T22:13:20Z
const ISO = "2023-11-14T22:13:20Z";

function capOf(name: string, actio: Partial<CapabilityActio> = {}): Capability {
  return { name, description: `does ${name}`, parameters: { type: "object" }, actio: { result: true, ...actio } };
}

/** A minimal valid peer spec; `disabled` posture/namespace/command fields are set per test. */
function peerSpec(over: Partial<Spec> = {}): Spec {
  return {
    id: PEER,
    actio_version: "0.1",
    datetime: ISO,
    description: "a peer agent",
    accepts_commands_from: [SELF],
    capabilities: [capOf("work")],
    security_support: [],
    security_posture: "trusted",
    ...over,
  };
}

function selfSpec(over: Partial<Spec> = {}): Spec {
  return peerSpec({ ...over, id: SELF });
}

function statusOf(id: string, state: Status["status"], datetime = ISO): Status {
  return { id, status: state, datetime, capability_status: [] };
}

/** A discovered peer as `scan` would hand it to `callable`. */
function agentOf(spec: Spec, status: Status, folder = `/actio/${spec.id}`): DiscoveredAgent {
  return { folder, spec, status };
}

/** `callable` for the always-callable base peer, with the peer's spec changed. */
function viewOf(over: Partial<Spec>, self: Partial<Spec> = {}, status?: Status): ReturnType<typeof callable> {
  return callable(selfSpec(self), agentOf(peerSpec(over), status ?? statusOf(PEER, "idle")), T0);
}

// --- rule 1: the disable filter ---

test("rule 1: a disabled peer keeps its commands and loses its capabilities", () => {
  const view = viewOf({ disabled: true });
  assert.ok(view, "a disabled peer is still reachable by command");
  assert.equal(view.commands, true);
  assert.equal(view.capabilities.size, 0);
  assert.equal(view.spec.capabilities.length, 1); // the spec itself is reported unchanged
  assert.equal(view.id, PEER);
});

test("rule 1: a disabled peer nobody may command is discarded (rule 6 follows)", () => {
  assert.equal(viewOf({ disabled: true, accepts_commands_from: [] }), undefined);
  assert.equal(viewOf({ disabled: true, accepts_commands_from: ["gamma"] }), undefined);
});

// --- rule 2: the namespace filter (*Agent Spec* namespace rules) ---

type Namespace = (string | null)[] | null | undefined;

const namespaceRows: { label: string; self: Namespace; peer: Namespace; shared: boolean }[] = [
  { label: "absent / absent: both in the null namespace", self: undefined, peer: undefined, shared: true },
  { label: "absent / explicit null", self: undefined, peer: null, shared: true },
  { label: "absent / empty list", self: undefined, peer: [], shared: true },
  { label: "empty list / explicit null", self: [], peer: null, shared: true },
  { label: "explicit null / explicit null", self: null, peer: null, shared: true },
  { label: "same single namespace", self: ["ns1"], peer: ["ns1"], shared: true },
  { label: "different single namespaces", self: ["ns1"], peer: ["ns2"], shared: false },
  { label: "several namespaces, one in common", self: ["ns1", "ns2"], peer: ["ns3", "ns2"], shared: true },
  { label: "a filled list excludes the null namespace", self: ["ns1"], peer: undefined, shared: false },
  { label: "…unless null is listed explicitly", self: ["ns1", null], peer: undefined, shared: true },
  { label: "null namespace against a filled peer list", self: undefined, peer: ["ns1", null], shared: true },
  { label: "two disjoint filled lists, one with null", self: ["ns1", null], peer: ["ns2"], shared: false },
  { label: "\"*\" matches a filled list", self: ["*"], peer: ["ns1"], shared: true },
  { label: "\"*\" matches the null namespace", self: ["*"], peer: undefined, shared: true },
  { label: "\"*\" on the peer side", self: ["ns1"], peer: ["*"], shared: true },
  { label: "\"*\" combined with other values", self: ["ns1", "*"], peer: ["zz"], shared: true },
  { label: "both \"*\"", self: ["*"], peer: ["*"], shared: true },
  { label: "the string \"null\" is not the void namespace", self: ["null"], peer: undefined, shared: false },
  { label: "\"null\" string on both sides is a real namespace", self: ["null"], peer: ["null"], shared: true },
];

test("rule 2: the namespace table decides whether the spec is considered, in both directions", () => {
  for (const row of namespaceRows) {
    const forward = viewOf({ namespaces: row.peer }, { namespaces: row.self });
    assert.equal(forward !== undefined, row.shared, `forward: ${row.label}`);
    const backward = viewOf({ namespaces: row.self }, { namespaces: row.peer }); // roles swapped
    assert.equal(backward !== undefined, row.shared, `backward: ${row.label}`);
  }
});

test("rule 2: no shared namespace discards the peer even though it accepts our commands", () => {
  assert.equal(viewOf({ namespaces: ["ns1"], accepts_commands_from: [SELF] }, { namespaces: ["ns2"] }), undefined);
  const view = viewOf({ namespaces: ["ns1", null] }, { namespaces: [null] });
  assert.ok(view);
  assert.deepEqual([...view.capabilities.keys()], ["work"]);
});

// --- rules 3 and 4: security posture (core profile: `trusted` only) ---

test("rules 3 and 4: only the trusted posture survives; the others are discarded", () => {
  for (const posture of ["trusted"] as const) {
    assert.ok(viewOf({ security_posture: posture, security_support: ["sign"] }), `${posture} must survive`);
  }
  for (const posture of ["untrusted", "secret", "zero-trust"] as const) {
    assert.equal(viewOf({ security_posture: posture }), undefined, `${posture} must be discarded`);
    // even with the issuer material published, we cannot support the posture (rule 3 first)
    assert.equal(
      viewOf({ security_posture: posture, security: [{ key: "issuer_key", value: "abc" }] }),
      undefined,
      `${posture} with issuer material must be discarded`,
    );
  }
});

// --- rule 5.1: the command filter ---

test("rule 5.1: commands are ours only when our ID is listed; an empty list means nobody", () => {
  const rows: { label: string; from: string[]; commands: boolean }[] = [
    { label: "empty list: nobody", from: [], commands: false },
    { label: "someone else", from: ["gamma"], commands: false },
    { label: "just us", from: [SELF], commands: true },
    { label: "us among others", from: ["gamma", SELF, "delta"], commands: true },
    { label: "a prefix of our ID does not match", from: [`${SELF}x`], commands: false },
  ];
  for (const row of rows) {
    const view = viewOf({ accepts_commands_from: row.from });
    assert.ok(view, row.label); // capabilities stay usable, so the peer survives either way
    assert.equal(view.commands, row.commands, row.label);
  }
});

// --- rule 5.2: the capability filter ---

test("rule 5.2: capability actio.disabled and accepts_from decide each capability", () => {
  const rows: { label: string; actio: Partial<CapabilityActio>; usable: boolean }[] = [
    { label: "no restrictions", actio: {}, usable: true },
    { label: "explicit disabled: false", actio: { disabled: false }, usable: true },
    { label: "disabled", actio: { disabled: true }, usable: false },
    { label: "disabled even though it lists us", actio: { disabled: true, accepts_from: [SELF] }, usable: false },
    { label: "empty accepts_from: everybody", actio: { accepts_from: [] }, usable: true },
    { label: "accepts_from lists us", actio: { accepts_from: [SELF] }, usable: true },
    { label: "accepts_from lists others", actio: { accepts_from: ["gamma"] }, usable: false },
    { label: "accepts_from lists us among others", actio: { accepts_from: ["gamma", SELF] }, usable: true },
    { label: "harness hints do not matter", actio: { tool: true, sync: false, max_parallel: 2 }, usable: true },
  ];
  for (const row of rows) {
    const view = viewOf({ capabilities: [capOf("c", row.actio)] });
    assert.ok(view, row.label); // one capability stays in the map, so rule 6 never triggers
    assert.equal(view.capabilities.has("c"), row.usable, row.label);
  }
});

test("rule 5.2: the map holds exactly the usable capabilities, by name and identity", () => {
  const open = capOf("open");
  const listed = capOf("listed", { accepts_from: [SELF] });
  const offLimits = capOf("off-limits", { accepts_from: ["gamma"] });
  const gone = capOf("gone", { disabled: true });
  const view = viewOf({ capabilities: [open, offLimits, listed, gone] });
  assert.ok(view);
  assert.deepEqual([...view.capabilities.keys()], ["open", "listed"]); // spec order preserved
  assert.equal(view.capabilities.get("open"), open); // the very objects of the peer spec
  assert.equal(view.capabilities.get("listed"), listed);
});

// --- rule 6: nothing usable ---

test("rule 6: a peer with no usable commands and no usable capabilities is discarded", () => {
  assert.equal(viewOf({ accepts_commands_from: [], capabilities: [] }), undefined);
  assert.equal(viewOf({ accepts_commands_from: [], capabilities: [capOf("gone", { disabled: true })] }), undefined);
  assert.equal(
    viewOf({ accepts_commands_from: [], capabilities: [capOf("priv", { accepts_from: ["gamma"] })] }),
    undefined,
  );
  // one usable capability is enough to keep the peer
  const oneOpen = viewOf({
    accepts_commands_from: [],
    capabilities: [capOf("priv", { accepts_from: ["gamma"] }), capOf("open")],
  });
  assert.ok(oneOpen);
});

// --- rule 7: the status filter ---

test("rule 7: inactive and error agents are filtered out; idle and busy are not", () => {
  for (const state of ["idle", "busy"] as const) {
    const view = viewOf({}, {}, statusOf(PEER, state));
    assert.ok(view, `${state} must survive`);
    assert.equal(view.status.status, state);
  }
  for (const state of ["inactive", "error"] as const) {
    assert.equal(viewOf({}, {}, statusOf(PEER, state)), undefined, `${state} must be filtered out`);
  }
});

test("rule 7 applies to a peer that passes every other filter, and precedes the health flag", () => {
  // a fresh, perfectly callable peer that is simply inactive
  assert.equal(viewOf({ accepts_commands_from: [SELF] }, {}, statusOf(PEER, "inactive")), undefined);
  // an inactive peer whose status is 700 s old is filtered out, not flagged zombie
  assert.equal(viewOf({}, {}, statusOf(PEER, "inactive", toIso(T0 - 700_000))), undefined);
});

// --- rule 8: the health flag ---

test("rule 8: degraded and zombie peers are flagged, not filtered out", () => {
  const rows: { label: string; ageMs: number; health: "healthy" | "online" | "degraded" | "zombie" }[] = [
    { label: "just updated", ageMs: 0, health: "healthy" },
    { label: "59 s", ageMs: 59_000, health: "healthy" },
    { label: "90 s (a healthy heartbeat interval)", ageMs: 90_000, health: "online" },
    { label: "150 s", ageMs: 150_000, health: "degraded" },
    { label: "600 s", ageMs: 600_000, health: "degraded" },
    { label: "700 s", ageMs: 700_000, health: "zombie" },
    { label: "a day", ageMs: 86_400_000, health: "zombie" },
  ];
  for (const row of rows) {
    const view = viewOf({}, {}, statusOf(PEER, "busy", toIso(T0 - row.ageMs)));
    assert.ok(view, `the ${row.label} peer must stay in the view`);
    assert.equal(view.health, row.health, row.label);
  }
});

test("rule 8: the health flag comes from status.ts, so an unrateable age leaves it undefined", () => {
  const impossible = "2023-02-30T00:00:00Z"; // pattern-valid for the schema, but not a real instant
  assert.ok(validateStatus(statusOf(PEER, "idle", impossible)).ok);
  const view = viewOf({}, {}, statusOf(PEER, "idle", impossible));
  assert.ok(view, "an unrateable status age is not a filter");
  assert.equal(view.health, undefined);
});

// --- the AgentView shape ---

test("the view carries the peer's id, folder, spec and status unchanged", () => {
  const spec = peerSpec();
  const status = statusOf(PEER, "busy");
  const peer = agentOf(spec, status, "/actio/folder-named-differently");
  const view = callable(selfSpec(), peer, T0);
  assert.ok(view);
  assert.equal(view.id, PEER); // the spec ID, not the folder name
  assert.equal(view.folder, "/actio/folder-named-differently");
  assert.equal(view.spec, spec);
  assert.equal(view.status, status);
  assert.equal(view.health, "healthy");
  assert.equal(view.commands, true);
  assert.deepEqual([...view.capabilities.keys()], ["work"]);
});

// --- the scan ---

/** Writes one agent folder (`spec.json` + `status.json`) under `root`. */
async function writeAgent(root: string, folder: string, spec: unknown, status: unknown): Promise<void> {
  const dir = path.join(root, folder);
  await io.writeJsonAtomic(path.join(dir, "spec.json"), spec, dir);
  await io.writeJsonAtomic(path.join(dir, "status.json"), status, dir);
}

const emptyScan: ScanResult = { agents: [], duplicates: [], ignored: [] };

test("scan finds a valid agent and reports its spec and status", async (t) => {
  const root = await makeRoot(t);
  const spec = peerSpec();
  const status = statusOf(PEER, "idle");
  await writeAgent(root, PEER, spec, status);
  const result = await scan(root);
  assert.deepEqual(result, { agents: [{ folder: path.join(root, PEER), spec, status }], duplicates: [], ignored: [] });
  assert.ok(validateSpec(result.agents[0]!.spec).ok);
  assert.ok(validateStatus(result.agents[0]!.status).ok);
});

test("scan ignores files and reserved folders, and hidden folders are never agents", async (t) => {
  const root = await makeRoot(t);
  for (const reserved of ["tools", "forum", "nuntium"]) {
    await writeAgent(root, reserved, peerSpec({ id: reserved }), statusOf(reserved, "idle")); // content is irrelevant
  }
  await writeAgent(root, ".hidden-agent", peerSpec({ id: "hidden" }), statusOf("hidden", "idle"));
  await io.writeJsonAtomic(path.join(root, "actio.json"), { settings: true }, root); // a file, not a folder
  await io.writeJsonAtomic(path.join(root, "nuntium.json"), { id: PEER }, root); // a broadcast file at the root
  await writeFile(path.join(root, ".keep"), ""); // a dot file
  const result = await scan(root);
  assert.deepEqual(result, emptyScan); // nothing discovered, nothing reported: none of it is an agent folder
  await writeAgent(root, PEER, peerSpec(), statusOf(PEER, "idle"));
  assert.deepEqual((await scan(root)).agents.map((a) => a.spec.id), [PEER]); // found among them
});

test("scan ignores silently a folder missing spec.json or status.json (rule 2)", async (t) => {
  const root = await makeRoot(t);
  await io.makeDir(path.join(root, "empty")); // no publication at all
  const onlySpec = path.join(root, "no-status");
  const onlyStatus = path.join(root, "no-spec");
  await io.writeJsonAtomic(path.join(onlySpec, "spec.json"), peerSpec({ id: "no-status" }), onlySpec);
  await io.writeJsonAtomic(path.join(onlyStatus, "status.json"), statusOf("no-spec", "idle"), onlyStatus);
  assert.deepEqual(await scan(root), emptyScan);
});

test("scan reports a non-conforming spec.json with the first schema reason (rule 3)", async (t) => {
  const root = await makeRoot(t);
  const badVersion = { ...peerSpec(), actio_version: "0" };
  await writeAgent(root, PEER, badVersion, statusOf(PEER, "idle"));
  const result = await scan(root);
  assert.deepEqual(result.agents, []);
  assert.equal(result.ignored.length, 1);
  assert.equal(result.ignored[0]!.path, path.join(root, PEER, "spec.json"));
  assert.match(result.ignored[0]!.reason, /actio_version|must match|pattern/);
});

test("scan reports a non-conforming status.json with the first schema reason (rule 3)", async (t) => {
  const root = await makeRoot(t);
  const badState = { ...statusOf(PEER, "idle"), status: "sleeping" };
  await writeAgent(root, PEER, peerSpec(), badState);
  const result = await scan(root);
  assert.deepEqual(result.agents, []);
  assert.equal(result.ignored.length, 1);
  assert.equal(result.ignored[0]!.path, path.join(root, PEER, "status.json"));
  assert.match(result.ignored[0]!.reason, /status must be equal to one of the allowed values/);
});

test("scan stops at the first unreadable publication and says why", async (t) => {
  const root = await makeRoot(t);
  const dir = path.join(root, PEER);
  await io.makeDir(dir);
  await writeFile(path.join(dir, "spec.json"), '{"id": "beta",'); // present but not valid JSON
  await io.writeJsonAtomic(path.join(dir, "status.json"), statusOf(PEER, "idle"), dir);
  const result = await scan(root);
  assert.deepEqual(result.agents, []);
  assert.deepEqual(result.ignored, [{ path: path.join(dir, "spec.json"), reason: "not valid JSON" }]);
});

test("scan of a folder whose only material is an unparsable status.json says so", async (t) => {
  const root = await makeRoot(t);
  const dir = path.join(root, PEER);
  await io.writeJsonAtomic(path.join(dir, "spec.json"), peerSpec(), dir);
  await writeFile(path.join(dir, "status.json"), "nope");
  assert.deepEqual((await scan(root)).ignored, [{ path: path.join(dir, "status.json"), reason: "not valid JSON" }]);
});

// A capability list that repeats a name is schema-valid (JSON Schema cannot express uniqueness across
// array elements), so the scan has to reject it as rule 3 does, and say which name it repeats.
test("scan ignores a peer spec that repeats a capability name, and names the repetition", async (t) => {
  const root = await makeRoot(t);
  const twice = peerSpec({ capabilities: [capOf("work"), capOf("play"), capOf("work"), capOf("play")] });
  assert.ok(validateSpec(twice).ok, "the schema accepts it: it is the rules outside the schema that reject it");
  await writeAgent(root, PEER, twice, statusOf(PEER, "idle"));
  assert.deepEqual(await scan(root), {
    agents: [],
    duplicates: [],
    ignored: [{ path: path.join(root, PEER, "spec.json"), reason: `duplicate capability name "work"` }],
  }); // the FIRST repeated name, and the offending file as the path
});

test("a duplicate-name peer does not disturb the discovery of a valid peer", async (t) => {
  const root = await makeRoot(t);
  await writeAgent(root, "01-good", peerSpec({ capabilities: [capOf("work"), capOf("play")] }), statusOf(PEER, "idle"));
  await writeAgent(root, "02-dup", peerSpec({ id: "dup", capabilities: [capOf("x"), capOf("x")] }), statusOf("dup", "idle"));
  const result = await scan(root);
  assert.deepEqual(result.agents.map((a) => a.spec.id), [PEER]);
  assert.deepEqual(result.duplicates, []);
  assert.deepEqual(result.ignored, [
    { path: path.join(root, "02-dup", "spec.json"), reason: `duplicate capability name "x"` },
  ]);
});

test("the duplicate-name rejection comes before the D-3 ID match: reported, never a duplicate", async (t) => {
  const root = await makeRoot(t);
  await writeAgent(root, "01-first", peerSpec(), statusOf(PEER, "idle")); // the ID "beta" is taken here
  await writeAgent(root, "02-twin-dup", peerSpec({ capabilities: [capOf("work"), capOf("work")] }), statusOf(PEER, "busy"));
  const result = await scan(root); // same ID, but an unusable spec: rule 3 decides first
  assert.deepEqual(result.agents.map((a) => a.folder), [path.join(root, "01-first")]);
  assert.deepEqual(result.duplicates, [], "an unusable spec never reaches the D-3 duplicate list");
  assert.deepEqual(result.ignored, [
    { path: path.join(root, "02-twin-dup", "spec.json"), reason: `duplicate capability name "work"` },
  ]);
});

test("a duplicate-name spec is reported once: the bad status adds no second entry", async (t) => {
  const root = await makeRoot(t);
  const dup = peerSpec({ capabilities: [capOf("work"), capOf("work")] });
  await writeAgent(root, PEER, dup, { ...statusOf(PEER, "idle"), status: "sleeping" });
  assert.deepEqual((await scan(root)).ignored, [
    { path: path.join(root, PEER, "spec.json"), reason: `duplicate capability name "work"` },
  ]); // the spec decides, as it does for a schema failure
});

test("D-3: the first folder in lexical order keeps a duplicated ID, the rest are reported", async (t) => {
  const root = await makeRoot(t);
  const order = ["03-third", "01-first", "02-second"]; // listed out of order to prove the scan sorts
  for (const folder of order) await writeAgent(root, folder, peerSpec(), statusOf(PEER, "idle"));
  const result = await scan(root);
  assert.deepEqual(result.agents.map((a) => a.folder), [path.join(root, "01-first")]);
  assert.deepEqual(result.duplicates, [path.join(root, "02-second"), path.join(root, "03-third")]);
  assert.deepEqual(result.ignored, []); // a duplicate is not an error (the caller logs it)
});

test("D-3: duplicates are matched by spec ID, not by folder name", async (t) => {
  const root = await makeRoot(t);
  await writeAgent(root, "aaa", peerSpec({ id: "zebra" }), statusOf("zebra", "idle"));
  await writeAgent(root, "bbb", peerSpec({ id: "zebra" }), statusOf("zebra", "busy"));
  const result = await scan(root);
  assert.equal(result.agents.length, 1);
  assert.equal(result.agents[0]!.folder, path.join(root, "aaa"));
  assert.equal(result.agents[0]!.status.status, "idle"); // the winning folder's status
  assert.deepEqual(result.duplicates, [path.join(root, "bbb")]);
});

test("scan returns the agents in lexical folder order and the duplicates in that order too", async (t) => {
  const root = await makeRoot(t);
  for (const [folder, id] of [["zulu", "z"], ["mike", "m"], ["alpha", "a"]] as const) {
    await writeAgent(root, folder, peerSpec({ id }), statusOf(id, "idle"));
  }
  assert.deepEqual((await scan(root)).agents.map((a) => a.folder), [
    path.join(root, "alpha"),
    path.join(root, "mike"),
    path.join(root, "zulu"),
  ]);
});

test("scan looks only at the agent folder itself: an inbox and extra files do not disturb it", async (t) => {
  const root = await makeRoot(t);
  await writeAgent(root, PEER, peerSpec(), statusOf(PEER, "idle"));
  const inbox = path.join(root, PEER, "inbox");
  await io.makeDir(inbox);
  await io.writeJsonAtomic(path.join(inbox, "1.json"), peerSpec({ id: "inside-inbox" }), inbox);
  const result = await scan(root);
  assert.deepEqual(result.agents.map((a) => a.spec.id), [PEER]);
  assert.deepEqual(result.ignored, []);
});

test("scan reports a folder once, and validates before matching a duplicated ID", async (t) => {
  const root = await makeRoot(t);
  await writeAgent(root, "01-good", peerSpec(), statusOf(PEER, "idle"));
  const bothBadSpec = { ...peerSpec(), id: "x", capabilities: 3 };
  const bothBadStatus = { ...statusOf("x", "idle"), status: "sleeping" };
  await writeAgent(root, "02-both-bad", bothBadSpec, bothBadStatus);
  await writeAgent(root, "03-bad-twin", { ...peerSpec(), description: 7 }, statusOf(PEER, "idle")); // same ID as 01
  const result = await scan(root);
  assert.deepEqual(result.agents.map((a) => a.folder), [path.join(root, "01-good")]);
  assert.deepEqual(result.duplicates, []); // an invalid folder never becomes a duplicate
  assert.deepEqual(
    result.ignored.map((i) => `${path.basename(path.dirname(i.path))}/${path.basename(i.path)}`),
    ["02-both-bad/spec.json", "03-bad-twin/spec.json"], // one entry per folder, the spec checked first
  );
});

test("scan of an empty root finds nothing and reports nothing", async (t) => {
  assert.deepEqual(await scan(await makeRoot(t)), emptyScan);
});

test("scan of a missing root propagates the error instead of inventing an empty view", async (t) => {
  const missing = path.join(await makeRoot(t), "not-created");
  await assert.rejects(() => scan(missing), /ENOENT/);
});

