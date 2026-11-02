// Golden strings for the `actio` tool output (format.ts); pins every §6.2 output row.
// Text outputs are golden strings; JSON outputs are compared after `JSON.parse` (D-20).
import assert from "node:assert/strict";
import { test } from "node:test";
import type { AgentView, Capability, RequestOutcome, ResponseBody } from "actio-ts";
import { formatCall, formatInfo, formatTeam } from "../src/format.js";

const greet: Capability = {
  name: "greet",
  description: "Say hello",
  parameters: { type: "object" },
  actio: { result: { type: "object" }, max_parallel: 2 },
};

const slow: Capability = {
  name: "slow",
  description: "A slow capability",
  parameters: { type: "object" },
  actio: { result: true },
};

const ping: Capability = {
  name: "ping",
  description: "Nothing",
  parameters: { type: "object" },
  actio: { result: true },
};

/** The shared peer view for every test. */
const view: AgentView = {
  id: "alice",
  folder: "/actio/agents/alice",
  spec: {
    id: "alice",
    actio_version: "1",
    datetime: "2026-01-01T00:00:00Z",
    description: "The test peer",
    namespaces: ["default", null],
    accepts_commands_from: ["*"],
    capabilities: [greet, slow],
    security_support: [],
    security_posture: "trusted",
  },
  status: {
    id: "alice",
    status: "idle",
    datetime: "2026-01-01T00:00:00Z",
    capability_status: [],
  },
  health: "online",
  commands: true,
  capabilities: new Map([
    ["greet", greet],
    ["slow", slow],
  ]),
};

/** A second peer, only for the two-agent list-team case. */
const bob: AgentView = {
  ...view,
  id: "bob",
  spec: { ...view.spec, id: "bob", description: "Bob the peer", capabilities: [ping] },
  capabilities: new Map([["ping", ping]]),
};

/** One `RequestOutcome` per outcome; `body` absent for the unreadable-response case. */
function outcome(finalStatus: RequestOutcome["finalStatus"], body?: ResponseBody): RequestOutcome {
  return body === undefined ? { finalStatus } : { finalStatus, response: body };
}

const response: ResponseBody = {
  ts: 1767225600,
  nonce: "nonce-1",
  signature: null,
  error: false,
  cancellation: false,
  result: { greeting: "hello, world" },
  attachments: [],
};

const errorResponse: ResponseBody = {
  ...response,
  nonce: "nonce-2",
  error: true,
  result: { code: 105, msg: "bad params", details: "name: required" },
};

const noCodeResponse: ResponseBody = { ...errorResponse, nonce: "nonce-3", result: { msg: "bad params" } };

test("list-team text, two agents", () => {
  assert.equal(
    formatTeam([bob, view], false),
    `- **alice** — The test peer — capabilities: "greet", "slow"\n- **bob** — Bob the peer — capabilities: "ping"`,
  );
});

test("list-team text, empty view", () => {
  assert.equal(formatTeam([], false), "No visible agents.");
});

test("list-team JSON, two agents", () => {
  assert.deepEqual(JSON.parse(formatTeam([bob, view], true)), [
    { id: "alice", description: "The test peer", capabilities: ["greet", "slow"] },
    { id: "bob", description: "Bob the peer", capabilities: ["ping"] },
  ]);
});

// The one exact-string JSON case: pins `JSON.stringify(…, null, 2)`'s serialization.
test("list-team JSON, empty view", () => {
  assert.equal(formatTeam([], true), "[]");
});

test("info text, all capabilities", () => {
  assert.equal(
    formatInfo(view, undefined, false),
    `# alice
The test peer
Status: idle · Health: online · Namespaces: default, void

## greet
Say hello
max_parallel: 2
Parameters
\`\`\`json
{
  "type": "object"
}
\`\`\`
Result
\`\`\`json
{
  "type": "object"
}
\`\`\`

## slow
A slow capability
Parameters
\`\`\`json
{
  "type": "object"
}
\`\`\`
Result
\`\`\`json
true
\`\`\``,
  );
});

test("info text, one capability", () => {
  assert.equal(
    formatInfo(view, "greet", false),
    `# alice
The test peer
Status: idle · Health: online · Namespaces: default, void

## greet
Say hello
max_parallel: 2
Parameters
\`\`\`json
{
  "type": "object"
}
\`\`\`
Result
\`\`\`json
{
  "type": "object"
}
\`\`\``,
  );
});

test("info JSON, all and one capability", () => {
  assert.deepEqual(JSON.parse(formatInfo(view, undefined, true)), {
    id: "alice",
    description: "The test peer",
    status: "idle",
    health: "online",
    namespaces: ["default", null],
    capabilities: [greet, slow],
  });
  assert.deepEqual(JSON.parse(formatInfo(view, "greet", true)), {
    id: "alice",
    description: "The test peer",
    status: "idle",
    health: "online",
    namespaces: ["default", null],
    capabilities: [greet],
  });
});

test("info throws on an unknown capability", () => {
  assert.throws(
    () => formatInfo(view, "nope", false),
    /unknown capability "nope" for agent "alice"; available: "greet", "slow"/,
  );
});

test("call text, confirmed", () => {
  assert.equal(
    formatCall("alice/greet", outcome("confirmed", response), false),
    `**alice/greet** → confirmed

\`\`\`json
{
  "greeting": "hello, world"
}
\`\`\``,
  );
});

test("call text, error answer", () => {
  assert.equal(
    formatCall("alice/greet", outcome("confirmed", errorResponse), false),
    `**alice/greet** → error 105: bad params

details:
name: required`,
  );
});

test("call JSON, confirmed and error answer", () => {
  assert.deepEqual(JSON.parse(formatCall("alice/greet", outcome("confirmed", response), true)), {
    target: "alice/greet",
    finalStatus: "confirmed",
    response,
  });
  assert.deepEqual(JSON.parse(formatCall("alice/greet", outcome("confirmed", errorResponse), true)), {
    target: "alice/greet",
    finalStatus: "confirmed",
    response: errorResponse,
  });
});

// The `call` forms beyond confirmed/error answer, in one table-driven text test.
const callForms = [
  { name: "error without code", outcome: outcome("confirmed", noCodeResponse), expected: `**alice/greet** → error: bad params` },
  { name: "cancelled", outcome: outcome("cancelled"), expected: `**alice/greet** → cancelled` },
  { name: "abandoned", outcome: outcome("abandoned"), expected: `**alice/greet** → abandoned (no response)` },
  { name: "unreadable response", outcome: outcome("confirmed"), expected: `**alice/greet** → confirmed (encrypted response, not readable)` },
];

test("call text, the other forms", () => {
  for (const row of callForms) {
    assert.equal(formatCall("alice/greet", row.outcome, false), row.expected, row.name);
  }
});

test("call JSON, cancelled and abandoned", () => {
  assert.deepEqual(JSON.parse(formatCall("alice/greet", outcome("cancelled"), true)), {
    target: "alice/greet",
    finalStatus: "cancelled",
  });
  assert.deepEqual(JSON.parse(formatCall("alice/greet", outcome("abandoned"), true)), {
    target: "alice/greet",
    finalStatus: "abandoned",
  });
});