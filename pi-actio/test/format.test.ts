// Golden strings for the `actio` tool output (format.ts); pins every §6.2 output row.
import assert from "node:assert/strict";
import { test } from "node:test";
import type { AgentView, Capability, RequestOutcome, ResponseBody } from "actio-ts";
import { formatCall, formatInfo, formatTeam } from "../src/format.js";

const greet: Capability = {
  name: "greet",
  description: "Say hello",
  parameters: { type: "object", properties: { name: { type: "string" } }, required: ["name"] },
  actio: { result: { type: "object", properties: { greeting: { type: "string" } } }, max_parallel: 2 },
};

const slow: Capability = {
  name: "slow",
  description: "A slow capability",
  parameters: { type: "object", properties: {} },
  actio: { result: true },
};

const alice: AgentView = {
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

const ping: Capability = {
  name: "ping",
  description: "Nothing",
  parameters: { type: "object", properties: {} },
  actio: { result: true },
};

const bob: AgentView = {
  id: "bob",
  folder: "/actio/agents/bob",
  spec: {
    id: "bob",
    actio_version: "1",
    datetime: "2026-01-01T00:00:00Z",
    description: "Bob the peer",
    namespaces: null,
    accepts_commands_from: ["*"],
    capabilities: [ping],
    security_support: [],
    security_posture: "trusted",
  },
  status: {
    id: "bob",
    status: "busy",
    datetime: "2026-01-01T00:00:00Z",
    capability_status: [],
  },
  health: "degraded",
  commands: false,
  capabilities: new Map([["ping", ping]]),
};

const response: ResponseBody = {
  ts: 1767225600,
  nonce: "nonce-1",
  signature: null,
  error: false,
  cancellation: false,
  result: { greeting: "hello, world" },
  attachments: [],
};

const outcome: RequestOutcome = { finalStatus: "confirmed", response };

test("list-team text", () => {
  assert.equal(
    formatTeam([bob, alice], false),
    `- **alice** — The test peer — capabilities: "greet", "slow"\n- **bob** — Bob the peer — capabilities: "ping"`,
  );
});

test("list-team text, no agents", () => {
  assert.equal(formatTeam([], false), "No visible agents.");
});

test("list-team JSON", () => {
  assert.equal(
    formatTeam([bob, alice], true),
    `[
  {
    "id": "alice",
    "description": "The test peer",
    "capabilities": [
      "greet",
      "slow"
    ]
  },
  {
    "id": "bob",
    "description": "Bob the peer",
    "capabilities": [
      "ping"
    ]
  }
]`,
  );
});

test("list-team JSON, no agents", () => {
  assert.equal(formatTeam([], true), "[]");
});

test("info text, all capabilities", () => {
  assert.equal(
    formatInfo(alice, undefined, false),
    `# alice
The test peer
Status: idle · Health: online · Namespaces: default, void

## greet
Say hello
max_parallel: 2
Parameters
\`\`\`json
{
  "type": "object",
  "properties": {
    "name": {
      "type": "string"
    }
  },
  "required": [
    "name"
  ]
}
\`\`\`
Result
\`\`\`json
{
  "type": "object",
  "properties": {
    "greeting": {
      "type": "string"
    }
  }
}
\`\`\`

## slow
A slow capability
Parameters
\`\`\`json
{
  "type": "object",
  "properties": {}
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
    formatInfo(alice, "greet", false),
    `# alice
The test peer
Status: idle · Health: online · Namespaces: default, void

## greet
Say hello
max_parallel: 2
Parameters
\`\`\`json
{
  "type": "object",
  "properties": {
    "name": {
      "type": "string"
    }
  },
  "required": [
    "name"
  ]
}
\`\`\`
Result
\`\`\`json
{
  "type": "object",
  "properties": {
    "greeting": {
      "type": "string"
    }
  }
}
\`\`\``,
  );
});

test("info JSON, all capabilities", () => {
  assert.equal(
    formatInfo(alice, undefined, true),
    `{
  "id": "alice",
  "description": "The test peer",
  "status": "idle",
  "health": "online",
  "namespaces": [
    "default",
    null
  ],
  "capabilities": [
    {
      "name": "greet",
      "description": "Say hello",
      "parameters": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string"
          }
        },
        "required": [
          "name"
        ]
      },
      "actio": {
        "result": {
          "type": "object",
          "properties": {
            "greeting": {
              "type": "string"
            }
          }
        },
        "max_parallel": 2
      }
    },
    {
      "name": "slow",
      "description": "A slow capability",
      "parameters": {
        "type": "object",
        "properties": {}
      },
      "actio": {
        "result": true
      }
    }
  ]
}`,
  );
});

test("info JSON, one capability", () => {
  assert.equal(
    formatInfo(alice, "greet", true),
    `{
  "id": "alice",
  "description": "The test peer",
  "status": "idle",
  "health": "online",
  "namespaces": [
    "default",
    null
  ],
  "capabilities": [
    {
      "name": "greet",
      "description": "Say hello",
      "parameters": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string"
          }
        },
        "required": [
          "name"
        ]
      },
      "actio": {
        "result": {
          "type": "object",
          "properties": {
            "greeting": {
              "type": "string"
            }
          }
        },
        "max_parallel": 2
      }
    }
  ]
}`,
  );
});

test("info throws on an unknown capability", () => {
  assert.throws(
    () => formatInfo(alice, "nope", false),
    /unknown capability "nope" for agent "alice"; available: "greet", "slow"/,
  );
});

test("call text, confirmed", () => {
  assert.equal(
    formatCall("alice/greet", outcome, false),
    `**alice/greet** → confirmed

\`\`\`json
{
  "greeting": "hello, world"
}
\`\`\``,
  );
});

test("call JSON, confirmed", () => {
  assert.equal(
    formatCall("alice/greet", outcome, true),
    `{
  "target": "alice/greet",
  "finalStatus": "confirmed",
  "response": {
    "ts": 1767225600,
    "nonce": "nonce-1",
    "signature": null,
    "error": false,
    "cancellation": false,
    "result": {
      "greeting": "hello, world"
    },
    "attachments": []
  }
}`,
  );
});

test("call text, error answer", () => {
  const errorOutcome: RequestOutcome = {
    finalStatus: "confirmed",
    response: {
      ts: 1767225601,
      nonce: "nonce-2",
      signature: null,
      error: true,
      cancellation: false,
      result: { code: 105, msg: "bad params", details: "name: required" },
      attachments: [],
    },
  };
  assert.equal(formatCall("alice/greet", errorOutcome, false), `**alice/greet** → error 105: bad params

details:
name: required`);
});

test("call JSON, error answer", () => {
  const errorOutcome: RequestOutcome = {
    finalStatus: "confirmed",
    response: {
      ts: 1767225601,
      nonce: "nonce-2",
      signature: null,
      error: true,
      cancellation: false,
      result: { code: 105, msg: "bad params", details: "name: required" },
      attachments: [],
    },
  };
  assert.equal(
    formatCall("alice/greet", errorOutcome, true),
    `{
  "target": "alice/greet",
  "finalStatus": "confirmed",
  "response": {
    "ts": 1767225601,
    "nonce": "nonce-2",
    "signature": null,
    "error": true,
    "cancellation": false,
    "result": {
      "code": 105,
      "msg": "bad params",
      "details": "name: required"
    },
    "attachments": []
  }
}`,
  );
});

test("call text, cancelled", () => {
  assert.equal(formatCall("alice/greet", { finalStatus: "cancelled" }, false), `**alice/greet** → cancelled`);
});

test("call JSON, cancelled", () => {
  assert.equal(
    formatCall("alice/greet", { finalStatus: "cancelled" }, true),
    `{
  "target": "alice/greet",
  "finalStatus": "cancelled"
}`,
  );
});

test("call text, abandoned", () => {
  assert.equal(
    formatCall("alice/greet", { finalStatus: "abandoned" }, false),
    `**alice/greet** → abandoned (no response)`,
  );
});

test("call JSON, abandoned", () => {
  assert.equal(
    formatCall("alice/greet", { finalStatus: "abandoned" }, true),
    `{
  "target": "alice/greet",
  "finalStatus": "abandoned"
}`,
  );
});

test("call text, confirmed without a readable response", () => {
  assert.equal(
    formatCall("alice/greet", { finalStatus: "confirmed" }, false),
    `**alice/greet** → confirmed (encrypted response, not readable)`,
  );
});

test("call JSON, confirmed without a readable response", () => {
  assert.equal(
    formatCall("alice/greet", { finalStatus: "confirmed" }, true),
    `{
  "target": "alice/greet",
  "finalStatus": "confirmed"
}`,
  );
});