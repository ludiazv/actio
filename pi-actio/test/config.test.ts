// Tests for config.ts: defaults, a full valid file, invalid files, and the default agent id.
// Only temp files and temp cwd dirs are used; the user's ~/.pi/agent/actio.json is never read.
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { configPath, defaultAgentId, loadConfig } from "../src/config.js";

function scratch(): { dir: string; file: string; cwd: string } {
  const dir = mkdtempSync(join(tmpdir(), "pi-actio-cfg-"));
  return { dir, file: join(dir, "actio.json"), cwd: join(dir, "workspace") };
}

test("loadConfig: a missing file means all defaults", () => {
  const { dir, file, cwd } = scratch();
  try {
    const config = loadConfig(file, cwd);
    assert.match(config.agentId, /^workspace-[0-9a-f]{4}$/);
    assert.equal(config.namespaces, undefined);
    assert.equal(config.actioDir, undefined);
    assert.equal(config.exposePrompt, false);
    assert.deepEqual(config.acceptPrompt, []);
    assert.equal(config.disabled, false);
    assert.equal(config.json, false);
    assert.equal(config.reportActivity, false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("loadConfig: a full valid file is returned as written", () => {
  const { dir, file, cwd } = scratch();
  try {
    writeFileSync(
      file,
      JSON.stringify({
        agentId: "alpha",
        namespaces: ["ns1", "ns2"],
        actioDir: "~/actio-test",
        exposePrompt: true,
        acceptPrompt: ["alpha1", "beta"],
        disabled: true,
        json: true,
        reportActivity: true,
      }),
    );
    assert.deepEqual(loadConfig(file, cwd), {
      agentId: "alpha",
      namespaces: ["ns1", "ns2"],
      actioDir: "~/actio-test",
      exposePrompt: true,
      acceptPrompt: ["alpha1", "beta"],
      disabled: true,
      json: true,
      reportActivity: true,
    });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("loadConfig: an unknown key throws an error that names the path", () => {
  const { dir, file, cwd } = scratch();
  try {
    writeFileSync(file, JSON.stringify({ agentId: "alpha", bogus: 1 }));
    assert.throws(
      () => loadConfig(file, cwd),
      (err: Error) => err.message.startsWith(`${file}: `) && err.message.includes("bogus"),
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("loadConfig: a wrong type throws an error that names the path", () => {
  const { dir, file, cwd } = scratch();
  try {
    writeFileSync(file, JSON.stringify({ agentId: 42 }));
    assert.throws(
      () => loadConfig(file, cwd),
      (err: Error) => err.message.startsWith(`${file}: `) && err.message.includes("agentId"),
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("loadConfig: an agentId outside the pattern throws naming the path", () => {
  const { dir, file, cwd } = scratch();
  try {
    writeFileSync(file, JSON.stringify({ agentId: "way too long" }));
    assert.throws(
      () => loadConfig(file, cwd),
      (err: Error) => err.message.startsWith(`${file}: `) && err.message.includes("agentId"),
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("loadConfig: an empty or partial file is valid with defaults; acceptPrompt items keep the id pattern", () => {
  const { dir, file, cwd } = scratch();
  try {
    writeFileSync(file, "{}");
    assert.match(loadConfig(file, cwd).agentId, /^workspace-[0-9a-f]{4}$/);
    writeFileSync(file, JSON.stringify({ disabled: true }));
    assert.equal(loadConfig(file, cwd).disabled, true);
    writeFileSync(file, JSON.stringify({ acceptPrompt: ["bad id!"] }));
    assert.throws(
      () => loadConfig(file, cwd),
      (err: Error) => err.message.startsWith(`${file}: `) && err.message.includes("acceptPrompt"),
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("loadConfig: bad JSON throws an error that names the path", () => {
  const { dir, file, cwd } = scratch();
  try {
    writeFileSync(file, '{"agentId": "alpha",');
    assert.throws(
      () => loadConfig(file, cwd),
      (err: Error) => err.message.startsWith(`${file}: `),
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("configPath: joins the agent dir and actio.json", () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-actio-cfg-"));
  const saved = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = dir;
  try {
    assert.equal(configPath(), join(dir, "actio.json"));
  } finally {
    if (saved === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = saved;
    rmSync(dir, { recursive: true, force: true });
  }
});

test("defaultAgentId: shape, length, pattern, basename rules, and stability per cwd", () => {
  const cwd = join(tmpdir(), "some dir.x");
  const first = defaultAgentId(cwd);
  assert.match(first, /^[a-zA-Z0-9_-]{1,32}$/);
  assert.ok(first.length <= 32);
  assert.match(first, /^some_dir_x-[0-9a-f]{4}$/);
  assert.equal(defaultAgentId(cwd), first); // kept per process and cwd
  const long = defaultAgentId(join(tmpdir(), "a".repeat(40)));
  assert.equal(long.length, 32); // 27 + "-" + 4 hex
  assert.match(long, /^a{27}-[0-9a-f]{4}$/);
  assert.match(defaultAgentId("/"), /^pi-[0-9a-f]{4}$/);
});