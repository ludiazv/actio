// pack-smoke.mjs — prove the published artifact is self-contained (Gate 6 of the publish gate).
//
// It packs the package, inspects the tarball, installs it into a throwaway directory exactly the way a
// consumer would (`--omit=dev`), and drives it from **plain Node**: no tsx, no jiti, nothing from this
// repository on the import path. It also verifies that the schemas embedded in the tarball still match
// `../doc/schemas/`. Run by `npm run check:publish`; needs network or a warm npm cache (ajv is installed).
//
// Usage: node scripts/pack-smoke.mjs
import { execFileSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const pkgDir = path.resolve(import.meta.dirname, "..");
const schemasDir = path.resolve(pkgDir, "..", "doc", "schemas");
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "actio-pack-smoke-"));

let failures = 0;
let packed; // the tarball npm pack writes into the package dir; removed at the end
const check = (ok, what) => {
  console.log(`${ok ? "ok  " : "FAIL"}  ${what}`);
  if (!ok) failures++;
};
// Nested npm calls inherit the parent's npm_* environment: inside `npm publish` (whose prepublishOnly hook
// runs this script) that includes npm_config_dry_run=true, so `npm pack` prints a tarball name but writes no
// file, and `npm install` installs nothing. Children get a clean environment.
const childEnv = Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("npm_")));
const npm = (args, cwd) => execFileSync("npm", args, { cwd, env: childEnv, encoding: "utf8" });

try {
  // 1. Pack, and unpack into the temp dir.
  const tgz = npm(["pack", "--silent"], pkgDir).trim().split("\n").pop();
  packed = tgz;
  check(/\.tgz$/.test(tgz), `packed ${tgz}`);
  const tarball = path.join(pkgDir, tgz);
  const listed = execFileSync("tar", ["-tzf", tarball], { encoding: "utf8" }).trim().split("\n");
  const pkg = path.join(tmp, "package");
  execFileSync("tar", ["-xzf", tarball, "-C", tmp]);
  const manifest = JSON.parse(fs.readFileSync(path.join(pkg, "package.json"), "utf8"));

  // 2. Nothing local in the manifest: a `file:`/`link:`/`workspace:` specifier or a path escaping the
  //    package would install fine here and break everywhere else.
  const specs = JSON.stringify({ ...(manifest.dependencies ?? {}), ...(manifest.peerDependencies ?? {}) });
  check(!/(file:|link:|workspace:|\.\.\/)/.test(specs), `no local dependency in the manifest: ${specs}`);
  if (manifest.private === true) console.log("note  private:true — flip it in the publish commit; publishing is not the job of this script");

  // 3. The file list: what must be there, and what must not.
  for (const need of ["package/dist/index.js", "package/dist/index.d.ts", "package/src/index.ts", "package/src/generated/schemas.ts", "package/LICENSE", "package/README.md"]) {
    check(listed.includes(need), `${need} published`);
  }
  for (const bad of ["package/test/", "package/scripts/", "package/examples/", "package/orchestra/", "package/node_modules/", "package/tsconfig"]) {
    check(!listed.some((f) => f.startsWith(bad)), `${bad} NOT published`);
  }

  // 4. The embedded schemas in the artifact are the current ones — the revision of record travels with it.
  const header = fs.readFileSync(path.join(pkg, "src/generated/schemas.ts"), "utf8").slice(0, 2000);
  const sources = fs.readdirSync(schemasDir).filter((f) => f.endsWith(".json"));
  const stale = sources.filter((f) => !header.includes(crypto.createHash("sha256").update(fs.readFileSync(path.join(schemasDir, f))).digest("hex")));
  check(sources.length === 11 && stale.length === 0, `embedded schema SHAs match ${schemasDir} (${sources.length} schemas${stale.length ? `, stale: ${stale.join(",")}` : ""})`);

  // 5. Install as a consumer does, then drive it from plain Node.
  const consumer = path.join(tmp, "consumer");
  fs.mkdirSync(consumer);
  fs.writeFileSync(path.join(consumer, "package.json"), JSON.stringify({ name: "smoke-consumer", private: true, type: "module" }));
  npm(["install", tarball, "--omit=dev", "--legacy-peer-deps", "--no-audit", "--no-fund", "--silent"], consumer);
  const consumerSrc = `
import fs from "node:fs/promises";
import path from "node:path";
import { ActioAgent, checkParams, health, readDiarium, scan } from "actio-ts";

const root = process.argv[2];
const PING = {
  name: "ping",
  description: "answers ping",
  parameters: { type: "object", properties: {}, additionalProperties: false },
  actio: { result: { type: "object", properties: { pong: { type: "boolean" } }, required: ["pong"] } },
};
const agent = new ActioAgent({
  id: "smoke",
  description: "pack smoke agent",
  acceptsCommandsFrom: [],
  config: { root, heartbeatInterval: 200 },
});
agent.capability(PING, async () => ({ pong: true }));
await agent.start();
const found = await scan(root);
const status = JSON.parse(await fs.readFile(path.join(root, "smoke", "status.json"), "utf8"));
const diary = await readDiarium(path.join(root, "smoke"));
const rejected = checkParams(PING, { unexpected: 1 }); // ajv over the embedded schemas
await agent.stop();
if (found.agents.length !== 1) throw new Error(\`scan found \${found.agents.length} agents\`);
if (!JSON.stringify(found.agents).includes("ping")) throw new Error("the capability is missing from the published spec");
if (health(status, Date.now()) !== "healthy") throw new Error(\`health: \${health(status, Date.now())}\`);
if (diary.lines.length === 0) throw new Error("the diarium is empty");
if (rejected === undefined) throw new Error("checkParams accepted an unknown property");
console.log("CONSUMER OK: published, discovered, healthy, diarium written, parameters validated");
`;
  fs.writeFileSync(path.join(consumer, "consumer.mjs"), consumerSrc);
  const out = execFileSync(process.execPath, ["consumer.mjs", path.join(tmp, "actio-root")], {
    cwd: consumer,
    encoding: "utf8",
    timeout: 60_000,
  });
  check(out.includes("CONSUMER OK"), `plain-Node consumer: ${out.trim().split("\n").pop()}`);
} finally {
  if (packed) fs.rmSync(path.join(pkgDir, packed), { force: true });
  fs.rmSync(tmp, { recursive: true, force: true });
}

if (failures > 0) {
  console.error(`pack-smoke: ${failures} failure(s)`);
  process.exit(1);
}
console.log("pack-smoke: the tarball is self-contained");
