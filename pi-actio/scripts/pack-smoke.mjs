// pack-smoke.mjs — prove the published artifact is what Pi can install and load.
//
// It packs the package, inspects the tarball, and installs it into a throwaway directory the way Pi installs
// an npm package (`npm install <spec> --prefix <pi root> --legacy-peer-deps`), then checks three things only a
// pack can show: that the extension entry named in `pi.extensions` is **inside** the tarball; that every
// import in the published source is declared in the manifest; and that the host packages Pi provides were not
// dragged in as physical copies. Unlike actio-ts there is no build and no schema to re-verify: Pi loads
// `src/*.ts` through jiti, so the source *is* the artifact.
//
// When the `pi` binary is on PATH (a development box, not a GitHub runner) it also runs `npm run smoke`, the
// end-to-end run of the extension inside real pi with the faux provider. Otherwise it says so and moves on:
// a CI job must not fail for a missing host.
//
// Usage: node scripts/pack-smoke.mjs
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const pkgDir = path.resolve(import.meta.dirname, "..");
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "pi-actio-pack-smoke-"));

let failures = 0;
let packed; // the tarball `npm pack` writes into the package dir; removed at the end
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

  // 2. The manifest: publishable, and free of anything local.
  const specs = JSON.stringify({ ...(manifest.dependencies ?? {}), ...(manifest.peerDependencies ?? {}) });
  check(!/(file:|link:|workspace:|\.\.\/)/.test(specs), `no local dependency in the manifest: ${specs}`);
  check(manifest.private !== true, "private is not true");
  //    Provenance refuses a package the registry has never seen unless the access is declared ("Can't
  //    generate provenance for new or private package, you must set `access` to public"), so the declaration
  //    lives in the manifest, where every publish path picks it up, not in a CI flag.
  check(manifest.publishConfig?.access === "public", "publishConfig.access is public (provenance demands it for a new package)");
  check((manifest.keywords ?? []).includes("pi-package"), "keywords include pi-package (what makes it discoverable in the pi gallery)");

  // 3. What Pi actually loads: the entry named by the pi manifest must be in the tarball.
  const entries = manifest.pi?.extensions ?? [];
  check(Array.isArray(entries) && entries.length > 0, "pi.extensions declares at least one entry");
  for (const entry of entries) {
    const inTarball = `package/${entry.replace(/^\.\//, "")}`;
    check(listed.includes(inTarball), `${inTarball} — the entry pi loads with jiti — is in the tarball`);
  }

  // 4. The file list: what must be there, and what must not.
  for (const need of ["package/src/index.ts", "package/src/config.ts", "package/src/team.ts", "package/src/prompt.ts", "package/src/tool.ts", "package/src/format.ts", "package/src/history.ts", "package/src/explorer.ts", "package/README.md", "package/internals.md", "package/CHANGELOG.md", "package/LICENSE"]) {
    check(listed.includes(need), `${need} published`);
  }
  for (const bad of ["package/test/", "package/scripts/", "package/orchestra/", "package/node_modules/", "package/tsconfig", "package/dist", "package/spec.md", "package/compaction-extension.md", "package/compaction-proposal.md"]) {
    check(!listed.some((f) => f.startsWith(bad)), `${bad} NOT published`);
  }

  // 5. Every import in the published source is declared. A missing entry here installs fine and then fails to
  //    resolve at load time inside pi, which is the worst place to discover it. Host packages are allowed as
  //    peers because pi provides them; anything else must be a real dependency.
  const declared = new Set([...Object.keys(manifest.dependencies ?? {}), ...Object.keys(manifest.peerDependencies ?? {})]);
  const importRe = /(?:from|import)\s+["']([^"']+)["']/g;
  const undeclared = new Set();
  for (const file of fs.readdirSync(path.join(pkg, "src"))) {
    if (!file.endsWith(".ts")) continue;
    for (const m of fs.readFileSync(path.join(pkg, "src", file), "utf8").matchAll(importRe)) {
      const spec = m[1];
      if (spec.startsWith(".") || spec.startsWith("node:")) continue;
      const root = spec.startsWith("@") ? spec.split("/").slice(0, 2).join("/") : spec.split("/")[0];
      if (!declared.has(root)) undeclared.add(`${root} (in src/${file})`);
    }
  }
  check(undeclared.size === 0, `every import in the published src is declared${undeclared.size ? `: undeclared ${[...undeclared].join(", ")}` : ` (${[...declared].join(", ")})`}`);

  // 6. Install the way pi installs an npm package, then look at what landed.
  const consumer = path.join(tmp, "consumer");
  fs.mkdirSync(consumer);
  fs.writeFileSync(path.join(consumer, "package.json"), JSON.stringify({ name: "smoke-consumer", private: true, type: "module" }));
  npm(["install", tarball, "--omit=dev", "--legacy-peer-deps", "--no-audit", "--no-fund", "--silent"], consumer);
  const installed = fs.readdirSync(path.join(consumer, "node_modules"));
  const has = (name) => installed.includes(name) || (name.startsWith("@") && fs.existsSync(path.join(consumer, "node_modules", name)));
  check(has("pi-actio"), "the consumer got pi-actio");
  check(has("actio-ts"), "the consumer got actio-ts from the registry, not from this repository");
  check(has("ajv"), "actio-ts's own dependency came along");
  //    A physical copy of a host package bypasses pi's extension module mapping and creates duplicate
  //    classes, registries and initialization work — pi warns about exactly this manifest shape.
  const bundled = installed.filter((n) => n.startsWith("@earendil-works") || n === "typebox");
  check(bundled.length === 0, `no host package was bundled (${installed.length} entries in node_modules)`);
  //    The dependency resolves and runs from plain Node, with nothing of this repository on the path.
  const out = execFileSync(process.execPath, ["-e", "import('actio-ts').then(m => { if (!m.ActioAgent) throw new Error('no ActioAgent'); console.log('actio-ts resolves from the installed tree: ' + Object.keys(m).length + ' exports'); })"], {
    cwd: consumer,
    encoding: "utf8",
    timeout: 60_000,
  });
  check(out.includes("actio-ts resolves"), `consumer: ${out.trim()}`);

  // 7. End to end inside real pi, when a pi binary exists. CI runners have none, and that is not a failure —
  //    but it is said out loud rather than skipped in silence.
  const hasPi = (() => {
    try {
      execFileSync("pi", ["--version"], { env: childEnv, encoding: "utf8", stdio: "ignore", timeout: 30_000 });
      return true;
    } catch {
      return false;
    }
  })();
  if (hasPi) {
    const smoke = npm(["run", "--silent", "smoke"], pkgDir);
    check(smoke.includes("SMOKE OK"), "pi ran the extension end to end (test/smoke.ts)");
  } else {
    console.log("skip  pi binary not on PATH: the end-to-end smoke (test/smoke.ts) runs on a development box");
  }
} finally {
  if (packed) fs.rmSync(path.join(pkgDir, packed), { force: true });
  fs.rmSync(tmp, { recursive: true, force: true });
}

if (failures > 0) {
  console.error(`pack-smoke: ${failures} failure(s)`);
  process.exit(1);
}
console.log("pack-smoke: the tarball is what pi can install");
