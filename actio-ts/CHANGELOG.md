# Changelog

Format: keep-a-changelog style. The package implements **Actio protocol v0.1, core conformance profile**;
the protocol revision is fixed by the SHA-256 of the source schemas, listed per release (the full digests
are in the header of `src/generated/schemas.ts`).

## 0.1.0 — 2026-11-13

First publishable release.

- **Protocol**: v0.1 core profile — `security_support: []`, `security_posture: "trusted"`,
  `signature: null`. The cryptographic layer of the protocol is *unspecified upstream*
  (`doc/actio_protocol.md` §Security); envelopes are unsigned and unauthenticated.
- **Self-contained artifact**: the 11 protocol schemas are embedded verbatim in
  `src/generated/schemas.ts` (validator input at runtime), and `src/generated/` is now **committed** — a
  published package cannot regenerate it, since the generator reads `../doc/schemas/` and needs
  devDependencies. `npm run gen:check` gates drift in the monorepo.
- **Entry point**: compiled `dist/` (ESM + `.d.ts`, **no maps** — `build` starts with `rm -rf dist`, and the
  tarball carries code, types and source rather than three copies of each) via `npm run build`.
  `src/` ships as source for tools that load TypeScript.
- **Revision of record** — SHA-256 (first 12 hex) of each source schema in `doc/schemas/`:
  `actio` `42878a1f7f68` · `announce` `2691e326edf2` · `cancellation` `cd52565378d6` ·
  `common` `d63b38915805` · `confirmation` `a4c32d859e58` · `diarium` `6dec1d475331` ·
  `error` `640695f68cb6` · `request` `41b9fe26797c` · `response` `3dbb69404d03` ·
  `spec` `51106bc814c3` · `status` `cdc840b01e5b`
- **Release gate**: `npm run check:publish` = `gen:check` → typecheck → the full suite (330 tests) → the
  independent black-box run (19/19) → build → `scripts/pack-smoke.mjs`, which installs the packed tarball
  into a temp directory with `--omit=dev` and drives it from plain Node.
- **Release pipeline**: `.github/workflows/publish-actio-ts.yml` — pushing the tag `actio-ts/v<version>`
  (a per-package namespace: a repo-wide `v*` publishes nothing in this monorepo) re-runs that gate on Node
  22 and 24 and publishes with provenance. The job refuses if the tag does not match `package.json`.
