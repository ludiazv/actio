# Changelog

Pi reads this file after it updates a package and shows the entries newer than the version it last saw under
"Extension Updates", matching a `## <version>` heading against `version` in `package.json`. Newest first.

The extension speaks **Actio protocol v0.1, core conformance profile**, and holds no protocol logic itself:
everything protocol-shaped comes from [`actio-ts`](https://www.npmjs.com/package/actio-ts), whose changelog
carries the SHA-256 digests that fix a protocol revision.

## 0.1.0 — 2026-11-14

First publishable release: install it with `pi install npm:pi-actio@0.1.0`.

- **What it adds**: the `actio` tool (`list-team`, `info`, `call`), the `/actio` Explorer (Team / Calls /
  Prompts), and — when `exposePrompt` is on — the prompt service: `prompt`, `retrieve`, `cancel`, `compact`.
  One configuration file, `actio.json`, in pi's agent directory; eight optional keys.
- **Artifact**: TypeScript source, no build step — pi loads the entry named in `pi.extensions` through jiti.
  `dependencies` carries exactly one thing, `actio-ts` pinned to **0.1.0**; the pi packages
  (`pi-coding-agent`, `pi-tui`, `pi-ai`) and `typebox` are `peerDependencies: "*"`, because pi provides them
  and a physical copy would bypass its extension module mapping.
- **Protocol status**: v0.1 is a **draft**. Its security layer is *unspecified* — envelopes are unsigned and
  unauthenticated, and write access to the shared actio root **is** peer authority. Do not point two teams'
  folders at each other.
- **Known limits**: history lives in memory per session (nothing survives `/reload`), no protocol commands
  (`cmd`), no attachments, no retries, no per-call timeout, and the prompt service answers with a run's final
  text only.
- **Release gate**: `npm run check:publish` = typecheck + the 35 tests + `scripts/pack-smoke.mjs`, which
  installs the packed tarball the way pi installs an npm package (`--legacy-peer-deps`), checks that the
  `pi.extensions` entry is inside the tarball, that every import in the published source is declared, and that
  no host package was bundled — and, where a `pi` binary exists, runs the extension end to end inside real pi.
- **Release pipeline**: `.github/workflows/publish-pi-actio.yml` — pushing the tag `pi-actio/v<version>` (a
  per-package namespace: a repo-wide `v*` publishes nothing in this monorepo) runs that gate on Node 22 and
  24, then publishes with provenance.
