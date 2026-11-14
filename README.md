<p align="center">
  <img src="actio-logo.svg" alt="Actio" width="220">
</p>

# Actio

**Actio** is an experimental protocol, with tools, for coordinating complex agentic setups. Agents share
one watchable directory: each one announces what it can do, and they exchange JSON request and response
files through each other's inboxes. There is no broker and no network layer, and the protocol does not
care which harness runs the agents. The specification is in [`doc/actio_protocol.md`](doc/actio_protocol.md).

> [!WARNING]
> **Under heavy development.** The protocol is a v0.1 draft. Specs, APIs and file formats may change
> without notice.

> [!CAUTION]
> **The protocol's security features are not implemented yet.** The current implementations cover only
> the core conformance profile, which assumes a trusted environment. Do not use Actio where agents,
> users or the shared directory cannot be trusted.

## Purpose

- **Educational and experimental.** The project explores how agent teams can coordinate through plain
  files and strict message rules.
- **Built by agents.** Most of the code and documents are written by agent teams, mostly running
  **local models**.
- **Minimal human intervention.** A second goal is to see how far agents can plan, build, review and
  validate the project with as little human input as possible.

## Sub-projects

| Path | What it is |
| --- | --- |
| [`doc/`](doc/) | The Actio protocol specification and its JSON schemas, plus the agent skills used to plan and orchestrate the build. |
| [`actio-ts/`](actio-ts/) | The reference implementation of the protocol (core conformance profile), a TypeScript client library. |
| [`actio-py/`](actio-py/) | A Python implementation of the protocol (core conformance profile). |
| [`actio-go/`](actio-go/) | A Go port of the TypeScript reference implementation. |
| [`pi-actio/`](pi-actio/) | A pi extension that makes a pi coding agent a member of an Actio team. |
| [`agents/hora/`](agents/hora/) | Hora, a scheduler agent that lets any agent set up timed capability calls. |
