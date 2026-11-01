// `actio` CLI (§2.8): ls, diarium, call, cmd, broadcast, validate. Reads ACTIO_DIR/ACTIO_SYNC.
import { readFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { loadConfig } from "./config.js";
import * as diary from "./diarium.js";
import { scan } from "./discovery.js";
import * as io from "./io.js";
import { Requestor } from "./requestor.js";
import { health } from "./status.js";
import { createSyncProvider } from "./sync.js";
import { toIso } from "./util.js";
import * as v from "./validate.js";
import type { ActioConfig, AgentContext, AgentFolders, RequestOutcome, SyncProvider } from "./types.js";

/** Where the CLI writes; tests pass collectors instead of the process streams. */
export interface Sinks {
  out(line: string): void;
  err(line: string): void;
}

const terminal: Sinks = {
  out: (line) => process.stdout.write(`${line}\n`),
  err: (line) => process.stderr.write(`${line}\n`),
};

/** One resolved subcommand call: config and sync provider, parsed arguments, sinks. */
interface Call {
  config: ActioConfig;
  sync: SyncProvider;
  json: boolean;
  pos: (string | undefined)[]; // positionals after the subcommand name
  opt: Record<string, string>; // value flags: -n, --timeout, --as
  out(line: string): void;
  err(line: string): void;
}

/** `call`'s own default wait in seconds: a CLI process has no heartbeat to keep it alive, so it
 * must not send `timeout: 0` — that is what only a started agent may afford. */
const DEFAULT_TIMEOUT = 60;

const USAGE =
  `usage: actio [--json] ls | diarium <agent> [-n N] | call <src-id> <dst> <capability> <json-params> [--timeout s (default ${DEFAULT_TIMEOUT})]` +
  ' | cmd <src-id> <dst> <name> [json-params] | broadcast "<text>" | validate <file> [--as <kind>]';

/** Bad arguments, unparsable JSON, unknown subcommand: exit 1 with the usage line. */
class Usage extends Error {}

const why = (err: unknown): string => (err instanceof Error ? err.message : String(err));

/** Flags that take a value; `--json` is global; everything else is positional. */
const VALUE_FLAGS = new Set(["-n", "--timeout", "--as"]);

function parse(argv: string[]): { json: boolean; pos: string[]; opt: Record<string, string> } {
  const pos: string[] = [];
  const opt: Record<string, string> = {};
  let json = false;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    if (arg === "--json") json = true;
    else if (VALUE_FLAGS.has(arg)) {
      const value = argv[++i]; // the value of a flag is not a positional
      if (value === undefined) throw new Usage(`${arg} needs a value`);
      opt[arg] = value;
    } else pos.push(arg);
  }
  return { json, pos, opt };
}

/** The number behind a value flag (`def` when absent); negative or non-numeric is a usage error. */
function num(opt: Record<string, string>, flag: string, def: number): number {
  const raw = opt[flag];
  if (raw === undefined) return def;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) throw new Usage(`${flag} must be a number >= 0, got "${raw}"`);
  return value;
}

/** `--timeout`: seconds, strictly above 0, else the usage error that states the rule. */
function timeoutOf(opt: Record<string, string>): number {
  const raw = opt["--timeout"];
  if (raw === undefined) return DEFAULT_TIMEOUT;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) throw new Usage(`--timeout must be > 0 seconds, got "${raw}"`);
  return value;
}

function parseJson(raw: string, what: string): unknown {
  try {
    return JSON.parse(raw);
  } catch (err) {
    throw new Usage(`${what} is not valid JSON: ${why(err)}`);
  }
}

/** Reads a text file, reporting a failure instead of throwing; null means "exit 1". */
async function readText(c: Call, file: string): Promise<string | null> {
  try {
    return await readFile(file, "utf8");
  } catch (err) {
    c.err(`cannot read "${file}": ${why(err)}`);
    return null;
  }
}

/** Entry point; returns the process exit code. */
export async function main(argv: string[], sinks: Sinks = terminal): Promise<number> {
  try {
    const { json, pos, opt } = parse(argv);
    const sub = pos[0];
    const handler = sub === undefined ? undefined : SUBS[sub];
    if (handler === undefined) {
      sinks.err(sub === undefined ? USAGE : `unknown subcommand "${sub}"\n${USAGE}`);
      return 1;
    }
    const { config, sync } = resolved(sinks); // ACTIO_DIR and ACTIO_SYNC (§2.8), I-10 process-wide
    io.setSyncProvider(sync);
    const c: Call = { config, sync, json, pos: pos.slice(1), opt, out: sinks.out, err: sinks.err };
    return await handler(c);
  } catch (err) {
    sinks.err(err instanceof Usage ? `${err.message}\n${USAGE}` : why(err));
    return 1;
  }
}

/** Config and sync provider from the environment: a bad value is fatal before anything runs. */
function resolved(sinks: Sinks): { config: ActioConfig; sync: SyncProvider } {
  try {
    const config = loadConfig(); // ACTIO_DIR and ACTIO_SYNC (§2.8)
    return { config, sync: createSyncProvider(config.sync, (err) => sinks.err(`sync: ${err.message}`)) };
  } catch (err) {
    throw new Error(`config: ${why(err)}`);
  }
}

/** The six subcommands of §2.8. */
const SUBS: Record<string, (c: Call) => Promise<number>> = {
  ls,
  diarium: readDiarium,
  call: (c) => send(c, "call"),
  cmd: (c) => send(c, "cmd"),
  broadcast,
  validate,
};

/** `ls`: what `discovery.scan` found — status, health, capabilities, plus D-3 and rule-3 reports. */
async function ls(c: Call): Promise<number> {
  const found = await scan(c.config.root);
  const now = c.config.clock();
  const agents = found.agents.map((a) => ({
    id: a.spec.id,
    folder: a.folder,
    status: a.status.status,
    datetime: a.status.datetime,
    health: health(a.status, now) ?? "unknown", // undefined = an age that cannot be rated
    capabilities: a.spec.capabilities.map((cap) => ({
      name: cap.name,
      disabled: cap.actio.disabled === true,
      accepts_from: cap.actio.accepts_from ?? [], // absent or empty means everybody
    })),
  }));
  if (c.json) {
    const counts = { agents: agents.length, duplicates: found.duplicates.length, ignored: found.ignored.length };
    c.out(JSON.stringify({ root: c.config.root, counts, agents, duplicates: found.duplicates, ignored: found.ignored }, null, 2));
    return 0;
  }
  for (const a of agents) {
    c.out(`${a.id}  ${a.status}  ${a.health}  ${a.datetime}  ${a.folder}`);
    for (const cap of a.capabilities) {
      const hints = `${cap.disabled ? " [disabled]" : ""}${cap.accepts_from.length > 0 ? ` [accepts_from: ${cap.accepts_from.join(",")}]` : ""}`;
      c.out(`    ${cap.name}${hints}`);
    }
  }
  for (const dup of found.duplicates) c.out(`duplicate agent id: ${dup}`);
  for (const ign of found.ignored) c.out(`ignored: ${ign.path}: ${ign.reason}`);
  if (agents.length === 0) c.out(`no agents under ${c.config.root}`);
  return 0;
}

/** `diarium <agent> [-n N]`: the last N lines, one JSON per line (indented with --json). */
async function readDiarium(c: Call): Promise<number> {
  const agent = c.pos[0];
  if (agent === undefined) throw new Usage("diarium needs an agent id");
  const n = num(c.opt, "-n", 20);
  if (!Number.isInteger(n) || n < 1) throw new Usage(`-n must be a positive integer, got "${c.opt["-n"]}"`);
  const raw = await readText(c, path.join(c.config.root, agent, "diarium.jsonl"));
  if (raw === null) return 1;
  for (const line of raw.split("\n").filter((l) => l.trim() !== "").slice(-n)) {
    try {
      c.out(c.json ? JSON.stringify(JSON.parse(line), null, 2) : line);
    } catch {
      c.out(line); // an unparsable line is shown as it is: this command reads, it does not judge
    }
  }
  return 0;
}

/** The lane-B vehicle of `call`/`cmd`. Not an agent: no spec, no status, no announce (brief §3.4). */
function vehicle(c: Call, src: string, logs: string[]): Requestor {
  const agent = path.join(c.config.root, src);
  const folders: AgentFolders = {
    root: c.config.root,
    agent,
    inbox: path.join(agent, "inbox"),
    forum: path.join(c.config.root, "forum"),
    nuntium: path.join(c.config.root, "nuntium"),
  };
  const ctx: AgentContext = {
    config: c.config,
    id: src,
    folders,
    sync: c.sync,
    now: () => c.config.clock(),
    spec: () => {
      throw new Error("the CLI vehicle has no spec: it is not a started agent"); // the requestor never asks
    },
    state: () => "idle",
    emit: (e) => logs.push(`${e.event}${"message" in e ? `: ${e.message}` : ""}`),
    appendDiarium: (line) => diary.append(agent, line),
  };
  return new Requestor(ctx, { onWatchError: (dir, err) => logs.push(`watch error in ${dir}: ${why(err)}`), onStateChange: () => undefined });
}

/** `call` and `cmd`: one temporary requestor for exactly one exchange (lane B). */
async function send(c: Call, mode: "call" | "cmd"): Promise<number> {
  const [src, dst, name, rawParams] = c.pos;
  if (src === undefined || dst === undefined || name === undefined || (mode === "call" && rawParams === undefined)) {
    throw new Usage(mode === "call" ? "call needs <src-id> <dst> <capability> <json-params> [--timeout s]" : "cmd needs <src-id> <dst> <name> [json-params]");
  }
  const params = rawParams === undefined ? null : parseJson(rawParams, "json-params");
  const timeout = timeoutOf(c.opt); // never 0: see DEFAULT_TIMEOUT
  await io.makeDir(agentFolder(c, src)); // the tmpDir of the atomic writes; the inbox is for self-calls
  await io.makeDir(path.join(agentFolder(c, src), "inbox"));
  const logs: string[] = [];
  const requestor = vehicle(c, src, logs);
  try {
    if (mode === "cmd") {
      // Only the rid: the envelope is delivered, not executed. A command is never answered, so a
      // peer-side D4 `dropped` is the peer's decision and the CLI cannot know it.
      c.out(`rid: ${await requestor.command(dst, name, params)}`); // written; the peer archives it (D3)
      return 0;
    }
    const outcome = await requestor.request(dst, name, params, { timeout });
    printOutcome(c, outcome);
    return outcome.response === undefined ? 1 : 0; // an answer is an outcome, an error answer included
  } finally {
    await requestor.stop(); // release the dst watchers and their timers
    for (const log of logs) c.err(`${src}: ${log}`);
  }
}

const agentFolder = (c: Call, id: string): string => path.join(c.config.root, id);

/** The outcome of a request: its final status and the answer, if one arrived. */
function printOutcome(c: Call, outcome: RequestOutcome): void {
  if (c.json) {
    c.out(JSON.stringify(outcome, null, 2));
    return;
  }
  c.out(`finalStatus: ${outcome.finalStatus}`);
  const res = outcome.response;
  if (res === undefined) {
    c.out("response: none");
    return;
  }
  const result = (res.result ?? {}) as Record<string, unknown>;
  c.out(res.error ? `error: ${String(result["code"])} ${JSON.stringify(result["msg"])}` : `result: ${JSON.stringify(res.result)}`);
  if (res.cancellation) c.out("cancellation: true");
}

/** `broadcast "<text>"`: the signal file. The CLI is a "special tool": only it writes this one. */
async function broadcast(c: Call): Promise<number> {
  const text = c.pos[0];
  if (text === undefined) throw new Usage('broadcast needs "<text>"');
  const now = c.config.clock();
  const file = { id: "broadcast", datetime: toIso(now), ts: Math.floor(now / 1000), nuntium: text };
  const checked = v.validateAnnounce(file); // I-2: what we are about to write must conform
  if (!checked.ok) {
    c.err(`broadcast: ${checked.errors.join("; ")}`);
    return 1;
  }
  await io.makeDir(path.join(c.config.root, "nuntium"));
  const target = path.join(c.config.root, "nuntium", "nuntium.json");
  await io.writeJsonAtomic(target, file, c.config.root); // tmpDir: the root, same FS as nuntium/
  c.out(`wrote ${target}`);
  return 0;
}

/** `--as` kinds and their validators; the file name decides when `--as` is absent (brief §3.7). */
const VALIDATORS: Record<string, (x: unknown) => v.Validated<unknown>> = {
  actio: v.validateActio,
  spec: v.validateSpec,
  status: v.validateStatus,
  announce: v.validateAnnounce,
  request: v.validateRequest,
  cancellation: v.validateCancellation,
  response: v.validateResponse,
  confirmation: v.validateConfirmation,
  diarium: v.validateDiarium,
};

function kindOf(file: string): string | undefined {
  const base = path.basename(file);
  if (base === "spec.json") return "spec";
  if (base === "status.json") return "status";
  if (base === "actio.json") return "actio";
  if (base === "diarium.jsonl") return "diarium";
  if (base === "nuntium.json" || base.endsWith(".n.json")) return "announce";
  if (base.endsWith(".can.json")) return "cancellation";
  if (base.endsWith(".res.json")) return "response";
  if (base.endsWith(".con.json")) return "confirmation";
  if (base.endsWith(".json")) return "request"; // <rid>.json
  return undefined;
}

/** `validate <file> [--as <kind>]`: `OK <kind>` or the first Ajv error; a diarium line by line. */
async function validate(c: Call): Promise<number> {
  const file = c.pos[0];
  if (file === undefined) throw new Usage("validate needs <file>");
  const kind = c.opt["--as"] ?? kindOf(file);
  if (kind === undefined || VALIDATORS[kind] === undefined) {
    c.err(`no validator for "${c.opt["--as"] ?? path.basename(file)}": use --as ${Object.keys(VALIDATORS).join("|")}`);
    return 1;
  }
  const raw = await readText(c, file);
  if (raw === null) return 1;
  if (kind === "diarium") return validateDiarium(c, raw);
  let obj: unknown;
  try {
    obj = JSON.parse(raw);
  } catch (err) {
    c.err(`${file}: not valid JSON: ${why(err)}`);
    return 1;
  }
  const checked = VALIDATORS[kind](obj);
  if (!checked.ok) {
    c.err(`${kind}: ${checked.errors[0]}`);
    return 1;
  }
  c.out(`OK ${kind}`);
  return 0;
}

/** Every line of a `diarium.jsonl` must be a valid event: report the first bad one with its number. */
function validateDiarium(c: Call, raw: string): number {
  let good = 0;
  for (const [i, text] of raw.split("\n").entries()) {
    if (text.trim() === "") continue;
    let obj: unknown;
    try {
      obj = JSON.parse(text);
    } catch (err) {
      c.err(`diarium: line ${i + 1} is not valid JSON: ${why(err)}`);
      return 1;
    }
    const checked = v.validateDiarium(obj);
    if (!checked.ok) {
      c.err(`diarium: line ${i + 1}: ${checked.errors[0]}`);
      return 1;
    }
    good++;
  }
  c.out(`OK diarium (${good} lines)`);
  return 0;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  process.exitCode = await main(process.argv.slice(2));
}
