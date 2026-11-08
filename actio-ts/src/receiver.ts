// Receiver: lanes A (requests) and D (commands) — inbox dispatch, per-rid exchange
// tracker, acceptance checks, work with AbortSignal, response, confirmation wait, archive,
// and command dispatch (D0–D4) with the four built-ins handed to the agent lifecycle.
import { open, readFile, rename, stat } from "node:fs/promises";
import path from "node:path";
import * as diarium from "./diarium.js";
import * as io from "./io.js";
import { ActioError } from "./types.js";
import { nonce, toIso } from "./util.js";
import { watchDir } from "./watch.js";
import {
  envelopeKind,
  nameMatches,
  sameHeader,
  validateCancellation,
  validateConfirmation,
  validateRequest,
  validateResponse,
  type Validated,
} from "./validate.js";
import type {
  AgentContext,
  AgentId,
  CapabilityEntry,
  CommandHandler,
  InreqFinalStatus,
} from "./types.js";
import type {
  CancellationBody,
  CancellationFile,
  ConfirmationFile,
  Exchange,
  Header,
  RequestBody,
  RequestFile,
  ResponseBody,
  ResponseFile,
  Rid,
  Spec,
} from "./generated/types.js";

/** Lifecycle callbacks the receiver hands over to the owning agent. */
export interface ReceiverHooks {
  /** Watch error on the own inbox (D-22): the agent logs and fails itself. */
  onWatchError(dir: string, err: Error): void;
  /** I-11: the requestor's spec as DISCOVERED by the last scan (before the callability rules),
   * or undefined when the scan did not find it: D-7's code 106 needs exactly this view, because a peer
   * with no shared namespace is never in the Agmen view. */
  peerSpec(src: AgentId): Spec | undefined;
  /** A busy/idle-relevant exchange state changed (D-8). */
  onStateChange(): void;
  /** Built-in commands that change the lifecycle; the receiver archives first (D3). */
  onCommandStop(force: boolean): Promise<void>;
  onCommandRestart(force: boolean, delaySeconds: number): Promise<void>;
  onCommandEnable(enabled: boolean): Promise<void>;
  onCommandPing(): Promise<void>;
}

/** The commands every agent must implement (*Commands*, the mandatory set); custom commands are the
 * keys of the `commands` map, and a command name is never advertised (*Callability*, D0). */
const STANDARD_COMMANDS = new Set(["stop", "enable", "restart", "ping"]);

/** What `D0` decided about one command envelope; `reason` is for the operator, never for the peer. */
type CommandVerdict = { ok: true; name: string } | { ok: false; reason: string };

/** The phases of one inbound request exchange (workflow A0 → A1 → A2 → A4). */
type Phase = "accepting" | "working" | "responded";

/** The per-rid tracker of one inbound exchange (in-memory only; D-4 re-arms from files at start). */
interface ExchangeState {
  rid: Rid;
  /** The request header (D-11 compares it with every later file, `datetime` excluded). */
  header: Header;
  /** The request file as exchanged; null when re-tracked from a bare `.res.json` at start (D-4). */
  request: RequestFile | null;
  phase: Phase;
  /** A cancellation was seen (before acceptance, during work, or after the response). */
  canSeen: boolean;
  can: CancellationFile | null;
  res: ResponseFile | null;
  con: ConfirmationFile | null;
  /** Response attachments published to `forum`; deleted at A4. */
  responseAttachments: string[];
  abort?: AbortController;
  confirmTimer?: NodeJS.Timeout;
}

/** The options of one response (the single response of the exchange, written only once). */
interface ResponseOptions {
  cancellation: boolean;
  error: boolean;
  /** null for a cancellation response; the capability result or an error result otherwise. */
  result: unknown;
  attachments: string[];
  /** Optional `work_time` (seconds of actual work). */
  workTime?: number;
}

/** The namespaced view of a spec (*Callability rules* 2): absent/null/[] → the null namespace. */
function namespacesOf(spec: Spec): (string | null)[] {
  const ns = spec.namespaces;
  if (ns === undefined || ns === null || ns.length === 0) return [null];
  return ns;
}

/** True when two agents share at least one namespace (`"*"` shares with everyone). */
function sharesNamespace(a: Spec, b: Spec): boolean {
  const na = namespacesOf(a);
  const nb = namespacesOf(b);
  if (na.includes("*") || nb.includes("*")) return true;
  return na.some((n) => nb.includes(n));
}

/** The receiver of one agent: serves inbound requests and commands from its own inbox. */
export class Receiver {
  /** The tracked (accepted, not yet archived) inbound exchanges, by rid. */
  private readonly exchanges = new Map<Rid, ExchangeState>();
  /** D-5: rids archived in this run; a repeated request file is ignored (process lifetime). */
  private readonly archived = new Set<Rid>();
  /** D-15: `(path, mtime)` markers already reported; a log is emitted once per marker. */
  private readonly ignoredSeen = new Set<string>();
  /** Envelopes that arrived before their request: a `.can`/`.con`/`.res`, by rid. */
  private readonly deferred = new Map<Rid, { kind: "can" | "res" | "con"; file: CancellationFile | ResponseFile | ConfirmationFile }[]>();
  /** D-9: the promise of every archive in flight, by rid; `stop()` awaits them all, so that when
   * it resolves EVERY archive has landed (the inreq line and the envelope removals precede C1). */
  private readonly archiving = new Map<Rid, Promise<void>>();
  /** D-5 for commands: the rids whose envelope this run has already dispatched (executed once). */
  private readonly commandRids = new Set<Rid>();
  /** Lane D: the envelopes handed over by `D0`, drained one at a time in rid order. */
  private readonly commandQueue: { rid: Rid; run: () => Promise<void> }[] = [];
  private commandDraining = false;
  private watcher: { close(): void } | null = null;
  private stopped = false;

  constructor(
    private readonly ctx: AgentContext,
    private readonly hooks: ReceiverHooks,
    private readonly capabilities: Map<string, CapabilityEntry>,
    private readonly commands: Map<string, CommandHandler>,
  ) {}

  /** Arms the inbox watcher. The watcher's initial scan is the SINGLE delivery path
   * for the pending envelopes (D-4): every present file is reported exactly once, in lexical order.
   * `start` resolves after arming; the delivery itself is asynchronous. Every file event → `onFile`. */
  async start(onFile: (name: string) => void): Promise<void> {
    const inbox = this.ctx.folders.inbox;
    this.watcher = watchDir(
      inbox,
      (name) => {
        onFile(name); // the agent may fan out (e.g. the requestor's self-calls share this watcher)
        void this.processFile(name);
      },
      (err) => this.hooks.onWatchError(inbox, err), // D-22
    );
  }

  /** Dispatches one inbox file (schema validation first, D-15; header consistency, D-11). */
  async processFile(name: string): Promise<void> {
    if (this.stopped || !name.endsWith(".json")) return; // D-2: only *.json in an inbox
    const full = path.join(this.ctx.folders.inbox, name);
    let raw: string;
    try {
      raw = await readFile(full, "utf8");
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") return; // vanished between event and read
      throw err;
    }
    let obj: unknown;
    try {
      obj = JSON.parse(raw);
    } catch (err) {
      await this.ignore(full, name, `malformed JSON: ${err instanceof Error ? err.message : String(err)}`);
      return;
    }
    const kind = envelopeKind(obj);
    if (kind === undefined) {
      // Not an envelope (no body key, or more than one): report the first ajv error (D-15).
      const checked = validateRequest(obj);
      await this.ignore(full, name, checked.ok ? "no envelope body key" : checked.errors[0]);
      return;
    }
    const body = (obj as Record<string, unknown>)[kind];
    if (typeof body === "string") {
      // D-14: an encrypted body (base64) received by a core agent: ignored + logged, never answered.
      await this.ignore(full, name, "base64 body (encryption is not supported)");
      return;
    }
    if (kind === "request") {
      const checked = validateRequest(obj);
      if (!checked.ok) {
        await this.ignore(full, name, checked.errors[0]); // D-15
        return;
      }
      return this.dispatchRequest(checked.value);
    }
    if (kind === "cancellation") {
      const checked = validateCancellation(obj);
      if (!checked.ok) {
        await this.ignore(full, name, checked.errors[0]); // D-15
        return;
      }
      return this.dispatchCancellation(full, name, checked.value);
    }
    if (kind === "response") {
      const checked = validateResponse(obj);
      if (!checked.ok) {
        await this.ignore(full, name, checked.errors[0]); // D-15
        return;
      }
      return this.dispatchResponse(checked.value);
    }
    const checked = validateConfirmation(obj);
    if (!checked.ok) {
      await this.ignore(full, name, checked.errors[0]); // D-15
      return;
    }
    return this.dispatchConfirmation(full, name, checked.value);
  }

  /** A request file: `cmd` goes to lane D; `req` tracks the exchange and starts A0 asynchronously. */
  private async dispatchRequest(file: RequestFile): Promise<void> {
    if (file.type === "cmd") return this.dispatchCommand(file);
    if (this.archived.has(file.rid) || this.exchanges.has(file.rid)) return; // D-5: duplicate rid
    const ex: ExchangeState = {
      rid: file.rid,
      header: pickHeader(file),
      request: file,
      phase: "accepting",
      canSeen: false,
      can: null,
      res: null,
      con: null,
      responseAttachments: [],
    };
    this.exchanges.set(file.rid, ex); // tracked before A0 starts: a concurrent .can matches it
    this.drainDeferred(ex); // the envelopes delivered before this request was tracked
    this.guarded(file.rid, this.acceptAndRun(ex)); // A0 runs asynchronously: do not block the dispatcher
  }

  /** Lane D entry: a schema-valid `type="cmd"` envelope goes through `D0`, then `D1`–`D3` or `D4`. */
  private async dispatchCommand(file: RequestFile): Promise<void> {
    if (this.stopped) return; // stop() owns the inbox now: the file stays for the next start (D-4)
    const rid = file.rid;
    // D-5: one command is executed once per rid per run. A command archived by a previous run left
    // no file behind, so a leftover envelope means a crash before the archive: it runs again.
    if (this.archived.has(rid) || this.commandRids.has(rid)) return;
    this.commandRids.add(rid);
    const verdict = this.acceptCommand(file); // D0
    if (!verdict.ok) {
      this.queueCommand(file, () => this.archiveCommand(file, "dropped", verdict.reason)); // D4
      return;
    }
    this.queueCommand(file, () => this.runCommand(file, verdict.name)); // D1–D3
  }

  /** Hands the envelope to the lane D queue; the drain that is already running picks it up. */
  private queueCommand(file: RequestFile, run: () => Promise<void>): void {
    this.commandQueue.push({ rid: file.rid, run });
    void this.drainCommands();
  }

  /**
   * Lane D runs one command at a time, sorted by rid (a ULID: the order the requestor created them),
   * whatever order the watcher handed the files over — so the pending envelopes of the initial scan
   * and a burst of arrivals are both processed in lexical-rid order. Nothing here blocks the inbox
   * scan or lane A: a slow custom handler only delays the commands behind it. `stop()` leaves what
   * is still queued untouched in the inbox for the next start (D-4).
   */
  private async drainCommands(): Promise<void> {
    if (this.commandDraining) return; // one drain: a command arriving while it runs joins the queue
    this.commandDraining = true;
    try {
      while (this.commandQueue.length > 0 && !this.stopped) {
        this.commandQueue.sort((a, b) => (a.rid < b.rid ? -1 : a.rid > b.rid ? 1 : 0));
        const next = this.commandQueue.shift()!;
        try {
          await next.run(); // the previous command settles (archive and hook) before the next one starts
        } catch (err) {
          this.reportInternal(next.rid, err); // a broken archive or agent hook never breaks the drain
        }
      }
    } finally {
      this.commandDraining = false;
    }
  }

  /** `D0`: may this `src` command us at all, and do we know the name? Disabled agents still answer. */
  private acceptCommand(file: RequestFile): CommandVerdict {
    const body = file.request as RequestBody; // validated by `commandRules`: the shape of each name is fixed
    // The spec is read fresh: `enable` may have changed `accepts_commands_from` since we started.
    // An empty list means nobody, exactly as in the callability rules (5.1).
    if (!this.ctx.spec().accepts_commands_from.includes(file.src)) {
      return { ok: false, reason: `"${file.src}" is not in accepts_commands_from` };
    }
    if (!STANDARD_COMMANDS.has(body.name) && !this.commands.has(body.name)) {
      return { ok: false, reason: `unknown command "${body.name}"` };
    }
    // `disabled` is NOT checked here: a disabled agent must still be able to answer `enable`,
    // `ping` and `stop` (*D0*: "commands are processed even if the agent is disabled").
    return { ok: true, name: body.name };
  }

  /** `D1`–`D2`: execute an accepted command. Built-ins archive first (`D3`), then hand over. */
  private async runCommand(file: RequestFile, name: string): Promise<void> {
    if (STANDARD_COMMANDS.has(name)) {
      // The archive always completes BEFORE the lifecycle hook runs: "the archive is always completed
      // before shutting down", and the restart `delay` only starts after the archive (D-10).
      await this.archiveCommand(file, "completed");
      return this.runStandardCommand(name, (file.request as RequestBody).parameters);
    }
    const handler = this.commands.get(name);
    if (handler === undefined) return; // unreachable: `D0` only accepts known names
    try {
      await handler((file.request as RequestBody).parameters, { rid: file.rid, src: file.src });
    } catch (err) {
      // An internal failure, not a protocol state: commands are never answered and `D4` is already
      // ruled out (`D0` accepted the envelope). It still archives as `completed`; the operator sees it.
      const why = err instanceof Error ? err.message : String(err);
      this.ctx.emit({ event: "log", message: `receiver: command "${name}" from ${file.src} failed: ${why}` });
    }
    await this.archiveCommand(file, "completed"); // executed: the envelope was accepted and run
  }

  /** `D1` for the four mandatory commands: the parsed parameters go to the agent lifecycle hooks. */
  private async runStandardCommand(name: string, parameters: unknown): Promise<void> {
    // `commandRules` fixed the shape (`stop {force?}`, `enable {enabled}`, `restart {force?, delay?}`,
    // `ping null`), so these reads cover every accepted envelope; anything else never reached here.
    const params = (parameters ?? {}) as Record<string, unknown>;
    switch (name) {
      case "stop":
        await this.hooks.onCommandStop(params.force === true); // the agent performs D-9 and C1/C2
        return;
      case "restart": {
        const delay = params.delay;
        // `delay` is a duration in seconds (*Timers and thresholds*); the agent waits it out after
        // the archive and then starts again in-process (D-10).
        await this.hooks.onCommandRestart(params.force === true, typeof delay === "number" ? delay : 0);
        return;
      }
      case "enable":
        await this.hooks.onCommandEnable(params.enabled === true); // the agent rewrites spec.json (D2)
        return;
      default:
        await this.hooks.onCommandPing(); // "ping": `parameters` is null; the agent republishes (D2)
    }
  }

  /** `D3`/`D4`: the `inreq` event with the command envelope alone, then delete the envelope. */
  private async archiveCommand(
    file: RequestFile,
    finalStatus: "completed" | "dropped",
    reason?: string,
  ): Promise<void> {
    // Commands are never cancelled and never answered: only `req` is ever filled (*Archive*, D3/D4).
    const envelopes: Exchange = { final_status: finalStatus, req: file, can: null, res: null, con: null };
    await this.ctx.appendDiarium(diarium.inreqEvent(this.ctx.id, this.ctx.now(), finalStatus, envelopes));
    await io.remove(path.join(this.ctx.folders.inbox, `${file.rid}.json`));
    this.archived.add(file.rid); // D-5: the same rid is never executed twice in this run
    if (finalStatus === "dropped") {
      // In-process observability only: `D4` touches no file but the envelope and the diarium.
      this.ctx.emit({
        event: "log",
        message: `receiver: dropped command ${file.rid}: ${reason ?? "rejected at D0"}`,
      });
    }
    this.ctx.emit({ event: "inreq", rid: file.rid, finalStatus });
  }

  /** Park one envelope until the request of its rid is tracked (the request file is present
   * in the inbox but its delivery has not happened yet — the watcher delivers in lexical order, so
   * a `.can.json` reaches us before its `.json`). */
  private defer(rid: Rid, kind: "can" | "res" | "con", file: CancellationFile | ResponseFile | ConfirmationFile): void {
    const list = this.deferred.get(rid);
    if (list === undefined) this.deferred.set(rid, [{ kind, file }]);
    else list.push({ kind, file });
  }

  /** Deliver the envelopes deferred for this rid — cancellation first, then response and
   * confirmation in arrival order. Runs synchronously in `dispatchRequest`, before A0 starts. */
  private drainDeferred(ex: ExchangeState): void {
    const list = this.deferred.get(ex.rid);
    if (list === undefined) return;
    this.deferred.delete(ex.rid);
    const cans = list.filter((d) => d.kind === "can");
    const rest = list.filter((d) => d.kind !== "can");
    for (const d of [...cans, ...rest]) {
      const name = `${ex.rid}.${d.kind}.json`;
      const full = path.join(this.ctx.folders.inbox, name);
      if (d.kind === "can") {
        const reason = this.applyCancellation(ex, d.file as CancellationFile);
        if (reason !== null) void this.ignore(full, name, reason);
      } else if (d.kind === "res") {
        this.applyResponse(ex, d.file as ResponseFile);
      } else {
        const reason = this.applyConfirmation(ex, d.file as ConfirmationFile);
        if (reason !== null) void this.ignore(full, name, reason);
      }
    }
  }

  /** A response file: ours (echoed by the watcher) or a leftover of a previous run (D-4). */
  private async dispatchResponse(file: ResponseFile): Promise<void> {
    const ex = this.exchanges.get(file.rid); // D-2: match exchanges by rid, never by file name
    if (ex === undefined) {
      if (await io.exists(path.join(this.ctx.folders.inbox, `${file.rid}.json`))) {
        this.defer(file.rid, "res", file); // the request file is still pending delivery
        return;
      }
      await this.retrackFromResponse(file); // D-4: a bare `.res.json` left by a previous run
      return;
    }
    if (ex.res === null) this.applyResponse(ex, file);
  }

  /** The effect of one matched response, by phase. A response for an exchange that has not
   * responded yet (a deferred D-4 leftover) puts it in `responded` and arms the confirmation wait. */
  private applyResponse(ex: ExchangeState, file: ResponseFile): void {
    ex.res = file;
    if (ex.phase === "responded") return; // our own echo: `respond()` already armed the wait
    ex.phase = "responded";
    if (ex.con !== null) {
      this.startArchive(ex); // the confirmation already arrived (out of order)
      return;
    }
    ex.confirmTimer = setTimeout(() => this.startArchive(ex), this.ctx.config.confirmTimeout);
  }

  /** The effect of one matched cancellation, by phase. Returns the ignore reason when the envelope
   * does not belong to this exchange (D-11 header, name match); the caller reports it (D-15). */
  private applyCancellation(ex: ExchangeState, file: CancellationFile): string | null {
    if (!sameHeader(file, ex.header)) return `header mismatch for rid ${ex.rid} (D-11)`;
    if (ex.can !== null) return null; // the cancellation file is written once; later ones are no-ops
    if (
      ex.request !== null &&
      !nameMatches(ex.request.request as RequestBody, file.cancellation as CancellationBody)
    ) {
      return `cancellation name mismatch for rid ${ex.rid}`;
    }
    ex.can = file;
    ex.canSeen = true;
    if (ex.phase === "working") ex.abort?.abort(); // A3: stop the work
    // "accepting": A0 checks canSeen before starting work (and re-reads the can file);
    // "responded": no second response — the exchange simply ends as `cancelled` at the archive.
    return null;
  }

  /** A cancellation file: matched by rid (D-2) with the header check (D-11) first. */
  private async dispatchCancellation(full: string, name: string, file: CancellationFile): Promise<void> {
    const ex = this.exchanges.get(file.rid);
    if (ex === undefined) {
      if (await io.exists(path.join(this.ctx.folders.inbox, `${file.rid}.json`))) {
        this.defer(file.rid, "can", file); // matched once the request is tracked
        return;
      }
      await this.ignore(full, name, `unmatched rid ${file.rid} (no tracked exchange)`);
      return;
    }
    const reason = this.applyCancellation(ex, file);
    if (reason !== null) await this.ignore(full, name, reason);
  }

  /** The effect of one matched confirmation, by phase. Returns the ignore reason on a D-11
   * header mismatch; the caller reports it (D-15). */
  private applyConfirmation(ex: ExchangeState, file: ConfirmationFile): string | null {
    if (!sameHeader(file, ex.header)) return `header mismatch for rid ${ex.rid} (D-11)`;
    if (ex.con !== null) return null; // the confirmation file is written once; later ones are no-ops
    ex.con = file;
    if (ex.phase === "responded" && ex.res !== null) {
      if (ex.confirmTimer) {
        clearTimeout(ex.confirmTimer);
        ex.confirmTimer = undefined;
      }
      this.startArchive(ex);
    }
    // a confirmation before the response (out of order) is stored: A2 archives immediately
    return null;
  }

  /** A confirmation file: matched by rid (D-2) with the header check (D-11) first. */
  private async dispatchConfirmation(full: string, name: string, file: ConfirmationFile): Promise<void> {
    const ex = this.exchanges.get(file.rid);
    if (ex === undefined) {
      if (await io.exists(path.join(this.ctx.folders.inbox, `${file.rid}.json`))) {
        this.defer(file.rid, "con", file); // matched once the request is tracked
        return;
      }
      await this.ignore(full, name, `unmatched rid ${file.rid} (no tracked exchange)`);
      return;
    }
    const reason = this.applyConfirmation(ex, file);
    if (reason !== null) await this.ignore(full, name, reason);
  }

  /** D-9 (receiver side), fully archiving: `stop()` awaits every archive before it resolves — the
   * stop-archives and the ones already in flight (D-9: the agent's C1 runs after `stop()`).
   * Accepted-and-unanswered requests get the code 300 response first; every exchange that has a
   * response is then archived with the normal A4 rule. Exchanges still in `accepting` stay in the
   * inbox for the next start (D-4). */
  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.watcher?.close();
    this.watcher = null;
    for (const ex of [...this.exchanges.values()]) {
      if (ex.confirmTimer) {
        clearTimeout(ex.confirmTimer);
        ex.confirmTimer = undefined;
      }
    }
    const toArchive: ExchangeState[] = [];
    for (const ex of this.exchanges.values()) {
      if (ex.phase === "working" && ex.res === null) {
        // D-9: an accepted, unanswered request → error response with code 300 (the single response;
        // the `ex.res === null` check keeps the rule even if the work settled in this very moment).
        ex.abort?.abort();
        await this.writeResponse(ex, {
          cancellation: false,
          error: true,
          result: { code: 300, msg: "agent_stopping: the agent stops before answering" },
          attachments: [],
        });
      }
      if (ex.res !== null) toArchive.push(ex); // waiting for confirmation, or answered with 300
    }
    for (const ex of toArchive) this.startArchive(ex);
    // D-9: the map now holds the stop-archives together with every archive that was already in
    // flight (confirmation-triggered, started before stop()); no new one can start (the dispatchers
    // are stopped-guarded and the timers are cleared). Awaiting them all: when stop() resolves,
    // EVERY archive has landed.
    await Promise.all(this.archiving.values());
  }

  /** Number of inbound exchanges in A1/A2/A3 (for busy, D-8) and capability_status. */
  runningCount(): number {
    return this.exchanges.size;
  }

  /** The exchanges whose handler is running (phase `working`): the agent's `stopGrace` wait (D-9
   * `force=false`) waits only for these, not for the confirmations. */
  workingCount(): number {
    let n = 0;
    for (const ex of this.exchanges.values()) if (ex.phase === "working") n += 1;
    return n;
  }

  /** A0: the acceptance checks in table order (first failure found is answered) → A1 → A2. */
  private async acceptAndRun(ex: ExchangeState): Promise<void> {
    const request = ex.request?.request as RequestBody | undefined;
    if (request === undefined) return;
    if (!ex.canSeen) {
      const pending = await this.findPendingCancellation(ex);
      if (pending !== null) {
        ex.can = pending;
        ex.canSeen = true;
      }
    }
    if (ex.canSeen) {
      // A cancellation is already present: no work is started, a cancellation response (A0 row).
      await this.respond(ex, { cancellation: true, error: false, result: null, attachments: [] });
      return;
    }
    const verdict = this.accept(ex.header.src, request);
    if (!verdict.ok) {
      await this.respond(ex, {
        cancellation: false,
        error: true,
        result: { code: verdict.code, msg: verdict.msg },
        attachments: [],
      });
      return;
    }
    await this.run(ex, verdict.entry, request);
  }

  /** A0 checks in table order; the first failure found is reported (104 is not implemented). */
  private accept(
    src: AgentId,
    request: RequestBody,
  ): { ok: true; entry: CapabilityEntry } | { ok: false; code: number; msg: string } {
    if (this.ctx.spec().disabled === true) {
      return { ok: false, code: 100, msg: "agent_disabled: the agent is disabled" };
    }
    const entry = this.capabilities.get(request.name);
    if (entry === undefined) {
      return { ok: false, code: 101, msg: `unknown_capability: no capability named "${request.name}"` };
    }
    if (entry.capability.actio.disabled === true) {
      return { ok: false, code: 102, msg: `capability_disabled: "${request.name}" is disabled` };
    }
    const acceptsFrom = entry.capability.actio.accepts_from;
    if (acceptsFrom !== undefined && acceptsFrom.length > 0 && !acceptsFrom.includes(src)) {
      return { ok: false, code: 103, msg: `requestor_not_allowed: "${request.name}" does not accept "${src}"` };
    }
    // `parameters: null` means "no parameters" (*Issue a request*): it is checked as the empty object.
    if (!entry.compiled.validateParams(request.parameters ?? {})) {
      return { ok: false, code: 105, msg: `invalid_parameters: parameters of "${request.name}" do not conform` };
    }
    const peer = this.hooks.peerSpec(src); // I-11: the discovered spec, not the filtered Agmen view
    if (peer !== undefined && !sharesNamespace(this.ctx.spec(), peer)) {
      return { ok: false, code: 106, msg: `namespace_mismatch: no shared namespace with "${src}"` };
    }
    const maxParallel = entry.capability.actio.max_parallel ?? 0; // 0 = unbounded
    if (maxParallel > 0 && entry.running >= maxParallel) {
      return { ok: false, code: 107, msg: `capacity_exceeded: "${request.name}" is at max_parallel ${maxParallel}` };
    }
    const nowSeconds = Math.floor(this.ctx.now() / 1000);
    if (request.timeout !== 0 && request.ts + request.timeout < nowSeconds) {
      return {
        ok: false,
        code: 108,
        msg: `request_expired: the request timeout of ${request.timeout}s passed before acceptance`,
      };
    }
    return { ok: true, entry };
  }

  /** A1: run the capability handler with an AbortSignal (A3 on cancellation) → A2. */
  private async run(ex: ExchangeState, entry: CapabilityEntry, request: RequestBody): Promise<void> {
    ex.phase = "working";
    ex.abort = new AbortController();
    entry.running += 1;
    this.hooks.onStateChange();
    const start = this.ctx.now();
    let result: unknown;
    let failure: unknown;
    try {
      result = await entry.handler(request.parameters, {
        rid: ex.rid,
        src: ex.header.src,
        signal: ex.abort.signal,
        attachments: request.attachments,
        publishAttachment: (name: string, data: Uint8Array) => this.publishAttachment(ex, name, data),
      });
    } catch (err) {
      failure = err;
    } finally {
      entry.running -= 1;
      this.hooks.onStateChange();
    }
    if (ex.abort?.signal.aborted) {
      // A3: the work was cancelled (the handler may have settled before the abort landed).
      await this.respond(ex, { cancellation: true, error: false, result: null, attachments: [] });
      return;
    }
    if (failure === undefined) {
      await this.respond(ex, {
        cancellation: false,
        error: false,
        result,
        attachments: ex.responseAttachments,
        workTime: Math.floor((this.ctx.now() - start) / 1000),
      });
      return;
    }
    const errorResult =
      failure instanceof ActioError
        ? {
            code: failure.code,
            msg: failure.message,
            ...(failure.details !== undefined ? { details: String(failure.details) } : {}),
          }
        : { msg: failure instanceof Error ? failure.message : String(failure) };
    await this.respond(ex, {
      cancellation: false,
      error: true,
      result: errorResult,
      attachments: ex.responseAttachments,
      workTime: Math.floor((this.ctx.now() - start) / 1000),
    });
  }

  /** A2: write the single response of the exchange and wait for the confirmation (or its timeout). */
  private async respond(ex: ExchangeState, options: ResponseOptions): Promise<void> {
    if (this.stopped || !this.exchanges.has(ex.rid)) return; // stop() owns the answer now
    await this.writeResponse(ex, options);
    if (ex.con !== null) {
      this.startArchive(ex); // the confirmation already arrived (out of order)
      return;
    }
    ex.confirmTimer = setTimeout(() => this.startArchive(ex), this.ctx.config.confirmTimeout);
  }

  /** A2 core: build the response, self-validate it (I-2), write it to the OWN inbox. */
  private async writeResponse(ex: ExchangeState, options: ResponseOptions): Promise<void> {
    const now = this.ctx.now();
    const body: ResponseBody = {
      ts: Math.floor(now / 1000),
      nonce: nonce(),
      signature: null,
      error: options.error,
      cancellation: options.cancellation,
      result: options.result as ResponseBody["result"],
      attachments: options.attachments,
    };
    if (options.workTime !== undefined) body.work_time = options.workTime;
    // `type` is "req" for every tracked exchange (commands are round B): the literal narrows the header.
    const file: ResponseFile = { ...ex.header, type: "req", datetime: toIso(now), response: body };
    const checked = validateResponse(file);
    if (!checked.ok) {
      throw new Error(`receiver.ts: invalid response for ${ex.rid}: ${checked.errors.join("; ")}`);
    }
    await io.writeJsonAtomic(path.join(this.ctx.folders.inbox, `${ex.rid}.res.json`), checked.value, this.ctx.folders.agent);
    ex.res = checked.value;
    // `ex.responseAttachments` is NOT touched here — it is grown ONLY by `publishAttachment`,
    // so A4 deletes every file the handler published, whatever THIS response body carried
    // (a cancellation or a code 300 response carries `attachments: []` by schema rule).
    ex.phase = "responded";
  }

  /** A4: the `inreq` diarium event, then delete all files of the rid and the response attachments.
   * No `stopped` guard: `stop()` itself awaits these archives (D-9); after `stop()` no other entry
   * point can trigger one (every dispatcher is `stopped`-guarded and the timers are cleared). */
  private async archive(ex: ExchangeState): Promise<void> {
    // One archive per rid: `archived` is marked BEFORE the first await, so an envelope landing while
    // this archive is still in flight (a confirmation after the confirmTimeout had already armed A4)
    // cannot append a second `inreq` for the same exchange (D-5).
    if (!this.exchanges.has(ex.rid) || this.archived.has(ex.rid)) return;
    this.archived.add(ex.rid); // D-5: the same rid is never archived twice in this run
    if (ex.confirmTimer) {
      clearTimeout(ex.confirmTimer);
      ex.confirmTimer = undefined;
    }
    const finalStatus: InreqFinalStatus = ex.canSeen ? "cancelled" : ex.con !== null ? "confirmed" : "completed";
    const envelopes: Exchange = { final_status: finalStatus, req: ex.request, can: ex.can, res: ex.res, con: ex.con }; // as exchanged
    await this.ctx.appendDiarium(diarium.inreqEvent(this.ctx.id, this.ctx.now(), finalStatus, envelopes));
    for (const suffix of [".json", ".can.json", ".res.json", ".con.json"]) {
      await io.remove(path.join(this.ctx.folders.inbox, `${ex.rid}${suffix}`));
    }
    for (const name of ex.responseAttachments) {
      await io.remove(path.join(this.ctx.folders.forum, name));
    }
    this.exchanges.delete(ex.rid);
    this.ctx.emit({ event: "inreq", rid: ex.rid, finalStatus });
    this.hooks.onStateChange();
  }

  /** D-4: re-track a pending `.res.json` (previous run) and restart the confirmation wait from now. */
  private async retrackFromResponse(file: ResponseFile): Promise<void> {
    if (this.stopped || this.archived.has(file.rid) || this.exchanges.has(file.rid)) return;
    const header = pickHeader(file);
    const ex: ExchangeState = {
      rid: file.rid,
      header,
      request: null,
      phase: "responded",
      canSeen: false,
      can: null,
      res: file,
      con: null,
      responseAttachments: [],
    };
    const pendingCan = await this.readEnvelope(`${file.rid}.can.json`, validateCancellation, header);
    if (pendingCan !== null) {
      ex.can = pendingCan;
      ex.canSeen = true;
    }
    const pendingCon = await this.readEnvelope(`${file.rid}.con.json`, validateConfirmation, header);
    if (pendingCon !== null) ex.con = pendingCon;
    this.exchanges.set(file.rid, ex);
    if (ex.con !== null) {
      this.startArchive(ex); // confirmed before this run started
      return;
    }
    ex.confirmTimer = setTimeout(() => this.startArchive(ex), this.ctx.config.confirmTimeout); // tracked, so stop() awaits it (D-9)
  }

  /** Starts one archive and tracks its promise (D-9: `stop()` awaits the in-flight ones). A second
   * start for the same rid is a no-op: the D-5 guard makes the second `archive()` return at once,
   * so tracking only the first never loses an in-flight archive. Failures are reported, not thrown. */
  private startArchive(ex: ExchangeState): void {
    if (this.archiving.has(ex.rid)) return;
    const p = this.archive(ex).catch((err: unknown) => this.reportInternal(ex.rid, err));
    this.archiving.set(ex.rid, p);
    void p.finally(() => {
      if (this.archiving.get(ex.rid) === p) this.archiving.delete(ex.rid);
    });
  }

  /** A0: a valid cancellation file already present for the rid (matching header and name). */
  private async findPendingCancellation(ex: ExchangeState): Promise<CancellationFile | null> {
    const file = await this.readEnvelope(`${ex.rid}.can.json`, validateCancellation, ex.header);
    if (file === null) return null;
    if (typeof file.cancellation === "string") return null; // D-14: not answered
    if (ex.request !== null && !nameMatches(ex.request.request as RequestBody, file.cancellation as CancellationBody)) {
      return null;
    }
    return file;
  }

  /** Reads `<inbox>/<name>`, schema-validates it and checks the header against `header` (D-11). */
  private async readEnvelope<T>(name: string, validate: (x: unknown) => Validated<T>, header: Header): Promise<T | null> {
    const obj = await io.readJson(path.join(this.ctx.folders.inbox, name));
    if (obj === undefined) return null;
    const checked = validate(obj);
    if (!checked.ok) return null;
    if (!sameHeader(checked.value as unknown as Header, header)) return null;
    return checked.value;
  }

  /** CapabilityContext.publishAttachment: write to `forum` (atomic, tmpDir = agent folder). */
  private async publishAttachment(ex: ExchangeState, name: string, data: Uint8Array): Promise<string> {
    const target = path.join(this.ctx.folders.forum, name);
    await io.makeDir(path.dirname(target));
    const tmpDir = this.ctx.folders.agent;
    const tmp = path.join(tmpDir, `.${path.basename(target)}.${nonce()}.tmp`);
    const fh = await open(tmp, "w");
    try {
      await fh.writeFile(data);
    } finally {
      await fh.close();
    }
    try {
      await rename(tmp, target);
    } catch (err) {
      await io.remove(tmp).catch(() => {});
      throw err;
    }
    ex.responseAttachments.push(name);
    return name;
  }

  /** D-14/D-15: `ignored <path>: <reason>` once per (path, mtime) + the `ignored` event. */
  private async ignore(full: string, name: string, reason: string): Promise<void> {
    let key: string;
    try {
      key = `${full}:${(await stat(full)).mtimeMs}`;
    } catch {
      return; // vanished: nothing to ignore
    }
    if (this.ignoredSeen.has(key)) return;
    this.ignoredSeen.add(key);
    this.ctx.emit({ event: "ignored", path: full, reason });
    await this.ctx.appendDiarium(diarium.logEvent(this.ctx.id, this.ctx.now(), `ignored ${name}: ${reason}`));
    // An ignored request file is never tracked — the envelopes deferred for its rid are
    // orphans: drop them from `deferred` and log each once (the files stay in the inbox; D-16).
    if (!/\.(can|res|con)\.json$/.test(name)) {
      await this.dropDeferredFor(name.slice(0, -".json".length));
    }
  }

  /** The request of `rid` was ignored (D-15) and will never be tracked — drop the envelopes
   * deferred for it and log each once (the same `ignore()` pattern: one event + one diarium line). */
  private async dropDeferredFor(rid: Rid): Promise<void> {
    const list = this.deferred.get(rid);
    if (list === undefined) return;
    this.deferred.delete(rid);
    for (const d of list) {
      const name = `${rid}.${d.kind}.json`;
      const full = path.join(this.ctx.folders.inbox, name);
      this.ctx.emit({ event: "ignored", path: full, reason: `the request of rid ${rid} was ignored (D-15)` });
      await this.ctx.appendDiarium(
        diarium.logEvent(this.ctx.id, this.ctx.now(), `deferred ${full}: the request of rid ${rid} was ignored (D-15)`),
      );
    }
  }

  /** Runs an internal async step; a rejection is a receiver bug — report it, never crash the loop. */
  private guarded(rid: Rid, step: Promise<void>): void {
    void step.catch((err: unknown) => this.reportInternal(rid, err));
  }

  /** The one report of an internal rejection (a bug or an agent hook failure), as a `log` event. */
  private reportInternal(rid: Rid, err: unknown): void {
    this.ctx.emit({
      event: "log",
      message: `receiver: internal error on ${rid}: ${err instanceof Error ? err.message : String(err)}`,
    });
  }
}

/** The header of a request/response file (D-11 compares everything but `datetime`). */
function pickHeader(file: { rid: Rid; src: AgentId; dst: AgentId; datetime: string; type: "cmd" | "req"; actio_version: string }): Header {
  return { rid: file.rid, src: file.src, dst: file.dst, datetime: file.datetime, type: file.type, actio_version: file.actio_version };
}