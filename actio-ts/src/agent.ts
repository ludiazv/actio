// ActioAgent: lifecycle steps 1–5, lane C wiring, commands, and the public API of §2.7.
import path from "node:path";
import { defaultConfig, loadConfig } from "./config.js";
import * as diarium from "./diarium.js";
import { callable, scan } from "./discovery.js";
import * as io from "./io.js";
import { Receiver } from "./receiver.js";
import { Requestor } from "./requestor.js";
import { buildSpec } from "./spec-builder.js";
import { announce, buildStatus, publishSpec, publishStatus, startHeartbeat } from "./status.js";
import { createSyncProvider } from "./sync.js";
import { ActioCallError, ActioResultError } from "./types.js";
import { sleep } from "./util.js";
import { compileCapability } from "./validate.js";
import { watchDir } from "./watch.js";
import type { ReceiverHooks } from "./receiver.js";
import type { RequestorHooks } from "./requestor.js";
import type { Capability, ResponseBody, Spec, Status } from "./generated/types.js";
import type { CompiledCapability } from "./validate.js";
import type {
  ActioConfig,
  ActioEvent,
  AgentContext,
  AgentFolders,
  AgentId,
  AgentOptions,
  AgentView,
  CapabilityEntry,
  CapabilityHandler,
  CommandContext,
  CommandHandler,
  RequestOutcome,
  RequestOptions,
  StatusState,
  SyncProvider,
} from "./types.js";

/** The mandatory commands (*Commands*); `onCommand` cannot shadow them, and they are never advertised. */
const STANDARD_COMMANDS = new Set(["stop", "enable", "restart", "ping"]);

/** Poll step of the `stopGrace` wait (D-9): counted, because `config.clock` may be a frozen fake clock. */
const GRACE_STEP_MS = 10;

/** The message of an unknown error value. */
function why(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** D-22's wording: the error's `code` when it has one, its message as the fallback. */
function errCode(err: unknown): string {
  return (err as NodeJS.ErrnoException).code ?? why(err);
}

/** A team member: publishes spec/status, serves requests and commands, issues requests. */
export class ActioAgent {
  private readonly capabilities = new Map<string, CapabilityEntry>(); // registration order
  private readonly commands = new Map<string, CommandHandler>();
  private readonly listeners: ((e: ActioEvent) => void)[] = [];
  /** D-19 (peer side): compiled peer capabilities, cached per (id, spec datetime, capability). */
  private readonly peerSchemas = new Map<string, CompiledCapability | null>();
  /** §2.6, D-21: nothing of the environment is read at construction; `start()` loads the config.
   * `root: ""` is a placeholder: no path of the shared tree may be used before `start()`. */
  private config: ActioConfig = { ...defaultConfig, root: "", clock: Date.now };
  private folders: AgentFolders = { root: "", agent: "", inbox: "", forum: "", nuntium: "" };
  private sync: SyncProvider = createSyncProvider("none");
  private receiver: Receiver | undefined;
  private requestor: Requestor | undefined;
  private nuntiumWatcher: { close(): void } | undefined;
  private heartbeat: { stop(): void } | undefined;
  private scanTimer: NodeJS.Timeout | undefined;
  private currentSpec!: Spec; // the spec published by the current start (set by `buildSpecNow`)
  private current: StatusState = "inactive";
  private view = new Map<AgentId, AgentView>();
  /** I-11: the specs of the last scan, unfiltered by the callability rules and keyed by id: D-7's
   * code 106 must see a requestor that rule 2 threw out of `view`, so it cannot come from `view`. */
  private discovered = new Map<AgentId, Spec>();
  /** D-10: the enabled state of the `enable` command (undefined = as the options say). Every spec built
   * during a run carries it; only a fresh start clears it (D-10). */
  private enabledState: boolean | undefined;
  private running = false; // between `start()` and `stop()`
  private stopping = false;
  /** The namespaces we own: the caller's array is cloned at construction, so a later
   * mutation of it cannot change what a rebuild (ping, enable) publishes. */
  private readonly namespaces: (string | null)[] | null | undefined;

  constructor(private readonly options: AgentOptions) {
    this.namespaces = options.namespaces === undefined ? undefined : structuredClone(options.namespaces);
  }

  /** Registers a capability and its handler; throws on a duplicate name or a non-strict schema (D-19).
   * The stored capability is a deep copy, so a caller mutating its object after registration
   * cannot change a running agent's A0 decisions or its next published spec. */
  capability(capability: Capability, handler: CapabilityHandler): void {
    if (this.capabilities.has(capability.name)) {
      throw new Error(`agent: capability "${capability.name}" is already registered`);
    }
    const copy = structuredClone(capability); // own the registration, not the caller's object
    let compiled: CompiledCapability;
    try {
      compiled = compileCapability(copy); // own side of D-19: a schema strict mode rejects is a bug
    } catch (err) {
      throw new Error(`agent: capability "${capability.name}" has a schema strict mode rejects: ${why(err)}`);
    }
    this.capabilities.set(capability.name, { capability: copy, compiled, handler, running: 0 });
  }

  /** Registers a custom command handler; stop/enable/restart/ping are built in. */
  onCommand(name: string, handler: (params: unknown, ctx: CommandContext) => Promise<void>): void {
    if (STANDARD_COMMANDS.has(name)) throw new Error(`agent: "${name}" is a built-in command`);
    if (this.commands.has(name)) throw new Error(`agent: command "${name}" is already registered`);
    this.commands.set(name, handler);
  }

  /** Subscribes to the observability event stream (every protocol event, incl. ignored files). */
  on(event: "event", handler: (e: ActioEvent) => void): void {
    void event; // the only event name in §2.7
    this.listeners.push(handler);
  }

  /** Lifecycle steps 1–4, then the event loop (5); validates config (sync provider, D-21). */
  async start(): Promise<void> {
    if (this.running) throw new Error(`agent "${this.options.id}": start() while already started`);
    this.running = true;
    this.stopping = false;
    try {
      await this.startUp();
    } catch (err) {
      this.abortStart(); // a configuration or registration error must not leave a half-started agent
      throw err;
    }
  }

  /** Steps 1–5 (folders, spec, status, diarium, inbox, announce), the watchers, the first discovery. */
  private async startUp(): Promise<void> {
    const config = loadConfig(this.options.config); // a bad `ACTIO_SYNC` throws here (D-21)
    this.config = config;
    this.folders = {
      root: config.root,
      agent: path.join(config.root, this.options.id), // D-1: the folder name is the id
      inbox: path.join(config.root, this.options.id, "inbox"),
      forum: path.join(config.root, "forum"),
      nuntium: path.join(config.root, "nuntium"),
    };
    // I-10: one provider per process by documented choice; the last started agent installs it.
    this.sync = createSyncProvider(config.sync, (err) => this.emit({ event: "log", message: `sync: ${err.message}` }));
    io.setSyncProvider(this.sync);

    // Steps 1–5: folders, spec, status, diarium, inbox, announce (in that protocol order).
    await io.makeDir(config.root);
    await io.makeDir(this.folders.nuntium);
    await io.makeDir(this.folders.forum);
    await io.makeDir(this.folders.agent);
    await io.makeDir(this.folders.inbox);
    for (const entry of this.capabilities.values()) entry.running = 0; // fresh counters after a restart
    this.enabledState = undefined; // D-10: a `disabled` set by `enable` does not survive a restart
    this.buildSpecNow(); // a bad registration fails here, before anything lands in the tree
    await publishSpec(this.folders.agent, this.currentSpec);
    await this.setStatus("idle"); // status.json (step 3) plus its diarium event (step 4)
    await this.reannounceAfterDelay(`agent ${this.options.id} is active`); // step 6, announceDelay after the status

    this.receiver = new Receiver(this.context(), this.receiverHooks(), this.capabilities, this.commands);
    this.requestor = new Requestor(this.context(), this.requestorHooks());
    // Nuntium watch (D-17): every event schedules one debounced full scan; the first discovery of the
    // workflow (row 4) runs now, not after the debounce.
    this.nuntiumWatcher = watchDir(
      this.folders.nuntium,
      () => this.scheduleScan(),
      (err) => {
        void this.failWatch(this.folders.nuntium, err);
      },
    );
    await this.rescan();
    this.heartbeat = startHeartbeat(config.heartbeatInterval, () => this.beat());
    // LAST step, on purpose : the inbox watcher, whose initial scan delivers the pending
    // envelopes (D-4); the fan-out feeds the requestor, which uses it for the responses of self-calls
    // (B2 on our own inbox). A pending `stop`/`restart` command now runs while everything is armed, so
    // its `stop()` (or `restart()`) can tear it all down: nothing below arms anything afterwards.
    await this.receiver.start((name) => this.requestor?.notifyFile(this.folders.inbox, name));
  }

  /** Drops what a failed `start()` armed: no event loop, no watchers, no heartbeat; not running. The
   * steps that can throw (config, spec build) come before any watcher is armed, so nothing leaks. */
  private abortStart(): void {
    this.heartbeat?.stop();
    this.heartbeat = undefined;
    this.nuntiumWatcher?.close();
    this.nuntiumWatcher = undefined;
    if (this.scanTimer !== undefined) clearTimeout(this.scanTimer);
    this.scanTimer = undefined;
    this.receiver = undefined;
    this.requestor = undefined;
    this.view = new Map();
    this.discovered = new Map();
    this.running = false;
  }

  /** The current Agmen view (Map<AgentId, AgentView> after the callability rules). */
  agmen(): Map<AgentId, AgentView> {
    return this.view;
  }

  /** A full discovery scan now (the same `rescan()` a nuntium event schedules); returns the new view.
   * Not running: the current view, no scan. */
  async refresh(): Promise<Map<AgentId, AgentView>> {
    await this.rescan();
    return this.view;
  }

  /** Issues a request (lane B); rejects locally with ActioCallError if not callable. */
  async request(dst: AgentId, name: string, params: unknown, options?: RequestOptions): Promise<RequestOutcome> {
    const view = this.view.get(dst);
    if (view === undefined) throw new ActioCallError(dst, name, "peer is not callable (discovery/callability rules)");
    if (!view.capabilities.has(name)) {
      throw new ActioCallError(dst, name, "capability is not usable (disabled or accepts_from)");
    }
    const requestor = this.requestor;
    if (requestor === undefined) throw new ActioCallError(dst, name, "the agent is not started");
    const outcome = await requestor.request(dst, name, params, options ?? {});
    this.checkResult(dst, name, view, outcome.response);
    return outcome;
  }

  /** Issues a command (lane B, type "cmd"); returns the rid; `outreq` "sent". */
  async command(dst: AgentId, name: string, params?: unknown): Promise<string> {
    const view = this.view.get(dst);
    if (view === undefined) throw new ActioCallError(dst, name, "peer is not callable (discovery/callability rules)");
    if (!view.commands) throw new ActioCallError(dst, name, "peer does not accept our commands");
    const requestor = this.requestor;
    if (requestor === undefined) throw new ActioCallError(dst, name, "the agent is not started");
    return requestor.command(dst, name, params ?? null);
  }

  /** Status "error" + announce (no recovery; spec note after the workflow). */
  async fail(message: string): Promise<void> {
    if (this.current === "error") return; // already failed: no second announce, no recovery
    await this.setStatus("error");
    await this.reannounceAfterDelay(`agent ${this.options.id} failed: ${message}`);
  }

  /** Lane C, D-9 in that order: heartbeat + scan, grace, receiver, abandon B2, C1 → C2. */
  async stop(options?: { force?: boolean }): Promise<void> {
    if (!this.running || this.stopping) return; // idempotent: a second stop() resolves
    this.stopping = true;
    // (1) the heartbeat and any pending discovery scan.
    this.heartbeat?.stop();
    this.heartbeat = undefined;
    if (this.scanTimer !== undefined) clearTimeout(this.scanTimer);
    this.scanTimer = undefined;
    // (2) the grace wait (D-9: only in-flight WORK). The requestor is NOT stopped yet: a handler may
    // still issue a request and receive its response inside `stopGrace`. `force` skips the wait,
    // it does not reorder the steps.
    if (options?.force !== true) {
      const steps = Math.ceil(this.config.stopGrace / GRACE_STEP_MS);
      for (let i = 0; i < steps && (this.receiver?.workingCount() ?? 0) > 0; i++) await sleep(GRACE_STEP_MS);
    }
    // (3) abort what remains: every accepted, unanswered request gets code 300, and the exchanges that
    // already have a response are archived at once.
    await this.receiver?.stop();
    // (4) last, the outbound side: what is still in B2 is abandoned (D-9).
    await this.requestor?.stop();
    // (5) C1: status first …
    await this.setStatus("inactive");
    await this.reannounceAfterDelay(`agent ${this.options.id} is inactive`); // … then, delayed, the announce
    await this.sync.flush(); // before C1 completes (§2.6): the last writes must reach the shared tree
    // C2: deinit. The watchers are gone, the in-memory view and the exchange bookkeeping are dropped.
    this.nuntiumWatcher?.close();
    this.nuntiumWatcher = undefined;
    this.receiver = undefined;
    this.requestor = undefined;
    this.view = new Map();
    this.discovered = new Map();
    await this.sync.close(); // only cancels a pending debounce (I-10)
    this.running = false;
    this.stopping = false;
  }

  // --- wiring of the lower modules  --------------------------------------

  /** The AgentContext shared by the receiver and the requestor (no DI container). */
  private context(): AgentContext {
    return {
      config: this.config,
      id: this.options.id,
      folders: this.folders,
      sync: this.sync,
      now: () => this.config.clock(),
      spec: () => this.currentSpec,
      state: () => this.current,
      emit: (e: ActioEvent) => this.emit(e),
      appendDiarium: (e) => diarium.append(this.folders.agent, e),
    };
  }

  private receiverHooks(): ReceiverHooks {
    return {
      onWatchError: (dir, err) => void this.failWatch(dir, err), // D-22: own inbox
      peerSpec: (src) => this.discovered.get(src), // code 106 needs the DISCOVERED spec, not the view (D-7)
      onStateChange: () => void this.syncBusy(),
      onCommandStop: (force) => this.stop({ force }),
      onCommandRestart: (force, delaySeconds) => this.restart(force, delaySeconds),
      onCommandEnable: (enabled) => this.applyEnable(enabled),
      onCommandPing: () => this.applyPing(),
    };
  }

  private requestorHooks(): RequestorHooks {
    return {
      // D-22 names only the own inbox and `nuntium` for `fail()`: a destination watch error has
      // already been answered by the requestor (its exchanges there are abandoned), so the agent stays up.
      onWatchError: (dir, err) => this.emit({ event: "log", message: `watch lost ${dir}: ${errCode(err)}` }),
      onStateChange: () => void this.syncBusy(), // outbound B2 also makes us busy (D-8)
    };
  }

  /** D-22: a watch error on the own inbox or `nuntium` is fatal: log it and enter `error`. */
  private async failWatch(dir: string, err: Error): Promise<void> {
    this.emit({ event: "log", message: `watch lost ${dir}: ${errCode(err)}` }); // D-22's wording
    await this.fail(`watch lost ${dir}`);
  }

  /** The observability fan-out; a listener's error is the listener's problem (§2.7). */
  private emit(event: ActioEvent): void {
    for (const listener of [...this.listeners]) listener(event);
  }

  /** One debounced full scan per burst of nuntium events (D-17). */
  private scheduleScan(): void {
    if (!this.running || this.stopping || this.scanTimer !== undefined) return;
    this.scanTimer = setTimeout(() => {
      this.scanTimer = undefined;
      void this.rescan();
    }, this.config.nuntiumDebounce);
    // A pending rescan is only a refresh: it must never hold the process open (and never is armed
    // again once `stop()` began, see the guard above).
    this.scanTimer.unref();
  }

  /** Discovery (row 4): `scan` directly, so `duplicates` and `ignored` become events (D-3). */
  private async rescan(): Promise<void> {
    if (!this.running) return;
    let found;
    try {
      found = await scan(this.config.root);
    } catch (err) {
      this.emit({ event: "log", message: `discovery scan failed: ${why(err)}` });
      return;
    }
    const next = new Map<AgentId, AgentView>();
    const specs = new Map<AgentId, Spec>();
    const now = this.now();
    for (const peer of found.agents) {
      specs.set(peer.spec.id, peer.spec); // unfiltered: the 106 check of A0 (D-7)
      const view = callable(this.currentSpec, peer, now); // our own folder is included: self-calls work
      if (view !== undefined) next.set(view.id, view);
    }
    this.view = next;
    this.discovered = specs;
    for (const folder of found.duplicates) {
      this.emit({ event: "log", message: `duplicate agent id: folder "${folder}" was skipped (D-3)` });
    }
    for (const entry of found.ignored) this.emit({ event: "ignored", path: entry.path, reason: entry.reason });
  }

  /** Heartbeat tick (rule 3): the same state, a fresh `datetime`, live `capability_status`. Never rejects. */
  private async beat(): Promise<void> {
    try {
      await publishStatus(this.folders.agent, this.statusFor(this.current));
    } catch (err) {
      this.emit({ event: "log", message: `heartbeat: ${why(err)}` });
    }
  }

  /** D-8: busy iff an inbound exchange (A1/A2/A3) or an outbound one (B2) is in flight. */
  private async syncBusy(): Promise<void> {
    // The fields being gone means C2 ran; `stopping` means lane C began. Either way the status belongs
    // to lane C now: a work settle landing after `stop()` must not resurrect the agent (D-9), and with
    // the fields gone the counts are 0, which would publish `idle` over the `inactive` of C1.
    if (!this.receiver || !this.requestor || !this.running || this.stopping) return;
    if (this.current !== "idle" && this.current !== "busy") return; // inactive/error: no automatic transition
    const busy = this.receiver.runningCount() > 0 || this.requestor.outboundCount() > 0;
    await this.setStatus(busy ? "busy" : "idle"); // only on change; the heartbeat keeps freshness otherwise
  }

  /** Publishes `status.json` and appends its diarium event; a no-op when the state did not change. */
  private async setStatus(state: StatusState): Promise<void> {
    if (state === this.current) return;
    this.current = state; // set before the first await: concurrent hooks must not publish it twice
    try {
      await publishStatus(this.folders.agent, this.statusFor(state));
      await diarium.append(this.folders.agent, diarium.statusEvent(this.options.id, this.now(), state));
    } catch (err) {
      this.emit({ event: "log", message: `status "${state}": ${why(err)}` }); // the state stays as chosen
      return;
    }
    this.emit({ event: "status", state });
  }

  /** spec.json from the options and the registered capabilities, with a fresh `datetime` (I-2 self-validated). */
  private buildSpecNow(): void {
    this.currentSpec = buildSpec(
      { ...this.options, namespaces: this.namespaces }, // our copy, not the caller's array
      this.capabilityList(),
      this.now(),
    );
    // D-10: the enabled state is agent state, not option state, so EVERY rebuild during a run (the `ping`
    // command above all) keeps it. `buildSpec` never writes `disabled`; this is the only writer.
    if (this.enabledState !== undefined) this.currentSpec.disabled = this.enabledState;
  }

  private statusFor(state: StatusState): Status {
    return buildStatus(
      this.options.id,
      state,
      this.now(),
      [...this.capabilities.values()].map((e) => ({ name: e.capability.name, running: e.running, queue: 0 })),
    );
  }

  /** The announce file after the status write, `announceDelay` later (rule 2), and its event. */
  private async reannounceAfterDelay(text: string): Promise<void> {
    await sleep(this.config.announceDelay);
    try {
      await announce(this.folders.agent, this.folders.nuntium, this.options.id, text, this.now());
    } catch (err) {
      this.emit({ event: "log", message: `announce: ${why(err)}` });
      return;
    }
    this.emit({ event: "announce", text });
  }

  /** D-12: a non-error response whose `result` breaks the peer's `result` schema ends as an error here. */
  private checkResult(dst: AgentId, name: string, view: AgentView, response: ResponseBody | undefined): void {
    if (response === undefined || response.error !== false) return; // an error result is the peer's answer
    if (response.cancellation === true) return; // a cancellation response carries `result: null` by protocol
    const compiled = this.peerSchema(dst, name, view);
    if (compiled === null) return; // D-19: the checks are skipped when the schema does not compile
    if (!compiled.validateResult(response.result ?? null)) {
      throw new ActioResultError(`agent: ${dst}/${name} returned a result that does not conform to its schema`, response);
    }
  }

  /** D-19 (peer side): lazy, cached per (id, spec datetime, capability); null = uncompilable schema. */
  private peerSchema(dst: AgentId, name: string, view: AgentView): CompiledCapability | null {
    const capability = view.capabilities.get(name);
    if (capability === undefined) return null;
    const key = `${dst}:${view.spec.datetime}:${name}`;
    const cached = this.peerSchemas.get(key);
    if (cached !== undefined) return cached;
    let compiled: CompiledCapability | null;
    try {
      compiled = compileCapability(capability);
    } catch (err) {
      compiled = null; // logged once (the cache keeps the null), and the checks are skipped
      this.emit({ event: "log", message: `peer capability ${dst}/${name}: ${why(err)}; checks skipped` });
    }
    this.peerSchemas.set(key, compiled);
    return compiled;
  }

  // --- the built-in commands' lifecycle side (D1/D2) ---------------------------------------------

  /** `restart` (D-10): the receiver has archived the command already; stop, wait `delay`, start fresh. */
  private async restart(force: boolean, delaySeconds: number): Promise<void> {
    await this.stop({ force });
    await sleep(Math.max(0, delaySeconds) * 1000);
    await this.start(); // new receiver, requestor, view and state; the registrations survive
  }

  /** `enable` (D2): `disabled` belongs to the built spec object, never to the options or the capabilities. */
  private async applyEnable(enabled: boolean): Promise<void> {
    this.enabledState = !enabled; // D-10: state of the run, not of the options: every later rebuild
    // re-applies it (a `ping` used to clear `disabled`), and only a fresh start clears it (D-10).
    this.buildSpecNow();
    await publishSpec(this.folders.agent, this.currentSpec);
    await this.reannounceAfterDelay(`agent ${this.options.id} is ${enabled ? "enabled" : "disabled"}`);
  }

  /** `ping` (D2): fresh `datetime` in spec and status, then re-announce. */
  private async applyPing(): Promise<void> {
    this.buildSpecNow();
    await publishSpec(this.folders.agent, this.currentSpec);
    await publishStatus(this.folders.agent, this.statusFor(this.current));
    await this.reannounceAfterDelay(`agent ${this.options.id} is alive`);
  }

  private capabilityList(): Capability[] {
    return [...this.capabilities.values()].map((e) => e.capability);
  }

  private now(): number {
    return this.config.clock();
  }
}
