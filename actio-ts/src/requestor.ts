// Requestor: lane B — write request/command, watch the destination inbox (D-18),
// cancellation (B4), confirmation (B3 with the D-13 guard), attachment cleanup (B5), abandon (D-6).
import path from "node:path";
import { stat } from "node:fs/promises";
import { logEvent, outreqEvent } from "./diarium.js";
import { copyFile, exists, readJson, remove, writeJsonAtomic } from "./io.js";
import { nonce, toIso, ulid } from "./util.js";
import { watchDir } from "./watch.js";
import { sameHeader, validateCancellation, validateConfirmation, validateRequest, validateResponse } from "./validate.js";
import type {
  CancellationFile,
  ConfirmationFile,
  Exchange,
  RequestBody,
  RequestFile,
  ResponseBody,
  ResponseFile,
} from "./generated/types.js";
import type { AgentContext, AgentId, OutreqFinalStatus, RequestOptions, RequestOutcome } from "./types.js";

/** Requestor callbacks into the ActioAgent. */
export interface RequestorHooks {
  /** Watch error on a destination inbox (D-22): every outbound exchange there is abandoned. */
  onWatchError(dir: string, err: Error): void;
  /** A busy/idle-relevant exchange state changed (D-8). */
  onStateChange(): void;
}

/** Internal state of one outbound exchange in B2. */
interface Outbound {
  rid: string;
  dst: AgentId;
  name: string;
  req: RequestFile;
  can: CancellationFile | null;
  res: ResponseFile | null;
  responseBody: ResponseBody | undefined;
  attachments: string[];
  cancelled: boolean;
  done: boolean;
  resolve: (outcome: RequestOutcome) => void;
  timeoutTimer?: ReturnType<typeof setTimeout>;
  cancelWaitTimer?: ReturnType<typeof setTimeout>;
  signal: AbortSignal | undefined;
  abortHandler: (() => void) | undefined;
}

/** One watcher per destination inbox (D-18), reference-counted by outstanding rids. */
interface DstWatcher {
  dir: string;
  refs: number;
  handle: { close(): void };
}

/** The requestor side of one agent: issues requests and commands to other agents. */
export class Requestor {
  private readonly exchanges = new Map<string, Outbound>();
  private readonly watchers = new Map<AgentId, DstWatcher>();
  /** D-11 ignore dedup: one log per (path, mtime). */
  private readonly ignoredSeen = new Set<string>();

  constructor(private readonly ctx: AgentContext, private readonly hooks: RequestorHooks) {}

  /** Feeds one file event of the OWN inbox (self-calls share the receiver's watcher). */
  notifyFile(folder: string, name: string): void {
    if (folder !== this.ctx.folders.inbox) return;
    if (!name.endsWith(".res.json")) return;
    this.onResponseFile(name.slice(0, -".res.json".length));
  }

  /** Lane B for `type="req"`: write, wait (B2), cancel on timeout/signal (B4), confirm (B3). */
  async request(dst: AgentId, name: string, params: unknown, options: RequestOptions): Promise<RequestOutcome> {
    const { rid, req } = await this.beginOutbound(dst, name, params, "req", options.timeout ?? 0, options.attachments ?? []);
    const ex: Outbound = {
      rid,
      dst,
      name,
      req,
      can: null,
      res: null,
      responseBody: undefined,
      attachments: (req.request as RequestBody).attachments,
      cancelled: false,
      done: false,
      resolve: () => undefined,
      signal: options.signal,
      abortHandler: undefined,
    };
    const promise = new Promise<RequestOutcome>((resolve) => {
      ex.resolve = resolve;
    });
    this.exchanges.set(rid, ex);
    if (this.exchanges.size === 1) this.hooks.onStateChange();
    this.acquireWatcher(dst);
    if ((options.timeout ?? 0) > 0) {
      ex.timeoutTimer = setTimeout(() => this.giveUp(rid), (options.timeout ?? 0) * 1000);
    }
    if (options.signal) {
      if (options.signal.aborted) {
        this.giveUp(rid);
      } else {
        const handler = () => this.giveUp(rid);
        ex.abortHandler = handler;
        options.signal.addEventListener("abort", handler, { once: true });
      }
    }
    return promise;
  }

  /** Lane B for `type="cmd"`: write the command; `outreq` "sent" right after (B1). */
  async command(dst: AgentId, name: string, params: unknown): Promise<string> {
    const { rid, req } = await this.beginOutbound(dst, name, params, "cmd", 0, []);
    // The diarium is the durable record — append it first, then the live signal (same order at all four outreq points).
    await this.ctx.appendDiarium(outreqEvent(this.ctx.id, this.ctx.now(), "sent", this.exchangeData("sent", req, null, null, null)));
    this.ctx.emit({ event: "outreq", rid, finalStatus: "sent" });
    return rid;
  }

  /** Explicit cancellation of an ongoing outbound request (writes `<rid>.can.json`, B4). */
  async cancel(rid: string): Promise<void> {
    const ex = this.exchanges.get(rid);
    if (!ex || ex.done || ex.cancelled || ex.res !== null) return;
    await this.writeCancellation(ex);
  }

  /** Abandons every outbound exchange in B2 (agent stop, D-9). */
  async stop(): Promise<void> {
    for (const w of this.watchers.values()) w.handle.close();
    this.watchers.clear();
    await Promise.all([...this.exchanges.values()].map((ex) => this.abandon(ex)));
  }

  /** Number of outbound exchanges waiting for a response in B2 (for busy, D-8). */
  outboundCount(): number {
    return this.exchanges.size;
  }

  /** B1: build the envelope, self-validate it (I-2), copy the attachments into `forum` first,
   * then write `<dst inbox>/<rid>.json`. A missing dst folder rejects (fail safe, no crash). */
  private async beginOutbound(
    dst: AgentId,
    name: string,
    params: unknown,
    type: "req" | "cmd",
    timeoutSec: number,
    attachmentPaths: string[],
  ): Promise<{ rid: string; req: RequestFile }> {
    const now = this.ctx.now();
    const rid = ulid(this.ctx.config.clock);
    const attachments: string[] = [];
    for (const p of attachmentPaths) {
      const base = path.basename(p);
      await copyFile(p, path.join(this.ctx.folders.forum, base), this.ctx.folders.agent);
      attachments.push(base);
    }
    const req: RequestFile = {
      rid,
      src: this.ctx.id,
      dst,
      datetime: toIso(now),
      type,
      actio_version: "0.1",
      request: {
        ts: Math.floor(now / 1000),
        nonce: nonce(),
        signature: null,
        name,
        // the schema allows object|null here; the generated type is object-only (cast at the boundary)
        parameters: (params ?? null) as { [k: string]: unknown },
        timeout: timeoutSec,
        attachments,
      },
    };
    const checked = validateRequest(req);
    if (!checked.ok) {
      throw new Error(`requestor: built an invalid request for ${dst}/${name}: ${checked.errors.join("; ")}`);
    }
    await writeJsonAtomic(path.join(this.dstInbox(dst), `${rid}.json`), req, this.ctx.folders.agent);
    return { rid, req };
  }

  /** B2: a `<rid>.res.json` event (dst inbox watcher, or own inbox via `notifyFile`). */
  private onResponseFile(rid: string): void {
    const ex = this.exchanges.get(rid);
    if (!ex || ex.done || ex.res !== null) return;
    void (async () => {
      const full = path.join(this.dstInbox(ex.dst), `${rid}.res.json`);
      const file = await readJson(full);
      if (typeof file !== "object" || file === null) return;
      const checked = validateResponse(file as ResponseFile);
      if (!checked.ok) return; // a broken peer file: stay in B2 until the give-up paths
      if (!sameHeader(checked.value, ex.req)) {
        // D-11: the header differs from the tracked request (except datetime): ignore + log once per (path, mtime)
        await this.ignore(full, "header mismatch (D-11)");
        return; // the exchange stays in B2 — its timers keep running
      }
      if (ex.done || ex.res !== null) return;
      ex.res = checked.value;
      // an encrypted (base64) body cannot be presented as a ResponseBody; the envelope file stays in the inbox
      if (typeof checked.value.response !== "string") ex.responseBody = checked.value.response;
      this.clearTimers(ex);
      // capture does NOT release the dst watcher (D-18): it serves the other outstanding rids to the same dst.
      // The release happens exactly once, in finish().
      void this.complete(ex, ex.cancelled ? "cancelled" : "confirmed");
    })();
  }

  /** B3 (+D-13) and B5: diarium first (with the confirmation about to be written), then the
   * confirmation file (skipped if the receiver already archived), then attachment cleanup. */
  private async complete(ex: Outbound, finalStatus: "confirmed" | "cancelled"): Promise<void> {
    try {
      const inbox = this.dstInbox(ex.dst);
      const hasRequestFile = await exists(path.join(inbox, `${ex.rid}.json`)); // D-13 guard
      if (hasRequestFile) {
        const now = this.ctx.now();
        const con: ConfirmationFile = {
          rid: ex.rid,
          src: ex.req.src,
          dst: ex.req.dst,
          datetime: toIso(now),
          type: "req",
          actio_version: ex.req.actio_version,
          confirmation: { ts: Math.floor(now / 1000), nonce: nonce(), signature: null },
        };
        const checked = validateConfirmation(con);
        if (!checked.ok) {
          throw new Error(`requestor: built an invalid confirmation for ${ex.rid}: ${checked.errors.join("; ")}`);
        }
        await this.ctx.appendDiarium(
          outreqEvent(this.ctx.id, this.ctx.now(), finalStatus, this.exchangeData(finalStatus, ex.req, ex.can, ex.res, con)),
        );
        this.ctx.emit({ event: "outreq", rid: ex.rid, finalStatus });
        await writeJsonAtomic(path.join(inbox, `${ex.rid}.con.json`), con, this.ctx.folders.agent);
      } else {
        // the receiver archived: record the final status without the confirmation envelope
        await this.ctx.appendDiarium(
          outreqEvent(this.ctx.id, this.ctx.now(), finalStatus, this.exchangeData(finalStatus, ex.req, ex.can, ex.res, null)),
        );
        this.ctx.emit({ event: "outreq", rid: ex.rid, finalStatus });
      }
      await this.deleteAttachments(ex); // B5
    } catch (err) {
      this.ctx.emit({ event: "log", message: `requestor: completion of ${ex.rid} failed: ${err instanceof Error ? err.message : String(err)}` });
    } finally {
      this.finish(ex, finalStatus);
    }
  }

  /** D-6/D-22: give up — `outreq` "abandoned", delete the request attachments, resolve. */
  private async abandon(ex: Outbound): Promise<void> {
    if (ex.done || ex.res !== null) return;
    try {
      await this.ctx.appendDiarium(
        outreqEvent(this.ctx.id, this.ctx.now(), "abandoned", this.exchangeData("abandoned", ex.req, ex.can, null, null)),
      );
      this.ctx.emit({ event: "outreq", rid: ex.rid, finalStatus: "abandoned" });
      await this.deleteAttachments(ex);
    } catch (err) {
      this.ctx.emit({ event: "log", message: `requestor: abandon of ${ex.rid} failed: ${err instanceof Error ? err.message : String(err)}` });
    } finally {
      this.finish(ex, "abandoned");
    }
  }

  /** D-11: `ignored <path>: <reason>` once per (path, mtime) — the receiver's `ignore()` pattern. */
  private async ignore(full: string, reason: string): Promise<void> {
    let key: string;
    try {
      key = `${full}:${(await stat(full)).mtimeMs}`;
    } catch {
      return; // vanished: nothing to ignore
    }
    if (this.ignoredSeen.has(key)) return;
    this.ignoredSeen.add(key);
    const message = `ignored ${full}: ${reason}`;
    this.ctx.emit({ event: "log", message });
    await this.ctx.appendDiarium(logEvent(this.ctx.id, this.ctx.now(), message));
  }

  /** B4: write `<rid>.can.json` (fresh datetime), then wait for the cancellation response up to
   * `cancelWaitTimeout` before abandoning (D-6). */
  private async writeCancellation(ex: Outbound): Promise<void> {
    const now = this.ctx.now();
    const can: CancellationFile = {
      rid: ex.rid,
      src: ex.req.src,
      dst: ex.req.dst,
      datetime: toIso(now),
      type: "req",
      actio_version: ex.req.actio_version,
      cancellation: { ts: Math.floor(now / 1000), nonce: nonce(), signature: null, name: ex.name, force: false },
    };
    const checked = validateCancellation(can);
    if (!checked.ok) {
      throw new Error(`requestor: built an invalid cancellation for ${ex.rid}: ${checked.errors.join("; ")}`);
    }
    ex.cancelled = true;
    ex.can = can;
    ex.cancelWaitTimer = setTimeout(() => void this.abandon(ex), this.ctx.config.cancelWaitTimeout);
    await writeJsonAtomic(path.join(this.dstInbox(ex.dst), `${ex.rid}.can.json`), can, this.ctx.folders.agent);
  }

  /** Timeout/signal give-up (D-6): B4, then the bounded wait for the cancellation response. */
  private giveUp(rid: string): void {
    const ex = this.exchanges.get(rid);
    if (!ex || ex.done || ex.cancelled || ex.res !== null) return;
    void this.writeCancellation(ex);
  }

  /** Resolves the exchange exactly once and updates the busy-relevant state (D-8). */
  private finish(ex: Outbound, finalStatus: "confirmed" | "cancelled" | "abandoned"): void {
    if (ex.done) return;
    ex.done = true;
    this.clearTimers(ex);
    this.releaseWatcher(ex.dst);
    this.exchanges.delete(ex.rid);
    if (this.exchanges.size === 0) this.hooks.onStateChange();
    ex.resolve({ finalStatus, response: ex.responseBody });
  }

  private async deleteAttachments(ex: Outbound): Promise<void> {
    for (const name of ex.attachments) {
      await remove(path.join(this.ctx.folders.forum, name));
    }
    ex.attachments = [];
  }

  private clearTimers(ex: Outbound): void {
    if (ex.timeoutTimer !== undefined) clearTimeout(ex.timeoutTimer);
    if (ex.cancelWaitTimer !== undefined) clearTimeout(ex.cancelWaitTimer);
    ex.timeoutTimer = undefined;
    ex.cancelWaitTimer = undefined;
    if (ex.signal && ex.abortHandler) ex.signal.removeEventListener("abort", ex.abortHandler);
    ex.abortHandler = undefined;
  }

  private exchangeData(
    finalStatus: OutreqFinalStatus,
    req: RequestFile,
    can: CancellationFile | null,
    res: ResponseFile | null,
    con: ConfirmationFile | null,
  ): Exchange {
    return { final_status: finalStatus, req, can, res, con };
  }

  private dstInbox(dst: AgentId): string {
    return path.join(this.ctx.folders.root, dst, "inbox");
  }

  /** D-18: one watcher per destination inbox, refcounted by outstanding rids; removed at 0.
   * Self-calls are not watched here: the receiver's own-inbox watcher feeds `notifyFile`. */
  private acquireWatcher(dst: AgentId): void {
    if (dst === this.ctx.id) return;
    const dir = this.dstInbox(dst);
    let w = this.watchers.get(dst);
    if (w === undefined) {
      w = {
        dir,
        refs: 0,
        handle: watchDir(
          dir,
          (name) => {
            if (name.endsWith(".res.json")) this.onResponseFile(name.slice(0, -".res.json".length));
          },
          (err) => this.onWatchError(dst, dir, err),
        ),
      };
      this.watchers.set(dst, w);
    }
    w.refs++;
  }

  private releaseWatcher(dst: AgentId): void {
    const w = this.watchers.get(dst);
    if (w === undefined) return;
    w.refs--;
    if (w.refs <= 0) {
      w.handle.close();
      this.watchers.delete(dst);
    }
  }

  /** D-22: a dst-inbox watch error abandons every outbound exchange to that destination. */
  private onWatchError(dst: AgentId, dir: string, err: Error): void {
    const w = this.watchers.get(dst);
    if (w !== undefined) {
      this.watchers.delete(dst);
      w.handle.close();
    }
    this.hooks.onWatchError(dir, err);
    for (const ex of [...this.exchanges.values()]) {
      if (ex.dst === dst) void this.abandon(ex);
    }
  }
}