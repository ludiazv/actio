// The /actio overlay: the team, the outgoing calls, the received prompt requests (D-19).
// Three tabs (Team / Calls / Prompts): tab and shift+tab switch, esc closes, r refreshes (D-9).
// The overlay subscribes to the history and to nothing else; it runs no timers.
import {
  getSelectListTheme,
  type ExtensionCommandContext,
  type Theme,
} from "@earendil-works/pi-coding-agent";
import {
  matchesKey,
  SelectList,
  truncateToWidth,
  type TUI,
} from "@earendil-works/pi-tui";
import type { CallRecord, History } from "./history.js";
import type { Team } from "./team.js";

const TABS = ["Team", "Calls", "Prompts"];

export function openExplorer(
  ctx: ExtensionCommandContext,
  team: Team,
  history: History,
  promptOn: boolean,
): Promise<void> {
  if (ctx.mode !== "tui") {
    ctx.ui.notify("/actio needs the interactive TUI", "error");
    return Promise.resolve();
  }
  return ctx.ui.custom<void>(
    (tui, theme, _keybindings, done) =>
      new ExplorerComponent(tui, theme, team, history, promptOn, () => done()),
    { overlay: true, overlayOptions: { width: "80%", maxHeight: "80%", anchor: "center" } },
  );
}

class ExplorerComponent {
  private tui: TUI;
  private theme: Theme;
  private team: Team;
  private history: History;
  private promptOn: boolean;
  private close: () => void;
  private tab = 0;
  private select: SelectList | undefined;
  private selectKey = "";
  private unsubscribe: () => void;

  constructor(
    tui: TUI,
    theme: Theme,
    team: Team,
    history: History,
    promptOn: boolean,
    close: () => void,
  ) {
    this.tui = tui;
    this.theme = theme;
    this.team = team;
    this.history = history;
    this.promptOn = promptOn;
    this.close = close;
    this.unsubscribe = history.onChange(() => this.requestRedraw());
  }

  handleInput(data: string): void {
    if (matchesKey(data, "escape")) {
      this.close();
      return;
    }
    if (matchesKey(data, "shift+tab")) {
      this.tab = (this.tab + TABS.length - 1) % TABS.length;
      this.requestRedraw();
      return;
    }
    if (matchesKey(data, "tab")) {
      this.tab = (this.tab + 1) % TABS.length;
      this.requestRedraw();
      return;
    }
    if (matchesKey(data, "r")) {
      // D-9: redraw when the scan has finished; Team.refresh() does not reject.
      void this.team.refresh().then(() => this.requestRedraw());
      return;
    }
    // Team tab: up/down/confirm/cancel go through the list's tui.select.* keybindings.
    if (this.tab === 0) this.select?.handleInput(data);
  }

  render(width: number): string[] {
    const th = this.theme;
    const root = this.team.root ?? "$ACTIO_DIR or ~/.actio";
    const lines: string[] = [
      th.fg(
        "accent",
        `Actio Explorer — ${this.team.id} · ${root} · prompt service: ${this.promptOn ? "on" : "off"}`,
      ),
      TABS.map((name, i) => (i === this.tab ? th.fg("accent", `[${name}]`) : name)).join("  "),
      th.fg("border", "─".repeat(width)),
    ];
    if (this.tab === 0) lines.push(...this.teamTab(width, th));
    else if (this.tab === 1) lines.push(...this.callsTab(width, th));
    else lines.push(...this.promptsTab(width, th));
    lines.push(th.fg("border", "─".repeat(width)));
    lines.push(th.fg("dim", "tab switch · ↑↓ select · r refresh · esc close"));
    return lines.map((line) => truncateToWidth(line, width));
  }

  /** Team tab: the agents (SelectList) and the selected agent's capabilities below it. */
  private teamTab(width: number, th: Theme): string[] {
    const views = this.team.view();
    if (views.length === 0) {
      this.select = undefined;
      this.selectKey = "";
      return [th.fg("dim", "no other agents in the view")];
    }
    const key = views.map((v) => `${v.id}:${v.status.status}:${v.health ?? "-"}`).join("|");
    if (this.select === undefined || key !== this.selectKey) {
      const previous = this.select?.getSelectedItem()?.value;
      const items = views.map((v) => ({
        value: v.id,
        label: `${v.id}  ${v.status.status}/${v.health ?? "-"}`,
        description: v.spec.description,
      }));
      this.select = new SelectList(items, 10, getSelectListTheme());
      this.select.onCancel = () => this.close();
      if (previous !== undefined) {
        const index = items.findIndex((item) => item.value === previous);
        if (index >= 0) this.select.setSelectedIndex(index);
      }
      this.selectKey = key;
    }
    const lines = this.select.render(width);
    const selected = views.find((v) => v.id === this.select?.getSelectedItem()?.value);
    if (selected === undefined) {
      lines.push(th.fg("dim", "no selection"));
    } else if (selected.capabilities.size === 0) {
      lines.push(th.fg("dim", "(no usable capabilities)"));
    } else {
      for (const cap of selected.capabilities.values()) {
        lines.push(`${cap.name} — ${cap.description}`);
      }
    }
    return lines;
  }

  /** Calls tab: newest first, one line per record (A-2). */
  private callsTab(width: number, th: Theme): string[] {
    const calls = [...this.history.calls].reverse();
    if (calls.length === 0) return [truncateToWidth(th.fg("dim", "no outgoing calls yet"), width)];
    return calls.map((record) =>
      truncateToWidth(
        `${th.fg("muted", stamp(record.startedAt))}  ${record.target}  ${this.callState(record, th)}`,
        width,
      ),
    );
  }

  /** A-2: how a call record (including in-flight) reads in the Calls tab. */
  private callState(record: CallRecord, th: Theme): string {
    switch (record.state) {
      case "in-flight":
        return th.fg("dim", "in-flight");
      case "confirmed":
        if (record.error === undefined) {
          const ms = record.endedAt === undefined ? 0 : record.endedAt - record.startedAt;
          return th.fg("success", `confirmed ${ms} ms`);
        }
        return th.fg("error", record.code === undefined ? "error" : `error ${record.code}`);
      case "failed":
        return th.fg("error", `failed: ${record.error ?? ""}`);
      case "cancelled":
      case "abandoned":
        return th.fg("warning", record.state);
    }
  }

  /** Prompts tab: newest first; the outcome is shown as it is (A-3). */
  private promptsTab(width: number, th: Theme): string[] {
    const prompts = [...this.history.prompts].reverse();
    if (prompts.length === 0) return [truncateToWidth(th.fg("dim", "no prompt requests received yet"), width)];
    return prompts.map((record) => {
      const id = record.promptId === undefined ? "" : `  [${record.promptId}]`;
      return truncateToWidth(
        `${th.fg("muted", stamp(record.at))}  ${record.src}  ${record.capability}${id}  ${record.outcome}`,
        width,
      );
    });
  }

  private requestRedraw(): void {
    this.invalidate();
    this.tui.requestRender();
  }

  invalidate(): void {
    this.select?.invalidate();
  }

  dispose(): void {
    this.unsubscribe();
    this.select = undefined;
  }
}

/** Local `hh:mm:ss` for a `Date.now()`-style timestamp. */
function stamp(ms: number): string {
  const t = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${pad(t.getHours())}:${pad(t.getMinutes())}:${pad(t.getSeconds())}`;
}