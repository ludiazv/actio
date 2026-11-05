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

/**
 * The window the explorer draws: a bordered frame of at least this many rows, borders and
 * header and footer included. Empty space is padded inside the frame, so the window keeps its
 * shape whether the tab has three rows of content or thirty.
 */
const MIN_HEIGHT = 12;

/** Frame glyphs: the outer frame is accented, the inner rules are the plain border color. */
const FRAME = { top: ["╭", "╮"], middle: ["├", "┤"], bottom: ["╰", "╯"] } as const;

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

export class ExplorerComponent {
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
    // The frame takes one column per side plus one space of padding, so the tabs render inside it.
    const inner = Math.max(1, width - 4);
    const root = this.team.root ?? "$ACTIO_DIR or ~/.actio";

    const header = [
      th.fg(
        "accent",
        `Actio Explorer — ${this.team.id} · ${root} · prompt service: ${this.promptOn ? "on" : "off"}`,
      ),
      TABS.map((name, i) => (i === this.tab ? th.fg("accent", `[${name}]`) : name)).join("  "),
    ];
    const body =
      this.tab === 0
        ? this.teamTab(inner, th)
        : this.tab === 1
          ? this.callsTab(inner, th)
          : this.promptsTab(inner, th);
    const footer = [th.fg("dim", "tab switch · ↑↓ select · r refresh · esc close")];
    // The two rules above and below the body, plus the top and bottom of the frame.
    const filler = Math.max(0, MIN_HEIGHT - (header.length + body.length + footer.length + 4));

    const lines: string[] = [
      this.rule(FRAME.top, width, "borderAccent"),
      ...header.map((line) => this.row(line, inner, width)),
      this.rule(FRAME.middle, width, "border"),
      ...body.map((line) => this.row(line, inner, width)),
      ...Array.from({ length: filler }, () => this.row("", inner, width)),
      this.rule(FRAME.middle, width, "border"),
      ...footer.map((line) => this.row(line, inner, width)),
      this.rule(FRAME.bottom, width, "borderAccent"),
    ];
    return lines.map((line) => truncateToWidth(line, width));
  }

  /** One horizontal rule: the two corners plus the fill, `width` cells wide. */
  private rule(corners: readonly [string, string], width: number, token: "border" | "borderAccent"): string {
    return this.theme.fg(token, `${corners[0]}${"─".repeat(Math.max(0, width - 2))}${corners[1]}`);
  }

  /** One framed row: the side borders around content padded to exactly the inner width. */
  private row(content: string, inner: number, width: number): string {
    const side = this.theme.fg("borderAccent", "│");
    return `${side} ${truncateToWidth(content, inner, "...", true)} ${side}`;
  }

  /** Team tab: the agents (SelectList) and the selected agent's capabilities below it. */
  private teamTab(width: number, th: Theme): string[] {
    const views = this.team.view();
    if (views.length === 0) {
      this.select = undefined;
      this.selectKey = "";
      return [th.fg("dim", "no other agents in the view")];
    }
    const key = views.map((v) => `${v.id}:${v.status.status}:${v.health}`).join("|");
    if (this.select === undefined || key !== this.selectKey) {
      const previous = this.select?.getSelectedItem()?.value;
      const items = views.map((v) => ({
        value: v.id,
        label: `${v.id}  ${v.status.status}/${v.health}`,
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

  /** Calls tab: newest first, one line per record (D-19). */
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

  /** D-19: how a call record (including in-flight) reads in the Calls tab. */
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

  /** Prompts tab: newest first; the outcome is shown as D-19 defines it. */
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