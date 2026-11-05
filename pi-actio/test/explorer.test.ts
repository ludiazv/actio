// The /actio explorer window: the bordered frame and its minimum height (MIN_HEIGHT rows).
// Only the geometry is pinned — the colors come from the theme, which is faked here.
import assert from "node:assert/strict";
import { test } from "node:test";
import type { Theme } from "@earendil-works/pi-coding-agent";
import { stripTerminalSequences, type TUI } from "@earendil-works/pi-tui";
import type { Config } from "../src/config.js";
import { ExplorerComponent } from "../src/explorer.js";
import { History } from "../src/history.js";
import { Team } from "../src/team.js";

const MIN_HEIGHT = 12;

/** A theme that passes the text through: the assertions are about shape, not color. */
const plainTheme = { fg: (_token: string, text: string) => text } as unknown as Theme;
const noTui = { requestRender: () => {} } as unknown as TUI;

function open(promptOn = false): ExplorerComponent {
  const config: Config = {
    agentId: "pi1",
    actioDir: "/actio",
    exposePrompt: promptOn,
    acceptPrompt: [],
    disabled: false,
    json: false,
    reportActivity: false,
  };
  // The team is never started, so `view()` is empty and the Team tab renders its empty state.
  return new ExplorerComponent(noTui, plainTheme, new Team(config, "/cwd", new History()), new History(), promptOn, () => {});
}

/** Every row of the window, with the colors stripped. */
function rows(component: ExplorerComponent, width: number): string[] {
  return component.render(width).map((line) => stripTerminalSequences(line));
}

function checkWindow(width: number): void {
  for (const tab of ["team", "calls", "prompts"]) {
    const component = open(true);
    for (let i = 0; i < (tab === "team" ? 0 : tab === "calls" ? 1 : 2); i++) component.handleInput("\t");
    const lines = rows(component, width);
    assert.ok(lines.length >= MIN_HEIGHT, `${tab}: ${lines.length} rows is below the minimum`);
    for (const [index, line] of lines.entries()) {
      assert.equal(line.length, width, `${tab}: row ${index} is ${line.length} cells, not ${width}`);
    }
    assert.ok(lines[0]!.startsWith("╭") && lines[0]!.endsWith("╮"), `${tab}: no top border`);
    assert.ok(lines.at(-1)!.startsWith("╰") && lines.at(-1)!.endsWith("╯"), `${tab}: no bottom border`);
    for (const line of lines.slice(1, -1)) {
      const framed =
        (line.startsWith("│") && line.endsWith("│")) || (line.startsWith("├") && line.endsWith("┤"));
      assert.ok(framed, `${tab}: row is not framed: ${JSON.stringify(line)}`);
    }
  }
}

test("the explorer is a bordered window at the widths it is shown at", () => {
  checkWindow(78);
  checkWindow(40);
  checkWindow(12);
});

test("the explorer pads a short tab up to the minimum height", () => {
  const lines = rows(open(), 60);
  assert.equal(lines.length, MIN_HEIGHT, "an empty Team tab is padded to the minimum height");
  assert.ok(lines.some((line) => line.includes("Actio Explorer — pi1 · /actio · prompt service: off")));
  assert.ok(lines.some((line) => line.includes("[Team]  Calls  Prompts")));
  assert.ok(lines.some((line) => line.includes("no other agents in the view")));
  assert.ok(lines.some((line) => line.includes("tab switch · ↑↓ select · r refresh · esc close")));
});

test("a long tab grows past the minimum without breaking the frame", () => {
  const history = new History();
  const config: Config = {
    agentId: "pi1",
    actioDir: "/actio",
    exposePrompt: false,
    acceptPrompt: [],
    disabled: false,
    json: false,
    reportActivity: false,
  };
  for (let i = 0; i < 20; i++) {
    history.addPrompt({ src: `agent-${i}`, capability: "echo", promptId: `p${i}`, outcome: "done" });
  }
  const component = new ExplorerComponent(noTui, plainTheme, new Team(config, "/cwd", history), history, false, () => {});
  component.handleInput("\t");
  component.handleInput("\t");
  const lines = rows(component, 60);
  assert.equal(lines.length, 20 + 7, "20 rows of content, the header, the footer and the four rules");
  for (const line of lines) assert.equal(line.length, 60);
});
