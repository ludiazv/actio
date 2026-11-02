// Test-only extension: registers the faux provider, so smoke runs and manual checks of this
// extension make no LLM call. Loaded with `-e test/faux.ts` before `-e src/index.ts`.
// Usage (temp dirs only): PI_CODING_AGENT_DIR=$(mktemp -d) ACTIO_DIR=$(mktemp -d) \
//   pi --mode rpc --no-session -ne -e test/faux.ts -e src/index.ts --provider faux --model echo
import { createFauxCore, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ProviderConfig } from "@earendil-works/pi-coding-agent";

const SLOW_MS = 1500;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface Block {
  type: string;
  text?: string;
}
interface Message {
  role: string;
  content: string | Block[];
}

function blocks(m: Message): Block[] {
  return typeof m.content === "string" ? [{ type: "text", text: m.content }] : m.content;
}

function textOf(m: Message): string {
  return blocks(m)
    .filter((b) => b.type === "text")
    .map((b) => b.text ?? "")
    .join("");
}

function lastUserText(messages: Message[]): string {
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].role === "user") return textOf(messages[i]);
  }
  return "";
}

function newestToolResult(messages: Message[]): string | undefined {
  const m = messages[messages.length - 1];
  return m && m.role === "toolResult" ? textOf(m) : undefined;
}

// A response is: the text of a fresh tool result (so a caller can see that a tool ran), otherwise a
// tool call for `toolcall <tool> <json>`, otherwise `echo: <prompt>`. "slow" replies are delayed.
// The tool-result branch must come first: the prompt text stays the last user message, so checking the
// `toolcall` shape first would make the model call the same tool again for every remaining response.
async function respond(context: unknown) {
  const messages = (context as { messages: Message[] }).messages ?? [];
  const prompt = lastUserText(messages);
  const toolResult = newestToolResult(messages);
  if (toolResult !== undefined) return fauxAssistantMessage(`tool: ${toolResult}`);
  const toolCall = /^toolcall\s+(\S+)\s+(\{[\s\S]*\})$/.exec(prompt);
  if (toolCall) return fauxAssistantMessage(fauxToolCall(toolCall[1], JSON.parse(toolCall[2])));
  if (prompt.includes("slow")) await sleep(SLOW_MS);
  return fauxAssistantMessage(`echo: ${prompt}`);
}

export default function (pi: ExtensionAPI): void {
  const faux = createFauxCore({
    provider: "faux",
    api: "faux-api",
    models: [{ id: "echo" }],
    tokensPerSecond: 100000,
  });
  faux.setResponses(Array.from({ length: 200 }, () => respond));
  pi.registerProvider("faux", {
    baseUrl: "http://faux",
    apiKey: "x",
    api: faux.api as ProviderConfig["api"],
    streamSimple: faux.streamSimple as ProviderConfig["streamSimple"],
    models: [
      {
        id: "echo",
        name: "echo",
        reasoning: false,
        input: ["text"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: 100000,
        maxTokens: 1000,
      },
    ],
  });
}
