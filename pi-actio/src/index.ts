// The extension entry: configuration, wiring of the tool, the command, the prompt service and
// the session lifecycle. No resources are started here; they belong to `session_start` (D-7).
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI): void {
  // Nothing registered yet: the tool, the command, the prompt service and the session hooks are
  // wired here in load order, and the configuration decides whether anything is registered at all
  // (D-2, D-3).
  void pi;
}
