// Discovery and callability: the scan of the root, and the rules that decide which peers we may
// talk to (*Announcement and Discovery*, *Callability rules* 1–8).
//
// A scan reads each candidate folder's spec.json and status.json as a pair: no pair means the folder
// is not an agent yet, and a pair that does not conform is ignored and reported. Two folders that
// declare the same id: the first in lexical order keeps it, the rest are duplicates.
//
// The agent wires the debounced rescan of nuntium (D-17) and the atomic swap of its peer table; this
// file is the pure part, so the rules can be tested without a running agent.
package actio

import "time"

// Scan reads the agent folders of root. Reserved names (tools, forum, nuntium) and dotted folders
// are never agents.
func Scan(root string) (ScanResult, error) { panic("actio: not implemented") }

// callable applies the callability rules for self looking at peer, and returns nil when the peer is
// discarded. In the core profile a peer whose posture is not "trusted" is always discarded: we have
// no security features to meet it with.
func callable(self *Spec, peer DiscoveredAgent, now time.Time) *AgentView {
	panic("actio: not implemented")
}
