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

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// reservedFolder is a root entry that is never an agent folder (*Announcement and Discovery* scan
// rule 1).
var reservedFolder = map[string]struct{}{
	"tools":   {},
	"forum":   {},
	"nuntium": {},
}

// publicationState is what readPublication found in one of the two publication files.
type publicationState int

const (
	missing    publicationState = iota // the file is not there: the folder is not an agent (silence, as in TS)
	unparsable                         // it is there, and it is JSON, but it is not valid JSON
	unreadable                         // it is there, but it cannot be read (a directory, a permission failure, …)
	jsonDoc                            // it is JSON; the bytes are returned
)

// readPublication reads one of the two publication files. A file that cannot be read is
// unreadable — a directory is not "invalid JSON", it is not a file at all; a file that is read
// but is not valid JSON is unparsable — the same two readings the reference's readJson-then-
// exists pair gives them.
func readPublication(file string) ([]byte, publicationState) {
	raw, err := os.ReadFile(file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, missing
		}
		return nil, unreadable
	}
	if !json.Valid(raw) {
		return nil, unparsable
	}
	return raw, jsonDoc
}

// Scan reads the agent folders of root (*Announcement and Discovery* scan rules 1–4).
//
// Reserved names (tools, forum, nuntium) and dotted folders are never agents. A folder without the
// spec.json/status.json pair is not an agent yet, and it is silent — as in the reference, which
// tests the silence on purpose. A file that cannot be read, a file that is not valid JSON, and a
// file that does not conform to its schema are reported in ScanResult.Ignored with a one-line
// reason. Two folders that declare the same id: the first in lexical order keeps it (D-3), the
// rest are ScanResult.Duplicates.
func Scan(root string) (ScanResult, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return ScanResult{}, err
	}
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	sort.Strings(names) // lexical order decides which duplicate id wins (D-3)
	var result ScanResult
	seen := map[AgentID]struct{}{}
	for _, name := range names {
		if name[0] == '.' {
			continue // hidden folders are never agents
		}
		if _, reserved := reservedFolder[name]; reserved {
			continue // scan rule 1
		}
		folder := filepath.Join(root, name)
		info, err := os.Stat(folder)
		if err != nil {
			continue // listed, then gone (or unreadable): silence, as in TS — not an error out of Scan
		}
		if !info.IsDir() {
			continue // `actio.json` and the like are not agent folders
		}
		specFile := filepath.Join(folder, "spec.json")
		statusFile := filepath.Join(folder, "status.json")
		specRaw, specState := readPublication(specFile)
		statusRaw, statusState := readPublication(statusFile)
		// Rule 2: a folder without the pair is not an agent yet — silence, as in the reference, which
		// tests it on purpose. A file that cannot be read is said so as unreadable — it is not
		// "not valid JSON", which is reserved for bytes that were read and are not JSON.
		switch {
		case specState == missing || statusState == missing:
			continue
		case specState == unreadable:
			result.Ignored = append(result.Ignored, IgnoredFile{Path: specFile, Reason: "unreadable"})
			continue
		case statusState == unreadable:
			result.Ignored = append(result.Ignored, IgnoredFile{Path: statusFile, Reason: "unreadable"})
			continue
		case specState == unparsable:
			result.Ignored = append(result.Ignored, IgnoredFile{Path: specFile, Reason: "not valid JSON"})
			continue
		case statusState == unparsable:
			result.Ignored = append(result.Ignored, IgnoredFile{Path: statusFile, Reason: "not valid JSON"})
			continue
		}
		spec, err := validateSpec(specRaw) // rule 3: a non-conforming pair is ignored, and said so
		if err != nil {
			result.Ignored = append(result.Ignored, IgnoredFile{Path: specFile, Reason: err.Error()})
			continue
		}
		// A spec that repeats a capability name is a rule the schema cannot express; it is ignored
		// like a schema failure, and reported with the name it repeats (*Schemas* rules).
		if repeated := duplicateCapabilityName(spec); repeated != "" {
			result.Ignored = append(result.Ignored, IgnoredFile{Path: specFile, Reason: `duplicate capability name "` + repeated + `"`})
			continue
		}
		status, err := validateStatus(statusRaw)
		if err != nil {
			result.Ignored = append(result.Ignored, IgnoredFile{Path: statusFile, Reason: err.Error()})
			continue
		}
		if _, taken := seen[spec.ID]; taken {
			result.Duplicates = append(result.Duplicates, folder) // D-3: the first folder keeps the id
			continue
		}
		seen[spec.ID] = struct{}{}
		result.Agents = append(result.Agents, DiscoveredAgent{Folder: folder, Spec: *spec, Status: *status}) // rule 4
	}
	return result, nil
}

// namespacesOf is the set of namespaces a spec belongs to (*Agent Spec): absent, nil and an empty
// list are the null namespace; a list containing "*" (alone or combined with other values) is every
// namespace.
func namespacesOf(spec *Spec) (every bool, hasNull bool, names map[string]struct{}) {
	if len(spec.Namespaces) == 0 {
		return false, true, nil
	}
	names = make(map[string]struct{}, len(spec.Namespaces))
	for _, ns := range spec.Namespaces {
		if ns == nil {
			hasNull = true
			continue
		}
		if *ns == "*" {
			return true, false, nil
		}
		names[*ns] = struct{}{}
	}
	return false, hasNull, names
}

// shareNamespace is rule 2: two agents share a namespace when one of them belongs to every
// namespace, or their sets meet — the null namespace meeting the null namespace included.
func shareNamespace(self, peer *Spec) bool {
	everySelf, nullSelf, namesSelf := namespacesOf(self)
	everyPeer, nullPeer, namesPeer := namespacesOf(peer)
	if everySelf || everyPeer {
		return true
	}
	if nullSelf && nullPeer {
		return true
	}
	for ns := range namesSelf {
		if _, ok := namesPeer[ns]; ok {
			return true
		}
	}
	return false
}

// capabilityUsable is rule 5.2: a capability is usable unless its actio object disables it, or its
// accepts_from excludes us; an absent or empty accepts_from means everybody. Both live in the
// capability's actio object.
func capabilityUsable(self AgentID, cap Capability) bool {
	if isTrue(cap.Actio.Disabled) {
		return false
	}
	from := cap.Actio.AcceptsFrom
	return len(from) == 0 || containsString(from, string(self))
}

// containsString reports whether list holds s.
func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// callable applies the callability rules 1–8 for self looking at peer, and returns nil when the
// peer is discarded. In the core profile a peer whose posture is not "trusted" is always discarded:
// we have no security features to meet it with (rules 3 and 4).
func callable(self *Spec, peer DiscoveredAgent, now time.Time) *AgentView {
	peerSpec := peer.Spec
	// Rule 1: a disabled peer offers no capabilities; commands still reach it, so it can be
	// enabled again. Only new requests are affected — accepted work continues (the receiver
	// decides that).
	var usable []Capability
	if !isTrue(peerSpec.Disabled) {
		usable = peerSpec.Capabilities
	}
	filtered := make([]Capability, 0, len(usable))
	for _, cap := range usable {
		if capabilityUsable(self.ID, cap) {
			filtered = append(filtered, cap)
		}
	}
	// Rule 2: no namespace in common → the spec is not considered at all.
	if !shareNamespace(self, &peerSpec) {
		return nil
	}
	// Rules 3 and 4: this implementation supports the core profile only — the "trusted" posture.
	// A peer requiring any other posture needs security features we do not have, and the
	// zero-trust issuer possession check is unreachable for the same reason.
	if peerSpec.SecurityPosture != "trusted" {
		return nil
	}
	// Rule 5.1: an empty accepts_commands_from means nobody can send commands.
	commands := containsString(peerSpec.AcceptsCommandsFrom, string(self.ID))
	// Rule 6: nothing usable → the agent is discarded.
	if !commands && len(filtered) == 0 {
		return nil
	}
	// Rule 7: an inactive or error agent will not respond.
	if peer.Status.Status == StatusInactive || peer.Status.Status == StatusError {
		return nil
	}
	// Rule 8: health is a flag (degraded and zombie stay in the view), never a filter.
	capabilities := make(map[string]Capability, len(filtered))
	for _, cap := range filtered {
		capabilities[cap.Name] = cap
	}
	return &AgentView{
		ID:           peerSpec.ID,
		Folder:       peer.Folder,
		Spec:         peerSpec,
		Status:       peer.Status,
		Health:       HealthOf(peer.Status, now),
		Commands:     commands,
		Capabilities: capabilities,
	}
}
