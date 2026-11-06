// The tests of discovery.go: one per callability rule (1–8, named), the scan's handling of the
// reserved folders, duplicates, the malformed shapes, and the §5.4 traps.
package actio

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The clock of the callable tests: the reference status's datetime, so the health ages are exact.
var testNowBase = time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)

const testTS = "2026-10-24T09:00:00Z"

// A peer that passes every rule: it accepts commands from us, offers one open capability, and is
// trusted. Each rule test changes exactly one thing in it.
func basePeerSpec(id AgentID) Spec {
	return Spec{
		ID:                  id,
		AcceptsCommandsFrom: []string{"summarizer"},
		Capabilities:        []Capability{openCapability("echo")},
		SecurityPosture:     "trusted",
	}
}

func openCapability(name string) Capability {
	return Capability{
		Name:        name,
		Description: "a capability",
		Parameters:  json.RawMessage(`{}`),
		Actio:       CapabilityActio{Result: json.RawMessage(`{}`)},
	}
}

func peerAgent(spec Spec, state StatusState) DiscoveredAgent {
	return DiscoveredAgent{
		Folder: "/root/" + string(spec.ID),
		Spec:   spec,
		Status: Status{ID: spec.ID, Status: state, Datetime: testTS},
	}
}

func selfSpec() *Spec {
	return &Spec{ID: "summarizer", AcceptsCommandsFrom: []string{}}
}

func boolPtr(b bool) *bool    { return &b }
func strPtr(s string) *string { return &s }

// Rule 1: a disabled peer offers no capabilities, but commands still reach it, so it can be
// enabled again.
func TestCallableRule1DisabledKeepsCommands(t *testing.T) {
	peer := peerAgent(basePeerSpec("peer"), StatusIdle)
	peer.Spec.Disabled = boolPtr(true)
	view := callable(selfSpec(), peer, testNowBase)
	if view == nil {
		t.Fatal("a disabled peer that accepts commands was discarded")
	}
	if len(view.Capabilities) != 0 {
		t.Errorf("a disabled peer offered %d capabilities", len(view.Capabilities))
	}
	if !view.Commands {
		t.Error("commands no longer reach a disabled peer")
	}
}

// Rule 2: no shared namespace → the spec is not considered at all; "*" shares with everyone;
// absent, nil and [] are the null namespace, which meets itself.
func TestCallableRule2Namespaces(t *testing.T) {
	cases := []struct {
		name string
		self []*string
		peer []*string
		want bool
	}{
		{"absent meets absent", nil, nil, true},
		{"absent meets a name", nil, []*string{strPtr("work")}, false},
		{"a name meets the same", []*string{strPtr("work")}, []*string{strPtr("work")}, true},
		{"a name meets another", []*string{strPtr("work")}, []*string{strPtr("ops")}, false},
		{"a name meets star", []*string{strPtr("work")}, []*string{strPtr("*")}, true},
		{"star alone meets a name", []*string{strPtr("*")}, []*string{strPtr("ops")}, true},
		{"star with others meets a name", []*string{strPtr("*"), strPtr("x")}, []*string{strPtr("y")}, true},
		{"explicit null meets absent", []*string{nil}, nil, true},
		{"explicit null meets a name", []*string{nil}, []*string{strPtr("work")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			self := selfSpec()
			self.Namespaces = tc.self
			peer := peerAgent(basePeerSpec("peer"), StatusIdle)
			peer.Spec.Namespaces = tc.peer
			view := callable(self, peer, testNowBase)
			if tc.want && view == nil {
				t.Error("a pair that shares a namespace was discarded")
			}
			if !tc.want && view != nil {
				t.Error("a pair without a shared namespace was kept")
			}
		})
	}
}

// Rule 3: a peer whose posture is not the core profile's "trusted" needs security features we do
// not have, so its spec is not considered.
func TestCallableRule3Posture(t *testing.T) {
	for _, posture := range []string{"untrusted", "secret", "zero-trust"} {
		t.Run(posture, func(t *testing.T) {
			peer := peerAgent(basePeerSpec("peer"), StatusIdle)
			peer.Spec.SecurityPosture = posture
			if view := callable(selfSpec(), peer, testNowBase); view != nil {
				t.Errorf("a peer with posture %q was callable in the core profile", posture)
			}
		})
	}
}

// Rule 4: a zero-trust peer would require the issuer possession check; in the core profile it is
// unreachable, because rule 3 already discards every posture but "trusted" (as in the reference).
func TestCallableRule4ZeroTrustUnreachable(t *testing.T) {
	peer := peerAgent(basePeerSpec("peer"), StatusIdle)
	peer.Spec.SecurityPosture = "zero-trust"
	if view := callable(selfSpec(), peer, testNowBase); view != nil {
		t.Error("a zero-trust peer was callable in the core profile")
	}
}

// Rule 5.1: the command flag follows accepts_commands_from; an empty list means nobody.
func TestCallableRule51CommandsFrom(t *testing.T) {
	peer := peerAgent(basePeerSpec("peer"), StatusIdle)
	if view := callable(selfSpec(), peer, testNowBase); view == nil || !view.Commands {
		t.Error("a peer that lists us in accepts_commands_from must set the command flag")
	}
	peer.Spec.AcceptsCommandsFrom = []string{"other"}
	if view := callable(selfSpec(), peer, testNowBase); view == nil || view.Commands {
		t.Error("a peer that does not list us must not set the command flag")
	}
	peer.Spec.AcceptsCommandsFrom = []string{}
	if view := callable(selfSpec(), peer, testNowBase); view == nil || view.Commands {
		t.Error("an empty accepts_commands_from means nobody")
	}
}

// Rule 5.2: per capability — disabled is out, accepts_from absent/empty/matching is in, excluding
// is out.
func TestCallableRule52CapabilityFilter(t *testing.T) {
	disabled := openCapability("off")
	disabled.Actio.Disabled = boolPtr(true)
	others := openCapability("others")
	others.Actio.AcceptsFrom = []string{"other"}
	self := openCapability("self")
	self.Actio.AcceptsFrom = []string{"summarizer"}
	empty := openCapability("empty")
	empty.Actio.AcceptsFrom = []string{}
	peer := peerAgent(basePeerSpec("peer"), StatusIdle)
	peer.Spec.Capabilities = []Capability{disabled, others, self, empty, openCapability("open")}
	view := callable(selfSpec(), peer, testNowBase)
	if view == nil {
		t.Fatal("a peer with usable capabilities was discarded")
	}
	want := map[string]bool{"self": true, "empty": true, "open": true}
	for name := range view.Capabilities {
		if !want[name] {
			t.Errorf("capability %q is not usable by us, but it is in the view", name)
		}
		delete(want, name)
	}
	for name := range want {
		t.Errorf("the usable capability %q is missing from the view", name)
	}
}

// Rule 6: no commands and no usable capabilities → the agent is discarded.
func TestCallableRule6NothingUsable(t *testing.T) {
	peer := peerAgent(basePeerSpec("peer"), StatusIdle)
	peer.Spec.AcceptsCommandsFrom = []string{}
	peer.Spec.Capabilities = nil
	if view := callable(selfSpec(), peer, testNowBase); view != nil {
		t.Error("a peer with nothing usable was kept")
	}
	peer.Spec.Capabilities = []Capability{openCapability("echo")}
	if view := callable(selfSpec(), peer, testNowBase); view == nil {
		t.Error("a peer with one usable capability was discarded")
	}
}

// Rule 7: an inactive or error agent will not respond, so it is filtered out.
func TestCallableRule7StatusFilter(t *testing.T) {
	for _, state := range []StatusState{StatusInactive, StatusError} {
		t.Run(string(state), func(t *testing.T) {
			peer := peerAgent(basePeerSpec("peer"), state)
			if view := callable(selfSpec(), peer, testNowBase); view != nil {
				t.Errorf("a %s peer was kept", state)
			}
		})
	}
	for _, state := range []StatusState{StatusIdle, StatusBusy} {
		peer := peerAgent(basePeerSpec("peer"), state)
		if view := callable(selfSpec(), peer, testNowBase); view == nil {
			t.Errorf("a %s peer was discarded", state)
		}
	}
}

// Rule 8: degraded (more than 100 s since the last status update) and zombie (more than 600 s)
// are flagged, not filtered.
func TestCallableRule8HealthFlaggedNotFiltered(t *testing.T) {
	peer := peerAgent(basePeerSpec("peer"), StatusIdle)
	view := callable(selfSpec(), peer, testNowBase.Add(101*time.Second))
	if view == nil {
		t.Fatal("a degraded peer was filtered out")
	}
	if view.Health != HealthDegraded {
		t.Errorf("a 101 s old peer has health %q, want degraded", view.Health)
	}
	view = callable(selfSpec(), peer, testNowBase.Add(601*time.Second))
	if view == nil {
		t.Fatal("a zombie peer was filtered out")
	}
	if view.Health != HealthZombie {
		t.Errorf("a 601 s old peer has health %q, want zombie", view.Health)
	}
}

// A valid peer pair, for the scan tests.
const (
	peerSpecJSON   = `{"id":"peer","actio_version":"0.1","datetime":"2026-10-24T09:00:00Z","description":"A peer","accepts_commands_from":[],"capabilities":[],"security_support":[],"security_posture":"trusted"}`
	peerStatusJSON = `{"id":"peer","status":"idle","datetime":"2026-10-24T09:00:00Z","capability_status":[]}`
)

// writeAgent makes a folder under root with the given spec.json and status.json ("" = absent).
func writeAgent(t *testing.T, root, name, spec, status string) string {
	t.Helper()
	folder := filepath.Join(root, name)
	if err := os.MkdirAll(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	if spec != "" {
		if err := os.WriteFile(filepath.Join(folder, "spec.json"), []byte(spec), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if status != "" {
		if err := os.WriteFile(filepath.Join(folder, "status.json"), []byte(status), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return folder
}

// assertOneLineReason pins the portable shape of a reason: one line, no local path.
func assertOneLineReason(t *testing.T, f IgnoredFile) {
	t.Helper()
	if strings.Contains(f.Reason, "\n") {
		t.Errorf("a reason is not a single line: %q", f.Reason)
	}
	if strings.Contains(f.Reason, "file://") {
		t.Errorf("a reason names a local path: %q", f.Reason)
	}
}

func TestScanFindsThePair(t *testing.T) {
	root := t.TempDir()
	folder := writeAgent(t, root, "peer", peerSpecJSON, peerStatusJSON)
	result, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Agents) != 1 {
		t.Fatalf("the scan found %d agents, want 1", len(result.Agents))
	}
	if result.Agents[0].Folder != folder {
		t.Errorf("the agent's folder is %q, want %q", result.Agents[0].Folder, folder)
	}
	if result.Agents[0].Spec.ID != "peer" || result.Agents[0].Status.Status != StatusIdle {
		t.Error("the agent's spec/status pair was not read")
	}
}

// Scan rule 1: the reserved folders and the dotted ones are never agents.
func TestScanReservedAndHiddenExcluded(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"tools", "forum", "nuntium", ".hidden"} {
		writeAgent(t, root, name, peerSpecJSON, peerStatusJSON)
	}
	result, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Agents) != 0 || len(result.Ignored) != 0 || len(result.Duplicates) != 0 {
		t.Errorf("reserved or hidden folders leaked into the result: %+v", result)
	}
}

// A folder without spec.json is not an agent: it is reported as an ignored file with a reason.
func TestScanMissingSpec(t *testing.T) {
	root := t.TempDir()
	writeAgent(t, root, "lone", "", peerStatusJSON)
	result, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Agents) != 0 {
		t.Fatalf("a folder without spec.json was counted as an agent")
	}
	if len(result.Ignored) != 1 || !strings.HasSuffix(result.Ignored[0].Path, "spec.json") {
		t.Fatalf("the missing spec.json was not reported: %+v", result.Ignored)
	}
	assertOneLineReason(t, result.Ignored[0])
}

// A folder without status.json is not an agent: it is reported as an ignored file with a reason.
func TestScanMissingStatus(t *testing.T) {
	root := t.TempDir()
	writeAgent(t, root, "lone", peerSpecJSON, "")
	result, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Agents) != 0 {
		t.Fatalf("a folder without status.json was counted as an agent")
	}
	if len(result.Ignored) != 1 || !strings.HasSuffix(result.Ignored[0].Path, "status.json") {
		t.Fatalf("the missing status.json was not reported: %+v", result.Ignored)
	}
	assertOneLineReason(t, result.Ignored[0])
}

// A file that is not valid JSON is not an agent: it is reported, and the reason is one line.
func TestScanBrokenJSON(t *testing.T) {
	root := t.TempDir()
	writeAgent(t, root, "broken", "{ not json", peerStatusJSON)
	result, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Agents) != 0 {
		t.Fatalf("a folder with a broken spec.json was counted as an agent")
	}
	if len(result.Ignored) != 1 || result.Ignored[0].Reason != "not valid JSON" {
		t.Fatalf("the broken spec.json was not reported as not valid JSON: %+v", result.Ignored)
	}
	assertOneLineReason(t, result.Ignored[0])
}

// A file that is JSON but does not conform to the schema is reported with the validator's first
// failure as the reason: one line, no local path.
func TestScanNonConformingSpec(t *testing.T) {
	root := t.TempDir()
	bad := strings.Replace(peerSpecJSON, `"description":"A peer"`, "", 1)
	writeAgent(t, root, "bad", bad, peerStatusJSON)
	result, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Agents) != 0 {
		t.Fatalf("a non-conforming spec was counted as an agent")
	}
	if len(result.Ignored) != 1 || result.Ignored[0].Reason == "" {
		t.Fatalf("the non-conforming spec.json was not reported: %+v", result.Ignored)
	}
	assertOneLineReason(t, result.Ignored[0])
}

// A spec that repeats a capability name is a rule the schema cannot express: ignored, and said so.
func TestScanDuplicateCapabilityName(t *testing.T) {
	root := t.TempDir()
	spec := `{"id":"peer","actio_version":"0.1","datetime":"2026-10-24T09:00:00Z","description":"A peer",` +
		`"accepts_commands_from":[],"capabilities":[` +
		`{"name":"echo","description":"d","parameters":{},"actio":{"result":{}}},` +
		`{"name":"echo","description":"d","parameters":{},"actio":{"result":{}}}],` +
		`"security_support":[],"security_posture":"trusted"}`
	writeAgent(t, root, "dup", spec, peerStatusJSON)
	result, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Agents) != 0 {
		t.Fatalf("a spec with a repeated capability name was counted as an agent")
	}
	if len(result.Ignored) != 1 || result.Ignored[0].Reason != `duplicate capability name "echo"` {
		t.Fatalf("the repeated capability name was not reported: %+v", result.Ignored)
	}
	assertOneLineReason(t, result.Ignored[0])
}

// An announce-only folder (a diarium and an inbox, no spec/status pair) is not an agent.
func TestScanAnnounceOnlyFolderNotAnAgent(t *testing.T) {
	root := t.TempDir()
	folder := writeAgent(t, root, "lone", "", "")
	if err := os.WriteFile(filepath.Join(folder, "diarium.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(folder, "inbox"), 0o755); err != nil {
		t.Fatal(err)
	}
	result, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Agents) != 0 {
		t.Errorf("an announce-only folder was counted as an agent")
	}
}

// Two folders with the same id: the first in lexical order keeps it, the rest are duplicates.
func TestScanDuplicateIDs(t *testing.T) {
	root := t.TempDir()
	first := writeAgent(t, root, "aa", peerSpecJSON, peerStatusJSON)
	second := writeAgent(t, root, "bb", peerSpecJSON, peerStatusJSON)
	result, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Agents) != 1 || result.Agents[0].Folder != first {
		t.Fatalf("the first folder in lexical order must keep the id: %+v", result.Agents)
	}
	if len(result.Duplicates) != 1 || result.Duplicates[0] != second {
		t.Fatalf("the second folder must be a duplicate: %+v", result.Duplicates)
	}
}

// The root itself is missing: Scan returns the read error, and the result is empty.
func TestScanRootMissing(t *testing.T) {
	result, err := Scan(filepath.Join(t.TempDir(), "no-such-root"))
	if err == nil {
		t.Fatal("a missing root was not an error")
	}
	if !os.IsNotExist(err) {
		t.Errorf("the error for a missing root is %v, want a not-exist error", err)
	}
	if len(result.Agents) != 0 {
		t.Errorf("a missing root reported agents: %+v", result.Agents)
	}
}

// §5.4: the scan's order is lexical, whatever the order the filesystem hands back.
func TestScanResultsSortedLexical(t *testing.T) {
	root := t.TempDir()
	zebraSpec := strings.Replace(peerSpecJSON, `"id":"peer"`, `"id":"zebra"`, 1)
	zebraStatus := strings.Replace(peerStatusJSON, `"id":"peer"`, `"id":"zebra"`, 1)
	writeAgent(t, root, "zebra", zebraSpec, zebraStatus)
	writeAgent(t, root, "alpha", peerSpecJSON, peerStatusJSON)
	result, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Agents) != 2 {
		t.Fatalf("the scan found %d agents, want 2", len(result.Agents))
	}
	if !strings.HasSuffix(result.Agents[0].Folder, "alpha") || !strings.HasSuffix(result.Agents[1].Folder, "zebra") {
		t.Errorf("the scan is not in lexical order: %+v", result.Agents)
	}
}

// §5.4: a folder that is gone by the time the scan reaches it (a dangling link plays the part)
// is an ignored entry, not an error out of Scan.
func TestScanVanishedFolderIgnored(t *testing.T) {
	root := t.TempDir()
	writeAgent(t, root, "peer", peerSpecJSON, peerStatusJSON)
	ghost := filepath.Join(root, "ghost")
	if err := os.Symlink(filepath.Join(root, "no-such-target"), ghost); err != nil {
		t.Fatal(err)
	}
	result, err := Scan(root)
	if err != nil {
		t.Fatalf("a vanished folder killed the scan: %v", err)
	}
	if len(result.Agents) != 1 {
		t.Fatalf("the surviving agent was lost: %+v", result.Agents)
	}
	found := false
	for _, f := range result.Ignored {
		if f.Path == ghost {
			found = true
			assertOneLineReason(t, f)
		}
	}
	if !found {
		t.Errorf("the vanished folder was not reported: %+v", result.Ignored)
	}
}
