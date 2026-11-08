// Tests for the agent: the lifecycle (D-9, D-10), lane C, and the public API.
//
// Every test runs in a private root with millisecond-scale timers, so a test never waits on the
// protocol's real timers. The slow-tick test uses the delay sync provider to make a tick's
// write outlive the stop's start.
package actio

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testConfig is the fast configuration every test agent runs with.
var testConfig = Config{
	HeartbeatInterval: 50 * time.Millisecond,
	AnnounceDelay:     5 * time.Millisecond,
	NuntiumDebounce:   10 * time.Millisecond,
	StopGrace:         2 * time.Second,
	CancelWaitTimeout: 2 * time.Second,
	ConfirmTimeout:    5 * time.Second,
}

// newTestAgent creates one agent under root and stops it (forced) when the test ends.
func newTestAgent(t *testing.T, root, id string, mutate func(*Options)) *Agent {
	t.Helper()
	cfg := testConfig
	cfg.Root = root
	opts := Options{
		ID:                  AgentID(id),
		AcceptsCommandsFrom: []string{},
		Config:              cfg,
	}
	if mutate != nil {
		mutate(&opts)
	}
	a, err := New(opts)
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.Stop(ctx, StopOptions{Force: true}); err != nil {
			t.Errorf("cleanup Stop(%s): %v", id, err)
		}
	})
	return a
}

// readJSON reads and decodes one file of the tree.
func readJSON(t *testing.T, path string, into any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("unmarshal %s: %v", path, err)
	}
}

// waitFor polls until f reports done or the bound runs out.
func waitFor(t *testing.T, bound time.Duration, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("gave up waiting for %s after %s", what, bound)
}

// waitForName polls one folder until a name matching f appears, and returns it. The bound is the
// deadline the poll gives up at.
func waitForName(t *testing.T, bound time.Duration, what, folder string, f func(string) bool) string {
	t.Helper()
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(folder)
		if err == nil {
			for _, e := range entries {
				if !e.IsDir() && f(e.Name()) {
					return e.Name()
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("gave up waiting for %s after %s", what, bound)
	return ""
}

// statusState reads the state of one status.json.
func statusState(t *testing.T, root, id string) string {
	t.Helper()
	var s Status
	readJSON(t, filepath.Join(root, id, "status.json"), &s)
	return string(s.Status)
}

// diariumStates counts the status events of one diarium, in file order.
func diariumStates(t *testing.T, root, id string) []string {
	t.Helper()
	read, err := ReadDiarium(filepath.Join(root, id))
	if err != nil {
		t.Fatalf("diarium of %s: %v", id, err)
	}
	var states []string
	for _, line := range read.Lines {
		if line.Event == "status" {
			var state string
			if err := json.Unmarshal(line.EventData, &state); err == nil {
				states = append(states, state)
			}
		}
	}
	return states
}

func startCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestAgentNewRejectsBadOptions: the id's shape and the namespace list are checked in New, and New
// arms nothing.
func TestAgentNewRejectsBadOptions(t *testing.T) {
	bad := []struct {
		name string
		opts Options
	}{
		{"empty id", Options{ID: ""}},
		{"id with a space", Options{ID: "bad id"}},
		{"id with a slash", Options{ID: "a/b"}},
		{"id of 33 characters", Options{ID: AgentID(strings.Repeat("a", 33))}},
		{"reserved id", Options{ID: "nuntium"}},
		{"empty namespace item", Options{ID: "alpha", Namespaces: []*string{ptrString("")}}},
	}
	for _, tc := range bad {
		if _, err := New(tc.opts); err == nil {
			t.Errorf("%s: New accepted it, want an error", tc.name)
		}
	}
	good, err := New(Options{ID: "alpha", Namespaces: []*string{nil, ptrString("*")}, Config: Config{Root: t.TempDir()}})
	if err != nil {
		t.Fatalf("New(alpha): %v", err)
	}
	if good == nil || good.shared == nil {
		t.Fatalf("New(alpha) returned no agent")
	}
	// New arms nothing: no folders on disk, no watcher, no goroutine.
	entries, err := os.ReadDir(good.shared.opts.Config.Root)
	if err != nil {
		t.Fatalf("list the root New was given: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("New created folders on disk: %d entries in the root", len(entries))
	}
}

func ptrString(s string) *string { return &s }

// TestAgentStartPublishesAndAnnounces: the lifecycle order of Start — the spec, the status, the
// delayed announce, and the diarium events that come with them.
func TestAgentStartPublishesAndAnnounces(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	cap := echoCap()
	if err := a.Capability(cap, func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		return map[string]any{}, nil
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	var spec Spec
	readJSON(t, filepath.Join(root, "alpha", "spec.json"), &spec)
	if spec.ID != "alpha" || len(spec.Capabilities) != 1 || spec.Capabilities[0].Name != "echo" {
		t.Errorf("spec.json: %+v", spec)
	}
	if len(spec.SecuritySupport) != 0 || spec.SecurityPosture != "trusted" {
		t.Errorf("security fields: support %v posture %q", spec.SecuritySupport, spec.SecurityPosture)
	}
	if spec.Datetime == "" {
		t.Error("spec.json has no datetime")
	}
	if got := statusState(t, root, "alpha"); got != "idle" {
		t.Errorf("status state: %q, want idle", got)
	}
	announce := filepath.Join(root, "nuntium", "alpha.n.json")
	raw, err := os.ReadFile(announce)
	if err != nil {
		t.Fatalf("the announce file: %v", err)
	}
	if !strings.Contains(string(raw), "agent alpha is active") {
		t.Errorf("announce file: %s", raw)
	}
	states := diariumStates(t, root, "alpha")
	if len(states) != 1 || states[0] != "idle" {
		t.Errorf("diarium status events: %v, want [idle]", states)
	}
	// The agent sees itself: self-calls work, as in the reference.
	if _, ok := a.Agmen()["alpha"]; !ok {
		t.Error("the agent's own view is missing from Agmen")
	}
}

// TestAgentCapabilityRejectsDuplicates: a duplicate name and a schema the strict mode rejects are
// errors, and registration after Start is closed.
func TestAgentCapabilityRejectsDuplicates(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	h := func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) { return nil, nil }
	if err := a.Capability(echoCap(), h); err != nil {
		t.Fatalf("first Capability: %v", err)
	}
	if err := a.Capability(echoCap(), h); err == nil {
		t.Error("a duplicate capability name was accepted")
	}
	bad := echoCap()
	bad.Actio.Result = json.RawMessage(`{"type":"object","unknown_keyword":true}`)
	if err := a.Capability(bad, h); err == nil {
		t.Error("a schema with an unknown keyword was accepted")
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	other := echoCap()
	other.Name = "other"
	if err := a.Capability(other, h); err == nil {
		t.Error("a capability registered after Start was accepted")
	}
}

// TestAgentOnCommandRejectsBuiltins: the four protocol command names cannot be replaced, a name
// cannot be registered twice, and registration after Start is closed.
func TestAgentOnCommandRejectsBuiltins(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	h := func(ctx context.Context, params json.RawMessage, cmd *CommandContext) error { return nil }
	for _, name := range []string{"stop", "enable", "restart", "ping"} {
		if err := a.OnCommand(name, h); err == nil {
			t.Errorf("OnCommand(%q) was accepted, want an error", name)
		}
	}
	if err := a.OnCommand("hello", h); err != nil {
		t.Fatalf("OnCommand(hello): %v", err)
	}
	if err := a.OnCommand("hello", h); err == nil {
		t.Error("a duplicate command name was accepted")
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := a.OnCommand("late", h); err == nil {
		t.Error("a command registered after Start was accepted")
	}
}

// TestAgentHeartbeatRefreshesStatus: the tick rewrites status.json with a fresh datetime and
// nothing else — no diarium event, no announce.
func TestAgentHeartbeatRefreshesStatus(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	statusPath := filepath.Join(root, "alpha", "status.json")
	var first Status
	readJSON(t, statusPath, &first)
	waitFor(t, 5*time.Second, "the status datetime to move", func() bool {
		var s Status
		if err := json.Unmarshal(mustRead(t, statusPath), &s); err != nil {
			return false
		}
		return s.Datetime != first.Datetime
	})
	if states := diariumStates(t, root, "alpha"); len(states) != 1 {
		t.Errorf("the tick wrote diarium events: %v", states)
	}
	_ = a
}

// TestAgentStopIsIdempotent: a second stop resolves, Done closes once, and a stop of an agent that
// never started resolves.
func TestAgentStopIsIdempotent(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Stop(startCtx(t), StopOptions{}); err != nil {
		t.Fatalf("Stop of an agent that never started: %v", err)
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	done := a.Done()
	ctx := startCtx(t)
	if err := a.Stop(ctx, StopOptions{}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := a.Stop(ctx, StopOptions{}); err != nil {
		t.Fatalf("a second Stop: %v", err)
	}
	select {
	case <-done:
	default:
		t.Fatal("Done did not close")
	}
}

// TestAgentStopPublishesInactive: C1 — the status first, then the delayed announce, and the
// diarium event that comes with the state.
func TestAgentStopPublishesInactive(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := a.Stop(startCtx(t), StopOptions{}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("status state after Stop: %q, want inactive", got)
	}
	raw, err := os.ReadFile(filepath.Join(root, "nuntium", "alpha.n.json"))
	if err != nil {
		t.Fatalf("the stop announce: %v", err)
	}
	if !strings.Contains(string(raw), "agent alpha is inactive") {
		t.Errorf("announce file after Stop: %s", raw)
	}
	states := diariumStates(t, root, "alpha")
	if len(states) != 2 || states[0] != "idle" || states[1] != "inactive" {
		t.Errorf("diarium status events: %v, want [idle inactive]", states)
	}
}

// TestAgentStartAfterStop: the same object starts again after a stop.
func TestAgentStartAfterStop(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := a.Stop(startCtx(t), StopOptions{}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if got := statusState(t, root, "alpha"); got != "idle" {
		t.Errorf("status after the second Start: %q, want idle", got)
	}
}

// TestAgentStartFailureArmsNothing: a bad sync value fails at Start, nothing is left armed, and a
// corrected Start works.
func TestAgentStartFailureArmsNothing(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.Config.Sync = "bogus" })
	if err := a.Start(startCtx(t)); err == nil {
		t.Fatal("Start with a bad sync value succeeded")
	}
	if _, err := os.Stat(filepath.Join(root, "alpha")); !os.IsNotExist(err) {
		t.Errorf("a failed Start left the agent folder behind")
	}
	a.cur().config.Sync = "" // the next Start reloads the configuration from the (valid) default
	a2, err := New(Options{ID: "alpha", AcceptsCommandsFrom: []string{}, Config: testConfigWithRoot(root)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a2.Start(startCtx(t)); err != nil {
		t.Fatalf("a corrected Start: %v", err)
	}
	// a2 must end before the test's folder is removed: a running agent writes into it.
	if err := a2.Stop(startCtx(t), StopOptions{Force: true}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	_ = a
}

// TestAgentBusyWhileWorking: D-8 — while the capability runs, the status says busy; after the
// answer, idle again.
func TestAgentBusyWhileWorking(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	cap := echoCap()
	cap.Name = "slow"
	running := make(chan struct{})
	release := make(chan struct{})
	if err := a.Capability(cap, func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		close(running)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return map[string]any{}, nil
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	outCtx, cancelOut := context.WithTimeout(startCtx(t), 10*time.Second)
	defer cancelOut()
	done := make(chan Outcome, 1)
	go func() {
		out, err := b.Request(outCtx, "alpha", "slow", map[string]any{"x": 1}, RequestOptions{Timeout: 10 * time.Second})
		if err != nil {
			t.Errorf("Request: %v", err)
		}
		done <- out
	}()
	<-running
	statusPath := filepath.Join(root, "alpha", "status.json")
	waitFor(t, 5*time.Second, "the status to say busy", func() bool {
		var s Status
		if err := json.Unmarshal(mustRead(t, statusPath), &s); err != nil {
			return false
		}
		return s.Status == "busy"
	})
	close(release)
	var out Outcome
	select {
	case out = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the request did not finish")
	}
	if out.FinalStatus != "confirmed" {
		t.Errorf("final status: %q, want confirmed", out.FinalStatus)
	}
	waitFor(t, 5*time.Second, "the status to say idle again", func() bool {
		var s Status
		if err := json.Unmarshal(mustRead(t, statusPath), &s); err != nil {
			return false
		}
		return s.Status == "idle"
	})
}

// TestAgentSelfCall: a request to one's own inbox. The requestor writes the request into the folder
// the agent's own receiver is already watching on the agent's one watcher, and the receiver answers
// it like any peer's: the exchange ends confirmed, the handler ran exactly once, and the diarium
// lines carry the exchange — req, res, con — byte for byte as the files were.
func TestAgentSelfCall(t *testing.T) {
	root := t.TempDir()
	ran := make(chan struct{})
	release := make(chan struct{})
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Capability(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		ran <- struct{}{} // a second run would block here and stall the exchange: exactly once
		select {
		case <-release:
		case <-ctx.Done():
		}
		return params, nil
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	inbox := filepath.Join(root, "alpha", "inbox")
	outCtx, cancelOut := context.WithTimeout(startCtx(t), 15*time.Second)
	defer cancelOut()
	done := make(chan Outcome, 1)
	go func() {
		out, err := a.Request(outCtx, "alpha", "echo", map[string]any{"msg": "self"}, RequestOptions{Timeout: 10 * time.Second})
		if err != nil {
			t.Errorf("Request(self): %v", err)
		}
		done <- out
	}()
	// The request file lands in our own inbox. Read its bytes while the handler holds the exchange
	// open: the archive deletes the files only at the end.
	reqName := waitForName(t, 5*time.Second, "the request file in our own inbox", inbox, func(name string) bool {
		return strings.HasSuffix(name, ".json") &&
			!strings.HasSuffix(name, ".res.json") &&
			!strings.HasSuffix(name, ".con.json") &&
			!strings.HasSuffix(name, ".can.json")
	})
	reqRaw := mustRead(t, filepath.Join(inbox, reqName))
	rid := strings.TrimSuffix(reqName, ".json")
	select {
	case <-ran: // the handler ran, once
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never ran: the request was not delivered to our own receiver")
	}
	close(release)
	var out Outcome
	select {
	case out = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the self-call did not finish")
	}
	if out.FinalStatus != "confirmed" {
		t.Fatalf("final status of the self-call: %q, want confirmed", out.FinalStatus)
	}
	// The diarium lines of the exchange, read raw: the envelopes land on the lines as raw bytes.
	// The inreq line lands when the receiver archives, which is after the confirmation write, so
	// poll for both lines of this rid.
	type envLine struct {
		Event     string `json:"event"`
		EventData struct {
			FinalStatus string          `json:"final_status"`
			Req         json.RawMessage `json:"req"`
			Can         json.RawMessage `json:"can"`
			Res         json.RawMessage `json:"res"`
			Con         json.RawMessage `json:"con"`
		} `json:"event_data"`
	}
	var inreq, outreq *envLine
	waitFor(t, 5*time.Second, "the diarium lines of the exchange", func() bool {
		inreq, outreq = nil, nil
		raw, err := os.ReadFile(filepath.Join(root, "alpha", "diarium.jsonl"))
		if err != nil {
			return false
		}
		for _, line := range strings.Split(string(raw), "\n") {
			var l envLine
			if json.Unmarshal([]byte(line), &l) != nil || l.EventData.Req == nil {
				continue
			}
			var head struct {
				Header
			}
			if json.Unmarshal(l.EventData.Req, &head) != nil || head.RID != RID(rid) {
				continue
			}
			if l.Event == "inreq" {
				inreq = &l
			} else if l.Event == "outreq" && l.EventData.FinalStatus == "confirmed" {
				outreq = &l
			}
		}
		return inreq != nil && outreq != nil
	})
	if inreq.EventData.FinalStatus != "confirmed" || outreq.EventData.FinalStatus != "confirmed" {
		t.Errorf("final statuses: inreq=%q outreq=%q, want confirmed", inreq.EventData.FinalStatus, outreq.EventData.FinalStatus)
	}
	// Byte for byte: the receiver's line against the requestor's line, and both against the file
	// the test read. The res and con files are deleted by the archive, so the two lines — one the
	// bytes written, the other the bytes read back — are the files.
	if !bytes.Equal(inreq.EventData.Req, outreq.EventData.Req) || !bytes.Equal(inreq.EventData.Req, reqRaw) {
		t.Error("the req envelope is not the request file's bytes on both lines")
	}
	if !bytes.Equal(inreq.EventData.Res, outreq.EventData.Res) {
		t.Error("the res envelope is not the response file's bytes on both lines")
	}
	if !bytes.Equal(inreq.EventData.Con, outreq.EventData.Con) {
		t.Error("the con envelope is not the confirmation file's bytes on both lines")
	}
	// The response file is the one our own receiver wrote into our own inbox: the header says so.
	var resFile struct {
		Header
		Response json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(inreq.EventData.Res, &resFile); err != nil {
		t.Fatalf("the res envelope is not a response file: %v", err)
	}
	if resFile.RID != RID(rid) || resFile.Src != "alpha" || resFile.Dst != "alpha" {
		t.Errorf("the response header: %+v, want rid %s from alpha to alpha", resFile.Header, rid)
	}
	var resBody struct {
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal(resFile.Response, &resBody) == nil {
		if !bytes.Equal(resBody.Result, reqBodyParams(reqRaw)) {
			t.Errorf("the response result %s is not the echo of the request parameters", resBody.Result)
		}
	}
}

// reqBodyParams extracts the parameters bytes from one request file's raw bytes.
func reqBodyParams(reqRaw []byte) []byte {
	var f struct {
		Request json.RawMessage `json:"request"`
	}
	if json.Unmarshal(reqRaw, &f) != nil {
		return nil
	}
	var body struct {
		Parameters json.RawMessage `json:"parameters"`
	}
	if json.Unmarshal(f.Request, &body) != nil {
		return nil
	}
	return body.Parameters
}

// TestAgentBusyOutbound (D-8): the outbound half of busy. While A's request to B is in flight, A
// publishes busy even though A's own receiver has no work; after the exchange settles, A publishes
// idle again.
func TestAgentBusyOutbound(t *testing.T) {
	root := t.TempDir()
	release := make(chan struct{})
	b := newTestAgent(t, root, "beta", nil)
	cap := echoCap()
	cap.Name = "slow"
	if err := b.Capability(cap, func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return map[string]any{}, nil
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	outCtx, cancelOut := context.WithTimeout(startCtx(t), 10*time.Second)
	defer cancelOut()
	done := make(chan Outcome, 1)
	go func() {
		out, err := a.Request(outCtx, "beta", "slow", map[string]any{}, RequestOptions{Timeout: 10 * time.Second})
		if err != nil {
			t.Errorf("Request: %v", err)
		}
		done <- out
	}()
	// While the exchange is in flight, A's status says busy: the outbound count is the half this
	// test pins.
	waitFor(t, 5*time.Second, "alpha's status to say busy", func() bool {
		return statusState(t, root, "alpha") == "busy"
	})
	close(release)
	var out Outcome
	select {
	case out = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the request did not finish")
	}
	if out.FinalStatus != "confirmed" {
		t.Errorf("final status: %q, want confirmed", out.FinalStatus)
	}
	waitFor(t, 5*time.Second, "alpha's status to say idle again", func() bool {
		return statusState(t, root, "alpha") == "idle"
	})
}

// TestAgentTwoAgentsExchange (M3): two agents in one process exchange a request, a response and a
// confirmation; a cancelled request leaves the cancellation in the peer's inbox.
func TestAgentTwoAgentsExchange(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	cap := echoCap()
	if err := a.Capability(cap, func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		return map[string]any{"echoed": true}, nil
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	slow := echoCap()
	slow.Name = "slow"
	if err := a.Capability(slow, func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}); err != nil {
		t.Fatalf("Capability(slow): %v", err)
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	// beta sees alpha, and alpha's disabled state filters its capabilities, not its presence.
	waitFor(t, 5*time.Second, "beta to see alpha", func() bool {
		_, _ = b.Refresh(startCtx(t))
		_, ok := b.Agmen()["alpha"]
		return ok
	})
	ctx := startCtx(t)
	out, err := b.Request(ctx, "alpha", "echo", map[string]any{"x": 1}, RequestOptions{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if out.FinalStatus != "confirmed" || out.Response == nil {
		t.Fatalf("outcome: %+v", out)
	}
	var result map[string]any
	if err := json.Unmarshal(out.Response.Result, &result); err != nil || !result["echoed"].(bool) {
		t.Errorf("the response result: %s (err %v)", out.Response.Result, err)
	}
	// The confirmation is in alpha's inbox (the destination archives it), and both diaria keep the
	// exchange.
	confirmations := mustList(t, filepath.Join(root, "alpha", "inbox"), ".con.json")
	if len(confirmations) != 1 {
		t.Fatalf("confirmations in alpha's inbox: %v", confirmations)
	}
	// The receiver archives the exchange when it sees the confirmation: give it a moment.
	waitFor(t, 5*time.Second, "the inreq event in alpha's diarium", func() bool {
		readA, err := ReadDiarium(filepath.Join(root, "alpha"))
		if err != nil {
			return false
		}
		for _, line := range readA.Lines {
			if line.Event == "inreq" {
				return true
			}
		}
		return false
	})
	readB, err := ReadDiarium(filepath.Join(root, "beta"))
	if err != nil {
		t.Fatalf("beta diarium: %v", err)
	}
	sawOut := false
	for _, line := range readB.Lines {
		if line.Event == "outreq" {
			var env ExchangeEnvelopes
			if err := json.Unmarshal(line.EventData, &env); err == nil && env.FinalStatus == "confirmed" {
				sawOut = true
			}
		}
	}
	if !sawOut {
		t.Error("beta's diarium has no confirmed outreq event")
	}

	// The cancellation: beta cancels while alpha is still working; the cancellation lands in
	// alpha's inbox and beta's final status is cancelled or abandoned, never an error.
	cancelCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	out2, err := b.Request(cancelCtx, "alpha", "slow", map[string]any{}, RequestOptions{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("Request(slow): %v", err)
	}
	if out2.FinalStatus != "cancelled" && out2.FinalStatus != "abandoned" {
		t.Errorf("final status of the cancelled request: %q", out2.FinalStatus)
	}
	waitFor(t, 5*time.Second, "the cancellation in alpha's inbox", func() bool {
		list, _ := listInbox(t, root, "alpha")
		return containsSuffix(list, ".can.json")
	})
}

// TestAgentCommandsNeverAnswered: a command runs in the peer, leaves no response file, and the
// sender is not told anything about it.
func TestAgentCommandsNeverAnswered(t *testing.T) {
	root := t.TempDir()
	ran := make(chan struct{}, 1)
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := a.OnCommand("hello", func(ctx context.Context, params json.RawMessage, cmd *CommandContext) error {
		ran <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("OnCommand: %v", err)
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	ctx := startCtx(t)
	rid, err := b.Command(ctx, "alpha", "hello", map[string]any{"greet": "team"})
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("the command handler never ran")
	}
	// The command file does not stay in the inbox: the receiver archives it — the inreq event with
	// the command envelope, then the deletion. What must never appear is a response file.
	waitFor(t, 2*time.Second, "the command's inreq event", func() bool {
		d, err := a.Diarium()
		if err != nil {
			return false
		}
		for _, line := range d.Lines {
			if line.Event != "inreq" {
				continue
			}
			var env struct {
				Req json.RawMessage `json:"req"`
			}
			if json.Unmarshal(line.EventData, &env) == nil && bytes.Contains(env.Req, []byte(rid)) {
				return true
			}
		}
		return false
	})
	list := mustList(t, filepath.Join(root, "alpha", "inbox"), ".json")
	for _, name := range list {
		if strings.HasSuffix(name, ".res.json") {
			t.Error("a command produced a response file; commands are never answered")
		}
	}
}

// TestAgentEnableCommand: the enable command rewrites the spec's disabled flag and re-announces;
// the flag is the run's state, not the options'.
func TestAgentEnableCommand(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := a.Capability(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	ctx := startCtx(t)
	if _, err := b.Command(ctx, "alpha", "enable", map[string]any{"enabled": false}); err != nil {
		t.Fatalf("Command(enable): %v", err)
	}
	specPath := filepath.Join(root, "alpha", "spec.json")
	waitFor(t, 5*time.Second, "the spec to say disabled", func() bool {
		var m map[string]any
		if err := json.Unmarshal(mustRead(t, specPath), &m); err != nil {
			return false
		}
		return m["disabled"] == true
	})
	raw, _ := os.ReadFile(filepath.Join(root, "nuntium", "alpha.n.json"))
	if !strings.Contains(string(raw), "agent alpha is disabled") {
		t.Errorf("the disable announce: %s", raw)
	}
	// A disabled peer offers no capabilities, but commands still reach it (rule 1).
	if _, err := b.Command(ctx, "alpha", "enable", map[string]any{"enabled": true}); err != nil {
		t.Fatalf("Command(enable again): %v", err)
	}
	waitFor(t, 5*time.Second, "the spec to say enabled", func() bool {
		var m map[string]any
		if err := json.Unmarshal(mustRead(t, specPath), &m); err != nil {
			return false
		}
		return m["disabled"] == false
	})
}

// TestAgentRestartKeepsRegAndClearsDisabled (D-10): the restart command stops and starts the same
// object; the registrations survive, a disabled flag set by enable does not, and Done is the same
// unclosed channel.
func TestAgentRestartKeepsRegAndClearsDisabled(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := a.Capability(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	ctx := startCtx(t)
	if _, err := b.Command(ctx, "alpha", "enable", map[string]any{"enabled": false}); err != nil {
		t.Fatalf("Command(enable): %v", err)
	}
	waitFor(t, 5*time.Second, "the spec to say disabled", func() bool {
		var m map[string]any
		if err := json.Unmarshal(mustRead(t, filepath.Join(root, "alpha", "spec.json")), &m); err != nil {
			return false
		}
		return m["disabled"] == true
	})
	doneBefore := a.Done()
	if _, err := b.Command(ctx, "alpha", "restart", map[string]any{}); err != nil {
		t.Fatalf("Command(restart): %v", err)
	}
	specPath := filepath.Join(root, "alpha", "spec.json")
	waitFor(t, 10*time.Second, "the restarted agent to publish its spec", func() bool {
		var m map[string]any
		if err := json.Unmarshal(mustRead(t, specPath), &m); err != nil {
			return false
		}
		caps, _ := m["capabilities"].([]any)
		_, disabled := m["disabled"]
		return len(caps) == 1 && !disabled && statusState(t, root, "alpha") == "idle"
	})
	if doneBefore != a.Done() {
		t.Error("Done changed after a restart")
	}
	select {
	case <-doneBefore:
		t.Error("Done closed on a restart; a restart does not close it")
	default:
	}
}

// TestAgentPingCommand: ping refreshes the spec and the status datetimes and re-announces.
func TestAgentPingCommand(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := a.Capability(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	var first Spec
	readJSON(t, filepath.Join(root, "alpha", "spec.json"), &first)
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	// The spec datetime is whole seconds; cross a boundary so the ping's refresh is observable.
	time.Sleep(1100 * time.Millisecond)
	ctx := startCtx(t)
	if _, err := b.Command(ctx, "alpha", "ping", nil); err != nil {
		t.Fatalf("Command(ping): %v", err)
	}
	specPath := filepath.Join(root, "alpha", "spec.json")
	waitFor(t, 5*time.Second, "the spec datetime to move", func() bool {
		var m map[string]any
		if err := json.Unmarshal(mustRead(t, specPath), &m); err != nil {
			return false
		}
		return m["datetime"] != first.Datetime
	})
	raw, _ := os.ReadFile(filepath.Join(root, "nuntium", "alpha.n.json"))
	if !strings.Contains(string(raw), "agent alpha is alive") {
		t.Errorf("the ping announce: %s", raw)
	}
}

// TestAgentStopCommand: the stop command stops the agent; Done closes.
func TestAgentStopCommand(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	ctx := startCtx(t)
	if _, err := b.Command(ctx, "alpha", "stop", map[string]any{"force": true}); err != nil {
		t.Fatalf("Command(stop): %v", err)
	}
	select {
	case <-a.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done did not close after the stop command")
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("status after the stop command: %q, want inactive", got)
	}
}

// TestAgentStopWithSlowTick: a tick whose publish blocks races the Stop. The heartbeat is
// stopped and waited for before C1, and the tick writes nothing once the state is inactive, so the
// file on disk ends inactive no matter how the race interleaves. The bound is Stop's own context:
// the tick takes no context, since a file write cannot be cancelled.
func TestAgentStopWithSlowTick(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) {
		o.Config.Sync = "delay:200"
		o.Config.HeartbeatInterval = 100 * time.Millisecond
	})
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// A tick's write takes 200ms and the interval is 100ms, so a tick is in flight when the stop
	// starts.
	time.Sleep(250 * time.Millisecond)
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.Stop(stopCtx, StopOptions{}); err != nil {
		t.Fatalf("Stop with a slow tick in flight: %v", err)
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("status after a Stop that raced a slow tick: %q, want inactive", got)
	}
}

// TestAgentTwoStopCommands: stopInternal is entered twice — once by a peer's stop command, once by
// the program's own Stop — and the second entry must not run a second D-9 sequence. The
// construction is synchronized, not a timing bet: a request sits in flight, so the command's stop
// spends the whole grace drain waiting for it; the test waits (on the heartbeat's own lock) until
// the handler has stopped the heartbeat — it is inside stopInternal, in the drain — and only then
// enters the second stop, well inside the drain's window. (A second command file would not do:
// the receiver unsubscribes its inbox at the start of the stop, so a late file is never
// dispatched.) No panic, the agent ends stopped, Done closes, and the inactive publication happens
// once: the second entry sees the stop in flight and resolves, so the status file and its diarium
// event are written a single time.
func TestAgentTwoStopCommands(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := a.Capability(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		<-ctx.Done() // in flight until the stop's grace expires and the exchange is cancelled
		return nil, ctx.Err()
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	go func() {
		_, _ = b.Request(context.Background(), "alpha", "echo", map[string]any{"in": "x"}, RequestOptions{Timeout: 30 * time.Second})
	}()
	waitFor(t, 10*time.Second, "alpha to be busy with the in-flight request", func() bool {
		return statusState(t, root, "alpha") == "busy"
	})
	if _, err := b.Command(startCtx(t), "alpha", "stop", map[string]any{"force": false}); err != nil {
		t.Fatalf("Command(stop): %v", err)
	}
	waitFor(t, 10*time.Second, "the stop command's handler to stop the heartbeat",
		func() bool { return heartbeatStopped(a) })
	// The handler's stop is in flight, in the grace drain: the second entry, from the program
	// itself, must wait for it at the gate, not resolve early: it returns only once the first stop
	// has finished, so inactive is on disk the moment it returns.
	if err := a.Stop(startCtx(t), StopOptions{}); err != nil {
		t.Fatalf("the concurrent Stop: %v", err)
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("the Stop returned before inactive was on disk: status %q", got)
	}
	select {
	case <-a.Done():
	case <-time.After(20 * time.Second):
		t.Fatal("Done did not close after the two stops")
	}
	inactive := 0
	for _, s := range diariumStates(t, root, "alpha") {
		if s == "inactive" {
			inactive++
		}
	}
	if inactive != 1 {
		t.Errorf("inactive status events in the diarium: %d, want exactly one", inactive)
	}
}

// heartbeatStopped reads the heartbeat's stopped flag under its own lock: true means a stop has
// entered the D-9 sequence (the heartbeat is the first step), and the read is race-free.
func heartbeatStopped(a *Agent) bool {
	hb := a.cur().hb
	hb.mu.Lock()
	defer hb.mu.Unlock()
	return hb.stopped
}

// TestAgentStopRetryAfterFailedC1: a Stop whose context expires in C1's announce delay leaves the
// agent half-stopped — the loops are dead, the watcher is open, the agent is still running — and
// the natural retry must finish the stop without panicking on the already-closed discovery stop.
func TestAgentStopRetryAfterFailedC1(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.Config.AnnounceDelay = 200 * time.Millisecond })
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// The first Stop: a context that expires inside the announce delay, so C1 fails and C2 is
	// skipped. Force keeps everything before C1 under the deadline.
	fastCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := a.Stop(fastCtx, StopOptions{Force: true}); err == nil {
		t.Fatal("the first Stop succeeded; the test needs a failed C1")
	}
	if !a.cur().running.Load() {
		t.Fatal("a failed C1 ended the agent; it must stay running for the retry")
	}
	// The retry: no panic, the agent ends stopped.
	retryCtx, cancelRetry := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRetry()
	if err := a.Stop(retryCtx, StopOptions{}); err != nil {
		t.Fatalf("the Stop retry: %v", err)
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("status after the retry: %q, want inactive", got)
	}
	select {
	case <-a.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done did not close after the retry")
	}
}

// TestAgentRestartHonoursDelay: restart {delay: 1} brings the agent back no earlier than about a
// second: the delay is honoured on a live timer, not swallowed by the receiver's dead context.
func TestAgentRestartHonoursDelay(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	start := time.Now()
	if _, err := b.Command(startCtx(t), "alpha", "restart", map[string]any{"delay": 1}); err != nil {
		t.Fatalf("Command(restart): %v", err)
	}
	waitFor(t, 10*time.Second, "alpha to stop for the restart", func() bool { return !a.cur().running.Load() })
	waitFor(t, 15*time.Second, "alpha to come back after the delay", func() bool { return a.cur().running.Load() })
	if elapsed := time.Since(start); elapsed < 950*time.Millisecond {
		t.Errorf("restart with delay 1 came back after %s; the delay must be honoured (>= ~1s)", elapsed)
	}
}

// TestAgentRestartFractionalDelayIsHonoured: a schema-valid fractional delay (the schema's
// duration is a number, not an integer) really delays the restart — the agent is still down at a
// bound inside the delay, and up again at a bound after it. A fraction dropped to 0 would restart
// immediately, which is what the int parse did: the delay is parsed as a number and the fraction is
// honoured.
func TestAgentRestartFractionalDelayIsHonoured(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	if _, err := b.Command(startCtx(t), "alpha", "restart", map[string]any{"delay": 1.5}); err != nil {
		t.Fatalf("Command(restart): %v", err)
	}
	time.Sleep(750 * time.Millisecond) // a bound inside the 1.5s delay, not through the clamp
	if a.cur().running.Load() {
		t.Fatal("a fractional delay of 1.5s restarted immediately: the fraction was dropped to 0")
	}
	waitFor(t, 15*time.Second, "alpha to come back after the fractional delay", func() bool { return a.cur().running.Load() })
}

// TestAgentClampedRestartDelay: the restart delay policy, unit-tested at the helper: a huge delay
// clamps to the bound, a delay at the bound passes through, a delay under the bound passes
// through, and zero or negative delays arm no wait. The constant's value is what its comment
// claims — if the bound moves, one line of this table moves with it.
func TestAgentClampedRestartDelay(t *testing.T) {
	if maxRestartDelay != 60*time.Second {
		t.Errorf("maxRestartDelay = %s; the comment claims 60s", maxRestartDelay)
	}
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"a huge delay clamps to the bound", 3600 * time.Second, maxRestartDelay},
		{"just above the bound clamps", maxRestartDelay + time.Second, maxRestartDelay},
		{"at the bound passes through", maxRestartDelay, maxRestartDelay},
		{"under the bound passes through", 1 * time.Second, 1 * time.Second},
		{"a fractional delay passes through", 1500 * time.Millisecond, 1500 * time.Millisecond},
		{"zero arms no wait", 0, 0},
		{"a negative delay arms no wait", -5 * time.Second, 0},
	}
	for _, tc := range cases {
		if got := clampedRestartDelay(tc.in); got != tc.want {
			t.Errorf("%s: clampedRestartDelay(%s) = %s, want %s", tc.name, tc.in, got, tc.want)
		}
	}
}

// TestAgentRefreshBeforeStart: Refresh before Start names the reason, as Request and Command do
// for the same situation, instead of returning an empty view and a nil error.
func TestAgentRefreshBeforeStart(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	views, err := a.Refresh(startCtx(t))
	if err == nil {
		t.Fatal("Refresh before Start succeeded; it must name the reason")
	}
	if views != nil {
		t.Errorf("Refresh before Start returned a view: %v", views)
	}
	if !strings.Contains(err.Error(), "not started") {
		t.Errorf("the Refresh error does not name the reason: %v", err)
	}
}

// TestAgentRescanWithoutSpec: a rescan that finds peers but has no self spec yet must return via
// the guard, not nil-deref in the callability rules. The peer's publication pair is real, so the
// scan succeeds and reaches the pairing.
func TestAgentRescanWithoutSpec(t *testing.T) {
	root := t.TempDir()
	spec, err := buildSpec(Options{ID: "beta", AcceptsCommandsFrom: []string{}}, nil, time.Now())
	if err != nil {
		t.Fatalf("buildSpec(beta): %v", err)
	}
	peerDir := filepath.Join(root, "beta")
	if err := os.MkdirAll(peerDir, 0o755); err != nil {
		t.Fatalf("mkdir beta: %v", err)
	}
	for name, value := range map[string]any{"spec.json": spec, "status.json": buildStatus("beta", StatusIdle, time.Now(), nil)} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(peerDir, name), raw, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	a := newTestAgent(t, root, "alpha", nil)
	a.cur().folders.root = root // a valid tree: the scan succeeds and would pair the nil spec
	a.rescan()                  // the guard must return before the callability rules touch the nil self
}

// Row 2.3 (protocol §2, a MUST): a change in nuntium triggers a scan. Alpha runs; beta starts
// afterwards; without any Refresh call, alpha's Agmen() gains beta within the debounce plus a
// margin. The neighbour TestAgentRescanWithoutSpec pins the nil-spec guard; this pins the watch —
// the nuntium.changed() case of discoveryLoop schedules the debounced scan.
func TestAgentNuntiumChangeTriggersScan(t *testing.T) {
	root := t.TempDir()
	alpha := newTestAgent(t, root, "alpha", nil)
	if err := alpha.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	// Beta starts afterwards: its announce file lands in nuntium, waking alpha's discovery. Beta
	// accepts alpha's commands, so the callability rules keep it in the view (rule 6).
	beta := newTestAgent(t, root, "beta", func(o *Options) { o.AcceptsCommandsFrom = []string{"alpha"} })
	if err := beta.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	// No Refresh: the scan is triggered by the nuntium change, debounced at 1 s (the default
	// NuntiumDebounce). The bound is the debounce plus a margin for the scan and the view swap;
	// 5 s is enough — the scan is a tree read, well under a second, and the margin covers
	// scheduler latency at GOMAXPROCS=1.
	waitFor(t, 5*time.Second, "alpha's Agmen() to gain beta without a Refresh", func() bool {
		_, ok := alpha.Agmen()["beta"]
		return ok
	})
}

// TestAgentFailPublishesError: Fail publishes the error state and announces it; the agent keeps
// running, and a second Fail does nothing.
func TestAgentFailPublishesError(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := a.Fail("the disk is full"); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if got := statusState(t, root, "alpha"); got != "error" {
		t.Errorf("status after Fail: %q, want error", got)
	}
	raw, _ := os.ReadFile(filepath.Join(root, "nuntium", "alpha.n.json"))
	if !strings.Contains(string(raw), "agent alpha failed: the disk is full") {
		t.Errorf("the fail announce: %s", raw)
	}
	if err := a.Fail("again"); err != nil {
		t.Fatalf("a second Fail: %v", err)
	}
	// The agent is still running: a Stop is needed to end it.
	if !a.cur().running.Load() {
		t.Error("Fail stopped the agent; it must stay in the error state")
	}
}

// TestAgentLogAndDiarium: Log appends a log event, and Diarium reads it back.
func TestAgentLogAndDiarium(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Log("before start"); err == nil {
		t.Error("Log before Start succeeded; the agent has no diarium yet")
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := a.Log("the team, this happened"); err != nil {
		t.Fatalf("Log: %v", err)
	}
	read, err := a.Diarium()
	if err != nil {
		t.Fatalf("Diarium: %v", err)
	}
	sawLog := false
	for _, line := range read.Lines {
		if line.Event == "log" {
			var msg string
			if err := json.Unmarshal(line.EventData, &msg); err == nil && msg == "the team, this happened" {
				sawLog = true
			}
		}
	}
	if !sawLog {
		t.Error("the log event is not in the diarium")
	}
}

// TestAgentRequestBeforeStart: the requestor is not armed before Start.
func TestAgentRequestBeforeStart(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	ctx := startCtx(t)
	if _, err := a.Request(ctx, "beta", "echo", nil, RequestOptions{}); err == nil {
		t.Error("Request before Start succeeded")
	}
	if _, err := a.Command(ctx, "beta", "ping", nil); err == nil {
		t.Error("Command before Start succeeded")
	}
}

// mustRead reads a file, failing the test on error.
func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return raw
}

// mustList lists the files of one folder that end in suffix.
func mustList(t *testing.T, dir, suffix string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("list %s: %v", dir, err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), suffix) {
			names = append(names, e.Name())
		}
	}
	return names
}

// listInbox lists one inbox without failing the test (a polling helper).
func listInbox(t *testing.T, root, id string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(root, id, "inbox"))
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

func containsSuffix(list []string, suffix string) bool {
	for _, name := range list {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

func testConfigWithRoot(root string) Config {
	cfg := testConfig
	cfg.Root = root
	return cfg
}

// --- one run is one object, and the lifecycle has a gate -------------------------------------

// doneOf reports whether Done has closed, without blocking.
func doneOf(a *Agent) bool {
	select {
	case <-a.Done():
		return true
	default:
		return false
	}
}

// within runs f on a goroutine and reports whether it returned within the bound.
func within(bound time.Duration, f func()) bool {
	done := make(chan struct{})
	go func() { f(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(bound):
		return false
	}
}

// ctxN is a detached context that expires after d: for calls that outlive the test's setup. The
// cancel is deliberately not called: the expiry is the point, and the context is short-lived.
func ctxN(d time.Duration) context.Context {
	c, cancel := context.WithTimeout(context.Background(), d)
	_ = cancel
	return c
}

// echoOK answers an echo request with its own marker.
func echoOK(ctx context.Context, p json.RawMessage, c *CallContext) (any, error) {
	return map[string]any{"out": "x"}, nil
}

// TestAgentStartFailsAtFirstPublishReturns: a Start whose first publication fails returns its
// error — it does not hang on a dead run's heartbeat, and it leaves nothing armed.
func TestAgentStartFailsAtFirstPublishReturns(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "alpha", "inbox"), 0o755)
	os.Chmod(filepath.Join(root, "alpha"), 0o500)
	t.Cleanup(func() { os.Chmod(filepath.Join(root, "alpha"), 0o755) })
	a := newTestAgent(t, root, "alpha", nil)
	var err error
	ok := within(5*time.Second, func() { err = a.Start(ctxN(2 * time.Second)) })
	if !ok {
		t.Fatal("Start hung on its first publication failure")
	}
	if err == nil {
		t.Fatal("Start succeeded though its first publication failed")
	}
	if a.cur().running.Load() {
		t.Error("a failed Start left the agent running")
	}
}

// TestAgentSecondStartBadConfigReturns: a Start that fails at the configuration returns its error.
// In the current lifecycle, one run is one agentState, published once through one atomic.Pointer, and
// the lifecycle gate serializes Start against the stops. A config failure happens before the new run is
// published, so a second Start leaves no half-started run: the previous run is already stopped and the
// gate is released when Start returns. A weak pin for the lifecycle (the strong pins are the
// running-rollback and the stop-during-start tests); it guards that a failed Start returns its error
// and does not hang.
func TestAgentSecondStartBadConfigReturns(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := a.Stop(startCtx(t), StopOptions{}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	a.shared.opts.Config.Sync = "bogus"
	var err error
	ok := within(5*time.Second, func() { err = a.Start(ctxN(2 * time.Second)) })
	if !ok {
		t.Fatal("the second Start hung on a bad configuration")
	}
	if err == nil {
		t.Fatal("the second Start succeeded with a bad configuration")
	}
}

// TestAgentStopWritesAbandonLinesBeforeInactive: D-9 (v1.11) — the stop waits, within its ctx, for the
// outbound exchanges it abandoned, so their outreq lines come before the inactive, as in TS. Over the
// probe's 20 stops × 3 in-flight requests, zero abandoned lines may come after the inactive: each
// exchange writes its outreq line before its deferred inflight.Done(), so the stop's wait is a
// happens-before edge over the line, not a race we usually win. A stop that cannot finish returns
// through its ctx (D-9's other half), so the test is bounded.
func TestAgentStopWritesAbandonLinesBeforeInactive(t *testing.T) {
	const (
		stopIterations = 20
		inFlightReqs   = 3
		settleSleep    = 50 * time.Millisecond
		startTimeout   = 5 * time.Second
		stopTimeout    = 10 * time.Second
		requestTimeout = 60 * time.Second
		busyBound      = 5 * time.Second
	)
	after, total := 0, 0
	for i := 0; i < stopIterations; i++ {
		root := t.TempDir()
		a := newTestAgent(t, root, "alpha", nil)
		b := newTestAgent(t, root, "beta", nil)
		release := make(chan struct{})
		if err := b.Capability(echoCap(), func(ctx context.Context, p json.RawMessage, c *CallContext) (any, error) {
			<-release
			return map[string]any{"out": "x"}, nil
		}); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		if err := a.Start(startCtx(t)); err != nil {
			t.Fatalf("Start(alpha): %v", err)
		}
		if err := b.Start(startCtx(t)); err != nil {
			t.Fatalf("Start(beta): %v", err)
		}
		back := make(chan struct{}, inFlightReqs)
		for j := 0; j < inFlightReqs; j++ {
			go func() {
				a.Request(context.Background(), "beta", "echo", map[string]any{"in": "x"}, RequestOptions{Timeout: requestTimeout})
				back <- struct{}{}
			}()
		}
		waitFor(t, busyBound, "beta to turn busy", func() bool { return statusState(t, root, "beta") == "busy" })
		time.Sleep(settleSleep)
		var stopErr error
		if !within(stopTimeout, func() { stopErr = a.Stop(ctxN(stopTimeout), StopOptions{}) }) {
			t.Fatalf("stop %d did not return within its ctx", i)
		}
		if stopErr != nil {
			t.Fatalf("stop %d: %v", i, stopErr)
		}
		for j := 0; j < inFlightReqs; j++ {
			<-back
		}
		read, err := ReadDiarium(filepath.Join(root, "alpha"))
		if err != nil {
			t.Fatalf("diarium: %v", err)
		}
		seen := false
		for _, line := range read.Lines {
			if line.Event == "status" && string(line.EventData) == `"inactive"` {
				seen = true
			}
			if line.Event == "outreq" && strings.Contains(string(line.EventData), `"final_status":"abandoned"`) {
				total++
				if seen {
					after++
				}
			}
		}
		close(release)
		if err := b.Stop(ctxN(stopTimeout), StopOptions{Force: true}); err != nil {
			t.Fatalf("Stop(beta) %d: %v", i, err)
		}
	}
	if after != 0 {
		t.Fatalf("%d of %d abandoned lines came after inactive (want 0)", after, total)
	}
}

// TestAgentSecondStartRootGoneReturns: a second Start whose root is gone returns its error.
func TestAgentSecondStartRootGoneReturns(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "tree")
	a := newTestAgent(t, sub, "alpha", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := a.Stop(startCtx(t), StopOptions{}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	os.RemoveAll(sub)
	os.WriteFile(sub, []byte("x"), 0o644) // the root is now a file
	var err error
	ok := within(5*time.Second, func() { err = a.Start(ctxN(2 * time.Second)) })
	if !ok {
		t.Fatal("the second Start hung with a missing root")
	}
	if err == nil {
		t.Fatal("the second Start succeeded with a missing root")
	}
}

// TestAgentRestartFailsAtStartClosesDone: a restart whose Start fails closes Done — nobody is left
// to bring the agent back, so a program selecting on Done must not wait forever.
func TestAgentRestartFailsAtStartClosesDone(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	b := newTestAgent(t, root, "beta", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	if _, err := b.Command(ctxN(5*time.Second), "alpha", "restart", map[string]any{"delay": 1}); err != nil {
		t.Fatalf("Command(restart): %v", err)
	}
	waitFor(t, 10*time.Second, "alpha to stop for the restart", func() bool { return !a.cur().running.Load() })
	folder := filepath.Join(root, "alpha")
	os.Chmod(folder, 0o500) // the restart's first publication now fails
	t.Cleanup(func() { os.Chmod(folder, 0o755) })
	waitFor(t, 10*time.Second, "Done to close after the failed restart", func() bool { return doneOf(a) })
	if a.cur().running.Load() {
		t.Error("the agent is running after a failed restart")
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("status after a failed restart: %q, want inactive", got)
	}
}

// TestAgentStartCancelledDuringAnnounceLeavesInactive: a Start cancelled during its announce delay
// leaves the agent inactive, not idle: a published idle must not outlive the run.
func TestAgentStartCancelledDuringAnnounceLeavesInactive(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.Config.AnnounceDelay = time.Second })
	err := a.Start(ctxN(100 * time.Millisecond))
	if err == nil {
		t.Fatal("Start succeeded though its ctx expired in the announce delay")
	}
	if a.cur().running.Load() {
		t.Error("a cancelled Start left the agent running")
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("status after a cancelled Start: %q, want inactive (a published idle must not outlive the run)", got)
	}
}

// TestAgentStopDuringStartNoCrash: a Stop that races a Start, from any side, leaves nothing armed
// and crashes nothing.
func TestAgentStopDuringStartNoCrash(t *testing.T) {
	const runs = 100
	for i := 0; i < runs; i++ {
		root := t.TempDir()
		a := newTestAgent(t, root, "alpha", nil)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); _ = a.Start(ctxN(5 * time.Second)) }()
		time.Sleep(time.Duration(rand.Intn(300)) * time.Microsecond)
		if err := a.Stop(ctxN(5*time.Second), StopOptions{Force: true}); err != nil {
			t.Fatalf("run %d: the Stop during Start: %v", i, err)
		}
		wg.Wait()
		if err := a.Stop(ctxN(5*time.Second), StopOptions{Force: true}); err != nil {
			t.Fatalf("run %d: the second Stop: %v", i, err)
		}
	}
}

// TestAgentRequestRacesStopRaceFree: the public API racing a stop is race-free and every call
// ends. The race detector is the verdict; this asserts the calls all return.
func TestAgentRequestRacesStopRaceFree(t *testing.T) {
	root := t.TempDir()
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Capability(echoCap(), echoOK); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	const workers, perWorker = 8, 400
	var wg sync.WaitGroup
	var n atomic.Int64
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				a.Request(ctxN(3*time.Second), "beta", "echo", map[string]any{"in": "x"}, RequestOptions{Timeout: time.Second})
				n.Add(1)
			}
		}()
	}
	time.Sleep(60 * time.Millisecond)
	if err := a.Stop(ctxN(10*time.Second), StopOptions{}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	wg.Wait()
	if n.Load() != workers*perWorker {
		t.Errorf("calls completed: %d, want %d", n.Load(), workers*perWorker)
	}
}

// TestAgentAPIDuringRestartRaceFree: the public API racing a peer-triggered restart is race-free:
// a program has no way to synchronise with a restart, and none of its calls may hang.
func TestAgentAPIDuringRestartRaceFree(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Capability(echoCap(), echoOK); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	calls := []func(){
		func() {
			a.Request(ctxN(2*time.Second), "beta", "echo", map[string]any{"in": "x"}, RequestOptions{Timeout: time.Second})
		},
		func() { a.Command(ctxN(2*time.Second), "beta", "nosuch", nil) },
		func() { a.Log("x") },
		func() { a.Diarium() },
		func() { a.Refresh(ctxN(2 * time.Second)) },
		func() { a.Agmen() },
	}
	for _, f := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					f()
				}
			}
		}()
	}
	const restarts = 4
	for i := 0; i < restarts; i++ {
		var err error
		for attempt := 0; attempt < 100; attempt++ { // a restart's down window refuses calls: retry until one lands
			_, err = b.Command(ctxN(5*time.Second), "alpha", "restart", map[string]any{})
			if err == nil || !strings.Contains(err.Error(), "not callable") {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("Command(restart %d): %v", i, err)
		}
		time.Sleep(300 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if err := a.Stop(ctxN(5*time.Second), StopOptions{Force: true}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestAgentPingDuringRestartRaceFree: the protocol commands, fired at a restarting agent, are
// race-free and end the agent cleanly.
func TestAgentPingDuringRestartRaceFree(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	b := newTestAgent(t, root, "beta", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	const rounds = 6
	for i := 0; i < rounds; i++ {
		for _, c := range []string{"ping", "enable", "restart", "ping"} {
			var p any
			switch c {
			case "enable":
				p = map[string]any{"enabled": true}
			case "restart":
				p = map[string]any{}
			} // ping takes null
			// A ping that lands in the restart's down window is refused by the callability rules —
			// that is the protocol working, not a defect; any other error is one.
			if _, err := b.Command(ctxN(5*time.Second), "alpha", c, p); err != nil && !strings.Contains(err.Error(), "not callable") {
				t.Fatalf("Command(%s, round %d): %v", c, i, err)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := a.Stop(ctxN(5*time.Second), StopOptions{Force: true}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestAgentStopDuringRestartDelayLeavesDown: a stop during a restart's delay cancels the restart
// and closes Done — the agent stays down.
func TestAgentStopDuringRestartDelayLeavesDown(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	b := newTestAgent(t, root, "beta", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	if _, err := b.Command(ctxN(5*time.Second), "alpha", "restart", map[string]any{"delay": 2}); err != nil {
		t.Fatalf("Command(restart): %v", err)
	}
	waitFor(t, 10*time.Second, "alpha to stop for the restart", func() bool { return !a.cur().running.Load() })
	if err := a.Stop(startCtx(t), StopOptions{}); err != nil {
		t.Fatalf("the Stop during the delay: %v", err)
	}
	if !doneOf(a) {
		t.Fatal("Done did not close: a stop during a restart's delay closes it")
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("status after the stop: %q, want inactive", got)
	}
	time.Sleep(2500 * time.Millisecond) // past the delay the restart was in
	if a.cur().running.Load() {
		t.Error("the agent came back on its own: the stop must cancel the pending restart")
	}
}

// TestAgentStopWaitsForStopCommand: a Stop that finds a stop command in flight waits for it at the
// gate: it returns only once the command's stop has finished, so inactive is on disk the moment it
// returns, and it took the time the first stop needed.
func TestAgentStopWaitsForStopCommand(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := a.Capability(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	go func() {
		_, _ = b.Request(context.Background(), "alpha", "echo", map[string]any{"in": "x"}, RequestOptions{Timeout: 30 * time.Second})
	}()
	waitFor(t, 10*time.Second, "alpha to be busy with the in-flight request", func() bool {
		return statusState(t, root, "alpha") == "busy"
	})
	if _, err := b.Command(ctxN(5*time.Second), "alpha", "stop", map[string]any{"force": false}); err != nil {
		t.Fatalf("Command(stop): %v", err)
	}
	waitFor(t, 10*time.Second, "the stop command's handler to stop the heartbeat",
		func() bool { return heartbeatStopped(a) })
	t0 := time.Now()
	if err := a.Stop(ctxN(10*time.Second), StopOptions{}); err != nil {
		t.Fatalf("the concurrent Stop: %v", err)
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("the Stop returned before inactive was on disk: status %q", got)
	}
	if elapsed := time.Since(t0); elapsed < time.Second {
		t.Errorf("the Stop returned in %s: it must wait for the stop command's drain", elapsed)
	}
	if !doneOf(a) {
		t.Fatal("Done did not close")
	}
}

// TestAgentStopAndRestartCommandsSettled: stop and restart commands, in either order, settle the
// agent: stop-then-restart ends running, restart-then-stop ends down with Done closed — never
// half-stopped, never a panic.
func TestAgentStopAndRestartCommandsSettled(t *testing.T) {
	orders := [][2]string{{"stop", "restart"}, {"restart", "stop"}}
	for _, order := range orders {
		for i := 0; i < 2; i++ {
			root := t.TempDir()
			a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
			b := newTestAgent(t, root, "beta", nil)
			if err := a.Start(startCtx(t)); err != nil {
				t.Fatalf("Start(alpha): %v", err)
			}
			if err := b.Start(startCtx(t)); err != nil {
				t.Fatalf("Start(beta): %v", err)
			}
			for _, c := range order {
				p := map[string]any{}
				if c == "restart" {
					p["delay"] = 1
				}
				var err error
				for attempt := 0; attempt < 100; attempt++ { // the down window between a stop and a start refuses calls
					_, err = b.Command(ctxN(5*time.Second), "alpha", c, p)
					if err == nil || !strings.Contains(err.Error(), "not callable") {
						break
					}
					time.Sleep(50 * time.Millisecond)
				}
				if err != nil {
					t.Fatalf("Command(%s): %v", c, err)
				}
			}
			if order[1] == "restart" {
				waitFor(t, 10*time.Second, "the agent to come back after the restart",
					func() bool { return a.cur().running.Load() })
			} else {
				waitFor(t, 10*time.Second, "Done to close after the final stop", func() bool { return doneOf(a) })
			}
		}
	}
}

// TestAgentRetryAfterStatusWriteFailed: a stop whose C1 status write fails is retried, and the
// retry lands: status.json is inactive, the diarium has the event, and the announce went out —
// not the old mix of a stale status, an announce, and no event.
func TestAgentRetryAfterStatusWriteFailed(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	folder := filepath.Join(root, "alpha")
	os.Chmod(folder, 0o500)
	t.Cleanup(func() { os.Chmod(folder, 0o755) })
	if err := a.Stop(ctxN(3*time.Second), StopOptions{}); err == nil {
		t.Fatal("the first Stop succeeded; the test needs a failed C1")
	}
	if !a.cur().running.Load() {
		t.Fatal("a failed C1 ended the agent; it must stay running for the retry")
	}
	os.Chmod(folder, 0o755)
	if err := a.Stop(ctxN(3*time.Second), StopOptions{}); err != nil {
		t.Fatalf("the retry: %v", err)
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("status after the retry: %q, want inactive", got)
	}
	states := diariumStates(t, root, "alpha")
	last := ""
	for _, s := range states {
		last = s
	}
	if last != "inactive" {
		t.Errorf("the last status event in the diarium: %q, want inactive", last)
	}
	ann, err := os.ReadFile(filepath.Join(root, "nuntium", "alpha.n.json"))
	if err != nil {
		t.Fatalf("the announce after the retry: %v", err)
	}
	if !strings.Contains(string(ann), "inactive") {
		t.Errorf("the announce: %q, want the inactive text", ann)
	}
}

// TestAgentRetryWhileFirstDrains: a Stop whose ctx expires in the grace drain returns its ctx
// error and keeps draining in the background; a Stop after the drain has settled takes the gate
// and finishes the job. (The interleaving where the second Stop's receiver stop reads the inbox
// while the first performStop is still writing it is a receiver race this round does not own: the
// flag the receiver's author introduces removes the read.)
func TestAgentRetryWhileFirstDrains(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	cancelled := make(chan struct{})
	if err := a.Capability(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	go func() {
		_, _ = b.Request(context.Background(), "alpha", "echo", map[string]any{"in": "x"}, RequestOptions{Timeout: 30 * time.Second})
	}()
	waitFor(t, 10*time.Second, "alpha to be busy with the in-flight request", func() bool {
		return statusState(t, root, "alpha") == "busy"
	})
	if err := a.Stop(ctxN(100*time.Millisecond), StopOptions{}); err == nil {
		t.Fatal("the first Stop succeeded; the test needs a ctx that expires in the drain")
	}
	waitFor(t, 10*time.Second, "the drain to cancel the in-flight handler", func() bool {
		select {
		case <-cancelled:
			return true
		default:
			return false
		}
	})
	if err := a.Stop(ctxN(10*time.Second), StopOptions{}); err != nil {
		t.Fatalf("the second Stop: %v", err)
	}
	if a.cur().running.Load() {
		t.Error("the agent is running after the second Stop")
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("status after the second Stop: %q, want inactive", got)
	}
	if !doneOf(a) {
		t.Fatal("Done did not close")
	}
}

// TestAgentCallbackReentryMatrix: any API may be called from a callback, and none deadlocks: the
// dispatch runs no callback under a mutex, so a re-entrant event joins the queue instead.
func TestAgentCallbackReentryMatrix(t *testing.T) {
	cases := []struct {
		name string
		call func(a *Agent, root string)
	}{
		{"Log", func(a *Agent, root string) { a.Log("x") }},
		{"Agmen", func(a *Agent, root string) { a.Agmen() }},
		{"Diarium", func(a *Agent, root string) { a.Diarium() }},
		{"Refresh", func(a *Agent, root string) { a.Refresh(ctxN(2 * time.Second)) }},
		{"Request", func(a *Agent, root string) {
			a.Request(ctxN(2*time.Second), "beta", "echo", map[string]any{"in": "x"}, RequestOptions{Timeout: time.Second})
		}},
		{"Command", func(a *Agent, root string) { a.Command(ctxN(2*time.Second), "beta", "ping", nil) }},
		{"Fail", func(a *Agent, root string) { a.Fail("x") }},
		{"Stop", func(a *Agent, root string) { a.Stop(ctxN(time.Second), StopOptions{}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			b := newTestAgent(t, root, "beta", nil)
			if err := b.Capability(echoCap(), echoOK); err != nil {
				t.Fatalf("Capability: %v", err)
			}
			if err := b.Start(startCtx(t)); err != nil {
				t.Fatalf("Start(beta): %v", err)
			}
			a := newTestAgent(t, root, "alpha", nil)
			ran := make(chan struct{})
			var once sync.Once
			a.OnEvent(func(e Event) {
				if _, ok := e.(AnnounceEvent); ok {
					once.Do(func() {
						tc.call(a, root)
						close(ran) // only once the call returned
					})
				}
			})
			go a.Start(startCtx(t))
			select {
			case <-ran:
			case <-time.After(5 * time.Second):
				t.Fatalf("the callback calling %s never returned: it deadlocked", tc.name)
			}
		})
	}
}

// TestAgentCyclesLeakNothing: Start/Stop cycles leave nothing behind: the goroutine and file
// descriptor counts are flat, and the agent ends inactive.
func TestAgentCyclesLeakNothing(t *testing.T) {
	fds := func() int { e, _ := os.ReadDir("/proc/self/fd"); return len(e) }
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := a.Stop(startCtx(t), StopOptions{}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	g0, f0 := runtime.NumGoroutine(), fds()
	const cycles = 40
	for i := 0; i < cycles; i++ {
		if err := a.Start(startCtx(t)); err != nil {
			t.Fatalf("cycle %d Start: %v", i, err)
		}
		if err := a.Stop(startCtx(t), StopOptions{}); err != nil {
			t.Fatalf("cycle %d Stop: %v", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if g := runtime.NumGoroutine(); g > g0+2 {
		t.Errorf("goroutines %d→%d: a cycle leaked", g0, g)
	}
	if f := fds(); f > f0+2 {
		t.Errorf("file descriptors %d→%d: a cycle leaked", f0, f)
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("status after the cycles: %q, want inactive", got)
	}
}

// TestAgentTickAfterStopLeavesFileAlone: a tick after Stop writes nothing — the status file's
// bytes and mtime come back as they were.
func TestAgentTickAfterStopLeavesFileAlone(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := a.Stop(startCtx(t), StopOptions{}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	f := filepath.Join(root, "alpha", "status.json")
	s0, err := os.Stat(f)
	if err != nil {
		t.Fatalf("stat status.json: %v", err)
	}
	r0, err := os.ReadFile(f)
	if err != nil {
		t.Fatalf("read status.json: %v", err)
	}
	time.Sleep(5 * time.Millisecond) // the mtime must differ if a tick writes
	if err := a.tick(); err != nil {
		t.Fatalf("tick after Stop: %v", err)
	}
	s1, err := os.Stat(f)
	if err != nil {
		t.Fatalf("stat status.json after the tick: %v", err)
	}
	r1, err := os.ReadFile(f)
	if err != nil {
		t.Fatalf("read status.json after the tick: %v", err)
	}
	if s0.ModTime() != s1.ModTime() || !bytes.Equal(r0, r1) {
		t.Errorf("a tick wrote after inactive: %s", r1)
	}
}

// TestAgentStopBoundedWithStuckTick: with a tick stuck on the publication mutex, Stop returns its
// ctx error — the heartbeat is stopped and waited for before C1, and that wait is bounded by the
// ctx.
func TestAgentStopBoundedWithStuckTick(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.Config.HeartbeatInterval = 10 * time.Millisecond })
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	a.cur().pubMu.Lock()               // a stuck publication: the next tick blocks on it, inside the heartbeat
	time.Sleep(100 * time.Millisecond) // let a tick reach the mutex
	var err error
	ok := within(2*time.Second, func() { err = a.Stop(ctxN(200*time.Millisecond), StopOptions{}) })
	a.cur().pubMu.Unlock()
	if !ok {
		t.Fatal("Stop ignored its ctx with a tick stuck on the publication mutex")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if err := a.Stop(startCtx(t), StopOptions{}); err != nil {
		t.Fatalf("the second Stop: %v", err)
	}
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Fatalf("status %q, want inactive", got)
	}
}

// TestAgentAbortStartBoundedByCtx: the start-failure cleanup is bounded by its context — a run
// whose heartbeat never ends and whose discovery goroutine never returns is dropped within the
// bound, and the lifecycle gate is released afterwards: a subsequent Stop returns instead of
// blocking in acquire. An unbounded wait here would hold the gate forever and brick the agent.
func TestAgentAbortStartBoundedByCtx(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)

	// A run whose heartbeat never ends (hbDone never closes) and whose discovery goroutine never
	// returns: the two waits the cleanup must bound.
	st := &agentState{agentShared: a.shared, id: "alpha", current: StatusInactive}
	st.view.Store(&peerSet{views: map[AgentID]AgentView{}, specs: map[AgentID]*Spec{}})
	st.hb = newHeartbeat(time.Second, func() error { return nil })
	st.hbDone = make(chan struct{}) // never closed: the heartbeat's goroutine never ends
	st.discArmed = true
	st.discStop = make(chan struct{})
	never := make(chan struct{})
	st.wg.Go(func() { <-never }) // the discovery goroutine never returns

	// Start holds the lifecycle gate while it cleans up a failed run; model that.
	if err := a.acquire(startCtx(t)); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	expired, cancel := context.WithCancel(context.Background())
	cancel() // an already-expired context: the cleanup's bounds run out at once
	aborted := false
	ok := within(5*time.Second, func() { a.abortStart(expired, st); aborted = true })
	a.release() // Start's deferred release, now that the cleanup has returned
	if !ok || !aborted {
		t.Fatal("the start-failure cleanup did not return within 5s: its waits are not bounded by ctx")
	}

	// The gate is released: a subsequent Stop returns instead of blocking in acquire.
	var stopErr error
	if !within(5*time.Second, func() { stopErr = a.Stop(ctxN(2*time.Second), StopOptions{}) }) {
		t.Fatal("Stop blocked in acquire: the lifecycle gate was not released")
	}
	if stopErr != nil {
		t.Fatalf("Stop after the bounded cleanup: %v", stopErr)
	}
}

// TestAgentStopDiscoveryWaitBoundedByCtx: the stop's wait for the discovery goroutine is bounded
// by the caller's context — a discovery that never returns (stuck in a blocking listener on a lost
// nuntium) does not hold the stop: Stop returns its ctx's deadline.
func TestAgentStopDiscoveryWaitBoundedByCtx(t *testing.T) {
	root := t.TempDir()
	block := make(chan struct{})
	defer close(block) // release the blocking listener so the discovery goroutine can end
	var stuck atomic.Bool
	a := newTestAgent(t, root, "alpha", nil)
	a.OnEvent(func(e Event) {
		if lg, ok := e.(LogEvent); ok && strings.Contains(lg.Message, "watch lost") {
			stuck.Store(true)
			<-block // block on the loss event: the discovery goroutine never returns
		}
	})
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Lose the own nuntium: the discovery goroutine emits the loss and blocks in the listener.
	os.RemoveAll(a.cur().folders.nuntium)
	waitFor(t, 10*time.Second, "the discovery goroutine to block on the loss", stuck.Load)
	var err error
	ok := within(5*time.Second, func() { err = a.Stop(ctxN(300*time.Millisecond), StopOptions{}) })
	if !ok {
		t.Fatal("Stop did not return within 5s: the discovery wait is not bounded by ctx")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("Stop err = %v, want DeadlineExceeded", err)
	}
}

// TestAgentCommandStopBound: a stop command's bound is the protocol's grace plus the stop's own
// 30 s — and a huge grace clamps nothing.
func TestAgentCommandStopBound(t *testing.T) {
	if commandStopBound != 30*time.Second {
		t.Errorf("commandStopBound = %s, want 30s", commandStopBound)
	}
	if got := commandStopBoundFor(30 * time.Second); got != 60*time.Second {
		t.Errorf("commandStopBoundFor(30s) = %s, want 60s", got)
	}
	if got := commandStopBoundFor(0); got != 30*time.Second {
		t.Errorf("commandStopBoundFor(0) = %s, want 30s", got)
	}
	if got := commandStopBoundFor(time.Hour); got != time.Hour+30*time.Second {
		t.Errorf("commandStopBoundFor(1h) = %s, want 1h30m: a huge grace clamps nothing", got)
	}
}

// TestAgentStopCommandShrunkGrace: a stop command, with a shrunk grace and work that outlives it,
// completes and leaves the agent inactive with Done closed: the command's bound covers the
// drain, and nothing is left half-stopped.
func TestAgentStopCommandShrunkGrace(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) {
		o.AcceptsCommandsFrom = []string{"beta"}
		o.Config.StopGrace = 300 * time.Millisecond
	})
	if err := a.Capability(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	go func() {
		_, _ = b.Request(context.Background(), "alpha", "echo", map[string]any{"in": "x"}, RequestOptions{Timeout: 30 * time.Second})
	}()
	waitFor(t, 10*time.Second, "alpha to be busy with the in-flight request", func() bool {
		return statusState(t, root, "alpha") == "busy"
	})
	if _, err := b.Command(ctxN(5*time.Second), "alpha", "stop", map[string]any{"force": false}); err != nil {
		t.Fatalf("Command(stop): %v", err)
	}
	waitFor(t, 15*time.Second, "Done to close", func() bool { return doneOf(a) })
	if got := statusState(t, root, "alpha"); got != "inactive" {
		t.Errorf("status after the stop command: %q, want inactive", got)
	}
}

// TestAgentE21OwnInboxLossFailsInOrder: a lost own inbox is D-22 — the LogEvent goes on the stream
// first, then the agent fails (status → error), and the diarium keeps its log line. The fail comes
// from the failWatch hook, not from an IgnoredEvent-matching hack.
func TestAgentE21OwnInboxLossFailsInOrder(t *testing.T) {
	root := t.TempDir()
	var mu sync.Mutex
	var events []Event
	a := newTestAgent(t, root, "alpha", nil)
	a.OnEvent(func(e Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	inbox := filepath.Join(root, "alpha", "inbox")
	if err := os.Rename(inbox, inbox+".gone"); err != nil {
		t.Fatal(err)
	}
	// The agent fails (D-22): the status goes to error.
	waitFor(t, 5*time.Second, "the agent to fail after the inbox loss", func() bool {
		return statusState(t, root, "alpha") == "error"
	})
	// The LogEvent is on the stream and comes before the failure (the StatusEvent{error}).
	mu.Lock()
	defer mu.Unlock()
	logIdx, errIdx := -1, -1
	for i, e := range events {
		if le, ok := e.(LogEvent); ok && strings.HasPrefix(le.Message, "watch lost "+inbox+":") {
			logIdx = i
		}
		if se, ok := e.(StatusEvent); ok && se.State == StatusError && errIdx == -1 {
			errIdx = i
		}
	}
	if logIdx == -1 {
		t.Fatalf("no LogEvent for the inbox loss on the stream: %+v", events)
	}
	if errIdx == -1 {
		t.Fatalf("no StatusEvent{error} on the stream: %+v", events)
	}
	if logIdx > errIdx {
		t.Fatalf("the LogEvent (index %d) came after the failure (index %d): D-22 wants the log first", logIdx, errIdx)
	}
	// The diarium keeps its log line.
	read, err := ReadDiarium(filepath.Join(root, "alpha"))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, l := range read.Lines {
		if l.Event == "log" {
			var msg string
			if json.Unmarshal(l.EventData, &msg) == nil && strings.HasPrefix(msg, "watch lost "+inbox+":") {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("the diarium has no log line for the inbox loss")
	}
}

// TestAgentRestartPendingDuringOperatorStart pins matrix row 11.14: a `restart` with a delay that is
// pending (the agent is down, the delay is counting) meets an operator `Start` in the delay. After the
// delay there must be exactly one live run, `Done` still open, and no error logged. Without
// `startLocked` closing `restartAbort`, the delayed restart hits "already started" and then closes
// `Done` while the agent runs — this test catches both.
func TestAgentRestartPendingDuringOperatorStart(t *testing.T) {
	const delaySeconds = 2.0            // the restart command's delay, in seconds (the schema's number)
	const settleBound = 3 * time.Second // the delay (2s) + a 1s margin: the restart loop has had its turn

	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	b := newTestAgent(t, root, "beta", nil)
	if err := b.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}

	// Observe the log through the hook the harness already has: capture every LogEvent's message.
	var logMu sync.Mutex
	var logs []string
	a.OnEvent(func(e Event) {
		if le, ok := e.(LogEvent); ok {
			logMu.Lock()
			logs = append(logs, le.Message)
			logMu.Unlock()
		}
	})

	// 1. Cause a restart with a delay: alpha goes down and the delay counts.
	if _, err := b.Command(startCtx(t), "alpha", "restart", map[string]any{"delay": delaySeconds}); err != nil {
		t.Fatalf("Command(restart): %v", err)
	}
	waitFor(t, 10*time.Second, "alpha to stop for the delayed restart", func() bool { return !a.cur().running.Load() })

	// 2. During the delay, an operator Start from the public API meets the pending restart.
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("the operator Start during the restart delay failed: %v", err)
	}

	// 3. After the delay, the restart loop has had its turn: assert one run, Done open, no error.
	time.Sleep(settleBound) // the delay (2s) + a margin: the restart loop has run and finished
	if !a.cur().running.Load() {
		t.Fatalf("after the delay the agent is not running: the operator Start must leave one live run")
	}
	select {
	case <-a.Done():
		t.Fatalf("Done is closed while the agent runs: an operator Start in the delay must not let the restart close it")
	default:
	}
	logMu.Lock()
	defer logMu.Unlock()
	for _, m := range logs {
		if strings.Contains(m, "already started") {
			t.Fatalf("an 'already started' error was logged: %q", m)
		}
	}
}
