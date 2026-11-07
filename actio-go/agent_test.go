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
	"os"
	"path/filepath"
	"strings"
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
	good, err := New(Options{ID: "alpha", Namespaces: []*string{nil, ptrString("*")}})
	if err != nil {
		t.Fatalf("New(alpha): %v", err)
	}
	if good == nil || good.st == nil {
		t.Fatalf("New(alpha) returned no agent")
	}
	// New arms nothing: no folders, no watcher, no goroutine.
	if _, err := os.Stat(filepath.Join(good.st.folders.root, "alpha")); good.st.folders.root != "" && !os.IsNotExist(err) {
		t.Errorf("New created folders, it must not")
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
	a.st.config.Sync = "" // the next Start reloads the configuration from the (valid) default
	a2, err := New(Options{ID: "alpha", AcceptsCommandsFrom: []string{}, Config: testConfigWithRoot(root)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a2.Start(startCtx(t)); err != nil {
		t.Fatalf("a corrected Start: %v", err)
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
	if !a.st.running.Load() {
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
