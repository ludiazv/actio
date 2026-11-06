package actio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// testCapability is the one capability the fake peers offer; its result schema is what the
// result-check tests run against.
func testCapability() Capability {
	return Capability{
		Name:        "cap",
		Description: "test capability",
		Parameters:  json.RawMessage(`{}`),
		Actio: CapabilityActio{
			Result: json.RawMessage(`{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"]}`),
		},
	}
}

// reqTest is one requestor wired on a real watcher and a fake discovery, over a temp root.
type reqTest struct {
	t        *testing.T
	root     string
	src      AgentID
	dst      AgentID
	r        *requestor
	events   chan Event
	inbox    string
	agentDir string
	logs     *logSink
}

// logSink is the harness's capture of the agent's log path, for the log assertions.
type logSink struct {
	mu sync.Mutex
	ls []string
}

func (s *logSink) logf(format string, a ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ls = append(s.ls, fmt.Sprintf(format, a...))
}

func (s *logSink) lines() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.ls))
	copy(out, s.ls)
	return out
}

func (s *logSink) count(fragment string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, l := range s.ls {
		if strings.Contains(l, fragment) {
			n++
		}
	}
	return n
}

func newReqTest(t *testing.T, src, dst AgentID, events chan Event) *reqTest {
	t.Helper()
	root := t.TempDir()
	agentDir := filepath.Join(root, string(src))
	inbox := filepath.Join(root, string(dst), FolderInbox)
	for _, dir := range []string{agentDir, inbox, filepath.Join(root, FolderForum)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	w, err := newWatcher()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	files := newFileIO(&nopProvider{})
	peer := Spec{
		ID:                  dst,
		ActioVersion:        ProtocolVersion,
		Datetime:            formatISO(time.Now()),
		Description:         "test peer",
		AcceptsCommandsFrom: []string{"*"},
		Capabilities:        []Capability{testCapability()},
		SecurityPosture:     "trusted",
	}
	logs := &logSink{}
	st := &agentState{
		id:     src,
		config: Config{Root: root, Sync: "none", CancelWaitTimeout: 300 * time.Millisecond, Clock: time.Now},
		folders: agentFolders{
			root:    root,
			agent:   agentDir,
			inbox:   filepath.Join(agentDir, FolderInbox),
			forum:   filepath.Join(root, FolderForum),
			nuntium: filepath.Join(root, FolderNuntium),
		},
		files:  files,
		sync:   &nopProvider{},
		diaria: newDiaria(files, agentDir, src),
		watch:  w,
		now:    time.Now,
		emit:   func(e Event) { events <- e },
		logf:   logs.logf,
		spec:   func() *Spec { return &Spec{ID: src, ActioVersion: ProtocolVersion, SecurityPosture: "trusted"} },
		state:  func() StatusState { return StatusIdle },
	}
	r := newRequestor(st)
	// The discovery seams, faked: the tests pin the requestor's behavior, not the scan's.
	r.scanFn = func(root string) (ScanResult, error) {
		return ScanResult{Agents: []DiscoveredAgent{{
			Folder: filepath.Join(root, string(dst)),
			Spec:   peer,
			Status: Status{ID: dst, Status: StatusIdle, Datetime: formatISO(time.Now()), CapabilityStatus: []CapabilityStatus{}},
		}}}, nil
	}
	r.callableFn = func(self *Spec, p DiscoveredAgent, now time.Time) *AgentView {
		caps := make(map[string]Capability, len(p.Spec.Capabilities))
		for _, c := range p.Spec.Capabilities {
			caps[c.Name] = c
		}
		return &AgentView{
			ID: p.Spec.ID, Folder: p.Folder, Spec: p.Spec, Status: p.Status,
			Health: HealthHealthy, Commands: true, Capabilities: caps,
		}
	}
	return &reqTest{t: t, root: root, src: src, dst: dst, r: r, events: events, inbox: inbox, agentDir: agentDir, logs: logs}
}

// answer writes a valid response file for rid into the destination inbox.
func (x *reqTest) answer(rid RID, body ResponseBody) {
	x.t.Helper()
	bodyRaw, err := json.Marshal(body)
	if err != nil {
		x.t.Fatal(err)
	}
	file := ResponseFile{
		Header:   Header{RID: rid, Src: x.src, Dst: x.dst, Datetime: formatISO(time.Now()), Type: TypeRequest, ActioVersion: ProtocolVersion},
		Response: bodyRaw,
	}
	raw, err := json.Marshal(file)
	if err != nil {
		x.t.Fatal(err)
	}
	writeFileAtomic(x.t, filepath.Join(x.inbox, string(rid)+".res.json"), raw)
}

func okBody() ResponseBody {
	return ResponseBody{TS: time.Now().Unix(), Nonce: newNonce(), Error: false, Cancellation: false, Result: json.RawMessage(`{"ok":true}`), Attachments: []string{}}
}

func cancelBody() ResponseBody {
	return ResponseBody{TS: time.Now().Unix(), Nonce: newNonce(), Error: false, Cancellation: true, Result: nil, Attachments: []string{}}
}

func errorBody(code int, msg string) ResponseBody {
	return ResponseBody{TS: time.Now().Unix(), Nonce: newNonce(), Error: true, Cancellation: false, Result: json.RawMessage(`{"code":` + itoa(code) + `,"msg":"` + msg + `"}`), Attachments: []string{}}
}

func itoa(n int) string { return strconv.Itoa(n) }

// writeFileAtomic writes data to path through a dot-file temp and a rename, as the protocol's
// writes are.
func writeFileAtomic(t *testing.T, path string, data []byte) {
	t.Helper()
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// listRequestRids names the request files (not the other envelopes) in inbox.
func listRequestRids(inbox string) []RID {
	entries, err := os.ReadDir(inbox)
	if err != nil {
		return nil
	}
	var rids []RID
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, ".") && strings.HasSuffix(name, ".json") &&
			!strings.Contains(name, ".res.") && !strings.Contains(name, ".can.") && !strings.Contains(name, ".con.") {
			rids = append(rids, RID(strings.TrimSuffix(name, ".json")))
		}
	}
	return rids
}

// waitForRequests blocks until n request files are in inbox.
func waitForRequests(t *testing.T, inbox string, n int) []RID {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if rids := listRequestRids(inbox); len(rids) >= n {
			return rids
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("only some of the %d request files appeared in %s", n, inbox)
	return nil
}

func waitForRequest(t *testing.T, inbox string) RID {
	t.Helper()
	return waitForRequests(t, inbox, 1)[0]
}

// waitForNewRequest blocks until a request file appears in inbox that is not in seen.
func waitForNewRequest(t *testing.T, inbox string, seen map[RID]bool) RID {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, rid := range listRequestRids(inbox) {
			if !seen[rid] {
				return rid
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no new request file appeared in " + inbox)
	return ""
}

// waitForFile blocks until path exists.
func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no file at " + path)
}

// settleGoroutines waits until the goroutine count is back at before.
func settleGoroutines(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines left: %d, want at most %d", runtime.NumGoroutine(), before)
}

// waitInotify blocks until the inotify instance count reaches want.
func waitInotify(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if inotifyCount(t) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("inotify instances: %d, want %d", inotifyCount(t), want)
}

// runRequest starts one request on its own goroutine with a cancellable ctx: if the test fails
// early, the cleanup cancels it, so the exchange ends through B4 instead of running forever.
func runRequest(t *testing.T, r *requestor, dst AgentID, name string, params any, opts RequestOptions) <-chan struct{} {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		_, _ = r.request(ctx, dst, name, params, opts)
		close(done)
	}()
	return done
}

// ---- the pinned tests ----

// Two requests to the same destination, one after the other and two at once: both answered, no
// deadlock, and one inotify instance for the whole agent.
func TestRequestSequentialAndConcurrentSameDestination(t *testing.T) {
	events := make(chan Event, 64)
	base := inotifyCount(t)
	x := newReqTest(t, "me", "peer", events)
	before := runtime.NumGoroutine()

	seen := map[RID]bool{}
	// One after the other.
	for i := 0; i < 2; i++ {
		done := runRequest(t, x.r, "peer", "cap", nil, RequestOptions{})
		rid := waitForNewRequest(t, x.inbox, seen)
		seen[rid] = true
		x.answer(rid, okBody())
		<-done
	}
	// Two at once to the same inbox: the two request files of this phase, not the earlier ones.
	doneA := runRequest(t, x.r, "peer", "cap", nil, RequestOptions{})
	doneB := runRequest(t, x.r, "peer", "cap", nil, RequestOptions{})
	deadline := time.Now().Add(10 * time.Second)
	var fresh []RID
	for time.Now().Before(deadline) && len(fresh) < 2 {
		for _, rid := range listRequestRids(x.inbox) {
			if !seen[rid] {
				seen[rid] = true
				fresh = append(fresh, rid)
			}
		}
		if len(fresh) < 2 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if len(fresh) < 2 {
		t.Fatal("the concurrent requests never both reached the inbox")
	}
	x.answer(fresh[0], okBody())
	x.answer(fresh[1], okBody())
	select {
	case <-doneA:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: the first concurrent request never ended")
	}
	select {
	case <-doneB:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: the second concurrent request never ended")
	}
	if got := inotifyCount(t); got != base+1 {
		t.Fatalf("inotify instances: %d, want one (the agent's watcher)", got)
	}
	settleGoroutines(t, before)
}

// The destination folder deleted mid-exchange: the loss path, an abandoned exchange, an
// IgnoredEvent, and no goroutine left.
func TestRequestDestinationFolderDeleted(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)
	before := runtime.NumGoroutine()

	done := runRequest(t, x.r, "peer", "cap", nil, RequestOptions{})
	waitForRequest(t, x.inbox)
	if err := os.RemoveAll(x.inbox); err != nil {
		t.Fatal(err)
	}
	<-done
	// The ignored report for the folder we can no longer watch.
	found := false
	deadline := time.Now().Add(2 * time.Second)
	for !found && time.Now().Before(deadline) {
		select {
		case ev := <-events:
			if ig, ok := ev.(IgnoredEvent); ok && ig.Path == x.inbox {
				found = true
			}
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	if !found {
		t.Fatal("no IgnoredEvent for the deleted destination inbox")
	}
	settleGoroutines(t, before)
}

// A timeout with no response: B4 writes the cancellation, the bounded wait is up, the exchange is
// abandoned, and the error is nil.
func TestRequestTimeoutAbandons(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)

	done := runRequest(t, x.r, "peer", "cap", nil, RequestOptions{Timeout: 400 * time.Millisecond})
	rid := waitForRequest(t, x.inbox)
	waitForFile(t, filepath.Join(x.inbox, string(rid)+".can.json"))
	<-done
	if _, err := os.Stat(filepath.Join(x.inbox, string(rid)+".con.json")); err == nil {
		t.Fatal("a confirmation was written for an abandoned exchange")
	}
	x.assertDiariumStatus(t, rid, FinalAbandoned)
}

// A cancelled ctx ends in a confirmation: the cancellation file is written, the cancellation
// response is captured, the confirmation goes out, and the Outcome says cancelled with a nil error.
func TestRequestCancelWaitsForConfirmation(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		_, _ = x.r.request(ctx, "peer", "cap", nil, RequestOptions{})
		close(done)
	}()
	rid := waitForRequest(t, x.inbox)
	cancel()
	waitForFile(t, filepath.Join(x.inbox, string(rid)+".can.json"))
	x.answer(rid, cancelBody())
	<-done
	if _, err := os.Stat(filepath.Join(x.inbox, string(rid)+".con.json")); err != nil {
		t.Fatalf("no confirmation after the cancellation response: %v", err)
	}
	x.assertDiariumStatus(t, rid, FinalCancelled)
}

// A cancelled ctx ending in a timeout: the cancellation is written, no response comes, the bounded
// wait is up, and the exchange is abandoned.
func TestRequestCancelWaitsThenTimesOut(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		_, _ = x.r.request(ctx, "peer", "cap", nil, RequestOptions{})
		close(done)
	}()
	rid := waitForRequest(t, x.inbox)
	cancel()
	waitForFile(t, filepath.Join(x.inbox, string(rid)+".can.json"))
	<-done
	if _, err := os.Stat(filepath.Join(x.inbox, string(rid)+".con.json")); err == nil {
		t.Fatal("a confirmation was written for an abandoned exchange")
	}
	x.assertDiariumStatus(t, rid, FinalAbandoned)
}

// A peer's error answer is not a Go error: 103 and any other code come back as a nil error with
// Outcome.Response.Error set.
func TestRequestPeerErrorAnswerIsNotAGoError(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)

	done := make(chan struct{})
	var out Outcome
	var err error
	go func() {
		out, err = x.r.request(context.Background(), "peer", "cap", nil, RequestOptions{})
		close(done)
	}()
	rid := waitForRequest(t, x.inbox)
	x.answer(rid, errorBody(CodeRequestorNotAllowed, "requestor_not_allowed: not listed in accepts_from"))
	<-done
	if err != nil {
		t.Fatalf("a peer's error answer is a Go error: %v", err)
	}
	if out.Response == nil || !out.Response.Error {
		t.Fatalf("the error answer is not in the outcome: %+v", out)
	}
	var er ErrorResult
	if jerr := json.Unmarshal(out.Response.Result, &er); jerr != nil {
		t.Fatal(jerr)
	}
	if er.Code == nil || *er.Code != CodeRequestorNotAllowed {
		t.Fatalf("the error code: %v, want %d", er.Code, CodeRequestorNotAllowed)
	}
	if out.FinalStatus != FinalConfirmed {
		t.Fatalf("final status: %s, want %s", out.FinalStatus, FinalConfirmed)
	}
}

// A result that violates the capability's result schema is a *ResultError.
func TestRequestResultSchemaViolationIsResultError(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)

	done := make(chan struct{})
	var err error
	go func() {
		_, err = x.r.request(context.Background(), "peer", "cap", nil, RequestOptions{})
		close(done)
	}()
	rid := waitForRequest(t, x.inbox)
	x.answer(rid, ResponseBody{TS: time.Now().Unix(), Nonce: newNonce(), Error: false, Cancellation: false, Result: json.RawMessage(`{"ok":"not a boolean"}`), Attachments: []string{}})
	<-done
	if err == nil {
		t.Fatal("no error for a result that violates the schema")
	}
	var re *ResultError
	if !errors.As(err, &re) {
		t.Fatalf("the error is not a *ResultError: %v", err)
	}
	if re.Response == nil || re.Response.Error {
		t.Fatalf("the ResultError lost the response: %+v", re)
	}
}

// The callability refusals are *CallError, and nothing is written.
func TestRequestCallabilityRefusals(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)
	ctx := context.Background()

	// A peer the rules discard.
	x.r.callableFn = func(*Spec, DiscoveredAgent, time.Time) *AgentView { return nil }
	if _, err := x.r.request(ctx, "peer", "cap", nil, RequestOptions{}); err == nil {
		t.Fatal("no error for a not-callable peer")
	} else {
		var ce *CallError
		if !errors.As(err, &ce) {
			t.Fatalf("the refusal is not a *CallError: %v", err)
		}
	}
	// A peer with no usable capability of that name.
	x.r.callableFn = func(*Spec, DiscoveredAgent, time.Time) *AgentView {
		return &AgentView{ID: "peer", Commands: true, Capabilities: map[string]Capability{}}
	}
	if _, err := x.r.request(ctx, "peer", "cap", nil, RequestOptions{}); err == nil {
		t.Fatal("no error for a capability that is not usable")
	} else {
		var ce *CallError
		if !errors.As(err, &ce) {
			t.Fatalf("the refusal is not a *CallError: %v", err)
		}
	}
	// A destination the scan does not find.
	x.r.scanFn = func(string) (ScanResult, error) { return ScanResult{}, nil }
	if _, err := x.r.request(ctx, "peer", "cap", nil, RequestOptions{}); err == nil {
		t.Fatal("no error for an unknown destination")
	} else {
		var ce *CallError
		if !errors.As(err, &ce) {
			t.Fatalf("the refusal is not a *CallError: %v", err)
		}
	}
	entries, _ := os.ReadDir(x.inbox)
	if len(entries) != 0 {
		t.Fatalf("a refusal left files in the inbox: %v", entries)
	}
}

// Call refuses a timeout of zero or less, and builds nothing.
func TestCallRefusesZeroTimeout(t *testing.T) {
	root := t.TempDir()
	for _, timeout := range []time.Duration{0, -time.Second} {
		_, err := Call(context.Background(), Config{Root: root}, "me", "peer", "cap", nil, RequestOptions{Timeout: timeout})
		if err == nil || !strings.Contains(err.Error(), "timeout") {
			t.Fatalf("Call with timeout %v: %v, want a timeout refusal", timeout, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "me")); err == nil {
		t.Fatal("a refused Call built the source folder")
	}
}

// Call and SendCommand close their watcher: one inotify instance per call, back to baseline after,
// on every exit path.
func TestCallClosesItsWatcher(t *testing.T) {
	base := inotifyCount(t)
	root := t.TempDir()
	peerInbox := filepath.Join(root, "peer", FolderInbox)
	if err := os.MkdirAll(peerInbox, 0o755); err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()

	type callResult struct {
		outcome Outcome
		err     error
	}
	res := make(chan callResult, 1)
	go func() {
		o, e := Call(context.Background(), Config{Root: root}, "me", "peer", "cap", nil, RequestOptions{Timeout: 10 * time.Second})
		res <- callResult{o, e}
	}()
	waitInotify(t, base+1)
	rid := waitForRequest(t, peerInbox)
	bodyRaw, _ := json.Marshal(okBody())
	file := ResponseFile{
		Header:   Header{RID: rid, Src: "me", Dst: "peer", Datetime: formatISO(time.Now()), Type: TypeRequest, ActioVersion: ProtocolVersion},
		Response: bodyRaw,
	}
	raw, _ := json.Marshal(file)
	writeFileAtomic(t, filepath.Join(peerInbox, string(rid)+".res.json"), raw)
	got := <-res
	if got.err != nil {
		t.Fatalf("Call: %v", got.err)
	}
	if got.outcome.FinalStatus != FinalConfirmed {
		t.Fatalf("Call final status: %s", got.outcome.FinalStatus)
	}

	cmdRid, err := SendCommand(context.Background(), Config{Root: root}, "me", "peer", "ping", nil)
	if err != nil {
		t.Fatalf("SendCommand: %v", err)
	}
	if cmdRid == "" {
		t.Fatal("SendCommand returned no rid")
	}
	if _, err := os.Stat(filepath.Join(peerInbox, string(cmdRid)+".json")); err != nil {
		t.Fatalf("the command file is missing: %v", err)
	}
	settleGoroutines(t, before)
	if got := inotifyCount(t); got != base {
		t.Fatalf("inotify instances after Call/SendCommand: %d, want %d", got, base)
	}
}

// 200 concurrent requests to one destination, half of them cancelled: every one ends, with the
// right final status, one inotify instance throughout, and no goroutine left.
func TestRequestTwoHundredConcurrentHalfCancel(t *testing.T) {
	const n = 200
	events := make(chan Event, 1024)
	base := inotifyCount(t)
	x := newReqTest(t, "me", "peer", events)
	before := runtime.NumGoroutine()

	ctxs := make([]context.CancelFunc, n)
	outcomes := make([]Outcome, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ctxs[i] = cancel
		wg.Go(func() {
			outcomes[i], errs[i] = x.r.request(ctx, "peer", "cap", nil, RequestOptions{Timeout: 60 * time.Second})
		})
	}
	rids := waitForRequests(t, x.inbox, n)
	for i := 0; i < n/2; i++ {
		ctxs[i]()
	}
	// Every cancelled exchange writes its cancellation file; the hundred may not be done at once.
	deadline := time.Now().Add(60 * time.Second)
	for {
		cancelled := 0
		for _, rid := range rids {
			if _, statErr := os.Stat(filepath.Join(x.inbox, string(rid)+".can.json")); statErr == nil {
				cancelled++
			}
		}
		if cancelled == n/2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cancellation files: %d, want %d", cancelled, n/2)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := inotifyCount(t); got != base+1 {
		t.Fatalf("inotify instances under load: %d, want one", got)
	}
	for _, rid := range rids {
		if _, statErr := os.Stat(filepath.Join(x.inbox, string(rid)+".can.json")); statErr == nil {
			x.answer(rid, cancelBody())
		} else {
			x.answer(rid, okBody())
		}
	}
	wg.Wait()
	seenCancelled, seenConfirmed := 0, 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("request %d: %v", i, errs[i])
		}
		switch outcomes[i].FinalStatus {
		case FinalCancelled:
			seenCancelled++
		case FinalConfirmed:
			seenConfirmed++
		default:
			t.Fatalf("request %d: final status %q", i, outcomes[i].FinalStatus)
		}
	}
	if seenCancelled != n/2 || seenConfirmed != n/2 {
		t.Fatalf("final statuses: %d cancelled, %d confirmed; want %d of each", seenCancelled, seenConfirmed, n/2)
	}
	if got := x.r.outboundCount(); got != 0 {
		t.Fatalf("outbound count after the load: %d, want 0", got)
	}
	settleGoroutines(t, before)
	if got := inotifyCount(t); got != base+1 {
		t.Fatalf("inotify instances after the load: %d, want one (the agent's watcher)", got)
	}
}

// A command is delivered, not executed: the envelope, the outreq "sent", no watcher, no count.
func TestCommandWritesEnvelopeAndReportsSent(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)
	base := inotifyCount(t)

	rid, err := x.r.command(context.Background(), "peer", "ping", nil)
	if err != nil {
		t.Fatal(err)
	}
	if rid == "" {
		t.Fatal("no rid for the command")
	}
	raw, err := os.ReadFile(filepath.Join(x.inbox, string(rid)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var file RequestFile
	if jerr := json.Unmarshal(raw, &file); jerr != nil {
		t.Fatal(jerr)
	}
	if file.Type != TypeCommand {
		t.Fatalf("command envelope type: %s", file.Type)
	}
	x.assertDiariumStatus(t, rid, FinalSent)
	if got := inotifyCount(t); got != base {
		t.Fatalf("a command armed a watcher: %d, want %d", got, base)
	}
	if got := x.r.outboundCount(); got != 0 {
		t.Fatalf("a command counted as outbound: %d", got)
	}
}

// Attachments are copied into forum at B1 and deleted at B5, and the envelope lists them.
func TestRequestAttachmentsPublishedAndDeleted(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)
	src := filepath.Join(t.TempDir(), "a.txt")
	if err := os.WriteFile(src, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	forumFile := filepath.Join(x.root, FolderForum, "a.txt")

	done := runRequest(t, x.r, "peer", "cap", nil, RequestOptions{Attachments: []string{src}})
	rid := waitForRequest(t, x.inbox)
	waitForFile(t, forumFile)
	x.answer(rid, okBody())
	<-done
	if _, err := os.Stat(forumFile); err == nil {
		t.Fatal("the attachment survived the exchange")
	}
	raw, err := os.ReadFile(filepath.Join(x.inbox, string(rid)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var file RequestFile
	if jerr := json.Unmarshal(raw, &file); jerr != nil {
		t.Fatal(jerr)
	}
	var body RequestBody
	if jerr := json.Unmarshal(file.Request, &body); jerr != nil {
		t.Fatal(jerr)
	}
	if len(body.Attachments) != 1 || body.Attachments[0] != "a.txt" {
		t.Fatalf("the envelope's attachments: %v", body.Attachments)
	}
}

// The diarium keeps the envelopes as exchanged: the outreq line carries the exact bytes of the
// request, the response and the confirmation.
func TestRequestDiariumKeepsEnvelopesAsExchanged(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)

	done := runRequest(t, x.r, "peer", "cap", nil, RequestOptions{})
	rid := waitForRequest(t, x.inbox)
	x.answer(rid, okBody())
	<-done

	// The raw file, not ReadDiarium: the reader re-validates each line and its decode reorders the
	// keys, so only the file proves the bytes as exchanged.
	raw, err := os.ReadFile(filepath.Join(x.agentDir, "diarium.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for name, suffix := range map[string]string{"req": ".json", "res": ".res.json", "con": ".con.json"} {
		onDisk, err := os.ReadFile(filepath.Join(x.inbox, string(rid)+suffix))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), string(onDisk)) {
			t.Fatalf("the diarium does not keep the %s envelope as exchanged", name)
		}
	}
}

// A malformed response file is not a response: the exchange stays in B2 until the give-up paths.
func TestRequestMalformedResponseIgnoredUntilTimeout(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)

	done := runRequest(t, x.r, "peer", "cap", nil, RequestOptions{Timeout: 400 * time.Millisecond})
	rid := waitForRequest(t, x.inbox)
	writeFileAtomic(t, filepath.Join(x.inbox, string(rid)+".res.json"), []byte("not json"))
	<-done
	if _, err := os.Stat(filepath.Join(x.inbox, string(rid)+".con.json")); err == nil {
		t.Fatal("a malformed response was confirmed")
	}
	x.assertDiariumStatus(t, rid, FinalAbandoned)
}

// assertDiariumStatus finds the exchange's outreq line and checks its final status.
func (x *reqTest) assertDiariumStatus(t *testing.T, rid RID, want string) {
	t.Helper()
	read, err := ReadDiarium(x.agentDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range read.Lines {
		if line.Event != EventOutreq {
			continue
		}
		var env ExchangeEnvelopes
		if jerr := json.Unmarshal(line.EventData, &env); jerr != nil {
			continue
		}
		if string(env.Req) == "" {
			continue
		}
		var req RequestFile
		if jerr := json.Unmarshal(env.Req, &req); jerr != nil || req.RID != rid {
			continue
		}
		if env.FinalStatus != want {
			t.Fatalf("outreq final status: %s, want %s", env.FinalStatus, want)
		}
		return
	}
	t.Fatalf("no outreq line for %s in the diarium", rid)
}

// mismatchFile builds a schema-valid response for rid whose header names a different
// destination: it validates, but the D-11 header match fails, so the capture reports it as
// ignored. The body and datetime are fixed, so two calls with the same dst are byte-identical.
func (x *reqTest) mismatchFile(rid RID, dst AgentID) []byte {
	x.t.Helper()
	bodyRaw, err := json.Marshal(ResponseBody{
		TS: 1767323045, Nonce: "b1468606ed4e34f887287403c4dfd506", Error: false, Cancellation: false,
		Result: json.RawMessage(`{"ok":true}`), Attachments: []string{},
	})
	if err != nil {
		x.t.Fatal(err)
	}
	h := Header{RID: rid, Src: x.src, Dst: dst, Datetime: "2026-01-02T03:04:05Z", Type: TypeRequest, ActioVersion: ProtocolVersion}
	raw, err := json.Marshal(ResponseFile{Header: h, Response: bodyRaw})
	if err != nil {
		x.t.Fatal(err)
	}
	return raw
}

// waitForLogCount blocks until at least n log lines contain fragment.
func waitForLogCount(t *testing.T, sink *logSink, fragment string, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if sink.count(fragment) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("only %d log lines with %q, want %d", sink.count(fragment), fragment, n)
}

func waitForLog(t *testing.T, sink *logSink, fragment string) {
	t.Helper()
	waitForLogCount(t, sink, fragment, 1)
}

// A failed B5 deletion on the abandon path is logged on the agent's log path (the same one the
// receiver's housekeeping uses); the exchange still ends abandoned with its diarium line.
func TestRequestAbandonLogsAttachmentDeletionFailure(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)
	before := runtime.NumGoroutine()

	src := filepath.Join(t.TempDir(), "att.bin")
	if err := os.WriteFile(src, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	var out Outcome
	go func() {
		out, _ = x.r.request(ctx, "peer", "cap", nil, RequestOptions{Attachments: []string{src}})
		close(done)
	}()
	rid := waitForRequest(t, x.inbox)
	forum := filepath.Join(x.root, FolderForum)
	waitForFile(t, filepath.Join(forum, "att.bin"))
	// The attachment is published: make the forum read-only so B5's deletion fails with a real
	// error (a removed forum would not do: removePath treats a missing file as success).
	if err := os.Chmod(forum, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(forum, 0o755) })
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the exchange never ended after the cancellation")
	}
	if out.FinalStatus != FinalAbandoned {
		t.Fatalf("final status: %s, want abandoned", out.FinalStatus)
	}
	found := false
	for _, l := range x.logs.lines() {
		if strings.Contains(l, string(rid)) && strings.Contains(l, "attachment deletion failed") {
			found = true
		}
	}
	if !found {
		t.Fatalf("the failed attachment deletion was not logged; log lines: %v", x.logs.lines())
	}
	x.assertDiariumStatus(t, rid, FinalAbandoned)
	settleGoroutines(t, before)
}

// D-18's look half: the response is written while the watcher's delivery is stalled, so its
// event cannot wake the exchange; the exchange still ends confirmed, found by a look.
//
// The exact "the first look is the sole finder" state is not constructable without a production
// seam: the request file's own wake is already queued by the first look, and a stale wake would
// let a loop without the initial look pass the same way. This test pins the reachable half: a
// response present on disk while its event is undelivered ends the exchange confirmed.
func TestRequestResponseFoundByLookWhenEventStalled(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	var out Outcome
	go func() {
		out, _ = x.r.request(ctx, "peer", "cap", nil, RequestOptions{Timeout: 5 * time.Second})
		close(done)
	}()
	rid := waitForRequest(t, x.inbox)
	// Stall the watcher's delivery: its wake and loss paths take the table mutex, so while it is
	// held no wake-up can reach the subscription, whatever events land.
	x.r.st.watch.mu.Lock()
	x.answer(rid, okBody())
	x.r.st.watch.mu.Unlock()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the exchange never ended: the response written while the delivery was stalled was not found")
	}
	if out.FinalStatus != FinalConfirmed {
		t.Fatalf("final status: %s, want confirmed: the look must find a response whose event is missed", out.FinalStatus)
	}
	x.assertDiariumStatus(t, rid, FinalConfirmed)
	if _, err := os.Stat(filepath.Join(x.inbox, string(rid)+".con.json")); err != nil {
		t.Fatalf("the confirmation file is missing: %v", err)
	}
	settleGoroutines(t, before)
}

// D-11 dedup is per file state, not per path: a broken response rewritten is reported again, and
// a byte-identical re-send is not. (The reference keys on (path, mtime); the Go file primitives
// expose no mtime outside the five files (§9), so the key is (path, content) — reportIgnored.)
func TestRequestIgnoredRewriteReportedAgain(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		_, _ = x.r.request(ctx, "peer", "cap", nil, RequestOptions{})
		close(done)
	}()
	rid := waitForRequest(t, x.inbox)
	path := filepath.Join(x.inbox, string(rid)+".res.json")
	fragment := "ignored " + path

	// v1: schema-valid, header mismatched: reported once, though the loop looks at it many times.
	writeFileAtomic(t, path, x.mismatchFile(rid, "someone-else"))
	waitForLog(t, x.logs, fragment)
	// v2: the same path rewritten with different content: reported again.
	writeFileAtomic(t, path, x.mismatchFile(rid, "someone-else-2"))
	waitForLogCount(t, x.logs, fragment, 2)
	// v3: the same bytes re-sent (new mtime, identical content): not reported again.
	writeFileAtomic(t, path, x.mismatchFile(rid, "someone-else-2"))
	time.Sleep(150 * time.Millisecond) // let v3's wake pace a look
	if n := x.logs.count(fragment); n != 2 {
		t.Fatalf("the ignored report for the re-sent file: %d lines, want 2", n)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the exchange never ended after the cancellation")
	}
	settleGoroutines(t, before)
}

// The D-13 guard, archived variant: the peer answered and archived the request (the request file
// is gone) before we confirmed. The guard sees no request file: no confirmation is written, and
// the diarium line carries con: null — the final status is still confirmed.
func TestRequestD13ArchivedBeforeConfirm(t *testing.T) {
	events := make(chan Event, 64)
	x := newReqTest(t, "me", "peer", events)
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	var out Outcome
	go func() {
		out, _ = x.r.request(ctx, "peer", "cap", nil, RequestOptions{})
		close(done)
	}()
	rid := waitForRequest(t, x.inbox)
	// The archive deletes the request file; the answer follows. Deleting before writing is
	// deterministic and puts the guard in the same state a between-capture-and-confirm archive
	// does: the answer is present, the request is gone.
	if err := os.Remove(filepath.Join(x.inbox, string(rid)+".json")); err != nil {
		t.Fatal(err)
	}
	x.answer(rid, okBody())
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the exchange never ended")
	}
	if out.FinalStatus != FinalConfirmed {
		t.Fatalf("final status: %s, want confirmed", out.FinalStatus)
	}
	if _, err := os.Stat(filepath.Join(x.inbox, string(rid)+".con.json")); err == nil {
		t.Fatal("a confirmation was written although the peer had archived the request")
	}
	raw, err := os.ReadFile(filepath.Join(x.agentDir, "diarium.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"con":null`) {
		t.Fatal("the diarium line does not carry con: null")
	}
	settleGoroutines(t, before)
}
