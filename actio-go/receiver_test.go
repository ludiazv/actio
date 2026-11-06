// Tests for the receiver: lanes A and D. The harness is a real actio root (temp dir) with a real
// watcher, a real fileIO (no-op sync provider) and a controllable clock; the tests drive the inbox
// by writing envelope files and assert on the files, the diarium and the emitted events they leave
// behind. Everything is awaited with a deadline poll (a failure detector, never an ordering pause).
package actio

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// recvTest is the harness: a temp actio root, an agentState wired to it, and a receiver.
type recvTest struct {
	t     *testing.T
	root  string
	st    *agentState
	r     *receiver
	watch *watcher
	wg    sync.WaitGroup
	stop  chan struct{}

	spec *Spec

	evMu         sync.Mutex
	events       []Event
	handlerCalls int

	nowMu  sync.Mutex
	nowVal time.Time
}

func newRecvTest(t *testing.T, confirmTimeout time.Duration) *recvTest {
	t.Helper()
	root := t.TempDir()
	tt := &recvTest{
		t:      t,
		root:   root,
		nowVal: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		stop:   make(chan struct{}),
	}
	agentFolder := filepath.Join(root, "agent")
	inbox := filepath.Join(agentFolder, "inbox")
	forward := filepath.Join(root, "forum")
	for _, d := range []string{agentFolder, inbox, forward} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	provider, err := newSyncProvider("none", nil)
	if err != nil {
		t.Fatal(err)
	}
	files := newFileIO(provider)
	watch, err := newWatcher()
	if err != nil {
		t.Fatal(err)
	}
	spec := Spec{
		ID:                  "agent",
		ActioVersion:        "1.0",
		Datetime:            formatISO(tt.nowVal),
		Description:         "a test agent",
		AcceptsCommandsFrom: []string{"peer"},
		Capabilities:        []Capability{echoCap()},
		SecuritySupport:     []string{},
		SecurityPosture:     "trusted",
	}
	if _, err := validateSpec(mustJSON(t, spec)); err != nil {
		t.Fatalf("the test spec is not valid: %v", err)
	}
	tt.spec = &spec
	emit := func(e Event) {
		tt.evMu.Lock()
		tt.events = append(tt.events, e)
		tt.evMu.Unlock()
	}
	tt.st = &agentState{
		id:      "agent",
		config:  Config{ConfirmTimeout: confirmTimeout},
		folders: agentFolders{root: root, agent: agentFolder, inbox: inbox, forum: forward, nuntium: filepath.Join(root, "nuntium")},
		files:   files,
		sync:    provider,
		diaria:  newDiaria(files, agentFolder, "agent"),
		watch:   watch,
		now:     tt.now,
		emit:    emit,
		logf:    func(format string, args ...any) { emit(LogEvent{Message: fmt.Sprintf(format, args...)}) },
		spec:    func() *Spec { return tt.spec },
		state:   func() StatusState { return StatusIdle },
	}
	tt.watch = watch
	tt.r = newReceiver(tt.st)
	t.Cleanup(func() {
		_ = tt.r.stop(false, 2*time.Second)
		tt.wg.Wait()
		watch.close()
	})
	return tt
}

func (tt *recvTest) now() time.Time {
	tt.nowMu.Lock()
	defer tt.nowMu.Unlock()
	return tt.nowVal
}

func (tt *recvTest) setNow(v time.Time) {
	tt.nowMu.Lock()
	tt.nowVal = v
	tt.nowMu.Unlock()
}

func (tt *recvTest) start() {
	if err := tt.r.start(&tt.wg, tt.stop); err != nil {
		tt.t.Fatal(err)
	}
}

// echoCap is the capability the harness registers: it echoes its parameters.
func echoCap() Capability {
	return Capability{
		Name:        "echo",
		Description: "echoes its parameters",
		Parameters:  json.RawMessage(`{"type":"object"}`),
		Actio:       CapabilityActio{Result: json.RawMessage(`{"type":"object"}`)},
	}
}

// registerCap registers one capability with the given handler, counting the handler's calls.
func (tt *recvTest) registerCap(cap Capability, handler CapabilityHandler) {
	wrapped := func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		tt.evMu.Lock()
		tt.handlerCalls++
		tt.evMu.Unlock()
		return handler(ctx, params, call)
	}
	if err := tt.r.register(cap, wrapped); err != nil {
		tt.t.Fatal(err)
	}
}

func (tt *recvTest) registerEcho() {
	tt.registerCap(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		var v any
		_ = json.Unmarshal(params, &v)
		return v, nil
	})
}

// writeRequest writes a valid request envelope for the capability name into the inbox.
func (tt *recvTest) writeRequest(t *testing.T, rid RID, name string, params json.RawMessage, timeout float64) {
	t.Helper()
	now := tt.now()
	body := RequestBody{
		TS: now.Unix(), Nonce: newNonce(), Signature: nil,
		Name: name, Parameters: params, Timeout: timeout, Attachments: []string{},
	}
	raw, _ := marshalJSON(body)
	file := RequestFile{
		Header:  Header{RID: rid, Src: "peer", Dst: "agent", Datetime: formatISO(now), Type: TypeRequest, ActioVersion: "1.0"},
		Request: raw,
	}
	path := filepath.Join(tt.st.folders.inbox, string(rid)+".json")
	if err := tt.st.files.writeJSONAtomic(path, file, tt.st.folders.agent); err != nil {
		t.Fatal(err)
	}
}

// writeCommand writes a valid command envelope into the inbox.
func (tt *recvTest) writeCommand(t *testing.T, rid RID, name string, params json.RawMessage) {
	t.Helper()
	now := tt.now()
	body := RequestBody{
		TS: now.Unix(), Nonce: newNonce(), Signature: nil,
		Name: name, Parameters: params, Timeout: 0, Attachments: []string{},
	}
	raw, _ := marshalJSON(body)
	file := RequestFile{
		Header:  Header{RID: rid, Src: "peer", Dst: "agent", Datetime: formatISO(now), Type: TypeCommand, ActioVersion: "1.0"},
		Request: raw,
	}
	path := filepath.Join(tt.st.folders.inbox, string(rid)+".json")
	if err := tt.st.files.writeJSONAtomic(path, file, tt.st.folders.agent); err != nil {
		t.Fatal(err)
	}
}

// writeConfirmation writes a valid confirmation for the rid (same header as the request).
func (tt *recvTest) writeConfirmation(t *testing.T, rid RID) {
	t.Helper()
	now := tt.now()
	body := ConfirmationBody{TS: now.Unix(), Nonce: newNonce(), Signature: nil}
	raw, _ := marshalJSON(body)
	file := ConfirmationFile{
		Header:       Header{RID: rid, Src: "peer", Dst: "agent", Datetime: formatISO(now), Type: TypeRequest, ActioVersion: "1.0"},
		Confirmation: raw,
	}
	path := filepath.Join(tt.st.folders.inbox, string(rid)+".con.json")
	if err := tt.st.files.writeJSONAtomic(path, file, tt.st.folders.agent); err != nil {
		t.Fatal(err)
	}
}

func (tt *recvTest) waitForFile(t *testing.T, path string, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(path); err == nil {
			return raw
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
	return nil
}

func (tt *recvTest) resPath(rid RID) string {
	return filepath.Join(tt.st.folders.inbox, string(rid)+".res.json")
}

func (tt *recvTest) readResponse(t *testing.T, rid RID) ResponseBody {
	t.Helper()
	raw := tt.waitForFile(t, tt.resPath(rid), 3*time.Second)
	var f ResponseFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("the response is not valid JSON: %v", err)
	}
	var b ResponseBody
	if err := json.Unmarshal(f.Response, &b); err != nil {
		t.Fatalf("the response body is not valid JSON: %v", err)
	}
	return b
}

func (tt *recvTest) errorResult(t *testing.T, b ResponseBody) ErrorResult {
	t.Helper()
	var er ErrorResult
	if err := json.Unmarshal(b.Result, &er); err != nil {
		t.Fatalf("the error result is not valid JSON: %v", err)
	}
	return er
}

func (tt *recvTest) waitForInboxEmpty(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, _ := os.ReadDir(tt.st.folders.inbox)
		if len(entries) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the inbox is not empty after the timeout")
}

func (tt *recvTest) waitForBusy(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tt.r.busy() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the receiver is not busy after the timeout")
}

func (tt *recvTest) waitForLoopExit(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-tt.r.done:
	case <-time.After(timeout):
		t.Fatal("the receiver loop did not end")
	}
}

// inreqLines returns the inreq events of the diarium, in file order.
func (tt *recvTest) inreqLines(t *testing.T) []DiariumLine {
	t.Helper()
	read, err := ReadDiarium(tt.st.folders.agent)
	if err != nil {
		t.Fatal(err)
	}
	var out []DiariumLine
	for _, l := range read.Lines {
		if l.Event == EventInreq {
			out = append(out, l)
		}
	}
	return out
}

func (tt *recvTest) hasEvent(t *testing.T, timeout time.Duration, check func(Event) bool) Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		tt.evMu.Lock()
		for _, e := range tt.events {
			if check(e) {
				tt.evMu.Unlock()
				return e
			}
		}
		tt.evMu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the expected event was not emitted")
	return nil
}

func (tt *recvTest) newRID() RID { return newULID(tt.now()) }

// calls is the number of times the registered capability's handler ran, read under the events lock.
func (tt *recvTest) calls() int {
	tt.evMu.Lock()
	defer tt.evMu.Unlock()
	return tt.handlerCalls
}

func strp(v string) *string { return &v }
func boolp(v bool) *bool    { return &v }

// isJSONNull reports whether a raw envelope slot is JSON null (absent in the diarium).
func isJSONNull(raw json.RawMessage) bool { return len(raw) == 0 || string(raw) == "null" }

// --- acceptance codes -----------------------------------------------------------

func TestRecvCode100AgentDisabled(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerEcho()
	tt.spec.Disabled = boolp(true)
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	b := tt.readResponse(t, rid)
	er := tt.errorResult(t, b)
	if !b.Error || er.Code == nil || *er.Code != CodeAgentDisabled {
		t.Fatalf("want code 100, got %+v", er)
	}
	if tt.calls() != 0 {
		t.Fatalf("the handler ran %d times, want 0", tt.calls())
	}
}

func TestRecvCode101UnknownCapability(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerEcho()
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "nope", json.RawMessage(`{}`), 0)
	b := tt.readResponse(t, rid)
	er := tt.errorResult(t, b)
	if !b.Error || er.Code == nil || *er.Code != CodeUnknownCapability {
		t.Fatalf("want code 101, got %+v", er)
	}
	if tt.calls() != 0 {
		t.Fatalf("the handler ran %d times, want 0", tt.calls())
	}
}

func TestRecvCode102CapabilityDisabled(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	cap := echoCap()
	cap.Actio.Disabled = boolp(true)
	tt.registerCap(cap, nilHandler())
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	b := tt.readResponse(t, rid)
	er := tt.errorResult(t, b)
	if !b.Error || er.Code == nil || *er.Code != CodeCapabilityDisabled {
		t.Fatalf("want code 102, got %+v", er)
	}
	if tt.calls() != 0 {
		t.Fatalf("the handler ran %d times, want 0", tt.calls())
	}
}

func TestRecvCode103RequestorNotAllowed(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	cap := echoCap()
	cap.Actio.AcceptsFrom = []string{"other"}
	tt.registerCap(cap, nilHandler())
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	b := tt.readResponse(t, rid)
	er := tt.errorResult(t, b)
	if !b.Error || er.Code == nil || *er.Code != CodeRequestorNotAllowed {
		t.Fatalf("want code 103, got %+v", er)
	}
	if tt.calls() != 0 {
		t.Fatalf("the handler ran %d times, want 0", tt.calls())
	}
}

func TestRecvCode105InvalidParameters(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	cap := echoCap()
	cap.Parameters = json.RawMessage(`{"type":"object","required":["text"]}`)
	tt.registerCap(cap, nilHandler())
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0) // missing "text"
	b := tt.readResponse(t, rid)
	er := tt.errorResult(t, b)
	if !b.Error || er.Code == nil || *er.Code != CodeInvalidParameters {
		t.Fatalf("want code 105, got %+v", er)
	}
	if tt.calls() != 0 {
		t.Fatalf("the handler ran %d times, want 0", tt.calls())
	}
}

func TestRecvCode106NamespaceMismatch(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerEcho()
	tt.spec.Namespaces = []*string{strp("ns1")}
	peer := &Spec{ID: "peer", Namespaces: []*string{strp("ns2")}}
	tt.r.setPeerSpec(func(id AgentID) *Spec { return peer })
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	b := tt.readResponse(t, rid)
	er := tt.errorResult(t, b)
	if !b.Error || er.Code == nil || *er.Code != CodeNamespaceMismatch {
		t.Fatalf("want code 106, got %+v", er)
	}
	if tt.calls() != 0 {
		t.Fatalf("the handler ran %d times, want 0", tt.calls())
	}
}

func TestRecvCode107CapacityExceeded(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	cap := echoCap()
	cap.Actio.MaxParallel = 1
	release := make(chan struct{})
	tt.registerCap(cap, func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		<-release // hold the single slot
		return map[string]any{}, nil
	})
	tt.start()
	first := tt.newRID()
	tt.writeRequest(t, first, "echo", json.RawMessage(`{}`), 0)
	tt.waitForBusy(t, 3*time.Second) // the first work is running
	second := tt.newRID()
	tt.writeRequest(t, second, "echo", json.RawMessage(`{}`), 0)
	b := tt.readResponse(t, second)
	er := tt.errorResult(t, b)
	if !b.Error || er.Code == nil || *er.Code != CodeCapacityExceeded {
		t.Fatalf("want code 107, got %+v", er)
	}
	if tt.calls() != 1 {
		t.Fatalf("the handler ran %d times, want 1 (only the first request)", tt.calls())
	}
	close(release)
}

func TestRecvCode108RequestExpired(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerEcho()
	tt.start()
	rid := tt.newRID()
	// ts + timeout is already in the past when the receiver picks it up.
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 10)
	tt.setNow(tt.now().Add(11 * time.Second))
	b := tt.readResponse(t, rid)
	er := tt.errorResult(t, b)
	if !b.Error || er.Code == nil || *er.Code != CodeRequestExpired {
		t.Fatalf("want code 108, got %+v", er)
	}
	if tt.calls() != 0 {
		t.Fatalf("the handler ran %d times, want 0", tt.calls())
	}
}

func nilHandler() CapabilityHandler {
	return func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		return map[string]any{}, nil
	}
}

// --- the normal exchange and the response-once rules ---------------------------

func TestRecvNormalExchangeConfirmed(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerEcho()
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{"x":1}`), 0)
	b := tt.readResponse(t, rid)
	if b.Error || b.Cancellation {
		t.Fatalf("the response is an error/cancellation: %+v", b)
	}
	var result map[string]any
	if err := json.Unmarshal(b.Result, &result); err != nil || result["x"] != float64(1) {
		t.Fatalf("the result is not the echoed parameters: %v", b.Result)
	}
	tt.writeConfirmation(t, rid)
	tt.waitForInboxEmpty(t, 3*time.Second)
	lines := tt.inreqLines(t)
	if len(lines) != 1 {
		t.Fatalf("want one inreq line, got %d", len(lines))
	}
	var env ExchangeEnvelopes
	if err := json.Unmarshal(lines[0].EventData, &env); err != nil {
		t.Fatal(err)
	}
	if env.FinalStatus != FinalConfirmed {
		t.Fatalf("final status = %q, want %q", env.FinalStatus, FinalConfirmed)
	}
	if isJSONNull(env.Req) || isJSONNull(env.Res) || isJSONNull(env.Con) || !isJSONNull(env.Can) {
		t.Fatalf("the envelopes are wrong: %+v", env)
	}
}

func TestRecvNoDoubleAnswerHeaderMismatch(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerEcho()
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	first := tt.readResponse(t, rid) // the receiver's own response (written once)
	// A .res whose rid matches but whose header does not (a different src): D-11, ignored.
	now := tt.now()
	body := ResponseBody{TS: now.Unix(), Nonce: newNonce(), Signature: nil, Error: false, Result: json.RawMessage(`{}`), Attachments: []string{}}
	rawBody, _ := marshalJSON(body)
	intruder := ResponseFile{
		Header:   Header{RID: rid, Src: "someone-else", Dst: "agent", Datetime: formatISO(now), Type: TypeRequest, ActioVersion: "1.0"},
		Response: rawBody,
	}
	rawFile, _ := marshalJSON(intruder)
	if err := tt.st.files.writeBytesAtomic(tt.resPath(rid), rawFile, tt.st.folders.agent); err != nil {
		t.Fatal(err)
	}
	// The intruder is ignored (D-11); the exchange archives on the confirmation with its OWN
	// response (the first one), not the intruder's.
	tt.writeConfirmation(t, rid)
	tt.waitForInboxEmpty(t, 3*time.Second)
	lines := tt.inreqLines(t)
	if len(lines) != 1 {
		t.Fatalf("want one inreq line, got %d", len(lines))
	}
	var env ExchangeEnvelopes
	if err := json.Unmarshal(lines[0].EventData, &env); err != nil {
		t.Fatal(err)
	}
	var res ResponseFile
	if err := json.Unmarshal(env.Res, &res); err != nil {
		t.Fatal(err)
	}
	var resBody ResponseBody
	if err := json.Unmarshal(res.Response, &resBody); err != nil {
		t.Fatal(err)
	}
	if resBody.Nonce != first.Nonce {
		t.Fatalf("the intruder response was used (nonce %q -> %q): the header-mismatched .res was not ignored", first.Nonce, resBody.Nonce)
	}
}

func TestRecvSecondResponseNotWritten(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerEcho()
	rid := tt.newRID()
	hdr := Header{RID: rid, Src: "peer", Dst: "agent", Datetime: formatISO(tt.now()), Type: TypeRequest, ActioVersion: "1.0"}
	// A response already present: a second writeResponse for the same exchange is a no-op.
	ex := &exchange{rid: rid, header: hdr, phase: phaseResponded, res: &ResponseFile{Header: hdr}}
	tt.r.exchanges[rid] = ex
	before := []byte("the single response")
	if err := os.WriteFile(tt.resPath(rid), before, 0o644); err != nil {
		t.Fatal(err)
	}
	tt.r.writeResponse(ex, responseOpts{result: json.RawMessage(`{"x":2}`), attachments: []string{}})
	after, _ := os.ReadFile(tt.resPath(rid))
	if string(after) != string(before) {
		t.Fatalf("a second response was written: %q", after)
	}
}

// --- the confirmation wait ------------------------------------------------------

func TestRecvConfirmationTimeoutCompletes(t *testing.T) {
	tt := newRecvTest(t, 200*time.Millisecond) // a short confirmation timeout
	tt.registerEcho()
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	tt.readResponse(t, rid)
	// No confirmation: the archive happens on the timeout, as completed.
	tt.waitForInboxEmpty(t, 3*time.Second)
	lines := tt.inreqLines(t)
	if len(lines) != 1 {
		t.Fatalf("want one inreq line, got %d", len(lines))
	}
	var env ExchangeEnvelopes
	if err := json.Unmarshal(lines[0].EventData, &env); err != nil {
		t.Fatal(err)
	}
	if env.FinalStatus != FinalCompleted {
		t.Fatalf("final status = %q, want %q", env.FinalStatus, FinalCompleted)
	}
	if !isJSONNull(env.Con) {
		t.Fatalf("the confirmation was recorded though it never arrived: %+v", env.Con)
	}
}

// --- cancellation and panic -----------------------------------------------------

func TestRecvCancellationDuringWork(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerCap(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return map[string]any{}, nil
		}
	})
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	tt.waitForBusy(t, 3*time.Second) // the work is running
	now := tt.now()
	canBody := CancellationBody{TS: now.Unix(), Nonce: newNonce(), Signature: nil, Name: "echo", Force: false}
	rawCan, _ := marshalJSON(canBody)
	canFile := CancellationFile{
		Header:       Header{RID: rid, Src: "peer", Dst: "agent", Datetime: formatISO(now), Type: TypeRequest, ActioVersion: "1.0"},
		Cancellation: rawCan,
	}
	canPath := filepath.Join(tt.st.folders.inbox, string(rid)+".can.json")
	if err := tt.st.files.writeJSONAtomic(canPath, canFile, tt.st.folders.agent); err != nil {
		t.Fatal(err)
	}
	b := tt.readResponse(t, rid)
	if !b.Cancellation || b.Error {
		t.Fatalf("want a cancellation response, got %+v", b)
	}
	tt.writeConfirmation(t, rid)
	tt.waitForInboxEmpty(t, 3*time.Second)
	var env ExchangeEnvelopes
	if err := json.Unmarshal(tt.inreqLines(t)[0].EventData, &env); err != nil {
		t.Fatal(err)
	}
	if env.FinalStatus != FinalCancelled {
		t.Fatalf("final status = %q, want %q", env.FinalStatus, FinalCancelled)
	}
}

func TestRecvHandlerPanicAnsweredAsFailure(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerCap(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		panic("the handler exploded")
	})
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	b := tt.readResponse(t, rid)
	if !b.Error {
		t.Fatalf("want an error response, got %+v", b)
	}
	er := tt.errorResult(t, b)
	if er.Code != nil {
		t.Fatalf("a panic has no protocol code, got %d", *er.Code)
	}
	if er.Msg == "" {
		t.Fatal("the error message is empty")
	}
	// The loop survived: a later request for a second, healthy capability is still served.
	rid2 := tt.newRID()
	second := echoCap()
	second.Name = "ok"
	tt.registerCap(second, nilHandler())
	tt.writeRequest(t, rid2, "ok", json.RawMessage(`{"ok":true}`), 0)
	if b2 := tt.readResponse(t, rid2); b2.Error {
		t.Fatalf("the loop did not survive the panic: %+v", b2)
	}
}

// --- stop ----------------------------------------------------------------------

func TestRecvStopAnswers300WithinGrace(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	release := make(chan struct{})
	tt.registerCap(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		select {
		case <-release:
			return map[string]any{"done": true}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	tt.waitForBusy(t, 3*time.Second)
	// The work finishes within the grace: stop lets it, and the response is its real result.
	go func() { time.Sleep(50 * time.Millisecond); close(release) }()
	if err := tt.r.stop(false, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	// The stop archives the exchange: the response is in the diarium, not on disk.
	lines := tt.inreqLines(t)
	if len(lines) != 1 {
		t.Fatalf("want one inreq line, got %d", len(lines))
	}
	var env ExchangeEnvelopes
	if err := json.Unmarshal(lines[0].EventData, &env); err != nil {
		t.Fatal(err)
	}
	var res ResponseFile
	if err := json.Unmarshal(env.Res, &res); err != nil {
		t.Fatal(err)
	}
	var resBody ResponseBody
	if err := json.Unmarshal(res.Response, &resBody); err != nil {
		t.Fatal(err)
	}
	if resBody.Error {
		t.Fatalf("the work finished in the grace, want its result, got an error: %+v", resBody)
	}
	var result map[string]any
	_ = json.Unmarshal(resBody.Result, &result)
	if result["done"] != true {
		t.Fatalf("the response is not the work's result: %v", resBody.Result)
	}
}

func TestRecvStopForceAnswers300(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerCap(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		<-ctx.Done() // never finishes on its own
		return nil, ctx.Err()
	})
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	tt.waitForBusy(t, 3*time.Second)
	if err := tt.r.stop(true, 0); err != nil {
		t.Fatal(err)
	}
	tt.waitForInboxEmpty(t, 3*time.Second) // the stop archives the exchange
	lines := tt.inreqLines(t)
	if len(lines) != 1 {
		t.Fatalf("want one inreq line, got %d", len(lines))
	}
	var env ExchangeEnvelopes
	if err := json.Unmarshal(lines[0].EventData, &env); err != nil {
		t.Fatal(err)
	}
	var res ResponseFile
	if err := json.Unmarshal(env.Res, &res); err != nil {
		t.Fatal(err)
	}
	var resBody ResponseBody
	if err := json.Unmarshal(res.Response, &resBody); err != nil {
		t.Fatal(err)
	}
	var er ErrorResult
	if err := json.Unmarshal(resBody.Result, &er); err != nil {
		t.Fatal(err)
	}
	if !resBody.Error || er.Code == nil || *er.Code != CodeAgentStopping {
		t.Fatalf("want code 300, got %+v", er)
	}
}

// --- the counters under a concurrent read --------------------------------------

func TestRecvBusyAndCapabilityStatusConcurrent(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	started := make(chan struct{})
	tt.registerCap(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		close(started)
		<-ctx.Done() // hold the slot until the test stops
		return nil, ctx.Err()
	})
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	<-started
	// Read busy() and capabilityStatus() from this goroutine while the loop runs the work.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Go(func() {
			_ = tt.r.busy()
			_ = tt.r.capabilityStatus()
		})
	}
	wg.Wait()
	if !tt.r.busy() {
		t.Fatal("busy() is false while a request is running")
	}
	st := tt.r.capabilityStatus()
	if len(st) != 1 || st[0].Name != "echo" || st[0].Running != 1 {
		t.Fatalf("capabilityStatus = %+v, want echo running=1", st)
	}
	_ = tt.r.stop(true, 0)
}

// --- malformed files and the folder loss ---------------------------------------

func TestRecvMalformedInboxFileIgnored(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerEcho()
	tt.start()
	badPath := filepath.Join(tt.st.folders.inbox, "not-a-rid.json")
	if err := os.WriteFile(badPath, []byte(`{ "this": "is not an envelope" }`), 0o644); err != nil {
		t.Fatal(err)
	}
	tt.hasEvent(t, 3*time.Second, func(e Event) bool {
		ig, ok := e.(IgnoredEvent)
		return ok && ig.Path == badPath && ig.Reason != ""
	})
	// The loop kept going: a real request is still served.
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	if b := tt.readResponse(t, rid); b.Error {
		t.Fatalf("the loop did not keep going: %+v", b)
	}
}

func TestRecvFolderLossEndsLoop(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerEcho()
	tt.start()
	inbox := tt.st.folders.inbox
	if err := os.Rename(inbox, inbox+".gone"); err != nil {
		t.Fatal(err)
	}
	tt.waitForLoopExit(t, 3*time.Second)
	tt.hasEvent(t, time.Second, func(e Event) bool {
		ig, ok := e.(IgnoredEvent)
		return ok && ig.Path == inbox
	})
}
