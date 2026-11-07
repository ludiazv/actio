// Tests for the receiver: lanes A and D. The harness is a real actio root (temp dir) with a real
// watcher, a real fileIO (no-op sync provider) and a controllable clock; the tests drive the inbox
// by writing envelope files and assert on the files, the diarium and the emitted events they leave
// behind. Everything is awaited with a deadline poll (a failure detector, never an ordering pause).
package actio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
		// logf writes a diarium log line, as the agent's logf does (no stream event).
		logf: func(format string, args ...any) {
			_ = tt.st.diaria.append(logEvent(tt.st.id, tt.st.now(), fmt.Sprintf(format, args...)))
		},
		// The harness's failWatch mirrors the stream half of the agent's D-22 hook: the LogEvent.
		// The diarium line and the Fail are agent-level and tested there.
		failWatch: func(path string, err error) { emit(LogEvent{Message: "watch lost " + path + ": " + err.Error()}) },
		spec:      func() *Spec { return tt.spec },
		state:     func() StatusState { return StatusIdle },
	}
	tt.watch = watch
	tt.r = newReceiver(tt.st)
	t.Cleanup(func() {
		_ = tt.r.stop(false, 2*time.Second)
		// Bounded: a loop that never settled (a regression) must not hang the cleanup.
		select {
		case <-tt.r.done:
		case <-time.After(5 * time.Second):
		}
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
	if err := tt.r.start(&tt.wg); err != nil {
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

// waitForInreqLines blocks until the diarium holds at least n inreq lines, or the timeout: the
// archive has landed. It observes the condition the test asserts (the line), not an earlier side
// effect (the inbox becoming empty). archive removes the inbox files before it writes the line, so
// waiting on the empty inbox can read the diarium in the gap and see 0 lines under load.
func (tt *recvTest) waitForInreqLines(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(tt.inreqLines(t)) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the diarium has %d inreq lines, want at least %d, after %v", len(tt.inreqLines(t)), n, timeout)
}

// logLines is the diarium's log lines (the `log` event), in order.
func (tt *recvTest) logLines(t *testing.T) []DiariumLine {
	t.Helper()
	read, err := ReadDiarium(tt.st.folders.agent)
	if err != nil {
		t.Fatal(err)
	}
	var out []DiariumLine
	for _, l := range read.Lines {
		if l.Event == EventLog {
			out = append(out, l)
		}
	}
	return out
}

// countLogLinesNaming counts the diarium log lines whose message names the rid.
func countLogLinesNaming(t *testing.T, tt *recvTest, rid RID) int {
	t.Helper()
	n := 0
	for _, l := range tt.logLines(t) {
		if strings.Contains(string(l.EventData), string(rid)) {
			n++
		}
	}
	return n
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

// waitForSettled bounds the wait for the receiver's loop to end after a stop, so a regression that
// leaves the loop un-settled (performStop never ran) fails with a named message, not a hang. The
// loop closes r.done as it ends, so a closed r.done is the settle.
func (tt *recvTest) waitForSettled(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-tt.r.done:
	case <-time.After(timeout):
		t.Fatalf("the receiver loop did not settle within %v after the stop: performStop did not run", timeout)
	}
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
	tt.waitForInreqLines(t, 1, 3*time.Second)
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
	tt.waitForInreqLines(t, 1, 3*time.Second)
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
	tt.waitForInreqLines(t, 1, 3*time.Second)
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
	tt.waitForInreqLines(t, 1, 3*time.Second)
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
	// The work is released from a state (closed right away), not the wall clock: it settles on
	// the normal path or in the stop's grace drain; both answer it with its real result, not 300.
	close(release)
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
	tt.waitForInreqLines(t, 1, 3*time.Second) // the stop archives the exchange
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

// --- lost inbox: the loop stays, the stop settles (R2) -------------------------

// A lost empty inbox must not crash the loop: 60 runs, no panic, the loss is a LogEvent (D-22),
// and the stop still settles. The probe this adopts is the oracle's TestR2InboxRemoved.
func TestRecvInboxRemovedEmptyNoCrash(t *testing.T) {
	for i := 0; i < 60; i++ {
		tt := newRecvTest(t, 5*time.Second)
		tt.registerEcho()
		tt.start()
		inbox := tt.st.folders.inbox
		if err := os.Rename(inbox, inbox+".gone"); err != nil {
			t.Fatal(err)
		}
		// The loss is a LogEvent, and the loop is still up (it does not end on a loss).
		tt.hasEvent(t, 3*time.Second, func(e Event) bool {
			le, ok := e.(LogEvent)
			return ok && strings.HasPrefix(le.Message, "watch lost "+inbox+":")
		})
		// The loop is alive: a stop runs the settle path and returns.
		if err := tt.r.stop(true, 0); err != nil {
			t.Fatalf("run %d: stop after an empty-inbox loss: %v", i, err)
		}
		tt.waitForSettled(t, 5*time.Second)
	}
}

// A lost inbox with files in it must not crash the loop. The oracle's probe panicked here (a nil
// dereference in the loop after the loss); a bounded number of runs, a named bound, no dependence
// on the scheduler. The probe this adopts is the oracle's TestR2InboxRemovedWithFiles.
const inboxLossFileRuns = 200

func TestRecvInboxRemovedWithFilesNoCrash(t *testing.T) {
	for i := 0; i < inboxLossFileRuns; i++ {
		tt := newRecvTest(t, 5*time.Second)
		tt.registerEcho()
		tt.start()
		inbox := tt.st.folders.inbox
		// Junk that is not an envelope: the loss must not trip on it.
		if err := os.WriteFile(filepath.Join(inbox, "junk.json"), []byte("not json"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(inbox, inbox+".gone"); err != nil {
			t.Fatal(err)
		}
		tt.hasEvent(t, 3*time.Second, func(e Event) bool {
			le, ok := e.(LogEvent)
			return ok && strings.HasPrefix(le.Message, "watch lost "+inbox+":")
		})
		if err := tt.r.stop(true, 0); err != nil {
			t.Fatalf("run %d: stop after a with-files-inbox loss: %v", i, err)
		}
		tt.waitForSettled(t, 5*time.Second)
	}
}

// After a loss, a Stop(Force) cancels the running handler and settles the exchange: the work is
// cancelled, and the exchange is left unsettled (the 300 answer cannot be written to a lost inbox,
// and the protocol has no `dropped` for a request). The probe this adopts is the oracle's
// TestR2InboxLostHandlerNotCancelled, with the settle assertion added.
func TestRecvInboxLostStopForceSettles(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerCap(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		<-ctx.Done() // the handler blocks until the stop cancels it
		return nil, ctx.Err()
	})
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	tt.waitForBusy(t, 3*time.Second)
	inbox := tt.st.folders.inbox
	if err := os.Rename(inbox, inbox+".gone"); err != nil {
		t.Fatal(err)
	}
	tt.hasEvent(t, 3*time.Second, func(e Event) bool {
		le, ok := e.(LogEvent)
		return ok && strings.HasPrefix(le.Message, "watch lost "+inbox+":")
	})
	// The loss does not stop the agent: a Stop(Force) is what settles the work.
	if err := tt.r.stop(true, 0); err != nil {
		t.Fatalf("stop after the loss: %v", err)
	}
	// The loop is observably settled (a bounded wait, not an unbounded signal): performStop ran and
	// the loop ended. If it never ran, this fails fast with a named message, not a hang.
	tt.waitForSettled(t, 5*time.Second)
	// Unsettled must not mean leaked: the exchange left the working set (it is no longer tracked),
	// and no goroutine keeps waiting on it (the loop ended, so the cancelled handler has returned).
	if len(tt.r.exchanges) != 0 {
		t.Fatalf("the unsettled exchange is still in the working set: %d tracked", len(tt.r.exchanges))
	}
	// The exchange is left unsettled: the protocol has no `dropped` for a request, so the diarium
	// holds no final_status line for the rid, and no inreq event carries `dropped`.
	ins := tt.inreqLines(t)
	if len(ins) != 0 {
		t.Fatalf("the inreq diarium has %d lines, want 0: a request whose answer was not written stays out of the archive", len(ins))
	}
	for _, l := range ins {
		var env ExchangeEnvelopes
		if err := json.Unmarshal(l.EventData, &env); err != nil {
			t.Fatalf("unmarshal the inreq event_data: %v", err)
		}
		if env.FinalStatus == FinalDropped {
			t.Fatalf("an inreq event carries final_status %q: a request is never dropped", env.FinalStatus)
		}
	}
	// Exactly one log line, naming the rid and why it has no answer.
	if n := countLogLinesNaming(t, tt, rid); n != 1 {
		t.Fatalf("the diarium has %d log lines naming the rid %s, want exactly 1", n, rid)
	}
}

// A second stop that enters during the first stop's drain must not race and must not settle
// anything twice. The race the oracle found: stop() read r.inbox while performStop() wrote
// r.inbox = nil. The started flag removes the read; this pins that two concurrent stops settle the
// exchange once. Run under -race.
func TestRecvStopAfterLossSettlesOnce(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerCap(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	tt.waitForBusy(t, 3*time.Second)
	inbox := tt.st.folders.inbox
	if err := os.Rename(inbox, inbox+".gone"); err != nil {
		t.Fatal(err)
	}
	// Two concurrent stops: the first runs the settle path, the second takes the done branch.
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Go(func() { errs <- tt.r.stop(true, 0) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("a concurrent stop errored: %v", err)
		}
	}
	tt.waitForSettled(t, 5*time.Second)
	// Settled exactly once: no inreq line (a request is never dropped), and exactly one log line
	// naming the rid (the D-5 guard writes it once, not per stop).
	ins := tt.inreqLines(t)
	if len(ins) != 0 {
		t.Fatalf("the inreq diarium has %d lines, want 0: a request whose answer was not written stays out of the archive", len(ins))
	}
	if n := countLogLinesNaming(t, tt, rid); n != 1 {
		t.Fatalf("the diarium has %d log lines naming the rid %s, want exactly 1 (settled once)", n, rid)
	}
}

// A command that is not accepted (D4) is archived as `dropped`: that is the one legal use of the
// status. A request is never dropped (it stays unsettled), but a command is.
func TestRecvDroppedCommandArchivesDropped(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.start()
	rid := tt.newRID()
	// An unknown command from the listed source: dropped at D4.
	tt.writeCommand(t, rid, "nosuchcmd", json.RawMessage(`{}`))
	// The command is archived: its file is deleted and the diarium holds an inreq line.
	tt.waitForInreqLines(t, 1, 3*time.Second)
	ins := tt.inreqLines(t)
	if len(ins) != 1 {
		t.Fatalf("the inreq diarium has %d lines, want 1 (the dropped command)", len(ins))
	}
	var env ExchangeEnvelopes
	if err := json.Unmarshal(ins[0].EventData, &env); err != nil {
		t.Fatalf("unmarshal the inreq event_data: %v", err)
	}
	if env.FinalStatus != FinalDropped {
		t.Fatalf("the dropped command has final_status %q, want %q", env.FinalStatus, FinalDropped)
	}
}

// An inbox file whose bytes are not JSON uses the "malformed JSON: <parser error>" reason, not a
// schema message about a schema the file never reached. A JSON object that is not an envelope keeps
// the schema's words (E14).
func TestRecvMalformedJSONPrefix(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerEcho()
	tt.start()
	inbox := tt.st.folders.inbox
	// Not JSON at all: the malformed JSON prefix.
	bad := filepath.Join(inbox, "01MALFORMED000000000000000001.json")
	if err := os.WriteFile(bad, []byte("this is not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	tt.hasEvent(t, 3*time.Second, func(e Event) bool {
		ig, ok := e.(IgnoredEvent)
		return ok && ig.Path == bad && strings.HasPrefix(ig.Reason, "malformed JSON:")
	})
	// A JSON object that is not an envelope: the schema's words, not the malformed JSON prefix.
	obj := filepath.Join(inbox, "01NOTENVELOPE000000000000000001.json")
	if err := os.WriteFile(obj, []byte(`{ "this": "is a json object, not an envelope" }`), 0o644); err != nil {
		t.Fatal(err)
	}
	tt.hasEvent(t, 3*time.Second, func(e Event) bool {
		ig, ok := e.(IgnoredEvent)
		return ok && ig.Path == obj && !strings.HasPrefix(ig.Reason, "malformed JSON:")
	})
}

// --- the diarium envelopes as exchanged, and the start state -------------------

// writeRawEnvelope writes one pre-built envelope file: the bytes land exactly as given.
func (tt *recvTest) writeRawEnvelope(t *testing.T, name string, raw []byte) {
	t.Helper()
	path := filepath.Join(tt.st.folders.inbox, name)
	if err := tt.st.files.writeBytesAtomic(path, raw, tt.st.folders.agent); err != nil {
		t.Fatal(err)
	}
}

// craftEnvelope builds one envelope file whose key order differs from the Go struct order (the
// body key first, the body keys reversed): a re-marshal of the decoded struct would not produce
// these bytes, so a diarium slot that equals them is the file's bytes as exchanged.
func craftEnvelope(rid RID, now time.Time, bodyKey, body string) []byte {
	return []byte(`{"` + bodyKey + `":` + body + `,"rid":"` + string(rid) + `","src":"peer","dst":"agent","datetime":"` + formatISO(now) + `","type":"req","actio_version":"1.0"}`)
}

func craftedRes(rid RID, now time.Time) []byte {
	return craftEnvelope(rid, now, "response", `{"attachments":[],"cancellation":false,"error":false,"nonce":"`+newNonce()+`","result":{},"signature":null,"ts":`+fmt.Sprint(now.Unix())+`}`)
}

func craftedCan(rid RID, now time.Time) []byte {
	return craftEnvelope(rid, now, "cancellation", `{"force":false,"name":"echo","nonce":"`+newNonce()+`","signature":null,"ts":`+fmt.Sprint(now.Unix())+`}`)
}

func craftedCon(rid RID, now time.Time) []byte {
	return craftEnvelope(rid, now, "confirmation", `{"nonce":"`+newNonce()+`","signature":null,"ts":`+fmt.Sprint(now.Unix())+`}`)
}

// TestRecvDiariumEnvelopesAsExchanged pins the as-exchanged rule on the normal path: the .con is
// delivered after the response, and the diarium must carry the confirmation and the response with
// the file's exact bytes (key order and all), not a re-marshal of the decoded structs.
func TestRecvDiariumEnvelopesAsExchanged(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerEcho()
	tt.start()
	rid := tt.newRID()
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0)
	resRaw := tt.waitForFile(t, tt.resPath(rid), 3*time.Second) // the receiver's own response, as written
	conRaw := craftedCon(rid, tt.now())
	tt.writeRawEnvelope(t, string(rid)+".con.json", conRaw)
	tt.waitForInreqLines(t, 1, 3*time.Second)
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
	if !bytes.Equal(env.Con, conRaw) {
		t.Fatalf("the diarium con is not the file's bytes as exchanged:\n file: %s\n diarium: %s", conRaw, env.Con)
	}
	if !bytes.Equal(env.Res, resRaw) {
		t.Fatalf("the diarium res is not the file's bytes as exchanged:\n file: %s\n diarium: %s", resRaw, env.Res)
	}
	if !isJSONNull(env.Can) {
		t.Fatalf("the cancellation was recorded though it never arrived: %s", env.Can)
	}
}

// TestRecvRetrackKeepsEnvelopes is the D-4 retrack at start: a previous run left the response and
// the cancellation (no request file). The retracked exchange must archive with the files' bytes
// as exchanged — "null" in the diarium means the file was never written, not that we lost it.
func TestRecvRetrackKeepsEnvelopes(t *testing.T) {
	tt := newRecvTest(t, 300*time.Millisecond) // the archive lands on the confirmation timeout
	tt.registerEcho()
	rid := tt.newRID()
	resRaw := craftedRes(rid, tt.now())
	canRaw := craftedCan(rid, tt.now())
	tt.writeRawEnvelope(t, string(rid)+".res.json", resRaw)
	tt.writeRawEnvelope(t, string(rid)+".can.json", canRaw)
	tt.start()
	tt.waitForInreqLines(t, 1, 3*time.Second)
	lines := tt.inreqLines(t)
	if len(lines) != 1 {
		t.Fatalf("want one inreq line, got %d", len(lines))
	}
	var env ExchangeEnvelopes
	if err := json.Unmarshal(lines[0].EventData, &env); err != nil {
		t.Fatal(err)
	}
	if env.FinalStatus != FinalCancelled {
		t.Fatalf("final status = %q, want %q", env.FinalStatus, FinalCancelled)
	}
	if !bytes.Equal(env.Res, resRaw) {
		t.Fatalf("the diarium res is not the file's bytes as exchanged:\n file: %s\n diarium: %s", resRaw, env.Res)
	}
	if !bytes.Equal(env.Can, canRaw) {
		t.Fatalf("the diarium can is null or re-marshalled, want the file's bytes:\n file: %s\n diarium: %s", canRaw, env.Can)
	}
	if !isJSONNull(env.Req) || !isJSONNull(env.Con) {
		t.Fatalf("the absent envelopes must be null: req=%s con=%s", env.Req, env.Con)
	}
}

// TestRecvStaleResponseAtStartDoesNotRunWork is the start state a previous run can leave: the
// response of the rid and its request are both in the inbox. The work must not run (the exchange
// is already answered); the stale response is applied, the exchange archives, and nothing leaks.
func TestRecvStaleResponseAtStartDoesNotRunWork(t *testing.T) {
	tt := newRecvTest(t, 300*time.Millisecond) // the archive lands on the confirmation timeout
	tt.registerCap(echoCap(), func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
		if _, err := call.PublishAttachment("leak.bin", []byte("leak")); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	})
	rid := tt.newRID()
	resRaw := craftedRes(rid, tt.now())
	tt.writeRawEnvelope(t, string(rid)+".res.json", resRaw)   // the previous run's answer
	tt.writeRequest(t, rid, "echo", json.RawMessage(`{}`), 0) // and its request
	tt.start()
	tt.waitForInreqLines(t, 1, 3*time.Second)
	if tt.calls() != 0 {
		t.Fatalf("the handler ran %d times, want 0: the exchange was already answered", tt.calls())
	}
	entries, err := os.ReadDir(tt.st.folders.forum)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the work's attachments leaked in the forum: %d entries", len(entries))
	}
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
	if !bytes.Equal(env.Res, resRaw) {
		t.Fatalf("the diarium res is not the file's bytes as exchanged:\n file: %s\n diarium: %s", resRaw, env.Res)
	}
}

// TestRecvDeferredEnvelopeKeepsRaw pins the deferred path white-box: an envelope parked before its
// request is tracked must hand its file bytes (not a re-marshal) to the exchange.
func TestRecvDeferredEnvelopeKeepsRaw(t *testing.T) {
	tt := newRecvTest(t, 5*time.Second)
	tt.registerEcho()
	rid := tt.newRID()
	hdr := Header{RID: rid, Src: "peer", Dst: "agent", Datetime: formatISO(tt.now()), Type: TypeRequest, ActioVersion: "1.0"}
	ex := &exchange{rid: rid, header: hdr, phase: phaseAccepting}
	tt.r.exchanges[rid] = ex
	canRaw := craftedCan(rid, tt.now())
	resRaw := craftedRes(rid, tt.now())
	can, err := validateCancellationFile(canRaw)
	if err != nil {
		t.Fatalf("the crafted cancellation is not valid: %v", err)
	}
	res, err := validateResponseFile(resRaw)
	if err != nil {
		t.Fatalf("the crafted response is not valid: %v", err)
	}
	tt.r.parkDeferred(rid, deferredEnv{kind: BodyCancellation, can: can, raw: canRaw})
	tt.r.parkDeferred(rid, deferredEnv{kind: BodyResponse, res: res, raw: resRaw})
	tt.r.drainDeferred(ex)
	if !bytes.Equal(ex.canRaw, canRaw) {
		t.Fatalf("the deferred can is not the file's bytes as exchanged:\n file: %s\n exchange: %s", canRaw, ex.canRaw)
	}
	if !bytes.Equal(ex.resRaw, resRaw) {
		t.Fatalf("the deferred res is not the file's bytes as exchanged:\n file: %s\n exchange: %s", resRaw, ex.resRaw)
	}
}
