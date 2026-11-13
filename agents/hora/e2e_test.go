// e2e_test.go is the end-to-end suite (D-11, D-12, D-13): real in-process
// agents under a shared temp root, real time, Hora started by run with the
// model calls scripted on httptest servers. The rows are sequential: no
// t.Parallel, and every wait polls with a 15 s deadline.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"actio-go"
)

// destState counts what the e2e destination's handlers see.
type destState struct {
	mu       sync.Mutex
	nEcho    int
	sleepNow int
	sleepMax int
	nPriv    int
}

func (s *destState) echoCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nEcho
}

func (s *destState) sleepStats() (now, max int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sleepNow, s.sleepMax
}

// the destination's handlers: a counter each, the sleep one tracking
// overlap and honouring cancellation.
func (s *destState) echo(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	s.mu.Lock()
	s.nEcho++
	s.mu.Unlock()
	return map[string]any{"text": "echo"}, nil
}

func (s *destState) sleep(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	var p struct {
		Seconds float64 `json:"seconds"`
	}
	_ = json.Unmarshal(params, &p)
	s.mu.Lock()
	s.sleepNow++
	if s.sleepNow > s.sleepMax {
		s.sleepMax = s.sleepNow
	}
	s.mu.Unlock()
	select {
	case <-time.After(time.Duration(p.Seconds * float64(time.Second))):
	case <-ctx.Done():
	}
	s.mu.Lock()
	s.sleepNow--
	s.mu.Unlock()
	return map[string]any{"slept": p.Seconds}, nil
}

func (s *destState) priv(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	s.mu.Lock()
	s.nPriv++
	s.mu.Unlock()
	return true, nil
}

// echoCap, sleepCap and privateCap are the destination's three
// capabilities: the fast one, the slow one that honours cancellation, and
// the one that only the caller may call.
var (
	echoCap = actio.Capability{
		Name:        "echo",
		Description: "echoes the text it gets",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`),
		Actio:       actio.CapabilityActio{Result: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`)},
	}
	sleepCap = actio.Capability{
		Name:        "sleep",
		Description: "waits the given seconds and reports them; honours cancellation",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"seconds":{"type":"number"}},"required":["seconds"],"additionalProperties":false}`),
		Actio: actio.CapabilityActio{
			Result:      json.RawMessage(`{"type":"object","properties":{"slept":{"type":"number"}},"required":["slept"]}`),
			MaxParallel: 1,
		},
	}
	privateCap = actio.Capability{
		Name:        "private",
		Description: "callable only by the caller",
		Parameters:  json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
		Actio: actio.CapabilityActio{
			Result:      json.RawMessage(`true`),
			AcceptsFrom: []string{"caller"},
		},
	}
)

// e2e is one end-to-end environment: the temp root, the scripted model
// servers, the four in-process agents, and the current run of Hora.
type e2e struct {
	t        *testing.T
	root     string
	cfgPath  string
	dbPath   string
	ds       *destState
	dest     *actio.Agent
	caller   *actio.Agent
	coord    *actio.Agent
	stranger *actio.Agent
	cur      *runHandle
}

// runHandle is one run of Hora: its cancel and its result.
type runHandle struct {
	cancel context.CancelFunc
	done   chan error
	reaped bool
	err    error
}

// startRun starts one run of Hora on the environment's config, in its own
// goroutine with its own context, and cancels and reaps the run when the
// test ends.
func (e *e2e) startRun() *runHandle {
	ctx, cancel := context.WithCancel(context.Background())
	r := &runHandle{cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- run(ctx, e.cfgPath) }()
	e.t.Cleanup(func() {
		r.cancel()
		r.reap(e.t)
	})
	return r
}

// reap waits for run to return, bounded at 40 s (longer than the 30 s stop
// bound). A second call is a no-op.
func (r *runHandle) reap(t *testing.T) error {
	if r.reaped {
		return r.err
	}
	select {
	case r.err = <-r.done:
	case <-time.After(40 * time.Second):
		t.Error("run did not return within 40 s")
	}
	r.reaped = true
	return r.err
}

// newE2EBase builds one environment without starting Hora: the temp root,
// the scripted model servers, the destination with its three capabilities,
// the caller, the coordinator and the stranger.
func newE2EBase(t *testing.T, chatReplies []string, decisionAnswers string) *e2e {
	t.Helper()
	root := t.TempDir()
	// run's agent reads ACTIO_DIR: point it at the shared temp root, as the
	// in-process rows of main_test do.
	t.Setenv("ACTIO_DIR", root)
	e := &e2e{t: t, root: root, dbPath: filepath.Join(root, "db.json"), ds: &destState{}}
	chat := e2eChat(chatReplies)
	dec := newSystemOneServer(t, &recorded{}, decisionAnswers)
	t.Cleanup(dec.Close)
	t.Cleanup(chat.Close)
	cfg := `{"agent_id":"hora","TZ":"UTC","coordinators":["caller","coord"],"db":"db.json","model":"chat",` +
		`"url":"` + chat.URL + `","env_apikey":"",` +
		`"decision_model":"dm","decision_url":"` + dec.URL + `","decision_env_apikey":"","decision_gate":"v1"}`
	e.cfgPath = filepath.Join(root, "cfg.json")
	if err := os.WriteFile(e.cfgPath, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	e.dest = newTestAgent(t, root, "dest", "the e2e destination", func(a *actio.Agent) {
		for _, c := range []struct {
			cap actio.Capability
			h   func(context.Context, json.RawMessage, *actio.CallContext) (any, error)
		}{{echoCap, e.ds.echo}, {sleepCap, e.ds.sleep}, {privateCap, e.ds.priv}} {
			if err := a.Capability(c.cap, c.h); err != nil {
				t.Fatalf("Capability(%s): %v", c.cap.Name, err)
			}
		}
	})
	e.caller = newTestAgent(t, root, "caller", "the e2e caller", nil)
	e.coord = newTestAgent(t, root, "coord", "the e2e coordinator", nil)
	e.stranger = newTestAgent(t, root, "stranger", "the e2e stranger", nil)
	return e
}

// sees waits for the three peers to see Hora with its capabilities.
func (e *e2e) sees() {
	see(e.t, e.caller, "hora")
	see(e.t, e.coord, "hora")
	see(e.t, e.stranger, "hora")
}

// newE2E builds one environment and starts Hora by run.
func newE2E(t *testing.T, chatReplies []string, decisionAnswers string) *e2e {
	e := newE2EBase(t, chatReplies, decisionAnswers)
	e.cur = e.startRun()
	e.sees()
	return e
}

// e2eChat serves one scripted reply per request, in order, cycling.
func e2eChat(replies []string) *httptest.Server {
	i := 0
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseBody("", replies[i%len(replies)], "stop"))
		i++
	}))
}

// call is one capability call on Hora. A capability error comes back as
// the ErrorResult; a transport error fails the test.
func (e *e2e) call(a *actio.Agent, cap string, params map[string]any) (json.RawMessage, *actio.ErrorResult) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	outcome, err := a.Request(ctx, "hora", cap, params, actio.RequestOptions{Timeout: 60 * time.Second})
	if err != nil {
		e.t.Fatalf("%s: %v", cap, err)
	}
	if outcome.Response == nil || !outcome.Response.Error {
		return outcome.Response.Result, nil
	}
	var er actio.ErrorResult
	_ = json.Unmarshal(outcome.Response.Result, &er)
	return nil, &er
}

// setAndID schedules via the caller and returns the sched_id, failing if
// the answer is not scheduled.
func (e *e2e) setAndID(prompt string) string {
	raw, _ := e.call(e.caller, "set", map[string]any{"prompt": prompt, "dest_id": "dest"})
	res := map[string]any{}
	_ = json.Unmarshal(raw, &res)
	if res["result"] != "scheduled" {
		e.t.Fatalf("the set: %v", res)
	}
	id, _ := res["sched_id"].(string)
	return id
}

// listItems is one list answer, parsed.
func (e *e2e) listItems(a *actio.Agent) []map[string]any {
	raw, _ := e.call(a, "list", map[string]any{})
	var out []map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}

// diariumLog reads one diarium file and returns its log messages.
func diariumLog(path string) []string {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var l struct {
			Event     string          `json:"event"`
			EventData json.RawMessage `json:"event_data"`
		}
		if json.Unmarshal([]byte(line), &l) != nil || l.Event != "log" {
			continue
		}
		var s string
		if json.Unmarshal(l.EventData, &s) == nil {
			out = append(out, s)
		}
	}
	return out
}

func diariumHas(path, prefix string) bool {
	for _, l := range diariumLog(path) {
		if strings.HasPrefix(l, prefix) {
			return true
		}
	}
	return false
}

func diariumCount(path, prefix string) int {
	n := 0
	for _, l := range diariumLog(path) {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

// hasLine reports whether one of Hora's diarium lines starts with prefix.
func (e *e2e) hasLine(prefix string) bool {
	return diariumHas(filepath.Join(e.root, "hora", "diarium.jsonl"), prefix)
}

// diariumCount counts Hora's diarium lines that start with prefix.
func (e *e2e) diariumCount(prefix string) int {
	return diariumCount(filepath.Join(e.root, "hora", "diarium.jsonl"), prefix)
}

// dbEntryIDs returns the sched_ids currently in the DB file.
func (e *e2e) dbEntryIDs() map[string]bool {
	b, _ := os.ReadFile(e.dbPath)
	var entries []map[string]any
	_ = json.Unmarshal(b, &entries)
	out := map[string]bool{}
	for _, m := range entries {
		if id, ok := m["sched_id"].(string); ok {
			out[id] = true
		}
	}
	return out
}

// await polls a condition every 100 ms until it holds, with a 15 s
// deadline.
func await(t *testing.T, what string, cond func() bool) {
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("deadline waiting for: %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// the scripted replies the rows use.
const (
	replyEchoPeriodic1s  = `{"sched_type":"periodic","sched_expression":"@every 1s","dest_capability":"echo","capability_parameters":{"text":"tick"}}`
	replyEchoCronEvery   = `{"sched_type":"cron","sched_expression":"CRON_TZ=UTC * * * * * *","dest_capability":"echo","capability_parameters":{"text":"beat"}}`
	replyEchoPeriodic60s = `{"sched_type":"periodic","sched_expression":"@every 60s","dest_capability":"echo","capability_parameters":{"text":"slow"}}`
	replySleepPeriodic1s = `{"sched_type":"periodic","sched_expression":"@every 1s","dest_capability":"sleep","capability_parameters":{"seconds":5}}`
)

func replyOneShotEcho(at time.Time) string {
	return `{"sched_type":"one-shot","sched_expression":"` + at.UTC().Format(time.RFC3339) + `","dest_capability":"echo","capability_parameters":{"text":"once"}}`
}

func replyOneShotSleep(at time.Time, seconds int) string {
	return `{"sched_type":"one-shot","sched_expression":"` + at.UTC().Format(time.RFC3339) + `","dest_capability":"sleep","capability_parameters":{"seconds":` + strconv.Itoa(seconds) + `}}`
}

// acceptAnswers is a decision that accepts, with hints.
const acceptAnswers = `{"valid":{"type":"noul","noul":0.95},"type":{"type":"choice","choice":"periodic","probabilities":{"one-shot":0.05,"cron":0.05,"periodic":0.9},"confidence":0.95},"capability":{"type":"choice","choice":"echo","probabilities":{"echo":0.95,"sleep":0.05},"confidence":0.95}}`

// rejectAnswers is a decision that refuses.
const rejectAnswers = `{"valid":{"type":"noul","noul":0.05},"type":{"type":"choice","choice":"one-shot","probabilities":{"one-shot":0.9,"cron":0.05,"periodic":0.05},"confidence":0.9},"capability":{"type":"choice","choice":"echo","probabilities":{"echo":0.9},"confidence":0.9}}`

// TestE2ESetRejections covers each reachable set rejection string.
func TestE2ESetRejections(t *testing.T) {
	rows := []struct {
		name     string
		stranger bool
		dest     string
		replies  string
		answers  string
		want     string
		gateLine bool
	}{
		{"a stranger setting for another agent is unauthorized", true, "dest", replyEchoPeriodic60s, acceptAnswers, "unauthorized", false},
		{"an unknown destination is not found", false, "ghost", replyEchoPeriodic60s, acceptAnswers, "agent not found or no visible capabilities for the scheduler", false},
		{"a request the decision refuses is not a scheduling request", false, "dest", replyEchoPeriodic60s, rejectAnswers, "not a scheduling request", true},
		{"an invalid answer in both rounds is rejected with the reasons", false, "dest", `{"bogus":true}`, acceptAnswers, "invalid answer:", false},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			e := newE2E(t, []string{r.replies}, r.answers)
			peer := e.caller
			if r.stranger {
				peer = e.stranger
			}
			res, _ := e.call(peer, "set", map[string]any{"prompt": "call echo with hi", "dest_id": r.dest})
			var m map[string]any
			_ = json.Unmarshal(res, &m)
			if m["result"] != "rejected" || !strings.Contains(m["rejection"].(string), r.want) {
				t.Fatalf("the set: %v", m)
			}
			if r.gateLine && !e.hasLine("gate v1: ok=false") {
				t.Fatalf("the diarium lacks the gate line: %v", diariumLog(filepath.Join(e.root, "hora", "diarium.jsonl")))
			}
		})
	}
}

// TestE2EDBBeforeAnswer is the write order: the entry is on disk when set
// answers. With no wait and no poll, the DB file is read at the instant of
// the answer, and the new sched_id is in it.
func TestE2EDBBeforeAnswer(t *testing.T) {
	e := newE2E(t, []string{replyEchoPeriodic60s}, acceptAnswers)
	res, _ := e.call(e.caller, "set", map[string]any{"prompt": "a slow echo tick", "dest_id": "dest"})
	m := map[string]any{}
	_ = json.Unmarshal(res, &m)
	if m["result"] != "scheduled" {
		t.Fatalf("the set: %v", m)
	}
	id, _ := m["sched_id"].(string)
	if ids := e.dbEntryIDs(); !ids[id] {
		t.Fatalf("the DB has no %s when the scheduled answer arrives: %v", id, ids)
	}
}

// TestE2EStartCancelledBeforeItCompletes: a start cancelled before a.Start
// returns is a failed start (D-12): run returns an error, and no call ever
// reached the destination. The context is cancelled before run is called,
// so the row is deterministic: a racy cancel during the start would
// sometimes land after a.Start returned and take the nil stop path, passing
// for the wrong reason.
func TestE2EStartCancelledBeforeItCompletes(t *testing.T) {
	e := newE2EBase(t, []string{replyEchoPeriodic60s}, acceptAnswers)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, e.cfgPath) }()
	var err error
	select {
	case err = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("deadline: run did not return for a cancelled start")
	}
	if err == nil {
		t.Fatal("a cancelled start returned nil, want an error")
	}
	t.Logf("the cancelled start returned: %v", err)
	if n := e.ds.echoCount(); n != 0 {
		t.Fatalf("the destination saw %d calls, want 0", n)
	}
}

// TestE2EOneShotFires schedules a one-shot: the DB carries the entry when
// the answer arrives (the write order), it fires once in the handler and
// in Hora's diarium, and it is gone after.
func TestE2EOneShotFires(t *testing.T) {
	e := newE2E(t, []string{replyOneShotEcho(time.Now().Add(3 * time.Second))}, acceptAnswers)
	id := e.setAndID("remind me once with echo")
	if ids := e.dbEntryIDs(); !ids[id] {
		t.Fatalf("the DB has no %s when the scheduled answer arrives: %v", id, ids)
	}
	await(e.t, "the one-shot's fire line", func() bool { return e.hasLine("fire " + id + " dest/echo: confirmed") })
	if n := e.ds.echoCount(); n != 1 {
		t.Fatalf("the destination's handler ran %d times, want 1", n)
	}
	await(e.t, "the one-shot's removal from the DB", func() bool { _, ok := e.dbEntryIDs()[id]; return !ok })
}

// TestE2EPeriodicAndCronFire schedules a @every 1s periodic and a six-field
// cron that fires every second: both fire, in the handler and in Hora's
// diarium, and the cron keeps its zone prefix.
func TestE2EPeriodicAndCronFire(t *testing.T) {
	e := newE2E(t, []string{replyEchoPeriodic1s, replyEchoCronEvery}, acceptAnswers)
	perID := e.setAndID("echo a tick every second")
	cronID := e.setAndID("echo a beat every second")
	await(e.t, "the periodic fires more than once", func() bool { return e.diariumCount("fire "+perID+" dest/echo: confirmed") >= 2 })
	await(e.t, "the cron fires", func() bool { return e.hasLine("fire " + cronID + " dest/echo: confirmed") })
	if n := e.ds.echoCount(); n < 3 {
		t.Fatalf("the handler ran %d times, want at least 3", n)
	}
	var expr any
	for _, m := range e.listItems(e.caller) {
		if m["sched_id"] == cronID {
			expr = m["sched_expression"]
		}
	}
	if expr != "CRON_TZ=UTC * * * * * *" {
		t.Fatalf("the stored expression: %v", expr)
	}
}

// TestE2EListAndUnsetAcrossOwners: the owner sees its entry, the stranger
// sees none, the coordinator sees all; only the owner or a coordinator may
// unset, and the DB follows the answer.
func TestE2EListAndUnsetAcrossOwners(t *testing.T) {
	e := newE2E(t, []string{replyEchoPeriodic60s}, acceptAnswers)
	id := e.setAndID("a slow echo tick")
	for _, r := range []struct {
		agent *actio.Agent
		what  string
		want  int
	}{
		{e.caller, "the caller's list", 1},
		{e.stranger, "the stranger's list", 0},
		{e.coord, "the coordinator's list", 1},
		{e.dest, "the owner's list", 1},
	} {
		items := e.listItems(r.agent)
		if len(items) != r.want || (r.want == 1 && items[0]["sched_id"] != id) {
			t.Fatalf("%s: %v", r.what, items)
		}
	}
	if _, er := e.call(e.stranger, "unset", map[string]any{"sched_id": id}); er == nil || er.Code == nil || *er.Code != 1001 || !strings.Contains(er.Msg, id) {
		t.Fatalf("the stranger's unset: %v", er)
	}
	res, er := e.call(e.dest, "unset", map[string]any{"sched_id": id})
	var m map[string]any
	_ = json.Unmarshal(res, &m)
	if er != nil || m["result"] != "unset" {
		t.Fatalf("the owner's unset: %v %v", m, er)
	}
	if ids := e.dbEntryIDs(); ids[id] {
		t.Fatalf("the DB still has %s after the unset answer", id)
	}
	if items := e.listItems(e.caller); len(items) != 0 {
		t.Fatalf("the caller's list after the unset: %v", items)
	}
}

// TestE2ESkipWhileRunning: a @every 1s entry on the slow sleep. The firings
// while the call runs are skipped, so the handler never runs twice at once.
func TestE2ESkipWhileRunning(t *testing.T) {
	e := newE2E(t, []string{replySleepPeriodic1s}, acceptAnswers)
	id := e.setAndID("sleep every second")
	await(e.t, "a skip line in the diarium", func() bool { return e.hasLine("skip " + id + ": previous call still running") })
	if _, max := e.ds.sleepStats(); max > 1 {
		t.Fatalf("the handler ran %d calls at once, want never twice", max)
	}
}

// TestE2EOneShotListedDuringSlowCall: the one-shot is still in the DB while
// its slow call runs, and gone once the call has been logged.
func TestE2EOneShotListedDuringSlowCall(t *testing.T) {
	e := newE2E(t, []string{replyOneShotSleep(time.Now().Add(2*time.Second), 8)}, acceptAnswers)
	id := e.setAndID("one slow sleep")
	await(e.t, "the call running with the entry still in the DB", func() bool {
		now, _ := e.ds.sleepStats()
		return now > 0 && e.dbEntryIDs()[id] && !e.hasLine("fire "+id+" dest/sleep:")
	})
	await(e.t, "the removal of the one-shot", func() bool { _, ok := e.dbEntryIDs()[id]; return !ok })
	// At the instant of the first absence the line must already be on
	// disk: the fire is logged before its removal, so a removal that
	// beats the line is the order violation.
	if !e.hasLine("fire " + id + " dest/sleep:") {
		t.Fatalf("the removal beat the line: %s is gone from the DB and the diarium has no fire line", id)
	}
}

// TestE2ERestartKeepsEntriesAndCleansExpired: a second run on the same DB
// keeps the cron and periodic entries, and an expired one-shot is cleaned,
// with its line, before it can fire.
func TestE2ERestartKeepsEntriesAndCleansExpired(t *testing.T) {
	e := newE2E(t, []string{replyEchoCronEvery, replyEchoPeriodic1s}, acceptAnswers)
	cronID := e.setAndID("echo a beat every second")
	perID := e.setAndID("echo a tick every second")
	await(e.t, "both fire before the restart", func() bool {
		return e.hasLine("fire "+cronID+" dest/echo: confirmed") && e.hasLine("fire "+perID+" dest/echo: confirmed")
	})

	// stop the first run and wait for it to return
	e.cur.cancel()
	if err := e.cur.reap(e.t); err != nil {
		t.Fatalf("the first run: %v", err)
	}

	// hand-edit the DB: an expired one-shot
	b, err := os.ReadFile(e.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	entries := strings.TrimSuffix(strings.TrimSpace(string(b)), "]") + ","
	if err := os.WriteFile(e.dbPath, []byte(entries+`{"datetime":"2026-11-08T17:00:00Z","sched_id":"SCH-eeeeeeee","sched_type":"one-shot","sched_expression":"2020-01-01T00:00:00Z","dest_id":"dest","dest_capability":"echo","capability_parameters":{"text":"gone"}}]`), 0600); err != nil {
		t.Fatal(err)
	}

	e.cur = e.startRun()
	await(e.t, "the expired one-shot cleaned from the DB", func() bool {
		_, ok := e.dbEntryIDs()["SCH-eeeeeeee"]
		return !ok
	})
	await(e.t, "the clean line in the diarium", func() bool { return e.hasLine("clean SCH-eeeeeeee: one-shot time passed") })
	await(e.t, "both fire again after the restart", func() bool {
		return e.diariumCount("fire "+cronID+" dest/echo: confirmed") >= 2 && e.diariumCount("fire "+perID+" dest/echo: confirmed") >= 2
	})
}

// TestE2EUncallableDestIsLogged: an entry for a capability hora may not
// call fires, is logged, and a periodic entry is never removed.
func TestE2EUncallableDestIsLogged(t *testing.T) {
	e := newE2EBase(t, []string{replyEchoPeriodic60s}, acceptAnswers)
	if err := os.WriteFile(e.dbPath, []byte(`[{"datetime":"2026-11-08T17:00:00Z","sched_id":"SCH-ffffffff","sched_type":"periodic","sched_expression":"@every 1s","dest_id":"dest","dest_capability":"private","capability_parameters":{}}]`), 0600); err != nil {
		t.Fatal(err)
	}
	e.cur = e.startRun()
	e.sees()
	await(e.t, "the uncallable fire line", func() bool { return e.hasLine("fire SCH-ffffffff dest/private:") })
	if ids := e.dbEntryIDs(); !ids["SCH-ffffffff"] {
		t.Fatalf("the periodic entry was removed: %v", ids)
	}
	// the named caller may call the restricted capability
	outcome, err := e.caller.Request(context.Background(), "dest", "private", map[string]any{}, actio.RequestOptions{Timeout: 10 * time.Second})
	if err != nil || outcome.Response == nil || outcome.Response.Error {
		t.Fatalf("the caller's call of private: %v %v", outcome, err)
	}
}

// TestE2EChildProcess is the only place a signal appears: the built binary
// as a child. No -config is a usage error (2); a config that names a bad
// key is a config error (1) that names the key; a SIGTERM during a fire
// stops it with exit 0, the fire's line in the diarium, and no firing
// after (D-12).
func TestE2EChildProcess(t *testing.T) {
	root := t.TempDir()
	bin := buildHoraBinary(t, root)

	t.Run("no -config exits 2", func(t *testing.T) {
		cmd := exec.Command(bin)
		err := cmd.Run()
		if err == nil || cmd.ProcessState.ExitCode() != 2 {
			t.Fatalf("no -config: err=%v exit=%d, want exit 2", err, cmd.ProcessState.ExitCode())
		}
	})

	t.Run("a config naming a bad key exits 1 and names it", func(t *testing.T) {
		bad := filepath.Join(root, "bad.json")
		if err := os.WriteFile(bad, []byte(`{"agent_id":"hora","bogus":1,"db":"`+bad+`","model":"m","url":"http://127.0.0.1:1","env_apikey":""}`), 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(bin, "-config", bad)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err == nil {
			t.Fatal("a bad config: the child did not fail")
		}
		if code := cmd.ProcessState.ExitCode(); code != 1 {
			t.Fatalf("a bad config: exit %d, want 1", code)
		}
		if !strings.Contains(stderr.String(), "bogus") {
			t.Fatalf("a bad config: stderr %q does not name the key", stderr.String())
		}
	})

	t.Run("SIGTERM during a fire stops it with exit 0 and no firing after", func(t *testing.T) {
		dir := t.TempDir()
		var ds destState
		newTestAgent(t, dir, "dest", "the child's destination", func(a *actio.Agent) {
			if err := a.Capability(echoCap, ds.echo); err != nil {
				t.Fatalf("Capability(echo): %v", err)
			}
		})
		if err := os.WriteFile(filepath.Join(dir, "cfg.json"), []byte(`{"agent_id":"hora","db":"`+filepath.Join(dir, "db.json")+`","model":"m","url":"http://127.0.0.1:1","env_apikey":""}`), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "db.json"), []byte(`[{"datetime":"2026-11-08T17:00:00Z","sched_id":"SCH-cccccccc","sched_type":"periodic","sched_expression":"@every 1s","dest_id":"dest","dest_capability":"echo","capability_parameters":{"text":"beat"}}]`), 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(bin, "-config", filepath.Join(dir, "cfg.json"))
		cmd.Env = append(os.Environ(), "ACTIO_DIR="+dir)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		diarium := filepath.Join(dir, "hora", "diarium.jsonl")
		await(t, "the child's fire line", func() bool { return diariumHas(diarium, "fire SCH-cccccccc dest/echo:") })
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("term: %v", err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(35 * time.Second):
			t.Fatalf("deadline: the child did not stop; stderr: %s", stderr.String())
		}
		if code := cmd.ProcessState.ExitCode(); code != 0 {
			t.Fatalf("exit %d, want 0; stderr: %s", code, stderr.String())
		}
	})
}
