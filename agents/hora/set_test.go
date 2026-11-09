// set_test.go covers the set pipeline (D-6, D-7, D-9, D-13) and the set
// shell against a real in-process agent.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"actio-go"
)

// testNow is the pipeline tests' clock.
var testNow = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

// echoParams is the test destination's capability schema: an object with a
// required text string and nothing else.
const echoParams = `{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`

// testDestView builds the test destination: an agent with the named
// capabilities, each with the echo schema. The result schema is present
// because actio-go's CheckParams compiles both of a capability's schemas
// and skips the checks when one does not compile (D-19).
func testDestView(caps ...string) actio.AgentView {
	m := map[string]actio.Capability{}
	for _, n := range caps {
		m[n] = actio.Capability{
			Name:        n,
			Description: n + " does its thing",
			Parameters:  json.RawMessage(echoParams),
			Actio:       actio.CapabilityActio{Result: json.RawMessage(`{"type":"object"}`)},
		}
	}
	return actio.AgentView{ID: "dest", Spec: actio.Spec{Description: "the test destination"}, Capabilities: m}
}

// goodReply is a valid extraction for the destination's echo capability.
func goodReply() string {
	return `{"sched_type":"periodic","sched_expression":"@every 30m","dest_capability":"echo","capability_parameters":{"text":"hi"}}`
}

// newTestHora builds a hora for the pipeline tests: the given clock, a
// store in a temp dir, a runner that fires nowhere, and the models pointed
// at the given servers (decisionURL empty means no decision model). It
// returns the hora and the store's DB path.
func newTestHora(t *testing.T, chatURL, decisionURL string, now time.Time) (*hora, string) {
	t.Helper()
	cfg := Config{
		TZ:           "UTC",
		Loc:          time.UTC,
		Model:        "chat-model",
		URL:          chatURL,
		DecisionGate: "v1",
	}
	if decisionURL != "" {
		cfg.DecisionModel = "decision-model"
		cfg.DecisionURL = decisionURL
	}
	dbPath := filepath.Join(t.TempDir(), "db.json")
	s, _, err := openStore(dbPath, now)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	return &hora{
		cfg:    cfg,
		store:  s,
		run:    newRunner(func(string) {}),
		models: newModels(cfg),
		now:    func() time.Time { return now },
	}, dbPath
}

// mustStore builds a store in a temp dir.
func mustStore(t *testing.T) *store {
	t.Helper()
	s, _, err := openStore(filepath.Join(t.TempDir(), "db.json"), testNow)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	return s
}

// recordedList records every request body a server sees.
type recordedList struct {
	bodies []map[string]any
}

func (rl *recordedList) add(r *http.Request) {
	var b map[string]any
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &b)
	rl.bodies = append(rl.bodies, b)
}

// messageContents is the request's messages, in order, content as strings.
func messageContents(t *testing.T, body map[string]any) []string {
	t.Helper()
	msgs, _ := body["messages"].([]any)
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		mm, _ := m.(map[string]any)
		c, _ := mm["content"].(string)
		out = append(out, c)
	}
	return out
}

// scriptedChat serves one reply per request, in order, and records them.
func scriptedChat(t *testing.T, replies ...string) (*httptest.Server, *recordedList) {
	t.Helper()
	rl := &recordedList{}
	i := 0
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rl.add(r)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseBody("", replies[i%len(replies)], "stop"))
		i++
	})), rl
}

// TestSetValidation is D-6's table: every check runs, every reason is
// collected, and the rejection carries them all.
func TestSetValidation(t *testing.T) {
	logf := func(string, ...any) {}
	ctx := context.Background()
	rows := []struct {
		name    string
		reply   string
		wantSub []string
	}{
		{
			name:  "a good reply is scheduled",
			reply: goodReply(),
		},
		{
			name:  "an unknown sched_type",
			reply: `{"sched_type":"weekly","sched_expression":"@every 30m","dest_capability":"echo","capability_parameters":{"text":"hi"}}`,
			wantSub: []string{
				`sched_type "weekly": not one-shot, cron or periodic`,
			},
		},
		{
			name:  "a one-shot with a fractional second",
			reply: `{"sched_type":"one-shot","sched_expression":"2026-12-01T09:30:00.5Z","dest_capability":"echo","capability_parameters":{"text":"hi"}}`,
			wantSub: []string{
				"sched_expression: one-shot 2026-12-01T09:30:00.5Z: fractional second",
			},
		},
		{
			name:  "a one-shot not after now",
			reply: `{"sched_type":"one-shot","sched_expression":"2026-11-08T11:59:59Z","dest_capability":"echo","capability_parameters":{"text":"hi"}}`,
			wantSub: []string{
				"sched_expression: one-shot 2026-11-08T11:59:59Z: not after now",
			},
		},
		{
			name:  "a cron with five fields",
			reply: `{"sched_type":"cron","sched_expression":"5 6 7","dest_capability":"echo","capability_parameters":{"text":"hi"}}`,
			wantSub: []string{
				"sched_expression: cron 5 6 7:",
			},
		},
		{
			name:  "a periodic under a second",
			reply: `{"sched_type":"periodic","sched_expression":"@every 500ms","dest_capability":"echo","capability_parameters":{"text":"hi"}}`,
			wantSub: []string{
				"sched_expression: periodic @every 500ms: less than 1s",
			},
		},
		{
			name:  "an unknown capability",
			reply: `{"sched_type":"periodic","sched_expression":"@every 30m","dest_capability":"nope","capability_parameters":{"text":"hi"}}`,
			wantSub: []string{
				`dest_capability "nope": not a visible capability of the destination`,
			},
		},
		{
			name:  "parameters that are not an object",
			reply: `{"sched_type":"periodic","sched_expression":"@every 30m","dest_capability":"echo","capability_parameters":"hi"}`,
			wantSub: []string{
				"capability_parameters: not an object",
			},
		},
		{
			name:  "parameters that fail the schema",
			reply: `{"sched_type":"periodic","sched_expression":"@every 30m","dest_capability":"echo","capability_parameters":{}}`,
			wantSub: []string{
				"capability_parameters:",
			},
		},
		{
			name:  "three failures report all three",
			reply: `{"sched_type":"weekly","sched_expression":"5 6 7","dest_capability":"echo","capability_parameters":"hi"}`,
			wantSub: []string{
				`sched_type "weekly": not one-shot, cron or periodic`,
				"sched_expression:",
				"capability_parameters: not an object",
			},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			srv := newChatServer(t, &recorded{}, "", row.reply, "stop")
			defer srv.Close()
			h, _ := newTestHora(t, srv.URL, "", testNow)
			out, err := h.scheduleFor(ctx, "dest", "call echo every 30 minutes", testDestView("echo"), logf)
			if err != nil {
				t.Fatalf("scheduleFor: %v", err)
			}
			res, _ := out.(map[string]any)
			if row.wantSub == nil {
				if res["result"] != "scheduled" {
					t.Fatalf("result = %v, want scheduled: %v", res["result"], res)
				}
				return
			}
			if res["result"] != "rejected" {
				t.Fatalf("result = %v, want rejected: %v", res["result"], res)
			}
			rej, _ := res["rejection"].(string)
			if !strings.HasPrefix(rej, "invalid answer: ") {
				t.Fatalf("rejection %q lacks the prefix invalid answer:", rej)
			}
			for _, sub := range row.wantSub {
				if !strings.Contains(rej, sub) {
					t.Fatalf("rejection %q lacks %q", rej, sub)
				}
			}
		})
	}
}

// TestSetTwoRounds is D-7's round 2: a fresh request with the original
// prompt, the first reply and the failure reasons, and the same system.
func TestSetTwoRounds(t *testing.T) {
	prompt := "call echo every 30 minutes"
	badReply := `{"sched_type":"cron","sched_expression":"5 6 7","dest_capability":"echo","capability_parameters":{"text":"hi"}}`
	srv, rl := scriptedChat(t, badReply, goodReply())
	defer srv.Close()
	h, _ := newTestHora(t, srv.URL, "", testNow)
	out, err := h.scheduleFor(context.Background(), "dest", prompt, testDestView("echo"), func(string, ...any) {})
	if err != nil {
		t.Fatalf("scheduleFor: %v", err)
	}
	res, _ := out.(map[string]any)
	if res["result"] != "scheduled" {
		t.Fatalf("result = %v, want scheduled: %v", res["result"], res)
	}
	if len(rl.bodies) != 2 {
		t.Fatalf("the model saw %d requests, want 2", len(rl.bodies))
	}
	first := messageContents(t, rl.bodies[0])
	second := messageContents(t, rl.bodies[1])
	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("each request wants exactly a system and a user message: %v, %v", first, second)
	}
	// The user message of round 1 is the original prompt, verbatim.
	if first[1] != prompt {
		t.Fatalf("round 1 user = %q, want the original prompt %q", first[1], prompt)
	}
	// The system is the same in both rounds, and it carries D-7's items.
	if first[0] != second[0] {
		t.Fatalf("round 2's system differs from round 1's")
	}
	system := first[0]
	for _, sub := range []string{
		"JSON only",
		"exactly the keys sched_type, sched_expression, dest_capability, capability_parameters",
		"one-shot, cron, periodic",
		"RFC 3339",
		"six fields (sec min hour dom month dow)",
		"@every",
		"2026-11-08T12:00:00Z UTC",
		"destination dest: the test destination",
	} {
		if !strings.Contains(system, sub) {
			t.Fatalf("the system prompt lacks %q: %s", sub, system)
		}
	}
	// Round 2 is a fresh request: its user message carries the original
	// prompt, the first reply and the failure reasons.
	user := second[1]
	for _, sub := range []string{prompt, badReply, "cron 5 6 7:"} {
		if !strings.Contains(user, sub) {
			t.Fatalf("round 2 user lacks %q: %s", sub, user)
		}
	}
}

// TestSetModelError: a model that fails on both rounds rejects with the
// model error as the reason.
func TestSetModelError(t *testing.T) {
	srv := newChatServer(t, &recorded{}, "", `{"a":1}`, "length")
	defer srv.Close()
	h, _ := newTestHora(t, srv.URL, "", testNow)
	out, err := h.scheduleFor(context.Background(), "dest", "call echo every 30 minutes", testDestView("echo"), func(string, ...any) {})
	if err != nil {
		t.Fatalf("scheduleFor: %v", err)
	}
	res, _ := out.(map[string]any)
	if res["result"] != "rejected" {
		t.Fatalf("result = %v, want rejected: %v", res["result"], res)
	}
	rej, _ := res["rejection"].(string)
	if !strings.HasPrefix(rej, "invalid answer: model error: model stopped with \"length\"") {
		t.Fatalf("rejection %q lacks the model error", rej)
	}
}

// TestSetGate is D-9's line: the shipped gate v1, selected through
// decision_gate, drives the pipeline's steps 4–9 through logf, with no
// change to non-test code: an accepting verdict goes on with hints, a
// rejecting verdict stops at step 4, a failed decision call goes on
// without hints, and no decision model means no gate line.
func TestSetGate(t *testing.T) {
	ctx := context.Background()
	view := testDestView("echo")

	// canned is a scripted /v1/systemone server with the given answers.
	run := func(t *testing.T, answersJSON string) (map[string]any, []string) {
		t.Helper()
		var decisionSrv *httptest.Server
		rec := &recorded{}
		if answersJSON != "" {
			decisionSrv = newSystemOneServer(t, rec, answersJSON)
			defer decisionSrv.Close()
		}
		chatSrv := newChatServer(t, &recorded{}, "", goodReply(), "stop")
		defer chatSrv.Close()
		var lines []string
		logf := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
		h, _ := newTestHora(t, chatSrv.URL, "", testNow)
		if answersJSON != "" {
			h.cfg.DecisionModel = "d1-2b"
			h.cfg.DecisionURL = decisionSrv.URL
		}
		h.models = newModels(h.cfg)
		out, err := h.scheduleFor(ctx, "dest", "call echo every 30 minutes", view, logf)
		if err != nil {
			t.Fatalf("scheduleFor: %v", err)
		}
		res, _ := out.(map[string]any)
		return res, lines
	}

	t.Run("an accepting verdict goes on with hints", func(t *testing.T) {
		res, lines := run(t, `{"valid":{"type":"noul","noul":0.87},"type":{"type":"choice","choice":"periodic","probabilities":{"one-shot":0.05,"cron":0.05,"periodic":0.9},"confidence":0.9}}`)
		if res["result"] != "scheduled" {
			t.Fatalf("result = %v, want scheduled: %v", res["result"], res)
		}
		want := "gate v1: ok=true score=0.870 type=periodic capability=echo"
		if len(lines) != 1 || lines[0] != want {
			t.Fatalf("lines = %v, want exactly [%q]", lines, want)
		}
	})

	t.Run("a rejecting verdict stops at step 4", func(t *testing.T) {
		res, lines := run(t, `{"valid":{"type":"noul","noul":0.10},"type":{"type":"choice","choice":"periodic","probabilities":{"one-shot":0.05,"cron":0.05,"periodic":0.9},"confidence":0.9}}`)
		if res["result"] != "rejected" || res["rejection"] != "not a scheduling request" {
			t.Fatalf("res = %v, want rejected: not a scheduling request", res)
		}
		want := "gate v1: ok=false score=0.100 type= capability="
		if len(lines) != 1 || lines[0] != want {
			t.Fatalf("lines = %v, want exactly [%q]", lines, want)
		}
	})

	t.Run("a failed decision call goes on without hints", func(t *testing.T) {
		// An answer the gate does not have is an error, not a panic.
		res, lines := run(t, `{}`)
		if res["result"] != "scheduled" {
			t.Fatalf("result = %v, want scheduled: %v", res["result"], res)
		}
		want := `gate v1: failed: decision stopped with "error": System One API did not return an answer for valid`
		if len(lines) != 1 || lines[0] != want {
			t.Fatalf("lines = %v, want exactly [%q]", lines, want)
		}
	})

	t.Run("no decision model means no gate line", func(t *testing.T) {
		res, lines := run(t, "")
		if res["result"] != "scheduled" {
			t.Fatalf("result = %v, want scheduled: %v", res["result"], res)
		}
		if len(lines) != 0 {
			t.Fatalf("lines = %v, want none", lines)
		}
	})
}

// newTestAgent builds, registers and starts one in-process agent under the
// shared temp root, and stops it (forced) when the test ends. Discovery is
// through the forum under the root, so the agents of one test share it.
func newTestAgent(t *testing.T, root, id, desc string, register func(a *actio.Agent)) *actio.Agent {
	t.Helper()
	a, err := actio.New(actio.Options{
		ID:          actio.AgentID(id),
		Description: desc,
		Config:      actio.Config{Root: root},
	})
	if err != nil {
		t.Fatalf("actio.New(%s): %v", id, err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.Stop(sctx, actio.StopOptions{Force: true}); err != nil {
			t.Errorf("Stop(%s): %v", id, err)
		}
	})
	if register != nil {
		register(a)
	}
	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start(%s): %v", id, err)
	}
	return a
}

// seen waits, with a deadline, for the other agent to appear in the
// agent's view.
func seen(t *testing.T, a *actio.Agent, other string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		views, err := a.Refresh(context.Background())
		if err == nil {
			if _, ok := views[actio.AgentID(other)]; ok {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("deadline: the agent never saw %s", other)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// see waits, with a deadline, for the agent to see the other in its view,
// with the other's capabilities visible.
func see(t *testing.T, a *actio.Agent, other string) {
	t.Helper()
	seen(t, a, other)
	deadline := time.Now().Add(15 * time.Second)
	for {
		views, err := a.Refresh(context.Background())
		if err == nil {
			if v, ok := views[actio.AgentID(other)]; ok && len(v.Capabilities) > 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("deadline: the agent never saw %s with capabilities", other)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestSetShell runs the set shell's steps 1–3 against a real in-process
// destination agent: the authorization, the missing destination, the
// destination with no visible capabilities, and the handoff to the
// pipeline with the real view.
func TestSetShell(t *testing.T) {
	t.Setenv("ACTIO_HEARTBEAT", "1")
	t.Setenv("ACTIO_CONFIRM_TIMEOUT", "2")
	t.Setenv("ACTIO_CANCEL_WAIT", "1")
	ctx := context.Background()

	echo := actio.Capability{
		Name:        "echo",
		Description: "echoes its parameters",
		Parameters:  json.RawMessage(echoParams),
		Actio:       actio.CapabilityActio{Result: json.RawMessage(`{"type":"object"}`)},
	}
	root := t.TempDir()
	newTestAgent(t, root, "dest", "the test destination", func(a *actio.Agent) {
		if err := a.Capability(echo, func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
			return map[string]any{"echoed": true}, nil
		}); err != nil {
			t.Fatalf("the destination's echo: %v", err)
		}
	})
	newTestAgent(t, root, "empty", "an agent with no capabilities", nil)

	srv := newChatServer(t, &recorded{}, "", goodReply(), "stop")
	defer srv.Close()

	cfg := Config{
		TZ:           "UTC",
		Loc:          time.UTC,
		Coordinators: []string{"hora"},
		Model:        "chat-model",
		URL:          srv.URL,
		DecisionGate: "v1",
	}
	var h *hora
	horaAgent := newTestAgent(t, root, "hora", "the test hora", func(a *actio.Agent) {
		h = &hora{
			cfg:    cfg,
			agent:  a,
			store:  mustStore(t),
			run:    newRunner(func(string) {}),
			models: newModels(cfg),
			now:    func() time.Time { return testNow },
		}
		if err := h.publish(a); err != nil {
			t.Fatalf("publish: %v", err)
		}
	})
	see(t, horaAgent, "dest")

	// A stranger cannot set for the destination.
	out, err := h.set(ctx, []byte(`{"prompt":"call echo every 30 minutes","dest_id":"dest"}`), &actio.CallContext{Src: "stranger"})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	res, _ := out.(map[string]any)
	if res["result"] != "rejected" || res["rejection"] != "unauthorized" {
		t.Fatalf("res = %v, want rejected: unauthorized", res)
	}

	// A coordinator's set for a missing destination.
	out, err = h.set(ctx, []byte(`{"prompt":"call echo every 30 minutes","dest_id":"ghost"}`), &actio.CallContext{Src: "hora"})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	res, _ = out.(map[string]any)
	if res["result"] != "rejected" || res["rejection"] != "agent not found or no visible capabilities for the scheduler" {
		t.Fatalf("res = %v, want the not-found rejection", res)
	}

	// A destination with no visible capabilities: actio-go's discovery
	// drops an agent with nothing usable from the view (rule 6), so a set
	// for it answers the same not-found rejection.
	out, err = h.set(ctx, []byte(`{"prompt":"call echo every 30 minutes","dest_id":"empty"}`), &actio.CallContext{Src: "hora"})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	res, _ = out.(map[string]any)
	if res["result"] != "rejected" || res["rejection"] != "agent not found or no visible capabilities for the scheduler" {
		t.Fatalf("res = %v, want the not-found rejection", res)
	}

	// The handoff: the pipeline runs with the real view, and the system
	// prompt carries the cap_context the shell built from it.
	rec := &recorded{}
	srv2 := newChatServer(t, rec, "", goodReply(), "stop")
	defer srv2.Close()
	h.cfg.URL = srv2.URL
	h.models = newModels(h.cfg)
	out, err = h.set(ctx, []byte(`{"prompt":"call echo every 30 minutes","dest_id":"dest"}`), &actio.CallContext{Src: "hora"})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	res, _ = out.(map[string]any)
	if res["result"] != "scheduled" {
		t.Fatalf("res = %v, want scheduled", res)
	}
	system := messageContents(t, rec.body)[0]
	if !strings.Contains(system, "destination dest: the test destination") {
		t.Fatalf("the system prompt lacks the real view's cap_context: %s", system)
	}
	if !strings.Contains(system, "echo (echoes its parameters)") {
		t.Fatalf("the system prompt lacks the capability from the real view: %s", system)
	}
}

// TestSetWritesDBBeforeAnswer is D-13's order for a successful set: when
// the answer returns, the DB file already carries the entry.
func TestSetWritesDBBeforeAnswer(t *testing.T) {
	srv := newChatServer(t, &recorded{}, "", goodReply(), "stop")
	defer srv.Close()
	h, dbPath := newTestHora(t, srv.URL, "", testNow)
	out, err := h.scheduleFor(context.Background(), "dest", "call echo every 30 minutes", testDestView("echo"), func(string, ...any) {})
	if err != nil {
		t.Fatalf("scheduleFor: %v", err)
	}
	res, _ := out.(map[string]any)
	if res["result"] != "scheduled" {
		t.Fatalf("res = %v, want scheduled", res)
	}
	b, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read the DB: %v", err)
	}
	var entries []Entry
	if err := json.Unmarshal(b, &entries); err != nil {
		t.Fatalf("the DB does not parse: %v", err)
	}
	if len(entries) != 1 || entries[0].SchedID != res["sched_id"] {
		t.Fatalf("the DB has %v, want the one scheduled entry", entries)
	}
}

// TestRunnerAddFailsOnPastOneShot is the runner.add failure row: a parse
// that fails in runner.add leaves no entry. The pipeline's own rollback
// branch is a defensive line: store.add runs the same parse with the real
// clock (R1-5), so a past one-shot reaches the DB save as a 1002 first,
// which TestSetPastOneShotLeavesNoEntry covers.
func TestRunnerAddFailsOnPastOneShot(t *testing.T) {
	r := newRunner(func(string) {})
	e := Entry{SchedID: "SCH-00000001", Datetime: "2026-11-08T12:00:00Z", SchedType: "one-shot", SchedExpression: "2020-01-01T00:00:00Z", DestID: "own", DestCapability: "echo", CapabilityParameters: json.RawMessage(`{}`)}
	if err := r.add(e); err == nil {
		t.Fatalf("runner.add accepted a one-shot in the past")
	}
	// The parse failed before the cron saw anything: nothing is scheduled,
	// and removing the unknown id is not an error.
	r.remove(e.SchedID)
	if len(r.ids) != 0 {
		t.Fatalf("the runner has %d entries, want none", len(r.ids))
	}
}

// TestSetPastOneShotLeavesNoEntry: a one-shot that is after the pipeline's
// clock but already in the past for the real clock is refused at the DB
// save with 1002, and the DB carries no entry.
func TestSetPastOneShotLeavesNoEntry(t *testing.T) {
	past := `{"sched_type":"one-shot","sched_expression":"2025-06-01T00:00:00Z","dest_capability":"echo","capability_parameters":{"text":"hi"}}`
	srv := newChatServer(t, &recorded{}, "", past, "stop")
	defer srv.Close()
	h, dbPath := newTestHora(t, srv.URL, "", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	_, err := h.scheduleFor(context.Background(), "dest", "call echo on the first of june", testDestView("echo"), func(string, ...any) {})
	aerr, ok := err.(*actio.Error)
	if !ok {
		t.Fatalf("err = %v, want an actio error", err)
	}
	if aerr.Code != 1002 || !strings.HasPrefix(aerr.Message, "storage: one-shot 2025-06-01T00:00:00Z: not after now") {
		t.Fatalf("err = %+v, want 1002 with the not-after-now parse error", aerr)
	}
	if got := h.store.all(); len(got) != 0 {
		t.Fatalf("the store has %v, want it empty", got)
	}
	b, err := os.ReadFile(dbPath)
	if os.IsNotExist(err) {
		return // a failed add never wrote the file: nothing to clean
	}
	if err != nil {
		t.Fatalf("read the DB: %v", err)
	}
	if string(b) != "[]" {
		t.Fatalf("the DB is %s, want []", b)
	}
}

// TestSetStorageFailure: a DB that will not write answers with 1002.
func TestSetStorageFailure(t *testing.T) {
	srv := newChatServer(t, &recorded{}, "", goodReply(), "stop")
	defer srv.Close()
	h, dbPath := newTestHora(t, srv.URL, "", testNow)
	if _, err := h.scheduleFor(context.Background(), "dest", "first", testDestView("echo"), func(string, ...any) {}); err != nil {
		t.Fatalf("the first set: %v", err)
	}
	if err := os.Chmod(dbPath, 0o444); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	_, err := h.scheduleFor(context.Background(), "dest", "second", testDestView("echo"), func(string, ...any) {})
	aerr, ok := err.(*actio.Error)
	if !ok {
		t.Fatalf("err = %v, want an actio error", err)
	}
	if aerr.Code != 1002 || !strings.HasPrefix(aerr.Message, "storage: ") {
		t.Fatalf("err = %+v, want code 1002 with a storage message", aerr)
	}
}

// TestSetEntryReopensClean is the store invariant a set must keep: an entry
// written by a set reopens with no cleanup at the real time.
func TestSetEntryReopensClean(t *testing.T) {
	srv := newChatServer(t, &recorded{}, "", goodReply(), "stop")
	defer srv.Close()
	h, dbPath := newTestHora(t, srv.URL, "", testNow)
	out, err := h.scheduleFor(context.Background(), "dest", "call echo every 30 minutes", testDestView("echo"), func(string, ...any) {})
	if err != nil {
		t.Fatalf("scheduleFor: %v", err)
	}
	if out.(map[string]any)["result"] != "scheduled" {
		t.Fatalf("res = %v, want scheduled", out)
	}
	s2, cleaned, err := openStore(dbPath, time.Now())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if len(cleaned) != 0 {
		t.Fatalf("the reopen cleaned %v, want nothing", cleaned)
	}
	if got := s2.all(); len(got) != 1 {
		t.Fatalf("the reopened store has %v, want the one entry", got)
	}
}
