// model_test.go covers the two model calls (D-7, D-8), the gate registry and
// gate v1 (D-9, D-14), with httptest servers and canned answers only.
package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/MichaelKinsy/PiG/ai"
)

// mkGate builds a gate whose state marks its identity with mark.
func mkGate(name, mark string) gate {
	return gate{
		name:  name,
		state: func(prompt, capContext string) ai.JsonObject { return ai.JsonObject{"mark": mark} },
	}
}

func TestFindGate(t *testing.T) {
	rows := []struct {
		name     string
		query    string
		registry []gate
		wantMark string
		wantOK   bool
	}{
		{name: "present", query: "alpha", registry: []gate{mkGate("alpha", "a"), mkGate("beta", "b")}, wantMark: "a", wantOK: true},
		{name: "absent", query: "nope", registry: []gate{mkGate("alpha", "a"), mkGate("beta", "b")}, wantOK: false},
		{name: "duplicate name returns the first", query: "dup",
			registry: []gate{mkGate("dup", "first"), mkGate("dup", "second"), mkGate("gamma", "g")}, wantMark: "first", wantOK: true},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			saved := gates
			gates = row.registry
			t.Cleanup(func() { gates = saved })

			got, ok := findGate(row.query)
			if ok != row.wantOK {
				t.Fatalf("findGate(%q) ok = %v, want %v", row.query, ok, row.wantOK)
			}
			if !ok {
				return
			}
			if mark := got.state("", "")["mark"]; mark != row.wantMark {
				t.Fatalf("findGate(%q) returned the gate marked %v, want %v", row.query, mark, row.wantMark)
			}
		})
	}
}

// TestFindGateShippedV1 pins that the shipped registry entry is v1 with all
// three of its functions (D-9, D-14).
func TestFindGateShippedV1(t *testing.T) {
	g, ok := findGate("v1")
	if !ok {
		t.Fatal("findGate(v1) = not found, want the shipped gate")
	}
	if g.name != "v1" {
		t.Fatalf("gate name = %q, want v1", g.name)
	}
	if g.state == nil || g.questions == nil || g.verdict == nil {
		t.Fatal("the shipped v1 gate is missing one of its functions")
	}
}

// recorded is what a fake model server saw: the path, the headers and the
// decoded JSON body.
type recorded struct {
	path    string
	headers http.Header
	body    map[string]any
}

func recordRequest(rec *recorded, r *http.Request) {
	rec.path = r.URL.Path
	rec.headers = r.Header.Clone()
	b, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(b, &rec.body)
}

// keys lists the keys of a map, sorted for stable messages.
func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sseBody is one OpenAI chat-completions SSE stream: an optional thinking
// delta, a content delta, a final delta with the finish reason, and [DONE].
func sseBody(thinking, content, finish string) string {
	c, _ := json.Marshal(content)
	b := ""
	if thinking != "" {
		th, _ := json.Marshal(thinking)
		b += "data: {\"choices\":[{\"delta\":{\"reasoning_content\":" + string(th) + "},\"finish_reason\":null}]}\n\n"
	}
	b += "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":" + string(c) + "},\"finish_reason\":null}]}\n\n"
	b += "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"" + finish + "\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n"
	b += "data: [DONE]\n"
	return b
}

// newChatServer serves one scripted chat reply and records the request.
func newChatServer(t *testing.T, rec *recorded, thinking, content, finish string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recordRequest(rec, r)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseBody(thinking, content, finish))
	}))
}

// newSystemOneServer serves one scripted classifier result and records the
// request. answersJSON is the body's answers object.
func newSystemOneServer(t *testing.T, rec *recorded, answersJSON string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recordRequest(rec, r)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"answers":`+answersJSON+`}`)
	}))
}

// testModels builds models against a chat server; key may be "" (D-8).
func testModels(url, key string) *models {
	return newModels(Config{Model: "chat-model", URL: url, APIKey: key, DecisionGate: "v1"})
}

func TestCompleteChat(t *testing.T) {
	t.Run("records the path, the key header and max_completion_tokens", func(t *testing.T) {
		rec := &recorded{}
		srv := newChatServer(t, rec, "", `{"sched_type":"cron","sched_expression":"0 0 * * *","dest_capability":"echo","capability_parameters":{}}`, "stop")
		defer srv.Close()
		m := testModels(srv.URL, "sk-test-key")
		out, err := m.complete(context.Background(), "system prompt", "user prompt")
		if err != nil {
			t.Fatalf("complete: %v", err)
		}
		if out != `{"sched_type":"cron","sched_expression":"0 0 * * *","dest_capability":"echo","capability_parameters":{}}` {
			t.Fatalf("reply text = %q", out)
		}
		if rec.path != "/v1/chat/completions" {
			t.Fatalf("path = %q, want /v1/chat/completions", rec.path)
		}
		t.Logf("recorded headers: %v", rec.headers)
		if got := rec.headers.Get("Authorization"); got != "Bearer sk-test-key" {
			t.Fatalf("Authorization = %q, want %q", got, "Bearer sk-test-key")
		}
		if got := rec.body["max_completion_tokens"]; got != float64(8192) {
			t.Fatalf("max_completion_tokens = %v, want 8192", got)
		}
	})

	t.Run("no key: the server sees no Authorization header", func(t *testing.T) {
		rec := &recorded{}
		srv := newChatServer(t, rec, "", `{"a":1}`, "stop")
		defer srv.Close()
		m := testModels(srv.URL, "")
		if _, err := m.complete(context.Background(), "s", "u"); err != nil {
			t.Fatalf("complete: %v", err)
		}
		t.Logf("recorded headers: %v", rec.headers)
		if got := rec.headers.Get("Authorization"); got != "" {
			t.Fatalf("Authorization = %q, want none", got)
		}
	})

	t.Run("a finish reason other than stop is an error", func(t *testing.T) {
		rec := &recorded{}
		srv := newChatServer(t, rec, "", `{"a":1}`, "length")
		defer srv.Close()
		m := testModels(srv.URL, "sk-test-key")
		if _, err := m.complete(context.Background(), "s", "u"); err == nil {
			t.Fatal("complete succeeded, want an error for a finish reason other than stop")
		}
	})
}

func TestCompleteReplyText(t *testing.T) {
	rows := []struct {
		name     string
		thinking string
		content  string
		want     string
		wantErr  bool
	}{
		{name: "plain JSON", content: `{"a":1}`, want: `{"a":1}`},
		{name: "a plain fence", content: "```{\"a\":1}```", want: `{"a":1}`},
		{name: "a json fence", content: "```json\n{\"a\":1}\n```", want: `{"a":1}`},
		{name: "leading and trailing whitespace", content: "  \n{\"a\":1}\n  ", want: `{"a":1}`},
		{name: "thinking plus text: only the text is kept", thinking: "let me think", content: `{"a":1}`, want: `{"a":1}`},
		{name: "not one JSON object", content: `[1,2]`, wantErr: true},
		{name: "two JSON objects in one reply", content: `{"a":1}{"b":2}`, wantErr: true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			rec := &recorded{}
			srv := newChatServer(t, rec, row.thinking, row.content, "stop")
			defer srv.Close()
			m := testModels(srv.URL, "sk-test-key")
			got, err := m.complete(context.Background(), "s", "u")
			if row.wantErr {
				if err == nil {
					t.Fatalf("complete = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("complete: %v", err)
			}
			if got != row.want {
				t.Fatalf("reply text = %q, want %q", got, row.want)
			}
		})
	}
}

// v1AnswersJSON is a working /v1/systemone answers object (D-8, D-14): the
// bool answer under "noul", the choice probabilities as an object.
const v1AnswersJSON = `{"valid":{"type":"noul","noul":0.94},"type":{"type":"choice","choice":"cron","probabilities":{"one-shot":0.02,"cron":0.9,"periodic":0.08},"confidence":0.9},"capability":{"type":"choice","choice":"backup","probabilities":{"backup":0.9,"echo":0.1},"confidence":0.9}}`

func TestDecide(t *testing.T) {
	t.Run("one systemone request with the v1 state and questions", func(t *testing.T) {
		rec := &recorded{}
		srv := newSystemOneServer(t, rec, v1AnswersJSON)
		defer srv.Close()
		m := newModels(Config{
			Model:         "chat-model",
			URL:           "http://chat.invalid",
			DecisionModel: "decision-model",
			DecisionURL:   srv.URL,
			DecisionGate:  "v1",
		})
		got, err := m.decide(context.Background(), "call backup every day at 13:00", "dest: backup-agent; capabilities: backup, echo", []string{"backup", "echo"})
		if err != nil {
			t.Fatalf("decide: %v", err)
		}
		if rec.path != "/v1/systemone" {
			t.Fatalf("path = %q, want /v1/systemone", rec.path)
		}
		state, ok := rec.body["state"].(map[string]any)
		if !ok {
			t.Fatalf("state = %T, want an object", rec.body["state"])
		}
		if len(state) != 3 || state["request"] == nil || state["destination"] == nil || state["modes"] == nil {
			t.Fatalf("state keys = %v, want exactly request, destination, modes", keys(state))
		}
		questions, ok := rec.body["questions"].(map[string]any)
		if !ok {
			t.Fatalf("questions = %T, want an object", rec.body["questions"])
		}
		for _, id := range []string{"valid", "type", "capability"} {
			if questions[id] == nil {
				t.Fatalf("questions is missing %q: %v", id, keys(questions))
			}
		}
		if !got.OK {
			t.Fatal("verdict = rejected, want accepted")
		}
		if got.Gate != "v1" || got.Score != 0.94 {
			t.Fatalf("verdict gate/score = %q/%v, want v1/0.94", got.Gate, got.Score)
		}
		if got.Hints.Type != "cron" || got.Hints.Capability != "backup" {
			t.Fatalf("hints = %+v, want cron/backup", got.Hints)
		}
	})

	t.Run("one capability: no capability question, the hint is that capability", func(t *testing.T) {
		rec := &recorded{}
		srv := newSystemOneServer(t, rec, `{"valid":{"type":"noul","noul":0.9},"type":{"type":"choice","choice":"periodic","probabilities":{"one-shot":0.05,"cron":0.05,"periodic":0.9},"confidence":0.9}}`)
		defer srv.Close()
		m := newModels(Config{
			Model:         "chat-model",
			URL:           "http://chat.invalid",
			DecisionModel: "decision-model",
			DecisionURL:   srv.URL,
			DecisionGate:  "v1",
		})
		got, err := m.decide(context.Background(), "call echo every 5 minutes", "dest: echo-agent; capabilities: echo", []string{"echo"})
		if err != nil {
			t.Fatalf("decide: %v", err)
		}
		questions, ok := rec.body["questions"].(map[string]any)
		if !ok {
			t.Fatalf("questions = %T, want an object", rec.body["questions"])
		}
		if questions["capability"] != nil {
			t.Fatalf("questions has capability for one capability: %v", keys(questions))
		}
		if got.Hints.Capability != "echo" {
			t.Fatalf("capability hint = %q, want echo", got.Hints.Capability)
		}
	})
}

func TestV1Verdict(t *testing.T) {
	g, ok := findGate("v1")
	if !ok {
		t.Fatal("findGate(v1) = not found, want the shipped gate")
	}
	boolEntry := func(id string, p float64) ai.ClassifierAnswerEntry {
		return ai.ClassifierAnswerEntry{ID: id, Answer: ai.ClassifierBoolAnswer{Probability: p}}
	}
	choiceEntry := func(id, choice string) ai.ClassifierAnswerEntry {
		return ai.ClassifierAnswerEntry{ID: id, Answer: ai.ClassifierChoiceAnswer{Choice: choice, Probabilities: []ai.ClassifierProbability{{Key: choice, Probability: 1}}, Confidence: 1}}
	}
	rows := []struct {
		name      string
		answers   ai.ClassifierAnswers
		caps      []string
		wantOK    bool
		wantScore float64
		wantType  string
		wantCap   string
		wantErr   bool
	}{
		{
			name:    "P(valid) 0.49 rejects",
			answers: ai.ClassifierAnswers{boolEntry("valid", 0.49), choiceEntry("type", "cron")},
			caps:    []string{"backup", "echo"},
			wantOK:  false, wantScore: 0.49,
		},
		{
			name:    "P(valid) 0.5 accepts",
			answers: ai.ClassifierAnswers{boolEntry("valid", 0.5), choiceEntry("type", "cron"), choiceEntry("capability", "backup")},
			caps:    []string{"backup", "echo"},
			wantOK:  true, wantScore: 0.5, wantType: "cron", wantCap: "backup",
		},
		{
			name:    "the hints carry the chosen type and capability",
			answers: ai.ClassifierAnswers{boolEntry("valid", 0.94), choiceEntry("type", "periodic"), choiceEntry("capability", "echo")},
			caps:    []string{"backup", "echo"},
			wantOK:  true, wantScore: 0.94, wantType: "periodic", wantCap: "echo",
		},
		{
			name:    "one capability: the hint is that capability, no answer needed",
			answers: ai.ClassifierAnswers{boolEntry("valid", 0.9), choiceEntry("type", "one-shot")},
			caps:    []string{"solo"},
			wantOK:  true, wantScore: 0.9, wantType: "one-shot", wantCap: "solo",
		},
		{
			name:    "a missing valid answer is an error, not a panic",
			answers: ai.ClassifierAnswers{choiceEntry("type", "cron")},
			caps:    []string{"backup", "echo"},
			wantErr: true,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			got, err := g.verdict(row.answers, row.caps)
			if row.wantErr {
				if err == nil {
					t.Fatalf("verdict = %+v, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("verdict: %v", err)
			}
			if got.OK != row.wantOK {
				t.Fatalf("OK = %v, want %v", got.OK, row.wantOK)
			}
			if got.Score != row.wantScore {
				t.Fatalf("Score = %v, want %v", got.Score, row.wantScore)
			}
			if got.Hints.Type != row.wantType || got.Hints.Capability != row.wantCap {
				t.Fatalf("Hints = %+v, want %q/%q", got.Hints, row.wantType, row.wantCap)
			}
		})
	}
}

// TestDecideUsesConfiguredGate registers a test-only gate and shows that
// newModels with DecisionGate set to its name makes decide run that gate's
// state, questions and verdict (D-9).
func TestDecideUsesConfiguredGate(t *testing.T) {
	rec := &recorded{}
	srv := newSystemOneServer(t, rec, `{"ok":{"type":"noul","noul":0.77}}`)
	defer srv.Close()

	saved := gates
	gates = append(gates, gate{
		name: "testgate",
		state: func(prompt, capContext string) ai.JsonObject {
			return ai.JsonObject{"tag": "testgate-state", "prompt": prompt, "caps": capContext}
		},
		questions: func(capNames []string) ai.ClassifierQuestions {
			return ai.ClassifierQuestions{{ID: "ok", Question: ai.ClassifierBoolQuestion{
				Instructions: "Is this fine?",
				Criteria:     ai.ClassifierBoolCriteria{True: "yes", False: "no"},
			}}}
		},
		verdict: func(a ai.ClassifierAnswers, capNames []string) (gateVerdict, error) {
			return gateVerdict{Gate: "testgate", OK: true, Score: 0.77, Hints: hints{Type: "from-testgate"}}, nil
		},
	})
	t.Cleanup(func() { gates = saved })

	m := newModels(Config{
		Model:         "chat-model",
		URL:           "http://chat.invalid",
		DecisionModel: "decision-model",
		DecisionURL:   srv.URL,
		DecisionGate:  "testgate",
	})
	got, err := m.decide(context.Background(), "a prompt", "a cap context", []string{"a"})
	if err != nil {
		t.Fatalf("decide: %v", err)
	}
	if got.Gate != "testgate" || !got.OK || got.Score != 0.77 || got.Hints.Type != "from-testgate" {
		t.Fatalf("verdict = %+v, want the testgate verdict", got)
	}
	state, ok := rec.body["state"].(map[string]any)
	if !ok || state["tag"] != "testgate-state" {
		t.Fatalf("state = %v, want the testgate state", rec.body["state"])
	}
	questions, ok := rec.body["questions"].(map[string]any)
	if !ok || questions["ok"] == nil {
		t.Fatalf("questions = %v, want the testgate questions", rec.body["questions"])
	}
}
