// model.go makes the two model calls (D-7, D-8) and the decision gate
// (D-9, D-14), per Hora.md, Setting a schedule and rejections.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/MichaelKinsy/PiG/ai"
)

// models holds the resolved chat and decision model calls (D-8).
type models struct {
	model         string
	baseURL       string
	apiKey        string
	fetch         *http.Client
	decisionModel string
	decisionBase  string
	decisionKey   string
	decisionFetch *http.Client
	gateName      string
}

// stripAuth deletes the Authorization header on every request (D-8).
type stripAuth struct{}

func (stripAuth) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Del("Authorization")
	return http.DefaultTransport.RoundTrip(req)
}

// newModels builds the model calls from the resolved config (D-8). PiG
// refuses an empty key, so an empty key becomes "none" plus a fetch client
// that deletes the Authorization header, so the server sees no header.
func newModels(cfg Config) *models {
	m := &models{
		model:         cfg.Model,
		baseURL:       strings.TrimRight(cfg.URL, "/") + "/v1",
		apiKey:        cfg.APIKey,
		decisionModel: cfg.DecisionModel,
		decisionBase:  strings.TrimRight(cfg.DecisionURL, "/") + "/v1",
		decisionKey:   cfg.DecisionAPIKey,
		gateName:      cfg.DecisionGate,
	}
	if m.apiKey == "" {
		m.apiKey, m.fetch = "none", &http.Client{Transport: stripAuth{}}
	}
	if m.decisionKey == "" {
		m.decisionKey, m.decisionFetch = "none", &http.Client{Transport: stripAuth{}}
	}
	return m
}

// hints are the gate's suggestions for the extraction prompt (D-14).
type hints struct {
	Type       string
	Capability string
}

// gateVerdict is the outcome of the decision step (D-9).
type gateVerdict struct {
	Gate  string
	OK    bool
	Hints hints
	Score float64
}

// gate is one named decision gate: a struct of functions, not an interface (D-9).
type gate struct {
	name      string
	state     func(prompt, capContext string) ai.JsonObject
	questions func(capNames []string) ai.ClassifierQuestions
	verdict   func(a ai.ClassifierAnswers, capNames []string) (gateVerdict, error)
}

// gates is the registry of the decision gates (D-9); v1 is D-14.
var gates = []gate{gateV1}

// findGate looks a gate up by name: the first registry entry with that name.
func findGate(name string) (gate, bool) {
	for _, g := range gates {
		if g.name == name {
			return g, true
		}
	}
	return gate{}, false
}

// decide runs the decision step through the configured gate (D-9).
func (m *models) decide(ctx context.Context, prompt, capContext string, capNames []string) (gateVerdict, error) {
	g, ok := findGate(m.gateName)
	if !ok {
		return gateVerdict{}, fmt.Errorf("unknown decision gate %q", m.gateName)
	}
	res := ai.ClassifyTypesafeSystemOne(ctx, ai.ClassifierModel{ID: m.decisionModel, API: ai.ClassifierAPITypesafeSystemOne, Provider: "hora", BaseURL: m.decisionBase}, ai.ClassifierContext{State: g.state(prompt, capContext), Questions: g.questions(capNames)}, ai.ClassifierOptions{APIKey: m.decisionKey, APIKeySet: true, Fetch: m.decisionFetch})
	if res.StopReason != ai.ClassifierStopReasonStop {
		return gateVerdict{}, fmt.Errorf("decision stopped with %q: %s", res.StopReason, res.ErrorMessage)
	}
	return g.verdict(res.Answers, capNames)
}

// complete runs one extraction round and returns the reply text (D-7, D-8).
func (m *models) complete(ctx context.Context, system, user string) (string, error) {
	msg, err := ai.CompleteSimple(ctx, &ai.Model{ID: m.model, ProviderMeta: ai.ProviderMetadata{ProviderID: "hora", API: ai.APIOpenAICompletions, BaseURL: m.baseURL}}, ai.Context{SystemPrompt: system, Messages: []ai.Message{ai.UserMessage{Content: ai.UserText(user)}}}, ai.StreamOptions{APIKey: m.apiKey, MaxTokens: 8192, Fetch: m.fetch})
	if err != nil {
		return "", err
	}
	if msg.StopReason != ai.StopReasonStop {
		return "", fmt.Errorf("model stopped with %q: %s", msg.StopReason, msg.ErrorMessage)
	}
	text := replyText(msg)
	if !isJSONObject(text) {
		return "", fmt.Errorf("reply is not one JSON object: %s", text)
	}
	return text, nil
}

// hasDecision reports whether a decision model is configured. The config
// validation guarantees the three decision keys are all present or all
// absent, so decision_model alone decides it.
func (m *models) hasDecision() bool {
	return m.decisionModel != ""
}

// replyText joins the reply's text blocks, ignoring thinking, trims, and
// removes one surrounding code fence (D-7).
func replyText(msg *ai.AssistantMessage) string {
	var b strings.Builder
	for _, block := range msg.Content {
		if t, ok := block.(ai.TextContent); ok {
			b.WriteString(t.Text)
		}
	}
	s := strings.TrimSpace(b.String())
	if strings.HasPrefix(s, "```") {
		s = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(s, "```"), "json"), "```"))
	}
	return s
}

// isJSONObject reports whether s decodes as exactly one JSON object.
func isJSONObject(s string) bool {
	var v map[string]json.RawMessage
	return json.Unmarshal([]byte(s), &v) == nil
}

// answerOf finds the answer with id in the list.
func answerOf(a ai.ClassifierAnswers, id string) (ai.ClassifierAnswer, bool) {
	for _, e := range a {
		if e.ID == id {
			return e.Answer, true
		}
	}
	return nil, false
}

// v1ValidCutOff is the P(valid) below which gate v1 rejects (D-14).
const v1ValidCutOff = 0.5

// gateV1 is the shipped gate v1 (D-14).
var gateV1 = gate{name: "v1", state: v1State, questions: v1Questions, verdict: v1Verdict}

// v1State is v1's classification state (D-14).
func v1State(prompt, capContext string) ai.JsonObject {
	return ai.JsonObject{"request": prompt, "destination": capContext, "modes": "one-shot fires once at a given date and time; cron fires on a calendar pattern; periodic fires every fixed interval"}
}

// v1Questions is v1's questions (D-14); capability is asked only when there
// are 2 or more visible capabilities.
func v1Questions(capNames []string) ai.ClassifierQuestions {
	qs := []ai.ClassifierQuestionEntry{
		{ID: "valid", Question: ai.ClassifierBoolQuestion{
			Instructions: "Is the request a valid scheduling request?",
			Criteria:     ai.ClassifierBoolCriteria{True: "it asks for something to happen at a time or repeatedly", False: "it does not ask to schedule anything"},
		}},
		{ID: "type", Question: ai.ClassifierChoiceQuestion{
			Instructions: "Which scheduling mode does the request ask for?",
			Criteria: []ai.ClassifierChoiceCriterion{
				{Key: "one-shot", Description: "a specific date or time"},
				{Key: "cron", Description: "a calendar pattern such as day of week or hour"},
				{Key: "periodic", Description: "a fixed interval such as every 30 minutes"},
			},
		}},
	}
	if len(capNames) >= 2 {
		crit := make([]ai.ClassifierChoiceCriterion, 0, len(capNames))
		for _, n := range capNames {
			crit = append(crit, ai.ClassifierChoiceCriterion{Key: n, Description: n})
		}
		qs = append(qs, ai.ClassifierQuestionEntry{ID: "capability", Question: ai.ClassifierChoiceQuestion{
			Instructions: "Which capability does the request ask to call?",
			Criteria:     crit,
		}})
	}
	return qs
}

// v1Verdict is v1's pure verdict (D-14): the score is P(valid), it rejects
// below v1ValidCutOff, and the hints come from the type and capability
// answers.
func v1Verdict(a ai.ClassifierAnswers, capNames []string) (gateVerdict, error) {
	raw, ok := answerOf(a, "valid")
	if !ok {
		return gateVerdict{}, fmt.Errorf("gate v1: missing answer %q", "valid")
	}
	valid, ok := raw.(ai.ClassifierBoolAnswer)
	if !ok {
		return gateVerdict{}, fmt.Errorf("gate v1: answer %q is not a bool", "valid")
	}
	if valid.Probability < v1ValidCutOff {
		return gateVerdict{Gate: "v1", OK: false, Score: valid.Probability}, nil
	}
	typ, ok := choiceAnswer(a, "type")
	if !ok {
		return gateVerdict{}, fmt.Errorf("gate v1: missing choice answer %q", "type")
	}
	capHint := typ
	if len(capNames) == 1 {
		capHint = capNames[0]
	} else {
		capHint, ok = choiceAnswer(a, "capability")
		if !ok {
			return gateVerdict{}, fmt.Errorf("gate v1: missing choice answer %q", "capability")
		}
	}
	return gateVerdict{Gate: "v1", OK: true, Hints: hints{Type: typ, Capability: capHint}, Score: valid.Probability}, nil
}

// choiceAnswer finds the choice answer with id and returns its choice.
func choiceAnswer(a ai.ClassifierAnswers, id string) (string, bool) {
	raw, ok := answerOf(a, id)
	if !ok {
		return "", false
	}
	choice, ok := raw.(ai.ClassifierChoiceAnswer)
	return choice.Choice, ok
}
