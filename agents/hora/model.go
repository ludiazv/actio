// model.go makes the two model calls (D-7, D-8) and the decision gate
// (D-9, D-14), per Hora.md, Setting a schedule and rejections.
package main

import (
	"context"

	"github.com/MichaelKinsy/PiG/ai"
)

// models holds the resolved chat and decision model settings (D-8).
type models struct {
	model         string
	url           string
	apiKey        string
	decisionModel string
	decisionURL   string
	decisionKey   string
}

// newModels builds the model calls from the resolved config.
func newModels(cfg Config) *models {
	panic("not implemented: newModels")
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
var gates []gate

// findGate looks a gate up by name.
func findGate(name string) (gate, bool) {
	panic("not implemented: findGate")
}

// decide runs the decision step through the configured gate (D-9).
func (m *models) decide(ctx context.Context, prompt, capContext string, capNames []string) (gateVerdict, error) {
	panic("not implemented: decide")
}

// complete runs one extraction round against the chat model and returns the
// reply text (D-7, D-8).
func (m *models) complete(ctx context.Context, system, user string) (string, error) {
	panic("not implemented: complete")
}

// hasDecision reports whether a decision model is configured.
func (m *models) hasDecision() bool {
	panic("not implemented: hasDecision")
}
