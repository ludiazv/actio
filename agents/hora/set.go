// set.go implements the set pipeline (Hora.md, Setting a schedule and
// rejections; D-6, D-7, D-9, D-13).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"actio-go"
	"github.com/MichaelKinsy/PiG/ai"
)

// hora is the agent: its config, the Actio agent, the store, the runner, the
// model calls, and the clock.
type hora struct {
	cfg    Config
	agent  *actio.Agent
	store  *store
	run    *runner
	models *models
	now    func() time.Time
	firing inFlight // D-11's skip and D-12's cancel of a running fire
}

// setParams is set's published parameters.
type setParams struct {
	Prompt string `json:"prompt"`
	DestID string `json:"dest_id"`
}

// set is the shell: dest resolution, the authorization, the view read, then the pipeline.
func (h *hora) set(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	var p setParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	dest := actio.AgentID(p.DestID)
	if dest == "" {
		dest = call.Src
	}
	if dest != call.Src && !h.isCoordinator(call.Src) {
		return rejectedAnswer(dest, "unauthorized")
	}
	if _, err := h.agent.Refresh(ctx); err != nil {
		return nil, err
	}
	view, ok := h.agent.Agmen()[dest]
	if !ok || len(view.Capabilities) == 0 {
		return rejectedAnswer(dest, "agent not found or no visible capabilities for the scheduler")
	}
	logf := func(format string, args ...any) {
		if err := h.agent.Log(fmt.Sprintf(format, args...)); err != nil {
			log.Printf("diarium: %v", err)
		}
	}
	return h.scheduleFor(ctx, dest, p.Prompt, view, logf)
}

// scheduleFor is the pipeline: the decision, the two rounds, the write order. It takes the destination's view data and
// a logf for the diarium lines, never the agent (R1, answer to question 5).
func (h *hora) scheduleFor(ctx context.Context, dest actio.AgentID, prompt string, view actio.AgentView, logf func(format string, args ...any)) (any, error) {
	caps := make([]capInfo, 0, len(view.Capabilities))
	for n, c := range view.Capabilities {
		caps = append(caps, capInfo{Name: n, Description: c.Description, Parameters: string(c.Parameters)})
	}
	sort.Slice(caps, func(i, j int) bool { return caps[i].Name < caps[j].Name })

	// Step 4: the decision, through the configured gate (D-9). A failure or
	// an errored verdict goes on to round 1 without hints.
	var hints hints
	if h.models.hasDecision() {
		verdict, err := h.models.decide(ctx, prompt, caps)
		if err != nil {
			logf("gate %s: failed: %v time=%s", h.cfg.DecisionGate, err, verdict.t)
		} else {
			logf("gate %s: ok=%v score=%.3f type=%s capability=%s time=%s",
				verdict.Gate, verdict.OK, verdict.Score, verdict.Hints.Type, verdict.Hints.Capability, verdict.t)
			if !verdict.OK {
				return rejectedAnswer(dest, "not a scheduling request")
			}
			hints = verdict.Hints
		}
	}

	// Steps 5–7: round 1, D-6's validation, and round 2 as a conversation
	// continuation: the model sees its own first reply as an assistant
	// message before a new user message with the validation errors.
	system := extractSystem(caps)
	user := h.extractUser(prompt, caps, hints)
	reply, err := h.models.complete(ctx, system, user)
	if err != nil {
		reply = ""
	}
	if len(caps) == 1 {
		reply = injectDestCap(reply, caps[0].Name)
	}
	ans, reasons := h.validateReply(reply, view)
	if err != nil {
		reasons = append(reasons, "model error: "+err.Error())
	}
	if len(reasons) > 0 {
		feedback := "Your previous reply failed validation: " + strings.Join(reasons, "; ") +
			"\nReply again with the corrected JSON object only."
		second, err := h.models.completeChat(ctx, system, []ai.Message{
			ai.UserMessage{Content: ai.UserText(user)},
			ai.AssistantMessage{Content: []ai.AssistantContentBlock{ai.TextContent{Text: reply}}},
			ai.UserMessage{Content: ai.UserText(feedback)},
		})
		if err != nil {
			return rejectedAnswer(dest, "invalid answer: model error: "+err.Error())
		}
		if len(caps) == 1 {
			second = injectDestCap(second, caps[0].Name)
		}
		ans, reasons = h.validateReply(second, view)
	}
	if len(reasons) > 0 {
		return rejectedAnswer(dest, "invalid answer: "+strings.Join(reasons, "; "))
	}

	// Steps 8–9: the entry, saved to the DB, then added to the runner (D-13).
	now := h.now()
	_, storedExpr, _ := parseSchedule(ans.SchedType, ans.SchedExpression, h.cfg.Loc, now)
	e := Entry{
		Datetime:             now.UTC().Format(time.RFC3339),
		SchedType:            ans.SchedType,
		SchedExpression:      storedExpr,
		DestID:               string(dest),
		DestCapability:       ans.DestCapability,
		CapabilityParameters: ans.CapabilityParameters,
	}
	stored, err := h.store.add(e)
	if err != nil {
		return nil, &actio.Error{Code: 1002, Message: "storage: " + err.Error()}
	}
	if err := h.run.add(stored); err != nil {
		h.store.remove(stored.SchedID)
		return rejectedAnswer(dest, "invalid answer: "+err.Error())
	}
	return map[string]any{
		"result":                "scheduled",
		"sched_id":              stored.SchedID,
		"dest_id":               stored.DestID,
		"dest_capability":       stored.DestCapability,
		"capability_parameters": stored.CapabilityParameters,
		"sched_expression":      stored.SchedExpression,
	}, nil
}

// scheduleAnswer is the model's extraction (D-7's four keys).
type scheduleAnswer struct {
	SchedType            string          `json:"sched_type"`
	SchedExpression      string          `json:"sched_expression"`
	DestCapability       string          `json:"dest_capability"`
	CapabilityParameters json.RawMessage `json:"capability_parameters"`
}

// validateReply runs D-6's checks on one reply: every check runs and every
// reason is collected, in that order.
func (h *hora) validateReply(reply string, view actio.AgentView) (scheduleAnswer, []string) {
	var ans scheduleAnswer
	if err := json.Unmarshal([]byte(reply), &ans); err != nil {
		return ans, []string{"the reply is not one JSON object: " + err.Error()}
	}
	var reasons []string
	if ans.SchedType != "one-shot" && ans.SchedType != "cron" && ans.SchedType != "periodic" {
		reasons = append(reasons, fmt.Sprintf("sched_type %q: not one-shot, cron or periodic", ans.SchedType))
	}
	if _, _, err := parseSchedule(ans.SchedType, ans.SchedExpression, h.cfg.Loc, h.now()); err != nil {
		reasons = append(reasons, "sched_expression: "+err.Error())
	}
	cap, ok := view.Capabilities[ans.DestCapability]
	if !ok {
		reasons = append(reasons, fmt.Sprintf("dest_capability %q: not a visible capability of the destination", ans.DestCapability))
		return ans, reasons
	}
	p := ans.CapabilityParameters
	if len(p) == 0 || string(p) == "null" {
		p = json.RawMessage("{}")
	}
	var pv any
	if len(p) == 0 || p[0] != '{' || json.Unmarshal(p, &pv) != nil {
		reasons = append(reasons, "capability_parameters: not an object")
	} else if err := actio.CheckParams(cap, pv); err != nil {
		reasons = append(reasons, "capability_parameters: "+err.Error())
	}
	return ans, reasons
}

// extractSystem is the static extraction system prompt: role, the JSON
// contract, the three modes and their expression formats, the capability
// rules, and three examples. It carries no per-request data (cacheable).
// With a single capability the contract drops dest_capability, which the
// pipeline fills in.
func extractSystem(caps []capInfo) string {
	if len(caps) == 1 {
		return `You are a scheduling assistant that turns a user's request into a precise schedule definition.

Reply with JSON only: one object with exactly the keys sched_type, sched_expression, capability_parameters. No prose, no code fences.

sched_type is one of: one-shot, cron, periodic.
- one-shot: sched_expression is an RFC 3339 UTC time with whole seconds, after now, like 2026-12-01T09:30:00Z.
- cron: sched_expression is six fields (sec min hour dom month dow), optionally prefixed "CRON_TZ=<zone> "; without a prefix it is read in the config zone.
- periodic: sched_expression is "@every <duration>", whole seconds, at least 1s, like @every 30m.

capability_parameters must be an object matching the capability's parameter schema; use {} if it takes none.

Examples:
{"sched_type":"one-shot","sched_expression":"2026-12-01T09:30:00Z","capability_parameters":{"profile":"fast"}}
{"sched_type":"cron","sched_expression":"0 0 13 * * *","capability_parameters":{}}
{"sched_type":"periodic","sched_expression":"@every 30m","capability_parameters":{"text":"ping"}}`
	}
	return `You are a scheduling assistant that turns a user's request into a precise schedule definition.

Reply with JSON only: one object with exactly the keys sched_type, sched_expression, dest_capability, capability_parameters. No prose, no code fences.

sched_type is one of: one-shot, cron, periodic.
- one-shot: sched_expression is an RFC 3339 UTC time with whole seconds, after now, like 2026-12-01T09:30:00Z.
- cron: sched_expression is six fields (sec min hour dom month dow), optionally prefixed "CRON_TZ=<zone> "; without a prefix it is read in the config zone.
- periodic: sched_expression is "@every <duration>", whole seconds, at least 1s, like @every 30m.

dest_capability must be exactly one of the capability names listed in the request.
capability_parameters must be an object matching the capability's parameter schema; use {} if it takes none.

Examples:
{"sched_type":"one-shot","sched_expression":"2026-12-01T09:30:00Z","dest_capability":"scan","capability_parameters":{"profile":"fast"}}
{"sched_type":"cron","sched_expression":"0 0 13 * * *","dest_capability":"backup","capability_parameters":{}}
{"sched_type":"periodic","sched_expression":"@every 30m","dest_capability":"echo","capability_parameters":{"text":"ping"}}`
}

// extractUser is the per-request extraction user message: the current time,
// the capability list with parameter schemas, the decision's hints, and the
// request verbatim last.
func (h *hora) extractUser(prompt string, caps []capInfo, hints hints) string {
	now := h.now()
	var b strings.Builder
	b.WriteString("Current time: " + now.UTC().Format(time.RFC3339) + " UTC; in " +
		h.cfg.Loc.String() + ": " + now.In(h.cfg.Loc).Format("2006-01-02T15:04:05 -0700 (MST)") + ".\n\n")
	if len(caps) == 1 {
		b.WriteString("The request targets the single available capability:\n")
	} else {
		b.WriteString("Available capabilities:\n")
	}
	for _, c := range caps {
		schema := c.Parameters
		if schema == "" {
			schema = `{"type":"object","properties":{},"additionalProperties":false}`
		}
		b.WriteString(fmt.Sprintf("- %s: %s. parameters: %s\n", c.Name, c.Description, schema))
	}
	if hints.Type != "" || hints.Capability != "" {
		if len(caps) == 1 {
			b.WriteString("\nDecision hints: type " + hints.Type + ".\n")
		} else {
			b.WriteString("\nDecision hints: type " + hints.Type + ", capability " + hints.Capability + ".\n")
		}
	}
	b.WriteString("\nRequest: " + prompt + "\n")
	return b.String()
}

// injectDestCap fills dest_capability with the single capability's name in a
// reply (the model is not asked for it when the destination has one
// capability).
func injectDestCap(reply, name string) string {
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(reply), &obj) != nil {
		return reply
	}
	obj["dest_capability"] = json.RawMessage(`"` + name + `"`)
	out, err := json.Marshal(obj)
	if err != nil {
		return reply
	}
	return string(out)
}

// rejectedAnswer is set's rejected result.
func rejectedAnswer(dest actio.AgentID, rejection string) (any, error) {
	return map[string]any{"result": "rejected", "dest_id": string(dest), "rejection": rejection}, nil
}
