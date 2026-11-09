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
	capNames := make([]string, 0, len(view.Capabilities))
	for n := range view.Capabilities {
		capNames = append(capNames, n)
	}
	sort.Strings(capNames)
	capCtx := destCapContext(dest, view)

	// Step 4: the decision, through the configured gate (D-9). A failure or
	// an errored verdict goes on to round 1 without hints.
	var hints hints
	if h.models.hasDecision() {
		verdict, err := h.models.decide(ctx, prompt, capCtx, capNames)
		if err != nil {
			logf("gate %s: failed: %v", h.cfg.DecisionGate, err)
		} else {
			logf("gate %s: ok=%v score=%.3f type=%s capability=%s",
				verdict.Gate, verdict.OK, verdict.Score, verdict.Hints.Type, verdict.Hints.Capability)
			if !verdict.OK {
				return rejectedAnswer(dest, "not a scheduling request")
			}
			hints = verdict.Hints
		}
	}

	// Steps 5–7: round 1, D-6's validation, and round 2 as a fresh request.
	system := h.systemPrompt(capCtx, hints)
	runRound := func(user string) (scheduleAnswer, string, []string) {
		reply, err := h.models.complete(ctx, system, user)
		if err != nil {
			return scheduleAnswer{}, reply, []string{"model error: " + err.Error()}
		}
		ans, reasons := h.validateReply(reply, view)
		return ans, reply, reasons
	}
	ans, reply, reasons := runRound(prompt)
	if len(reasons) > 0 {
		round2 := "Original prompt: " + prompt +
			"\nPrevious reply: " + reply +
			"\nIt failed validation: " + strings.Join(reasons, "; ")
		ans, _, reasons = runRound(round2)
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

// systemPrompt is D-7's extraction prompt: the JSON-only rule with exactly
// the four keys, the three modes and their expression formats, the current
// time in UTC and in the config zone, cap_context, and the decision's hints.
func (h *hora) systemPrompt(capCtx string, hints hints) string {
	now := h.now()
	var b strings.Builder
	b.WriteString("Reply with JSON only: one object with exactly the keys sched_type, sched_expression, dest_capability, capability_parameters.\n")
	b.WriteString("sched_type is one of: one-shot, cron, periodic.\n")
	b.WriteString("one-shot: sched_expression is an RFC 3339 UTC time with whole seconds, after now, like 2026-12-01T09:30:00Z.\n")
	b.WriteString("cron: sched_expression is six fields (sec min hour dom month dow), optionally prefixed CRON_TZ=<zone> ; a cron without a prefix is read in the config zone.\n")
	b.WriteString("periodic: sched_expression is @every <duration>, whole seconds of at least 1s, like @every 30m.\n")
	inZone := now.In(h.cfg.Loc)
	b.WriteString("Current time: " + now.UTC().Format(time.RFC3339) + " UTC; in " +
		h.cfg.Loc.String() + ": " + inZone.Format("2006-01-02T15:04:05 -0700 (MST)") + ".\n")
	b.WriteString(capCtx + "\n")
	if hints.Type != "" || hints.Capability != "" {
		b.WriteString("Decision hints: type " + hints.Type + ", capability " + hints.Capability + ".\n")
	}
	return b.String()
}

// destCapContext is set's cap_context: the destination's id and
// description, and each visible capability's name, description and
// parameters schema.
func destCapContext(dest actio.AgentID, view actio.AgentView) string {
	names := make([]string, 0, len(view.Capabilities))
	for n := range view.Capabilities {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	fmt.Fprintf(&b, "destination %s: %s. capabilities: ", dest, view.Spec.Description)
	for i, n := range names {
		if i > 0 {
			b.WriteString(", ")
		}
		c := view.Capabilities[n]
		fmt.Fprintf(&b, "%s (%s) parameters: %s", n, c.Description, c.Parameters)
	}
	return b.String()
}

// rejectedAnswer is set's rejected result.
func rejectedAnswer(dest actio.AgentID, rejection string) (any, error) {
	return map[string]any{"result": "rejected", "dest_id": string(dest), "rejection": rejection}, nil
}
