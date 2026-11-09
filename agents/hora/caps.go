// caps.go publishes Hora's three capabilities (Hora.md, Capabilities) with
// the parameter schemas that make actio-go reject bad parameters itself with
// code 105, and implements list and unset (D-2, D-13).
package main

import (
	"context"
	"encoding/json"
	"sort"

	"actio-go"
)

// The three capabilities, with the parameter schemas verbatim and the
// result schemas.
var (
	listCap = actio.Capability{
		Name:        "list",
		Description: "lists the existing schedulings",
		Parameters:  json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
		Actio: actio.CapabilityActio{
			Result: json.RawMessage(`{"type":"array","items":{"type":"object","properties":{"sched_id":{"type":"string"},"dest_id":{"type":"string"},"sched_type":{"type":"string"},"dest_capability":{"type":"string"},"sched_expression":{"type":"string"}},"required":["sched_id","dest_id","sched_type","dest_capability","sched_expression"],"additionalProperties":false}}`),
		},
	}
	unsetCap = actio.Capability{
		Name:        "unset",
		Description: "removes a schedule by its sched_id",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"sched_id":{"type":"string","pattern":"^SCH-[0-9a-f]{8}$"}},"required":["sched_id"],"additionalProperties":false}`),
		Actio: actio.CapabilityActio{
			Result: json.RawMessage(`{"type":"object","properties":{"result":{"type":"string"},"sched_id":{"type":"string"}},"required":["result","sched_id"]}`),
		},
	}
	setCap = actio.Capability{
		Name:        "set",
		Description: "sets a schedule event from a prompt",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"prompt":{"type":"string","minLength":1},"dest_id":{"type":"string","minLength":1}},"required":["prompt"],"additionalProperties":false}`),
		Actio: actio.CapabilityActio{
			Result: json.RawMessage(`{"type":"object","properties":{"result":{"type":"string"},"sched_id":{"type":"string"},"dest_id":{"type":"string"},"dest_capability":{"type":"string"},"capability_parameters":{"type":"object"},"sched_expression":{"type":"string"},"rejection":{"type":"string"}},"required":["result","dest_id"]}`),
			// A set can take two rounds: a chat round ran up to 18.6 s and the
			// decision 2.3 s on the measured server.
			RecommendedTimeout: 180,
		},
	}
)

// publish registers the three capabilities on the agent: list, unset, set.
func (h *hora) publish(a *actio.Agent) error {
	if err := a.Capability(listCap, h.list); err != nil {
		return err
	}
	if err := a.Capability(unsetCap, h.unset); err != nil {
		return err
	}
	return a.Capability(setCap, h.set)
}

// isCoordinator reports whether the agent is in the config's coordinators.
func (h *hora) isCoordinator(id actio.AgentID) bool {
	for _, c := range h.cfg.Coordinators {
		if c == string(id) {
			return true
		}
	}
	return false
}

// listResult is one entry of list's answer (U-3: only these five fields).
type listResult struct {
	SchedID         string `json:"sched_id"`
	DestID          string `json:"dest_id"`
	SchedType       string `json:"sched_type"`
	DestCapability  string `json:"dest_capability"`
	SchedExpression string `json:"sched_expression"`
}

// list returns the caller's entries, or every entry when the caller is a
// coordinator (D-2): sorted by datetime, then sched_id; [] when empty.
func (h *hora) list(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	entries := h.store.all()
	if !h.isCoordinator(call.Src) {
		own := entries[:0]
		for _, e := range entries {
			if e.DestID == string(call.Src) {
				own = append(own, e)
			}
		}
		entries = own
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Datetime != entries[j].Datetime {
			return entries[i].Datetime < entries[j].Datetime
		}
		return entries[i].SchedID < entries[j].SchedID
	})
	out := make([]listResult, 0, len(entries))
	for _, e := range entries {
		out = append(out, listResult{
			SchedID:         e.SchedID,
			DestID:          e.DestID,
			SchedType:       e.SchedType,
			DestCapability:  e.DestCapability,
			SchedExpression: e.SchedExpression,
		})
	}
	return out, nil
}

// unset removes the entry when the caller is its owner or a coordinator
// (D-2, D-13): the DB save, then the runner removal, then the answer. The
// same error, 1001, for the missing entry and the not-yours (D-2). A call
// that is running when its entry is unset is not cancelled.
func (h *hora) unset(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	var p struct {
		SchedID string `json:"sched_id"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	e, ok := h.store.get(p.SchedID)
	if !ok || (e.DestID != string(call.Src) && !h.isCoordinator(call.Src)) {
		return nil, &actio.Error{Code: 1001, Message: "unknown sched_id: " + p.SchedID}
	}
	if _, _, err := h.store.remove(p.SchedID); err != nil {
		return nil, &actio.Error{Code: 1002, Message: "storage: " + err.Error()}
	}
	h.run.remove(p.SchedID)
	return map[string]any{"result": "unset", "sched_id": p.SchedID}, nil
}
