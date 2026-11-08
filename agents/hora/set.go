// set.go implements the set pipeline and validation (Hora.md, Setting a
// schedule and rejections; D-6, D-7, D-13).
package main

import (
	"context"
	"encoding/json"
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

// set runs the set pipeline (D-13) and returns the scheduled or rejected result.
func (h *hora) set(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	panic("not implemented: set")
}
