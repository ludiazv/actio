// echo: the conformance target agent — the black-box harness and the interop suite drive
// it the same way they drive the reference. It answers echo, sleeps, fails, and refuses.
//
// Env: ACTIO_AGENT_ID (default "echo"), ACTIO_COMMANDERS (comma list), ACTIO_NAMESPACES (comma
// list), plus the config env overrides (ACTIO_DIR, ACTIO_SYNC, ACTIO_HEARTBEAT,
// ACTIO_CONFIRM_TIMEOUT, ACTIO_CANCEL_WAIT), which the library reads.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"actio-go"
)

// noParams is the {} schema of a capability that takes nothing (echo-agent.ts NO_PARAMS).
const noParams = `{"type":"object","properties":{},"additionalProperties":false}`

// The five capabilities of the conformance target, exactly as the reference declares them.
var (
	capEcho = actio.Capability{
		Name:        "echo",
		Description: "echoes the text it gets",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`),
		Actio: actio.CapabilityActio{
			Result: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`),
		},
	}
	capSleep = actio.Capability{
		Name:        "sleep",
		Description: "waits the given seconds and reports them; honours cancellation",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"seconds":{"type":"number"}},"required":["seconds"],"additionalProperties":false}`),
		Actio: actio.CapabilityActio{
			Result:      json.RawMessage(`{"type":"object","properties":{"slept":{"type":"number"}},"required":["slept"]}`),
			MaxParallel: 1,
		},
	}
	capFail = actio.Capability{
		Name:        "fail",
		Description: "always fails with ActioError code 1000",
		Parameters:  json.RawMessage(noParams),
		Actio:       actio.CapabilityActio{Result: json.RawMessage("true")},
	}
	capOff = actio.Capability{
		Name:        "off",
		Description: "disabled in the spec",
		Parameters:  json.RawMessage(noParams),
		Actio:       actio.CapabilityActio{Result: json.RawMessage("true"), Disabled: &disabledTrue},
	}
	capPrivate = actio.Capability{
		Name:        "private",
		Description: "callable only by nobody",
		Parameters:  json.RawMessage(noParams),
		Actio:       actio.CapabilityActio{Result: json.RawMessage("true"), AcceptsFrom: []string{"nobody"}},
	}
	disabledTrue = true
)

func main() {
	id := os.Getenv("ACTIO_AGENT_ID")
	if id == "" {
		id = "echo"
	}
	opts := actio.Options{
		ID:                  actio.AgentID(id),
		Description:         "conformance target agent (echo)",
		AcceptsCommandsFrom: splitList(os.Getenv("ACTIO_COMMANDERS")),
		Config:              mustConfig(),
	}
	for _, ns := range splitList(os.Getenv("ACTIO_NAMESPACES")) { // nil when unset: the key stays out of spec.json
		opts.Namespaces = append(opts.Namespaces, &ns)
	}
	agent, err := actio.New(opts)
	if err != nil {
		fatalf("actio echo: New: %v", err)
	}
	for _, reg := range []struct {
		cap actio.Capability
		fn  actio.CapabilityHandler
	}{
		{capEcho, echoHandler}, {capSleep, sleepHandler}, {capFail, failHandler},
		{capOff, okHandler}, {capPrivate, okHandler},
	} {
		if err := agent.Capability(reg.cap, reg.fn); err != nil {
			fatalf("actio echo: Capability(%s): %v", reg.cap.Name, err)
		}
	}
	if err := agent.OnCommand("noop", noopHandler); err != nil {
		fatalf("actio echo: OnCommand(noop): %v", err)
	}
	if err := agent.Start(context.Background()); err != nil {
		fatalf("actio echo: Start: %v", err)
	}
	// The library never installs signal handlers; the example does (echo-agent.ts): SIGINT/SIGTERM
	// stop the agent gracefully. A stop command stops it too; when it is stopped, exit 0.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		if err := agent.Stop(context.Background(), actio.StopOptions{}); err != nil {
			fatalf("actio echo: Stop: %v", err)
		}
	}()
	<-agent.Done()
	os.Exit(0)
}

// echoHandler returns the text it gets.
func echoHandler(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	var p struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	return map[string]any{"text": p.Text}, nil
}

// sleepHandler waits the given seconds and reports them; a cancellation ends the wait early
// (echo-agent.ts, A3).
func sleepHandler(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	var p struct {
		Seconds float64 `json:"seconds"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	select {
	case <-time.After(time.Duration(p.Seconds * float64(time.Second))):
	case <-ctx.Done():
	}
	return map[string]any{"slept": p.Seconds}, nil
}

// failHandler always fails with the conformance error.
func failHandler(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	return nil, &actio.Error{Code: 1000, Message: "fail: requested failure"}
}

// okHandler answers the capabilities that take nothing.
func okHandler(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	return map[string]any{}, nil
}

// noopHandler is the custom command: it does nothing, and commands are never answered.
func noopHandler(ctx context.Context, params json.RawMessage, cmd *actio.CommandContext) error {
	return nil
}

// splitList is the reference's comma split with the empty items dropped.
func splitList(raw string) []string {
	var out []string
	for _, s := range strings.Split(raw, ",") {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// mustConfig reads the config env overrides (ACTIO_DIR and friends); a bad value is fatal before
// anything runs, as in the reference.
func mustConfig() actio.Config {
	cfg, err := actio.LoadConfig(actio.Config{})
	if err != nil {
		fatalf("actio echo: config: %v", err)
	}
	return cfg
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
