// main.go is the hora entry point: flags, signals, the lifecycle (D-12) and
// fire (D-11), per Hora.md, High level logic.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"actio-go"

	_ "time/tzdata" // the IANA zone database, inside the binary (Hora.md, Implementation Constraints)
)

// stopBound bounds the stop phase (D-12).
const stopBound = 30 * time.Second

// main starts the agent and blocks until it stops. A usage error exits 2;
// a config or start error prints its reason, which names the key, and
// exits 1 (Hora.md, Configuration).
func main() {
	configPath := flag.String("config", "", "the config file (required)")
	flag.Parse()
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "usage: hora -config <file>")
		os.Exit(2)
	}
	if err := run(context.Background(), *configPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run starts Hora from the config file and stops it on SIGINT, SIGTERM or
// the agent's done signal (D-12). A failure at any start step undoes the
// earlier ones and returns an error, so the process exits 1.
func run(ctx context.Context, configPath string) error {
	// The handler goes in first: a signal at any time after it runs the
	// stop, and a stop racing the start is serialised by the agent's
	// lifecycle gate.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	st, cleaned, err := openStore(cfg.DB, time.Now())
	if err != nil {
		return err
	}
	a, err := actio.New(actio.Options{ID: actio.AgentID(cfg.AgentID), Namespaces: cfg.Namespaces})
	if err != nil {
		return err
	}
	h := &hora{
		cfg:    cfg,
		agent:  a,
		store:  st,
		models: newModels(cfg),
		now:    time.Now,
	}
	h.run = newRunner(h.fire)
	// The three capabilities register here, through caps.go (D-12).
	if err := h.publish(a); err != nil {
		return err
	}
	if err := a.Start(ctx); err != nil {
		return err
	}
	for _, e := range cleaned {
		h.logLine("clean " + e.SchedID + ": one-shot time passed")
	}
	for _, e := range st.all() {
		if err := h.run.add(e); err != nil {
			// The start failed after the agent: stop it, undoing the start (D-12).
			sctx, scancel := context.WithTimeout(context.Background(), stopBound)
			if serr := a.Stop(sctx, actio.StopOptions{}); serr != nil {
				fmt.Fprintf(os.Stderr, "stop: %v\n", serr)
			}
			scancel()
			return err
		}
	}
	h.run.start()

	select {
	case <-stop:
	case <-a.Done():
	}
	h.cancelFiring()
	stopCtx, cancel := context.WithTimeout(context.Background(), stopBound)
	defer cancel()
	if err := h.run.stop(stopCtx); err != nil {
		fmt.Fprintf(os.Stderr, "stop: %v\n", err)
	}
	return a.Stop(stopCtx, actio.StopOptions{})
}

// inFlight is D-11's skip and D-12's cancel of a running fire: one mutex,
// one map, one flag; the zero value works, the map is created on first use.
type inFlight struct {
	mu      sync.Mutex
	calls   map[string]context.CancelFunc // sched_id -> the cancel of its running fire
	stopped bool
}

// cancelFiring is the stop's first step (D-12): after it, no Request starts.
func (h *hora) cancelFiring() {
	h.firing.mu.Lock()
	h.firing.stopped = true
	cancels := make([]context.CancelFunc, 0, len(h.firing.calls))
	for _, cancel := range h.firing.calls {
		cancels = append(cancels, cancel)
	}
	h.firing.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// fire executes one firing (D-11): the capability call, the log line, and
// the one-shot cleanup. The mutex is released before the view read and the
// call; the id leaves the map on every path.
func (h *hora) fire(id string) {
	e, ok := h.store.get(id)
	if !ok {
		return
	}
	h.firing.mu.Lock()
	if h.firing.stopped {
		h.firing.mu.Unlock()
		return
	}
	if _, running := h.firing.calls[id]; running {
		h.firing.mu.Unlock()
		h.logLine("skip " + id + ": previous call still running")
		return
	}
	fireCtx, cancel := context.WithCancel(context.Background())
	if h.firing.calls == nil {
		h.firing.calls = make(map[string]context.CancelFunc)
	}
	h.firing.calls[id] = cancel
	h.firing.mu.Unlock()
	defer func() {
		h.firing.mu.Lock()
		delete(h.firing.calls, id)
		h.firing.mu.Unlock()
	}()

	rt := 300 * time.Second
	if views, err := h.agent.Refresh(fireCtx); err == nil {
		if v, ok := views[actio.AgentID(e.DestID)]; ok {
			if cap, ok := v.Capabilities[e.DestCapability]; ok && cap.Actio.RecommendedTimeout > 0 {
				rt = time.Duration(cap.Actio.RecommendedTimeout * float64(time.Second))
			}
		}
	}
	outcome, rerr := h.agent.Request(fireCtx, actio.AgentID(e.DestID), e.DestCapability, e.CapabilityParameters, actio.RequestOptions{Timeout: rt})
	line := "fire " + id + " " + e.DestID + "/" + e.DestCapability + ": "
	if rerr != nil {
		line += "failed: " + rerr.Error()
	} else {
		line += outcome.FinalStatus
		if outcome.Response != nil && outcome.Response.Error {
			var er actio.ErrorResult
			if json.Unmarshal(outcome.Response.Result, &er) == nil {
				code := 0
				if er.Code != nil {
					code = *er.Code
				}
				line += fmt.Sprintf(" error %d %s", code, er.Msg)
			}
		}
	}
	h.logLine(line)
	if e.SchedType == "one-shot" {
		if _, _, err := h.store.remove(id); err != nil {
			fmt.Fprintf(os.Stderr, "remove: %v\n", err)
		}
		h.run.remove(id)
	}
}

// logLine writes one diarium line; a failed log goes to stderr, never lost.
func (h *hora) logLine(message string) {
	if err := h.agent.Log(message); err != nil {
		fmt.Fprintf(os.Stderr, "log: %v\n", err)
	}
}
