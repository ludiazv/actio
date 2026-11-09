// main.go is the hora entry point: flags, signals, the lifecycle (D-12) and
// fire (D-11), per Hora.md, High level logic.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
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
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	st, cleaned, err := openStore(cfg.DB, time.Now())
	if err != nil {
		return err
	}
	a, err := actio.New(actio.Options{
		ID:         actio.AgentID(cfg.AgentID),
		Namespaces: cfg.Namespaces,
	})
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
	if err := a.Start(ctx); err != nil {
		return err
	}
	for _, e := range cleaned {
		if lerr := a.Log("clean " + e.SchedID + ": one-shot time passed"); lerr != nil {
			fmt.Fprintf(os.Stderr, "log: %v\n", lerr)
		}
	}
	for _, e := range st.all() {
		if err := h.run.add(e); err != nil {
			stopAgent(a)
			return err
		}
	}
	h.run.start()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)
	select {
	case <-stop:
	case <-a.Done():
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), stopBound)
	defer cancel()
	if err := h.run.stop(stopCtx); err != nil {
		fmt.Fprintf(os.Stderr, "stop: %v\n", err)
	}
	return a.Stop(stopCtx, actio.StopOptions{})
}

// stopAgent ends a half-started agent when a start step fails after the
// agent exists (D-12).
func stopAgent(a *actio.Agent) {
	ctx, cancel := context.WithTimeout(context.Background(), stopBound)
	defer cancel()
	if err := a.Stop(ctx, actio.StopOptions{}); err != nil {
		fmt.Fprintf(os.Stderr, "stop: %v\n", err)
	}
}

// fire executes one firing: the capability call, the log line, and the
// one-shot cleanup (D-11).
func (h *hora) fire(id string) {
	panic("not implemented: fire")
}
