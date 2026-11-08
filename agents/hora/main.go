// main.go is the hora entry point: flags, signals, the lifecycle (D-12) and
// fire (D-11), per Hora.md, High level logic.
package main

import (
	"context"

	_ "time/tzdata" // the IANA zone database, inside the binary (Hora.md, Implementation Constraints)
)

// main starts the agent and blocks until it stops.
func main() {
	panic("not implemented: main")
}

// run starts Hora from the config file and stops it on SIGINT, SIGTERM or the
// agent's done signal (D-12).
func run(ctx context.Context, configPath string) error {
	panic("not implemented: run")
}

// fire executes one firing: the capability call, the log line, and the
// one-shot cleanup (D-11).
func (h *hora) fire(id string) {
	panic("not implemented: fire")
}
