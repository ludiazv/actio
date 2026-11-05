// Lane A and lane D: the receiver, the only owner of inbound exchange state.
//
// One goroutine holds every exchange in progress and selects on inbox file names, handler
// completions, timer expiries and stop. Nothing else touches that state, so there is no lock on it.
// Accepted work runs on its own goroutine, which gets a cancellable context and sends its result back
// over a channel; it recovers a panic and answers as for any other failure.
//
// The rules it enforces: the required acceptance checks (100, 101, 102, 103) and the optional ones
// this implementation makes (105, 106, 107, 108) at A0; the four envelopes of an exchange matched by
// rid and body key; a response written once; the confirmation wait (A2) and the archive (A4) that
// empties the inbox and writes the inreq diarium event with the envelopes as exchanged.
package actio

import (
	"sync"
	"time"
)

// capabilityEntry is one registration: what we publish about the capability, what its schemas
// compiled to, the handler, and how many requests of it are running or waiting. The receiver owns
// the counters, because status.json's capability_status reads what the receiver knows.
type capabilityEntry struct {
	capability  Capability
	schemas     *capabilitySchemas
	handler     CapabilityHandler
	running     int
	queue       int
	maxParallel int
}

// receiver owns all inbound exchange state of one agent.
type receiver struct{}

// newReceiver builds it on the shared state.
func newReceiver(st *agentState) *receiver { panic("actio: not implemented") }

// register adds a capability before Start, or during a run. A duplicate name is a registration
// error; a capability whose schemas do not compile, or whose schema uses a keyword this validator
// does not know, is a registration error too (D-19).
func (r *receiver) register(cap Capability, handler CapabilityHandler) error {
	panic("actio: not implemented")
}

// registerCommand adds a command handler; the protocol commands are already there.
func (r *receiver) registerCommand(name string, handler CommandHandler) error {
	panic("actio: not implemented")
}

// start watches our inbox and runs lane A and lane D on one tracked goroutine.
func (r *receiver) start(wg *sync.WaitGroup, stop <-chan struct{}) error {
	panic("actio: not implemented")
}

// stop answers the accepted work that has not answered yet with the work error 300 (agent_stopping),
// lets it finish within the grace period or cancels it when force is set, and ends the receiver loop.
// After it nothing is armed (D-9).
func (r *receiver) stop(force bool, grace time.Duration) error { panic("actio: not implemented") }

// capabilityStatus is the capability_status list for status.json: one entry per capability with the
// requests running and in queue.
func (r *receiver) capabilityStatus() []CapabilityStatus { panic("actio: not implemented") }

// busy reports whether any request is running or waiting: it decides the idle and busy states (D-8).
func (r *receiver) busy() bool { panic("actio: not implemented") }
