// The public non-protocol types: the API's options, results, events and errors. The protocol's own
// JSON shapes live in protocol.go.
package actio

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Options configures an agent (*Issue a request*, *Agent Spec*). A nil or empty slice means "not
// set" unless the field says otherwise.
type Options struct {
	ID          AgentID
	Description string
	// Namespaces: nil leaves the key out of spec.json (the null namespace); a nil item is the void
	// namespace listed explicitly, and "*" is every namespace (*Agent Spec*).
	Namespaces          []*string
	Depends, Wants      []string
	AcceptsCommandsFrom []string // an empty list means nobody may command us
	Info                map[string]any
	Config              Config
}

// CapabilityHandler runs one accepted request (*Issue a request*). Params is the request body's
// parameters as exchanged; return an *Error to answer with its code and message, or any other error
// to answer with the message and no code. A panic is recovered and answered the same way.
type CapabilityHandler func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error)

// CommandHandler runs one accepted command (*Commands*). Commands are never answered: an error is
// logged, and the peer is not told.
type CommandHandler func(ctx context.Context, params json.RawMessage, cmd *CommandContext) error

// CallContext is what a capability handler knows about its request. Cancellation arrives through
// ctx, not through a field (TS: ctx.signal).
type CallContext struct {
	RID         RID
	Src         AgentID
	Attachments []string // request attachments, as paths relative to forum
	// PublishAttachment writes data into forum and returns its forum-relative name for the response.
	PublishAttachment func(name string, data []byte) (string, error)
}

// CommandContext is what a command handler knows about its command.
type CommandContext struct {
	RID RID
	Src AgentID
}

// Outcome is the end of a request (lane B). FinalStatus is "confirmed", "cancelled" or "abandoned";
// Response is nil when no response was received. A peer's error answer is not a Go error: check
// Response.Error.
type Outcome struct {
	FinalStatus string
	Response    *ResponseBody
}

// RequestOptions tunes one request. Timeout 0 waits until ctx is done; a positive Timeout is written
// to the envelope in whole seconds, rounded up.
type RequestOptions struct {
	Timeout     time.Duration
	Attachments []string
}

// StopOptions: Force cancels the work in progress instead of letting it finish within StopGrace.
type StopOptions struct {
	Force bool
}

// Error is a capability error: a handler returns it to answer with a code and a message. Codes are
// the agent's own, 1000 and up (*Acceptance errors*).
type Error struct {
	Code    int
	Message string
	Details any
}

func (e *Error) Error() string {
	return fmt.Sprintf("actio: capability error %d: %s", e.Code, e.Message)
}

// CallError is a local refusal: the callability rules rejected the peer, so nothing was written.
type CallError struct {
	Dst    AgentID
	Name   string
	Reason string
}

func (e *CallError) Error() string {
	return fmt.Sprintf("actio: cannot call %s/%s: %s", e.Dst, e.Name, e.Reason)
}

// ResultError is an exchange that ended with a response whose result does not follow the
// capability's published result schema.
type ResultError struct {
	Message  string
	Response *ResponseBody
}

func (e *ResultError) Error() string { return "actio: " + e.Message }

// Event is one observability event (agent.OnEvent). Switch on the concrete type:
// StatusEvent, AnnounceEvent, InreqEvent, OutreqEvent, LogEvent, IgnoredEvent.
type Event interface{ isEvent() }

// StatusEvent reports a change of our own published state.
type StatusEvent struct{ State StatusState }

// AnnounceEvent reports that we announced ourselves.
type AnnounceEvent struct{ Text string }

// InreqEvent reports an archived incoming exchange and its final status.
type InreqEvent struct {
	RID         RID
	FinalStatus string
}

// OutreqEvent reports an outgoing exchange and its final status.
type OutreqEvent struct {
	RID         RID
	FinalStatus string
}

// LogEvent is a message worth surfacing: an ignored file, a lost watcher, a failed sync.
type LogEvent struct{ Message string }

// IgnoredEvent reports a file or folder the agent read and could not use, and why.
type IgnoredEvent struct {
	Path   string
	Reason string
}

func (StatusEvent) isEvent()   {}
func (AnnounceEvent) isEvent() {}
func (InreqEvent) isEvent()    {}
func (OutreqEvent) isEvent()   {}
func (LogEvent) isEvent()      {}
func (IgnoredEvent) isEvent()  {}

// AgentView is a peer agent we can talk to, after the callability rules (*Callability rules*).
// Agmen returns a snapshot: treat it as read-only.
type AgentView struct {
	ID           AgentID
	Folder       string
	Spec         Spec
	Status       Status
	Health       Health
	Commands     bool                  // their accepts_commands_from lists us (rule 5.1)
	Capabilities map[string]Capability // usable by us after the disabled/accepts_from filter
}

// Health is a peer's freshness, derived from its status and never written by it (*Agent's status*
// rules 4–7). The empty value means "not rateable": an inactive or error agent, or a datetime that
// cannot be read.
type Health string

// The four health values, and the empty one.
const (
	HealthHealthy  Health = "healthy"
	HealthOnline   Health = "online"
	HealthDegraded Health = "degraded"
	HealthZombie   Health = "zombie"
	HealthUnknown  Health = ""
)

// StatusState is one of the four protocol states (*Agent's status* rule 1).
type StatusState string

// The four mandatory states.
const (
	StatusInactive StatusState = "inactive"
	StatusIdle     StatusState = "idle"
	StatusBusy     StatusState = "busy"
	StatusError    StatusState = "error"
)

// DiscoveredAgent is a valid agent folder found by a scan (*Announcement and Discovery* rules 1–4).
type DiscoveredAgent struct {
	Folder string
	Spec   Spec
	Status Status
}

// ScanResult is what a scan found, plus what it left out and why.
type ScanResult struct {
	Agents     []DiscoveredAgent
	Duplicates []string      // folders skipped because the id was already taken
	Ignored    []IgnoredFile // spec or status that did not conform, or was not JSON
}

// IgnoredFile names a file the scan could not use, and the first reason.
type IgnoredFile struct {
	Path   string
	Reason string
}

// DiariumEntry is one valid line of a diarium file, the same thing the protocol writes: EventData is
// a string for "status" and "log", null for "announce", an ExchangeEnvelopes object for "inreq" and
// "outreq". It is an alias of the pinned shape, not a second copy of it.
type DiariumEntry = DiariumLine

// DiariumRead is the read-only view of one agent's diarium: the valid lines in file order, and the
// lines that were not, each with its 1-based line number and the reason.
type DiariumRead struct {
	Lines   []DiariumEntry
	Ignored []IgnoredLine
}

// IgnoredLine is one line a reader could not use.
type IgnoredLine struct {
	Line   int
	Reason string
}
