// The agent: its lifecycle, the protocol's own commands (lane C), and the public API.
//
// Start creates the folders, publishes spec.json and status.json, announces, watches its inbox and
// nuntium, and starts the heartbeat and the debounced rescans. Stop publishes inactive, announces,
// and leaves nothing armed: no goroutine, ticker or timer (D-9). A restart rebuilds that state in
// the same agent object: the registrations survive, a disabled flag set by the enable command does
// not, and Done stays the same channel (D-10).
//
// Lane C is the four protocol commands — stop, enable, restart, ping — plus whatever the program
// registers with OnCommand. A command is never answered: the sender is not told whether it worked.
package actio

import (
	"context"
	"time"
)

// Agent is one running agent. Create it with New; one object per agent id in one actio root.
type Agent struct {
	st *agentState
}

// New checks the options: the id's shape, and a namespace list the protocol can write. It reads no
// configuration and arms nothing: a bad ACTIO_* value surfaces at Start, as in the reference, because
// Start is where LoadConfig runs.
func New(opts Options) (*Agent, error) { panic("actio: not implemented") }

// Capability registers a capability: its published description, its parameters and result schemas,
// and the handler. Register before Start. A duplicate name, or a schema that does not compile or
// uses a keyword this validator does not know, is an error (D-19).
func (a *Agent) Capability(cap Capability, handler CapabilityHandler) error {
	panic("actio: not implemented")
}

// OnCommand registers a handler for one command name. A name that is one of the protocol's four
// built-in commands, or a name already registered, is an error: the built-in's behavior is not
// replaceable, as in the reference.
func (a *Agent) OnCommand(name string, handler CommandHandler) error { panic("actio: not implemented") }

// OnEvent adds an observability callback. Callbacks run synchronously on the protocol goroutine that
// produced the event, one at a time: return quickly and never call Stop from one.
func (a *Agent) OnEvent(fn func(Event)) { panic("actio: not implemented") }

// Start publishes the agent and arms its watchers, its heartbeat and its receiver. ctx bounds the
// startup only: cancelling it later does not stop the agent. A stop during Start leaves nothing
// armed (D-9).
func (a *Agent) Start(ctx context.Context) error { panic("actio: not implemented") }

// Stop ends the agent: it answers or cancels the work in progress, publishes inactive, announces,
// releases its watchers and timers, and waits for every goroutine to end. ctx bounds the wait, not
// the protocol's grace period: opts.Force cancels the work instead of waiting for it.
func (a *Agent) Stop(ctx context.Context, opts StopOptions) error { panic("actio: not implemented") }

// Done closes when the agent stops. A restart does not close it. It is what lets a program select
// on the agent's life instead of polling.
func (a *Agent) Done() <-chan struct{} { panic("actio: not implemented") }

// Agmen is the snapshot of the peers we can talk to, keyed by id (the callability rules applied).
// Treat the maps as read-only: the next scan replaces them.
func (a *Agent) Agmen() map[AgentID]AgentView { panic("actio: not implemented") }

// Refresh rescans now and returns the same view, so a program that just started a peer can see it
// without waiting for the debounced scan.
func (a *Agent) Refresh(ctx context.Context) (map[AgentID]AgentView, error) {
	panic("actio: not implemented")
}

// Request is lane B from a started agent. Cancellation of ctx does not return at once: it writes the
// cancellation and keeps waiting for the response as the protocol requires, then returns with
// FinalStatus cancelled or abandoned and a nil error.
func (a *Agent) Request(ctx context.Context, dst AgentID, name string, params any, opts RequestOptions) (Outcome, error) {
	panic("actio: not implemented")
}

// Command delivers a command to dst and returns its rid. Whether the peer accepts it is the peer's
// decision, and a command is never answered.
func (a *Agent) Command(ctx context.Context, dst AgentID, name string, params any) (RID, error) {
	panic("actio: not implemented")
}

// Fail publishes the error state and announces it: a fatal error we cannot recover from, told to the
// team instead of hidden. It does not stop the agent — the agent stays in the error state until it is
// stopped (D-22), so a peer can see why.
func (a *Agent) Fail(message string) error { panic("actio: not implemented") }

// Diarium reads this agent's own log.
func (a *Agent) Diarium() (DiariumRead, error) { panic("actio: not implemented") }

// Log appends a log event to our diarium. It emits no ActioEvent: an event callback is not a log
// consumer, as in the reference.
func (a *Agent) Log(message string) error { panic("actio: not implemented") }

// agentState is the wiring the protocol files share, created by New and by the CLI's Call. Every
// piece of it belongs to one agent: there is no process-wide state in this package.
type agentState struct {
	id      AgentID
	config  Config
	folders agentFolders
	files   *fileIO
	sync    syncProvider
	diaria  *diaria
	hub     *watchHub
	now     func() time.Time
	// emit hands an observability event to the callbacks; logf is the shortcut for a LogEvent.
	emit func(event Event)
	logf func(format string, args ...any)
	// spec and state read what the agent has published, for the code that needs the current view.
	spec  func() *Spec
	state func() StatusState
}

// agentFolders are the folders of one agent inside the actio root, all absolute.
type agentFolders struct {
	root    string // the shared actio root
	agent   string // this agent's folder: also the tmpDir of every atomic write
	inbox   string
	forum   string
	nuntium string
}
