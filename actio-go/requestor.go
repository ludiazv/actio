// Lane B: the requestor side, one sequential exchange per call (B1–B5).
//
// A request writes its envelope and attachments, then waits in one select over the destination
// inbox's watcher, its timeout timer, the caller's ctx and the agent's stop. Cancellation is not a
// return: ctx ending starts B4, which writes the cancellation and then keeps waiting for the
// response up to CancelWaitTimeout, because the protocol requires the exchange to end with a
// confirmation or a timeout.
//
// Watching the destination is shared: one watcher per inbox, reference-counted by the requests
// waiting on it (D-18), so a hundred concurrent requests do not hold a hundred inotify instances.
package actio

import "context"

// requestor is the lane-B machinery of one agent (or of one CLI call).
type requestor struct{}

// newRequestor builds it on the shared state: the folders, the file primitives, the hub of
// destination watchers, the diarium and the clock.
func newRequestor(st *agentState) *requestor { panic("actio: not implemented") }

// request runs B1–B5 for one request and reports how the exchange ended. It returns a nil error for
// a peer's error answer: check Outcome.Response.Error. Only a local failure is an error: a refusal by
// the callability rules (*CallError), a bad result (*ResultError), or I/O.
func (r *requestor) request(ctx context.Context, dst AgentID, name string, params any, opts RequestOptions) (Outcome, error) {
	panic("actio: not implemented")
}

// command writes one command envelope and returns its rid: a command is delivered, not executed, and
// is never answered.
func (r *requestor) command(ctx context.Context, dst AgentID, name string, params any) (RID, error) {
	panic("actio: not implemented")
}

// stop releases the watchers and timers of the exchanges still going.
func (r *requestor) stop() { panic("actio: not implemented") }

// Call is lane B without an agent: one exchange written from src's folder, with no spec, no status
// and no announce. The CLI's call subcommand is its user; it must pass a timeout above zero, since a
// process this short-lived cannot hold a request open forever.
func Call(ctx context.Context, cfg Config, src, dst AgentID, name string, params any, opts RequestOptions) (Outcome, error) {
	panic("actio: not implemented")
}

// SendCommand is the command form of Call.
func SendCommand(ctx context.Context, cfg Config, src, dst AgentID, name string, params any) (RID, error) {
	panic("actio: not implemented")
}
