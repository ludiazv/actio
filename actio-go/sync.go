// Sync providers: what the storage owes a write before a peer on another machine can see it
// (*Sync and race conditions*). Four forms: "none", "delay[:<ms>]", "fsync+delay[:<ms>]" and
// "mutagen:<session>", taken from Config.Sync (which is ACTIO_SYNC). The provider belongs to the
// agent, and the file primitives call its hooks.
package actio

import (
	"context"
	"time"
)

// SyncProvider is the hook pair the file primitives call: before the rename and after it.
type SyncProvider interface {
	// Fsync asks for the file to be synced before the rename and its folder after it.
	Fsync() bool
	// Delay is how long every write waits before it returns, 0 for none.
	Delay() time.Duration
	// Notify says "something was written": the provider decides when to act, if at all.
	Notify()
	// Flush forces a sync now and waits for it.
	Flush(ctx context.Context) error
	// Close releases what the provider holds; a pending debounced run is cancelled.
	Close() error
}

// newSyncProvider parses a Config.Sync value. An unknown form, or a delay outside 1–10 000 ms, is a
// configuration error reported at Start. A provider's own failures (a mutagen run that fails) never
// reach the protocol: they go to onError once per failed run, which the agent wires to its log.
func newSyncProvider(value string, onError func(error)) (SyncProvider, error) {
	panic("actio: not implemented")
}
