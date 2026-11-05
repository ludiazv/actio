// Self-publication: spec.json, status.json, the announce file, the heartbeat, and the health a peer
// derives from a status (*Agent's status*).
//
// What we publish is validated before it is written; a rejection is our bug, and the error surfaces
// to the caller rather than reaching the shared tree.
package actio

import (
	"sync"
	"time"
)

// Health thresholds (*Agent's status* rules 4–7): protocol constants, not configuration.
const (
	healthHealthyAge  = 60 * time.Second
	healthOnlineAge   = 100 * time.Second
	healthDegradedAge = 600 * time.Second
)

// buildStatus makes a valid status object: id, state, a fresh datetime from the agent's clock, and
// the capability_status list (empty is legal, nil is not).
func buildStatus(id AgentID, state StatusState, now time.Time, capabilityStatus []CapabilityStatus) Status {
	panic("actio: not implemented")
}

// publishSpec writes spec.json in the agent's folder.
func publishSpec(files *fileIO, folder string, spec *Spec) error { panic("actio: not implemented") }

// publishStatus writes status.json in the agent's folder.
func publishStatus(files *fileIO, folder string, status *Status) error {
	panic("actio: not implemented")
}

// publishAnnounce writes <nuntium>/<id>.n.json and appends the announce event to the agent's
// diarium: one call, two folders (*Agent's status* rule 2). The caller keeps the announce delay
// between its status write and this call.
func publishAnnounce(files *fileIO, log *diaria, agentFolder, nuntiumDir string, id AgentID, text string, now time.Time) error {
	panic("actio: not implemented")
}

// Broadcast writes the reserved <root>/nuntium/nuntium.json, the signal no agent writes: a tool's
// way of telling the team to rescan. The CLI's broadcast subcommand is its only user.
func Broadcast(cfg Config, text string) error { panic("actio: not implemented") }

// HealthOf rates a peer's status by its age (*Agent's status* rules 4–8). An inactive or error
// agent, or a datetime that cannot be read, gives HealthUnknown (""): rule 8 says such an agent owes
// no answer, and a reader cannot rate it.
func HealthOf(status Status, now time.Time) Health { panic("actio: not implemented") }

// heartbeat re-publishes the status on a fixed interval: the protocol demands an update at least
// every 90 seconds even when nothing changed, which keeps peers from rating us degraded.
type heartbeat struct{}

// newHeartbeat makes a heartbeat that calls tick on every interval. The first tick waits a whole
// interval: the agent publishes its first status itself.
func newHeartbeat(interval time.Duration, tick func() error) *heartbeat {
	panic("actio: not implemented")
}

// start runs it on one tracked goroutine; it ends when stop closes, and stop ends it early. After
// stop no timer is armed (D-9).
func (h *heartbeat) start(wg *sync.WaitGroup, stop <-chan struct{}) { panic("actio: not implemented") }

// stop ends it; idempotent.
func (h *heartbeat) stop() { panic("actio: not implemented") }
