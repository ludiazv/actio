// Self-publication: spec.json, status.json, the announce file, the heartbeat, and the health a peer
// derives from a status (*Agent's status*).
//
// What we publish is validated before it is written; a rejection is our bug, and the error surfaces
// to the caller rather than reaching the shared tree.
package actio

import (
	"path/filepath"
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
	if capabilityStatus == nil {
		capabilityStatus = []CapabilityStatus{}
	}
	return Status{
		ID:               id,
		Status:           state,
		Datetime:         formatISO(now),
		CapabilityStatus: capabilityStatus,
	}
}

// publishSpec writes spec.json in the agent's folder.
func publishSpec(files *fileIO, folder string, spec *Spec) error {
	raw, err := marshalJSON(spec)
	if err != nil {
		return err
	}
	checked, err := validateSpec(raw)
	if err != nil {
		return err // G-5: nothing invalid reaches the tree, so no file is created or changed
	}
	return files.writeJSONAtomic(filepath.Join(folder, "spec.json"), checked, folder)
}

// publishStatus writes status.json in the agent's folder.
func publishStatus(files *fileIO, folder string, status *Status) error {
	raw, err := marshalJSON(status)
	if err != nil {
		return err
	}
	checked, err := validateStatus(raw)
	if err != nil {
		return err // G-5: nothing invalid reaches the tree, so no file is created or changed
	}
	return files.writeJSONAtomic(filepath.Join(folder, "status.json"), checked, folder)
}

// publishAnnounce writes <nuntium>/<id>.n.json and appends the announce event to the agent's
// diarium: one call, two folders (*Agent's status* rule 2). The caller keeps the announce delay
// between its status write and this call.
func publishAnnounce(files *fileIO, log *diaria, agentFolder, nuntiumDir string, id AgentID, text string, now time.Time) error {
	file := Announce{ID: id, Datetime: formatISO(now), TS: now.Unix(), Nuntium: text}
	raw, err := marshalJSON(file)
	if err != nil {
		return err
	}
	checked, err := validateAnnounce(raw)
	if err != nil {
		return err // G-5: the announce file is not written and the diarium is not touched
	}
	if err := files.makeDir(nuntiumDir); err != nil {
		return err
	}
	if err := files.writeJSONAtomic(filepath.Join(nuntiumDir, string(id)+".n.json"), checked, agentFolder); err != nil {
		return err
	}
	return log.append(announceEvent(id, now)) // records the completed announce
}

// Broadcast writes the reserved <root>/nuntium/nuntium.json, the signal no agent writes: a tool's
// way of telling the team to rescan. The CLI's broadcast subcommand is its only user.
func Broadcast(cfg Config, text string) error {
	now := cfg.Clock()
	file := Announce{ID: "broadcast", Datetime: formatISO(now), TS: now.Unix(), Nuntium: text}
	raw, err := marshalJSON(file)
	if err != nil {
		return err
	}
	if _, err := validateAnnounce(raw); err != nil {
		return err // G-5: the reserved file is not written
	}
	// A tool write: its own provider from cfg.Sync, no agent involved. Its hooks cannot fail here —
	// Notify and Delay have no error path and Flush is never called — so the sink has nothing to
	// receive.
	provider, err := newSyncProvider(cfg.Sync, func(error) {})
	if err != nil {
		return err
	}
	files := newFileIO(provider)
	nuntiumDir := filepath.Join(cfg.Root, "nuntium")
	if err := files.makeDir(nuntiumDir); err != nil {
		return err
	}
	return files.writeJSONAtomic(filepath.Join(nuntiumDir, "nuntium.json"), file, cfg.Root) // tmpDir: the root
}

// HealthOf rates a peer's status by its age (*Agent's status* rules 4–8). An inactive or error
// agent, or a datetime that cannot be read, gives HealthUnknown (""): rule 8 says such an agent owes
// no answer, and a reader cannot rate it.
func HealthOf(status Status, now time.Time) Health {
	if status.Status != StatusIdle && status.Status != StatusBusy {
		return HealthUnknown
	}
	updated, ok := parseISO(status.Datetime)
	if !ok {
		return HealthUnknown
	}
	age := now.Sub(updated) // a datetime in the future is the freshest possible
	switch {
	case age < healthHealthyAge:
		return HealthHealthy
	case age < healthOnlineAge:
		return HealthOnline
	case age <= healthDegradedAge:
		return HealthDegraded
	}
	return HealthZombie
}

// heartbeat re-publishes the status on a fixed interval: the protocol demands an update at least
// every 90 seconds even when nothing changed, which keeps peers from rating us degraded.
type heartbeat struct {
	interval time.Duration
	tick     func() error
	mu       sync.Mutex
	started  bool // start was called: one heartbeat, one goroutine, no second start
	stopped  bool
	stopCh   chan struct{} // closed by stop(): the heartbeat's own end, beside the agent's stop
}

// newHeartbeat makes a heartbeat that calls tick on every interval. The first tick waits a whole
// interval: the agent publishes its first status itself.
func newHeartbeat(interval time.Duration, tick func() error) *heartbeat {
	return &heartbeat{interval: interval, tick: tick, stopCh: make(chan struct{})}
}

// start runs it on one tracked goroutine; it ends when stop() closes. A second start — while
// running, or after a stop — does nothing: one heartbeat, one goroutine, and after Stop nothing is
// armed (D-9). The tracked goroutine ends when the in-flight tick returns, so a tick that blocks
// delays the agent's wait for Stop: that wait is bounded by Stop's own context, which tells the
// caller; a tick takes no context, since a file write can't be cancelled.
func (h *heartbeat) start(wg *sync.WaitGroup) {
	h.mu.Lock()
	if h.started || h.stopped {
		h.mu.Unlock()
		return
	}
	h.started = true
	h.mu.Unlock()
	wg.Go(func() {
		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()
		for {
			select {
			case <-h.stopCh:
				return
			case <-ticker.C:
				// A tick that returns an error reports it to the caller's sink through the tick
				// itself; the heartbeat neither swallows it nor stops the chain, so the next tick
				// comes (*Agent's status* rule 3 keeps the freshness promise alive).
				_ = h.tick()
			}
		}
	})
}

// stop ends it; idempotent.
func (h *heartbeat) stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.stopped {
		h.stopped = true
		close(h.stopCh)
	}
}
