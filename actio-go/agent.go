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
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Agent is one running agent. Create it with New; one object per agent id in one actio root.
type Agent struct {
	st *agentState
}

// New checks the options: the id's shape, and a namespace list the protocol can write. It reads no
// configuration and arms nothing: a bad ACTIO_* value surfaces at Start, as in the reference,
// because Start is where LoadConfig runs.
func New(opts Options) (*Agent, error) {
	id := string(opts.ID)
	if !isAgentID(id) {
		return nil, fmt.Errorf("actio: the agent id %q is not a valid id (1-32 characters of [a-zA-Z0-9_-])", id)
	}
	if _, reserved := reservedFolder[id]; reserved {
		return nil, fmt.Errorf("actio: the agent id %q is a reserved folder name", id)
	}
	for _, ns := range opts.Namespaces {
		if ns != nil && *ns == "" {
			return nil, fmt.Errorf("actio: a namespace name may not be empty")
		}
	}
	empty := &peerSet{views: map[AgentID]AgentView{}, specs: map[AgentID]*Spec{}}
	st := &agentState{
		id: opts.ID, opts: opts, now: time.Now,
		emit: func(Event) {}, logf: func(string, ...any) {},
		spec: func() *Spec { return nil }, state: func() StatusState { return StatusInactive },
		doneCh: make(chan struct{}), current: StatusInactive,
		caps: map[string]Capability{}, handlers: map[string]CapabilityHandler{}, cmds: map[string]CommandHandler{},
	}
	st.view.Store(empty)
	return &Agent{st: st}, nil
}

// isAgentID is the id's shape (common.schema.json, agentId): 1-32 characters of [a-zA-Z0-9_-].
func isAgentID(id string) bool {
	if id == "" || len(id) > 32 {
		return false
	}
	for _, r := range id {
		if !(r == '_' || r == '-' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
			return false
		}
	}
	return true
}

// Capability registers a capability: its published description, its parameters and result schemas,
// and the handler. Register before Start. A duplicate name, or a schema that does not compile or
// uses a keyword this validator does not know, is an error (D-19).
func (a *Agent) Capability(cap Capability, handler CapabilityHandler) error {
	st := a.st
	if st.running.Load() {
		return fmt.Errorf("actio: the capabilities of %s are fixed once it has started", st.id)
	}
	if _, dup := st.caps[cap.Name]; dup {
		return fmt.Errorf("actio: the capability %q is already registered", cap.Name)
	}
	if _, err := compileCapability(&cap); err != nil {
		return fmt.Errorf("actio: the capability %q has a schema the strict mode rejects: %w", cap.Name, err)
	}
	st.caps[cap.Name] = cap
	st.capOrder = append(st.capOrder, cap.Name)
	st.handlers[cap.Name] = handler
	return nil
}

// OnCommand registers a handler for one command name. A name that is one of the protocol's four
// built-in commands, or a name already registered, is an error: the built-in's behavior is not
// replaceable, as in the reference.
func (a *Agent) OnCommand(name string, handler CommandHandler) error {
	st := a.st
	if st.running.Load() {
		return fmt.Errorf("actio: the commands of %s are fixed once it has started", st.id)
	}
	if _, standard := standardCommands[name]; standard {
		return fmt.Errorf("actio: the command %q is a protocol command and cannot be replaced", name)
	}
	if _, dup := st.cmds[name]; dup {
		return fmt.Errorf("actio: the command %q is already registered", name)
	}
	st.cmds[name] = handler
	return nil
}

// OnEvent adds an observability callback. Callbacks run synchronously on the protocol goroutine
// that produced the event, one at a time: return quickly and never call Stop from one.
func (a *Agent) OnEvent(fn func(Event)) {
	a.st.dispatchMu.Lock()
	a.st.listeners = append(a.st.listeners, fn)
	a.st.dispatchMu.Unlock()
}

// Start publishes the agent and arms its watchers, its heartbeat and its receiver. ctx bounds the
// startup only: cancelling it later does not stop the agent. A stop during Start leaves nothing
// armed (D-9).
func (a *Agent) Start(ctx context.Context) error {
	st := a.st
	if !st.running.CompareAndSwap(false, true) {
		return fmt.Errorf("actio: the agent %s is already started", st.id)
	}
	st.stopping.Store(false)
	if err := a.startUp(ctx); err != nil {
		a.abortStart()
		return err
	}
	return nil
}

// startUp is the lifecycle order: config, folders, the one watcher, the subscriptions, the
// receiver, the heartbeat, and the first publication. A failure at any step leaves nothing armed.
func (a *Agent) startUp(ctx context.Context) error {
	st := a.st
	cfg, err := LoadConfig(st.opts.Config)
	if err != nil {
		return err // a bad ACTIO_* value surfaces here, not in New
	}
	st.config = cfg
	st.folders = agentFolders{root: cfg.Root, agent: filepath.Join(cfg.Root, string(st.id)), inbox: filepath.Join(cfg.Root, string(st.id), FolderInbox), forum: filepath.Join(cfg.Root, FolderForum), nuntium: filepath.Join(cfg.Root, FolderNuntium)}
	provider, err := newSyncProvider(cfg.Sync, func(e error) { st.emit(LogEvent{Message: "sync: " + e.Error()}) })
	if err != nil {
		return err // ACTIO_SYNC="" and the other bad provider values are rejected here
	}
	st.sync = provider
	st.files = newFileIO(provider)
	st.diaria = newDiaria(st.files, st.folders.agent, st.id)
	st.now, st.emit, st.logf = cfg.Clock, a.onEvent, a.logf
	st.spec = func() *Spec { return st.specNow.Load() }
	st.state = func() StatusState {
		st.pubMu.Lock()
		defer st.pubMu.Unlock()
		return a.publishedStateLocked()
	}
	st.haltCh, st.discStop, st.hbDone = make(chan struct{}), make(chan struct{}), make(chan struct{})
	st.enabled = nil // D-10: a `disabled` set by `enable` does not survive a restart
	st.current = StatusInactive

	for _, dir := range []string{cfg.Root, st.folders.nuntium, st.folders.forum, st.folders.agent, st.folders.inbox} {
		if err := st.files.makeDir(dir); err != nil {
			return err // abortStart closes the provider
		}
	}
	// A bad registration fails here, before anything lands in the tree.
	spec, err := a.rebuildSpec()
	if err != nil {
		return err
	}
	watch, err := newWatcher()
	if err != nil {
		return err
	}
	st.watch = watch
	nuntium, err := watch.subscribe(st.folders.nuntium)
	if err != nil {
		return err // abortStart closes the watcher
	}
	st.wg.Go(func() { a.discoveryLoop(nuntium) })

	recv := newReceiver(st)
	for _, name := range st.capOrder {
		if err := recv.register(st.caps[name], st.handlers[name]); err != nil {
			return err
		}
	}
	// Lane C: the four protocol commands. The receiver refuses to replace them by name, so the
	// wiring goes through its view directly: they are wired by the agent, not registered.
	a.wireStandardCommands(recv)
	for name, handler := range st.cmds {
		if err := recv.registerCommand(name, handler); err != nil {
			return err
		}
	}
	recv.setPeerSpec(func(id AgentID) *Spec { return st.view.Load().specs[id] }) // code 106 sees the discovered spec
	st.recv = recv
	if err := recv.start(&st.wg, st.haltCh); err != nil {
		return err
	}
	st.req = newRequestor(st)
	st.hb = newHeartbeat(cfg.HeartbeatInterval, a.tick)
	st.hb.start(&st.hbWg, st.haltCh)
	st.wg.Go(func() {
		st.hbWg.Wait()
		close(st.hbDone)
	})

	// The first publication: the spec, then the status with its diarium event. Status before
	// announce, as the protocol orders it.
	st.pubMu.Lock()
	err = publishSpec(st.files, st.folders.agent, spec)
	st.pubMu.Unlock()
	if err != nil {
		a.abortStart()
		return err
	}
	if err := a.setState(StatusIdle); err != nil {
		a.abortStart()
		return err
	}
	a.rescan() // the first discovery: now that the spec and the status are in the tree
	return a.announceAfterDelay(ctx, fmt.Sprintf("agent %s is active", st.id))
}

// wireStandardCommands puts the four protocol command handlers into the receiver's view. It runs
// right after newReceiver, whose view is empty, so nothing is copied.
func (a *Agent) wireStandardCommands(recv *receiver) {
	cmds := map[string]CommandHandler{
		"stop": a.cmdStop, "enable": a.cmdEnable, "restart": a.cmdRestart, "ping": a.cmdPing,
	}
	old := recv.view.Load()
	recv.view.Store(&capView{caps: old.caps, cmds: cmds})
}

// resetArmed drops the armed handles; the caller has already ended the loops behind them.
func (a *Agent) resetArmed() {
	a.st.haltCh, a.st.discStop, a.st.hbDone, a.st.recv, a.st.req, a.st.watch, a.st.sync = nil, nil, nil, nil, nil, nil, nil
}

// abortStart drops what a failed Start armed, and re-arms none: the discovery loop, the receiver,
// the heartbeat, the watcher and the provider all end, and the agent is not running (D-9).
func (a *Agent) abortStart() {
	st := a.st
	if st.hb != nil {
		st.hb.stop()
		<-st.hbDone
	}
	if st.recv != nil {
		_ = st.recv.stop(true, 0)
	}
	if st.discStop != nil {
		close(st.discStop)
		st.wg.Wait()
	}
	if st.watch != nil {
		st.watch.close()
	}
	if st.sync != nil {
		st.sync.Close()
	}
	a.resetArmed()
	st.files, st.diaria = nil, nil
	st.running.Store(false)
}

// Stop ends the agent: it answers or cancels the work in progress, publishes inactive, announces,
// releases its watchers and timers, and waits for every goroutine to end. ctx bounds the wait, not
// the protocol's grace period: opts.Force cancels the work instead of waiting for it.
func (a *Agent) Stop(ctx context.Context, opts StopOptions) error {
	st := a.st
	if !st.stopping.CompareAndSwap(false, true) {
		return nil // idempotent: a second stop resolves
	}
	defer st.stopping.Store(false)
	if !st.running.Load() {
		return nil
	}
	return a.stopInternal(ctx, opts, true)
}

// stopInternal is D-9 in that order: the heartbeat, waited for within ctx, before C1 (the tick
// publishes under the publication mutex and writes nothing once the state is inactive, so it can
// never overwrite C1; the bound is this ctx, and a stuck tick costs one goroutine until its write
// returns); the receiver; the outbound side; C1, the status first and the delayed announce after;
// C2, nothing armed. All the waits are bounded by ctx.
func (a *Agent) stopInternal(ctx context.Context, opts StopOptions, closeDone bool) error {
	st := a.st
	st.hb.stop()
	select {
	case <-st.hbDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	// The receiver: the work in progress settles within the grace (or is cancelled when Force), the
	// accepted-but-unanswered work gets the stopping answer, and the answered exchanges are
	// archived. Bounded by ctx the same way: a stuck one costs one goroutine until it returns.
	stopDone := make(chan struct{})
	st.wg.Go(func() {
		_ = st.recv.stop(opts.Force, st.config.StopGrace)
		close(stopDone)
	})
	select {
	case <-stopDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	st.req.stop()      // the outbound side: what is still in B2 is abandoned (D-9)
	close(st.discStop) // the discovery loop: no more scans, the pending debounce is dropped
	st.wg.Wait()
	// C1: the status first, then the delayed announce, then the sync flush. A failed publish is
	// logged, as in the reference: the state stays as chosen.
	a.setState(StatusInactive)
	if err := a.announceAfterDelay(ctx, fmt.Sprintf("agent %s is inactive", st.id)); err != nil {
		return err
	}
	if err := st.sync.Flush(ctx); err != nil {
		return err
	}
	// C2: deinit. The watchers are gone, the in-memory view is dropped, nothing is armed.
	st.watch.close()
	st.view.Store(&peerSet{views: map[AgentID]AgentView{}, specs: map[AgentID]*Spec{}})
	a.resetArmed()
	st.running.Store(false)
	if closeDone && !st.doneClosed {
		close(st.doneCh)
		st.doneClosed = true
	}
	return nil
}

// Done closes when the agent stops. A restart does not close it. It is what lets a program select
// on the agent's life instead of polling.
func (a *Agent) Done() <-chan struct{} { return a.st.doneCh }

// Agmen is the snapshot of the peers we can talk to, keyed by id (the callability rules applied).
// Treat the maps as read-only: the next scan replaces them.
func (a *Agent) Agmen() map[AgentID]AgentView { return a.st.view.Load().views }

// Refresh rescans now and returns the same view, so a program that just started a peer can see it
// without waiting for the debounced scan.
func (a *Agent) Refresh(ctx context.Context) (map[AgentID]AgentView, error) {
	a.rescan()
	return a.st.view.Load().views, nil
}

// Request is lane B from a started agent. Cancellation of ctx does not return at once: it writes the
// cancellation and keeps waiting for the response as the protocol requires, then returns with
// FinalStatus cancelled or abandoned and a nil error.
func (a *Agent) Request(ctx context.Context, dst AgentID, name string, params any, opts RequestOptions) (Outcome, error) {
	req := a.st.req
	if req == nil {
		return Outcome{}, &CallError{Dst: dst, Name: name, Reason: "the agent is not started"}
	}
	return req.request(ctx, dst, name, params, opts)
}

// Command delivers a command to dst and returns its rid. Whether the peer accepts it is the peer's
// decision, and a command is never answered.
func (a *Agent) Command(ctx context.Context, dst AgentID, name string, params any) (RID, error) {
	req := a.st.req
	if req == nil {
		return "", &CallError{Dst: dst, Name: name, Reason: "the agent is not started"}
	}
	return req.command(ctx, dst, name, params)
}

// Fail publishes the error state and announces it: a fatal error we cannot recover from, told to
// the team instead of hidden. It does not stop the agent — the agent stays in the error state until
// it is stopped (D-22), so a peer can see why. A second call while already failed does nothing: no
// second announce, no recovery.
func (a *Agent) Fail(message string) error {
	st := a.st
	if st.files == nil {
		return fmt.Errorf("actio: the agent %s has never started", st.id)
	}
	st.pubMu.Lock()
	already := st.current == StatusError
	st.pubMu.Unlock()
	if already {
		return nil
	}
	if err := a.setState(StatusError); err != nil {
		return err
	}
	return a.announceAfterDelay(context.Background(), fmt.Sprintf("agent %s failed: %s", st.id, message))
}

// Diarium reads this agent's own log.
func (a *Agent) Diarium() (DiariumRead, error) {
	if a.st.folders.agent == "" {
		return DiariumRead{}, fmt.Errorf("actio: the agent %s has never started", a.st.id)
	}
	return ReadDiarium(a.st.folders.agent)
}

// Log appends a log event to our diarium. It emits no ActioEvent: an event callback is not a log
// consumer, as in the reference.
func (a *Agent) Log(message string) error {
	st := a.st
	if st.diaria == nil {
		return fmt.Errorf("actio: the agent %s has never started", st.id)
	}
	return st.diaria.append(logEvent(st.id, st.now(), message))
}

// onEvent is the agent's dispatch of one observability event: the user's callbacks, one at a time
// under the dispatch mutex (the second of the four), then the agent's own observation, outside the
// mutex so the observation's publication cannot invert the lock order.
func (a *Agent) onEvent(e Event) {
	a.st.dispatchMu.Lock()
	for _, fn := range a.st.listeners {
		fn(e)
	}
	a.st.dispatchMu.Unlock()
	a.observe(e)
}

// observe is the agent's own reading of the events: a lost own inbox or nuntium is D-22 — the log
// event came with it, and the agent fails itself from it.
func (a *Agent) observe(e Event) {
	ig, ok := e.(IgnoredEvent)
	if !ok || (ig.Path != a.st.folders.inbox && ig.Path != a.st.folders.nuntium) {
		return
	}
	a.Fail("watch lost " + ig.Path)
}

// logf is the agent's log shortcut: a diarium log line, no event. A failed append is dropped: the
// log is the report of the state, not the state itself.
func (a *Agent) logf(format string, args ...any) {
	if a.st.diaria == nil {
		return
	}
	_ = a.st.diaria.append(logEvent(a.st.id, a.st.now(), fmt.Sprintf(format, args...)))
}

// tick is the heartbeat's tick (D-9 v1.8): it publishes under the publication mutex, and it writes
// nothing once the state is inactive, so it can never overwrite C1. It is a refresh: the status
// file with a fresh datetime, no diarium event, no announce.
func (a *Agent) tick() error {
	a.st.pubMu.Lock()
	defer a.st.pubMu.Unlock()
	if a.st.current == StatusInactive {
		return nil
	}
	return a.publishStatusFileLocked()
}

// setState changes the lifecycle state: the state first, then the status file, then the diarium
// event, and then the StatusEvent, outside the publication mutex, as every event is. A failed
// publish keeps the state as chosen, logs it, and reports the error.
func (a *Agent) setState(s StatusState) error {
	st := a.st
	st.pubMu.Lock()
	changed := st.current != s
	st.current = s
	var err error
	if changed {
		err = a.publishStatusFileLocked()
		if err == nil {
			err = st.diaria.append(statusEvent(st.id, st.now(), s))
		}
	}
	st.pubMu.Unlock()
	if err != nil {
		st.logf("status %q: %v", s, err)
		return err
	}
	if changed {
		st.emit(StatusEvent{State: s})
	}
	return nil
}

// publishStatusFileLocked writes status.json for the live state: the state as published (busy when
// work is in flight, D-8), a fresh datetime, and the capability counters as they are now. The
// caller holds the publication mutex.
func (a *Agent) publishStatusFileLocked() error {
	var caps []CapabilityStatus
	if a.st.recv != nil {
		caps = a.st.recv.capabilityStatus()
	}
	status := buildStatus(a.st.id, a.publishedStateLocked(), a.st.now(), caps)
	return publishStatus(a.st.files, a.st.folders.agent, &status)
}

// publishedStateLocked is the state as the peers will see it: busy when the lifecycle state is
// idle and work is in flight, an inbound exchange (the receiver's counters) or an outbound one (B2)
// (D-8). The caller holds the publication mutex.
func (a *Agent) publishedStateLocked() StatusState {
	if a.st.current == StatusIdle && a.busy() {
		return StatusBusy
	}
	return a.st.current
}

// busy is D-8: busy iff an inbound exchange is running or an outbound one (B2) is in flight.
func (a *Agent) busy() bool {
	if a.st.recv != nil && a.st.recv.busy() {
		return true
	}
	return a.st.req != nil && a.st.req.outboundCount() > 0
}

// rebuildSpec builds the spec from the options and the registrations, re-applies the run's enabled
// state (D-10), and stores it. It is called with nothing held: the store is atomic.
func (a *Agent) rebuildSpec() (*Spec, error) {
	st := a.st
	caps := make([]Capability, 0, len(st.capOrder))
	for _, name := range st.capOrder {
		caps = append(caps, st.caps[name])
	}
	spec, err := buildSpec(st.opts, caps, st.now())
	if err != nil {
		return nil, err
	}
	if st.enabled != nil { // the enable command's state: every rebuild during a run keeps it
		spec.Disabled = st.enabled
	}
	st.specNow.Store(spec)
	return spec, nil
}

// announceAfterDelay writes the announce file after the protocol's delay, with its diarium event,
// under the publication mutex, as every publication is. The delay is bounded by ctx, as every wait
// of the lifecycle is.
func (a *Agent) announceAfterDelay(ctx context.Context, text string) error {
	st := a.st
	select {
	case <-time.After(st.config.AnnounceDelay):
	case <-ctx.Done():
		return ctx.Err()
	}
	st.pubMu.Lock()
	err := publishAnnounce(st.files, st.diaria, st.folders.agent, st.folders.nuntium, st.id, text, st.now())
	st.pubMu.Unlock()
	if err != nil {
		st.logf("announce: %v", err)
		return err
	}
	st.emit(AnnounceEvent{Text: text})
	return nil
}

// discoveryLoop is the agent's discovery goroutine: a nuntium wake-up schedules one debounced full
// scan (D-17), a loss is D-22, and the stop ends it with the pending debounce dropped. The timer
// and its fired channel belong to this goroutine alone; the timer callback only sends, as §5.4
// orders.
func (a *Agent) discoveryLoop(nuntium *folderWatch) {
	st := a.st
	var timer *time.Timer
	fired := make(chan struct{}, 1)
	for {
		select {
		case <-st.discStop:
			if timer != nil {
				timer.Stop()
			}
			return
		case <-nuntium.changed():
			if timer == nil { // a burst of wake-ups schedules one scan, not one per event
				timer = time.AfterFunc(st.config.NuntiumDebounce, func() { fired <- struct{}{} })
			}
		case <-fired:
			timer = nil
			a.rescan()
		case err := <-nuntium.lost():
			st.watch.unsubscribe(nuntium)
			st.emit(IgnoredEvent{Path: st.folders.nuntium, Reason: "watch lost: " + err.Error()})
			return
		}
	}
}

// rescan is one full discovery scan: the tree read by Scan, the callability rules per peer, and
// the atomic swap of the view (the discovery's publish, no mutex: one pointer). What reaches
// IgnoredEvent is exactly what Scan reports — no dedup, no reason of our own.
func (a *Agent) rescan() {
	st := a.st
	seen, err := Scan(st.folders.root)
	if err != nil {
		st.logf("discovery scan failed: %v", err)
		return
	}
	views, specs := make(map[AgentID]AgentView, len(seen.Agents)), make(map[AgentID]*Spec, len(seen.Agents))
	self, now := st.specNow.Load(), st.now()
	for _, peer := range seen.Agents {
		spec := peer.Spec
		specs[spec.ID] = &spec // unfiltered: code 106 sees what the scan found
		if view := callable(self, peer, now); view != nil {
			views[view.ID] = *view
		}
	}
	st.view.Store(&peerSet{views: views, specs: specs})
	for _, folder := range seen.Duplicates {
		st.emit(LogEvent{Message: fmt.Sprintf("duplicate agent id: folder %q was skipped (D-3)", folder)})
	}
	for _, entry := range seen.Ignored {
		st.emit(IgnoredEvent{Path: entry.Path, Reason: entry.Reason})
	}
}

// peerSet is the discovery's published state: the callable peers and the unfiltered specs of the
// last scan, swapped together by one atomic store.
type peerSet struct {
	views map[AgentID]AgentView
	specs map[AgentID]*Spec
}

// --- lane C: the four protocol commands ---------------------------------------------------------

// cmdStop is the `stop` command: the agent stops itself, with the force the sender gave. The
// handler's own context is the receiver's and dies with the stop, so the stop runs on its own bound
// context: the command is never answered, and the stop must be able to finish.
func (a *Agent) cmdStop(ctx context.Context, params json.RawMessage, cmd *CommandContext) error {
	var p struct {
		Force bool `json:"force"`
	}
	_ = json.Unmarshal(params, &p)
	bound, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return a.stopInternal(bound, StopOptions{Force: p.Force}, true)
}

// cmdEnable is the `enable` command: the disabled flag belongs to the built spec, not to the
// options (D-10), so every later rebuild keeps it, and only a fresh start clears it.
func (a *Agent) cmdEnable(ctx context.Context, params json.RawMessage, cmd *CommandContext) error {
	var p struct {
		Enabled bool `json:"enabled"`
	}
	_ = json.Unmarshal(params, &p)
	disabled := !p.Enabled
	st := a.st
	st.pubMu.Lock()
	st.enabled = &disabled
	spec, err := a.rebuildSpec()
	if err == nil {
		err = publishSpec(st.files, st.folders.agent, spec)
	}
	st.pubMu.Unlock()
	if err != nil {
		return err
	}
	word := "disabled"
	if p.Enabled {
		word = "enabled"
	}
	return a.announceAfterDelay(ctx, fmt.Sprintf("agent %s is %s", st.id, word))
}

// cmdRestart is the `restart` command (D-10): stop, wait the delay, start fresh in the same
// object. The registrations survive; the disabled flag set by enable does not. It runs on the
// receiver's command goroutine, which outlives the stop: the stop cancels the receiver's context,
// not the goroutine, and the new state is armed before the handler returns.
func (a *Agent) cmdRestart(ctx context.Context, params json.RawMessage, cmd *CommandContext) error {
	var p struct {
		Force *bool `json:"force"`
		Delay *int  `json:"delay"`
	}
	_ = json.Unmarshal(params, &p)
	force := p.Force != nil && *p.Force
	bound, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := a.stopInternal(bound, StopOptions{Force: force}, false)
	cancel()
	if err != nil {
		return err
	}
	if p.Delay != nil && *p.Delay > 0 {
		select {
		case <-time.After(time.Duration(*p.Delay) * time.Second):
		case <-ctx.Done(): // the receiver's stop cancelled this context: start without the delay
		}
	}
	return a.Start(context.Background())
}

// cmdPing is the `ping` command: a fresh datetime in the spec and the status, then the re-announce.
func (a *Agent) cmdPing(ctx context.Context, params json.RawMessage, cmd *CommandContext) error {
	a.st.pubMu.Lock()
	spec, err := a.rebuildSpec()
	if err == nil {
		err = publishSpec(a.st.files, a.st.folders.agent, spec)
	}
	if err == nil {
		err = a.publishStatusFileLocked()
	}
	a.st.pubMu.Unlock()
	if err != nil {
		return err
	}
	return a.announceAfterDelay(ctx, fmt.Sprintf("agent %s is alive", a.st.id))
}

// agentState is the wiring the protocol files share, created by New and by the CLI's Call. Every
// piece of it belongs to one agent: there is no process-wide state in this package.
type agentState struct {
	id      AgentID
	config  Config
	folders agentFolders
	files   *fileIO
	sync    syncProvider
	diaria  *diaria
	watch   *watcher
	now     func() time.Time
	// emit hands an observability event to the callbacks; logf is the shortcut for a LogEvent.
	emit func(event Event)
	logf func(format string, args ...any)
	// spec and state read what the agent has published, for the code that needs the current view.
	spec  func() *Spec
	state func() StatusState

	// The lifecycle wiring (frozen-interfaces §3, the one allowed change to agentState): the
	// registrations, the two agent-owned mutexes (publication and the OnEvent dispatch), the
	// loops, and the lifecycle state.
	opts       Options
	caps       map[string]Capability
	capOrder   []string
	handlers   map[string]CapabilityHandler
	cmds       map[string]CommandHandler
	pubMu      sync.Mutex // 1: the publication of status, spec, the announce and the state's diarium
	dispatchMu sync.Mutex // 2: the OnEvent dispatch
	listeners  []func(Event)
	recv       *receiver
	req        *requestor
	hb         *heartbeat
	hbWg       sync.WaitGroup // the heartbeat's one tracked goroutine
	hbDone     chan struct{}
	wg         sync.WaitGroup // the agent's own tracked goroutines: discovery, the hb join, the stop runner
	haltCh     chan struct{}  // the hard abort of a failed Start: the receiver's force path
	discStop   chan struct{}
	running    atomic.Bool
	stopping   atomic.Bool
	doneCh     chan struct{}
	doneClosed bool
	enabled    *bool       // D-10: the enable command's disabled state for this run
	current    StatusState // the lifecycle state; busy is derived, under pubMu
	specNow    atomic.Pointer[Spec]
	view       atomic.Pointer[peerSet]
}

// agentFolders are the folders of one agent inside the actio root, all absolute: the shared root,
// the agent's folder (also the tmpDir of every atomic write), and the three protocol folders.
type agentFolders struct{ root, agent, inbox, forum, nuntium string }
