// The agent: its lifecycle, the protocol's own commands (lane C), and the public API.
//
// Start creates the folders, publishes spec.json and status.json, announces, watches its inbox and
// nuntium, and starts the heartbeat and the debounced rescans. Stop publishes inactive, announces,
// and leaves nothing armed: no goroutine, ticker or timer (D-9). A restart rebuilds that state in
// the same agent object: the registrations survive, a disabled flag set by the enable command does
// not, and Done stays the same channel (D-10).
//
// One run is one agentState: fully built, published through one atomic pointer, and never
// rewritten, so a goroutine that outlives its run reads a stale run, not a live one changed under
// it — that is what makes a peer-triggered restart safe against Request, Command, Log and rescan.
// What outlives a run lives in the shared part, embedded in every run. The lifecycle has one gate,
// a channel of capacity 1, that Start and every stop take, so a wait for it can be bounded by a
// ctx, a second stop waits for the first, and a stop during Start waits for Start (D-9).
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
	shared *agentShared
	run    atomic.Pointer[agentState]
}

// cur is the current run: its fields are never rewritten, so a stale goroutine reads a stale run,
// which is harmless.
func (a *Agent) cur() *agentState { return a.run.Load() }

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
	sh := &agentShared{
		opts: opts, doneCh: make(chan struct{}), life: make(chan struct{}, 1),
		caps: map[string]Capability{}, handlers: map[string]CapabilityHandler{}, cmds: map[string]CommandHandler{},
	}
	st := &agentState{
		agentShared: sh, id: opts.ID, now: time.Now,
		emit: func(Event) {}, logf: func(string, ...any) {}, failWatch: func(string, error) {},
		spec: func() *Spec { return nil }, state: func() StatusState { return StatusInactive },
		current: StatusInactive,
	}
	st.view.Store(&peerSet{views: map[AgentID]AgentView{}, specs: map[AgentID]*Spec{}})
	a := &Agent{shared: sh}
	a.run.Store(st)
	return a, nil
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
	st := a.cur()
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
	st := a.cur()
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

// OnEvent adds an observability callback. Callbacks run synchronously on the goroutine that
// produced the event, in order, one at a time, and none runs under a mutex: any API may be called
// from a callback. A callback must return: the one limit this does not remove is that a callback
// that never returns cannot be bounded when it runs on Stop's own goroutine.
func (a *Agent) OnEvent(fn func(Event)) {
	a.shared.dispatchMu.Lock()
	a.shared.listeners = append(a.shared.listeners, fn)
	a.shared.dispatchMu.Unlock()
}

// Start publishes the agent and arms its watchers, its heartbeat and its receiver. ctx bounds the
// startup only: cancelling it later does not stop the agent. A stop during Start waits for it at
// the gate, and a failed Start aborts once, leaving nothing armed (D-9).
func (a *Agent) Start(ctx context.Context) error {
	if err := a.acquire(ctx); err != nil {
		return err
	}
	defer a.release()
	return a.startLocked(ctx)
}

// acquire takes the lifecycle gate, bounded by ctx: Start and every stop run one at a time, so a
// wait for the gate can be told to give up.
func (a *Agent) acquire(ctx context.Context) error {
	select {
	case a.shared.life <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release gives back the lifecycle gate.
func (a *Agent) release() { <-a.shared.life }

// startLocked starts one run. The caller holds the gate. A start cancels a restart still in its
// delay: the agent is being started, so the pending restart is superseded (D-10).
func (a *Agent) startLocked(ctx context.Context) error {
	sh := a.shared
	if !sh.running.CompareAndSwap(false, true) {
		return fmt.Errorf("actio: the agent %s is already started", sh.opts.ID)
	}
	if sh.restartAbort != nil {
		close(sh.restartAbort)
		sh.restartAbort = nil
	}
	st := &agentState{agentShared: sh, id: sh.opts.ID, current: StatusInactive}
	st.view.Store(&peerSet{views: map[AgentID]AgentView{}, specs: map[AgentID]*Spec{}})
	if err := a.startUp(ctx, st); err != nil {
		a.abortStart(ctx, st)
		return err
	}
	return nil
}

// startUp is the lifecycle order: config, folders, the one watcher, the subscriptions, the
// receiver, the heartbeat, and the first publication. A failure at any step leaves nothing armed.
// The run is published through a.run only once everything in it is built: from then on, everyone
// sees this run, and nothing in it is rewritten.
func (a *Agent) startUp(ctx context.Context, st *agentState) error {
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
	// D-22: a lost own inbox or nuntium is fatal — the log event on the stream, the diarium line,
	// and then the fail, in that order.
	st.failWatch = func(path string, err error) {
		st.emit(LogEvent{Message: "watch lost " + path + ": " + err.Error()})
		st.logf("watch lost %s: %v", path, err)
		_ = a.Fail("watch lost " + path)
	}
	st.spec = func() *Spec { return st.specNow.Load() }
	st.state = func() StatusState {
		st.pubMu.Lock()
		defer st.pubMu.Unlock()
		return a.publishedStateLocked()
	}
	st.discStop = make(chan struct{})
	for _, dir := range []string{cfg.Root, st.folders.nuntium, st.folders.forum, st.folders.agent, st.folders.inbox} {
		if err := st.files.makeDir(dir); err != nil {
			return err // abortStart closes the provider
		}
	}
	// A bad registration fails here, before anything lands in the tree.
	spec, err := a.rebuildSpec(st)
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
	// The receiver's code 106 pairs the self spec (one atomic) with a peer spec (another): the pair
	// can be one scan apart, and a namespace decision made on a stale self spec self-corrects on the
	// next scan — the specs change only on a start, an enable, or a peer's publication, each of
	// which re-scans. Kept on purpose: a shared snapshot would add a publication for a window that
	// costs at most one transient accept/reject, and the protocol re-evaluates per exchange.
	recv.setPeerSpec(func(id AgentID) *Spec { return st.view.Load().specs[id] }) // code 106 sees the discovered spec
	st.recv = recv
	st.req = newRequestor(st)
	st.hb = newHeartbeat(cfg.HeartbeatInterval, a.tick)
	// Everything is built: from here on, everyone sees this run, and nothing in it is rewritten.
	a.run.Store(st)
	st.discArmed = true
	st.wg.Go(func() { a.discoveryLoop(nuntium) })
	if err := recv.start(&st.wg); err != nil {
		return err
	}
	st.hbDone = make(chan struct{})
	st.hb.start(&st.hbWg)
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
		return err
	}
	if err := a.setState(StatusIdle); err != nil {
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

// abortStart drops what a failed Start armed, and re-arms none: the discovery loop, the receiver,
// the heartbeat, the watcher and the provider all end, and the agent is not running (D-9). It
// runs once, on the run that failed, and it publishes inactive if that run had published idle: a
// published idle must not outlive the run. The two waits are bounded by ctx — what is stuck costs
// its own goroutine until it returns, and the caller is told through ctx — so a failed Start can
// never hold the lifecycle gate forever (D-9).
func (a *Agent) abortStart(ctx context.Context, st *agentState) {
	if st.hb != nil {
		st.hb.stop()
	}
	if st.hbDone != nil {
		select {
		case <-st.hbDone:
		case <-ctx.Done():
		}
	}
	if st.current != StatusInactive { // a published idle must not outlive the run
		_ = a.setState(StatusInactive)
	}
	if st.recv != nil {
		_ = st.recv.stop(true, 0)
	}
	if st.req != nil {
		st.req.stop()
	}
	if st.discArmed {
		close(st.discStop)
		_ = waitBounded(ctx, &st.wg)
	}
	if st.watch != nil {
		st.watch.close()
	}
	if st.sync != nil {
		st.sync.Close()
	}
	a.shared.running.Store(false)
}

// Stop ends the agent: it answers or cancels the work in progress, publishes inactive, announces,
// releases its watchers and timers, and waits for every goroutine to end. ctx bounds the wait, not
// the protocol's grace period: opts.Force cancels the work instead of waiting for it. A stop that
// finds one in flight waits for it at the gate and then returns: it returns only once the other
// stop has finished, so the caller can rely on inactive being on disk (D-9).
func (a *Agent) Stop(ctx context.Context, opts StopOptions) error {
	return a.stopInternal(ctx, opts, true)
}

// stopInternal is D-9 in that order: the heartbeat, waited for within ctx, before C1 (the tick
// publishes under the publication mutex and writes nothing once the state is inactive, so it can
// never overwrite C1; the bound is this ctx, and a stuck tick costs one goroutine until its write
// returns); the receiver; the outbound side; C1, the status first and the delayed announce after;
// C2, nothing armed. All the waits are bounded by ctx.
//
// It takes the lifecycle gate, so one D-9 sequence runs at a time, from any entry: a second stop —
// a second stop command, a retry after a failed C1 — waits for the first at the gate, within its
// ctx, and a stop after a completed one sees the agent not running and does nothing.
func (a *Agent) stopInternal(ctx context.Context, opts StopOptions, closeDone bool) error {
	if err := a.acquire(ctx); err != nil {
		return err // a stop or a start is in flight and ctx ran out: the caller is told
	}
	defer a.release()
	sh := a.shared
	if closeDone && sh.restartAbort != nil { // a stop wins over a restart still in its delay
		close(sh.restartAbort)
		sh.restartAbort = nil
		if !sh.running.Load() {
			a.closeDoneLocked()
			return nil
		}
	}
	if !sh.running.Load() {
		return nil
	}
	st := a.cur()
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
	st.req.stop()
	// D-9 (v1.11): the outbound abandons, then C1. The stop waits, within its ctx, for the exchanges
	// it abandoned so their outreq lines land before the inactive, as in TS. This is a guarantee, not
	// a race we usually win: each exchange writes its outreq line before its deferred inflight.Done(),
	// so the wait is a happens-before edge over the line. D-9's other half: a blocked exchange costs a
	// goroutine and Stop returns ctx.Err().
	if err := waitBounded(ctx, &st.req.inflight); err != nil {
		return err
	}
	if st.discClosed.CompareAndSwap(false, true) {
		close(st.discStop) // the discovery loop: no more scans, the pending debounce is dropped
	}
	if err := waitBounded(ctx, &st.wg); err != nil {
		return err
	}
	// C1: the status first, then the delayed announce, then the sync flush. A failed publish is
	// logged, as in the reference: the state stays as chosen, and the next setState republishes it.
	a.setState(StatusInactive)
	if err := a.announceAfterDelay(ctx, fmt.Sprintf("agent %s is inactive", st.id)); err != nil {
		return err
	}
	if err := st.sync.Flush(ctx); err != nil {
		return err
	}
	// C2: deinit. The watchers are gone, the provider is closed, the in-memory view is dropped,
	// nothing is armed. Done closes once per agent's life — a restart's stop must not close it.
	st.watch.close()
	st.sync.Close()
	st.view.Store(&peerSet{views: map[AgentID]AgentView{}, specs: map[AgentID]*Spec{}})
	sh.running.Store(false)
	if closeDone {
		a.closeDoneLocked()
	} else {
		sh.restartAbort = make(chan struct{}) // the restart is pending in its delay; a stop closes it
	}
	return nil
}

// closeDoneLocked closes Done once per agent's life. The caller holds the lifecycle gate.
func (a *Agent) closeDoneLocked() {
	if !a.shared.doneClosed {
		close(a.shared.doneCh)
		a.shared.doneClosed = true
	}
}

// waitBounded waits for wg within ctx: what is stuck costs its goroutines until it returns, and
// the caller is told.
func waitBounded(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Done closes when the agent stops. A restart does not close it. It is what lets a program select
// on the agent's life instead of polling.
func (a *Agent) Done() <-chan struct{} { return a.shared.doneCh }

// Agmen is the snapshot of the peers we can talk to, keyed by id (the callability rules applied).
// Treat the maps as read-only: the next scan replaces them.
func (a *Agent) Agmen() map[AgentID]AgentView { return a.cur().view.Load().views }

// Refresh rescans now and returns the same view, so a program that just started a peer can see it
// without waiting for the debounced scan. Before Start there is nothing to scan, and it says so,
// as Request and Command do for the same situation.
func (a *Agent) Refresh(ctx context.Context) (map[AgentID]AgentView, error) {
	if !a.cur().running.Load() {
		return nil, fmt.Errorf("actio: the agent %s has not started", a.cur().id)
	}
	a.rescan()
	return a.cur().view.Load().views, nil
}

// Request is lane B from a started agent. Cancellation of ctx does not return at once: it writes the
// cancellation and keeps waiting for the response as the protocol requires, then returns with
// FinalStatus cancelled or abandoned and a nil error.
func (a *Agent) Request(ctx context.Context, dst AgentID, name string, params any, opts RequestOptions) (Outcome, error) {
	req := a.cur().req
	if req == nil {
		return Outcome{}, &CallError{Dst: dst, Name: name, Reason: "the agent is not started"}
	}
	return req.request(ctx, dst, name, params, opts)
}

// Command delivers a command to dst and returns its rid. Whether the peer accepts it is the peer's
// decision, and a command is never answered.
func (a *Agent) Command(ctx context.Context, dst AgentID, name string, params any) (RID, error) {
	req := a.cur().req
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
	st := a.cur()
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
	if a.cur().folders.agent == "" {
		return DiariumRead{}, fmt.Errorf("actio: the agent %s has never started", a.cur().id)
	}
	return ReadDiarium(a.cur().folders.agent)
}

// Log appends a log event to our diarium. It emits no ActioEvent: an event callback is not a log
// consumer, as in the reference.
func (a *Agent) Log(message string) error {
	st := a.cur()
	if st.diaria == nil {
		return fmt.Errorf("actio: the agent %s has never started", st.id)
	}
	return st.diaria.append(logEvent(st.id, st.now(), message))
}

// onEvent is the agent's dispatch of observability events. The dispatch mutex guards the queue
// only (the second of the four): an event that arrives while a delivery is in progress joins the
// queue, and whoever is delivering delivers the rest, in order, one at a time. No callback runs
// under a mutex, so any API may be called from a callback.
func (a *Agent) onEvent(e Event) {
	sh := a.shared
	sh.dispatchMu.Lock()
	sh.queue = append(sh.queue, e)
	if sh.dispatching { // someone is delivering, maybe us from a callback: they deliver this one too
		sh.dispatchMu.Unlock()
		return
	}
	sh.dispatching = true
	for len(sh.queue) > 0 {
		next, listeners := sh.queue[0], sh.listeners
		sh.queue = sh.queue[1:]
		sh.dispatchMu.Unlock()
		for _, fn := range listeners {
			fn(next)
		}
		sh.dispatchMu.Lock()
	}
	sh.dispatching = false
	sh.dispatchMu.Unlock()
}

// logf is the agent's log shortcut: a diarium log line, no event. A failed append is dropped: the
// log is the report of the state, not the state itself.
func (a *Agent) logf(format string, args ...any) {
	if a.cur().diaria == nil {
		return
	}
	_ = a.cur().diaria.append(logEvent(a.cur().id, a.cur().now(), fmt.Sprintf(format, args...)))
}

// tick is the heartbeat's tick (D-9): it publishes under the publication mutex, and it writes
// nothing once the state is inactive, so it can never overwrite C1. The guard is belt-and-braces:
// every stop waits for the heartbeat before C1, so only a tick in the start window, before the
// first idle, can see inactive. It is a refresh: the status file with a fresh datetime, no diarium
// event, no announce.
func (a *Agent) tick() error {
	a.cur().pubMu.Lock()
	defer a.cur().pubMu.Unlock()
	if a.cur().current == StatusInactive {
		return nil
	}
	return a.publishStatusFileLocked()
}

// setState changes the lifecycle state: the state first, then the status file, then the diarium
// event, and then the StatusEvent, outside the publication mutex, as every event is. A failed
// publish keeps the state as chosen, logs it, and reports the error — and it is published again by
// the next setState: a stop that fails C1 is retried, and the retry must land.
func (a *Agent) setState(s StatusState) error {
	st := a.cur()
	st.pubMu.Lock()
	changed := st.current != s || st.pubFailed // a retry publishes what the last try failed to
	st.current = s
	var err error
	if changed {
		err = a.publishStatusFileLocked()
		if err == nil {
			err = st.diaria.append(statusEvent(st.id, st.now(), s))
		}
	}
	st.pubFailed = err != nil
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
	if a.cur().recv != nil {
		caps = a.cur().recv.capabilityStatus()
	}
	status := buildStatus(a.cur().id, a.publishedStateLocked(), a.cur().now(), caps)
	return publishStatus(a.cur().files, a.cur().folders.agent, &status)
}

// publishedStateLocked is the state as the peers will see it: busy when the lifecycle state is
// idle and work is in flight, an inbound exchange (the receiver's counters) or an outbound one (B2)
// (D-8). The caller holds the publication mutex.
func (a *Agent) publishedStateLocked() StatusState {
	if a.cur().current == StatusIdle && a.busy() {
		return StatusBusy
	}
	return a.cur().current
}

// busy is D-8: busy iff an inbound exchange is running or an outbound one (B2) is in flight.
func (a *Agent) busy() bool {
	if a.cur().recv != nil && a.cur().recv.busy() {
		return true
	}
	return a.cur().req != nil && a.cur().req.outboundCount() > 0
}

// rebuildSpec builds the spec from the options and the registrations, re-applies the run's enabled
// state (D-10), and stores it. It is called with nothing held: the store is atomic.
func (a *Agent) rebuildSpec(st *agentState) (*Spec, error) {
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
	st := a.cur()
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
	st := a.cur()
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
			st.failWatch(st.folders.nuntium, err) // D-22: the own nuntium is fatal
			return
		}
	}
}

// rescan is one full discovery scan: the tree read by Scan, the callability rules per peer, and
// the atomic swap of the view (the discovery's publish, no mutex: one pointer). What reaches
// IgnoredEvent is exactly what Scan reports — no dedup, no reason of our own.
func (a *Agent) rescan() {
	st := a.cur()
	seen, err := Scan(st.folders.root)
	if err != nil {
		st.logf("discovery scan failed: %v", err)
		return
	}
	views, specs := make(map[AgentID]AgentView, len(seen.Agents)), make(map[AgentID]*Spec, len(seen.Agents))
	self, now := st.specNow.Load(), st.now()
	if self == nil {
		st.logf("discovery scan skipped: the spec is not built yet")
		return
	}
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

// maxRestartDelay bounds the restart command's delay: a peer in accepts_commands_from must not be
// able to keep the agent down for an arbitrary time, so a delay above the bound is clamped to it.
const maxRestartDelay = 60 * time.Second

// commandStopBound is what a stop command gets beyond the protocol's grace: the grace is the
// work's, this is the stop's own.
const commandStopBound = 30 * time.Second

// clampedRestartDelay is the restart command's delay policy: a positive delay is honoured up to
// the bound, anything above it clamps to the bound, and a non-positive delay arms no wait.
func clampedRestartDelay(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	if d > maxRestartDelay {
		return maxRestartDelay
	}
	return d
}

// commandStopBoundFor is a stop command's bound: the protocol's grace plus what the stop itself
// needs beyond it. A huge grace clamps nothing: the bound grows with the grace.
func commandStopBoundFor(grace time.Duration) time.Duration { return grace + commandStopBound }

// cmdStop is the `stop` command: the agent stops itself, with the force the sender gave. The
// handler's own context is the receiver's and dies with the stop, so the stop runs on its own bound
// context: the command is never answered, and the stop must be able to finish. The bound is the
// protocol's grace plus the stop's own: with work that outlives the grace, a bare grace-length
// bound would expire first and leave the agent half-stopped.
func (a *Agent) cmdStop(ctx context.Context, params json.RawMessage, cmd *CommandContext) error {
	var p struct {
		Force bool `json:"force"`
	}
	_ = json.Unmarshal(params, &p)
	bound, cancel := context.WithTimeout(context.Background(), commandStopBoundFor(a.cur().config.StopGrace))
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
	st := a.cur()
	st.pubMu.Lock()
	st.enabled = &disabled
	spec, err := a.rebuildSpec(st)
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
// not the goroutine, and the new state is armed before the handler returns. A stop or a start
// during the delay cancels the restart: a stop then closes Done, since the agent is down and
// nobody is left to bring it back, and a restart whose Start fails closes Done for the same
// reason.
func (a *Agent) cmdRestart(ctx context.Context, params json.RawMessage, cmd *CommandContext) error {
	var p struct {
		Force *bool    `json:"force"`
		Delay *float64 `json:"delay"` // the schema's duration is a number: a fraction is honoured
	}
	_ = json.Unmarshal(params, &p)
	force := p.Force != nil && *p.Force
	bound, cancel := context.WithTimeout(context.Background(), commandStopBoundFor(a.cur().config.StopGrace))
	err := a.stopInternal(bound, StopOptions{Force: force}, false)
	cancel()
	if err != nil {
		return err
	}
	// The delay is honoured on a fresh timer: the handler's context is the receiver's, and the stop
	// above already cancelled it, so it is dead and cannot carry the wait. Above the bound, it is
	// clamped to the bound: the peer asked for a restart, and it gets one.
	delay := time.Duration(0)
	if p.Delay != nil {
		delay = clampedRestartDelay(time.Duration(*p.Delay * float64(time.Second)))
	}
	if err := a.acquire(context.Background()); err != nil {
		return err
	}
	abort := a.shared.restartAbort
	a.release()
	if abort == nil {
		return nil // a stop or a start got there first
	}
	select {
	case <-time.After(delay):
	case <-abort:
		return nil // a stop won the delay: it closed Done
	}
	if err := a.acquire(context.Background()); err != nil {
		return err
	}
	defer a.release()
	if a.shared.restartAbort != abort {
		return nil // a stop or a start superseded this restart
	}
	if err := a.startLocked(context.Background()); err != nil {
		a.closeDoneLocked() // the restart failed: nobody will restart it, so the agent has stopped
		return err
	}
	return nil
}

// cmdPing is the `ping` command: a fresh datetime in the spec and the status, then the re-announce.
func (a *Agent) cmdPing(ctx context.Context, params json.RawMessage, cmd *CommandContext) error {
	a.cur().pubMu.Lock()
	spec, err := a.rebuildSpec(a.cur())
	if err == nil {
		err = publishSpec(a.cur().files, a.cur().folders.agent, spec)
	}
	if err == nil {
		err = a.publishStatusFileLocked()
	}
	a.cur().pubMu.Unlock()
	if err != nil {
		return err
	}
	return a.announceAfterDelay(ctx, fmt.Sprintf("agent %s is alive", a.cur().id))
}

// agentState is one run of one agent: the wiring the protocol files share for this run, created
// by startLocked and published whole. Its fields are never rewritten; a restart builds a fresh
// one. What outlives a run is the embedded shared part.
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
	// failWatch is the D-22 fail for a lost own inbox or nuntium: the log event, the diarium line,
	// and the Fail, in that order. The requestor's destination losses do not use it (they only log).
	failWatch func(path string, err error)
	// spec and state read what the agent has published, for the code that needs the current view.
	spec  func() *Spec
	state func() StatusState

	// The run's own wiring: the loops and their stops. The receiver and the requestor keep this
	// run: a goroutine that outlives it reads a stale run, which is harmless.
	recv       *receiver
	req        *requestor
	hb         *heartbeat
	hbWg       sync.WaitGroup // the heartbeat's one tracked goroutine
	hbDone     chan struct{}
	wg         sync.WaitGroup // the agent's own tracked goroutines: discovery, the hb join, the stop runner
	discStop   chan struct{}
	discClosed atomic.Bool // the discovery stop's close has run for this run: stopInternal is re-entrant
	discArmed  bool        // the discovery loop is running: abortStart's close and wait apply
	enabled    *bool       // D-10: the enable command's disabled state for this run
	current    StatusState // the lifecycle state; busy is derived, under pubMu
	pubFailed  bool        // the last status publish failed: the next setState republishes
	specNow    atomic.Pointer[Spec]
	view       atomic.Pointer[peerSet]

	*agentShared
}

// agentShared is what outlives a run: the registrations, the two agent-owned mutexes (publication
// and the OnEvent dispatch), the lifecycle gate, and Done. Embedded by pointer in every run, so
// st.caps and the rest read the same way.
type agentShared struct {
	opts     Options
	caps     map[string]Capability
	capOrder []string
	handlers map[string]CapabilityHandler
	cmds     map[string]CommandHandler

	pubMu       sync.Mutex // 1: the publication of status, spec, the announce and the state's diarium
	dispatchMu  sync.Mutex // 2: the OnEvent queue, never a callback
	listeners   []func(Event)
	queue       []Event
	dispatching bool

	running      atomic.Bool
	life         chan struct{} // the lifecycle gate: capacity 1, so a wait for it can be bounded
	restartAbort chan struct{} // set while a restart is in its delay; a stop or a start closes it
	doneCh       chan struct{}
	doneClosed   bool // Done closes once per life: a restart's stop must not close it again
}

// agentFolders are the folders of one agent inside the actio root, all absolute: the shared root,
// the agent's folder (also the tmpDir of every atomic write), and the three protocol folders.
type agentFolders struct{ root, agent, inbox, forum, nuntium string }
