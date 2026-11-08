// Lane A and lane D: the receiver, the only owner of inbound exchange state.
//
// One goroutine holds every exchange in progress and selects on inbox file names, handler
// completions, timer expiries and stop. Nothing else touches that state, so there is no lock on it.
// Accepted work runs on its own goroutine with a cancellable context and sends its result back over a
// channel; it recovers a panic and answers as for any other failure.
//
// The rules it enforces: the acceptance checks (100, 101, 102, 103 required; 105, 106, 107, 108 made
// here; 104 not) at A0; the four envelopes of an exchange matched by rid and body key; a response
// written once; the confirmation wait (A2); and the archive (A4) that empties the inbox and writes
// the inreq diarium event with the envelopes as exchanged.
package actio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The phases of one inbound request exchange (workflow A0 → A1 → A2 → A4).
const (
	phaseAccepting = "accepting"
	phaseWorking   = "working"
	phaseResponded = "responded"
)

// standardCommands is the set the protocol mandates (*Commands*); a command name is never advertised
// (D0), which accepts these four plus any custom command registered.
var standardCommands = map[string]struct{}{"stop": {}, "enable": {}, "restart": {}, "ping": {}}

// capabilityEntry is one registration: what we publish, its compiled schemas, the handler, and how
// many requests are running or waiting. The receiver owns the counters (status.json reads them); they
// are atomic because capabilityStatus and busy read them from the publication path on another
// goroutine and the receiver may add no mutex of its own. queue is always 0: a request is accepted at
// A0 or refused, never queued.
type capabilityEntry struct {
	capability  Capability
	schemas     *capabilitySchemas
	handler     CapabilityHandler
	running     atomic.Int32
	queue       atomic.Int32
	maxParallel int
}

// capView is the immutable registration view the loop reads; register and registerCommand publish a
// new view (copy-on-write) through the atomic pointer, so a registration during a run needs no lock.
type capView struct {
	caps map[string]*capabilityEntry
	cmds map[string]CommandHandler
}

// exchange is the per-rid state of one inbound request (in memory only; D-4 re-arms from files at
// start). The raw envelopes are kept as exchanged for the A4 diarium line; the decoded structs serve
// the matching and the checks.
type exchange struct {
	rid                            RID
	header                         Header
	request                        *RequestFile
	phase                          string
	canSeen                        bool
	can                            *CancellationFile
	res                            *ResponseFile
	con                            *ConfirmationFile
	reqRaw, canRaw, resRaw, conRaw json.RawMessage // as exchanged; null in the diarium when absent
	responseAttachments            []string        // published to forum; deleted at A4
	cancel                         context.CancelFunc
	timer                          *time.Timer
	workStart                      time.Time
}

// deferredEnv is one envelope that arrived before its request (the watcher delivers in lexical order,
// so .can.json precedes .json). raw is the file's bytes as exchanged, kept for the diarium.
type deferredEnv struct {
	kind string
	can  *CancellationFile
	res  *ResponseFile
	con  *ConfirmationFile
	raw  json.RawMessage
}

// completion is the result of one accepted request, sent back by the work goroutine. Attachments are
// collected goroutine-locally and moved into the exchange by the loop (the state owner).
type completion struct {
	rid         RID
	result      json.RawMessage
	err         error
	attachments []string
}

type commandDone struct {
	rid RID
	err error
}

type stopParams struct {
	force bool
	grace time.Duration
}

// commandJob is one accepted custom command awaiting its archive, so the loop can find it when the
// handler's goroutine reports back.
type commandJob struct {
	file *RequestFile
	raw  json.RawMessage
}

// receiver owns all inbound exchange state of one agent.
type receiver struct {
	started bool // start ran: written before the loop, and every stop after it runs the settle path
	st      *agentState

	// Lane A state, owned by the loop (no lock).
	exchanges   map[RID]*exchange
	archived    map[RID]struct{}
	ignoredSeen map[string]struct{}
	deferred    map[RID][]deferredEnv

	// Lane D state.
	commandRids map[RID]struct{}
	commands    map[RID]*commandJob

	view     atomic.Pointer[capView]
	peerSpec func(AgentID) *Spec

	inbox        *folderWatch
	markers      map[string]string
	initialNames []string

	stopReq     chan stopParams
	done        chan struct{}
	completions chan completion
	commandDone chan commandDone
	timerFired  chan RID

	stopCtx     context.Context
	stopCancel  context.CancelFunc
	stoppedFlag bool
}

// newReceiver builds it on the shared state. It arms nothing: start subscribes the inbox and runs the
// loop on one tracked goroutine.
func newReceiver(st *agentState) *receiver {
	stopCtx, stopCancel := context.WithCancel(context.Background())
	r := &receiver{
		st:          st,
		exchanges:   map[RID]*exchange{},
		archived:    map[RID]struct{}{},
		ignoredSeen: map[string]struct{}{},
		deferred:    map[RID][]deferredEnv{},
		commandRids: map[RID]struct{}{},
		commands:    map[RID]*commandJob{},
		peerSpec:    func(AgentID) *Spec { return nil },
		markers:     map[string]string{},
		stopReq:     make(chan stopParams, 1),
		done:        make(chan struct{}),
		completions: make(chan completion, 64),
		commandDone: make(chan commandDone, 64),
		timerFired:  make(chan RID, 64),
		stopCtx:     stopCtx,
		stopCancel:  stopCancel,
	}
	r.view.Store(&capView{caps: map[string]*capabilityEntry{}, cmds: map[string]CommandHandler{}})
	return r
}

// setPeerSpec wires the requestor-spec lookup code 106 needs: the spec as DISCOVERED by the last scan
// (before the callability rules), or nil when the scan did not find the peer. The agent sets it from
// discovery; until then it returns nil and 106 is not made.
func (r *receiver) setPeerSpec(fn func(AgentID) *Spec) { r.peerSpec = fn }

// register adds a capability before Start, or during a run. A duplicate name, or a capability whose
// schemas do not compile (or use an unknown keyword), is a registration error (D-19).
func (r *receiver) register(cap Capability, handler CapabilityHandler) error {
	schemas, err := compileCapability(&cap)
	if err != nil {
		return err
	}
	for {
		old := r.view.Load()
		if _, dup := old.caps[cap.Name]; dup {
			return fmt.Errorf("actio: the capability %q is already registered", cap.Name)
		}
		caps := make(map[string]*capabilityEntry, len(old.caps)+1)
		for k, v := range old.caps {
			caps[k] = v
		}
		caps[cap.Name] = &capabilityEntry{capability: cap, schemas: schemas, handler: handler, maxParallel: cap.Actio.MaxParallel}
		if r.view.CompareAndSwap(old, &capView{caps: caps, cmds: old.cmds}) {
			return nil
		}
	}
}

// registerCommand adds a command handler; the protocol commands are already there. A name that is one
// of the four protocol commands, or one already registered, is an error.
func (r *receiver) registerCommand(name string, handler CommandHandler) error {
	if _, standard := standardCommands[name]; standard {
		return fmt.Errorf("actio: the command %q is a protocol command and cannot be replaced", name)
	}
	for {
		old := r.view.Load()
		if _, dup := old.cmds[name]; dup {
			return fmt.Errorf("actio: the command %q is already registered", name)
		}
		cmds := make(map[string]CommandHandler, len(old.cmds)+1)
		for k, v := range old.cmds {
			cmds[k] = v
		}
		cmds[name] = handler
		if r.view.CompareAndSwap(old, &capView{caps: old.caps, cmds: cmds}) {
			return nil
		}
	}
}

// start subscribes our inbox on the agent's one watcher (D-23) and runs lanes A and D on one tracked
// goroutine. It scans once after subscribing, so the files present at start are reported once.
func (r *receiver) start(wg *sync.WaitGroup) error {
	sub, err := r.st.watch.subscribe(r.st.folders.inbox)
	if err != nil {
		return err
	}
	changed, next, err := scanFolder(r.st.folders.inbox, r.markers)
	if err != nil {
		r.st.watch.unsubscribe(sub)
		return err
	}
	r.inbox, r.markers, r.initialNames = sub, next, changed
	r.started = true // from here on, every stop runs the settle path, loss or no loss
	wg.Go(func() { r.loop() })
	return nil
}

// loop is the one goroutine that owns the inbound state.
func (r *receiver) loop() {
	defer close(r.done)
	for _, name := range r.initialNames {
		r.processName(name)
	}
	for {
		// After a loss the inbox is nil: nothing more arrives, and the loop stays to answer stop.
		// A lost own inbox is fatal to the agent (D-22), not to this loop.
		var changed <-chan struct{}
		var lost <-chan error
		if r.inbox != nil {
			changed, lost = r.inbox.changed(), r.inbox.lost()
		}
		select {
		case p := <-r.stopReq:
			r.performStop(p.force, p.grace)
			return
		case <-changed:
			r.scanAndProcess()
		case err := <-lost:
			r.handleLoss(err)
		case rid := <-r.timerFired:
			r.onConfirmTimeout(rid)
		case c := <-r.completions:
			r.onCompletion(c)
		case d := <-r.commandDone:
			r.onCommandDone(d)
		}
	}
}

// scanAndProcess re-looks at the inbox after a wake-up: an event only means "look again" (D-23).
func (r *receiver) scanAndProcess() {
	changed, next, err := scanFolder(r.st.folders.inbox, r.markers)
	if err != nil {
		r.handleLoss(err) // a failing scan is the owner's D-22
		return
	}
	r.markers = next
	for _, name := range changed {
		r.processName(name)
	}
}

// handleLoss is the inbox's D-22: release the watch and fail the agent. No retry, no re-arm, no
// polling fallback. The loop stays alive (the inbox channels go nil), so a stop can still settle the
// work in flight.
func (r *receiver) handleLoss(err error) {
	if r.inbox != nil {
		r.st.watch.unsubscribe(r.inbox)
		r.inbox = nil
	}
	r.st.failWatch(r.st.folders.inbox, err)
}

// stop answers the accepted work that has not answered yet with 300 (agent_stopping), lets it finish
// within the grace period or cancels it when force is set, and ends the loop. After it nothing is
// armed (D-9). It blocks until the loop has finished the stop sequence, so every archive has landed.
func (r *receiver) stop(force bool, grace time.Duration) error {
	if !r.started {
		return nil // not started
	}
	select {
	case r.stopReq <- stopParams{force: force, grace: grace}:
	case <-r.done: // already stopped
	}
	<-r.done
	return nil
}

// performStop is the receiver side of D-9, run on the loop: stop accepting, let the running work
// finish within the grace (unless force), answer whatever is still unanswered with 300, and archive
// every exchange that has a response. The archives are inline, so when it returns every inreq line
// and envelope removal has landed (the agent's C1 runs after stop).
func (r *receiver) performStop(force bool, grace time.Duration) {
	if r.stopped() {
		return
	}
	r.stoppedFlag = true
	if r.inbox != nil {
		r.st.watch.unsubscribe(r.inbox)
		r.inbox = nil
	}
	for _, ex := range r.exchanges {
		if ex.timer != nil {
			ex.timer.Stop()
		}
	}
	if !force { // a completion within the grace is answered with its real result, not 300
		deadline := time.Now().Add(grace)
		for r.anyWorking() && time.Until(deadline) > 0 {
			select {
			case c := <-r.completions:
				r.onCompletion(c)
			case <-time.After(time.Until(deadline)):
			}
		}
	}
	r.stopCancel() // cancel the fire-and-forget command goroutines
	for _, ex := range r.exchanges {
		if ex.cancel != nil {
			ex.cancel()
		}
		if ex.phase == phaseWorking && ex.res == nil {
			r.writeResponse(ex, responseOpts{error: true, result: stoppingResult()})
		}
	}
	for _, ex := range r.exchanges {
		r.archive(ex) // settled by the stop: answered, or left unsettled when the answer could not be written
	}
}

func (r *receiver) stopped() bool { return r.stoppedFlag }

// anyWorking reports whether any tracked exchange is still running work: the stop's grace drain ends
// as soon as it is false.
func (r *receiver) anyWorking() bool {
	for _, ex := range r.exchanges {
		if ex.phase == phaseWorking {
			return true
		}
	}
	return false
}

// capabilityStatus is the capability_status list for status.json; it reads the atomic counters, so it
// is safe from the publication path while the loop runs. The counters are loaded one per capability at
// different instants, so the list is a snapshot of a moving system that can mix instants (e.g. the
// agent state "idle" alongside a running count); publishing it is safe and the next publication
// corrects it.
func (r *receiver) capabilityStatus() []CapabilityStatus {
	view := r.view.Load()
	out := make([]CapabilityStatus, 0, len(view.caps))
	for _, e := range view.caps {
		out = append(out, CapabilityStatus{Name: e.capability.Name, Running: int(e.running.Load()), Queue: 0})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// busy reports whether any request is running or waiting: it decides the idle and busy states (D-8).
func (r *receiver) busy() bool {
	for _, e := range r.view.Load().caps {
		if e.running.Load() > 0 {
			return true
		}
	}
	return false
}

// processName dispatches one inbox file (schema validation first, D-15; header consistency, D-11).
func (r *receiver) processName(name string) {
	if r.stopped() || !strings.HasSuffix(name, ".json") {
		return // D-2: only *.json in an inbox
	}
	full := filepath.Join(r.st.folders.inbox, name)
	raw, err := r.st.files.readFile(full)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return // vanished between the wake-up and the read
		}
		r.ignore(full, name, "unreadable: "+err.Error())
		return
	}
	kind, ok := envelopeKind(raw)
	if !ok {
		reason := "no envelope body key"
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(raw, &probe); err != nil {
			reason = "malformed JSON: " + oneLine(err) // the bytes never reached a schema (E14)
		} else if _, verr := validateRequestFile(raw); verr != nil {
			reason = oneLine(verr) // a JSON object that is not an envelope: the schema's words
		}
		r.ignore(full, name, reason)
		return
	}
	var m map[string]json.RawMessage
	_ = json.Unmarshal(raw, &m)
	var enc string
	if json.Unmarshal(m[kind], &enc) == nil { // a string body is base64: encryption (D-14)
		r.ignore(full, name, "base64 body (encryption is not supported)")
		return
	}
	switch kind {
	case BodyRequest:
		if file, err := validateRequestFile(raw); err != nil {
			r.ignore(full, name, oneLine(err))
		} else {
			r.dispatchRequest(file, raw)
		}
	case BodyCancellation:
		if file, err := validateCancellationFile(raw); err != nil {
			r.ignore(full, name, oneLine(err))
		} else {
			r.dispatchCancellation(full, name, file, raw)
		}
	case BodyResponse:
		if file, err := validateResponseFile(raw); err != nil {
			r.ignore(full, name, oneLine(err))
		} else {
			r.dispatchResponse(full, name, file, raw)
		}
	case BodyConfirmation:
		if file, err := validateConfirmationFile(raw); err != nil {
			r.ignore(full, name, oneLine(err))
		} else {
			r.dispatchConfirmation(full, name, file, raw)
		}
	}
}

// dispatchRequest is the lane A entry: cmd goes to lane D; req tracks the exchange and runs A0.
func (r *receiver) dispatchRequest(file *RequestFile, raw json.RawMessage) {
	if file.Type == TypeCommand {
		r.dispatchCommand(file, raw)
		return
	}
	if _, done := r.archived[file.RID]; done {
		return // D-5: duplicate rid
	}
	if _, tracked := r.exchanges[file.RID]; tracked {
		return
	}
	ex := &exchange{rid: file.RID, header: file.Header, request: file, phase: phaseAccepting, reqRaw: raw}
	r.exchanges[file.RID] = ex // tracked before A0: a concurrent .can matches it
	r.drainDeferred(ex)        // the envelopes delivered before this request was tracked
	r.acceptAndRun(ex)
}

// dispatchCommand is the lane D entry: a schema-valid type="cmd" envelope goes through D0, then
// D1–D3 or D4.
func (r *receiver) dispatchCommand(file *RequestFile, raw json.RawMessage) {
	if r.stopped() {
		return // stop() owns the inbox now: the file stays for the next start (D-4)
	}
	rid := file.RID
	if _, done := r.archived[rid]; done {
		return
	}
	if _, done := r.commandRids[rid]; done {
		return // D-5: one command is executed once per rid per run
	}
	r.commandRids[rid] = struct{}{}
	var body RequestBody
	_ = json.Unmarshal(file.Request, &body)
	if reason, ok := r.acceptCommand(file, &body); !ok { // D0
		r.archiveCommand(file, raw, FinalDropped, reason) // D4
		return
	}
	if _, standard := standardCommands[body.Name]; standard {
		r.archiveCommand(file, raw, FinalCompleted, "") // the archive always completes before the hook (D3)
		r.runStandardCommand(file, body.Name, body.Parameters)
		return
	}
	r.commands[rid] = &commandJob{file: file, raw: raw}
	r.runCustomCommand(file, body.Name, body.Parameters)
}

// acceptCommand is D0: may this src command us at all, and do we know the name? A disabled agent
// still answers, so it can be enabled again or shut down.
func (r *receiver) acceptCommand(file *RequestFile, body *RequestBody) (reason string, ok bool) {
	spec := r.st.spec()
	if !containsString(spec.AcceptsCommandsFrom, string(file.Header.Src)) {
		return fmt.Sprintf("%q is not in accepts_commands_from", file.Header.Src), false
	}
	if _, standard := standardCommands[body.Name]; !standard && r.view.Load().cmds[body.Name] == nil {
		return fmt.Sprintf("unknown command %q", body.Name), false
	}
	return "", true
}

// runStandardCommand is D1 for the four mandatory commands: the archive already ran (D3), and the
// handler — wired by the agent — is handed the parsed parameters.
func (r *receiver) runStandardCommand(file *RequestFile, name string, params json.RawMessage) {
	handler := r.view.Load().cmds[name]
	if handler == nil {
		r.st.logf("receiver: the command %q is a lifecycle command the agent wires", name)
		return
	}
	ctx, cancel := context.WithCancel(r.stopCtx)
	go func() {
		defer cancel()
		if err := handler(ctx, params, &CommandContext{RID: file.RID, Src: file.Header.Src}); err != nil {
			r.st.emit(LogEvent{Message: fmt.Sprintf("receiver: command %q from %s failed: %v", name, file.Header.Src, err)})
		}
	}()
}

// runCustomCommand is D1 for a registered custom command: the handler runs on its own goroutine with
// a cancellable context; the loop archives as completed when it reports back (D3).
func (r *receiver) runCustomCommand(file *RequestFile, name string, params json.RawMessage) {
	handler := r.view.Load().cmds[name]
	if handler == nil {
		r.archiveCommand(file, r.commands[file.RID].raw, FinalCompleted, "") // unreachable: D0 accepted it
		return
	}
	ctx, cancel := context.WithCancel(r.stopCtx)
	go func() {
		defer cancel()
		err := handler(ctx, params, &CommandContext{RID: file.RID, Src: file.Header.Src})
		select {
		case r.commandDone <- commandDone{rid: file.RID, err: err}:
		case <-r.done:
		}
	}()
}

// onCommandDone archives a custom command as completed (D3) when its handler reports back; a failure
// is logged, never reported to the peer (commands are never answered).
func (r *receiver) onCommandDone(d commandDone) {
	job := r.commands[d.rid]
	if job == nil {
		return
	}
	delete(r.commands, d.rid)
	if d.err != nil {
		r.st.emit(LogEvent{Message: fmt.Sprintf("receiver: command %s from %s failed: %v", d.rid, job.file.Header.Src, d.err)})
	}
	r.archiveCommand(job.file, job.raw, FinalCompleted, "")
}

// archiveCommand is D3/D4: the inreq event with the command envelope alone, then delete it. Commands
// are never cancelled and never answered: only req is ever filled.
func (r *receiver) archiveCommand(file *RequestFile, raw json.RawMessage, finalStatus, reason string) {
	env := ExchangeEnvelopes{FinalStatus: finalStatus, Req: raw, Can: nil, Res: nil, Con: nil}
	_ = r.st.diaria.append(inreqEvent(r.st.id, r.st.now(), finalStatus, env))
	_ = r.st.files.removePath(filepath.Join(r.st.folders.inbox, string(file.RID)+".json"))
	r.archived[file.RID] = struct{}{} // D-5: the same rid is never executed twice in this run
	if finalStatus == FinalDropped {
		r.st.emit(LogEvent{Message: fmt.Sprintf("receiver: dropped command %s: %s", file.RID, reason)})
	}
	r.st.emit(InreqEvent{RID: file.RID, FinalStatus: finalStatus})
}

// parkDeferred parks one envelope until the request of its rid is tracked.
func (r *receiver) parkDeferred(rid RID, d deferredEnv) {
	r.deferred[rid] = append(r.deferred[rid], d)
}

// drainDeferred delivers the envelopes deferred for this rid — cancellation first, then response and
// confirmation in arrival order. It runs in dispatchRequest, before A0 starts.
func (r *receiver) drainDeferred(ex *exchange) {
	list := r.deferred[ex.rid]
	if len(list) == 0 {
		return
	}
	delete(r.deferred, ex.rid)
	var cans, rest []deferredEnv
	for _, d := range list {
		if d.kind == BodyCancellation {
			cans = append(cans, d)
		} else {
			rest = append(rest, d)
		}
	}
	for _, d := range append(cans, rest...) {
		name := string(ex.rid) + "." + d.kind + ".json"
		full := filepath.Join(r.st.folders.inbox, name)
		switch d.kind {
		case BodyCancellation:
			r.applyCancellation(ex, d.can, full, name, d.raw)
		case BodyResponse:
			r.applyResponse(ex, d.res, full, name, d.raw)
		case BodyConfirmation:
			r.applyConfirmation(ex, d.con, full, name, d.raw)
		}
	}
}

// dispatchResponse: ours (echoed by the watcher) or a leftover of a previous run (D-4).
func (r *receiver) dispatchResponse(full, name string, file *ResponseFile, raw json.RawMessage) {
	if ex := r.exchanges[file.RID]; ex != nil { // D-2: match exchanges by rid, never by file name
		r.applyResponse(ex, file, full, name, raw)
		return
	}
	if r.st.files.exists(filepath.Join(r.st.folders.inbox, string(file.RID)+".json")) {
		r.parkDeferred(file.RID, deferredEnv{kind: BodyResponse, res: file, raw: raw}) // the request is still pending
		return
	}
	r.retrackFromResponse(file, raw) // D-4: a bare .res.json left by a previous run
}

// applyResponse is the effect of one matched response, by phase; a D-11 header mismatch is ignored.
// raw is the file's bytes as exchanged, kept for the diarium (never re-marshal what we read).
func (r *receiver) applyResponse(ex *exchange, file *ResponseFile, full, name string, raw json.RawMessage) {
	if !sameHeader(&file.Header, &ex.header) {
		r.ignore(full, name, "header mismatch for rid "+string(ex.rid)+" (D-11)")
		return
	}
	if ex.resRaw == nil {
		ex.resRaw = raw
	}
	ex.res = file
	if ex.phase == phaseResponded {
		return // our own echo: respond() already armed the wait
	}
	ex.phase = phaseResponded
	if ex.con == nil {
		r.armConfirm(ex)
		return
	}
	r.archive(ex) // the confirmation already arrived (out of order)
}

// dispatchCancellation: matched by rid (D-2).
func (r *receiver) dispatchCancellation(full, name string, file *CancellationFile, raw json.RawMessage) {
	if ex := r.exchanges[file.RID]; ex != nil {
		r.applyCancellation(ex, file, full, name, raw)
		return
	}
	if r.st.files.exists(filepath.Join(r.st.folders.inbox, string(file.RID)+".json")) {
		r.parkDeferred(file.RID, deferredEnv{kind: BodyCancellation, can: file, raw: raw})
		return
	}
	r.ignore(full, name, "unmatched rid "+string(file.RID)+" (no tracked exchange)")
}

// applyCancellation is the effect of one matched cancellation, by phase; a D-11 header or name
// mismatch is ignored. raw is the file's bytes as exchanged, kept for the diarium.
func (r *receiver) applyCancellation(ex *exchange, file *CancellationFile, full, name string, raw json.RawMessage) {
	if !sameHeader(&file.Header, &ex.header) {
		r.ignore(full, name, "header mismatch for rid "+string(ex.rid)+" (D-11)")
		return
	}
	if ex.can == nil {
		if ex.request != nil && !cancellationNameOK(ex, file) {
			r.ignore(full, name, "cancellation name mismatch for rid "+string(ex.rid))
			return
		}
		ex.can, ex.canRaw = file, raw
	}
	ex.canSeen = true
	if ex.phase == phaseWorking && ex.cancel != nil {
		ex.cancel() // A3: stop the work
	}
}

// dispatchConfirmation: matched by rid (D-2).
func (r *receiver) dispatchConfirmation(full, name string, file *ConfirmationFile, raw json.RawMessage) {
	if ex := r.exchanges[file.RID]; ex != nil {
		r.applyConfirmation(ex, file, full, name, raw)
		return
	}
	if r.st.files.exists(filepath.Join(r.st.folders.inbox, string(file.RID)+".json")) {
		r.parkDeferred(file.RID, deferredEnv{kind: BodyConfirmation, con: file, raw: raw})
		return
	}
	r.ignore(full, name, "unmatched rid "+string(file.RID)+" (no tracked exchange)")
}

// applyConfirmation is the effect of one matched confirmation, by phase; a D-11 header mismatch is
// ignored. raw is the file's bytes as exchanged, kept for the diarium.
func (r *receiver) applyConfirmation(ex *exchange, file *ConfirmationFile, full, name string, raw json.RawMessage) {
	if !sameHeader(&file.Header, &ex.header) {
		r.ignore(full, name, "header mismatch for rid "+string(ex.rid)+" (D-11)")
		return
	}
	if ex.con != nil {
		return // the confirmation file is written once; later ones are no-ops
	}
	ex.con, ex.conRaw = file, raw
	if ex.phase == phaseResponded && ex.res != nil {
		if ex.timer != nil {
			ex.timer.Stop()
		}
		r.archive(ex)
	}
}

// acceptAndRun is A0 (the checks, first failure answered) → A1 → A2.
func (r *receiver) acceptAndRun(ex *exchange) {
	if _, ok := r.exchanges[ex.rid]; !ok {
		return // drainDeferred archived it: a leftover response+confirmation from a previous run
	}
	if ex.res != nil {
		return // drainDeferred applied a leftover response: the exchange is answered, only the
		// confirmation or the timeout can end it — no work, no second answer
	}
	if r.st.files.exists(filepath.Join(r.st.folders.inbox, string(ex.rid)+".res.json")) {
		return // D-4: a previous run already answered this request and left its .res.json in the
		// inbox; the work would run for nothing and its attachments would leak in the forum
	}
	var body RequestBody
	_ = json.Unmarshal(ex.request.Request, &body)
	if !ex.canSeen {
		if pending, raw := r.readCancellation(ex.rid, ex.header); pending != nil {
			ex.can, ex.canRaw, ex.canSeen = pending, raw, true
		}
	}
	if ex.canSeen {
		r.respondCancellation(ex) // a cancellation is already present: no work, a cancellation response (A0)
		return
	}
	entry, code, msg := r.accept(ex.header.Src, &body)
	if entry == nil {
		r.respondError(ex, code, msg)
		return
	}
	r.startWork(ex, entry, &body)
}

// accept is A0: the checks in table order; the first failure found is reported (104 is not made).
func (r *receiver) accept(src AgentID, body *RequestBody) (entry *capabilityEntry, code int, msg string) {
	spec := r.st.spec()
	if isTrue(spec.Disabled) {
		return nil, CodeAgentDisabled, "agent_disabled: the agent is disabled"
	}
	e := r.view.Load().caps[body.Name]
	if e == nil {
		return nil, CodeUnknownCapability, "unknown_capability: " + body.Name
	}
	if isTrue(e.capability.Actio.Disabled) {
		return nil, CodeCapabilityDisabled, "capability_disabled: " + body.Name
	}
	af := e.capability.Actio.AcceptsFrom
	if len(af) > 0 && !containsString(af, string(src)) {
		return nil, CodeRequestorNotAllowed, "requestor_not_allowed: " + body.Name + " does not accept " + string(src)
	}
	params := body.Parameters
	if len(params) == 0 || string(params) == "null" {
		params = []byte("{}")
	}
	if err := e.schemas.checkParams(params); err != nil {
		return nil, CodeInvalidParameters, "invalid_parameters: " + body.Name
	}
	if peer := r.peerSpec(src); peer != nil && !sharesNamespace(spec, peer) {
		return nil, CodeNamespaceMismatch, "namespace_mismatch: no shared namespace with " + string(src)
	}
	if e.maxParallel > 0 && int(e.running.Load()) >= e.maxParallel {
		return nil, CodeCapacityExceeded, "capacity_exceeded: " + body.Name
	}
	if body.Timeout != 0 && body.TS+int64(body.Timeout) < r.st.now().Unix() {
		return nil, CodeRequestExpired, "request_expired: the timeout passed before acceptance"
	}
	return e, 0, ""
}

// startWork is A1: run the capability handler on its own goroutine with a cancellable context (A3).
// The loop stays free; the result comes back over completions.
func (r *receiver) startWork(ex *exchange, entry *capabilityEntry, body *RequestBody) {
	ex.phase = phaseWorking
	ctx, cancel := context.WithCancel(r.stopCtx)
	ex.cancel = cancel
	entry.running.Add(1)
	ex.workStart = r.st.now()
	go func() {
		defer entry.running.Add(-1)
		defer cancel()
		var result any
		var err error
		var atts []string
		func() {
			defer func() {
				if p := recover(); p != nil { // a panic is answered as any other failure
					err = fmt.Errorf("actio: the handler for %q panicked: %v", body.Name, p)
				}
			}()
			call := &CallContext{RID: ex.rid, Src: ex.header.Src, Attachments: body.Attachments, PublishAttachment: func(name string, data []byte) (string, error) {
				if err := r.writeAttachment(name, data); err != nil {
					return "", err
				}
				atts = append(atts, name)
				return name, nil
			}}
			result, err = entry.handler(ctx, body.Parameters, call)
		}()
		var raw json.RawMessage
		if err == nil {
			raw, err = marshalJSON(result)
		}
		select {
		case r.completions <- completion{rid: ex.rid, result: raw, err: err, attachments: atts}:
		case <-r.done:
		}
	}()
}

// onCompletion is A2/A3 for a settled work: the loop (the state owner) writes the single response.
func (r *receiver) onCompletion(c completion) {
	ex := r.exchanges[c.rid]
	if ex == nil || ex.phase != phaseWorking {
		return
	}
	ex.responseAttachments = append(ex.responseAttachments, c.attachments...)
	if ex.canSeen {
		r.respondCancellation(ex) // A3: the work was cancelled
		return
	}
	if c.err == nil {
		r.respondSuccess(ex, c.result)
		return
	}
	r.respondHandlerError(ex, c.err)
}

// onConfirmTimeout is the A2 → A4 edge: no confirmation before the timeout, archive (completed, or
// cancelled if a cancellation was seen).
func (r *receiver) onConfirmTimeout(rid RID) {
	ex := r.exchanges[rid]
	if ex == nil || ex.phase != phaseResponded {
		return
	}
	r.archive(ex)
}

// respondCancellation writes the acknowledgement of a cancellation: error=false, cancellation=true,
// result=null, no attachments.
func (r *receiver) respondCancellation(ex *exchange) {
	r.writeResponse(ex, responseOpts{cancellation: true, attachments: []string{}})
}

// respondError writes an A0 rejection: an error result with the standardized code.
func (r *receiver) respondError(ex *exchange, code int, msg string) {
	raw, _ := marshalJSON(ErrorResult{Code: intp(code), Msg: msg})
	r.writeResponse(ex, responseOpts{error: true, result: raw, attachments: []string{}})
}

// respondHandlerError writes the response to a handler failure: an *Error carries its code, any other
// error its message, no code.
func (r *receiver) respondHandlerError(ex *exchange, err error) {
	var er ErrorResult
	if e, ok := err.(*Error); ok {
		er = ErrorResult{Code: intp(e.Code), Msg: e.Message}
		if e.Details != nil {
			d := fmt.Sprintf("%v", e.Details)
			er.Details = d
		}
	} else {
		er = ErrorResult{Msg: err.Error()}
	}
	raw, _ := marshalJSON(er)
	r.writeResponse(ex, responseOpts{error: true, result: raw, attachments: ex.responseAttachments, workTime: r.workSeconds(ex)})
}

// respondSuccess writes the response to a finished work.
func (r *receiver) respondSuccess(ex *exchange, result json.RawMessage) {
	r.writeResponse(ex, responseOpts{result: result, attachments: ex.responseAttachments, workTime: r.workSeconds(ex)})
}

// responseOpts is the single response of an exchange, written only once.
type responseOpts struct {
	cancellation bool
	error        bool
	result       json.RawMessage // null for a cancellation
	attachments  []string
	workTime     *int
}

// writeResponse is the A2 core: build the response, self-validate it, write it to the OWN
// inbox, and arm the confirmation wait (or archive if the confirmation already arrived).
func (r *receiver) writeResponse(ex *exchange, o responseOpts) {
	if ex.res != nil {
		return // the response is written once
	}
	now := r.st.now()
	if o.result == nil {
		o.result = json.RawMessage("null")
	}
	if o.attachments == nil {
		o.attachments = []string{}
	}
	body := ResponseBody{TS: now.Unix(), Nonce: newNonce(), Error: o.error, Cancellation: o.cancellation, Result: o.result, Attachments: o.attachments}
	if o.workTime != nil {
		wt := float64(*o.workTime)
		body.WorkTime = &wt
	}
	bodyRaw, err := marshalJSON(body)
	if err == nil {
		hdr := ex.header
		hdr.Datetime = formatISO(now)
		bodyRaw, err = marshalJSON(ResponseFile{Header: hdr, Response: bodyRaw})
	}
	var validated *ResponseFile
	if err == nil {
		validated, err = validateResponseFile(bodyRaw) // self-validate before the write
	}
	if err == nil {
		err = r.st.files.writeJSONAtomic(filepath.Join(r.st.folders.inbox, string(ex.rid)+".res.json"), validated, r.st.folders.agent)
	}
	if err != nil {
		r.internal(ex.rid, "write the response: "+oneLine(err))
		return
	}
	ex.res, ex.resRaw, ex.phase = validated, bodyRaw, phaseResponded
	if ex.con == nil {
		r.armConfirm(ex)
		return
	}
	r.archive(ex) // the confirmation already arrived (out of order)
}

// armConfirm arms the confirmation timeout (A2): a time.AfterFunc that only sends on a channel, with
// a select on the loop's end.
func (r *receiver) armConfirm(ex *exchange) {
	ex.timer = time.AfterFunc(r.st.config.ConfirmTimeout, func() {
		select {
		case r.timerFired <- ex.rid:
		case <-r.done:
		}
	})
}

// archive is A4: the inreq diarium event with the final status and the envelopes as exchanged, then
// delete all files of the rid and the response attachments. There is no stopped guard: performStop
// itself runs these archives, and after it no other entry point can start one.
func (r *receiver) archive(ex *exchange) {
	if _, ok := r.exchanges[ex.rid]; !ok {
		return
	}
	if _, done := r.archived[ex.rid]; done {
		return // D-5: the same rid is never archived twice in this run
	}
	r.archived[ex.rid] = struct{}{}
	if ex.timer != nil {
		ex.timer.Stop()
	}
	// A4 is two steps in this order (protocol, *Archive*): (1) the inreq line, then (2) the files.
	// A request whose answer was not written (the inbox was lost) gets no line: the protocol has no
	// `dropped` for a request. It stays unsettled, with one log line naming the rid.
	finalStatus := FinalCompleted
	if ex.res == nil {
		r.st.logf("the answer for %s was not written (the inbox was lost); the exchange is left unsettled", ex.rid)
	} else {
		if ex.canSeen {
			finalStatus = FinalCancelled
		} else if ex.con != nil {
			finalStatus = FinalConfirmed
		}
		env := ExchangeEnvelopes{FinalStatus: finalStatus, Req: ex.reqRaw, Can: ex.canRaw, Res: ex.resRaw, Con: ex.conRaw}
		_ = r.st.diaria.append(inreqEvent(r.st.id, r.st.now(), finalStatus, env))
	}
	// The files are cleaned and the exchange leaves the working set whether or not it is archived:
	// an unsettled exchange must not be leaked.
	for _, suffix := range []string{".json", ".can.json", ".res.json", ".con.json"} {
		_ = r.st.files.removePath(filepath.Join(r.st.folders.inbox, string(ex.rid)+suffix))
	}
	for _, name := range ex.responseAttachments {
		_ = r.st.files.removePath(filepath.Join(r.st.folders.forum, name))
	}
	delete(r.exchanges, ex.rid)
	if ex.res != nil {
		r.st.emit(InreqEvent{RID: ex.rid, FinalStatus: finalStatus})
	}
}

// retrackFromResponse is D-4: re-track a pending .res.json (a previous run) and restart the
// confirmation wait from now. The envelopes are kept as exchanged: a "null" in the diarium means the
// file was never written, so the bytes of every file that is present must land on the line.
func (r *receiver) retrackFromResponse(file *ResponseFile, raw json.RawMessage) {
	if r.stopped() {
		return
	}
	if _, done := r.archived[file.RID]; done {
		return
	}
	if _, tracked := r.exchanges[file.RID]; tracked {
		return
	}
	ex := &exchange{rid: file.RID, header: file.Header, phase: phaseResponded, res: file, resRaw: raw}
	if can, canRaw := r.readCancellation(file.RID, file.Header); can != nil {
		ex.can, ex.canRaw, ex.canSeen = can, canRaw, true
	}
	if con, conRaw := r.readConfirmation(file.RID, file.Header); con != nil {
		ex.con, ex.conRaw = con, conRaw
	}
	r.exchanges[file.RID] = ex
	if ex.con == nil {
		r.armConfirm(ex) // tracked, so performStop archives it (D-9)
		return
	}
	r.archive(ex) // confirmed before this run started
}

// readCancellation reads <inbox>/<rid>.can.json, validates it, and checks the header (D-11). It
// returns the file and its bytes as exchanged (the diarium keeps the bytes, never a re-marshal).
func (r *receiver) readCancellation(rid RID, hdr Header) (*CancellationFile, json.RawMessage) {
	raw, err := r.st.files.readFile(filepath.Join(r.st.folders.inbox, string(rid)+".can.json"))
	if err != nil {
		return nil, nil
	}
	file, err := validateCancellationFile(raw)
	if err != nil || !sameHeader(&file.Header, &hdr) {
		return nil, nil
	}
	if !cancellationNameOK(r.exchanges[rid], file) {
		return nil, nil
	}
	return file, raw
}

// readConfirmation reads <inbox>/<rid>.con.json, validates it, and checks the header (D-11). It
// returns the file and its bytes as exchanged (the diarium keeps the bytes, never a re-marshal).
func (r *receiver) readConfirmation(rid RID, hdr Header) (*ConfirmationFile, json.RawMessage) {
	raw, err := r.st.files.readFile(filepath.Join(r.st.folders.inbox, string(rid)+".con.json"))
	if err != nil {
		return nil, nil
	}
	file, err := validateConfirmationFile(raw)
	if err != nil || !sameHeader(&file.Header, &hdr) {
		return nil, nil
	}
	return file, raw
}

// cancellationNameOK reports whether a cancellation's name matches the exchange's request (the
// name-match half of D-11); with no tracked request there is nothing to mismatch.
func cancellationNameOK(ex *exchange, file *CancellationFile) bool {
	if ex == nil || ex.request == nil {
		return true
	}
	var req RequestBody
	var can CancellationBody
	_ = json.Unmarshal(ex.request.Request, &req)
	_ = json.Unmarshal(file.Cancellation, &can)
	return nameMatches(&req, &can)
}

// writeAttachment is CallContext.publishAttachment: write to forum (atomic, tmpDir = the agent
// folder). It runs on the work goroutine and touches no loop state.
func (r *receiver) writeAttachment(name string, data []byte) error {
	if err := r.st.files.makeDir(r.st.folders.forum); err != nil {
		return err
	}
	return r.st.files.writeBytesAtomic(filepath.Join(r.st.folders.forum, name), data, r.st.folders.agent)
}

// ignore is D-14/D-15: an ignored <path>: <reason>, once per (path, marker), + the ignored event and
// a log diarium line. The request of an ignored file is never tracked, so the envelopes deferred for
// its rid are dropped and logged once.
func (r *receiver) ignore(full, name, reason string) {
	if !r.st.files.exists(full) {
		return // vanished: nothing to ignore
	}
	key := full
	if m, ok := r.markers[name]; ok {
		key += ":" + m
	}
	if _, seen := r.ignoredSeen[key]; seen {
		return
	}
	r.ignoredSeen[key] = struct{}{}
	r.st.emit(IgnoredEvent{Path: full, Reason: reason})
	_ = r.st.diaria.append(logEvent(r.st.id, r.st.now(), "ignored "+name+": "+reason))
	if !strings.HasSuffix(name, ".can.json") && !strings.HasSuffix(name, ".res.json") && !strings.HasSuffix(name, ".con.json") {
		r.dropDeferredFor(RID(name[:len(name)-len(".json")]))
	}
}

// dropDeferredFor: the request of rid was ignored (D-15) and will never be tracked — drop the
// envelopes deferred for it and log each once.
func (r *receiver) dropDeferredFor(rid RID) {
	list := r.deferred[rid]
	if len(list) == 0 {
		return
	}
	delete(r.deferred, rid)
	for _, d := range list {
		name := string(rid) + "." + d.kind + ".json"
		full := filepath.Join(r.st.folders.inbox, name)
		reason := "the request of rid " + string(rid) + " was ignored (D-15)"
		r.st.emit(IgnoredEvent{Path: full, Reason: reason})
		_ = r.st.diaria.append(logEvent(r.st.id, r.st.now(), "deferred "+full+": "+reason))
	}
}

// internal reports a receiver bug (a build or write failure of our own envelope) as a log event.
func (r *receiver) internal(rid RID, msg string) {
	r.st.emit(LogEvent{Message: "receiver: internal error on " + string(rid) + ": " + msg})
}

// workSeconds is the whole seconds of actual work since the work started, for work_time. It uses the
// agent's clock, not the wall clock: both ends come from the same source.
func (r *receiver) workSeconds(ex *exchange) *int {
	s := int(r.st.now().Sub(ex.workStart).Seconds())
	if s < 0 {
		s = 0
	}
	return &s
}

func intp(v int) *int { return &v }

// stoppingResult is the work error 300 result of a stop that preempts an accepted, unanswered request.
func stoppingResult() json.RawMessage {
	raw, _ := marshalJSON(ErrorResult{Code: intp(CodeAgentStopping), Msg: "agent_stopping: the agent stops before answering"})
	return raw
}

// oneLine renders a validation error as the single-line reason D-15 records.
func oneLine(err error) string {
	s := err.Error()
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// nsSet is the namespaced view of a spec (*Callability rules* 2): absent/null/[] is the null
// namespace, "" here.
func nsSet(spec *Spec) map[string]struct{} {
	set := map[string]struct{}{}
	if len(spec.Namespaces) == 0 {
		set[""] = struct{}{}
		return set
	}
	for _, n := range spec.Namespaces {
		if n == nil {
			set[""] = struct{}{}
		} else {
			set[*n] = struct{}{}
		}
	}
	return set
}

// sharesNamespace reports whether two agents share at least one namespace ("*" shares with everyone).
func sharesNamespace(a, b *Spec) bool {
	na, nb := nsSet(a), nsSet(b)
	if _, ok := na["*"]; ok {
		return true
	}
	if _, ok := nb["*"]; ok {
		return true
	}
	for n := range na {
		if _, ok := nb[n]; ok {
			return true
		}
	}
	return false
}
