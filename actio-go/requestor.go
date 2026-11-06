// Lane B: the requestor side, one sequential exchange per call (B1–B5).
//
// A request writes its envelope and attachments, then waits in one select over the destination
// inbox's watcher, its timeout timer, the caller's ctx and the agent's stop. Cancellation is not a
// return: ctx ending starts B4, which writes the cancellation and then keeps waiting for the
// response up to CancelWaitTimeout, because the protocol requires the exchange to end with a
// confirmation or a timeout.
//
// Watching the destination uses the agent's one watcher (D-23): a request subscribes to the
// destination inbox before it writes (D-18), so a hundred concurrent requests still hold one
// inotify instance. Call and SendCommand, which run with no agent, create their own watcher.
package actio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"time"
)

// requestor is the lane-B machinery of one agent (or of one CLI call).
type requestor struct {
	st     *agentState
	stopCh chan struct{}

	// The mutex of the outbound-exchange count (D-8): the third of the four the concurrency
	// contract allows. It also guards the two dedup sets and the stopped flag; nothing else in
	// this file takes a lock.
	mu      sync.Mutex
	out     int
	stopped bool
	ignored map[string]struct{} // D-11: the paths already reported
	skipped map[string]struct{} // D-19: the peer schemas whose compile failure is already logged

	// Test-only seams: the production values are the frozen discovery functions.
	scanFn     func(root string) (ScanResult, error)
	callableFn func(self *Spec, peer DiscoveredAgent, now time.Time) *AgentView
}

// newRequestor builds it on the shared state: the folders, the file primitives, the agent's one
// watcher, the diarium and the clock.
func newRequestor(st *agentState) *requestor {
	return &requestor{
		st: st, stopCh: make(chan struct{}),
		ignored: map[string]struct{}{}, skipped: map[string]struct{}{},
		scanFn: Scan, callableFn: callable,
	}
}

// outbound is one exchange in flight, owned by the goroutine running it. The envelope bytes are
// kept as written or as read: the diarium stores them as exchanged, never re-marshalled.
type outbound struct {
	dst         AgentID
	rid         RID
	header      *Header
	name        string
	reqRaw      []byte
	canRaw      []byte // set once B4 has written the cancellation
	attachments []string
}

// request runs B1–B5 for one request and reports how the exchange ended. It returns a nil error for
// a peer's error answer: check Outcome.Response.Error. Only a local failure is an error: a refusal by
// the callability rules (*CallError), a bad result (*ResultError), or I/O.
func (r *requestor) request(ctx context.Context, dst AgentID, name string, params any, opts RequestOptions) (Outcome, error) {
	r.mu.Lock()
	stopped := r.stopped
	r.mu.Unlock()
	if stopped {
		return Outcome{}, errors.New("actio: the requestor is stopped")
	}
	// The callability rules, before anything is written or armed: a refusal leaves no trace.
	capObj, err := r.callablePeer(dst, name)
	if err != nil {
		return Outcome{}, err
	}
	inbox := r.inboxOf(dst)
	// D-18: subscribe before the write, so a response written any time from now on wakes us.
	sub, err := r.st.watch.subscribe(inbox)
	if err != nil {
		return Outcome{}, err // a missing destination inbox: the request fails locally
	}
	defer r.st.watch.unsubscribe(sub)

	ex, err := r.beginOutbound(dst, name, params, TypeRequest, opts.Timeout, opts.Attachments)
	if err != nil {
		return Outcome{}, err
	}
	// The outbound count (for busy, D-8) is kept under its own mutex, the third of the four.
	r.mu.Lock()
	r.out++
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.out--
		r.mu.Unlock()
	}()

	// D-6: the timeout timer; a zero timeout waits until ctx is done.
	var timedOut <-chan struct{}
	var timer *time.Timer
	if opts.Timeout > 0 {
		timedOut, timer = r.fireAfter(opts.Timeout)
		defer timer.Stop()
	}

	// B2: look, then select; a wake-up only means "look again".
	for {
		if resRaw, body, ok := r.capture(ex); ok {
			return r.finishAnswered(ex, capObj, resRaw, body)
		}
		select {
		case <-sub.changed():
		case err := <-sub.lost():
			return r.finishLost(inbox, err, ex), nil
		case <-timedOut:
			return r.cancelAndWait(sub, ex, capObj)
		case <-ctx.Done():
			return r.cancelAndWait(sub, ex, capObj)
		case <-r.stopCh:
			return r.abandon(ex), nil
		}
	}
}

// command writes one command envelope and returns its rid: a command is delivered, not executed, and
// is never answered.
func (r *requestor) command(ctx context.Context, dst AgentID, name string, params any) (RID, error) {
	r.mu.Lock()
	stopped := r.stopped
	r.mu.Unlock()
	if stopped {
		return "", errors.New("actio: the requestor is stopped")
	}
	view, err := r.callablePeerView(dst, name)
	if err != nil {
		return "", err
	}
	// A nil view is the no-spec form (the CLI vehicle): no rules, the command goes out.
	if view != nil && !view.Commands {
		return "", &CallError{Dst: dst, Name: name, Reason: "peer does not accept our commands"}
	}
	// B1 for a command: the envelope, then the diarium right after, then the live signal.
	ex, err := r.beginOutbound(dst, name, params, TypeCommand, 0, nil)
	if err != nil {
		return "", err
	}
	if err := r.st.diaria.append(outreqEvent(r.st.id, r.st.now(), FinalSent, ExchangeEnvelopes{Req: ex.reqRaw})); err != nil {
		return ex.rid, err
	}
	r.st.emit(OutreqEvent{RID: ex.rid, FinalStatus: FinalSent})
	return ex.rid, nil
}

// stop releases the watchers and timers of the exchanges still going: every in-flight exchange
// selects on stopCh and ends, and nothing is armed after it (D-9).
func (r *requestor) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.stopped {
		r.stopped = true
		close(r.stopCh)
	}
}

// outboundCount is the number of outbound exchanges waiting for a response (for busy, D-8).
func (r *requestor) outboundCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.out
}

// callablePeerView applies the callability rules for one destination. A nil view with a nil error
// is the CLI vehicle: no spec, no rules. A refusal is a *CallError, before any file is written.
func (r *requestor) callablePeerView(dst AgentID, name string) (*AgentView, error) {
	self := r.st.spec()
	if self == nil {
		return nil, nil
	}
	seen, err := r.scanFn(r.st.folders.root)
	if err != nil {
		return nil, err
	}
	refused := &CallError{Dst: dst, Name: name, Reason: "peer is not callable (callability rules)"}
	for _, a := range seen.Agents {
		if a.Spec.ID == dst {
			if view := r.callableFn(self, a, r.st.now()); view != nil {
				return view, nil
			}
		}
	}
	return nil, refused
}

// callablePeer is callablePeerView plus the capability check a request needs.
func (r *requestor) callablePeer(dst AgentID, name string) (*Capability, error) {
	view, err := r.callablePeerView(dst, name)
	if err != nil || view == nil {
		return nil, err
	}
	cap, ok := view.Capabilities[name]
	if !ok {
		return nil, &CallError{Dst: dst, Name: name, Reason: "capability is not usable (disabled or accepts_from)"}
	}
	return &cap, nil
}

// beginOutbound is B1: copy the attachments into forum, build the envelope, self-validate it (G-5)
// and write <rid>.json in the destination inbox.
func (r *requestor) beginOutbound(dst AgentID, name string, params any, typ string, timeout time.Duration, attachments []string) (*outbound, error) {
	st := r.st
	now := st.now()
	rid := newULID(now)
	published := []string{}
	for _, p := range attachments {
		base := filepath.Base(p)
		if err := st.files.copyFile(p, filepath.Join(st.folders.forum, base), st.folders.agent); err != nil {
			return nil, err
		}
		published = append(published, base)
	}
	paramsRaw, err := marshalJSON(params) // nil encodes to null, which the schema allows
	if err != nil {
		return nil, err
	}
	header := &Header{RID: rid, Src: st.id, Dst: dst, Datetime: formatISO(now), Type: typ, ActioVersion: ProtocolVersion}
	body := RequestBody{
		TS:          now.Unix(),
		Nonce:       newNonce(),
		Signature:   nil, // the core profile signs with null
		Name:        name,
		Parameters:  paramsRaw,
		Timeout:     timeoutSeconds(timeout),
		Attachments: published, // never nil: the schema requires the list
	}
	bodyRaw, err := marshalJSON(body)
	if err != nil {
		return nil, err
	}
	raw, err := marshalJSON(RequestFile{Header: *header, Request: bodyRaw})
	if err != nil {
		return nil, err
	}
	if _, err := validateRequestFile(raw); err != nil {
		return nil, err // G-5: what we are about to write must conform
	}
	if err := st.files.writeBytesAtomic(filepath.Join(r.inboxOf(dst), string(rid)+".json"), raw, st.folders.agent); err != nil {
		return nil, err
	}
	return &outbound{dst: dst, rid: rid, header: header, name: name, reqRaw: raw, attachments: published}, nil
}

// capture looks for this exchange's response in the destination inbox. It never trusts a name it
// was told about: it reads its own path, validates the bytes, and matches the header (D-11).
func (r *requestor) capture(ex *outbound) (resRaw []byte, body *ResponseBody, ok bool) {
	path := filepath.Join(r.inboxOf(ex.dst), string(ex.rid)+".res.json")
	raw, err := r.st.files.readFile(path)
	if err != nil {
		return nil, nil, false
	}
	file, err := validateResponseFile(raw)
	if err != nil {
		return nil, nil, false // a broken peer file: stay in B2 until the give-up paths
	}
	if !sameHeader(&file.Header, ex.header) {
		r.reportIgnored(path, raw, "header mismatch (D-11)")
		return nil, nil, false
	}
	body, err = validateResponseBody(file.Response)
	if err != nil {
		return nil, nil, false
	}
	return raw, body, true
}

// finishAnswered is B3 with the D-13 guard, then B5, then the result-schema check. The final status
// is cancelled when B4 ran, confirmed otherwise.
func (r *requestor) finishAnswered(ex *outbound, capObj *Capability, resRaw []byte, body *ResponseBody) (Outcome, error) {
	final := FinalConfirmed
	if ex.canRaw != nil {
		final = FinalCancelled
	}
	out, err := r.confirm(ex, resRaw, body, final)
	if err != nil {
		return out, err
	}
	// The result schema (D-19): only a non-error, non-cancellation answer is checked.
	if capObj != nil && body != nil && !body.Error && !body.Cancellation {
		if cs, cerr := compileCapability(capObj); cerr == nil {
			if serr := cs.checkResult(body.Result); serr != nil {
				return out, &ResultError{
					Message:  fmt.Sprintf("the result of %s/%s does not follow its schema: %v", ex.dst, ex.name, serr),
					Response: body,
				}
			}
		} else {
			r.schemaSkipped(ex, cerr)
		}
	}
	return out, nil
}

// confirm is B3 (+D-13) and B5: the diarium first (with the confirmation about to be written), then
// the live signal, then the confirmation file, then the attachment cleanup.
func (r *requestor) confirm(ex *outbound, resRaw []byte, body *ResponseBody, final string) (Outcome, error) {
	st := r.st
	out := Outcome{FinalStatus: final, Response: body}
	env := ExchangeEnvelopes{Req: ex.reqRaw, Can: ex.canRaw, Res: resRaw}
	if st.files.exists(filepath.Join(r.inboxOf(ex.dst), string(ex.rid)+".json")) { // the D-13 guard: confirm before the archive, not after
		conRaw, err := r.buildConfirmation(ex.header)
		if err != nil {
			return out, err
		}
		env.Con = conRaw
	}
	// The diarium is the durable record: append it first, then the live signal, then the file.
	if err := st.diaria.append(outreqEvent(st.id, st.now(), final, env)); err != nil {
		return out, err
	}
	st.emit(OutreqEvent{RID: ex.rid, FinalStatus: final})
	var werr error
	if env.Con != nil {
		werr = st.files.writeBytesAtomic(filepath.Join(r.inboxOf(ex.dst), string(ex.rid)+".con.json"), env.Con, st.folders.agent)
	}
	if derr := r.deleteAttachments(ex); werr == nil {
		werr = derr
	}
	return out, werr
}

// abandon ends the exchange without a response (D-6/D-22): the outreq line, the live signal, the
// attachments. A failed diarium append is logged, as in the reference: the exchange has ended.
func (r *requestor) abandon(ex *outbound) Outcome {
	if err := r.st.diaria.append(outreqEvent(r.st.id, r.st.now(), FinalAbandoned, ExchangeEnvelopes{Req: ex.reqRaw, Can: ex.canRaw})); err != nil {
		r.st.logf("abandon of %s failed: %v", ex.rid, err)
	}
	r.st.emit(OutreqEvent{RID: ex.rid, FinalStatus: FinalAbandoned})
	// A failed B5 deletion is logged, not returned: the exchange has ended, and the reference
	// reports it the same way (the agent's log path, as the receiver's housekeeping does).
	if err := r.deleteAttachments(ex); err != nil {
		r.st.logf("abandon of %s: the attachment deletion failed: %v", ex.rid, err)
	}
	return Outcome{FinalStatus: FinalAbandoned}
}

// finishLost is the D-22 loss of the destination inbox mid-exchange: the folder we can no longer
// watch is reported as ignored, and the exchange is abandoned.
func (r *requestor) finishLost(inbox string, err error, ex *outbound) Outcome {
	r.st.emit(IgnoredEvent{Path: inbox, Reason: "watch lost: " + err.Error()})
	return r.abandon(ex)
}

// cancelAndWait is B4: write the cancellation, then keep waiting for the response up to
// CancelWaitTimeout before abandoning (D-6). The cancellation is not a return.
func (r *requestor) cancelAndWait(sub *folderWatch, ex *outbound, capObj *Capability) (Outcome, error) {
	st := r.st
	canRaw, err := r.buildCancellation(ex.header, ex.name)
	if err != nil {
		return Outcome{}, err
	}
	if err := st.files.writeBytesAtomic(filepath.Join(r.inboxOf(ex.dst), string(ex.rid)+".can.json"), canRaw, st.folders.agent); err != nil {
		return Outcome{}, err
	}
	ex.canRaw = canRaw
	waited, timer := r.fireAfter(st.config.CancelWaitTimeout)
	defer timer.Stop()
	for {
		if resRaw, body, ok := r.capture(ex); ok {
			return r.finishAnswered(ex, capObj, resRaw, body)
		}
		select {
		case <-sub.changed():
		case err := <-sub.lost():
			return r.finishLost(r.inboxOf(ex.dst), err, ex), nil
		case <-waited:
			return r.abandon(ex), nil // the bounded wait for the cancellation response is up
		case <-r.stopCh:
			return r.abandon(ex), nil
		}
	}
}

// buildCancellation writes the bytes of <rid>.can.json: the same header with a fresh datetime, and
// a name that matches the request.
func (r *requestor) buildCancellation(header *Header, name string) ([]byte, error) {
	now := r.st.now()
	bodyRaw, err := marshalJSON(CancellationBody{TS: now.Unix(), Nonce: newNonce(), Signature: nil, Name: name, Force: false})
	if err != nil {
		return nil, err
	}
	h := *header
	h.Datetime = formatISO(now)
	raw, err := marshalJSON(CancellationFile{Header: h, Cancellation: bodyRaw})
	if err != nil {
		return nil, err
	}
	if _, err := validateCancellationFile(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// buildConfirmation writes the bytes of <rid>.con.json.
func (r *requestor) buildConfirmation(header *Header) ([]byte, error) {
	now := r.st.now()
	bodyRaw, err := marshalJSON(ConfirmationBody{TS: now.Unix(), Nonce: newNonce(), Signature: nil})
	if err != nil {
		return nil, err
	}
	h := *header
	h.Datetime = formatISO(now)
	raw, err := marshalJSON(ConfirmationFile{Header: h, Confirmation: bodyRaw})
	if err != nil {
		return nil, err
	}
	if _, err := validateConfirmationFile(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// reportIgnored is the D-11 report: one log per file state, in the diarium and to the event
// callbacks. The reference dedups per (path, mtime); the Go file primitives expose no mtime
// outside the five files (§9), so the key is (path, content): a rewrite that changes a byte is
// reported again, and a byte-identical re-send is not.
func (r *requestor) reportIgnored(path string, raw []byte, reason string) {
	sum := sha256.Sum256(raw)
	if !r.once(r.ignored, path+":"+hex.EncodeToString(sum[:])) {
		return
	}
	message := "ignored " + path + ": " + reason
	r.st.logf("%s", message)
	if err := r.st.diaria.append(logEvent(r.st.id, r.st.now(), message)); err != nil {
		r.st.logf("diarium append failed: %v", err)
	}
}

// schemaSkipped is the D-19 report for a peer capability whose schema does not compile: the result
// check is skipped, logged once.
func (r *requestor) schemaSkipped(ex *outbound, err error) {
	if !r.once(r.skipped, string(ex.dst)+"/"+ex.name) {
		return
	}
	r.st.logf("result check skipped for %s/%s: %v", ex.dst, ex.name, err)
}

// once records key in set and reports whether it was new; the set is guarded by the count's mutex.
func (r *requestor) once(set map[string]struct{}, key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, seen := set[key]; seen {
		return false
	}
	set[key] = struct{}{}
	return true
}

// fireAfter arms a one-shot timer whose callback only sends on a buffered channel; the caller
// stops the timer, and a lost wake-up is harmless because the exchange re-looks on its next pass.
func (r *requestor) fireAfter(d time.Duration) (<-chan struct{}, *time.Timer) {
	fired := make(chan struct{}, 1)
	timer := time.AfterFunc(d, func() { fired <- struct{}{} })
	return fired, timer
}

func (r *requestor) deleteAttachments(ex *outbound) error {
	for _, name := range ex.attachments {
		if err := r.st.files.removePath(filepath.Join(r.st.folders.forum, name)); err != nil {
			return err
		}
	}
	ex.attachments = nil
	return nil
}

func (r *requestor) inboxOf(dst AgentID) string {
	return filepath.Join(r.st.folders.root, string(dst), FolderInbox)
}

// timeoutSeconds is the envelope's timeout in whole seconds, rounded up: the receiver's 108 check
// reads the envelope, not our timer.
func timeoutSeconds(d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return math.Ceil(d.Seconds())
}

// vehicle is the no-agent form of the requestor: one sync provider from cfg.Sync, one watcher,
// one agent folder for the atomic writes. Call and SendCommand end it on every exit path.
func vehicle(cfg Config, src AgentID) (*requestor, func(), error) {
	cfg, err := LoadConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	provider, err := newSyncProvider(cfg.Sync, func(error) {})
	if err != nil {
		return nil, nil, err
	}
	files := newFileIO(provider)
	agentDir := filepath.Join(cfg.Root, string(src))
	folders := agentFolders{
		root: cfg.Root, agent: agentDir, inbox: filepath.Join(agentDir, FolderInbox),
		forum: filepath.Join(cfg.Root, FolderForum), nuntium: filepath.Join(cfg.Root, FolderNuntium),
	}
	if err := files.makeDir(folders.agent); err != nil {
		provider.Close()
		return nil, nil, err
	}
	if err := files.makeDir(folders.inbox); err != nil { // the inbox exists for a self-call
		provider.Close()
		return nil, nil, err
	}
	watch, err := newWatcher()
	if err != nil {
		provider.Close()
		return nil, nil, err
	}
	st := &agentState{
		id: src, config: cfg, folders: folders, files: files, sync: provider,
		diaria: newDiaria(files, agentDir, src), watch: watch, now: cfg.Clock,
		emit: func(Event) {}, logf: func(string, ...any) {},
		spec:  func() *Spec { return nil }, // no spec: no callability rules
		state: func() StatusState { return StatusIdle },
	}
	return newRequestor(st), func() { watch.close(); provider.Close() }, nil
}

// Call is lane B without an agent: one exchange written from src's folder, with no spec, no status
// and no announce. The CLI's call subcommand is its user; it must pass a timeout above zero, since a
// process this short-lived cannot hold a request open forever.
func Call(ctx context.Context, cfg Config, src, dst AgentID, name string, params any, opts RequestOptions) (Outcome, error) {
	if opts.Timeout <= 0 {
		return Outcome{}, errors.New("actio: Call requires a timeout above zero")
	}
	r, end, err := vehicle(cfg, src)
	if err != nil {
		return Outcome{}, err
	}
	defer r.stop()
	defer end()
	return r.request(ctx, dst, name, params, opts)
}

// SendCommand is the command form of Call.
func SendCommand(ctx context.Context, cfg Config, src, dst AgentID, name string, params any) (RID, error) {
	r, end, err := vehicle(cfg, src)
	if err != nil {
		return "", err
	}
	defer r.stop()
	defer end()
	return r.command(ctx, dst, name, params)
}
