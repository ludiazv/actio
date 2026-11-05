// Sync providers: what the storage owes a write before a peer on another machine can see it
// (*Sync and race conditions*). Four forms: "none", "delay[:<ms>]", "fsync+delay[:<ms>]" and
// "mutagen:<session>", taken from Config.Sync (which is ACTIO_SYNC). The provider belongs to the
// agent, and the file primitives call its hooks.
package actio

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// syncProvider is the hook pair the file primitives call: before the rename and after it. It stays
// unexported: §4 does not list it, nothing outside the package builds one, and TS removed its
// equivalent from the package entry.
type syncProvider interface {
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
func newSyncProvider(value string, onError func(error)) (syncProvider, error) {
	if value == "none" {
		return &nopProvider{}, nil
	}
	if m := delayFormRe.FindStringSubmatch(value); m != nil {
		d, err := parseSyncDelay(value, m[2])
		if err != nil {
			return nil, err
		}
		return &nopProvider{fsync: m[1] == "fsync+delay", delay: d}, nil
	}
	if m := mutagenFormRe.FindStringSubmatch(value); m != nil {
		return newMutagenProvider(m[1], onError), nil
	}
	return nil, fmt.Errorf("actio: unknown sync provider %q (expected none, delay[:<ms>], fsync+delay[:<ms>] or mutagen:<session>)", value)
}

const (
	minSyncDelayMS     = 1
	maxSyncDelayMS     = 10000
	defaultSyncDelayMS = 50
	mutagenDebounce    = 200 * time.Millisecond
)

var (
	delayFormRe   = regexp.MustCompile(`^(delay|fsync\+delay)(?::(\d+))?$`)
	mutagenFormRe = regexp.MustCompile(`^mutagen:(.+)$`)
	bareIntRe     = regexp.MustCompile(`^\d+$`)
)

// parseSyncDelay reads the <ms> of a delay form: a bare integer in 1–10 000; omitted means 50.
func parseSyncDelay(value, raw string) (time.Duration, error) {
	if raw == "" {
		return defaultSyncDelayMS * time.Millisecond, nil
	}
	if !bareIntRe.MatchString(raw) {
		return 0, syncDelayError(value)
	}
	ms, _ := strconv.Atoi(raw)
	if ms < minSyncDelayMS || ms > maxSyncDelayMS {
		return 0, syncDelayError(value)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func syncDelayError(value string) error {
	return fmt.Errorf("actio: invalid sync provider %q: <ms> must be a bare integer between %d and %d", value, minSyncDelayMS, maxSyncDelayMS)
}

// nopProvider is the none/delay/fsync+delay form: no state, and Flush and Close are no-ops.
type nopProvider struct {
	fsync bool
	delay time.Duration
}

func (p *nopProvider) Fsync() bool                 { return p.fsync }
func (p *nopProvider) Delay() time.Duration        { return p.delay }
func (p *nopProvider) Notify()                     {}
func (p *nopProvider) Flush(context.Context) error { return nil }
func (p *nopProvider) Close() error                { return nil }

// mutagenRun is one `mutagen sync flush <session>` execution, or the slot promised to it as a
// follow-up. done closes when the run is over — for a promised follow-up, when the run that covers
// it is over.
type mutagenRun struct {
	after *mutagenRun // the in-flight run a follow-up waits on; nil for a direct run

	done chan struct{}

	mu   sync.Mutex
	proc *os.Process
}

// mutagenProvider is the debounced `mutagen sync flush <session>` provider: Notify collapses the
// notifications inside the window into one run 200 ms later; Flush runs one immediately and waits
// for it; Close cancels a pending debounced run (a promised follow-up still runs).
//
// A flush request is never absorbed by a run already in flight: while a run is going, at most one
// follow-up run is promised (pending), and a Flush that arrives mid-run resolves only when a run
// that started after the call has finished — so the writes made during a flush are covered by the
// next run.
type mutagenProvider struct {
	session string
	onError func(error)

	mu      sync.Mutex
	closed  bool // Close won: a debounced run whose timer fires from then on must not start
	timer   *time.Timer
	running *mutagenRun
	pending *mutagenRun // the single promised follow-up, never two
}

func newMutagenProvider(session string, onError func(error)) *mutagenProvider {
	return &mutagenProvider{session: session, onError: onError}
}

// The mutagen provider never fsyncs and never delays: mutagen itself is the sync.
func (p *mutagenProvider) Fsync() bool          { return false }
func (p *mutagenProvider) Delay() time.Duration { return 0 }

// Notify schedules one run 200 ms later; notifications inside the window collapse into it.
func (p *mutagenProvider) Notify() {
	p.mu.Lock()
	if p.timer != nil {
		p.mu.Unlock()
		return
	}
	p.timer = time.AfterFunc(mutagenDebounce, p.onDebounce)
	p.mu.Unlock()
}

// onDebounce is the timer's callback. It tests closed under the mutex before it runs: in the
// window between the timer firing and this clearing p.timer, Close() can land (Stop then returns
// false), and the run must not start after a Close.
func (p *mutagenProvider) onDebounce() {
	p.mu.Lock()
	p.timer = nil
	shouldRun := !p.closed
	p.mu.Unlock()
	if shouldRun {
		p.run() // mid-run this becomes the coalesced follow-up instead of being swallowed
	}
}

// Flush forces a sync now: it cancels the pending debounced run (the forced run covers whatever it
// would have flushed) and waits for the run it is entitled to. A cancelled ctx stops the wait, kills
// the process, and returns the context error.
func (p *mutagenProvider) Flush(ctx context.Context) error {
	p.mu.Lock()
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
	p.mu.Unlock()
	r := p.run()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		r.kill()
		return ctx.Err()
	}
}

// Close cancels the pending debounced run; a promised follow-up still runs, because a caller of
// Flush is already waiting on it. A debounced run whose timer has already fired cannot start:
// onDebounce sees closed. Idempotent.
func (p *mutagenProvider) Close() error {
	p.mu.Lock()
	p.closed = true
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
	p.mu.Unlock()
	return nil
}

// run is the run the caller is entitled to: it starts now when idle; mid-run it is the single
// promised follow-up, which starts only after the in-flight run settles and resolves when that
// follow-up ends.
func (p *mutagenProvider) run() *mutagenRun {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running == nil {
		return p.spawn()
	}
	if p.pending == nil {
		follow := &mutagenRun{after: p.running, done: make(chan struct{})}
		p.pending = follow
		go p.startFollowUp(follow)
	}
	return p.pending
}

// spawn starts one run now. The caller holds p.mu.
func (p *mutagenProvider) spawn() *mutagenRun {
	r := &mutagenRun{done: make(chan struct{})}
	p.running = r
	go p.runProcess(r)
	return r
}

// startFollowUp starts the promised follow-up once the run it waits on settles.
func (p *mutagenProvider) startFollowUp(follow *mutagenRun) {
	<-follow.after.done
	p.mu.Lock()
	if p.pending == follow {
		p.pending = nil
	}
	if p.running == nil {
		p.running = follow
		p.mu.Unlock()
		go p.runProcess(follow)
		return
	}
	// A run started in the gap between the after-run settling and this lock: it already covers
	// the follow-up, so the follow-up resolves when that run ends.
	adopt := p.running
	p.mu.Unlock()
	<-adopt.done
	close(follow.done)
}

// runProcess executes one `mutagen sync flush <session>`. The binary path comes from
// ACTIO_MUTAGEN_BIN, read at run time (default "mutagen"). A failure (spawn error or non-zero exit)
// calls onError once per failed run and never returns an error to the protocol.
func (p *mutagenProvider) runProcess(r *mutagenRun) {
	defer p.settle(r)
	bin := os.Getenv("ACTIO_MUTAGEN_BIN")
	if bin == "" {
		bin = "mutagen"
	}
	cmd := exec.Command(bin, "sync", "flush", p.session)
	if err := cmd.Start(); err != nil {
		p.failRun(fmt.Errorf("actio: mutagen %q: %w", bin, err))
		return
	}
	r.mu.Lock()
	r.proc = cmd.Process
	r.mu.Unlock()
	if err := cmd.Wait(); err != nil {
		p.failRun(fmt.Errorf("actio: mutagen %q: %w", bin, err))
		return
	}
}

// settle marks r over and frees the in-flight slot if r holds it.
func (p *mutagenProvider) settle(r *mutagenRun) {
	p.mu.Lock()
	if p.running == r {
		p.running = nil
	}
	p.mu.Unlock()
	close(r.done)
}

// failRun reports a failed run to onError; an onError that itself panics must not break the write.
func (p *mutagenProvider) failRun(err error) {
	if p.onError == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	p.onError(err)
}

// kill stops the run's process if it is going; a run that already ended is left alone.
func (r *mutagenRun) kill() {
	r.mu.Lock()
	proc := r.proc
	r.mu.Unlock()
	if proc != nil {
		_ = proc.Kill()
	}
}
