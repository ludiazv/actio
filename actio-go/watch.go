// The folder watcher: one fsnotify watcher and one goroutine per watched folder, non-recursive
// (*Announcement and Discovery*, D-23).
//
// An fsnotify event means exactly one thing: "scan this folder again". Its Op is never acted on and
// its name is never trusted — a file renamed in and a file replaced by a rename both arrive as a
// create. What decides what changed is one scan (ReadDir plus stat) compared with the previous one
// by the per-file marker "mtime:size:inode". The watcher is armed before the first scan, so nothing
// written in between is lost; files already present are reported once, as changed.
//
// A folder that disappears, or a watcher that dies, is reported once on the lost channel and never
// re-armed: there is no retry, no polling fallback (D-22). The owner decides what a loss means — the
// agent fails, a request abandons.
package actio

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/fsnotify/fsnotify"
)

// dirWatcher is the watcher of one folder.
type dirWatcher struct {
	dir    string
	fsw    *fsnotify.Watcher
	direct bool // the watcher's own files/lost channels are a delivery target (watchDir) or not (hub)

	// The watcher's own subscription channels (buffer 1 each). A delivery target when direct;
	// closed on death or close() either way.
	filesCh chan string
	lostCh  chan error

	done chan struct{} // closed once, by death or by close(); the run goroutine exits on it
	once sync.Once     // guards the close of done
	gone chan struct{} // closed by the run goroutine on exit; close() waits on it
	subs chan subReq   // subscription add/remove requests, processed by the run goroutine

	// dead, marked and targets are owned by the run goroutine: it is the only goroutine that
	// reads or writes them, so no lock is needed around the scan state.
	dead    bool
	marked  map[string]string
	targets map[target]struct{}

	// scans counts the scan passes; the tests read it to check D-23's burst bound.
	scans atomic.Int64
}

// target is one delivery destination: a subscriber's files channel and the lost channel that
// receives the one death error. release is closed when the subscription leaves, so a delivery that
// is parked on it can move on.
type target struct {
	files   chan string
	lost    chan error
	release chan struct{}
}

// subReq is one subscription change for the run goroutine to apply.
type subReq struct {
	t   target
	add bool
}

// entry is one watched file and its change marker.
type entry struct {
	name   string
	marker string
}

// newDirWatcher arms the watcher on dir and starts its goroutine. A missing dir is an error, not a
// watcher that reports nothing. direct selects whether the watcher's own files/lost channels
// receive the scans (watchDir) or only the hub's subscriptions do.
func newDirWatcher(dir string, direct bool) (*dirWatcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	// Arm before scanning: a write between the arm and the first scan is not lost (plan §5.4).
	if err := fsw.Add(dir); err != nil {
		_ = fsw.Close()
		return nil, err
	}
	w := &dirWatcher{
		dir:     dir,
		fsw:     fsw,
		direct:  direct,
		filesCh: make(chan string, 1),
		lostCh:  make(chan error, 1),
		done:    make(chan struct{}),
		gone:    make(chan struct{}),
		subs:    make(chan subReq),
		marked:  make(map[string]string),
		targets: make(map[target]struct{}),
	}
	if direct {
		w.targets[target{files: w.filesCh, lost: w.lostCh, release: make(chan struct{})}] = struct{}{}
	}
	go w.run()
	return w, nil
}

// watchDir arms the watcher on dir and runs the first scan. A missing dir is an error, not a watcher
// that reports nothing.
func watchDir(dir string) (*dirWatcher, error) {
	return newDirWatcher(dir, true)
}

// files carries each name that appeared or changed, in lexical order, after every scan. The first
// scan reports what was already there.
func (w *dirWatcher) files() <-chan string { return w.filesCh }

// lost carries one error when the folder or the watcher died, and the channels stop after it.
func (w *dirWatcher) lost() <-chan error { return w.lostCh }

// close stops the goroutine and the fsnotify watcher; idempotent, and it never sends again.
func (w *dirWatcher) close() {
	w.once.Do(func() { close(w.done) })
	<-w.gone
}

// run is the watcher's single goroutine. It owns the dead/marked/targets state, so nothing around
// it needs a lock. It consumes events and discards them: every one of them only means "scan again",
// and one scan covers every event that is pending when it runs — a burst of N writes costs at most
// two scans (D-23).
func (w *dirWatcher) run() {
	defer close(w.gone)
	defer func() {
		if !w.direct {
			// The hub's watchers deliver only to subscriptions: the own channels close here.
			close(w.filesCh)
			close(w.lostCh)
		}
	}()
	defer w.closeTargets()
	defer func() { _ = w.fsw.Close() }()

	w.scan() // the first scan: what was already there is reported once, as changed

	for {
		select {
		case <-w.done:
			return
		case r := <-w.subs:
			if r.add {
				w.targets[r.t] = struct{}{}
			} else {
				delete(w.targets, r.t)
			}
		case _, ok := <-w.fsw.Events:
			if !ok {
				return
			}
			// The event's Op and name are ignored: drain what is pending and scan once.
			w.drainEvents()
			w.scan()
		case err, ok := <-w.fsw.Errors:
			if !ok {
				return
			}
			// In fsnotify v1.10 the Errors channel carries plain errors: an overflow arrives
			// as ErrEventOverflow itself. An overflow means rescan, not failure; any other
			// fsnotify error is death (D-22).
			if errors.Is(err, fsnotify.ErrEventOverflow) {
				w.scan()
				continue
			}
			w.die(err)
			return
		}
	}
}

// drainEvents discards every event already in the queue: each one means "scan this folder again",
// and one scan covers them all.
func (w *dirWatcher) drainEvents() {
	for {
		select {
		case _, ok := <-w.fsw.Events:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

// scan runs one ReadDir+stat pass and delivers the changed names. Events that arrive while it runs
// sit in the event queue: the run loop hands them back as exactly one more scan — the Go form of
// the TS scanning/dirty pair.
func (w *dirWatcher) scan() {
	if w.dead {
		return
	}
	entries, err := w.readEntries()
	if err != nil {
		w.die(err) // a ReadDir that fails is death: the folder is gone (D-22)
		return
	}
	w.report(entries)
}

// readEntries is one non-recursive pass over the folder: regular files only. Dot files never reach
// a caller — an atomic write names its temp file .<base>.<nonce>.tmp in the agent's own folder
// (D-23). A file that vanishes between the ReadDir and the Stat is skipped silently; the next scan
// tells the truth.
func (w *dirWatcher) readEntries() ([]entry, error) {
	w.scans.Add(1)
	dirents, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	entries := make([]entry, 0, len(dirents))
	for _, d := range dirents {
		name := d.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		st, err := os.Stat(filepath.Join(w.dir, name))
		if err != nil {
			continue
		}
		if !st.Mode().IsRegular() {
			continue
		}
		sys, ok := st.Sys().(*syscall.Stat_t)
		if !ok {
			continue
		}
		// The marker "mtime:size:inode", with mtime at millisecond resolution: a rewrite in
		// place with the same size in the same millisecond can be missed — the documented
		// limitation of the TS marker too (plan §5.4).
		entries = append(entries, entry{
			name:   name,
			marker: fmt.Sprintf("%d:%d:%d", st.ModTime().UnixMilli(), st.Size(), sys.Ino),
		})
	}
	return entries, nil
}

// report is the Go form of the TS diffMarkers: names whose marker is new or different are delivered
// in lexical order (ReadDir is name-sorted), and the markers of the reported names are kept — a
// name that disappeared loses its marker. Delivery never blocks a consumer: a name goes to each
// subscriber's buffered channel, and a subscriber that is not ready slows only the loop's walk of
// the batch, never the consumer — the loop resumes as soon as the receiver reads, and a
// subscription that leaves (release closed) is skipped without waiting.
func (w *dirWatcher) report(entries []entry) {
	next := make(map[string]string, len(entries))
	for _, e := range entries {
		next[e.name] = e.marker
	}
	changed := make([]entry, 0, len(entries))
	for _, e := range entries {
		if prev, seen := w.marked[e.name]; !seen || prev != e.marker {
			changed = append(changed, e)
		}
	}
	for _, e := range changed {
		for t := range w.targets {
			select {
			case t.files <- e.name:
			case <-w.done:
				return
			case <-t.release:
				// the subscription left while the loop was walking the batch: skip it
			}
		}
	}
	w.marked = next
}

// die is the death path (D-22): report once on every subscriber's lost channel, stop the loop,
// close the channels (in run's defers), and never send again. There is no retry, no re-arm, no
// polling fallback — the owner decides what a loss means.
func (w *dirWatcher) die(err error) {
	if w.dead {
		return
	}
	w.dead = true
	for t := range w.targets {
		t.lost <- err // each buffered 1 and sent at most once: this send never blocks
	}
	w.once.Do(func() { close(w.done) })
}

// closeTargets closes every delivery channel exactly once, on death or on close: after it, nothing
// is ever sent.
func (w *dirWatcher) closeTargets() {
	for t := range w.targets {
		close(t.lost)
		close(t.files)
	}
}

// watchHub holds the watchers of destination inboxes: one per inbox, shared by the requests that are
// waiting on it and released when the last one leaves (D-18). A watcher holds an inotify instance,
// which systems limit per user, so requests never hold one of their own.
type watchHub struct {
	// The mutex of the destination-watcher table: one of the four the concurrency contract
	// allows (D-18).
	mu    sync.Mutex
	table map[string]*hubEntry
}

// hubEntry is one folder's watcher and its subscriber count.
type hubEntry struct {
	watcher *dirWatcher
	refs    int
}

// inboxWatch is one request's subscription to an inbox watcher.
type inboxWatch struct {
	hub     *watchHub
	inbox   string
	watcher *dirWatcher

	// This subscription's delivery channels (buffer 1 each). Closed with the watcher, on death
	// or on the last unsubscribe.
	files    chan string
	lost     chan error
	release  chan struct{}
	released bool
}

// newWatchHub creates an empty hub.
func newWatchHub() *watchHub {
	return &watchHub{table: make(map[string]*hubEntry)}
}

// newSub builds one subscription's channels; it does not register them.
func (h *watchHub) newSub(inbox string, w *dirWatcher) *inboxWatch {
	return &inboxWatch{
		hub:     h,
		inbox:   inbox,
		watcher: w,
		files:   make(chan string, 1),
		lost:    make(chan error, 1),
		release: make(chan struct{}),
	}
}

// target is the subscription as a delivery destination of its watcher.
func (s *inboxWatch) target() target {
	return target{files: s.files, lost: s.lost, release: s.release}
}

// subscribe returns the subscription of one request to inbox, creating and reference-counting the
// watcher when it is the first subscriber. A missing inbox is an error: the request fails locally.
func (h *watchHub) subscribe(inbox string) (*inboxWatch, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for {
		if e, seen := h.table[inbox]; seen {
			e.refs++
			sub := h.newSub(inbox, e.watcher)
			select {
			case e.watcher.subs <- subReq{t: sub.target(), add: true}:
				return sub, nil
			case <-e.watcher.done:
				// The watcher died or closed in the meantime: roll back the reference and
				// fall through to a fresh watcher for the same folder.
				e.refs--
				if e.refs == 0 {
					delete(h.table, inbox)
				}
				continue
			}
		}

		w, err := newDirWatcher(inbox, false)
		if err != nil {
			return nil, err
		}
		sub := h.newSub(inbox, w)
		h.table[inbox] = &hubEntry{watcher: w, refs: 1}
		select {
		case w.subs <- subReq{t: sub.target(), add: true}:
			return sub, nil
		case <-w.done:
			// The folder vanished between creation and registration: retry, and
			// newDirWatcher fails with a missing-dir error if it is still gone.
			delete(h.table, inbox)
		}
	}
}

// unsubscribe releases one subscription; at zero the watcher closes and its channels close.
func (h *watchHub) unsubscribe(sub *inboxWatch) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if sub.released {
		return
	}
	sub.released = true
	e, seen := h.table[sub.inbox]
	if !seen || e.watcher != sub.watcher {
		return
	}
	e.refs--
	if e.refs > 0 {
		// Close release first: a delivery parked on this subscription moves on, and the run
		// goroutine can process the removal.
		close(sub.release)
		select {
		case e.watcher.subs <- subReq{t: sub.target(), add: false}:
		case <-e.watcher.done:
		}
		return
	}
	delete(h.table, sub.inbox)
	e.watcher.close()
}

// close releases every watcher in the hub.
func (h *watchHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for inbox, e := range h.table {
		delete(h.table, inbox)
		e.watcher.close()
	}
}
