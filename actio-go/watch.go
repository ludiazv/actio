package actio

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"
)

// watcher is a whole agent's watching: one fsnotify.Watcher (one inotify instance) and one
// goroutine, however many folders it watches (D-23). An event only means "look at this folder
// again"; the watcher does no file I/O, so it can't block on a consumer.
type watcher struct {
	fsw     *fsnotify.Watcher
	gone    chan struct{}
	mu      sync.Mutex
	folders map[string]map[*folderWatch]struct{}
}

type folderWatch struct {
	dir    string
	wakeCh chan struct{} // capacity 1: "look again"
	lostCh chan error    // capacity 1: once, then silent (D-22)
}

// newWatcher creates the agent's watcher and starts its one goroutine (created at Start, closed by Stop).
func newWatcher() (*watcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	w := &watcher{fsw: fsw, gone: make(chan struct{}), folders: map[string]map[*folderWatch]struct{}{}}
	go w.run()
	return w, nil
}

// close stops the watcher and waits for its goroutine to end.
func (w *watcher) close() {
	w.mu.Lock()
	w.folders = map[string]map[*folderWatch]struct{}{}
	w.mu.Unlock()
	w.fsw.Close()
	<-w.gone
}

// subscribe arms dir if nobody watches it yet and returns the subscription. The path is cleaned so
// an event's folder matches the table (D-23); a subscriber looks first, so nothing is missed (D-18).
func (w *watcher) subscribe(dir string) (*folderWatch, error) {
	dir = filepath.Clean(dir)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.folders[dir] == nil {
		if err := w.fsw.Add(dir); err != nil {
			return nil, err
		}
		w.folders[dir] = map[*folderWatch]struct{}{}
	}
	s := &folderWatch{dir: dir, wakeCh: make(chan struct{}, 1), lostCh: make(chan error, 1)}
	w.folders[dir][s] = struct{}{}
	return s, nil
}

// unsubscribe removes the subscription. It is safe to call twice, after a loss, and after close.
func (w *watcher) unsubscribe(s *folderWatch) {
	w.mu.Lock()
	defer w.mu.Unlock()
	subs := w.folders[s.dir]
	if subs == nil {
		return
	}
	delete(subs, s)
	if len(subs) == 0 {
		delete(w.folders, s.dir)
		_ = w.fsw.Remove(s.dir)
	}
}

// run is the watcher's one goroutine. An event only means "look at this folder again" (D-23), so it
// wakes the folder's subscribers, never acting on the event's Op or name. The one exception (D-22):
// a REMOVE or RENAME naming a watched folder is a loss, reported at once, because a folder
// recreated under the same name would otherwise read fine with nobody watching it.
func (w *watcher) run() {
	defer close(w.gone)
	for {
		select {
		case ev, ok := <-w.fsw.Events:
			if !ok {
				return
			}
			if ev.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
				w.mu.Lock()
				known := w.folders[ev.Name] != nil
				w.mu.Unlock()
				if known {
					w.lose(ev.Name, fmt.Errorf("actio: the watched folder %s was removed or renamed", ev.Name))
					continue
				}
			}
			w.wake(filepath.Dir(ev.Name))
		case err, ok := <-w.fsw.Errors:
			if !ok {
				return
			}
			w.handleError(err)
		}
	}
}

// handleError is what an fsnotify error means (D-23): an overflow is a "look again" for every
// subscriber, never a failure; anything else is a loss for every folder.
func (w *watcher) handleError(err error) {
	if errors.Is(err, fsnotify.ErrEventOverflow) {
		w.wake("")
		return
	}
	w.lose("", err)
}

// wake sends the "look again" wake-up to the subscribers of dir, or to everyone when dir is "" (D-23).
func (w *watcher) wake(dir string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for d, subs := range w.folders {
		if dir != "" && d != dir {
			continue
		}
		for s := range subs {
			select {
			case s.wakeCh <- struct{}{}:
			default:
			}
		}
	}
}

// lose reports the loss of dir to each subscriber and removes the folder from the table, or every
// folder when dir is "" (D-22); it happens once, because the folder leaves the table at the same time.
func (w *watcher) lose(dir string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for d, subs := range w.folders {
		if dir != "" && d != dir {
			continue
		}
		for s := range subs {
			s.lostCh <- err
		}
		delete(w.folders, d)
	}
}

func (s *folderWatch) changed() <-chan struct{} { return s.wakeCh } // capacity 1, "look again" (D-23)
func (s *folderWatch) lost() <-chan error       { return s.lostCh } // capacity 1, once, then silent (D-22)

// scanFolder is the Go form of TS's diffMarkers (D-23): the names that are new or changed, in
// lexical order, and the markers to keep. It has no lock: its caller is the folder's owner, on its
// own goroutine, after a wake-up. It skips dot files and non-regular files.
func scanFolder(dir string, prev map[string]string) ([]string, map[string]string, error) {
	dirents, err := os.ReadDir(dir)
	if err != nil {
		return nil, prev, err
	}
	next := make(map[string]string, len(dirents))
	var changed []string
	for _, d := range dirents {
		st, err := os.Stat(filepath.Join(dir, d.Name()))
		if d.Name()[0] == '.' || err != nil || !st.Mode().IsRegular() {
			continue
		}
		m := markerOf(st)
		next[d.Name()] = m
		if prev[d.Name()] != m {
			changed = append(changed, d.Name())
		}
	}
	return changed, next, nil
}
