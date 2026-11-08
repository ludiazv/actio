package actio

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// putWatch writes one file into dir atomically: a dot-file temp in the parent, renamed into place,
// as the protocol's writes are.
func putWatch(t *testing.T, dir, name string) {
	t.Helper()
	if err := put(dir, name); err != nil {
		t.Fatal(err)
	}
}

func put(dir, name string) error {
	tmp := filepath.Join(filepath.Dir(dir), "."+filepath.Base(dir)+name+".tmp")
	if err := os.WriteFile(tmp, []byte("x"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, name))
}

// inotifyCount counts this process's inotify file descriptors, via the /proc/self/fd symlinks.
func inotifyCount(t *testing.T) int {
	t.Helper()
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("reading /proc/self/fd: %v", err)
	}
	n := 0
	for _, fd := range fds {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if err == nil && strings.HasSuffix(target, "inotify") {
			n++
		}
	}
	return n
}

// The deadlock: a finished request stops reading, more files arrive, a
// second request subscribes to the same inbox, and the first unsubscribes. Nothing may block, so
// all of it finishes (D-23).
func TestWatchDeadlockTwoRequests(t *testing.T) {
	w, err := newWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	inbox := filepath.Join(t.TempDir(), "inbox")
	if err := os.Mkdir(inbox, 0o755); err != nil {
		t.Fatal(err)
	}
	s1, err := w.subscribe(inbox)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		putWatch(t, inbox, fmt.Sprintf("f%d", i))
		time.Sleep(10 * time.Millisecond)
	}
	done := make(chan struct{})
	go func() {
		s2, err := w.subscribe(inbox)
		if err == nil {
			w.unsubscribe(s2)
		}
		w.unsubscribe(s1)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("DEADLOCK: a control path blocked on a delivery")
	}
}

// The deaf folder: a folder deleted (or renamed away) and recreated
// under the same name must report a loss — a reported lost() is the only pass, seeing a file in
// the new folder is not — and the same path must be watchable again (D-22).
func TestWatchFolderRecreatedIsNotDeaf(t *testing.T) {
	w, err := newWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	for _, how := range []string{"delete", "rename"} {
		for i := 0; i < 20; i++ {
			dir := filepath.Join(t.TempDir(), "inbox")
			if err := os.Mkdir(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			s, err := w.subscribe(dir)
			if err != nil {
				t.Fatal(err)
			}
			if how == "delete" {
				if err := os.RemoveAll(dir); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Rename(dir, dir+".old"); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Mkdir(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			select {
			case <-s.lost():
			case <-time.After(2 * time.Second):
				t.Fatalf("DEAF: %s, try %d", how, i)
			}
			s2, err := w.subscribe(dir)
			if err != nil {
				t.Fatalf("resubscribing the new folder: %s, try %d: %v", how, i, err)
			}
			putWatch(t, dir, "again")
			select {
			case <-s2.changed():
			case <-time.After(2 * time.Second):
				t.Fatalf("watching the new folder: %s, try %d", how, i)
			}
			w.unsubscribe(s2)
		}
	}
}

// The inbox owner: every file of a 500-file burst is seen exactly once,
// files already present are reported by the first look, and a replaced file (a new inode) is seen
// again (D-23).
func TestWatchOwnerSeesEveryFileOnce(t *testing.T) {
	w, err := newWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	inbox := filepath.Join(t.TempDir(), "inbox")
	if err := os.Mkdir(inbox, 0o755); err != nil {
		t.Fatal(err)
	}
	putWatch(t, inbox, "before")
	s, err := w.subscribe(inbox)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	marks := map[string]string{}
	look := func() {
		ch, next, err := scanFolder(inbox, marks)
		if err != nil {
			t.Fatal(err)
		}
		marks = next
		for _, n := range ch {
			seen[n]++
		}
	}
	look()
	const n = 500
	go func() {
		for i := 0; i < n; i++ {
			if err := put(inbox, fmt.Sprintf("f%03d", i)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	scans := 0
	deadline := time.After(10 * time.Second)
	for len(seen) < n+1 {
		select {
		case <-s.changed():
			scans++
			look()
		case <-deadline:
			t.Fatalf("saw %d of %d", len(seen), n+1)
		}
	}
	for name, c := range seen {
		if c != 1 {
			t.Fatalf("%s seen %d times", name, c)
		}
	}
	t.Logf("%d files in %d scans", n+1, scans)
	putWatch(t, inbox, "f000") // replaced: a new inode
	select {
	case <-s.changed():
		look()
	case <-time.After(2 * time.Second):
		t.Fatal("no wake for a replaced file")
	}
	if seen["f000"] != 2 {
		t.Fatalf("the replaced file was not reported: %d", seen["f000"])
	}
}

// 200 requests at once to one inbox, half leaving without reading, under -race: nothing left
// running, and one inotify instance for the whole watcher, from creation through 200 subscribers
// to close (D-23).
func TestWatchRequestsUnderLoad(t *testing.T) {
	goroutines := runtime.NumGoroutine()
	inotify := inotifyCount(t)
	w, err := newWatcher()
	if err != nil {
		t.Fatal(err)
	}
	if n := inotifyCount(t); n != inotify+1 {
		t.Fatalf("the watcher's inotify instances: %d, want %d", n, inotify+1)
	}
	inbox := filepath.Join(t.TempDir(), "inbox")
	if err := os.Mkdir(inbox, 0o755); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Go(func() {
			s, err := w.subscribe(inbox)
			if err != nil {
				t.Error(err)
				return
			}
			defer w.unsubscribe(s)
			name := fmt.Sprintf("r%d.res.json", i)
			go func() {
				if err := put(inbox, name); err != nil {
					t.Error(err)
				}
			}()
			if i%2 == 0 {
				return
			}
			deadline := time.After(20 * time.Second)
			for {
				if _, err := os.Stat(filepath.Join(inbox, name)); err == nil {
					return
				}
				select {
				case <-s.changed():
				case <-deadline:
					t.Errorf("request %d missed its response", i)
					return
				}
			}
		})
	}
	wg.Wait()
	if n := inotifyCount(t); n != inotify+1 {
		t.Fatalf("inotify instances with 200 subscribers: %d, want %d", n, inotify+1)
	}
	w.close()
	time.Sleep(100 * time.Millisecond)
	if n := inotifyCount(t); n != inotify {
		t.Fatalf("inotify instances after close: %d, want %d", n, inotify)
	}
	if n := runtime.NumGoroutine(); n > goroutines+1 {
		t.Errorf("goroutines left: %d, before %d", n, goroutines)
	}
}

// An overflow wakes every subscriber: a look again, never a failure (D-23). The handler is called
// directly, as the event loop would.
func TestWatchOverflowWakesEverySubscriber(t *testing.T) {
	w, err := newWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	dirs := []string{}
	for _, name := range []string{"a", "b"} {
		dir := filepath.Join(t.TempDir(), name)
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		dirs = append(dirs, dir)
	}
	subs := make([]*folderWatch, len(dirs))
	for i, dir := range dirs {
		s, err := w.subscribe(dir)
		if err != nil {
			t.Fatal(err)
		}
		subs[i] = s
	}
	w.handleError(fsnotify.ErrEventOverflow)
	for i, s := range subs {
		select {
		case <-s.changed():
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %d was not woken by the overflow", i)
		}
		select {
		case <-s.lost():
			t.Fatalf("subscriber %d got a loss from an overflow", i)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// The real kernel overflow, not a call to handleError: stall the watch path (holding the table's
// mutex stops run() at its next wake, and fsnotify stops reading once its send blocks), overflow
// the kernel's event queue, and resume. The overflow must reach the overflow path, wake every
// subscriber of every folder, change nothing about what a scan sees, and leave the folder watched
// (D-23: an overflow is "look again", never a lost folder).
func TestWatchRealOverflowWakesEverySubscriber(t *testing.T) {
	w, err := newWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	dir := filepath.Join(t.TempDir(), "inbox")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := w.subscribe(dir)
	if err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(t.TempDir(), "other")
	if err := os.Mkdir(other, 0o755); err != nil {
		t.Fatal(err)
	}
	s2, err := w.subscribe(other)
	if err != nil {
		t.Fatal(err)
	}

	// The kernel's queue limit; the write count is adapted to it. The default is the fallback for a
	// platform where the value is not readable, where there is no inotify queue to overflow anyway.
	limit := 16384
	if raw, rerr := os.ReadFile("/proc/sys/fs/inotify/max_queued_events"); rerr == nil {
		var n int
		if _, serr := fmt.Sscan(string(raw), &n); serr == nil && n > 0 {
			limit = n
		}
	}
	t.Logf("inotify queue limit: %d", limit)

	// Stall the watch path and overflow the queue in one piece: the deferred unlock also runs if a
	// Fatal fires in between, so the deferred watcher close can never block on the mutex.
	stallAndFlood := func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		flooded := make(chan struct{})
		go func() {
			putWatch(t, dir, "first")
			time.Sleep(50 * time.Millisecond)
			for i := 0; i < limit+1000; i++ { // an empty file makes one event, so go well past the limit
				if werr := os.WriteFile(filepath.Join(dir, fmt.Sprintf("n%06d", i)), nil, 0o644); werr != nil {
					t.Errorf("flood write %d: %v", i, werr)
				}
			}
			close(flooded)
		}()
		<-flooded
	}
	stallAndFlood()

	// The overflow reaches the watcher's overflow path: the folder's subscriber is woken, and it is
	// a wake-up, never a loss.
	select {
	case <-s.changed():
	case <-s.lost():
		t.Fatal("an overflow was reported as a loss")
	case <-time.After(20 * time.Second):
		t.Fatal("nothing after the overflow")
	}

	// Every subscriber is woken: the other folder has no event of its own, so only the overflow's
	// wake of everyone can reach it. The overflow event sits at the tail of the kernel's queue, so
	// this leg waits out the hand-off of every event queued before it (~16 000 of them, no file I/O).
	// Measured: 1.7 s of its own solo, and 1.8 s with eight whole-suite -race runs at GOMAXPROCS=1
	// going at the same time; one instance was seen past the old 10 s bound on a machine that was also
	// running black-box agents and builds, so the bound carries the machine's load, not one run's.
	select {
	case <-s2.changed():
	case <-s2.lost():
		t.Fatal("an overflow was reported as a loss, for the other folder")
	case <-time.After(30 * time.Second):
		t.Error("the overflow did not reach every subscriber")
	}

	// A scan after the overflow sees every file that was written.
	changed, _, err := scanFolder(dir, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != limit+1001 {
		t.Errorf("a scan after the overflow saw %d files of %d", len(changed), limit+1001)
	}

	// The folder is still watched: drain the buffered wake-ups, then a new write must wake again.
	for ok := true; ok; {
		select {
		case <-s.changed():
		case <-time.After(300 * time.Millisecond):
			ok = false
		}
	}
	putWatch(t, dir, "after-the-overflow")
	select {
	case <-s.changed():
	case <-s.lost():
		t.Fatal("the folder was lost by the overflow")
	case <-time.After(3 * time.Second):
		t.Error("DEAF after the overflow")
	}
}

// A trailing slash on subscribe: the path is cleaned, so an event's folder matches the table
// (D-23).
func TestWatchSubscribeTrailingSlash(t *testing.T) {
	w, err := newWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	inbox := filepath.Join(t.TempDir(), "inbox")
	if err := os.Mkdir(inbox, 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := w.subscribe(inbox + "/")
	if err != nil {
		t.Fatal(err)
	}
	putWatch(t, inbox, "x")
	select {
	case <-s.changed():
	case <-time.After(2 * time.Second):
		t.Fatal("no wake-up for a trailing-slash subscribe")
	}
}

// unsubscribe is safe to call twice, after a loss, and after close (D-23).
func TestWatchUnsubscribeTwiceAndAfterClose(t *testing.T) {
	w, err := newWatcher()
	if err != nil {
		t.Fatal(err)
	}
	inbox := filepath.Join(t.TempDir(), "inbox")
	if err := os.Mkdir(inbox, 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := w.subscribe(inbox)
	if err != nil {
		t.Fatal(err)
	}
	w.unsubscribe(s)
	w.unsubscribe(s)            // twice: a no-op
	s, err = w.subscribe(inbox) // the folder is armed again
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(inbox); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.lost():
	case <-time.After(2 * time.Second):
		t.Fatal("no loss for the deleted folder")
	}
	w.unsubscribe(s) // after a loss: a no-op
	w.close()
	w.unsubscribe(s) // after close: a no-op

	w2, err := newWatcher()
	if err != nil {
		t.Fatal(err)
	}
	inbox2 := filepath.Join(t.TempDir(), "inbox")
	if err := os.Mkdir(inbox2, 0o755); err != nil {
		t.Fatal(err)
	}
	s2, err := w2.subscribe(inbox2)
	if err != nil {
		t.Fatal(err)
	}
	w2.close()
	w2.unsubscribe(s2) // after close, with the folder still in the table
}

// subscribe after close is an error (D-23).
func TestWatchSubscribeAfterClose(t *testing.T) {
	w, err := newWatcher()
	if err != nil {
		t.Fatal(err)
	}
	inbox := filepath.Join(t.TempDir(), "inbox")
	if err := os.Mkdir(inbox, 0o755); err != nil {
		t.Fatal(err)
	}
	w.close()
	if _, err := w.subscribe(inbox); err == nil {
		t.Fatal("subscribe after close succeeded, want an error")
	}
}

// scanFolder reports new and changed names in lexical order, skips dot files and directories, and
// fails on a missing folder (D-23).
func TestScanFolderSkipsDotFilesAndDirectories(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".hidden"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	changed, next, err := scanFolder(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != 1 || changed[0] != "a" {
		t.Fatalf("changed %v, want [a]", changed)
	}
	if len(next) != 1 {
		t.Fatalf("markers %v, want one", next)
	}
	// A second look with the markers kept reports nothing.
	changed, _, err = scanFolder(dir, next)
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != 0 {
		t.Fatalf("second look changed %v, want none", changed)
	}
	// A missing folder is an error, and the very map of markers that was given comes back.
	probe := map[string]string{"p": "1"}
	if _, back, err := scanFolder(filepath.Join(dir, "nope"), probe); err == nil {
		t.Fatal("a missing folder scanned fine")
	} else {
		back["sentinel"] = "1"
		if probe["sentinel"] != "1" {
			t.Fatal("a failed scan did not hand back the markers it was given")
		}
	}
}

// The marker is mtime:size:inode, with the mtime in nanoseconds (D-23).
func TestMarkerIsMtimeSizeInode(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "m")
	if err := os.WriteFile(name, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.SplitN(markerOf(st), ":", 3)
	if len(parts) != 3 {
		t.Fatalf("marker %q: want three parts", markerOf(st))
	}
	if parts[0] != strconv.FormatInt(st.ModTime().UnixNano(), 10) {
		t.Fatalf("marker mtime %q, want %d", parts[0], st.ModTime().UnixNano())
	}
	if parts[1] != strconv.FormatInt(st.Size(), 10) {
		t.Fatalf("marker size %q, want %d", parts[1], st.Size())
	}
	if _, err := strconv.ParseUint(parts[2], 10, 64); err != nil {
		t.Fatalf("marker inode %q: %v", parts[2], err)
	}
}
