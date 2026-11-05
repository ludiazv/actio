package actio

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// waitForName waits until name is delivered on files, with a two-second deadline. Names delivered
// in the meantime (the initial batch) are consumed and ignored.
func waitForName(t *testing.T, files <-chan string, name string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case got, ok := <-files:
			if !ok {
				t.Fatalf("files channel closed before %q arrived", name)
			}
			if got == name {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %q on files", name)
		}
	}
}

// expectQuiet asserts that files stays quiet for ~200 ms: no name, and no closure, in that window.
func expectQuiet(t *testing.T, files <-chan string) {
	t.Helper()
	select {
	case got, ok := <-files:
		t.Fatalf("expected files to stay quiet, got %q (open=%v)", got, ok)
	case <-time.After(200 * time.Millisecond):
	}
}

// mustWrite writes one file into dir and fails the test on error.
func mustWrite(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// TestWatchExistingFiles: the files already in the folder are each reported once, after watchDir
// returns (the first scan reports what was there, D-23).
func TestWatchExistingFiles(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, dir, "a", "alpha")
	mustWrite(t, dir, "b", "bravo")

	w, err := watchDir(dir)
	if err != nil {
		t.Fatalf("watchDir: %v", err)
	}
	defer w.close()

	waitForName(t, w.files(), "a")
	waitForName(t, w.files(), "b")
	expectQuiet(t, w.files()) // each exactly once
}

// TestWatchCreatedFile: a file created after the watch is reported by name. The event's Op and
// name are never trusted: the scan decides (D-23).
func TestWatchCreatedFile(t *testing.T) {
	dir := t.TempDir()
	w, err := watchDir(dir)
	if err != nil {
		t.Fatalf("watchDir: %v", err)
	}
	defer w.close()

	mustWrite(t, dir, "new.txt", "hello")
	waitForName(t, w.files(), "new.txt")
	expectQuiet(t, w.files())
}

// TestWatchAtomicWrite: an atomic write (a .x.tmp renamed to x) is reported once, as x; the
// dot-file temp name never reaches the caller (D-23).
func TestWatchAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	w, err := watchDir(dir)
	if err != nil {
		t.Fatalf("watchDir: %v", err)
	}
	defer w.close()

	tmp := filepath.Join(dir, ".x.tmp")
	if err := os.WriteFile(tmp, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, "x")); err != nil {
		t.Fatal(err)
	}

	counts := map[string]int{}
	gotX := false
	deadline := time.After(2 * time.Second)
	for !gotX {
		select {
		case n, ok := <-w.files():
			if !ok {
				t.Fatal("files channel closed before x arrived")
			}
			if strings.HasPrefix(n, ".") {
				t.Fatalf("dot file %q reached the caller", n)
			}
			counts[n]++
			if n == "x" {
				gotX = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for x, got %v", counts)
		}
	}
	if counts["x"] != 1 {
		t.Fatalf("x reported %d times, want 1", counts["x"])
	}
	expectQuiet(t, w.files())
}

// TestWatchRenameReplace: a file replaced by a rename over an existing name (same name, new inode,
// same size) is reported. A rename-in and a replace both arrive as a create; the marker's inode
// component is what catches the replace (plan §5.4).
func TestWatchRenameReplace(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, dir, "x", "abcd")

	w, err := watchDir(dir)
	if err != nil {
		t.Fatalf("watchDir: %v", err)
	}
	defer w.close()

	waitForName(t, w.files(), "x") // the first scan reports what is there
	expectQuiet(t, w.files())

	staging := t.TempDir()
	mustWrite(t, staging, "y", "wxyz") // same size, a new inode
	if err := os.Rename(filepath.Join(staging, "y"), filepath.Join(dir, "x")); err != nil {
		t.Fatal(err)
	}

	waitForName(t, w.files(), "x") // reported again: the inode changed
	expectQuiet(t, w.files())
}

// TestWatchSameMarkerUnreported: a rewrite with the identical size, the identical millisecond mtime
// and the same inode is not reported. That is the accepted marker limitation (a same-millisecond,
// same-size in-place rewrite can be missed), shared with the TS marker (plan §5.4).
func TestWatchSameMarkerUnreported(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x")
	mustWrite(t, dir, "x", "abcd")

	w, err := watchDir(dir)
	if err != nil {
		t.Fatalf("watchDir: %v", err)
	}
	defer w.close()

	waitForName(t, w.files(), "x")
	expectQuiet(t, w.files())

	orig, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("wxyz"), 0o644); err != nil { // same size, same inode
		t.Fatal(err)
	}
	// Force the same millisecond mtime the first scan recorded, so only the content differs.
	if err := os.Chtimes(p, orig.ModTime(), orig.ModTime()); err != nil {
		t.Fatal(err)
	}

	expectQuiet(t, w.files()) // not reported: the marker is unchanged (the accepted limitation)
}

// TestWatchBurst: a burst of 30 files costs at most two scans (the scanning/dirty pair, D-23). The
// 30 files are created before the watch, so the whole burst lands in the first scan; the number of
// scans is read from the counter the watcher exposes for the tests.
func TestWatchBurst(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 30; i++ {
		mustWrite(t, dir, fmt.Sprintf("f%02d", i), "x")
	}

	w, err := watchDir(dir)
	if err != nil {
		t.Fatalf("watchDir: %v", err)
	}
	defer w.close()

	counts := map[string]int{}
	deadline := time.After(2 * time.Second)
	for len(counts) < 30 {
		select {
		case n, ok := <-w.files():
			if !ok {
				t.Fatalf("files closed after %d names", len(counts))
			}
			counts[n]++
		case <-deadline:
			t.Fatalf("timed out with %d names", len(counts))
		}
	}
	expectQuiet(t, w.files())
	if n := w.scans.Load(); n > 2 {
		t.Fatalf("the burst cost %d scans, want at most 2", n)
	}
}

// TestWatchFolderGone: a folder that disappears is death: one error on lost, then both channels
// are closed and nothing more is ever sent; a new folder with the same name produces nothing — no
// re-arm (D-22).
func TestWatchFolderGone(t *testing.T) {
	dir := t.TempDir()
	w, err := watchDir(dir)
	if err != nil {
		t.Fatalf("watchDir: %v", err)
	}
	defer w.close()

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}

	select {
	case err, ok := <-w.lost():
		if !ok {
			t.Fatal("lost closed without an error")
		}
		if err == nil {
			t.Fatal("nil loss error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the loss")
	}

	if _, ok := <-w.files(); ok {
		t.Fatal("files not closed after the loss")
	}
	if _, ok := <-w.lost(); ok {
		t.Fatal("a second loss was reported")
	}

	// No re-arm: a new folder with the same name produces nothing.
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, dir, "late", "too late")
	if _, ok := <-w.files(); ok {
		t.Fatal("an event after the loss: the watcher re-armed")
	}
}

// TestWatchCloseTwice: close() is idempotent and safe from any goroutine; after it the watcher's
// goroutine is gone, both channels are closed, and nothing is ever sent again.
func TestWatchCloseTwice(t *testing.T) {
	dir := t.TempDir()
	w, err := watchDir(dir)
	if err != nil {
		t.Fatalf("watchDir: %v", err)
	}

	w.close()
	w.close() // safe

	if _, ok := <-w.files(); ok {
		t.Fatal("files not closed after close")
	}
	if _, ok := <-w.lost(); ok {
		t.Fatal("lost not closed after close")
	}
}

// TestWatchHubSharing: two subscriptions on one folder share one watcher (one inotify instance,
// D-18): one write is seen once by each. The last unsubscribe closes the folder's watcher, and a
// later subscribe builds a fresh, working one.
func TestWatchHubSharing(t *testing.T) {
	dir := t.TempDir()
	h := newWatchHub()
	defer h.close()

	sub1, err := h.subscribe(dir)
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := h.subscribe(dir)
	if err != nil {
		t.Fatal(err)
	}
	if sub1.watcher != sub2.watcher {
		t.Fatal("the two subscriptions do not share one watcher")
	}

	mustWrite(t, dir, "hello", "world")
	waitForName(t, sub1.files, "hello")
	waitForName(t, sub2.files, "hello")

	h.unsubscribe(sub1) // one ref left: the watcher is still alive
	mustWrite(t, dir, "again", "x")
	waitForName(t, sub2.files, "again")

	h.unsubscribe(sub2) // the last one: the folder's watcher closes
	if _, ok := <-sub2.files; ok {
		t.Fatal("the shared watcher's channels are not closed after the last unsubscribe")
	}
	if _, ok := <-sub2.lost; ok {
		t.Fatal("lost not closed after the last unsubscribe")
	}

	sub3, err := h.subscribe(dir)
	if err != nil {
		t.Fatal(err)
	}
	if sub3.watcher == sub2.watcher {
		t.Fatal("the third subscribe reused the closed watcher")
	}
	mustWrite(t, dir, "third", "x")
	waitForName(t, sub3.files, "third")
	h.unsubscribe(sub3)
}

// TestWatchHubMissingInbox: a subscribe on a missing folder returns an error and does not create
// the folder: the request fails locally (D-18).
func TestWatchHubMissingInbox(t *testing.T) {
	h := newWatchHub()
	defer h.close()

	missing := filepath.Join(t.TempDir(), "no-such-inbox")
	if _, err := h.subscribe(missing); err == nil {
		t.Fatal("subscribe on a missing folder must fail")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("subscribe created the folder")
	}
}

// TestWatchHubClose: hub.close() closes every folder's channels, and the hub is usable for a new
// subscribe afterwards (D-18).
func TestWatchHubClose(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	h := newWatchHub()

	s1, err := h.subscribe(dir1)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := h.subscribe(dir2)
	if err != nil {
		t.Fatal(err)
	}

	h.close()
	if _, ok := <-s1.files; ok {
		t.Fatal("dir1's channels are not closed by hub.close")
	}
	if _, ok := <-s2.files; ok {
		t.Fatal("dir2's channels are not closed by hub.close")
	}

	s3, err := h.subscribe(dir1)
	if err != nil {
		t.Fatalf("the hub is not usable after close: %v", err)
	}
	mustWrite(t, dir1, "after", "x")
	waitForName(t, s3.files, "after")
	h.unsubscribe(s3)
}
