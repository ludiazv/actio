// Tests for the file primitives. The fake SyncProvider records the order of its hook calls; it is
// deliberately not the implementation of sync.go (that belongs to a separate task).
package actio

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// hookRecorder is a SyncProvider fake that records the order of its hook calls. probe, when set,
// is consulted at every Fsync() call so a test can see whether the target is in place at that
// moment (false before the rename, true at the directory sync).
type hookRecorder struct {
	mu    sync.Mutex
	fsync bool
	delay time.Duration
	calls []string
	probe func() bool
	vis   []bool
}

func (h *hookRecorder) Fsync() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "fsync-ask")
	if h.probe != nil {
		h.vis = append(h.vis, h.probe())
	}
	return h.fsync
}

func (h *hookRecorder) Delay() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "delay-ask")
	return h.delay
}

func (h *hookRecorder) Notify() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "notify")
}

func (h *hookRecorder) Flush(context.Context) error { return nil }
func (h *hookRecorder) Close() error                { return nil }

func (h *hookRecorder) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.calls...)
}

// TestWriteJSONAtomicHookOrder pins the order of the post-write hooks: the provider's file sync
// happens before the rename (the target is not there yet), the directory sync after it (the target
// is), then Notify, then the delay before the write returns.
func TestWriteJSONAtomicHookOrder(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "out", "status.json")
	tmpDir := filepath.Join(dir, "agent")
	// The parent of the target (the inbox) already exists: the write renames into it, it does not
	// create it.
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	h := &hookRecorder{fsync: true, delay: 20 * time.Millisecond, probe: func() bool {
		_, err := os.Stat(target)
		return err == nil
	}}
	f := newFileIO(h)

	start := time.Now()
	if err := f.writeJSONAtomic(target, map[string]string{"a": "b"}, tmpDir); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)

	want := []string{"fsync-ask", "fsync-ask", "notify", "delay-ask"}
	if got := h.snapshot(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("hook order = %v, want %v", got, want)
	}
	if len(h.vis) != 2 || h.vis[0] || !h.vis[1] {
		t.Fatalf("target visibility at the Fsync calls = %v, want [false true] (file sync before the rename, directory sync after)", h.vis)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("the write returned in %v, before the provider's delay", elapsed)
	}
	b, err := f.readFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"a":"b"}` {
		t.Fatalf("target content = %q, want %q", b, `{"a":"b"}`)
	}
}

// TestWriteBytesAtomicTempNameAndCleanup checks the temp file's name form while the write is in
// flight, and that no temp file survives a successful write.
func TestWriteBytesAtomicTempNameAndCleanup(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "payload.bin")
	tmpDir := filepath.Join(dir, "agent")
	f := newFileIO(&hookRecorder{})
	payload := make([]byte, 4<<20) // big enough that the temp is observable while the write runs
	tmpRe := regexp.MustCompile(`^\.payload\.bin\.[0-9a-f]{32}\.tmp$`)

	caught := ""
	inflight := make(chan struct{})
outer:
	for attempt := 0; attempt < 20; attempt++ {
		done := make(chan struct{})
		inflight = done
		go func() {
			if err := f.writeBytesAtomic(target, payload, tmpDir); err != nil {
				t.Error(err)
			}
			close(done)
		}()
		for {
			names, err := f.listDir(tmpDir) // the temp dir may not exist yet: keep polling
			if err == nil {
				for _, n := range names {
					if tmpRe.MatchString(n) {
						caught = n
						break outer
					}
				}
			}
			select {
			case <-done:
				// this attempt finished before the temp was seen; try again
			case <-time.After(time.Millisecond):
			}
		}
	}
	if caught == "" {
		t.Fatal("the temp file was never observed while the write was in flight")
	}
	<-inflight // let the in-flight write finish before the temp dir goes away

	// A successful write leaves no temp file behind.
	names, err := f.listDir(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		t.Fatalf("temp files survive a successful write: %v", names)
	}
	b, err := f.readFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, payload) {
		t.Fatalf("target has %d bytes, want %d", len(b), len(payload))
	}
}

// TestRenameFailureLeavesNoTempFile makes the target path unusable (it is a directory), so the
// rename must fail; the temp file must not survive.
func TestRenameFailureLeavesNoTempFile(t *testing.T) {
	dir := t.TempDir()
	tmpDir := filepath.Join(dir, "agent")
	target := filepath.Join(dir, "blocked")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	f := newFileIO(&hookRecorder{})

	if err := f.writeBytesAtomic(target, []byte("x"), tmpDir); err == nil {
		t.Fatal("a write over a directory succeeded, want an error")
	}
	names, err := f.listDir(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		t.Fatalf("temp files survive a failed write: %v", names)
	}
	if !f.exists(target) {
		t.Fatal("the failed write removed the target directory")
	}
}

// TestMarshalJSON checks that a peer's <, > and & come back unchanged and that nothing is appended.
func TestMarshalJSON(t *testing.T) {
	raw, err := marshalJSON(map[string]string{"msg": "<peer> & \"friends\" > ok"})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"msg":"<peer> & \"friends\" > ok"}`
	if string(raw) != want {
		t.Fatalf("marshalJSON = %s, want %s", raw, want)
	}
	if strings.HasSuffix(string(raw), "\n") {
		t.Fatal("marshalJSON appends a trailing newline")
	}

	// A nil slice marshals as null: this documents the trap, it is not a bug.
	raw, err = marshalJSON(struct {
		S []int `json:"s"`
	}{})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"s":null}` {
		t.Fatalf("a nil slice marshals as %s, want null", raw)
	}
}

// TestListDirOrderIsByteLexical: listDir's order is load-bearing (peers list the same folder and
// must agree on the same names in the same order), so the rule is pinned exactly, not as
// "sorted". The names below are created in an order that is neither their byte order nor their
// natural order: a dot-file, an uppercase name, a name with a digit, a prefix of another, and a
// non-ASCII byte. The dot-file stays in the result on purpose: the layer above decides (watch.go,
// next door, skips dot files in its scan — the two files disagree deliberately).
func TestListDirOrderIsByteLexical(t *testing.T) {
	dir := t.TempDir()
	// Created in a scrambled order: creation order is not the byte order.
	created := []string{"alpha", "écho", "a1", ".hidden", "abc", "ab", "Beta"}
	for _, n := range created {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	f := newFileIO(&hookRecorder{})
	got, err := f.listDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Byte-lexical: '.' (0x2E) < 'B' (0x42) < 'a' (0x61); among the 'a' names the second byte
	// decides ('1' 0x31 < 'b' 0x62 < 'l' 0x6C); a prefix sorts before its extension; the UTF-8
	// bytes of é (0xC3 0xA9) sort after every ASCII byte.
	want := []string{".hidden", "Beta", "a1", "ab", "abc", "alpha", "écho"}
	if len(got) != len(want) {
		t.Fatalf("listDir = %v (len %d), want %v (len %d) — dot files are included on purpose", got, len(got), want, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("listDir = %v, want the exact byte-lexical order %v", got, want)
		}
	}
}

// countingWriter records every Write it is given, whole: the oracle for the one-Write rule.
type countingWriter struct {
	mu     sync.Mutex
	writes [][]byte
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, append([]byte(nil), p...))
	return len(p), nil
}

// TestWriteJSONLineSingleWrite: the line and its newline go out as exactly one Write of line +
// "\n" — a half line must never be visible, and a line must never be split across two writes.
func TestWriteJSONLineSingleWrite(t *testing.T) {
	w := &countingWriter{}
	line := []byte(`{"a":1}`)
	if _, err := writeJSONLine(w, line); err != nil {
		t.Fatal(err)
	}
	if len(w.writes) != 1 {
		t.Fatalf("writeJSONLine made %d Writes, want exactly 1: %q", len(w.writes), w.writes)
	}
	if string(w.writes[0]) != string(line)+"\n" {
		t.Fatalf("the single Write carried %q, want %q (the line and its newline, together)", w.writes[0], string(line)+"\n")
	}
	// An empty line still writes its newline, in one Write.
	w2 := &countingWriter{}
	if _, err := writeJSONLine(w2, nil); err != nil {
		t.Fatal(err)
	}
	if len(w2.writes) != 1 || string(w2.writes[0]) != "\n" {
		t.Fatalf("an empty line: %d writes %q, want one write of the newline alone", len(w2.writes), w2.writes)
	}
}

// TestAppendJSONL checks the one-line contract: one Write of line + "\n", the file is created, and
// two appends in a row yield two lines.
func TestAppendJSONL(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "diarium.jsonl")
	h := &hookRecorder{}
	f := newFileIO(h)

	if f.exists(target) {
		t.Fatal("the diarium exists before any append")
	}
	if err := f.appendJSONL(target, []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	b, err := f.readFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "{\"a\":1}\n" {
		t.Fatalf("after one append: %q, want one line with a trailing newline", b)
	}
	if err := f.appendJSONL(target, []byte(`{"a":2}`)); err != nil {
		t.Fatal(err)
	}
	b, err = f.readFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "{\"a\":1}\n{\"a\":2}\n" {
		t.Fatalf("after two appends: %q, want two lines", b)
	}
	// The getters are always consulted (they return false here, so no sync and no delay happen):
	// per append the order is file-sync ask, directory-sync ask, notify, delay ask.
	want := []string{"fsync-ask", "fsync-ask", "notify", "delay-ask", "fsync-ask", "fsync-ask", "notify", "delay-ask"}
	if got := h.snapshot(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("hook calls = %v, want %v", got, want)
	}
}

// TestCopyFile checks that the source is untouched and the target appears atomically with the
// source's content.
func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in", "attach.bin")
	tmpDir := filepath.Join(dir, "agent")
	dst := filepath.Join(dir, "out", "attach.bin")
	content := []byte("attachment bytes")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, content, 0o644); err != nil {
		t.Fatal(err)
	}

	f := newFileIO(&hookRecorder{fsync: true})
	if err := f.copyFile(src, dst, tmpDir); err != nil {
		t.Fatal(err)
	}
	b, err := f.readFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, content) {
		t.Fatalf("target content = %q, want the source content", b)
	}
	b, err = f.readFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, content) {
		t.Fatalf("the source changed: %q", b)
	}
	names, err := f.listDir(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		t.Fatalf("temp files survive the copy: %v", names)
	}
}

// TestReadFileMissingIsErrNotExist pins that a missing file is fs.ErrNotExist, not a parse and not
// some other error.
func TestReadFileMissingIsErrNotExist(t *testing.T) {
	f := newFileIO(&hookRecorder{})
	_, err := f.readFile(filepath.Join(t.TempDir(), "never-published.json"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("readFile of a missing file: %v, want fs.ErrNotExist", err)
	}
}

// TestRemovePath checks that a missing path is not an error and that a folder tree goes away.
func TestRemovePath(t *testing.T) {
	f := newFileIO(&hookRecorder{})
	if err := f.removePath(filepath.Join(t.TempDir(), "never-existed")); err != nil {
		t.Fatalf("removePath of a missing path: %v, want nil", err)
	}

	tree := filepath.Join(t.TempDir(), "tree")
	if err := os.MkdirAll(filepath.Join(tree, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tree, "sub", "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := f.removePath(tree); err != nil {
		t.Fatal(err)
	}
	if f.exists(tree) {
		t.Fatal("the folder tree is still there")
	}
}
