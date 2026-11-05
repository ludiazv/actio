// The file primitives: atomic writes, JSONL appends, copies. Every write of a protocol file goes
// through here, so the sync provider's hooks are never forgotten and no other file reaches for os.
//
// An atomic write is a temp file named .<base>.<nonce>.tmp inside the agent's own folder, then a
// rename: a peer either sees the old file or the new one, never a half-written one. A JSONL append
// is one Write of one line on an O_APPEND file.
//
// Validation is the caller's job, not this file's: a protocol write is validated against its schema
// first (that is what the publication and diarium files do), and this file writes what it is given.
package actio

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// fileIO is the file access of one agent, bound to that agent's sync provider: there is no
// package-level provider to install.
type fileIO struct {
	provider SyncProvider
}

// newFileIO binds the primitives to a sync provider.
func newFileIO(provider SyncProvider) *fileIO { return &fileIO{provider: provider} }

// tempName is the temp file of an atomic write: .<base>.<nonce>.tmp inside tmpDir, which is always
// the agent's own folder and on the same filesystem as the target.
func tempName(target, tmpDir string) string {
	return filepath.Join(tmpDir, "."+filepath.Base(target)+"."+newNonce()+".tmp")
}

// postWriteHooks applies the provider's hooks once a write is in place, in the protocol's order
// (*Sync and race conditions*): a directory sync when the provider asks, then Notify, then the
// delay. Syncing a directory is not allowed on some platforms, so that failure is ignored.
func (f *fileIO) postWriteHooks(target string) {
	if f.provider.Fsync() {
		if dh, err := os.Open(filepath.Dir(target)); err == nil {
			_ = dh.Sync()
			dh.Close()
		}
	}
	f.provider.Notify()
	if d := f.provider.Delay(); d > 0 {
		time.Sleep(d)
	}
}

// writeJSONAtomic marshals value and renames it into target. tmpDir is the folder of the temp file:
// always the agent's own folder, which is on the same filesystem as the target.
func (f *fileIO) writeJSONAtomic(target string, value any, tmpDir string) error {
	raw, err := marshalJSON(value)
	if err != nil {
		return err
	}
	return f.writeBytesAtomic(target, raw, tmpDir)
}

// writeBytesAtomic renames raw bytes into target, with the sync provider's hooks. It is the form of
// writeJSONAtomic for content that must not be re-marshalled.
func (f *fileIO) writeBytesAtomic(target string, raw []byte, tmpDir string) error {
	if err := f.makeDir(tmpDir); err != nil {
		return err
	}
	tmp := tempName(target, tmpDir)
	fh, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	// A failed write leaves no temp file behind.
	defer func() {
		if err != nil {
			os.Remove(tmp)
		}
	}()
	if _, err = fh.Write(raw); err != nil {
		fh.Close()
		return err
	}
	// The provider's file sync, before the rename.
	if f.provider.Fsync() {
		if err = fh.Sync(); err != nil {
			fh.Close()
			return err
		}
	}
	if err = fh.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, target); err != nil {
		return err
	}
	f.postWriteHooks(target)
	return nil
}

// appendJSONL appends one line (no trailing newline in line) to an O_APPEND file in a single Write,
// then runs the sync provider's hooks. The single Write is what keeps a half line from ever being
// visible to a peer reading the diarium.
func (f *fileIO) appendJSONL(target string, line []byte) error {
	full := make([]byte, 0, len(line)+1)
	full = append(append(full, line...), '\n')
	fh, err := os.OpenFile(target, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	var werr error
	if _, werr = fh.Write(full); werr == nil && f.provider.Fsync() {
		werr = fh.Sync()
	}
	if cerr := fh.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return werr
	}
	f.postWriteHooks(target)
	return nil
}

// copyFile copies a file through a temp file and a rename (attachments in forum). The destination
// name must already have passed filepath.IsLocal when it came from a peer.
func (f *fileIO) copyFile(src, dst, tmpDir string) error {
	if err := f.makeDir(tmpDir); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	tmp := tempName(dst, tmpDir)
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		in.Close()
		return err
	}
	var cerr error
	if _, cerr = io.Copy(out, in); cerr == nil {
		cerr = in.Close()
	}
	if cerr == nil && f.provider.Fsync() {
		cerr = out.Sync()
	}
	if werr := out.Close(); cerr == nil {
		cerr = werr
	}
	if cerr != nil {
		os.Remove(tmp)
		return cerr
	}
	if err = os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return err
	}
	f.postWriteHooks(dst)
	return nil
}

// marshalJSON encodes a protocol value the way the protocol expects: HTML escaping off, so a peer
// reads the same bytes back. No trailing newline.
func marshalJSON(value any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// readFile returns the bytes of a file: a JSON envelope, a diarium, team metadata. A missing
// file is fs.ErrNotExist, and the caller decides whether that is silence (a peer that has not
// published yet) or a report. The bytes are validated before they are decoded (G-5): this file
// hands out bytes, it does not parse them.
func (f *fileIO) readFile(target string) ([]byte, error) {
	return os.ReadFile(target)
}

// listDir names the entries of a folder in lexical order.
func (f *fileIO) listDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	sort.Strings(names)
	return names, nil
}

// makeDir creates a folder and its parents; idempotent.
func (f *fileIO) makeDir(dir string) error { return os.MkdirAll(dir, 0o755) }

// removePath deletes a file or a folder tree; a missing path is not an error.
func (f *fileIO) removePath(target string) error {
	err := os.Remove(target)
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return os.RemoveAll(target) // a folder with content
}

// exists reports whether a file or folder is there.
func (f *fileIO) exists(target string) bool {
	_, err := os.Stat(target)
	return err == nil
}
