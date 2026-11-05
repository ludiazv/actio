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

// fileIO is the file access of one agent, bound to that agent's sync provider: there is no
// package-level provider to install.
type fileIO struct {
	provider SyncProvider
}

// newFileIO binds the primitives to a sync provider.
func newFileIO(provider SyncProvider) *fileIO { panic("actio: not implemented") }

// writeJSONAtomic marshals value and renames it into target. tmpDir is the folder of the temp file:
// always the agent's own folder, which is on the same filesystem as the target.
func (f *fileIO) writeJSONAtomic(target string, value any, tmpDir string) error {
	panic("actio: not implemented")
}

// writeBytesAtomic renames raw bytes into target, with the sync provider's hooks. It is the form of
// writeJSONAtomic for content that must not be re-marshalled.
func (f *fileIO) writeBytesAtomic(target string, raw []byte, tmpDir string) error {
	panic("actio: not implemented")
}

// appendJSONL appends one line (no trailing newline in line) to an O_APPEND file in a single Write,
// then runs the sync provider's hooks.
func (f *fileIO) appendJSONL(target string, line []byte) error { panic("actio: not implemented") }

// copyFile copies a file through a temp file and a rename (attachments in forum). The destination
// name must already have passed filepath.IsLocal when it came from a peer.
func (f *fileIO) copyFile(src, dst, tmpDir string) error { panic("actio: not implemented") }

// marshalJSON encodes a protocol value the way the protocol expects: HTML escaping off, so a peer
// reads the same bytes back.
func marshalJSON(value any) ([]byte, error) { panic("actio: not implemented") }

// readFile returns the bytes of a file: a JSON envelope, a diarium, team metadata. A missing
// file is fs.ErrNotExist, and the caller decides whether that is silence (a peer that has not
// published yet) or a report. The bytes are validated before they are decoded (G-5): this file
// hands out bytes, it does not parse them.
func (f *fileIO) readFile(target string) ([]byte, error) { panic("actio: not implemented") }

// listDir names the entries of a folder in lexical order.
func (f *fileIO) listDir(dir string) ([]string, error) { panic("actio: not implemented") }

// makeDir creates a folder and its parents; idempotent.
func (f *fileIO) makeDir(dir string) error { panic("actio: not implemented") }

// removePath deletes a file or a folder tree; a missing path is not an error.
func (f *fileIO) removePath(target string) error { panic("actio: not implemented") }

// exists reports whether a file or folder is there.
func (f *fileIO) exists(target string) bool { panic("actio: not implemented") }
