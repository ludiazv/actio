//go:build unix

package actio

import (
	"fmt"
	"os"
	"syscall"
)

// markerOf is the per-file marker of D-23: "mtime:size:inode", with mtime in nanoseconds.
// Nanoseconds, not milliseconds: a same-size rewrite within a millisecond is exactly what the
// marker is for, and a file's mtime on a modern file system carries sub-millisecond precision
// (the reference's mtimeMs does too, as a fraction).
func markerOf(st os.FileInfo) string {
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Sprintf("%d:%d:0", st.ModTime().UnixNano(), st.Size())
	}
	return fmt.Sprintf("%d:%d:%d", st.ModTime().UnixNano(), st.Size(), sys.Ino)
}
