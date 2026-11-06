//go:build !unix

package actio

import (
	"fmt"
	"os"
)

// markerOf is the per-file marker of D-23 on platforms without syscall.Stat_t:
// "mtime:size:0", with mtime in nanoseconds and the inode component 0.
func markerOf(st os.FileInfo) string {
	return fmt.Sprintf("%d:%d:0", st.ModTime().UnixNano(), st.Size())
}
