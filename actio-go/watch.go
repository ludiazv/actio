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

// dirWatcher is the watcher of one folder.
type dirWatcher struct{}

// watchDir arms the watcher on dir and runs the first scan. A missing dir is an error, not a watcher
// that reports nothing.
func watchDir(dir string) (*dirWatcher, error) { panic("actio: not implemented") }

// files carries each name that appeared or changed, in lexical order, after every scan. The first
// scan reports what was already there.
func (w *dirWatcher) files() <-chan string { panic("actio: not implemented") }

// lost carries one error when the folder or the watcher died, and the channels stop after it.
func (w *dirWatcher) lost() <-chan error { panic("actio: not implemented") }

// close stops the goroutine and the fsnotify watcher; idempotent, and it never sends again.
func (w *dirWatcher) close() { panic("actio: not implemented") }

// watchHub holds the watchers of destination inboxes: one per inbox, shared by the requests that are
// waiting on it and released when the last one leaves (D-18). A watcher holds an inotify instance,
// which systems limit per user, so requests never hold one of their own.
type watchHub struct{}

// inboxWatch is one request's subscription to an inbox watcher.
type inboxWatch struct{}

// newWatchHub creates an empty hub.
func newWatchHub() *watchHub { panic("actio: not implemented") }

// subscribe returns the subscription of one request to inbox, creating and reference-counting the
// watcher when it is the first subscriber. A missing inbox is an error: the request fails locally.
func (h *watchHub) subscribe(inbox string) (*inboxWatch, error) { panic("actio: not implemented") }

// unsubscribe releases one subscription; at zero the watcher closes and its channels close.
func (h *watchHub) unsubscribe(sub *inboxWatch) { panic("actio: not implemented") }

// close releases every watcher in the hub.
func (h *watchHub) close() { panic("actio: not implemented") }
