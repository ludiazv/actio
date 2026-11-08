// sched.go parses the three schedule expression forms (D-4, D-5, D-6) and
// runs the cron-based scheduler that fires entries (D-10).
package main

import (
	"context"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// parseSchedule validates an expression for typ and returns its schedule and
// the stored form of the expression (D-4, D-5, D-6).
func parseSchedule(typ, expr string, loc *time.Location, now time.Time) (cron.Schedule, string, error) {
	panic("not implemented: parseSchedule")
}

// runner wraps one cron.Cron in UTC and maps sched ids to cron entries (D-10).
type runner struct {
	cron *cron.Cron
	mu   sync.Mutex
	ids  map[string]cron.EntryID
	fire func(id string)
}

// newRunner builds a runner that calls fire for each firing.
func newRunner(fire func(id string)) *runner {
	panic("not implemented: newRunner")
}

// add schedules the entry.
func (r *runner) add(e Entry) error {
	panic("not implemented: add")
}

// remove unschedules the entry.
func (r *runner) remove(id string) {
	panic("not implemented: remove")
}

// start begins firing.
func (r *runner) start() {
	panic("not implemented: start")
}

// stop ends firing, cancelling pending firings and waiting for running jobs (D-12).
func (r *runner) stop(ctx context.Context) error {
	panic("not implemented: stop")
}
