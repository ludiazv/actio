// sched.go parses the three schedule expression forms (D-4, D-5, D-6) and
// runs the cron-based scheduler that fires entries (D-10).
package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// oneShot is a cron.Schedule that yields its instant once and the zero time
// after, so the runner fires it exactly once (D-4).
type oneShot struct {
	t     time.Time
	fired bool
}

// Next returns the instant on the first call and the zero time afterwards.
func (o *oneShot) Next(time.Time) time.Time {
	if o.fired {
		return time.Time{}
	}
	o.fired = true
	return o.t
}

// parseSchedule validates an expression for typ and returns its schedule and
// the stored form of the expression (D-4, D-5, D-6).
func parseSchedule(typ, expr string, loc *time.Location, now time.Time) (cron.Schedule, string, error) {
	switch typ {
	case "one-shot":
		t, err := time.Parse(time.RFC3339, expr)
		if err != nil {
			return nil, "", fmt.Errorf("one-shot %s: not RFC 3339", expr)
		}
		if t.Nanosecond() != 0 {
			return nil, "", fmt.Errorf("one-shot %s: fractional second", expr)
		}
		if !t.After(now) {
			return nil, "", fmt.Errorf("one-shot %s: not after now", expr)
		}
		return &oneShot{t: t}, t.UTC().Format("2006-01-02T15:04:05Z"), nil
	case "periodic":
		if !strings.HasPrefix(expr, "@every ") {
			return nil, "", fmt.Errorf("periodic %s: must be @every <d>", expr)
		}
		d, err := time.ParseDuration(expr[len("@every "):])
		if err != nil {
			return nil, "", fmt.Errorf("periodic %s: bad duration", expr)
		}
		if d < time.Second {
			return nil, "", fmt.Errorf("periodic %s: less than 1s", expr)
		}
		if d%time.Second != 0 {
			return nil, "", fmt.Errorf("periodic %s: not a whole number of seconds", expr)
		}
		return cron.Every(d), expr, nil
	case "cron":
		stored := expr
		if !strings.HasPrefix(expr, "CRON_TZ=") && !strings.HasPrefix(expr, "TZ=") {
			stored = "CRON_TZ=" + loc.String() + " " + expr
		}
		parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		s, err := parser.Parse(stored)
		if err != nil {
			return nil, "", fmt.Errorf("cron %s: %v", expr, err)
		}
		if s.Next(now).IsZero() {
			return nil, "", fmt.Errorf("cron %s: never fires", expr)
		}
		return s, stored, nil
	default:
		return nil, "", fmt.Errorf("unknown schedule type: %s", typ)
	}
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
