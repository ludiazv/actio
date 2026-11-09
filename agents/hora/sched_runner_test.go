// sched_runner_test.go pins the runner's D-10 behaviour under testing/synctest.
// Each row runs in its own bubble whose clock starts at 2000-01-01T00:00:00Z,
// so the rows take their times from time.Now() inside the bubble and advance
// it with bubble timers. The runner is stopped by the bubble's t.Cleanup,
// which runs inside the bubble.
package main

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// fireCounter records the fire calls a test's runner makes.
type fireCounter struct {
	mu    sync.Mutex
	n     int
	times []time.Time
}

func (f *fireCounter) fire(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	f.times = append(f.times, time.Now())
}

func (f *fireCounter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

func (f *fireCounter) at(i int) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.times[i]
}

// entry builds an Entry with the fields the runner reads.
func entry(id, typ, expr string) Entry {
	return Entry{SchedID: id, SchedType: typ, SchedExpression: expr}
}

// advance moves the bubble clock forward by d by blocking on a bubble timer.
func advance(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-time.After(d):
	}
}

func TestRunner(t *testing.T) {
	t.Run("one-shot fires exactly once", func(t *testing.T) {
		synctest.Test(t, func(bt *testing.T) {
			c := &fireCounter{}
			var r *runner
			r = newRunner(func(id string) {
				c.fire(id)
				if c.count() > 1 {
					// A mutant whose one-shot keeps handing back the same time would spin
					// the cron loop at one instant forever, so this row would time out
					// instead of failing. End the runner here and fail with a message.
					bt.Errorf("one-shot fired %d times", c.count())
					if err := r.stop(context.Background()); err != nil {
						bt.Errorf("stop after the extra firing: %v", err)
					}
				}
			})
			bt.Cleanup(func() { _ = r.stop(context.Background()) })
			e := entry("SCH-00000001", "one-shot",
				time.Now().Add(30*time.Second).UTC().Format("2006-01-02T15:04:05Z"))
			if err := r.add(e); err != nil {
				bt.Fatal(err)
			}
			r.start()
			advance(bt, 35*time.Second)
			if c.count() != 1 {
				bt.Fatalf("count after 35s = %d, want 1", c.count())
			}
			advance(bt, 45*time.Second) // 80s in: past the second minute
			if c.count() != 1 {
				bt.Fatalf("count after 80s = %d, want 1", c.count())
			}
		})
	})

	t.Run("@every 30s fires 3 times over 100s", func(t *testing.T) {
		synctest.Test(t, func(bt *testing.T) {
			c := &fireCounter{}
			r := newRunner(c.fire)
			bt.Cleanup(func() { _ = r.stop(context.Background()) })
			if err := r.add(entry("SCH-00000002", "periodic", "@every 30s")); err != nil {
				bt.Fatal(err)
			}
			r.start()
			advance(bt, 100*time.Second)
			if c.count() != 3 {
				bt.Fatalf("count over 100s = %d, want 3", c.count())
			}
		})
	})

	t.Run("Madrid cron fires at its UTC instant", func(t *testing.T) {
		synctest.Test(t, func(bt *testing.T) {
			c := &fireCounter{}
			r := newRunner(c.fire)
			bt.Cleanup(func() { _ = r.stop(context.Background()) })
			e := entry("SCH-00000003", "cron", "CRON_TZ=Europe/Madrid 0 0 9 * * *")
			if err := r.add(e); err != nil {
				bt.Fatal(err)
			}
			r.start()
			// The bubble starts at 2000-01-01T00:00:00Z; Madrid is on CET
			// (UTC+1) in January, so 09:00 Madrid is 08:00Z.
			advance(bt, 8*time.Hour+time.Second)
			if c.count() != 1 {
				bt.Fatalf("count = %d, want 1", c.count())
			}
			want := time.Date(2000, 1, 1, 8, 0, 0, 0, time.UTC)
			if got := c.at(0); !got.Equal(want) {
				bt.Fatalf("firing at %v, want %v", got, want)
			}
		})
	})

	t.Run("add and remove at runtime", func(t *testing.T) {
		synctest.Test(t, func(bt *testing.T) {
			c := &fireCounter{}
			r := newRunner(c.fire)
			bt.Cleanup(func() { _ = r.stop(context.Background()) })
			r.start()
			e := entry("SCH-00000004", "periodic", "@every 10s")
			if err := r.add(e); err != nil {
				bt.Fatal(err)
			}
			advance(bt, 25*time.Second)
			if c.count() != 2 {
				bt.Fatalf("count before remove = %d, want 2", c.count())
			}
			r.remove(e.SchedID)
			advance(bt, 25*time.Second)
			if c.count() != 2 {
				bt.Fatalf("count after remove = %d, want 2", c.count())
			}
		})
	})

	t.Run("remove from inside its own job", func(t *testing.T) {
		synctest.Test(t, func(bt *testing.T) {
			c := &fireCounter{}
			var r *runner
			r = newRunner(func(id string) {
				c.fire(id)
				r.remove(id) // the one-shot's life: fire, then remove itself
			})
			bt.Cleanup(func() { _ = r.stop(context.Background()) })
			e := entry("SCH-00000005", "one-shot",
				time.Now().Add(5*time.Second).UTC().Format("2006-01-02T15:04:05Z"))
			if err := r.add(e); err != nil {
				bt.Fatal(err)
			}
			r.start()
			advance(bt, 30*time.Second)
			if c.count() != 1 {
				bt.Fatalf("count = %d, want 1", c.count())
			}
		})
	})

	t.Run("add rejects a bad entry and adds nothing", func(t *testing.T) {
		synctest.Test(t, func(bt *testing.T) {
			c := &fireCounter{}
			r := newRunner(c.fire)
			bt.Cleanup(func() { _ = r.stop(context.Background()) })
			past := entry("SCH-00000006", "one-shot",
				time.Now().Add(-time.Second).UTC().Format("2006-01-02T15:04:05Z"))
			if err := r.add(past); err == nil {
				bt.Fatal("add accepted a past one-shot")
			}
			if err := r.add(entry("SCH-00000007", "periodic", "@every 500ms")); err == nil {
				bt.Fatal("add accepted @every 500ms")
			}
			r.start()
			advance(bt, 5*time.Minute)
			if c.count() != 0 {
				bt.Fatalf("count = %d, want 0", c.count())
			}
		})
	})

	t.Run("stop is bounded by its context", func(t *testing.T) {
		synctest.Test(t, func(bt *testing.T) {
			// A long-running job: stop on an expired context must return the
			// context's error without waiting for the job.
			c := &fireCounter{}
			r := newRunner(func(id string) {
				c.fire(id)
				select {
				case <-time.After(10 * time.Minute): // a long-running call
				}
			})
			bt.Cleanup(func() { _ = r.stop(context.Background()) })
			e := entry("SCH-00000008", "one-shot",
				time.Now().Add(5*time.Second).UTC().Format("2006-01-02T15:04:05Z"))
			if err := r.add(e); err != nil {
				bt.Fatal(err)
			}
			r.start()
			advance(bt, 10*time.Second)
			if c.count() != 1 {
				bt.Fatalf("count = %d, want 1", c.count())
			}
			ctx, cancel := context.WithDeadline(context.Background(),
				time.Date(1, 1, 1, 0, 0, 2, 0, time.UTC))
			cancel()
			if err := r.stop(ctx); err == nil {
				bt.Fatal("stop on an expired context returned nil")
			}
			// Let the long job finish so the bubble's goroutines are done.
			advance(bt, 10*time.Minute+time.Second)

			// An idle runner stops cleanly.
			r2 := newRunner(func(string) {})
			r2.start()
			if err := r2.stop(context.Background()); err != nil {
				bt.Fatalf("stop on an idle runner: %v", err)
			}
		})
	})
}
