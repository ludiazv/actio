// Tests for the sync providers. The mutagen provider is exercised with a fake binary: a shell
// script that appends one line to a counter file, optionally sleeping and/or failing.
package actio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSyncProviderForms(t *testing.T) {
	ok := []struct {
		value string
		fsync bool
		delay time.Duration
	}{
		{"none", false, 0},
		{"delay", false, 50 * time.Millisecond},
		{"delay:1", false, time.Millisecond},
		{"delay:10000", false, 10 * time.Second},
		{"fsync+delay", true, 50 * time.Millisecond},
		{"fsync+delay:250", true, 250 * time.Millisecond},
		{"mutagen:sess-1", false, 0},
	}
	for _, c := range ok {
		p, err := newSyncProvider(c.value, nil)
		if err != nil {
			t.Errorf("%q: %v, want a provider", c.value, err)
			continue
		}
		if p.Fsync() != c.fsync || p.Delay() != c.delay {
			t.Errorf("%q: Fsync()=%v Delay()=%v, want %v/%v", c.value, p.Fsync(), p.Delay(), c.fsync, c.delay)
		}
		if err := p.Close(); err != nil {
			t.Errorf("%q: Close(): %v", c.value, err)
		}
	}

	bad := []string{"", "bogus", "delay:0", "delay:10001", "delay:abc", "delay:", "delay:+5", "fsync+delay:0", "mutagen:"}
	for _, value := range bad {
		_, err := newSyncProvider(value, nil)
		if err == nil {
			t.Errorf("%q: accepted, want an error", value)
			continue
		}
		if !strings.Contains(err.Error(), value) {
			t.Errorf("%q: the error %q does not name the value", value, err)
		}
	}
}

// fakeMutagen writes a shell script that appends one line to counter (and, optionally, sleeps and
// exits non-zero) and returns its path.
func fakeMutagen(t *testing.T, counter string, sleepSecs string, fail bool) string {
	t.Helper()
	var sb strings.Builder
	sb.WriteString("#!/bin/sh\n")
	fmt.Fprintf(&sb, "echo run >> %s\n", counter)
	if sleepSecs != "" {
		fmt.Fprintf(&sb, "sleep %s\n", sleepSecs)
	}
	if fail {
		sb.WriteString("exit 1\n")
	}
	p := filepath.Join(t.TempDir(), "mutagen-fake.sh")
	if err := os.WriteFile(p, []byte(sb.String()), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func countRuns(counter string) int {
	b, err := os.ReadFile(counter)
	if err != nil {
		return 0
	}
	return strings.Count(string(b), "run")
}

// pollUntil waits up to ~1 s for cond to hold, polling every 5 ms.
func pollUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestMutagenNotifyBurstCollapsesToOneRun: notifications inside the 200 ms window collapse into
// one run.
func TestMutagenNotifyBurstCollapsesToOneRun(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagen(t, counter, "", false))
	p, err := newSyncProvider("mutagen:sess", nil)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		p.Notify()
	}
	pollUntil(t, "the debounced run", func() bool { return countRuns(counter) >= 1 })
	// The burst armed exactly one timer; well past the window there must be no second run.
	time.Sleep(400 * time.Millisecond)
	if n := countRuns(counter); n != 1 {
		t.Fatalf("a burst of 5 notifies ran mutagen %d times, want 1", n)
	}
}

// TestMutagenFlushRunsImmediately: Flush starts a run at once and waits for it.
func TestMutagenFlushRunsImmediately(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagen(t, counter, "", false))
	p, err := newSyncProvider("mutagen:sess", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := countRuns(counter); n != 1 {
		t.Fatalf("after Flush: %d runs, want 1", n)
	}
}

// TestMutagenFlushMidRunNotSwallowed: a Flush that arrives while a run is in flight is not
// absorbed by it: it resolves only once a run that started after the call has finished, and that
// follow-up run happens.
func TestMutagenFlushMidRunNotSwallowed(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagen(t, counter, "0.4", false))
	p, err := newSyncProvider("mutagen:sess", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	var mu sync.Mutex
	var returns []time.Time
	record := func() {
		mu.Lock()
		returns = append(returns, time.Now())
		mu.Unlock()
	}
	first := make(chan struct{})
	go func() {
		if err := p.Flush(ctx); err != nil {
			t.Error(err)
		}
		record()
		close(first)
	}()
	pollUntil(t, "run 1 to start", func() bool { return countRuns(counter) >= 1 })

	// Mid-run: this Flush must not be absorbed by the in-flight run. It resolves only once a run
	// that started after the call has finished, and that follow-up run happens.
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	record()
	<-first

	if n := countRuns(counter); n != 2 {
		t.Fatalf("two Flushes, one mid-run, ran mutagen %d times, want 2 (the mid-run one is a follow-up, not a swallow)", n)
	}
	// The follow-up is a full run that started only after the in-flight one ended, so the two
	// resolutions are a whole run apart: an absorbed Flush would resolve with the in-flight one.
	if gap := returns[1].Sub(returns[0]); gap < 300*time.Millisecond {
		t.Fatalf("the two Flushes resolved %v apart, within one run time: the mid-run one was absorbed", gap)
	}
}

// TestMutagenTwoMidRunRequestsOneFollowUp: while a run is going, several requests (Flushes and a
// Notify) coalesce into the single promised follow-up: one in-flight run plus exactly one more.
func TestMutagenTwoMidRunRequestsOneFollowUp(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagen(t, counter, "0.4", false))
	p, err := newSyncProvider("mutagen:sess", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	first := make(chan error, 1)
	go func() { first <- p.Flush(ctx) }()
	pollUntil(t, "run 1 to start", func() bool { return countRuns(counter) >= 1 })

	results := make(chan error, 2)
	go func() { results <- p.Flush(ctx) }()
	go func() { results <- p.Flush(ctx) }()
	p.Notify() // mid-run: it collapses into the same single follow-up
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	pollUntil(t, "the single follow-up to run", func() bool { return countRuns(counter) >= 2 })
	time.Sleep(400 * time.Millisecond) // well past the window: a third run would show up
	if n := countRuns(counter); n != 2 {
		t.Fatalf("two mid-run requests plus a notify ran mutagen %d times, want 2 (one in-flight, one follow-up)", n)
	}
}

// TestMutagenFailingRunCallsOnErrorOncePerRun: a failing run (non-zero exit) calls onError once per
// failed run and never returns an error to the protocol; an onError that panics does not break the
// provider.
func TestMutagenFailingRunCallsOnErrorOncePerRun(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagen(t, counter, "", true))
	var failures atomic.Int64
	p, err := newSyncProvider("mutagen:sess", func(error) { failures.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush returned %v: run failures go to onError, not the protocol", err)
	}
	pollUntil(t, "onError after run 1", func() bool { return failures.Load() == 1 })
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	pollUntil(t, "onError after run 2", func() bool { return failures.Load() == 2 })
	if n := countRuns(counter); n != 2 {
		t.Fatalf("%d runs, want 2", n)
	}

	var panics atomic.Int64
	p2, err := newSyncProvider("mutagen:sess", func(error) {
		panics.Add(1)
		panic("onError must not break the write")
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p2.Flush(ctx); err != nil {
		t.Fatalf("Flush after a panicking onError: %v", err)
	}
	pollUntil(t, "the panicking onError to fire", func() bool { return panics.Load() == 1 })
	if err := p2.Flush(ctx); err != nil {
		t.Fatalf("the provider is broken after a panicking onError: %v", err)
	}
}

// TestMutagenCloseCancelsPendingRunAndIsIdempotent: Close cancels the pending debounced run, and a
// second Close is a no-op.
func TestMutagenCloseCancelsPendingRunAndIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagen(t, counter, "", false))
	p, err := newSyncProvider("mutagen:sess", nil)
	if err != nil {
		t.Fatal(err)
	}

	p.Notify() // arms the debounced run 200 ms out
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	// Well past the debounce window: the cancelled run must not have fired.
	time.Sleep(500 * time.Millisecond)
	if n := countRuns(counter); n != 0 {
		t.Fatalf("Close did not cancel the pending debounced run: %d runs", n)
	}
}

// TestMutagenCloseLetsPromisedFollowUpRun: Close cancels only the debounced run; a follow-up
// already promised to a mid-run Flush still runs.
func TestMutagenCloseLetsPromisedFollowUpRun(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagen(t, counter, "0.4", false))
	p, err := newSyncProvider("mutagen:sess", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	first := make(chan error, 1)
	go func() { first <- p.Flush(ctx) }()
	pollUntil(t, "run 1 to start", func() bool { return countRuns(counter) >= 1 })

	second := make(chan error, 1)
	go func() { second <- p.Flush(ctx) }()
	time.Sleep(50 * time.Millisecond) // let the mid-run Flush register the promised follow-up
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	pollUntil(t, "the promised follow-up to run", func() bool { return countRuns(counter) >= 2 })
	if n := countRuns(counter); n != 2 {
		t.Fatalf("the promised follow-up ran mutagen %d times in total, want 2", n)
	}
}

// TestMutagenFlushRespectsContext: a cancelled context stops the wait, kills the process, and
// returns the context error.
func TestMutagenFlushRespectsContext(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagen(t, counter, "1", false))
	p, err := newSyncProvider("mutagen:sess", nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = p.Flush(ctx)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Flush = %v, want the context error", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Flush kept waiting %v after the context was cancelled", elapsed)
	}
}
