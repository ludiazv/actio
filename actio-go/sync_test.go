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
		// The value must be named in quotes: for the empty value a bare Contains is vacuous ("" is
		// a substring of every message), and the quotes are what the message owes the operator.
		if !strings.Contains(err.Error(), fmt.Sprintf("%q", value)) {
			t.Errorf("%q: the error %q does not name the value in quotes", value, err)
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

// TestMutagenBinReadAtRunTime: the binary path is read from ACTIO_MUTAGEN_BIN at run time, not at
// construction: changing the variable after the provider exists changes the binary the next run
// uses.
func TestMutagenBinReadAtRunTime(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "old-runs")
	neu := filepath.Join(dir, "new-runs")
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagen(t, old, "", false))
	p, err := newSyncProvider("mutagen:sess", nil)
	if err != nil {
		t.Fatal(err)
	}
	// The variable changes after the provider was built; the run must follow it.
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagen(t, neu, "", false))
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := countRuns(old); n != 0 {
		t.Fatalf("the run used the pre-construction binary: %d runs, want 0", n)
	}
	if n := countRuns(neu); n != 1 {
		t.Fatalf("the run did not use the post-construction binary: %d runs, want 1", n)
	}
}

// TestMutagenDefaultBinaryName: with ACTIO_MUTAGEN_BIN unset, the binary is the default name
// "mutagen", resolved through PATH: a fake `mutagen` placed first on a t.Setenv PATH is the one
// that runs.
func TestMutagenDefaultBinaryName(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	binDir := t.TempDir()
	script := "#!/bin/sh\necho run >> " + counter + "\n"
	if err := os.WriteFile(filepath.Join(binDir, "mutagen"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ACTIO_MUTAGEN_BIN", "") // unset: the default name must be used
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	p, err := newSyncProvider("mutagen:sess", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := countRuns(counter); n != 1 {
		t.Fatalf("the default binary name did not run the fake mutagen: %d runs, want 1", n)
	}
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

// fakeMutagenHolding writes a shell script that signals its start ("started"), then holds the run
// open until marker exists, and only then records its run: the test controls when the run ends,
// so nothing here depends on how long the machine takes.
func fakeMutagenHolding(t *testing.T, counter, marker string) string {
	t.Helper()
	var sb strings.Builder
	sb.WriteString("#!/bin/sh\n")
	fmt.Fprintf(&sb, "echo started >> %s\n", counter)
	fmt.Fprintf(&sb, "while [ ! -f %s ]; do sleep 0.05; done\n", marker)
	fmt.Fprintf(&sb, "echo run >> %s\n", counter)
	p := filepath.Join(t.TempDir(), "mutagen-fake.sh")
	if err := os.WriteFile(p, []byte(sb.String()), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

// countLines counts the lines of counter that contain needle (0 when the file is missing).
func countLines(counter, needle string) int {
	b, err := os.ReadFile(counter)
	if err != nil {
		return 0
	}
	return strings.Count(string(b), needle)
}

// TestMutagenCloseLetsPromisedFollowUpRun: Close cancels only the debounced run; a follow-up
// already promised to a mid-run Flush still runs. The ordering is explicit, not timed: the fake
// binary signals its start, the test waits for the follow-up to be actually promised (p.pending),
// and only then closes; the held run is released by a marker file. If the follow-up were not
// promised — or Close cancelled it — the mid-run Flush would never resolve and the test fails.
func TestMutagenCloseLetsPromisedFollowUpRun(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	marker := filepath.Join(dir, "release")
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagenHolding(t, counter, marker))
	p, err := newSyncProvider("mutagen:sess", nil)
	if err != nil {
		t.Fatal(err)
	}
	mp, ok := p.(*mutagenProvider)
	if !ok {
		t.Fatal("not a mutagen provider")
	}
	ctx := context.Background()

	first := make(chan error, 1)
	go func() { first <- p.Flush(ctx) }()
	pollUntil(t, "run 1 to start", func() bool { return countLines(counter, "started") >= 1 })

	// Mid-run: this Flush is entitled to the single promised follow-up, not a swallow.
	second := make(chan error, 1)
	go func() { second <- p.Flush(ctx) }()
	// Wait for the promise explicitly (the test is in-package): without this, Close on a slow
	// machine could land before the registration and the test would pass while proving nothing.
	pollUntil(t, "the follow-up to be promised", func() bool {
		mp.mu.Lock()
		defer mp.mu.Unlock()
		return mp.pending != nil
	})
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// Release the held run: the follow-up was promised before the Close, so it must run after it.
	if err := os.WriteFile(marker, []byte("go"), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the mid-run Flush never resolved: the promised follow-up did not run after Close")
	}
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first Flush never resolved")
	}
	pollUntil(t, "the promised follow-up to run", func() bool { return countRuns(counter) >= 2 })
	if n := countRuns(counter); n != 2 {
		t.Fatalf("the promised follow-up ran mutagen %d times in total, want 2", n)
	}
}

// TestMutagenCloseBeatsCallbackInFireWindow: in the window between the timer firing and the
// callback clearing p.timer, Close() lands and the run must not start — this test pins that the
// process never started. The window itself is sub-microsecond and not deterministically
// reproducible by a probe, so the test simulates the
// exact interleaving deterministically: Close first, then the callback. With the closed flag the
// callback sees it and the run never starts; without it the callback would spawn the process and
// this test fails.
func TestMutagenCloseBeatsCallbackInFireWindow(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagen(t, counter, "", false))
	p, err := newSyncProvider("mutagen:sess", nil)
	if err != nil {
		t.Fatal(err)
	}
	mp, ok := p.(*mutagenProvider)
	if !ok {
		t.Fatal("not a mutagen provider")
	}
	mp.Notify() // arms the debounced run
	if err := mp.Close(); err != nil {
		t.Fatal(err)
	}
	// The timer has "fired" and its callback runs after the Close: it must see closed and not
	// start the process.
	mp.onDebounce()
	time.Sleep(100 * time.Millisecond) // enough for a started process to record itself
	if n := countRuns(counter); n != 0 {
		t.Fatalf("the run started after Close: %d runs, want 0 (the process never started)", n)
	}
}

// TestMutagenFlushRespectsContext: a cancelled context stops the wait, kills the process, and
// returns the context error.
func TestMutagenFlushRespectsContext(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	t.Setenv("ACTIO_MUTAGEN_BIN", fakeMutagenKillRecording(t, counter))
	var killErr atomic.Value // the error the killed run reports to onError
	p, err := newSyncProvider("mutagen:sess", func(e error) { killErr.Store(e) })
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
	// The process is not just abandoned: the kill is delivered, the run reports its death to
	// onError, and only then does the provider settle. (The kill is a SIGKILL, which no shell can
	// trap, so the death is observed through the run's failure, not a trap.)
	pollUntil(t, "the killed run to be reported to onError", func() bool { return killErr.Load() != nil })
	if e, _ := killErr.Load().(error); e == nil || !strings.Contains(e.Error(), "killed") {
		t.Fatalf("the cancelled run was not killed: onError = %v, want a killed-process failure", e)
	}
}

// fakeMutagenKillRecording writes a shell script that records its start and then sleeps so long
// that only a kill can end it: a run that were not killed would still be going when the test ends.
func fakeMutagenKillRecording(t *testing.T, counter string) string {
	t.Helper()
	var sb strings.Builder
	sb.WriteString("#!/bin/sh\n")
	fmt.Fprintf(&sb, "echo run >> %s\n", counter)
	sb.WriteString("sleep 30\n")
	p := filepath.Join(t.TempDir(), "mutagen-fake.sh")
	if err := os.WriteFile(p, []byte(sb.String()), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}
