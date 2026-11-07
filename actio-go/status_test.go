// Tests for the publication: the builders, the publishers (validated before the write), the health
// derivation, and the heartbeat. The publishers are tested through the files they leave behind —
// and the files they must not leave.
package actio

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testFiles gives a *fileIO bound to the no-op provider: no hooks, no delays.
func testFiles(t *testing.T) *fileIO {
	t.Helper()
	provider, err := newSyncProvider("none", nil)
	if err != nil {
		t.Fatal(err)
	}
	return newFileIO(provider)
}

func testSpec(t *testing.T, id AgentID, now time.Time) Spec {
	t.Helper()
	spec := Spec{
		ID:                  id,
		ActioVersion:        "1.0",
		Datetime:            formatISO(now),
		Description:         "a test agent",
		AcceptsCommandsFrom: []string{},
		Capabilities:        []Capability{},
		SecuritySupport:     []string{},
		SecurityPosture:     "trusted",
	}
	if _, err := validateSpec(mustJSON(t, spec)); err != nil {
		t.Fatalf("the test spec is not valid: %v", err)
	}
	return spec
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := marshalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestBuildStatus(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 123_000_000, time.UTC)
	st := buildStatus("agent", StatusBusy, now, nil)
	if st.ID != "agent" || st.Status != StatusBusy || st.Datetime != "2026-01-02T03:04:05Z" {
		t.Fatalf("buildStatus = %+v", st)
	}
	// nil capability_status is written as an empty list, never null
	raw := mustJSON(t, st)
	if !strings.Contains(string(raw), `"capability_status":[]`) {
		t.Fatalf("nil capability_status is not an empty list: %s", raw)
	}
	cs := []CapabilityStatus{{Name: "echo", Running: 1, Queue: 2}}
	if got := buildStatus("agent", StatusIdle, now, cs); len(got.CapabilityStatus) != 1 || got.CapabilityStatus[0].Queue != 2 {
		t.Fatalf("capability_status = %+v", got.CapabilityStatus)
	}
}

func TestPublishStatus(t *testing.T) {
	dir := t.TempDir()
	folder := filepath.Join(dir, "agent")
	files := testFiles(t)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	st := buildStatus("agent", StatusIdle, now, nil)
	if err := publishStatus(files, folder, &st); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(folder, "status.json"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := validateStatus(raw)
	if err != nil {
		t.Fatalf("the written status.json is not valid: %v", err)
	}
	if got.ID != "agent" || got.Status != StatusIdle || got.Datetime != formatISO(now) {
		t.Fatalf("status.json = %+v", got)
	}

	// An invalid status is an error and changes nothing: the good file keeps its bytes, and no
	// temp file is left in the folder.
	bad := st
	bad.Status = StatusState("bogus")
	if err := publishStatus(files, folder, &bad); err == nil {
		t.Fatal("an invalid state was accepted")
	}
	again, err := os.ReadFile(filepath.Join(folder, "status.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(raw) {
		t.Fatal("the rejected write changed the file")
	}
	entries, err := os.ReadDir(folder)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			t.Fatalf("a temp file was left behind: %s", e.Name())
		}
	}
}

func TestPublishSpec(t *testing.T) {
	dir := t.TempDir()
	folder := filepath.Join(dir, "agent")
	files := testFiles(t)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	spec := testSpec(t, "agent", now)
	if err := publishSpec(files, folder, &spec); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(folder, "spec.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validateSpec(raw); err != nil {
		t.Fatalf("the written spec.json is not valid: %v", err)
	}

	// An invalid spec is rejected before the write: no file is created.
	bad := spec
	bad.ActioVersion = "not a version"
	if err := publishSpec(files, filepath.Join(dir, "other"), &bad); err == nil {
		t.Fatal("an invalid spec was accepted")
	}
	if _, err := os.Stat(filepath.Join(dir, "other", "spec.json")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("the rejected spec created a file")
	}
}

func TestPublishAnnounce(t *testing.T) {
	dir := t.TempDir()
	agentFolder := filepath.Join(dir, "agent")
	nuntiumDir := filepath.Join(dir, "nuntium")
	files := testFiles(t)
	log := newDiaria(files, agentFolder, "agent")
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	if err := publishAnnounce(files, log, agentFolder, nuntiumDir, "agent", "back to work", now); err != nil {
		t.Fatal(err)
	}
	// The announce file: in the shared nuntium folder, created on the fly, and valid.
	raw, err := os.ReadFile(filepath.Join(nuntiumDir, "agent.n.json"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := validateAnnounce(raw)
	if err != nil {
		t.Fatalf("the written announce file is not valid: %v", err)
	}
	if got.ID != "agent" || got.Nuntium != "back to work" || got.Datetime != formatISO(now) || got.TS != now.Unix() {
		t.Fatalf("announce file = %+v", got)
	}
	// The diarium line: the completed announce, in the agent's own folder.
	read, err := ReadDiarium(agentFolder)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Lines) != 1 || read.Lines[0].Event != "announce" || read.Lines[0].ID != "agent" {
		t.Fatalf("diarium = %+v, want one announce line", read.Lines)
	}

	// An invalid announce: an error, and nothing in either folder.
	badDir := t.TempDir()
	if err := publishAnnounce(files, log, filepath.Join(badDir, "agent"), filepath.Join(badDir, "nuntium"), AgentID("Bad ID!"), "x", now); err == nil {
		t.Fatal("an invalid id was accepted")
	}
	if _, err := os.Stat(filepath.Join(badDir, "nuntium", "Bad ID!.n.json")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("the rejected announce created a file")
	}
	if _, err := os.Stat(filepath.Join(badDir, "agent", "diarium.jsonl")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("the rejected announce touched the diarium")
	}
	again, err := ReadDiarium(agentFolder)
	if err != nil || len(again.Lines) != 1 {
		t.Fatalf("the good diarium changed: %+v (%v)", again.Lines, err)
	}
}

func TestBroadcast(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	root := t.TempDir()
	cfg := Config{Root: root, Sync: "none", Clock: func() time.Time { return now }}

	if err := Broadcast(cfg, "rescan, something changed"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "nuntium", "nuntium.json"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := validateAnnounce(raw)
	if err != nil {
		t.Fatalf("the written nuntium.json is not valid: %v", err)
	}
	if got.ID != "broadcast" || got.Nuntium != "rescan, something changed" || got.TS != now.Unix() || got.Datetime != formatISO(now) {
		t.Fatalf("nuntium.json = %+v", got)
	}

	// An unparseable sync value: an error, and the reserved file is not written.
	badRoot := t.TempDir()
	bad := cfg
	bad.Root = badRoot
	bad.Sync = "bogus"
	if err := Broadcast(bad, "x"); err == nil {
		t.Fatal("a bad sync value was accepted")
	}
	if _, err := os.Stat(filepath.Join(badRoot, "nuntium", "nuntium.json")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("the reserved file was written despite the error")
	}
}

// TestHealthOf pins every boundary of rules 4–7 and the unrated states of rule 8. The ages are
// measured from a second-resolution datetime; the sub-second cases come from the reader's clock.
func TestHealthOf(t *testing.T) {
	updated := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	dt := formatISO(updated)
	base := func(state StatusState, datetime string) Status {
		return Status{ID: "a", Status: state, Datetime: datetime, CapabilityStatus: []CapabilityStatus{}}
	}
	cases := []struct {
		name     string
		state    StatusState
		datetime string
		age      time.Duration
		want     Health
	}{
		{"fresh", StatusIdle, dt, 0, HealthHealthy},
		{"just under 60s", StatusIdle, dt, 59*time.Second + 999*time.Millisecond, HealthHealthy},
		{"exactly 60s", StatusIdle, dt, 60 * time.Second, HealthOnline},
		{"75s", StatusBusy, dt, 75 * time.Second, HealthOnline},
		{"just under 100s", StatusBusy, dt, 99*time.Second + 999*time.Millisecond, HealthOnline},
		{"exactly 100s", StatusIdle, dt, 100 * time.Second, HealthDegraded},
		{"300s", StatusIdle, dt, 300 * time.Second, HealthDegraded},
		{"exactly 600s", StatusBusy, dt, 600 * time.Second, HealthDegraded},
		{"just over 600s", StatusBusy, dt, 600*time.Second + time.Millisecond, HealthZombie},
		{"a day old", StatusIdle, dt, 24 * time.Hour, HealthZombie},
		{"a future datetime is the freshest", StatusIdle, formatISO(updated.Add(10 * time.Second)), 0, HealthHealthy},
		{"inactive is not rateable", StatusInactive, dt, 0, HealthUnknown},
		{"error is not rateable", StatusError, dt, 0, HealthUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := HealthOf(base(tc.state, tc.datetime), updated.Add(tc.age)); got != tc.want {
				t.Fatalf("HealthOf = %q, want %q", got, tc.want)
			}
		})
	}
	for _, bad := range []string{"", "garbage", "2026-01-02T03:04:05+00:00", "2026-01-02T03:04:05.5Z"} {
		if got := HealthOf(base(StatusIdle, bad), updated); got != HealthUnknown {
			t.Fatalf("datetime %q: HealthOf = %q, want unknown (a datetime that cannot be read)", bad, got)
		}
	}
}

// The heartbeat's tests order events by channel handshakes, never by sleeps: a receive that times
// out is a failure of the test, not a pause of it.

func TestHeartbeatTicksOnInterval(t *testing.T) {
	ticks := make(chan struct{}, 16)
	h := newHeartbeat(20*time.Millisecond, func() error { ticks <- struct{}{}; return nil })
	var wg sync.WaitGroup
	h.start(&wg)

	got := 0
	for i := 0; i < 3; i++ {
		select {
		case <-ticks:
			got++
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for tick %d", i+1)
		}
	}
	h.stop()
	wg.Wait() // the loop ended: from here on no tick can arrive, nothing is armed (D-9)
	if got != 3 {
		t.Fatalf("got %d ticks, want 3", got)
	}
}

func TestHeartbeatSurvivesTickError(t *testing.T) {
	var mu sync.Mutex
	var calls int
	ticks := make(chan struct{}, 16)
	h := newHeartbeat(20*time.Millisecond, func() error {
		mu.Lock()
		first := calls == 0
		calls++
		mu.Unlock()
		ticks <- struct{}{}
		if first {
			return errors.New("a transient publish failure")
		}
		return nil
	})
	var wg sync.WaitGroup
	h.start(&wg)
	// The errored tick and the two that follow it: the error did not stop the chain.
	for i := 0; i < 3; i++ {
		select {
		case <-ticks:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for tick %d: the tick error stopped the chain", i+1)
		}
	}
	h.stop()
	wg.Wait()
}

func TestHeartbeatStopIdempotentAndEndsLoop(t *testing.T) {
	h := newHeartbeat(20*time.Millisecond, func() error { return nil })
	h.stop() // before start: legal
	h.stop() // idempotent

	ticks := make(chan struct{}, 16)
	h2 := newHeartbeat(20*time.Millisecond, func() error { ticks <- struct{}{}; return nil })
	var wg sync.WaitGroup
	h2.start(&wg)
	select {
	case <-ticks:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first tick")
	}
	h2.stop()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the loop did not end after stop")
	}
	h2.stop() // idempotent after the loop has ended
}

// TestHeartbeatStartTwiceAddsNoTicks: a second start on a running heartbeat adds no goroutine and
// no ticks — the stream stays one tick per interval. The reviewer's shape: at a 10 ms interval,
// one start gives ~5–6 ticks in 60 ms, an unguarded second start ~11. The ticks are received over
// a channel (the handshake, no sleeps ordering events), and the assertion is on the rate, so a
// slow machine slows both windows equally.
func TestHeartbeatStartTwiceAddsNoTicks(t *testing.T) {
	const interval = 10 * time.Millisecond
	ticks := make(chan struct{}, 32)
	h := newHeartbeat(interval, func() error { ticks <- struct{}{}; return nil })
	var wg sync.WaitGroup
	h.start(&wg)

	recv := func(n int, what string) {
		for i := 0; i < n; i++ {
			select {
			case <-ticks:
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for tick %d (%s)", i+1, what)
			}
		}
	}
	recv(2, "warming up")

	t0 := time.Now()
	recv(6, "one start")
	one := time.Since(t0)

	h.start(&wg) // the misuse the guard must swallow

	t1 := time.Now()
	recv(6, "after the second start")
	two := time.Since(t1)

	h.stop()
	wg.Wait()

	// One ticker: both windows are stretches of the same stream and take the same time. A second
	// goroutine would double the rate and halve the second window; 3/5 leaves it on its side.
	if two < one*3/5 {
		t.Fatalf("six ticks after a second start took %v where one start took %v: the stream ticked faster (a second goroutine)", two, one)
	}
}

// TestHeartbeatStartAfterStopDoesNothing: after a stop — the heartbeat's own, or the agent's — a
// start arms nothing and produces no ticks (D-9). The 100 ms is ten intervals: the absence check
// is a bounded detector, not an ordering pause.
func TestHeartbeatStartAfterStopDoesNothing(t *testing.T) {
	run := func(ownStop bool) {
		name := "after the agent's stop"
		if ownStop {
			name = "after stop()"
		}
		ticks := make(chan struct{}, 8)
		h := newHeartbeat(10*time.Millisecond, func() error { ticks <- struct{}{}; return nil })
		var wg sync.WaitGroup
		if ownStop {
			h.stop()
			h.start(&wg) // must do nothing
		} else {
			h.start(&wg)
			select {
			case <-ticks:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for the first tick")
			}
			h.stop()
			wg.Wait()
			h.start(&wg) // the loop already ended with the agent's stop: must do nothing
		}
		select {
		case <-ticks:
			t.Fatalf("a start %s armed a ticker", name)
		case <-time.After(100 * time.Millisecond):
		}
		wg.Wait() // nothing tracked to wait for
	}
	run(true)
	run(false)
}
