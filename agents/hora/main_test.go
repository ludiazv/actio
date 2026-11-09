// main_test.go pins run's start failures (D-3, D-12; Hora.md,
// Configuration): each one names its reason, and none leaves a
// half-started state behind. It also pins fire's lines and the stop
// (D-11, D-12): the skip, the no-firing-after-stop, the cancelled fire
// of a real child, and the TERM-immediately row.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"actio-go"
)

// validConfig passes loadConfig; its db file need not exist, because a
// missing file is an empty DB.
const validConfig = `{"agent_id":"hora-test","model":"m","url":"http://127.0.0.1:1","db":"db.json","env_apikey":""}`

func TestRunStartFailure(t *testing.T) {
	rows := []struct {
		name    string
		config  string // "" leaves the config file unwritten
		db      string // content to write to the db file, "" for none
		wantErr string
	}{
		{
			name:    "a bad config names the key",
			config:  `{"agent_id":"hora-test"}`,
			wantErr: "model",
		},
		{
			name:    "a missing config file",
			wantErr: "no such file",
		},
		{
			name:    "a db whose directory does not exist",
			config:  `{"agent_id":"hora-test","model":"m","url":"http://127.0.0.1:1","db":"nodir/db.json","env_apikey":""}`,
			wantErr: "db",
		},
		{
			name:    "an unparseable db",
			config:  validConfig,
			db:      "not a json array",
			wantErr: "not a JSON array",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("ACTIO_DIR", dir)
			before := runtime.NumGoroutine()
			configPath := filepath.Join(dir, "config.json")
			if row.config != "" {
				if err := os.WriteFile(configPath, []byte(row.config), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if row.db != "" {
				if err := os.WriteFile(filepath.Join(dir, "db.json"), []byte(row.db), 0600); err != nil {
					t.Fatal(err)
				}
			}
			err := run(context.Background(), configPath)
			if err == nil {
				t.Fatal("run succeeded, want an error")
			}
			if !strings.Contains(err.Error(), row.wantErr) {
				t.Fatalf("error %q does not contain %q", err, row.wantErr)
			}
			// No half-started state: the temp root holds only the files the
			// row wrote, and no goroutine the run left behind.
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				if name := e.Name(); name != "config.json" && name != "db.json" {
					t.Fatalf("unexpected %q left in the temp root", name)
				}
			}
			settled(t, before)
			if row.db != "" {
				data, err := os.ReadFile(filepath.Join(dir, "db.json"))
				if err != nil {
					t.Fatal(err)
				}
				if string(data) != row.db {
					t.Fatalf("db file rewritten to %q", data)
				}
			}
		})
	}
}

// settled polls, with a real-time deadline, until the goroutine count is
// back at the baseline the row measured before run.
func settled(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline+2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("goroutine count %d did not settle to %d", runtime.NumGoroutine(), baseline)
}

// logLines returns the agent's diarium log messages, in file order.
func logLines(t *testing.T, root, id string) []string {
	t.Helper()
	read, err := actio.ReadDiarium(filepath.Join(root, id))
	if err != nil {
		t.Fatalf("ReadDiarium(%s): %v", id, err)
	}
	var out []string
	for _, l := range read.Lines {
		if l.Event == "log" {
			var msg string
			if json.Unmarshal(l.EventData, &msg) == nil {
				out = append(out, msg)
			}
		}
	}
	return out
}

// waitLogLine polls, with a deadline, for a diarium log line that matches.
func waitLogLine(t *testing.T, root, id, what string, match func(string) bool) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, l := range logLines(t, root, id) {
			if match(l) {
				return l
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("deadline: %s never appeared in %s's diarium", what, id)
	return ""
}

// waitFor polls, with a deadline, until f is true.
func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("deadline: %s", what)
}

// buildHoraBinary builds the hora binary under the temp root.
func buildHoraBinary(t *testing.T, root string) string {
	t.Helper()
	bin := filepath.Join(root, "bin", "hora")
	if err := os.MkdirAll(filepath.Dir(bin), 0700); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v: %s", err, out)
	}
	return bin
}

// newFireHora starts an in-process hora under root with its three
// capabilities published, and returns it.
func newFireHora(t *testing.T, root string) *hora {
	t.Helper()
	var h *hora
	newTestAgent(t, root, "hora", "the test hora", func(a *actio.Agent) {
		h = &hora{
			cfg:    Config{Loc: time.UTC},
			agent:  a,
			store:  mustStore(t),
			models: newModels(Config{Loc: time.UTC}),
			now:    time.Now,
		}
		h.run = newRunner(h.fire)
		if err := h.publish(a); err != nil {
			t.Fatalf("publish: %v", err)
		}
	})
	return h
}

// stopHora ends the in-process hora: cancel the firing, then stop the runner.
func stopHora(t *testing.T, h *hora) {
	t.Helper()
	h.cancelFiring()
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.run.stop(sctx); err != nil {
		t.Errorf("run.stop: %v", err)
	}
}

// TestFireSkip runs a periodic entry on a destination that holds its first
// call: the next firing writes the skip line and the destination's handler
// is not called again.
func TestFireSkip(t *testing.T) {
	t.Setenv("ACTIO_HEARTBEAT", "1")
	t.Setenv("ACTIO_CONFIRM_TIMEOUT", "2")
	t.Setenv("ACTIO_CANCEL_WAIT", "1")
	root := t.TempDir()
	var calls int32
	release := make(chan struct{})
	newTestAgent(t, root, "dest", "the test destination", func(a *actio.Agent) {
		cap := actio.Capability{
			Name:        "slow",
			Description: "waits until released",
			Parameters:  json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
			Actio:       actio.CapabilityActio{Result: json.RawMessage(`{"type":"object"}`), MaxParallel: 1},
		}
		if err := a.Capability(cap, func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
			atomic.AddInt32(&calls, 1)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return map[string]any{}, nil
		}); err != nil {
			t.Fatalf("the destination's slow: %v", err)
		}
	})
	h := newFireHora(t, root)
	see(t, h.agent, "dest")
	entry, err := h.store.add(Entry{
		Datetime:             time.Now().UTC().Format(time.RFC3339),
		SchedType:            "periodic",
		SchedExpression:      "@every 1s",
		DestID:               "dest",
		DestCapability:       "slow",
		CapabilityParameters: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("store.add: %v", err)
	}
	if err := h.run.add(entry); err != nil {
		t.Fatalf("run.add: %v", err)
	}
	h.run.start()
	t.Cleanup(func() { stopHora(t, h) })

	// The first firing reaches the destination's handler, which holds it.
	waitFor(t, "the first call to reach the handler", func() bool {
		return atomic.LoadInt32(&calls) >= 1
	})
	// The next firing is skipped: the line, and no second handler call.
	waitLogLine(t, root, "hora", "the skip line", func(s string) bool {
		return s == "skip "+entry.SchedID+": previous call still running"
	})
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("the destination's handler ran %d times, want exactly 1 (no Request behind the skip)", got)
	}
	// Release the first call; its line lands.
	close(release)
	waitLogLine(t, root, "hora", "the fire line", func(s string) bool {
		return s == "fire "+entry.SchedID+" dest/slow: confirmed"
	})
}

// TestFireNoFiringAfterStop calls cancelFiring, then drives fire directly:
// no Request, no diarium line.
func TestFireNoFiringAfterStop(t *testing.T) {
	t.Setenv("ACTIO_HEARTBEAT", "1")
	t.Setenv("ACTIO_CONFIRM_TIMEOUT", "2")
	t.Setenv("ACTIO_CANCEL_WAIT", "1")
	root := t.TempDir()
	var calls int32
	newTestAgent(t, root, "dest", "the test destination", func(a *actio.Agent) {
		cap := actio.Capability{
			Name:        "tick",
			Description: "counts its calls",
			Parameters:  json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
			Actio:       actio.CapabilityActio{Result: json.RawMessage(`{"type":"object"}`)},
		}
		if err := a.Capability(cap, func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
			atomic.AddInt32(&calls, 1)
			return map[string]any{}, nil
		}); err != nil {
			t.Fatalf("the destination's tick: %v", err)
		}
	})
	h := newFireHora(t, root)
	see(t, h.agent, "dest")
	entry, err := h.store.add(Entry{
		Datetime:             time.Now().UTC().Format(time.RFC3339),
		SchedType:            "periodic",
		SchedExpression:      "@every 1s",
		DestID:               "dest",
		DestCapability:       "tick",
		CapabilityParameters: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("store.add: %v", err)
	}
	h.cancelFiring()
	h.fire(entry.SchedID)
	h.fire(entry.SchedID)
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("the destination's handler ran %d times after the stop, want 0 (no Request)", got)
	}
	for _, l := range logLines(t, root, "hora") {
		if strings.Contains(l, entry.SchedID) {
			t.Fatalf("diarium line after the stop: %q", l)
		}
	}
}

// TestFireLines pins the two remaining line texts: the error answer and the
// request failure, driven as direct fires.
func TestFireLines(t *testing.T) {
	t.Setenv("ACTIO_HEARTBEAT", "1")
	t.Setenv("ACTIO_CONFIRM_TIMEOUT", "2")
	t.Setenv("ACTIO_CANCEL_WAIT", "1")
	root := t.TempDir()
	newTestAgent(t, root, "dest", "the test destination", func(a *actio.Agent) {
		cap := actio.Capability{
			Name:        "fail",
			Description: "always fails with code 1000",
			Parameters:  json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
			Actio:       actio.CapabilityActio{Result: json.RawMessage(`true`)},
		}
		if err := a.Capability(cap, func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
			return nil, &actio.Error{Code: 1000, Message: "fail: requested failure"}
		}); err != nil {
			t.Fatalf("the destination's fail: %v", err)
		}
		cap2 := actio.Capability{
			Name:        "nocode",
			Description: "fails with no code",
			Parameters:  json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
			Actio:       actio.CapabilityActio{Result: json.RawMessage(`true`)},
		}
		if err := a.Capability(cap2, func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
			return nil, errors.New("no code here")
		}); err != nil {
			t.Fatalf("the destination's nocode: %v", err)
		}
	})
	h := newFireHora(t, root)
	see(t, h.agent, "dest")
	add := func(destID, capName string) string {
		t.Helper()
		e, err := h.store.add(Entry{
			Datetime:             time.Now().UTC().Format(time.RFC3339),
			SchedType:            "periodic",
			SchedExpression:      "@every 1h",
			DestID:               destID,
			DestCapability:       capName,
			CapabilityParameters: json.RawMessage(`{}`),
		})
		if err != nil {
			t.Fatalf("store.add: %v", err)
		}
		return e.SchedID
	}

	t.Run("an error answer", func(t *testing.T) {
		id := add("dest", "fail")
		h.fire(id)
		waitLogLine(t, root, "hora", "the error line", func(s string) bool {
			return s == "fire "+id+" dest/fail: confirmed error 1000 fail: requested failure"
		})
	})
	t.Run("an error answer with no code", func(t *testing.T) {
		id := add("dest", "nocode")
		h.fire(id)
		waitLogLine(t, root, "hora", "the no-code line", func(s string) bool {
			return s == "fire "+id+" dest/nocode: confirmed error - no code here"
		})
	})
	t.Run("a request failure", func(t *testing.T) {
		id := add("ghost", "fail")
		h.fire(id)
		waitLogLine(t, root, "hora", "the failed line", func(s string) bool {
			return strings.HasPrefix(s, "fire "+id+" ghost/fail: failed: ")
		})
	})
}

// TestFireCancelledByStop runs the real binary as a child with an entry on
// the slow sleep: TERM mid-call cancels the call, the diarium carries the
// fire's line with the cancelled status, and the child exits 0.
func TestFireCancelledByStop(t *testing.T) {
	t.Setenv("ACTIO_HEARTBEAT", "1")
	t.Setenv("ACTIO_CONFIRM_TIMEOUT", "2")
	t.Setenv("ACTIO_CANCEL_WAIT", "1")
	root := t.TempDir()
	var started, ended int64
	running := make(chan struct{})
	newTestAgent(t, root, "dest", "the test destination", func(a *actio.Agent) {
		cap := actio.Capability{
			Name:        "sleep",
			Description: "waits the given seconds and reports them; honours cancellation",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"seconds":{"type":"number"}},"required":["seconds"],"additionalProperties":false}`),
			Actio:       actio.CapabilityActio{Result: json.RawMessage(`{"type":"object","properties":{"slept":{"type":"number"}},"required":["slept"]}`), MaxParallel: 1},
		}
		if err := a.Capability(cap, func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
			var p struct {
				Seconds float64 `json:"seconds"`
			}
			_ = json.Unmarshal(params, &p)
			atomic.StoreInt64(&started, time.Now().UnixNano())
			running <- struct{}{}
			defer func() {
				atomic.StoreInt64(&ended, time.Now().UnixNano())
				<-running
			}()
			select {
			case <-time.After(time.Duration(p.Seconds) * time.Second):
			case <-ctx.Done():
			}
			return map[string]any{"slept": p.Seconds}, nil
		}); err != nil {
			t.Fatalf("the destination's sleep: %v", err)
		}
	})
	bin := buildHoraBinary(t, root)
	const id = "SCH-00000001"
	db := `[{"datetime":"2026-11-01T00:00:00Z","sched_id":"` + id + `","sched_type":"periodic","sched_expression":"@every 2s","dest_id":"dest","dest_capability":"sleep","capability_parameters":{"seconds":30}}]`
	cfg := `{"agent_id":"hora","model":"m","url":"http://127.0.0.1:1","db":"` + filepath.Join(root, "db.json") + `","env_apikey":""}`
	if err := os.WriteFile(filepath.Join(root, "db.json"), []byte(db), 0600); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(root, "config.json")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-config", cfgPath)
	cmd.Env = append(os.Environ(),
		"ACTIO_DIR="+root,
		"ACTIO_HEARTBEAT=1", "ACTIO_CONFIRM_TIMEOUT=2", "ACTIO_CANCEL_WAIT=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the child: %v", err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	// The call is in flight: TERM it mid-call.
	waitFor(t, "the call to be in flight", func() bool {
		select {
		case <-running:
			return true
		default:
			return false
		}
	})
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("term the child: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(35 * time.Second):
		t.Fatalf("deadline: the child did not stop within the bound; stderr: %s", stderr.String())
	}
	if code := cmd.ProcessState.ExitCode(); code != 0 {
		t.Fatalf("exit %d, want 0; stderr: %s", code, stderr.String())
	}
	waitLogLine(t, root, "hora", "the fire's line with the cancelled status", func(s string) bool {
		return s == "fire "+id+" dest/sleep: cancelled" || s == "fire "+id+" dest/sleep: abandoned"
	})
	// The cancellation reached the destination at once: the handler was
	// woken by the cancel, not by its own timer.
	if d := time.Duration(atomic.LoadInt64(&ended) - atomic.LoadInt64(&started)); d >= 10*time.Second {
		t.Fatalf("the destination's handler ran %v, want a cancelled call well under its 30 s", d)
	}
}

// TestTermImmediately sends TERM as soon as the child is guaranteed to run
// our code, twenty times: the agent folder appears inside the start, after
// the handler is in and before the agent is up, so every one of them
// exits 0.
func TestTermImmediately(t *testing.T) {
	root := t.TempDir()
	bin := buildHoraBinary(t, root)
	cfg := `{"agent_id":"hora","model":"m","url":"http://127.0.0.1:1","db":"` + filepath.Join(root, "db.json") + `","env_apikey":""}`
	cfgPath := filepath.Join(root, "config.json")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		dir := t.TempDir()
		cmd := exec.Command(bin, "-config", cfgPath)
		cmd.Env = append(os.Environ(),
			"ACTIO_DIR="+dir,
			"ACTIO_HEARTBEAT=1", "ACTIO_CONFIRM_TIMEOUT=2", "ACTIO_CANCEL_WAIT=1")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("iteration %d: start: %v", i, err)
		}
		waitFor(t, "the child's agent folder", func() bool {
			_, err := os.Stat(filepath.Join(dir, "hora"))
			return err == nil
		})
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("iteration %d: term: %v", i, err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(35 * time.Second):
			t.Fatalf("iteration %d: deadline: the child did not stop; stderr: %s", i, stderr.String())
		}
		if code := cmd.ProcessState.ExitCode(); code != 0 {
			t.Fatalf("iteration %d: exit %d, want 0; stderr: %s", i, code, stderr.String())
		}
	}
}

// TestCapabilitiesPublished checks that run's registration publishes the
// three capabilities: a second agent's view of hora carries list, set and
// unset.
func TestCapabilitiesPublished(t *testing.T) {
	t.Setenv("ACTIO_HEARTBEAT", "1")
	t.Setenv("ACTIO_CONFIRM_TIMEOUT", "2")
	t.Setenv("ACTIO_CANCEL_WAIT", "1")
	root := t.TempDir()
	newFireHora(t, root)
	watcher := newTestAgent(t, root, "watcher", "the watcher", nil)
	see(t, watcher, "hora")
	views, err := watcher.Refresh(context.Background())
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	caps := views[actio.AgentID("hora")].Capabilities
	for _, name := range []string{"list", "set", "unset"} {
		if _, ok := caps[name]; !ok {
			t.Fatalf("the published capabilities lack %s (have %v)", name, caps)
		}
	}
}

// TestExitCodes re-checks the two exit codes from the built binary: usage
// is 2, a config error is 1 and names the key.
func TestExitCodes(t *testing.T) {
	root := t.TempDir()
	bin := buildHoraBinary(t, root)

	cmd := exec.Command(bin)
	if err := cmd.Run(); err == nil {
		t.Fatal("no -config: the child did not fail, want a usage error")
	}
	if code := cmd.ProcessState.ExitCode(); code != 2 {
		t.Fatalf("no -config: exit %d, want 2", code)
	}

	bad := filepath.Join(root, "bad.json")
	if err := os.WriteFile(bad, []byte(`{"agent_id":"hora"}`), 0600); err != nil {
		t.Fatal(err)
	}
	cmd = exec.Command(bin, "-config", bad)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatal("a bad config: the child did not fail")
	}
	if code := cmd.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("a bad config: exit %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "model") {
		t.Fatalf("a bad config: stderr %q does not name the key", stderr.String())
	}
}

// TestRunStopsOnContext cancels run's context while a slow call is in
// flight: the full stop runs — the fire's diarium line included — and run
// returns nil.
func TestRunStopsOnContext(t *testing.T) {
	t.Setenv("ACTIO_HEARTBEAT", "1")
	t.Setenv("ACTIO_CONFIRM_TIMEOUT", "2")
	t.Setenv("ACTIO_CANCEL_WAIT", "1")
	root := t.TempDir()
	t.Setenv("ACTIO_DIR", root)
	running := make(chan struct{})
	newTestAgent(t, root, "dest", "the test destination", func(a *actio.Agent) {
		cap := actio.Capability{
			Name:        "sleep",
			Description: "waits the given seconds and reports them; honours cancellation",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"seconds":{"type":"number"}},"required":["seconds"],"additionalProperties":false}`),
			Actio:       actio.CapabilityActio{Result: json.RawMessage(`{"type":"object","properties":{"slept":{"type":"number"}},"required":["slept"]}`), MaxParallel: 1},
		}
		if err := a.Capability(cap, func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
			var p struct {
				Seconds float64 `json:"seconds"`
			}
			_ = json.Unmarshal(params, &p)
			running <- struct{}{}
			defer func() { <-running }()
			select {
			case <-time.After(time.Duration(p.Seconds) * time.Second):
			case <-ctx.Done():
			}
			return map[string]any{"slept": p.Seconds}, nil
		}); err != nil {
			t.Fatalf("the destination's sleep: %v", err)
		}
	})
	const id = "SCH-00000002"
	db := `[{"datetime":"2026-11-01T00:00:00Z","sched_id":"` + id + `","sched_type":"periodic","sched_expression":"@every 1s","dest_id":"dest","dest_capability":"sleep","capability_parameters":{"seconds":30}}]`
	if err := os.WriteFile(filepath.Join(root, "db.json"), []byte(db), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := `{"agent_id":"hora","model":"m","url":"http://127.0.0.1:1","db":"` + filepath.Join(root, "db.json") + `","env_apikey":""}`
	cfgPath := filepath.Join(root, "config.json")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfgPath) }()
	consumed := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-consumed:
		case <-done:
		case <-time.After(40 * time.Second):
			t.Errorf("deadline: run did not return within 40 s of the cancel")
		}
	})
	// The slow call is in flight: cancel run's context.
	waitFor(t, "the call to be in flight", func() bool {
		select {
		case <-running:
			return true
		default:
			return false
		}
	})
	cancel()
	running <- struct{}{} // release the handler's deferred <-running so its completion is sent
	// The full stop ran: run returns nil, and the fire's line is in the diarium.
	select {
	case err := <-done:
		close(consumed)
		if err != nil {
			t.Fatalf("run returned %v on the ctx stop, want nil", err)
		}
	case <-time.After(40 * time.Second):
		t.Fatalf("deadline: run did not return within 40 s of the cancel")
	}
	waitFor(t, "the fire's line", func() bool {
		for _, l := range logLines(t, root, "hora") {
			if l == "fire "+id+" dest/sleep: cancelled" {
				return true
			}
		}
		return false
	})
}

// TestOneShotDueDuringStart is the one-shot that falls due while the start
// is running: the runner loads before the agent starts, so the entry is
// not re-parsed as past — it fires once, late, and run does not fail.
func TestOneShotDueDuringStart(t *testing.T) {
	t.Setenv("ACTIO_HEARTBEAT", "1")
	t.Setenv("ACTIO_CONFIRM_TIMEOUT", "2")
	t.Setenv("ACTIO_CANCEL_WAIT", "1")
	root := t.TempDir()
	t.Setenv("ACTIO_DIR", root)
	var calls int32
	newTestAgent(t, root, "dest", "the test destination", func(a *actio.Agent) {
		cap := actio.Capability{
			Name:        "tick",
			Description: "counts its calls",
			Parameters:  json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
			Actio:       actio.CapabilityActio{Result: json.RawMessage(`{"type":"object"}`)},
		}
		if err := a.Capability(cap, func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
			atomic.AddInt32(&calls, 1)
			return map[string]any{}, nil
		}); err != nil {
			t.Fatalf("the destination's tick: %v", err)
		}
	})
	// The one-shot is due at the next whole second, inside the start.
	for time.Now().Nanosecond() < 960e6 {
		time.Sleep(2 * time.Millisecond)
	}
	due := time.Now().Truncate(time.Second).Add(time.Second)
	const id = "SCH-00000003"
	db := `[{"datetime":"2026-11-01T00:00:00Z","sched_id":"` + id + `","sched_type":"one-shot","sched_expression":"` + due.UTC().Format("2006-01-02T15:04:05Z") + `","dest_id":"dest","dest_capability":"tick","capability_parameters":{}}]`
	if err := os.WriteFile(filepath.Join(root, "db.json"), []byte(db), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := `{"agent_id":"hora","model":"m","url":"http://127.0.0.1:1","db":"` + filepath.Join(root, "db.json") + `","env_apikey":""}`
	cfgPath := filepath.Join(root, "config.json")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfgPath) }()
	consumed := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-consumed:
		case <-done:
		case <-time.After(40 * time.Second):
			t.Errorf("deadline: run did not return within 40 s of the cancel")
		}
	})
	// The one-shot fired: the destination saw it.
	waitFor(t, "the one-shot to fire", func() bool {
		return atomic.LoadInt32(&calls) >= 1
	})
	// run did not fail the start: it is still running.
	select {
	case err := <-done:
		close(consumed)
		t.Fatalf("run failed at the start: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		close(consumed)
		if err != nil {
			t.Fatalf("run returned %v after the cancel, want nil", err)
		}
	case <-time.After(40 * time.Second):
		t.Fatalf("deadline: run did not return within 40 s of the cancel")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("the destination's handler ran %d times, want exactly 1", got)
	}
}

// agentStatus is the part of status.json the stop row reads.
type agentStatus struct {
	Status   string `json:"status"`
	Datetime string `json:"datetime"`
}

// writeTestFile writes one file, failing the test on error.
func writeTestFile(t *testing.T, path, content string, mode os.FileMode) {
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

// TestRunStopBound is the stop-bounds table: a destination that ignores
// cancellation, the bound shortened for the test. The rows share
// everything but the stub mutagen: without it, the runner uses up its
// bound and the agent stops on its fresh one, so run returns nil; with
// it, the post-inactive sync flush (a 5 s sleep) outlasts the agent's
// fresh bound, so run returns the stop's error. In both, the status is
// inactive, and the in-flight fire's line, if it lands, is abandoned —
// never after the inactive.
func TestRunStopBound(t *testing.T) {
	t.Setenv("ACTIO_HEARTBEAT", "1")
	t.Setenv("ACTIO_CONFIRM_TIMEOUT", "2")
	// The cancel wait keeps its 60 s default: twice the bound.
	oldBound := stopBound
	stopBound = 3 * time.Second
	defer func() { stopBound = oldBound }()
	for _, row := range []struct {
		name    string
		stub    bool // the stub mutagen stalls the post-inactive flush past the bound
		wantErr bool
	}{
		{"the agent stops after the runner's bound", false, false},
		{"a stop that cannot stop the agent fails", true, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			root := t.TempDir()
			t.Setenv("ACTIO_DIR", root)
			if row.stub {
				// The stub sleeps 5 s: past the 3 s bound, inside the test agents' 10 s.
				writeTestFile(t, filepath.Join(root, "mutagen"), "#!/bin/sh\nsleep 5\n", 0700)
				t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
				t.Setenv("ACTIO_SYNC", "mutagen:wp8a")
			}
			var held int32
			newTestAgent(t, root, "dest", "the test destination", func(a *actio.Agent) {
				cap := actio.Capability{
					Name:       "hold",
					Parameters: json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
					Actio:      actio.CapabilityActio{Result: json.RawMessage(`{"type":"object"}`), MaxParallel: 1},
				}
				if err := a.Capability(cap, func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
					atomic.StoreInt32(&held, 1)
					time.Sleep(90 * time.Second) // ignores ctx
					return map[string]any{}, nil
				}); err != nil {
					t.Fatalf("the destination's hold: %v", err)
				}
			})
			const id = "SCH-00000004"
			writeTestFile(t, filepath.Join(root, "db.json"), `[{"datetime":"2026-11-01T00:00:00Z","sched_id":"`+id+`","sched_type":"periodic","sched_expression":"@every 2s","dest_id":"dest","dest_capability":"hold","capability_parameters":{}}]`, 0600)
			cfgPath := filepath.Join(root, "config.json")
			writeTestFile(t, cfgPath, validConfig, 0600)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- run(ctx, cfgPath) }()
			t.Cleanup(cancel)
			// The call is in flight: cancel run's context.
			waitFor(t, "the call to be in flight", func() bool { return atomic.LoadInt32(&held) == 1 })
			cancelAt := time.Now()
			cancel()
			var err error
			select {
			case err = <-done:
			case <-time.After(2*stopBound + 10*time.Second):
				t.Fatalf("deadline: run did not return within two bounds plus slack of the cancel")
			}
			t.Logf("the stop took %v with a %v bound per phase", time.Since(cancelAt), stopBound)
			if row.wantErr {
				// The stop failed: the agent could not be stopped within its bound.
				if err == nil {
					t.Fatal("run returned nil, want the stop's error")
				}
				if !strings.Contains(err.Error(), "stop") {
					t.Fatalf("the error %q does not name the stop failure", err)
				}
			} else if err != nil {
				t.Fatalf("run returned %v, want nil", err)
			}
			// The status is inactive: published before the stop failed.
			var st agentStatus
			data, rerr := os.ReadFile(filepath.Join(root, "hora-test", "status.json"))
			if rerr != nil || json.Unmarshal(data, &st) != nil {
				t.Fatalf("status.json: %v", rerr)
			}
			if st.Status != "inactive" {
				t.Fatalf("the status is %q, want inactive", st.Status)
			}
			// The fire line, if it lands, is abandoned — and never after the
			// inactive status.
			read, rerr := actio.ReadDiarium(filepath.Join(root, "hora-test"))
			if rerr != nil {
				t.Fatalf("ReadDiarium(hora-test): %v", rerr)
			}
			for _, l := range read.Lines {
				var msg string
				if l.Event != "log" || json.Unmarshal(l.EventData, &msg) != nil || !strings.HasPrefix(msg, "fire "+id+" ") {
					continue
				}
				if !strings.HasSuffix(msg, "abandoned") {
					t.Fatalf("the fire line is %q, want abandoned", msg)
				}
				if l.Datetime > st.Datetime {
					t.Fatalf("the fire line %s landed after the inactive status %s", l.Datetime, st.Datetime)
				}
			}
		})
	}
}

// wireTimeout reads the destination's diarium for the inreq line of a call
// of the named capability and returns the timeout (seconds) on the wire:
// event_data -> req -> request.timeout.
func wireTimeout(root, id, capName string) (float64, bool) {
	read, err := actio.ReadDiarium(filepath.Join(root, id))
	if err != nil {
		return 0, false
	}
	for _, l := range read.Lines {
		if l.Event != "inreq" {
			continue
		}
		var ex actio.ExchangeEnvelopes
		if json.Unmarshal(l.EventData, &ex) != nil {
			continue
		}
		var rf actio.RequestFile
		if json.Unmarshal(ex.Req, &rf) != nil {
			continue
		}
		var body actio.RequestBody
		if json.Unmarshal(rf.Request, &body) != nil || body.Name != capName {
			continue
		}
		return body.Timeout, true
	}
	return 0, false
}

// TestFireWireTimeout is D-11's timeout on the wire: the destination's
// inreq line carries the timeout hora put on the request — the
// capability's recommended_timeout when declared, 300 s when not.
func TestFireWireTimeout(t *testing.T) {
	t.Setenv("ACTIO_HEARTBEAT", "1")
	t.Setenv("ACTIO_CONFIRM_TIMEOUT", "2")
	root := t.TempDir()
	newTestAgent(t, root, "dest", "the test destination", func(a *actio.Agent) {
		ok := func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
			return map[string]any{}, nil
		}
		tick := func(name string, rt float64) {
			cap := actio.Capability{
				Name:       name,
				Parameters: json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
				Actio:      actio.CapabilityActio{Result: json.RawMessage(`{"type":"object"}`), RecommendedTimeout: rt},
			}
			if err := a.Capability(cap, ok); err != nil {
				t.Fatalf("the destination's %s: %v", name, err)
			}
		}
		tick("tick7", 7)
		tick("ticknone", 0)
	})
	h := newFireHora(t, root)
	see(t, h.agent, "dest")
	t.Cleanup(func() { stopHora(t, h) })
	add := func(capName string) string {
		e, err := h.store.add(Entry{
			Datetime:             time.Now().UTC().Format(time.RFC3339),
			SchedType:            "periodic",
			SchedExpression:      "@every 1h",
			DestID:               "dest",
			DestCapability:       capName,
			CapabilityParameters: json.RawMessage(`{}`),
		})
		if err != nil {
			t.Fatalf("store.add: %v", err)
		}
		return e.SchedID
	}
	rows := []struct {
		capName string
		want    float64
	}{
		{"tick7", 7},
		{"ticknone", 300},
	}
	for _, row := range rows {
		t.Run(row.capName, func(t *testing.T) {
			id := add(row.capName)
			h.fire(id)
			var got float64
			waitFor(t, "the inreq line for "+row.capName, func() bool {
				v, ok := wireTimeout(root, "dest", row.capName)
				got = v
				return ok
			})
			if got != row.want {
				t.Fatalf("the wire timeout is %v s, want %v", got, row.want)
			}
		})
	}
}
