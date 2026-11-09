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
	})
	h := newFireHora(t, root)
	see(t, h.agent, "dest")
	add := func(destID string) string {
		t.Helper()
		e, err := h.store.add(Entry{
			Datetime:             time.Now().UTC().Format(time.RFC3339),
			SchedType:            "periodic",
			SchedExpression:      "@every 1h",
			DestID:               destID,
			DestCapability:       "fail",
			CapabilityParameters: json.RawMessage(`{}`),
		})
		if err != nil {
			t.Fatalf("store.add: %v", err)
		}
		return e.SchedID
	}

	t.Run("an error answer", func(t *testing.T) {
		id := add("dest")
		h.fire(id)
		waitLogLine(t, root, "hora", "the error line", func(s string) bool {
			return s == "fire "+id+" dest/fail: confirmed error 1000 fail: requested failure"
		})
	})
	t.Run("a request failure", func(t *testing.T) {
		id := add("ghost")
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
