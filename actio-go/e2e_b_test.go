// The end-to-end suite, part 2: the robustness scenarios E14–E21, through the public API, on
// t.TempDir() roots, with the protocol's timers shrunk. The request/command scenarios E1–E13 are in
// e2e_a_test.go. The interop half is in interop_test.go, behind the interop build tag.
package actio

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// writeJSONFile writes one value as a JSON file under dir, creating dir if needed.
func writeJSONFile(t *testing.T, dir, name string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %s: %v", name, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// e2eHasLogLine reports whether one diarium has a log line containing the substring.
func e2eHasLogLine(t *testing.T, root, id, substring string) bool {
	t.Helper()
	read, err := ReadDiarium(filepath.Join(root, id))
	if err != nil {
		return false
	}
	for _, line := range read.Lines {
		if line.Event != "log" {
			continue
		}
		var msg string
		if err := json.Unmarshal(line.EventData, &msg); err == nil && strings.Contains(msg, substring) {
			return true
		}
	}
	return false
}

// e2eWaitLogLine waits, with the named bound, for a log line containing the substring.
func e2eWaitLogLine(t *testing.T, root, id, substring string) {
	t.Helper()
	waitFor(t, e2eBound, id+"'s diarium to log "+substring, func() bool {
		return e2eHasLogLine(t, root, id, substring)
	})
}

// TestE14MalformedFilesIgnored: a bad-JSON file, a schema violation, and an encrypted body in the
// inbox are all ignored — no response is written and a log line names the file and the reason. The
// reason text is asserted as the Go implementation emits it (the reference's wording differs).
func TestE14MalformedFilesIgnored(t *testing.T) {
	t.Run("bad_json", func(t *testing.T) {
		root := t.TempDir()
		alpha := newE2EAgent(t, root, "alpha", nil)
		if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		mustStart(t, alpha)
		name := "bad.json"
		if err := os.WriteFile(filepath.Join(root, "alpha", "inbox", name), []byte(`{"not": valid json`), 0o644); err != nil {
			t.Fatalf("write the bad file: %v", err)
		}
		e2eWaitLogLine(t, root, "alpha", "ignored "+name)
		e2eAssertNoResponse(t, root, "alpha", name)
	})
	t.Run("schema_violation", func(t *testing.T) {
		root := t.TempDir()
		alpha := newE2EAgent(t, root, "alpha", nil)
		if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		mustStart(t, alpha)
		// A request file with no nonce: valid JSON, a schema violation.
		name := "nononce.json"
		now := time.Now()
		body := json.RawMessage(`{"ts":` + strconv.FormatInt(now.Unix(), 10) + `,"signature":null,"name":"echo","parameters":{},"timeout":60,"attachments":[]}`)
		file := RequestFile{
			Header:  Header{RID: newULID(now), Src: "beta", Dst: "alpha", Datetime: formatISO(now), Type: "req", ActioVersion: ProtocolVersion},
			Request: body,
		}
		raw, _ := json.Marshal(file)
		if err := os.WriteFile(filepath.Join(root, "alpha", "inbox", name), raw, 0o644); err != nil {
			t.Fatalf("write the schema-violating file: %v", err)
		}
		e2eWaitLogLine(t, root, "alpha", "ignored "+name)
		e2eAssertNoResponse(t, root, "alpha", name)
	})
	t.Run("encrypted_body", func(t *testing.T) {
		root := t.TempDir()
		alpha := newE2EAgent(t, root, "alpha", nil)
		if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		mustStart(t, alpha)
		// A request whose body is a base64 string (an encrypted body): ignored in the core profile.
		name := "encrypted.json"
		now := time.Now()
		file := RequestFile{
			Header:  Header{RID: newULID(now), Src: "beta", Dst: "alpha", Datetime: formatISO(now), Type: "req", ActioVersion: ProtocolVersion},
			Request: json.RawMessage(`"aGVsbG8="`),
		}
		raw, _ := json.Marshal(file)
		if err := os.WriteFile(filepath.Join(root, "alpha", "inbox", name), raw, 0o644); err != nil {
			t.Fatalf("write the encrypted file: %v", err)
		}
		e2eWaitLogLine(t, root, "alpha", "ignored "+name)
		e2eAssertNoResponse(t, root, "alpha", name)
	})
}

// e2eAssertNoResponse fails the test if a response file for one request name was written.
func e2eAssertNoResponse(t *testing.T, root, id, reqName string) {
	t.Helper()
	for _, name := range e2eInboxNames(t, root, id) {
		if strings.HasSuffix(name, ".res.json") {
			t.Errorf("a response was written for the ignored file %s: %s", reqName, name)
		}
	}
}

// writeFixtureAgent writes a fixture agent's spec.json and status.json under root, for the
// discovery rows. The mutators tweak the defaults.
func writeFixtureAgent(t *testing.T, root, id string, specMut func(*Spec), statusMut func(*Status)) {
	t.Helper()
	now := time.Now()
	spec := Spec{
		ID:                  AgentID(id),
		ActioVersion:        ProtocolVersion,
		Datetime:            formatISO(now),
		Description:         "fixture",
		AcceptsCommandsFrom: []string{},
		Capabilities:        []Capability{},
		SecuritySupport:     []string{},
		SecurityPosture:     "trusted",
	}
	if specMut != nil {
		specMut(&spec)
	}
	status := Status{
		ID:               AgentID(id),
		Status:           StatusIdle,
		Datetime:         formatISO(now),
		CapabilityStatus: []CapabilityStatus{},
	}
	if statusMut != nil {
		statusMut(&status)
	}
	dir := filepath.Join(root, id)
	writeJSONFile(t, dir, "spec.json", spec)
	writeJSONFile(t, dir, "status.json", status)
}

// TestE15DiscoveryCallability: fixture peers are filtered by the callability rules — a valid peer is
// in the view, a disabled peer is in the view with no capabilities, a no-namespace and an untrusted
// and an inactive peer are out, and a zombie peer is in the view flagged zombie.
func TestE15DiscoveryCallability(t *testing.T) {
	root := t.TempDir()
	echo := e2eEchoCap()
	writeFixtureAgent(t, root, "valid", func(s *Spec) { s.Capabilities = []Capability{echo} }, nil)
	writeFixtureAgent(t, root, "disabled", func(s *Spec) {
		d := true
		s.Disabled = &d
		s.Capabilities = []Capability{echo}
		// A usable command keeps it in the view: rule 1 empties the capabilities, not the commands.
		s.AcceptsCommandsFrom = []string{"alpha"}
	}, nil)
	writeFixtureAgent(t, root, "otherns", func(s *Spec) {
		ns := "other"
		s.Namespaces = []*string{&ns}
		s.Capabilities = []Capability{echo}
	}, nil)
	writeFixtureAgent(t, root, "untrusted", func(s *Spec) {
		s.SecurityPosture = "untrusted"
		s.Capabilities = []Capability{echo}
	}, nil)
	writeFixtureAgent(t, root, "inactive", nil, func(st *Status) { st.Status = StatusInactive })
	writeFixtureAgent(t, root, "zombie", func(s *Spec) {
		s.Capabilities = []Capability{echo} // a usable capability keeps it past rule 6
	}, func(st *Status) {
		st.Datetime = formatISO(time.Now().Add(-700 * time.Second))
	})
	alpha := newE2EAgent(t, root, "alpha", nil)
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	mustStart(t, alpha)
	views, err := alpha.Refresh(startCtx(t))
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if _, ok := views["valid"]; !ok {
		t.Error("the valid peer is not in the view")
	}
	dis, ok := views["disabled"]
	if !ok {
		t.Fatal("the disabled peer is not in the view")
	}
	if len(dis.Capabilities) != 0 {
		t.Errorf("the disabled peer's capabilities are not emptied: %v", dis.Capabilities)
	}
	if _, ok := views["otherns"]; ok {
		t.Error("the no-shared-namespace peer is in the view")
	}
	if _, ok := views["untrusted"]; ok {
		t.Error("the untrusted peer is in the view")
	}
	if _, ok := views["inactive"]; ok {
		t.Error("the inactive peer is in the view")
	}
	zom, ok := views["zombie"]
	if !ok {
		t.Fatal("the zombie peer is not in the view")
	}
	if zom.Health != HealthZombie {
		t.Errorf("the zombie peer's health: %q (want zombie)", zom.Health)
	}
}

// TestE16AttachmentsCleaned: a request attachment is copied into the forum and deleted at B5; a
// response attachment is published by the handler and deleted at A4; the forum is empty after the
// exchange.
func TestE16AttachmentsCleaned(t *testing.T) {
	root := t.TempDir()
	reqAtt := filepath.Join(root, "req-attachment.txt")
	if err := os.WriteFile(reqAtt, []byte("request attachment"), 0o644); err != nil {
		t.Fatalf("write the request attachment: %v", err)
	}
	alpha := newE2EAgent(t, root, "alpha", nil)
	if err := alpha.Capability(e2eEchoCap(), func(ctx context.Context, p json.RawMessage, c *CallContext) (any, error) {
		if len(c.Attachments) != 1 || c.Attachments[0] != "req-attachment.txt" {
			return nil, &Error{Code: 1000, Message: "the request attachment is missing: " + strings.Join(c.Attachments, ",")}
		}
		if _, err := c.PublishAttachment("res-attachment.txt", []byte("response attachment")); err != nil {
			return nil, err
		}
		return map[string]any{"echoed": true}, nil
	}); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	beta := newE2EAgent(t, root, "beta", nil)
	if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	mustStart(t, alpha)
	mustStart(t, beta)
	e2eSee(t, beta, "beta", "alpha")
	out, err := beta.Request(startCtx(t), "alpha", "echo", map[string]any{}, RequestOptions{Timeout: 5 * time.Second, Attachments: []string{reqAtt}})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if out.FinalStatus != "confirmed" {
		t.Fatalf("final status: %q (want confirmed)", out.FinalStatus)
	}
	e2eWaitInboxDrains(t, root, "alpha")
	waitFor(t, e2eBound, "the forum to be empty after the exchange", func() bool {
		entries, err := os.ReadDir(filepath.Join(root, "forum"))
		return err == nil && len(entries) == 0
	})
}

// TestE17PendingEnvelopesAtStartup: a request written into the inbox before the agent starts is
// processed by the D-4 initial scan; an expired one is answered 108.
func TestE17PendingEnvelopesAtStartup(t *testing.T) {
	t.Run("processed", func(t *testing.T) {
		root := t.TempDir()
		// Create beta's inbox and a pending request before beta starts.
		if err := os.MkdirAll(filepath.Join(root, "beta", "inbox"), 0o755); err != nil {
			t.Fatalf("mkdir the inbox: %v", err)
		}
		writeRequestFile(t, root, "beta", "alpha", "echo", 0, 60)
		beta := newE2EAgent(t, root, "beta", nil)
		if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		mustStart(t, beta)
		// The D-4 initial scan processes the pending request and archives it.
		waitFor(t, e2eBound, "beta to archive the pending request", func() bool {
			in, _ := e2eDiariumFinals(t, root, "beta")
			return len(in) >= 1
		})
	})
	t.Run("expired", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "beta", "inbox"), 0o755); err != nil {
			t.Fatalf("mkdir the inbox: %v", err)
		}
		writeRequestFile(t, root, "beta", "alpha", "echo", 1000*time.Second, 1)
		beta := newE2EAgent(t, root, "beta", nil)
		if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		mustStart(t, beta)
		e2eExpectInreqCode(t, root, "beta", 108)
	})
}

// TestE18SelfCall: an agent calling itself works like any other exchange.
func TestE18SelfCall(t *testing.T) {
	root := t.TempDir()
	alpha := newE2EAgent(t, root, "alpha", nil)
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	mustStart(t, alpha)
	out, err := alpha.Request(startCtx(t), "alpha", "echo", map[string]any{"x": 1}, RequestOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Request(self): %v", err)
	}
	if out.FinalStatus != "confirmed" || out.Response == nil {
		t.Fatalf("outcome: %+v", out)
	}
	e2eWaitInboxDrains(t, root, "alpha")
}

// TestE19HeartbeatKeepsStatusFresh: while running, the status's datetime is republished at least
// every heartbeat interval, so it is never older than the interval plus slack.
func TestE19HeartbeatKeepsStatusFresh(t *testing.T) {
	root := t.TempDir()
	// The status datetime has one-second resolution, so the observed age is the heartbeat interval
	// plus at most one second of truncation; the bound below covers both with slack.
	alpha := newE2EAgent(t, root, "alpha", func(o *Options) { o.Config.HeartbeatInterval = time.Second })
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	mustStart(t, alpha)
	// Sample the status's datetime for a few seconds; it must stay fresh (not stale).
	deadline := time.Now().Add(3 * time.Second)
	var oldest time.Duration
	for time.Now().Before(deadline) {
		var s Status
		readJSON(t, filepath.Join(root, "alpha", "status.json"), &s)
		dt, ok := parseISO(s.Datetime)
		if !ok {
			t.Fatalf("the status datetime: %q", s.Datetime)
		}
		age := time.Since(dt)
		if age > oldest {
			oldest = age
		}
		time.Sleep(100 * time.Millisecond)
	}
	if oldest > 3*time.Second {
		t.Errorf("the status datetime was %s old (want at most ~3s: the heartbeat keeps it fresh)", oldest)
	}
}

// TestE20SyncProvidersIdentical: the E1 exchange runs to the same confirmed outcome under the delay,
// fsync+delay and mutagen sync providers. The mutagen provider is a fake binary on ACTIO_MUTAGEN_BIN
// that receives the debounced flushes.
func TestE20SyncProvidersIdentical(t *testing.T) {
	mutagen := t.TempDir() + "/fake-mutagen"
	if err := os.WriteFile(mutagen, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write the fake mutagen: %v", err)
	}
	for _, sync := range []string{"delay:20", "fsync+delay:20", "mutagen:sess"} {
		t.Run(sync, func(t *testing.T) {
			if strings.HasPrefix(sync, "mutagen") {
				t.Setenv(EnvMutagenBin, mutagen)
			}
			root := t.TempDir()
			alpha := newE2EAgent(t, root, "alpha", func(o *Options) { o.Config.Sync = sync })
			if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
				t.Fatalf("Capability: %v", err)
			}
			beta := newE2EAgent(t, root, "beta", nil)
			if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
				t.Fatalf("Capability: %v", err)
			}
			mustStart(t, alpha)
			mustStart(t, beta)
			e2eSee(t, beta, "beta", "alpha")
			out, err := beta.Request(startCtx(t), "alpha", "echo", map[string]any{}, RequestOptions{Timeout: 5 * time.Second})
			if err != nil {
				t.Fatalf("Request: %v", err)
			}
			if out.FinalStatus != "confirmed" || out.Response == nil {
				t.Fatalf("final status: %q (want confirmed)", out.FinalStatus)
			}
			e2eWaitInboxDrains(t, root, "alpha")
		})
	}
}

// TestE21WatchLost: deleting the agent's own inbox reports the loss — a log line, an error status
// and an announce. Deleting the destination's inbox during an in-flight request abandons it. The
// "watch lost" text is asserted in the diarium and the IgnoredEvent, where the Go implementation
// puts it.
func TestE21WatchLost(t *testing.T) {
	t.Run("own_inbox", func(t *testing.T) {
		root := t.TempDir()
		beta := newE2EAgent(t, root, "beta", nil)
		if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		mustStart(t, beta)
		lost := make(chan LogEvent, 1)
		beta.OnEvent(func(e Event) {
			if lg, ok := e.(LogEvent); ok && strings.Contains(lg.Message, "watch lost") {
				lost <- lg
			}
		})
		if err := os.RemoveAll(filepath.Join(root, "beta", "inbox")); err != nil {
			t.Fatalf("remove the inbox: %v", err)
		}
		select {
		case lg := <-lost:
			if !strings.Contains(lg.Message, "watch lost") {
				t.Errorf("the LogEvent message: %q", lg.Message)
			}
		case <-time.After(e2eBound):
			t.Fatal("no watch-lost LogEvent was emitted")
		}
		waitFor(t, e2eBound, "beta's status to turn error", func() bool { return statusState(t, root, "beta") == "error" })
		e2eWaitLogLine(t, root, "beta", "watch lost")
	})
	t.Run("dst_inbox", func(t *testing.T) {
		root := t.TempDir()
		alpha := newE2EAgent(t, root, "alpha", nil)
		if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		slow := e2eEchoCap()
		slow.Name = "sleep"
		beta := newE2EAgent(t, root, "beta", nil)
		if err := beta.Capability(slow, func(ctx context.Context, p json.RawMessage, c *CallContext) (any, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}); err != nil {
			t.Fatalf("Capability(sleep): %v", err)
		}
		mustStart(t, alpha)
		mustStart(t, beta)
		e2eSee(t, alpha, "alpha", "beta")
		// An in-flight request to beta, then beta's inbox is deleted while B2 is watching it.
		inflight := make(chan Outcome, 1)
		go func() {
			out, _ := alpha.Request(startCtx(t), "beta", "sleep", map[string]any{}, RequestOptions{Timeout: 30 * time.Second})
			inflight <- out
		}()
		waitFor(t, e2eBound, "the request to reach beta", func() bool { return statusState(t, root, "beta") == "busy" })
		if err := os.RemoveAll(filepath.Join(root, "beta", "inbox")); err != nil {
			t.Fatalf("remove the destination inbox: %v", err)
		}
		select {
		case out := <-inflight:
			if out.FinalStatus != "abandoned" {
				t.Errorf("final status: %q (want abandoned)", out.FinalStatus)
			}
		case <-time.After(e2eBound):
			t.Fatal("the in-flight request did not abandon")
		}
	})
}
