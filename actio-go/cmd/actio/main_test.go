// The CLI's own tests: the argument parsing, the usage errors, and the output shape of ls,
// diarium, call, cmd, broadcast and validate on a fixture root.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"actio-go"
)

// newCLI points ACTIO_DIR at a fresh root and returns a runner: one call to the CLI, with the
// exit code and both streams.
func newCLI(t *testing.T) (string, func(...string) (int, string, string)) {
	t.Helper()
	root := t.TempDir()
	t.Setenv("ACTIO_DIR", root)
	t.Setenv("ACTIO_SYNC", "none")
	return root, func(args ...string) (int, string, string) {
		var out, errb bytes.Buffer
		code := run(args, &out, &errb)
		return code, out.String(), errb.String()
	}
}

// nowISO is the protocol's second-resolution UTC stamp, fresh enough to rate healthy.
func nowISO() string { return time.Now().UTC().Format("2006-01-02T15:04:05Z") }

// writeAgent puts a valid spec.json and status.json under root for id, with a fresh datetime so
// the health rates as healthy.
func writeAgent(t *testing.T, root, id string) string {
	t.Helper()
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(filepath.Join(dir, "inbox"), 0o755); err != nil {
		t.Fatal(err)
	}
	datetime := nowISO()
	spec := `{"id":"` + id + `","actio_version":"0.1","datetime":"` + datetime + `","description":"a test agent","accepts_commands_from":[],"capabilities":[{"name":"echo","description":"echoes the text it gets","parameters":{"type":"object","properties":{},"additionalProperties":false},"actio":{"result":true}}],"security_support":[],"security_posture":"trusted"}`
	status := `{"id":"` + id + `","status":"idle","datetime":"` + datetime + `","capability_status":[]}`
	for name, body := range map[string]string{"spec.json": spec, "status.json": status} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return datetime
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantJSON bool
		wantPos  []string
		wantN    int
		wantTO   time.Duration
		wantAs   string
		wantErr  string
	}{
		{"ls", []string{"ls"}, false, []string{"ls"}, 20, defaultTimeout, "", ""},
		{"json first", []string{"--json", "ls"}, true, []string{"ls"}, 20, defaultTimeout, "", ""},
		{"json after a positional", []string{"ls", "--json"}, true, []string{"ls"}, 20, defaultTimeout, "", ""},
		{"value flag after a positional", []string{"diarium", "alpha", "-n", "5"}, false, []string{"diarium", "alpha"}, 5, defaultTimeout, "", ""},
		{"timeout five seconds", []string{"call", "a", "b", "c", "{}", "--timeout", "5"}, false, []string{"call", "a", "b", "c", "{}"}, 20, 5 * time.Second, "", ""},
		{"timeout fractional", []string{"call", "a", "b", "c", "{}", "--timeout", "1.5"}, false, []string{"call", "a", "b", "c", "{}"}, 20, 1500 * time.Millisecond, "", ""},
		{"timeout zero is a usage error", []string{"call", "a", "b", "c", "{}", "--timeout", "0"}, false, nil, 0, 0, "", `--timeout must be > 0 seconds, got "0"`},
		{"timeout negative is a usage error", []string{"cmd", "a", "b", "ping", "--timeout", "-3"}, false, nil, 0, 0, "", `--timeout must be > 0 seconds, got "-3"`},
		{"timeout not a number", []string{"call", "a", "b", "c", "{}", "--timeout", "abc"}, false, nil, 0, 0, "", `--timeout must be > 0 seconds, got "abc"`},
		{"flag without a value", []string{"call", "a", "b", "c", "{}", "--timeout"}, false, nil, 0, 0, "", "--timeout needs a value"},
		{"as without a value", []string{"validate", "f.json", "--as"}, false, nil, 0, 0, "", "--as needs a value"},
		{"n not a number", []string{"diarium", "alpha", "-n", "abc"}, false, nil, 0, 0, "", `-n must be a number >= 0, got "abc"`},
		{"n fractional", []string{"diarium", "alpha", "-n", "1.5"}, false, nil, 0, 0, "", `-n must be a positive integer, got "1.5"`},
		{"n negative", []string{"diarium", "alpha", "-n", "-2"}, false, nil, 0, 0, "", `-n must be a number >= 0, got "-2"`},
		{"dash word is a positional", []string{"broadcast", "-5"}, false, []string{"broadcast", "-5"}, 20, defaultTimeout, "", ""},
		{"as keeps any value", []string{"validate", "f.json", "--as", "bogus"}, false, []string{"validate", "f.json"}, 20, defaultTimeout, "bogus", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotJSON, pos, n, to, as, err := parseArgs(tt.args)
			if tt.wantErr != "" {
				if err == nil || err.msg != tt.wantErr {
					t.Fatalf("parseArgs: got err %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseArgs: unexpected err %v", err)
			}
			if gotJSON != tt.wantJSON {
				t.Errorf("json: got %v, want %v", gotJSON, tt.wantJSON)
			}
			if strings.Join(pos, "|") != strings.Join(tt.wantPos, "|") {
				t.Errorf("pos: got %v, want %v", pos, tt.wantPos)
			}
			if n != tt.wantN || to != tt.wantTO || as != tt.wantAs {
				t.Errorf("values: got n=%d timeout=%v as=%q, want n=%d timeout=%v as=%q", n, to, as, tt.wantN, tt.wantTO, tt.wantAs)
			}
		})
	}
	// no flag state leaks between calls: a parsed -n must not survive into a later parse
	if _, _, _, _, _, err := parseArgs([]string{"diarium", "alpha", "-n", "7"}); err != nil {
		t.Fatal(err)
	}
	if _, _, n, _, _, err := parseArgs([]string{"diarium", "alpha"}); err != nil || n != 20 {
		t.Errorf("the default -n after a parsed one: got %d (err %v), want 20", n, err)
	}
}

func TestRunUsage(t *testing.T) {
	_, cli := newCLI(t)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"no subcommand", nil, "usage: actio"},
		{"unknown subcommand", []string{"bogus"}, `unknown subcommand "bogus"`},
		{"call missing params", []string{"call", "a", "b", "c"}, "call needs <src-id> <dst> <capability> <json-params> [--timeout s]"},
		{"cmd missing name", []string{"cmd", "a", "b"}, "cmd needs <src-id> <dst> <name> [json-params]"},
		{"diarium without an agent", []string{"diarium"}, "diarium needs an agent id"},
		{"broadcast without text", []string{"broadcast"}, `broadcast needs "<text>"`},
		{"validate without a file", []string{"validate"}, "validate needs <file>"},
		{"call with a zero timeout", []string{"call", "a", "b", "c", "{}", "--timeout", "0"}, `--timeout must be > 0 seconds, got "0"`},
		{"cmd with a zero timeout", []string{"cmd", "a", "b", "ping", "--timeout", "0"}, `--timeout must be > 0 seconds, got "0"`},
		{"bad json-params", []string{"call", "a", "b", "c", "nope"}, "json-params is not valid JSON"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, _, errOut := cli(tt.args...)
			if code != 1 {
				t.Errorf("exit: got %d, want 1 (stderr: %s)", code, errOut)
			}
			if !strings.Contains(errOut, tt.want) {
				t.Errorf("stderr: want %q in %q", tt.want, errOut)
			}
		})
	}
}

func TestRunValidate(t *testing.T) {
	root, cli := newCLI(t)
	datetime := nowISO()
	specBody := `{"id":"alpha","actio_version":"0.1","datetime":"` + datetime + `","description":"d","accepts_commands_from":[],"capabilities":[],"security_support":[],"security_posture":"trusted"}`
	statusBody := `{"id":"alpha","status":"idle","datetime":"` + datetime + `","capability_status":[]}`
	diariumLine := `{"id":"alpha","datetime":"` + datetime + `","event":"log","event_data":"a line"}`

	t.Run("a valid spec", func(t *testing.T) {
		writeFile(t, filepath.Join(root, "spec.json"), specBody)
		code, out, _ := cli("validate", filepath.Join(root, "spec.json"))
		if code != 0 || strings.TrimSpace(out) != "OK spec" {
			t.Errorf("got exit %d out %q, want 0 / OK spec", code, out)
		}
	})
	t.Run("a valid status", func(t *testing.T) {
		writeFile(t, filepath.Join(root, "status.json"), statusBody)
		code, out, _ := cli("validate", filepath.Join(root, "status.json"))
		if code != 0 || strings.TrimSpace(out) != "OK status" {
			t.Errorf("got exit %d out %q, want 0 / OK status", code, out)
		}
	})
	// The four envelope files a real exchange wrote (doc/actio_protocol.md:97, :99, :101, :103):
	// each validates by its name alone — no --as — and the response also by --as, so both routes
	// are pinned. The names come from an exchange the library ran, not from hand-made files.
	t.Run("the envelopes an exchange wrote", func(t *testing.T) {
		cfg, err := actio.LoadConfig(actio.Config{
			HeartbeatInterval: 50 * time.Millisecond,
			AnnounceDelay:     5 * time.Millisecond,
			NuntiumDebounce:   10 * time.Millisecond,
			StopGrace:         2 * time.Second,
			CancelWaitTimeout: 300 * time.Millisecond,
			ConfirmTimeout:    5 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		emptyObject := `{"type":"object","properties":{},"additionalProperties":false}`
		peer, err := actio.New(actio.Options{ID: "peer", AcceptsCommandsFrom: []string{}, Config: cfg})
		if err != nil {
			t.Fatal(err)
		}
		if err := peer.Capability(actio.Capability{Name: "echo", Description: "echoes", Parameters: json.RawMessage(emptyObject), Actio: actio.CapabilityActio{Result: json.RawMessage("true")}},
			func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
				return map[string]any{}, nil
			}); err != nil {
			t.Fatal(err)
		}
		if err := peer.Capability(actio.Capability{Name: "block", Description: "blocks until cancelled", Parameters: json.RawMessage(emptyObject), Actio: actio.CapabilityActio{Result: json.RawMessage("true")}},
			func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			}); err != nil {
			t.Fatal(err)
		}
		if err := peer.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := peer.Stop(stopCtx, actio.StopOptions{Force: true}); err != nil {
				t.Errorf("cleanup Stop(peer): %v", err)
			}
		})
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, err := os.Stat(filepath.Join(root, "peer", "status.json")); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the peer agent did not start")
			}
			time.Sleep(10 * time.Millisecond)
		}

		// The exchange archives its own files once the confirmation is seen (A4, protocol :628),
		// and under a slow scheduler the archive can land before a read after the call. So the
		// envelopes are taken from the exchange's A4 record: the receiver's diarium inreq event
		// carries the envelopes as written (the archive's first step, :628), and the files are
		// written under the names the protocol gives them (the rid of the request's header).
		capDir := t.TempDir()
		type inreq struct {
			Event string `json:"event"`
			Data  struct {
				FinalStatus string          `json:"final_status"`
				Req         json.RawMessage `json:"req"`
				Can         json.RawMessage `json:"can"`
				Res         json.RawMessage `json:"res"`
				Con         json.RawMessage `json:"con"`
			} `json:"event_data"`
		}
		var ridCancelled, ridConfirmed string
		findEnv := func(rid, final string) map[string]json.RawMessage {
			data, err := os.ReadFile(filepath.Join(root, "peer", "diarium.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			for _, line := range strings.Split(string(data), "\n") {
				var e inreq
				if json.Unmarshal([]byte(line), &e) != nil || e.Event != "inreq" || e.Data.FinalStatus != final {
					continue
				}
				var h struct {
					RID string `json:"rid"`
				}
				if json.Unmarshal(e.Data.Req, &h) != nil || h.RID != rid {
					continue
				}
				return map[string]json.RawMessage{"req": e.Data.Req, "can": e.Data.Can, "res": e.Data.Res, "con": e.Data.Con}
			}
			t.Fatalf("the diarium has no archived %s exchange with rid %s", final, rid)
			return nil
		}
		writeEnv := func(rid, final, key, suffix string) string {
			raw := findEnv(rid, final)[key]
			if len(raw) == 0 || string(raw) == "null" {
				t.Fatalf("the diarium's record of the %s exchange has no %s envelope", final, key)
			}
			p := filepath.Join(capDir, rid+suffix)
			if err := os.WriteFile(p, raw, 0o644); err != nil {
				t.Fatal(err)
			}
			return p
		}

		// A timed-out call leaves the request and the cancellation in the receiver's inbox;
		// the exchange is archived as cancelled (:739).
		if _, err := actio.Call(context.Background(), cfg, "src", "peer", "block", map[string]any{}, actio.RequestOptions{Timeout: 50 * time.Millisecond}); err != nil {
			t.Fatalf("Call(block): %v", err)
		}
		// A confirmed call: the response and the confirmation join the exchange in the
		// receiver's inbox (protocol :508); it is archived as confirmed.
		outcome, err := actio.Call(context.Background(), cfg, "src", "peer", "echo", map[string]any{}, actio.RequestOptions{Timeout: 5 * time.Second})
		if err != nil || outcome.Response == nil {
			t.Fatalf("Call(echo) = (%v, %v): no confirmed response", outcome, err)
		}
		deadline = time.Now().Add(5 * time.Second)
		for {
			data, err := os.ReadFile(filepath.Join(root, "peer", "diarium.jsonl"))
			ridCancelled, ridConfirmed = "", ""
			if err == nil {
				for _, line := range strings.Split(string(data), "\n") {
					var e inreq
					if json.Unmarshal([]byte(line), &e) != nil || e.Event != "inreq" {
						continue
					}
					var h struct {
						RID string `json:"rid"`
					}
					if json.Unmarshal(e.Data.Req, &h) != nil {
						continue
					}
					switch e.Data.FinalStatus {
					case "cancelled":
						ridCancelled = h.RID
					case "confirmed":
						ridConfirmed = h.RID
					}
				}
			}
			if ridCancelled != "" && ridConfirmed != "" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("both exchanges were not archived in the receiver's diarium")
			}
			time.Sleep(10 * time.Millisecond)
		}

		for _, row := range [][2]string{
			{writeEnv(ridCancelled, "cancelled", "req", ".json"), "OK request"},          // <rid>.json, protocol :97
			{writeEnv(ridCancelled, "cancelled", "can", ".can.json"), "OK cancellation"}, // <rid>.can.json, :99
			{writeEnv(ridConfirmed, "confirmed", "res", ".res.json"), "OK response"},     // <rid>.res.json, :101
			{writeEnv(ridConfirmed, "confirmed", "con", ".con.json"), "OK confirmation"}, // <rid>.con.json, :103
		} {
			code, out, errOut := cli("validate", row[0]) // no --as: the name alone must decide
			if code != 0 || strings.TrimSpace(out) != row[1] {
				t.Errorf("validate %s: got exit %d out %q err %q, want 0 / %s (the envelope's own name, no --as)", filepath.Base(row[0]), code, out, errOut, row[1])
			}
		}
		// the response also by --as: both routes pinned
		resCopy := filepath.Join(capDir, ridConfirmed+".res.json")
		code, out, _ := cli("validate", resCopy, "--as", "response")
		if code != 0 || strings.TrimSpace(out) != "OK response" {
			t.Errorf("got exit %d out %q, want 0 / OK response by --as", code, out)
		}
	})
	// The announce file the agent's own test run wrote: a name something real produced, not one
	// invented by hand (protocol: <agent_id>.n.json).
	t.Run("the announce an agent run wrote", func(t *testing.T) {
		cfg, err := actio.LoadConfig(actio.Config{
			HeartbeatInterval: 50 * time.Millisecond,
			AnnounceDelay:     5 * time.Millisecond,
			NuntiumDebounce:   10 * time.Millisecond,
			StopGrace:         2 * time.Second,
			CancelWaitTimeout: 2 * time.Second,
			ConfirmTimeout:    5 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		agent, err := actio.New(actio.Options{ID: "announcer", AcceptsCommandsFrom: []string{}, Config: cfg})
		if err != nil {
			t.Fatal(err)
		}
		if err := agent.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := agent.Stop(stopCtx, actio.StopOptions{Force: true}); err != nil {
				t.Errorf("cleanup Stop(announcer): %v", err)
			}
		})
		announcePath := filepath.Join(root, "nuntium", "announcer.n.json")
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, err := os.Stat(announcePath); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the agent did not write its announce file %s", announcePath)
			}
			time.Sleep(10 * time.Millisecond)
		}
		code, out, _ := cli("validate", announcePath)
		if code != 0 || strings.TrimSpace(out) != "OK announce" {
			t.Errorf("got exit %d out %q, want 0 / OK announce", code, out)
		}
	})
	// nuntium/nuntium.json, written by the broadcast case: the same format as the announce files
	// (protocol :193), so it validates as an announce.
	t.Run("the broadcast file is an announce", func(t *testing.T) {
		code, _, errOut := cli("broadcast", "fix16: the broadcast file is an announce")
		if code != 0 {
			t.Fatalf("broadcast: %s", errOut)
		}
		f := filepath.Join(root, "nuntium", "nuntium.json")
		code, out, _ := cli("validate", f)
		if code != 0 || strings.TrimSpace(out) != "OK announce" {
			t.Errorf("got exit %d out %q, want 0 / OK announce", code, out)
		}
	})
	t.Run("an invalid status", func(t *testing.T) {
		bad := filepath.Join(root, "badstatus.json")
		writeFile(t, bad, `{"id":"alpha","status":"asleep","datetime":"`+datetime+`","capability_status":[]}`)
		code, _, errOut := cli("validate", bad, "--as", "status")
		if code != 1 || !strings.Contains(errOut, "status.schema.json") {
			t.Errorf("got exit %d err %q, want 1 / a schema reason", code, errOut)
		}
	})
	t.Run("a diarium, line by line", func(t *testing.T) {
		di := filepath.Join(root, "diarium.jsonl")
		writeFile(t, di, diariumLine+"\n"+diariumLine+"\n")
		code, out, _ := cli("validate", di)
		if code != 0 || strings.TrimSpace(out) != "OK diarium (2 lines)" {
			t.Errorf("got exit %d out %q, want 0 / OK diarium (2 lines)", code, out)
		}
	})
	t.Run("a diarium with a bad line", func(t *testing.T) {
		badDir := filepath.Join(root, "badagent")
		if err := os.MkdirAll(badDir, 0o755); err != nil {
			t.Fatal(err)
		}
		di := filepath.Join(badDir, "diarium.jsonl")
		writeFile(t, di, diariumLine+"\n{not a line}\n")
		code, _, errOut := cli("validate", di)
		if code != 1 || !strings.Contains(errOut, "line 2") {
			t.Errorf("got exit %d err %q, want 1 / line 2 named", code, errOut)
		}
	})
	t.Run("not valid JSON under a known name", func(t *testing.T) {
		badDir := filepath.Join(root, "junkagent")
		if err := os.MkdirAll(badDir, 0o755); err != nil {
			t.Fatal(err)
		}
		bad := filepath.Join(badDir, "spec.json")
		writeFile(t, bad, "not json at all")
		code, _, errOut := cli("validate", bad)
		if code != 1 || !strings.Contains(errOut, "spec.schema.json") {
			t.Errorf("got exit %d err %q, want 1 / a schema reason", code, errOut)
		}
	})
	// The seven rows of the reference transcript (orchestra/tasks/FIX19-A-6.brief.md): the name
	// decides, the reference's rule (cli.ts kindOf). A bare <stem>.json is a request (cli.ts:312,
	// the CLI's own UX rule, not a second protocol table); a non-.json name gets the reference's
	// "no validator". Each row asserts the exit code AND the word the CLI prints, so a rename of
	// the .json → request fallback cannot hide behind an exit code.
	t.Run("the seven reference rows", func(t *testing.T) {
		requestBody, err := os.ReadFile("../../testdata/vectors/request/valid/01-a1-request.json")
		if err != nil {
			t.Fatal(err)
		}
		rows := []struct {
			name     string // the file name
			body     string // the content
			wantExit int
			wantOut  string // for exit 0: the stdout word
			wantErr  string // for exit 1: a substring of the stderr
		}{
			{"nuntium/nuntium.json", `{"id":"broadcast","datetime":"` + datetime + `","ts":1792836000,"nuntium":"hello"}`, 0, "OK announce", ""},
			{"spec.json", specBody, 0, "OK spec", ""},
			{"status.json", statusBody, 0, "OK status", ""},
			{"copy.json", string(requestBody), 0, "OK request", ""},
			{"mis.json", `{"rid":"01JB7Q3W8K2M4N6P8R0S2T4V6X","src":"s","dst":"d","datetime":"` + datetime + `","type":"res","actio_version":"0.1","response":{"ok":true}}`, 1, "", "request.schema.json"},
			{"diaryx.json", diariumLine, 1, "", "request.schema.json"},
			{"junk.json", "not json at all", 1, "", "request.schema.json"},
			{"notes.txt", "hello", 1, "", `no validator for "notes.txt"`},
		}
		for _, row := range rows {
			t.Run(row.name, func(t *testing.T) {
				f := filepath.Join(root, row.name)
				if err := os.MkdirAll(filepath.Dir(f), 0o755); err != nil {
					t.Fatal(err)
				}
				writeFile(t, f, row.body)
				code, out, errOut := cli("validate", f)
				if code != row.wantExit {
					t.Errorf("validate %s: got exit %d out %q err %q, want exit %d", row.name, code, out, errOut, row.wantExit)
				}
				if row.wantOut != "" && strings.TrimSpace(out) != row.wantOut {
					t.Errorf("validate %s: got out %q, want %q (the word, not just the exit code)", row.name, out, row.wantOut)
				}
				if row.wantErr != "" && !strings.Contains(errOut, row.wantErr) {
					t.Errorf("validate %s: got err %q, want it to contain %q", row.name, errOut, row.wantErr)
				}
			})
		}
	})
	t.Run("an unknown --as kind", func(t *testing.T) {
		f := filepath.Join(root, "x.json")
		writeFile(t, f, "{}")
		code, _, errOut := cli("validate", f, "--as", "bogus")
		if code != 1 || !strings.Contains(errOut, `no validator for "bogus"`) {
			t.Errorf("got exit %d err %q, want 1 / no validator", code, errOut)
		}
	})
	t.Run("a missing file", func(t *testing.T) {
		code, _, errOut := cli("validate", filepath.Join(root, "nope", "spec.json"))
		if code != 1 || !strings.Contains(errOut, "cannot read") {
			t.Errorf("got exit %d err %q, want 1 / cannot read", code, errOut)
		}
	})
}

func TestRunLs(t *testing.T) {
	root, cli := newCLI(t)
	datetime := writeAgent(t, root, "alpha")
	dup := filepath.Join(root, "alpha-dup")
	if err := os.MkdirAll(dup, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"spec.json":   `{"id":"alpha","actio_version":"0.1","datetime":"` + datetime + `","description":"d","accepts_commands_from":[],"capabilities":[],"security_support":[],"security_posture":"trusted"}`,
		"status.json": `{"id":"alpha","status":"idle","datetime":"` + datetime + `","capability_status":[]}`,
	} {
		writeFile(t, filepath.Join(dup, name), body)
	}
	bad := filepath.Join(root, "bad")
	if err := os.MkdirAll(bad, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(bad, "spec.json"), "{not json")
	writeFile(t, filepath.Join(bad, "status.json"), `{"id":"bad","status":"idle","datetime":"`+datetime+`","capability_status":[]}`)

	t.Run("human output", func(t *testing.T) {
		code, out, _ := cli("ls")
		if code != 0 {
			t.Fatalf("exit: got %d, want 0", code)
		}
		want := []string{
			"alpha  idle  healthy  " + datetime + "  " + filepath.Join(root, "alpha"),
			"    echo",
			"duplicate agent id: " + dup,
			"ignored: " + filepath.Join(bad, "spec.json") + ": not valid JSON",
		}
		got := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
		if len(got) != len(want) {
			t.Fatalf("lines: got %d (%q), want %d", len(got), out, len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("line %d: got %q, want %q", i, got[i], want[i])
			}
		}
	})
	t.Run("json output", func(t *testing.T) {
		code, out, _ := cli("--json", "ls")
		if code != 0 {
			t.Fatalf("exit: got %d, want 0", code)
		}
		var view struct {
			Root   string `json:"root"`
			Counts struct {
				Agents     int `json:"agents"`
				Duplicates int `json:"duplicates"`
				Ignored    int `json:"ignored"`
			} `json:"counts"`
			Agents []struct {
				ID           string `json:"id"`
				Folder       string `json:"folder"`
				Status       string `json:"status"`
				Datetime     string `json:"datetime"`
				Health       string `json:"health"`
				Capabilities []struct {
					Name        string   `json:"name"`
					Disabled    bool     `json:"disabled"`
					AcceptsFrom []string `json:"accepts_from"`
				} `json:"capabilities"`
			} `json:"agents"`
			Duplicates []string `json:"duplicates"`
			Ignored    []struct {
				Path   string `json:"path"`
				Reason string `json:"reason"`
			} `json:"ignored"`
		}
		if err := json.Unmarshal([]byte(out), &view); err != nil {
			t.Fatalf("the json output does not parse: %v\n%s", err, out)
		}
		if view.Root != root {
			t.Errorf("root: got %q, want %q", view.Root, root)
		}
		if view.Counts.Agents != 1 || view.Counts.Duplicates != 1 || view.Counts.Ignored != 1 {
			t.Errorf("counts: got %+v, want 1/1/1", view.Counts)
		}
		if len(view.Agents) != 1 || view.Agents[0].ID != "alpha" || view.Agents[0].Health != "healthy" ||
			view.Agents[0].Status != "idle" || view.Agents[0].Datetime != datetime {
			t.Errorf("agents: got %+v", view.Agents)
		}
		caps := view.Agents[0].Capabilities
		if len(caps) != 1 || caps[0].Name != "echo" || len(caps[0].AcceptsFrom) != 0 {
			t.Errorf("capabilities: got %+v (accepts_from must be present and empty)", caps)
		}
		if len(view.Duplicates) != 1 || view.Duplicates[0] != dup {
			t.Errorf("duplicates: got %+v", view.Duplicates)
		}
		if len(view.Ignored) != 1 || view.Ignored[0].Reason != "not valid JSON" {
			t.Errorf("ignored: got %+v", view.Ignored)
		}
	})
	t.Run("an empty root", func(t *testing.T) {
		empty, cliEmpty := newCLI(t)
		code, out, _ := cliEmpty("ls")
		if code != 0 || strings.TrimSpace(out) != "no agents under "+empty {
			t.Errorf("got exit %d out %q, want 0 / no agents", code, out)
		}
		code, out, _ = cliEmpty("--json", "ls")
		if code != 0 || !strings.Contains(out, `"agents": []`) || !strings.Contains(out, `"duplicates": []`) || !strings.Contains(out, `"ignored": []`) {
			t.Errorf("the empty json lists must be [], not null: %s", out)
		}
	})
}

func TestRunDiarium(t *testing.T) {
	root, cli := newCLI(t)
	dir := filepath.Join(root, "alpha")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	datetime := nowISO()
	line := func(n int) string {
		return `{"id":"alpha","datetime":"` + datetime + `","event":"log","event_data":"line ` + strings.Repeat("x", n) + `"}`
	}
	writeFile(t, filepath.Join(dir, "diarium.jsonl"), line(1)+"\n"+line(2)+"\n"+line(3)+"\n")

	t.Run("all the lines, as written", func(t *testing.T) {
		code, out, _ := cli("diarium", "alpha")
		if code != 0 {
			t.Fatalf("exit: got %d, want 0", code)
		}
		want := line(1) + "\n" + line(2) + "\n" + line(3) + "\n"
		if out != want {
			t.Errorf("got %q, want %q", out, want)
		}
	})
	t.Run("the last n lines", func(t *testing.T) {
		code, out, _ := cli("diarium", "alpha", "-n", "2")
		if code != 0 {
			t.Fatalf("exit: got %d, want 0", code)
		}
		want := line(2) + "\n" + line(3) + "\n"
		if out != want {
			t.Errorf("got %q, want %q", out, want)
		}
	})
	t.Run("json mode pretty-prints", func(t *testing.T) {
		code, out, _ := cli("--json", "diarium", "alpha", "-n", "1")
		if code != 0 {
			t.Fatalf("exit: got %d, want 0", code)
		}
		if !strings.Contains(out, "\"event_data\": \"line xxx\"") {
			t.Errorf("want the pretty-printed line in %q", out)
		}
	})
	t.Run("a missing diarium", func(t *testing.T) {
		code, _, errOut := cli("diarium", "ghost")
		if code != 1 || !strings.Contains(errOut, "cannot read") {
			t.Errorf("got exit %d err %q, want 1 / cannot read", code, errOut)
		}
	})
}

// TestRunCallAgainstAnEmptyPeer pins the no-response path: the exit code is 1, the human output
// names the final status and the missing response, and the json output omits the response key.
func TestRunCallAgainstAnEmptyPeer(t *testing.T) {
	root, cli := newCLI(t)
	t.Setenv("ACTIO_CANCEL_WAIT", "1") // the vehicle's config comes from the environment
	dst := filepath.Join(root, "dst")
	if err := os.MkdirAll(filepath.Join(dst, "inbox"), 0o755); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	code, out, _ := cli("call", "src", "dst", "echo", `{"text":"x"}`, "--timeout", "1")
	elapsed := time.Since(start)
	if code != 1 {
		t.Errorf("exit: got %d, want 1 (a no-response call is a failure)", code)
	}
	if !strings.Contains(out, "finalStatus: abandoned") || !strings.Contains(out, "response: none") {
		t.Errorf("human output: got %q", out)
	}
	if elapsed > 15*time.Second {
		t.Errorf("the call took %v: the timeout and the cancel wait must bound it", elapsed)
	}
	code, out, _ = cli("--json", "call", "src", "dst", "echo", `{}`, "--timeout", "1")
	if code != 1 {
		t.Errorf("exit: got %d, want 1", code)
	}
	var view map[string]any
	if err := json.Unmarshal([]byte(out), &view); err != nil {
		t.Fatalf("the json output does not parse: %v\n%s", err, out)
	}
	if view["finalStatus"] != "abandoned" {
		t.Errorf("finalStatus: got %v", view["finalStatus"])
	}
	if _, present := view["response"]; present {
		t.Errorf("the response key must be omitted when no response arrived: %s", out)
	}
}

func TestRunCmd(t *testing.T) {
	root, cli := newCLI(t)
	dst := filepath.Join(root, "dst")
	if err := os.MkdirAll(filepath.Join(dst, "inbox"), 0o755); err != nil {
		t.Fatal(err)
	}
	code, out, _ := cli("cmd", "src", "dst", "ping")
	if code != 0 {
		t.Fatalf("exit: got %d, want 0", code)
	}
	rid := strings.TrimPrefix(strings.TrimSpace(out), "rid: ")
	if len(rid) != 26 {
		t.Fatalf("rid: got %q, want a 26-character rid", rid)
	}
	entries, err := os.ReadDir(filepath.Join(dst, "inbox"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != rid+".json" {
		t.Errorf("the dst inbox: got %+v, want one file %s.json", entries, rid)
	}
}

func TestRunBroadcast(t *testing.T) {
	_, cli := newCLI(t)
	code, out, _ := cli("broadcast", "a signal")
	if code != 0 {
		t.Fatalf("exit: got %d, want 0", code)
	}
	target := strings.TrimPrefix(strings.TrimSpace(out), "wrote ")
	if !strings.HasSuffix(target, filepath.Join("nuntium", "nuntium.json")) {
		t.Errorf("wrote: got %q", out)
	}
	raw, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	var file map[string]any
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("the signal file does not parse: %v", err)
	}
	if file["id"] != "broadcast" || file["nuntium"] != "a signal" {
		t.Errorf("the signal file: got %v", file)
	}
}
