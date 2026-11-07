// The CLI's own tests: the argument parsing, the usage errors, and the output shape of ls,
// diarium, call, cmd, broadcast and validate on a fixture root.
package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	t.Run("a response file by its name", func(t *testing.T) {
		raw, err := os.ReadFile("../../testdata/vectors/response/valid/01-a1-normal.json")
		if err != nil {
			t.Fatal(err)
		}
		f := filepath.Join(root, "some.res.json")
		writeFile(t, f, string(raw))
		code, out, _ := cli("validate", f)
		if code != 0 || strings.TrimSpace(out) != "OK response" {
			t.Errorf("got exit %d out %q, want 0 / OK response", code, out)
		}
	})
	t.Run("an announced file by its name", func(t *testing.T) {
		raw, err := os.ReadFile("../../testdata/vectors/announce/valid/01-active.json")
		if err != nil {
			t.Fatal(err)
		}
		f := filepath.Join(root, "alpha.1.json")
		writeFile(t, f, string(raw))
		code, out, _ := cli("validate", f)
		if code != 0 || strings.TrimSpace(out) != "OK announce" {
			t.Errorf("got exit %d out %q, want 0 / OK announce", code, out)
		}
	})
	t.Run("the signal file infers the signal kind", func(t *testing.T) {
		raw, err := os.ReadFile("../../testdata/vectors/actio/valid/01-minimal.json")
		if err != nil {
			t.Fatal(err)
		}
		f := filepath.Join(root, "nuntium.json")
		writeFile(t, f, string(raw))
		code, out, _ := cli("validate", f)
		if code != 0 || strings.TrimSpace(out) != "OK actio" {
			t.Errorf("got exit %d out %q, want 0 / OK actio (the reference maps nuntium.json to announce, which can never pass)", code, out)
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
	t.Run("not valid JSON under a bare .json name", func(t *testing.T) {
		bad := filepath.Join(root, "junk.json")
		writeFile(t, bad, "not json at all")
		code, _, errOut := cli("validate", bad)
		if code != 1 || !strings.Contains(errOut, "request.schema.json") {
			t.Errorf("got exit %d err %q, want 1 / a request schema reason (any .json infers request, as in the reference)", code, errOut)
		}
	})
	t.Run("no validator for the name", func(t *testing.T) {
		f := filepath.Join(root, "notes.txt")
		writeFile(t, f, "hello")
		code, _, errOut := cli("validate", f)
		if code != 1 || !strings.Contains(errOut, `no validator for "notes.txt"`) {
			t.Errorf("got exit %d err %q, want 1 / no validator", code, errOut)
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
