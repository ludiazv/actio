//go:build interop

// The interop suite: the Go and the TypeScript implementations of the protocol talk to each other
// over one shared actio root. It is behind the interop build tag, so its absence never breaks the
// default build or vet. Each row is green only when the two implementations really talked; a row
// that cannot be run (a missing binary, a Node quirk) is reported as not demonstrated, with the
// reason, and the test stays honest about it.
package actio

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestInterop runs the interop rows. The Go side is the built CLI and echo; the TS side is the
// reference CLI and echo, run through node and tsx. A row that cannot be run is reported as not
// demonstrated, not skipped silently.
func TestInterop(t *testing.T) {
	// The actio-ts project is a sibling of the module (symlinked in the scratch copy).
	tsDir := filepath.Join("..", "actio-ts")
	if _, err := os.Stat(filepath.Join(tsDir, "src", "cli.ts")); err != nil {
		t.Fatalf("the actio-ts project is not available at %s: %v", tsDir, err)
	}
	// Build the Go CLI and echo into a temp dir.
	binDir := t.TempDir()
	goCLI := filepath.Join(binDir, "actio")
	goEcho := filepath.Join(binDir, "echo")
	for _, b := range []struct{ out, pkg string }{{goCLI, "./cmd/actio"}, {goEcho, "./examples/echo"}} {
		if out, err := exec.Command("go", "build", "-o", b.out, b.pkg).CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v: %s", b.pkg, err, out)
		}
	}
	// The shared root: both implementations live in it.
	root := t.TempDir()
	t.Setenv("ACTIO_DIR", root)

	// Start the TS echo agent.
	tsEcho := exec.Command("node", "--import", "tsx", "examples/echo-agent.ts")
	tsEcho.Dir = tsDir
	tsEcho.Env = append(os.Environ(), "ACTIO_DIR="+root, "ACTIO_AGENT_ID=tsecho", "ACTIO_COMMANDERS=tscli")
	if err := tsEcho.Start(); err != nil {
		t.Fatalf("start the TS echo: %v", err)
	}
	defer tsEcho.Process.Kill()
	// Start the Go echo agent.
	goEchoProc := exec.Command(goEcho)
	goEchoProc.Env = append(os.Environ(), "ACTIO_DIR="+root, "ACTIO_AGENT_ID=goecho", "ACTIO_COMMANDERS=gocli")
	if err := goEchoProc.Start(); err != nil {
		t.Fatalf("start the Go echo: %v", err)
	}
	defer goEchoProc.Process.Kill()

	// Wait for both agents to publish their status.
	waitFor(t, 15*time.Second, "both agents to publish their status", func() bool {
		_, e1 := os.Stat(filepath.Join(root, "tsecho", "status.json"))
		_, e2 := os.Stat(filepath.Join(root, "goecho", "status.json"))
		return e1 == nil && e2 == nil
	})

	// The cross calls: each CLI drives the other implementation's echo.
	t.Run("go_cli_to_ts_echo", func(t *testing.T) {
		out, err := exec.Command(goCLI, "call", "gocli", "tsecho", "echo", `{"text":"hi-from-go"}`, "--timeout", "10").CombinedOutput()
		if err != nil {
			t.Fatalf("not demonstrated: the Go CLI call to the TS echo failed: %v: %s", err, out)
		}
		if !strings.Contains(string(out), "hi-from-go") {
			t.Errorf("the echoed text is missing from the TS echo's answer: %s", out)
		}
	})
	t.Run("ts_cli_to_go_echo", func(t *testing.T) {
		cmd := exec.Command("node", "--import", "tsx", "src/cli.ts", "call", "tscli", "goecho", "echo", `{"text":"hi-from-ts"}`, "--timeout", "10")
		cmd.Dir = tsDir
		cmd.Env = append(os.Environ(), "ACTIO_DIR="+root)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("not demonstrated: the TS CLI call to the Go echo failed: %v: %s", err, out)
		}
		if !strings.Contains(string(out), "hi-from-ts") {
			t.Errorf("the echoed text is missing from the Go echo's answer: %s", out)
		}
	})
	t.Run("both_ls_list_both_agents", func(t *testing.T) {
		for name, fn := range map[string]func() (string, error){
			"go": func() (string, error) {
				out, err := exec.Command(goCLI, "ls").CombinedOutput()
				return string(out), err
			},
			"ts": func() (string, error) {
				cmd := exec.Command("node", "--import", "tsx", "src/cli.ts", "ls")
				cmd.Dir = tsDir
				cmd.Env = append(os.Environ(), "ACTIO_DIR="+root)
				out, err := cmd.CombinedOutput()
				return string(out), err
			},
		} {
			out, err := fn()
			if err != nil {
				t.Fatalf("not demonstrated: the %s ls failed: %v: %s", name, err, out)
			}
			for _, id := range []string{"tsecho", "goecho"} {
				if !strings.Contains(out, id) {
					t.Errorf("the %s ls does not list %s: %s", name, id, out)
				}
			}
		}
	})
	t.Run("go_reads_ts_diarium", func(t *testing.T) {
		read, err := ReadDiarium(filepath.Join(root, "tsecho"))
		if err != nil {
			t.Fatalf("not demonstrated: the Go ReadDiarium over the TS diarium failed: %v", err)
		}
		for _, line := range read.Lines {
			if line.Event == "ignored" {
				t.Errorf("the TS diarium has an ignored line that the Go reader flags: %s", line.EventData)
			}
		}
	})

	// The error and rejection rows, in both directions: the same four answers the echo target owes.
	for name, d := range map[string]struct{ cli, dst, src, dir string }{
		"go_to_ts": {cli: goCLI, dst: "tsecho", src: "gocli", dir: ""},
		"ts_to_go": {cli: "ts", dst: "goecho", src: "tscli", dir: tsDir},
	} {
		run := func(args ...string) (string, error) {
			if d.cli == "ts" {
				full := append([]string{"--import", "tsx", "src/cli.ts"}, args...)
				cmd := exec.Command("node", full...)
				cmd.Dir = d.dir
				cmd.Env = append(os.Environ(), "ACTIO_DIR="+root)
				out, err := cmd.CombinedOutput()
				return string(out), err
			}
			out, err := exec.Command(d.cli, args...).CombinedOutput()
			return string(out), err
		}
		t.Run(name+"_fail", func(t *testing.T) {
			out, err := run("call", d.src, d.dst, "fail", `{}`)
			if err != nil || !strings.Contains(out, "1000") {
				t.Errorf("not demonstrated: the fail answer is missing (code 1000): %v: %s", err, out)
			}
		})
		t.Run(name+"_private_103", func(t *testing.T) {
			out, err := run("call", d.src, d.dst, "private", `{}`)
			if err != nil || !strings.Contains(out, "103") {
				t.Errorf("not demonstrated: the private rejection is missing (103): %v: %s", err, out)
			}
		})
		t.Run(name+"_sleep_cancelled", func(t *testing.T) {
			out, err := run("call", d.src, d.dst, "sleep", `{"seconds":30}`, "--timeout", "1")
			if err != nil || !strings.Contains(strings.ToLower(out), "cancel") {
				t.Errorf("not demonstrated: the sleep cancellation is missing: %v: %s", err, out)
			}
		})
	}

	// The validate rows: every kind each side publishes, in both directions. Each CLI
	// prints "OK <kind>"; the kind word is asserted, not just exit 0, and for the shared broadcast
	// file both CLIs' kind words are compared. A row that cannot be run is reported as not
	// demonstrated; a row that runs and disagrees fails.
	runValidate := func(side, file string) (string, error) {
		if side == "ts" {
			cmd := exec.Command("node", "--import", "tsx", "src/cli.ts", "validate", file)
			cmd.Dir = tsDir
			cmd.Env = append(os.Environ(), "ACTIO_DIR="+root)
			out, err := cmd.CombinedOutput()
			return string(out), err
		}
		out, err := exec.Command(goCLI, "validate", file).CombinedOutput()
		return string(out), err
	}
	expectKind := func(t *testing.T, row, out string, wantKind string) {
		t.Helper()
		if !strings.Contains(out, "OK "+wantKind) {
			t.Errorf("%s: want a line %q, got: %s", row, "OK "+wantKind, out)
		}
	}
	// announceFile takes the writing agent's announce file by the name it really used, from the folder.
	// The TS agent writes it announceDelay after the status, so the helper waits for it with a named
	// bound rather than assuming it is present when the status is.
	const announceBound = 15 * time.Second
	announceFile := func(t *testing.T, agent string) string {
		t.Helper()
		dir := filepath.Join(root, "nuntium")
		var found string
		waitFor(t, announceBound, "the "+agent+" announce file to appear in "+dir, func() bool {
			entries, err := os.ReadDir(dir)
			if err != nil {
				return false
			}
			for _, e := range entries {
				if e.Name() == agent+".n.json" {
					found = filepath.Join(dir, e.Name())
					return true
				}
			}
			return false
		})
		return found
	}
	// captureCmd sends a command to dst and copies its inbox file before the receiver archives it.
	// A command is the honest way to get an inbox file that is never answered (it has no response).
	// The send runs in a goroutine so the copy loop is already polling when the file appears; the
	// window between the atomic write and the receiver's archive is small, so polling must not start
	// after the (blocking) CLI call returns. If the receiver wins the race, the file's exact bytes are
	// still recoverable: the receiver records the req envelope it read from the inbox in the diarium's
	// inreq line, so captureCmd falls back to that (same bytes, read after the archive).
	const envelopeBound = 5 * time.Second
	// diariumCmdFile reads the newest inreq line's req envelope from the agent's diarium and writes it
	// to a temp file. The receiver records the exact bytes it read from the inbox, so this is the same
	// file the inbox capture would have caught.
	diariumCmdFile := func(t *testing.T, dst string) string {
		t.Helper()
		deadline := time.Now().Add(envelopeBound)
		for time.Now().Before(deadline) {
			read, err := ReadDiarium(filepath.Join(root, dst))
			if err == nil {
				for i := len(read.Lines) - 1; i >= 0; i-- {
					line := read.Lines[i]
					if line.Event != "inreq" {
						continue
					}
					var env ExchangeEnvelopes
					if json.Unmarshal(line.EventData, &env) == nil && len(env.Req) > 0 {
						tmp := filepath.Join(t.TempDir(), "cmd-from-diarium.json")
						if werr := os.WriteFile(tmp, env.Req, 0o644); werr == nil {
							return tmp
						}
					}
					break // the first inreq from the end is the newest
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		return ""
	}
	// captureCmd sends a command to dst and copies its inbox file before the receiver archives it.
	// A command is the honest way to get an inbox file that is never answered (it has no response).
	// The send runs in a goroutine so the copy loop is already polling when the file appears; the
	// window between the atomic write and the receiver's archive is small, so polling must not start
	// after the (blocking) CLI call returns. If the receiver wins the race, the file's exact bytes are
	// still recoverable: the receiver records the req envelope it read from the inbox in the diarium's
	// inreq line, so captureCmd falls back to that (same bytes, read after the archive).
	captureCmd := func(t *testing.T, side, dst string) string {
		t.Helper()
		inbox := filepath.Join(root, dst, "inbox")
		seen := map[string]bool{}
		if entries, err := os.ReadDir(inbox); err == nil {
			for _, e := range entries {
				seen[e.Name()] = true
			}
		}
		sent := make(chan struct{})
		go func() {
			defer close(sent)
			if side == "ts" {
				cmd := exec.Command("node", "--import", "tsx", "src/cli.ts", "cmd", "tscli", dst, "ping", "null")
				cmd.Dir = tsDir
				cmd.Env = append(os.Environ(), "ACTIO_DIR="+root)
				cmd.CombinedOutput()
				return
			}
			exec.Command(goCLI, "cmd", "gocli", dst, "ping", "null").CombinedOutput()
		}()
		deadline := time.Now().Add(envelopeBound)
		for time.Now().Before(deadline) {
			if entries, err := os.ReadDir(inbox); err == nil {
				for _, e := range entries {
					n := e.Name()
					if seen[n] || !strings.HasSuffix(n, ".json") || strings.Contains(n, ".res.") || strings.Contains(n, ".con.") || strings.Contains(n, ".can.") {
						continue
					}
					if data, rerr := os.ReadFile(filepath.Join(inbox, n)); rerr == nil {
						tmp := filepath.Join(t.TempDir(), n)
						if werr := os.WriteFile(tmp, data, 0o644); werr == nil {
							<-sent
							return tmp
						}
					}
				}
			}
			time.Sleep(100 * time.Microsecond)
		}
		<-sent
		if file := diariumCmdFile(t, dst); file != "" {
			return file
		}
		t.Errorf("not demonstrated: the %s command file in %s was archived before it could be copied, and no inreq envelope was recorded (bound %v)", side, inbox, envelopeBound)
		return ""
	}
	for kind, name := range map[string]string{"spec": "spec.json", "status": "status.json"} {
		t.Run("go_validates_ts_"+kind, func(t *testing.T) {
			out, err := runValidate("go", filepath.Join(root, "tsecho", name))
			if err != nil {
				t.Fatalf("not demonstrated: %v: %s", err, out)
			}
			expectKind(t, "go on the ts "+kind, out, kind)
		})
		t.Run("ts_validates_go_"+kind, func(t *testing.T) {
			out, err := runValidate("ts", filepath.Join(root, "goecho", name))
			if err != nil {
				t.Fatalf("not demonstrated: %v: %s", err, out)
			}
			expectKind(t, "ts on the go "+kind, out, kind)
		})
	}
	t.Run("go_validates_ts_announce", func(t *testing.T) {
		out, err := runValidate("go", announceFile(t, "tsecho"))
		if err != nil {
			t.Fatalf("not demonstrated: %v: %s", err, out)
		}
		expectKind(t, "go on the ts announce", out, "announce")
	})
	t.Run("ts_validates_go_announce", func(t *testing.T) {
		out, err := runValidate("ts", announceFile(t, "goecho"))
		if err != nil {
			t.Fatalf("not demonstrated: %v: %s", err, out)
		}
		expectKind(t, "ts on the go announce", out, "announce")
	})
	t.Run("go_broadcast_ts_validates", func(t *testing.T) {
		if out, err := exec.Command(goCLI, "broadcast", "from-go").CombinedOutput(); err != nil {
			t.Fatalf("the Go broadcast failed: %v: %s", err, out)
		}
		out, err := runValidate("ts", filepath.Join(root, "nuntium", "nuntium.json"))
		if err != nil {
			t.Fatalf("not demonstrated: %v: %s", err, out)
		}
		expectKind(t, "ts on the go broadcast", out, "announce")
	})
	t.Run("ts_broadcast_go_validates", func(t *testing.T) {
		cmd := exec.Command("node", "--import", "tsx", "src/cli.ts", "broadcast", "from-ts")
		cmd.Dir = tsDir
		cmd.Env = append(os.Environ(), "ACTIO_DIR="+root)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("the TS broadcast failed: %v: %s", err, out)
		}
		out, err := runValidate("go", filepath.Join(root, "nuntium", "nuntium.json"))
		if err != nil {
			t.Fatalf("not demonstrated: %v: %s", err, out)
		}
		expectKind(t, "go on the ts broadcast", out, "announce")
	})
	t.Run("go_validates_ts_diarium", func(t *testing.T) {
		out, err := runValidate("go", filepath.Join(root, "tsecho", "diarium.jsonl"))
		if err != nil || !strings.Contains(out, "OK diarium (") {
			t.Fatalf("not demonstrated: the Go validate over the TS diarium: %v: %s", err, out)
		}
	})
	t.Run("ts_validates_go_diarium", func(t *testing.T) {
		out, err := runValidate("ts", filepath.Join(root, "goecho", "diarium.jsonl"))
		if err != nil || !strings.Contains(out, "OK diarium (") {
			t.Fatalf("not demonstrated: the TS validate over the Go diarium: %v: %s", err, out)
		}
	})
	t.Run("go_validates_ts_cmd", func(t *testing.T) {
		file := captureCmd(t, "go", "tsecho")
		if file == "" {
			return
		}
		out, err := runValidate("go", file)
		if err != nil {
			t.Fatalf("not demonstrated: %v: %s", err, out)
		}
		expectKind(t, "go on the ts cmd envelope", out, "request")
	})
	t.Run("ts_validates_go_cmd", func(t *testing.T) {
		file := captureCmd(t, "ts", "goecho")
		if file == "" {
			return
		}
		out, err := runValidate("ts", file)
		if err != nil {
			t.Fatalf("not demonstrated: %v: %s", err, out)
		}
		expectKind(t, "ts on the go cmd envelope", out, "request")
	})
	// The negative rows: garbage under a known name must not validate, in both implementations.
	garbage := func(t *testing.T) string {
		t.Helper()
		f := filepath.Join(t.TempDir(), "spec.json")
		if err := os.WriteFile(f, []byte("{ not json "), 0o644); err != nil {
			t.Fatalf("write the garbage file: %v", err)
		}
		return f
	}
	t.Run("go_rejects_garbage", func(t *testing.T) {
		out, err := runValidate("go", garbage(t))
		if err == nil {
			t.Errorf("the Go validate accepted a garbage spec.json (exit 0): %s", out)
		}
	})
	t.Run("ts_rejects_garbage", func(t *testing.T) {
		out, err := runValidate("ts", garbage(t))
		if err == nil {
			t.Errorf("the TS validate accepted a garbage spec.json (exit 0): %s", out)
		}
	})
	t.Run("ts_reads_go_diarium", func(t *testing.T) {
		cmd := exec.Command("node", "--import", "tsx", "src/cli.ts", "diarium", "goecho")
		cmd.Dir = tsDir
		cmd.Env = append(os.Environ(), "ACTIO_DIR="+root)
		out, err := cmd.CombinedOutput()
		if err != nil || strings.TrimSpace(string(out)) == "" {
			t.Errorf("not demonstrated: the TS diarium over the Go diarium: %v: %s", err, out)
		}
	})
}
