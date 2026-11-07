//go:build interop

// The interop suite: the Go and the TypeScript implementations of the protocol talk to each other
// over one shared actio root. It is behind the interop build tag, so its absence never breaks the
// default build or vet. Each row is green only when the two implementations really talked; a row
// that cannot be run (a missing binary, a Node quirk) is reported as not demonstrated, with the
// reason, and the test stays honest about it.
package actio

import (
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

	// The validate rows: each CLI validates a file the other implementation wrote.
	t.Run("ts_validates_go_spec", func(t *testing.T) {
		cmd := exec.Command("node", "--import", "tsx", "src/cli.ts", "validate", filepath.Join(root, "goecho", "spec.json"))
		cmd.Dir = tsDir
		cmd.Env = append(os.Environ(), "ACTIO_DIR="+root)
		out, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), "OK spec") {
			t.Errorf("not demonstrated: the TS validate over the Go spec: %v: %s", err, out)
		}
	})
	t.Run("go_validates_ts_spec", func(t *testing.T) {
		out, err := exec.Command(goCLI, "validate", filepath.Join(root, "tsecho", "spec.json")).CombinedOutput()
		if err != nil || !strings.Contains(string(out), "OK spec") {
			t.Errorf("not demonstrated: the Go validate over the TS spec: %v: %s", err, out)
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
