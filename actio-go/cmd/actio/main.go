// The actio command: ls, diarium, call, cmd, broadcast, validate over one shared actio
// root. It reads the same environment as the reference CLI (ACTIO_DIR and the timer overrides,
// resolved by the library) and consumes only the public API of the library.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"actio-go"
)

// defaultTimeout is the reference's DEFAULT_TIMEOUT: a CLI process has no heartbeat to keep it
// alive, so it must not send timeout 0 (cli.ts).
const defaultTimeout = 60 * time.Second

// usage is the reference's usage line, verbatim (cli.ts USAGE).
const usage = "usage: actio [--json] ls | diarium <agent> [-n N] | call <src-id> <dst> <capability> <json-params> [--timeout s (default 60)]" +
	" | cmd <src-id> <dst> <name> [json-params] | broadcast \"<text>\" | validate <file> [--as <kind>]"

// usageErr is a bad-arguments error: it is printed with the usage line, and the exit code is 1
// (cli.ts Usage).
type usageErr struct{ msg string }

func (e *usageErr) Error() string { return e.msg }

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// run parses the arguments, resolves the configuration and dispatches; it returns the process
// exit code.
func run(args []string, out, errW io.Writer) int {
	jsonMode, pos, n, timeout, as, perr := parseArgs(args)
	if perr != nil {
		usageFail(errW, perr.msg)
		return 1
	}
	if len(pos) == 0 {
		usageFail(errW, "")
		return 1
	}
	sub, rest := pos[0], pos[1:]
	switch sub {
	case "ls", "diarium", "call", "cmd", "broadcast", "validate":
	default:
		fmt.Fprintf(errW, "unknown subcommand %q\n%s\n", sub, usage)
		return 1
	}
	cfg, err := actio.LoadConfig(actio.Config{})
	if err != nil {
		fmt.Fprintf(errW, "config: %v\n", err)
		return 1
	}
	switch sub {
	case "ls":
		return lsCmd(cfg, jsonMode, out, errW)
	case "diarium":
		return diariumCmd(cfg, rest, n, jsonMode, out, errW)
	case "call":
		return sendCmd(context.Background(), cfg, rest, timeout, true, jsonMode, out, errW)
	case "cmd":
		return sendCmd(context.Background(), cfg, rest, timeout, false, jsonMode, out, errW)
	case "broadcast":
		return broadcastCmd(cfg, rest, out, errW)
	}
	return validateCmd(cfg, rest, as, out, errW)
}

// parseArgs scans the argument list the way the reference does (cli.ts parse): --json is global,
// -n, --timeout and --as take a value, everything else is positional, so a flag may come after a
// positional. The value flags are parsed by a fresh standard flag set per call, so no flag state
// leaks between calls.
func parseArgs(args []string) (jsonMode bool, pos []string, n int, timeout time.Duration, as string, err *usageErr) {
	fs := flag.NewFlagSet("actio", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fsN := fs.Int("n", 20, "")
	fsT := fs.Float64("timeout", 0, "")
	fsAs := fs.String("as", "", "")
	var rawTimeout string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch a {
		case "--json":
			jsonMode = true
		case "-n", "--timeout", "--as":
			if i+1 >= len(args) {
				return false, nil, 0, 0, "", &usageErr{a + " needs a value"}
			}
			i++
			if ferr := fs.Set(strings.TrimLeft(a, "-"), args[i]); ferr != nil {
				return false, nil, 0, 0, "", &usageErr{valueFlagMessage(a, args[i])}
			}
			if a == "-n" && *fsN < 1 { // the flag set accepts negative integers; the reference does not
				return false, nil, 0, 0, "", &usageErr{valueFlagMessage("-n", args[i])}
			}
			if a == "--timeout" {
				rawTimeout = args[i]
			}
		default:
			pos = append(pos, a)
		}
	}
	if rawTimeout != "" && *fsT <= 0 {
		return false, nil, 0, 0, "", &usageErr{valueFlagMessage("--timeout", rawTimeout)}
	}
	timeout = defaultTimeout // a CLI process has no heartbeat: never 0 (cli.ts)
	if rawTimeout != "" {
		timeout = time.Duration(*fsT * float64(time.Second))
	}
	return jsonMode, pos, *fsN, timeout, *fsAs, nil
}

// valueFlagMessage is the reference's usage message for a bad flag value (cli.ts num and
// timeoutOf), with its exact wording.
func valueFlagMessage(flagName, raw string) string {
	if flagName == "-n" {
		v, perr := strconv.ParseFloat(raw, 64)
		if perr != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return fmt.Sprintf(`-n must be a number >= 0, got %q`, raw)
		}
		return fmt.Sprintf(`-n must be a positive integer, got %q`, raw)
	}
	return fmt.Sprintf(`--timeout must be > 0 seconds, got %q`, raw)
}

// usageFail prints a usage error: the message (when any) and the usage line, as the reference
// does.
func usageFail(w io.Writer, msg string) {
	if msg == "" {
		fmt.Fprintln(w, usage)
		return
	}
	fmt.Fprintf(w, "%s\n%s\n", msg, usage)
}

// The --json shapes of ls and call: the same field names and order as the reference (cli.ts).
// The library's Outcome and IgnoredFile carry no JSON tags, so the CLI maps them.
type lsJSON struct {
	Root       string        `json:"root"`
	Counts     lsCounts      `json:"counts"`
	Agents     []agentJSON   `json:"agents"`
	Duplicates []string      `json:"duplicates"`
	Ignored    []ignoredJSON `json:"ignored"`
}

type lsCounts struct {
	Agents     int `json:"agents"`
	Duplicates int `json:"duplicates"`
	Ignored    int `json:"ignored"`
}

type agentJSON struct {
	ID           actio.AgentID     `json:"id"`
	Folder       string            `json:"folder"`
	Status       actio.StatusState `json:"status"`
	Datetime     string            `json:"datetime"`
	Health       actio.Health      `json:"health"`
	Capabilities []capJSON         `json:"capabilities"`
}

type capJSON struct {
	Name        string   `json:"name"`
	Disabled    bool     `json:"disabled"`
	AcceptsFrom []string `json:"accepts_from"`
}

type ignoredJSON struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

type outcomeJSON struct {
	FinalStatus string              `json:"finalStatus"`
	Response    *actio.ResponseBody `json:"response,omitempty"`
}

// printJSON is the reference's JSON.stringify(x, null, 2) through a sink.
func printJSON(w io.Writer, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	fmt.Fprintln(w, string(b))
}

// ls: what the scan found, with each agent's status, health and capabilities (cli.ts ls).
func lsCmd(cfg actio.Config, jsonMode bool, out, errW io.Writer) int {
	found, err := actio.Scan(cfg.Root)
	if err != nil {
		fmt.Fprintln(errW, err)
		return 1
	}
	now := cfg.Clock()
	agents := make([]agentJSON, 0, len(found.Agents))
	for _, a := range found.Agents {
		health := actio.HealthOf(a.Status, now)
		if health == "" {
			health = "unknown"
		}
		caps := make([]capJSON, 0, len(a.Spec.Capabilities))
		for _, c := range a.Spec.Capabilities {
			accepts := c.Actio.AcceptsFrom
			if accepts == nil {
				accepts = []string{}
			}
			caps = append(caps, capJSON{
				Name:        c.Name,
				Disabled:    c.Actio.Disabled != nil && *c.Actio.Disabled,
				AcceptsFrom: accepts,
			})
		}
		agents = append(agents, agentJSON{
			ID:           a.Spec.ID,
			Folder:       a.Folder,
			Status:       a.Status.Status,
			Datetime:     a.Status.Datetime,
			Health:       health,
			Capabilities: caps,
		})
	}
	if jsonMode {
		dups := found.Duplicates
		if dups == nil {
			dups = []string{}
		}
		ign := make([]ignoredJSON, 0, len(found.Ignored))
		for _, i := range found.Ignored {
			ign = append(ign, ignoredJSON{Path: i.Path, Reason: i.Reason})
		}
		printJSON(out, lsJSON{Root: cfg.Root, Counts: lsCounts{Agents: len(agents), Duplicates: len(dups), Ignored: len(ign)}, Agents: agents, Duplicates: dups, Ignored: ign})
		return 0
	}
	for _, a := range agents {
		fmt.Fprintf(out, "%s  %s  %s  %s  %s\n", a.ID, a.Status, a.Health, a.Datetime, a.Folder)
		for _, c := range a.Capabilities {
			hints := ""
			if c.Disabled {
				hints += " [disabled]"
			}
			if len(c.AcceptsFrom) > 0 {
				hints += " [accepts_from: " + strings.Join(c.AcceptsFrom, ",") + "]"
			}
			fmt.Fprintf(out, "    %s%s\n", c.Name, hints)
		}
	}
	for _, d := range found.Duplicates {
		fmt.Fprintf(out, "duplicate agent id: %s\n", d)
	}
	for _, i := range found.Ignored {
		fmt.Fprintf(out, "ignored: %s: %s\n", i.Path, i.Reason)
	}
	if len(agents) == 0 {
		fmt.Fprintf(out, "no agents under %s\n", cfg.Root)
	}
	return 0
}

// diarium <agent> [-n N]: the last N lines of the agent's diarium, shown as written (cli.ts
// readDiarium): this command reads, it does not judge.
func diariumCmd(cfg actio.Config, rest []string, n int, jsonMode bool, out, errW io.Writer) int {
	if len(rest) == 0 {
		usageFail(errW, "diarium needs an agent id")
		return 1
	}
	file := filepath.Join(cfg.Root, rest[0], "diarium.jsonl")
	raw, err := os.ReadFile(file)
	if err != nil {
		fmt.Fprintf(errW, "cannot read %q: %v\n", file, err)
		return 1
	}
	lines := make([]string, 0, 8)
	for _, l := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	for _, l := range lines {
		var v any
		if jsonMode && json.Unmarshal([]byte(l), &v) == nil {
			printJSON(out, v)
			continue
		}
		fmt.Fprintln(out, l)
	}
	return 0
}

// send is call and cmd: one exchange through the library's vehicle (cli.ts send). A peer's
// error answer is an outcome, not a failure: the exit code is 0 whenever a response arrived.
func sendCmd(ctx context.Context, cfg actio.Config, rest []string, timeout time.Duration, isCall, jsonMode bool, out, errW io.Writer) int {
	var (
		src, dst, name, rawParams string
		haveParams                bool
	)
	if isCall {
		if len(rest) < 4 {
			usageFail(errW, "call needs <src-id> <dst> <capability> <json-params> [--timeout s]")
			return 1
		}
		src, dst, name, rawParams, haveParams = rest[0], rest[1], rest[2], rest[3], true
	} else {
		if len(rest) < 3 {
			usageFail(errW, "cmd needs <src-id> <dst> <name> [json-params]")
			return 1
		}
		src, dst, name = rest[0], rest[1], rest[2]
		if len(rest) > 3 {
			rawParams, haveParams = rest[3], true
		}
	}
	var params any
	if haveParams {
		p, err := parseParams(rawParams)
		if err != nil {
			usageFail(errW, err.Error())
			return 1
		}
		params = p
	}
	if isCall {
		outcome, err := actio.Call(ctx, cfg, actio.AgentID(src), actio.AgentID(dst), name, params, actio.RequestOptions{Timeout: timeout})
		if err != nil {
			fmt.Fprintln(errW, err)
			return 1
		}
		printOutcome(outcome, jsonMode, out)
		if outcome.Response == nil {
			return 1
		}
		return 0
	}
	rid, err := actio.SendCommand(ctx, cfg, actio.AgentID(src), actio.AgentID(dst), name, params)
	if err != nil {
		fmt.Fprintln(errW, err)
		return 1
	}
	fmt.Fprintf(out, "rid: %s\n", rid)
	return 0
}

// parseParams is the reference's parseJson for the json-params positional (cli.ts). The parsed
// bytes are kept raw: the envelope carries them exactly as the operator typed them.
func parseParams(raw string) (any, error) {
	var probe any
	if err := json.Unmarshal([]byte(raw), &probe); err != nil {
		return nil, fmt.Errorf("json-params is not valid JSON: %v", err)
	}
	return json.RawMessage(raw), nil
}

// printOutcome is the reference's printOutcome (cli.ts).
func printOutcome(o actio.Outcome, jsonMode bool, out io.Writer) {
	if jsonMode {
		printJSON(out, outcomeJSON{FinalStatus: o.FinalStatus, Response: o.Response})
		return
	}
	fmt.Fprintf(out, "finalStatus: %s\n", o.FinalStatus)
	res := o.Response
	if res == nil {
		fmt.Fprintln(out, "response: none")
		return
	}
	if res.Error {
		fmt.Fprintf(out, "error: %s %s\n", errorField(res.Result, "code"), errorField(res.Result, "msg"))
	} else {
		fmt.Fprintf(out, "result: %s\n", string(res.Result))
	}
	if res.Cancellation {
		fmt.Fprintln(out, "cancellation: true")
	}
}

// errorField is the reference's String(result["code"]) / JSON.stringify(result["msg"]) (cli.ts):
// an absent field reads "undefined", as in the template.
func errorField(result json.RawMessage, key string) string {
	var m map[string]any
	if err := json.Unmarshal(result, &m); err != nil {
		return "undefined"
	}
	v, ok := m[key]
	if !ok || v == nil {
		return "undefined"
	}
	if key == "msg" {
		b, _ := json.Marshal(v)
		return string(b)
	}
	switch n := v.(type) {
	case float64:
		return strconv.FormatFloat(n, 'f', -1, 64)
	case string:
		return n
	}
	return "undefined"
}

// broadcast "<text>": the reserved signal file, written and validated by the library (cli.ts
// broadcast).
func broadcastCmd(cfg actio.Config, rest []string, out, errW io.Writer) int {
	if len(rest) == 0 {
		usageFail(errW, `broadcast needs "<text>"`)
		return 1
	}
	if err := actio.Broadcast(cfg, rest[0]); err != nil {
		fmt.Fprintf(errW, "broadcast: %v\n", err)
		return 1
	}
	fmt.Fprintf(out, "wrote %s\n", filepath.Join(cfg.Root, "nuntium", "nuntium.json"))
	return 0
}

// validateKinds is the reference's validator set, in its order (cli.ts VALIDATORS).
var validateKinds = []string{"actio", "spec", "status", "announce", "request", "cancellation", "response", "confirmation", "diarium"}

// validate <file> [--as kind] (cli.ts validate): OK <kind> on stdout, the reason on stderr, and
// the exit code 1.
func validateCmd(cfg actio.Config, rest []string, as string, out, errW io.Writer) int {
	if len(rest) == 0 {
		usageFail(errW, "validate needs <file>")
		return 1
	}
	file := rest[0]
	raw, err := os.ReadFile(file)
	if err != nil {
		fmt.Fprintf(errW, "cannot read %q: %v\n", file, err)
		return 1
	}
	kind := as
	if kind == "" {
		// The name decides, the reference's rule (cli.ts kindOf): the published names first, and a
		// bare <stem>.json is a request — the reference's own last line (cli.ts:312), the CLI's own
		// UX rule, not a second protocol table. It must not grow any other rule.
		kind = actio.KindOf(file)
		if kind == "" && strings.HasSuffix(filepath.Base(file), ".json") {
			kind = "request"
		}
		if kind == "" {
			noValidator(errW, filepath.Base(file))
			return 1
		}
	} else if !kindIn(validateKinds, kind) {
		noValidator(errW, kind)
		return 1
	}
	if verr := actio.Validate(file, kind); verr != nil {
		fmt.Fprintln(errW, verr)
		return 1
	}
	if kind == "diarium" {
		fmt.Fprintf(out, "OK diarium (%d lines)\n", countNonEmpty(raw))
		return 0
	}
	fmt.Fprintf(out, "OK %s\n", kind)
	return 0
}

// noValidator is the reference's "no validator" failure (cli.ts validate).
func noValidator(w io.Writer, label string) {
	fmt.Fprintf(w, "no validator for %q: use --as %s\n", label, strings.Join(validateKinds, "|"))
}

func kindIn(kinds []string, k string) bool {
	for _, v := range kinds {
		if v == k {
			return true
		}
	}
	return false
}

// countNonEmpty counts the non-blank lines of a diarium, as the reference counts them.
func countNonEmpty(raw []byte) int {
	n := 0
	for _, l := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(l) != "" {
			n++
		}
	}
	return n
}
