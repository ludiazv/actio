// ar is a minimal CLI to test the accept/reject decision gate from model.go.
//
// Single:  ar [-caps "name: desc,name2"] <request...>
// Batch:   ar -batch corpus.json   (rows: {prompt, capabilities:[{name,description}], want, type})
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/MichaelKinsy/PiG/ai"
)

const base = "http://192.168.60.49:80"

// Config is a minimal stub of the fields newModels in model.go reads
// (the real one lives in config.go, which this harness does not use).
type Config struct {
	Model          string
	URL            string
	APIKey         string
	DecisionModel  string
	DecisionURL    string
	DecisionAPIKey string
	DecisionGate   string
}

// newM builds the models: fixed extraction model, chosen decision model.
func newM(decisionModel, extractModel string) *models {
	return newModels(Config{
		Model:         extractModel,
		URL:           base,
		DecisionModel: decisionModel,
		DecisionURL:   base,
		DecisionGate:  "v1",
	})
}

func main() {
	// flags may appear anywhere; everything else is the request.
	var caps, batch, model, mode = "echo", "", "d1-2b", "v0"
	var extractModel = "agents-a1-4b"
	extract := false
	var req []string
	for i := 1; i < len(os.Args); i++ {
		switch arg := os.Args[i]; arg {
		case "-caps", "--caps":
			i++
			caps = os.Args[i]
		case "-batch", "--batch":
			i++
			batch = os.Args[i]
		case "-model", "--model":
			i++
			model = os.Args[i]
		case "-extract-model":
			i++
			extractModel = os.Args[i]
		case "-sysmode":
			i++
			mode = os.Args[i]
		case "-forceround2":
			forceR2 = true
		case "-extract":
			extract = true
		case "-h", "--help":
			fmt.Fprintln(os.Stderr, `usage: ar [-caps "name: desc,name2"] [-model m] [-extract-model m] [-sysmode v0|v1|v2] <request...>
       ar -batch corpus.json [-model m]
       ar -extract [-sysmode v0|v1|v2] [-extract-model m] [-caps "name: desc,name2"] <request...>
       ar -extract -batch corpus.json [-extract-model m]`)
			os.Exit(0)
		default:
			req = append(req, arg)
		}
	}
	sysmode = mode

	m := newM(model, extractModel)
	if len(parseCaps(caps)) == 0 && extract {
		fmt.Println("rejected up front: destination has no callable capabilities")
		return
	}
	if batch != "" && extract {
		runExtractBatch(m, batch)
		return
	}
	if batch != "" {
		runBatch(m, batch)
		return
	}
	if len(req) == 0 {
		fmt.Fprintln(os.Stderr, `usage: ar [-caps "name: desc,name2"] [-model m] <request...>`)
		os.Exit(2)
	}

	if extract {
		runExtract(m, strings.TrimSpace(strings.Join(req, " ")), parseCaps(caps))
		return
	}

	v, err := runOne(m, strings.TrimSpace(strings.Join(req, " ")), parseCaps(caps))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	printVerdict(v)
}

// parseCaps splits "name: desc,name2" into capInfos.
func parseCaps(s string) []capInfo {
	var out []capInfo
	for _, e := range strings.Split(s, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if i := strings.Index(e, ":"); i >= 0 {
			out = append(out, capInfo{Name: strings.TrimSpace(e[:i]), Description: strings.TrimSpace(e[i+1:])})
		} else {
			out = append(out, capInfo{Name: e})
		}
	}
	return out
}

func runOne(m *models, prompt string, caps []capInfo) (gateVerdict, error) {
	return m.decide(context.Background(), prompt, caps)
}

// sysmode selects the extraction prompt design: v0 = shipped, v1 =
// static system (rules+examples) / user (data), no destination.
var sysmode = "v0"

// forceR2 is a test flag: force round 1 to fail so the round-2
// conversation continuation runs on demand.
var forceR2 = false

// systemV1 is the static v1 system prompt: role, contract, rules, examples.
func systemV1() string {
	return `You are a scheduling assistant that turns a user's request into a precise schedule definition.

Reply with JSON only: one object with exactly the keys sched_type, sched_expression, dest_capability, capability_parameters. No prose, no code fences.

sched_type is one of: one-shot, cron, periodic.
- one-shot: sched_expression is an RFC 3339 UTC time with whole seconds, after now, like 2026-12-01T09:30:00Z.
- cron: sched_expression is six fields (sec min hour dom month dow), optionally prefixed "CRON_TZ=<zone> "; without a prefix it is read in the config zone.
- periodic: sched_expression is "@every <duration>", whole seconds, at least 1s, like @every 30m.

dest_capability must be exactly one of the capability names listed in the request.
capability_parameters must be an object matching that capability's parameter schema; use {} if it takes none.

Examples:
{"sched_type":"one-shot","sched_expression":"2026-12-01T09:30:00Z","dest_capability":"scan","capability_parameters":{"profile":"fast"}}
{"sched_type":"cron","sched_expression":"0 0 13 * * *","dest_capability":"backup","capability_parameters":{}}
{"sched_type":"periodic","sched_expression":"@every 30m","dest_capability":"echo","capability_parameters":{"text":"ping"}}`
}

// userV1 is the v1 per-request user message: time, capabilities, hints, request.
func userV1(prompt string, caps []capInfo, h hints) string {
	now := time.Now().UTC()
	var b strings.Builder
	b.WriteString("Current time: " + now.Format(time.RFC3339) + " UTC (config zone: UTC).\n\n")
	b.WriteString("Available capabilities:\n")
	for _, c := range caps {
		schema, ok := harnessParams[c.Name]
		if !ok {
			schema = `{"type":"object","properties":{},"additionalProperties":false}`
		}
		b.WriteString(fmt.Sprintf("- %s: %s. parameters: %s\n", c.Name, c.Description, schema))
	}
	if h.Type != "" || h.Capability != "" {
		b.WriteString("\nDecision hints: type " + h.Type + ", capability " + h.Capability + ".\n")
	}
	b.WriteString("\nRequest: " + prompt + "\n")
	return b.String()
}

// prompts builds the (system, user) pair for the configured sysmode.
func prompts(prompt string, caps []capInfo, h hints) (string, string) {
	switch sysmode {
	case "v1":
		return systemV1(), userV1(prompt, caps, h)
	case "v2":
		if len(caps) == 1 {
			return systemV2(), userV2(prompt, caps, h)
		}
		return systemV1(), userV1(prompt, caps, h)
	}
	return systemPrompt(destCapCtx(caps), h), prompt
}

// systemV2 is v1's system prompt for a destination with a single
// capability: the contract drops dest_capability (the harness fills it).
func systemV2() string {
	return `You are a scheduling assistant that turns a user's request into a precise schedule definition.

Reply with JSON only: one object with exactly the keys sched_type, sched_expression, capability_parameters. No prose, no code fences.

sched_type is one of: one-shot, cron, periodic.
- one-shot: sched_expression is an RFC 3339 UTC time with whole seconds, after now, like 2026-12-01T09:30:00Z.
- cron: sched_expression is six fields (sec min hour dom month dow), optionally prefixed "CRON_TZ=<zone> "; without a prefix it is read in the config zone.
- periodic: sched_expression is "@every <duration>", whole seconds, at least 1s, like @every 30m.

capability_parameters must be an object matching the capability's parameter schema; use {} if it takes none.

Examples:
{"sched_type":"one-shot","sched_expression":"2026-12-01T09:30:00Z","capability_parameters":{"profile":"fast"}}
{"sched_type":"cron","sched_expression":"0 0 13 * * *","capability_parameters":{}}
{"sched_type":"periodic","sched_expression":"@every 30m","capability_parameters":{"text":"ping"}}`
}

// userV2 is v2's user message: the single capability is pre-bound.
func userV2(prompt string, caps []capInfo, h hints) string {
	now := time.Now().UTC()
	var b strings.Builder
	b.WriteString("Current time: " + now.Format(time.RFC3339) + " UTC (config zone: UTC).\n\n")
	c := caps[0]
	schema, ok := harnessParams[c.Name]
	if !ok {
		schema = `{"type":"object","properties":{},"additionalProperties":false}`
	}
	b.WriteString("The request targets the single available capability:\n")
	b.WriteString(fmt.Sprintf("- %s: %s. parameters: %s\n", c.Name, c.Description, schema))
	if h.Type != "" || h.Capability != "" {
		b.WriteString("\nDecision hints: type " + h.Type + ".\n")
	}
	b.WriteString("\nRequest: " + prompt + "\n")
	return b.String()
}

// injectDestCap fills dest_capability with the single capability's name in
// a v2 reply (the model is not asked for it).
func injectDestCap(reply, name string) string {
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(reply), &obj) != nil {
		return reply
	}
	obj["dest_capability"] = json.RawMessage(`"` + name + `"`)
	out, err := json.Marshal(obj)
	if err != nil {
		return reply
	}
	return string(out)
}

// harnessParams synthesizes parameter schemas for the corpus capabilities
// (the real ones come from the destination's actio view).
var harnessParams = map[string]string{
	"echo":    `{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`,
	"backup":  `{"type":"object","properties":{},"additionalProperties":false}`,
	"scan":    `{"type":"object","properties":{"profile":{"type":"string"}},"additionalProperties":false}`,
	"workers": `{"type":"object","properties":{},"additionalProperties":false}`,
}

// destCapCtx mirrors set.go's destCapContext for the harness.
func destCapCtx(caps []capInfo) string {
	var b strings.Builder
	b.WriteString("destination test-agent: test destination. capabilities: ")
	for i, c := range caps {
		if i > 0 {
			b.WriteString(", ")
		}
		schema, ok := harnessParams[c.Name]
		if !ok {
			schema = `{"type":"object","properties":{},"additionalProperties":false}`
		}
		b.WriteString(fmt.Sprintf("%s (%s) parameters: %s", c.Name, c.Description, schema))
	}
	return b.String()
}

// systemPrompt mirrors set.go's D-7 extraction prompt.
func systemPrompt(capCtx string, h hints) string {
	now := time.Now().UTC()
	var b strings.Builder
	b.WriteString("Reply with JSON only: one object with exactly the keys sched_type, sched_expression, dest_capability, capability_parameters.\n")
	b.WriteString("sched_type is one of: one-shot, cron, periodic.\n")
	b.WriteString("one-shot: sched_expression is an RFC 3339 UTC time with whole seconds, after now, like 2026-12-01T09:30:00Z.\n")
	b.WriteString("cron: sched_expression is six fields (sec min hour dom month dow), optionally prefixed CRON_TZ=<zone> ; a cron without a prefix is read in the config zone.\n")
	b.WriteString("periodic: sched_expression is @every <duration>, whole seconds of at least 1s, like @every 30m.\n")
	b.WriteString("Current time: " + now.Format(time.RFC3339) + " UTC; in UTC: " + now.Format("2006-01-02T15:04:05 -0700 (MST)") + ".\n")
	b.WriteString(capCtx + "\n")
	if h.Type != "" || h.Capability != "" {
		b.WriteString("Decision hints: type " + h.Type + ", capability " + h.Capability + ".\n")
	}
	return b.String()
}

// validateReplyLite runs the harness's D-6 checks: type in the set,
// capability visible, parameters an object.
func validateReplyLite(reply string, caps []capInfo) []string {
	var ans struct {
		SchedType            string          `json:"sched_type"`
		SchedExpression      string          `json:"sched_expression"`
		DestCapability       string          `json:"dest_capability"`
		CapabilityParameters json.RawMessage `json:"capability_parameters"`
	}
	if err := json.Unmarshal([]byte(reply), &ans); err != nil {
		return []string{"the reply is not one JSON object: " + err.Error()}
	}
	var reasons []string
	if ans.SchedType != "one-shot" && ans.SchedType != "cron" && ans.SchedType != "periodic" {
		reasons = append(reasons, fmt.Sprintf("sched_type %q: not one-shot, cron or periodic", ans.SchedType))
	}
	if ans.SchedExpression == "" {
		reasons = append(reasons, "sched_expression: empty")
	} else if er := checkExpression(ans.SchedType, ans.SchedExpression); er != "" {
		reasons = append(reasons, "sched_expression: "+er)
	}
	ok := false
	for _, c := range caps {
		if c.Name == ans.DestCapability {
			ok = true
		}
	}
	if !ok {
		reasons = append(reasons, fmt.Sprintf("dest_capability %q: not a visible capability of the destination", ans.DestCapability))
	}
	p := ans.CapabilityParameters
	if len(p) == 0 || string(p) == "null" {
		p = json.RawMessage("{}")
	}
	if len(p) == 0 || p[0] != '{' || json.Unmarshal(p, &struct{}{}) != nil {
		reasons = append(reasons, "capability_parameters: not an object")
	} else if schema, ok := harnessParams[ans.DestCapability]; ok {
		reasons = append(reasons, checkParamsLite(schema, p)...)
	}
	return reasons
}

// checkExpression validates the expression per type: harness mirror of
// set.go's parseSchedule (RFC 3339 whole-seconds future time / six-field
// cron with optional CRON_TZ / @every whole-second duration).
func checkExpression(typ, expr string) string {
	switch typ {
	case "one-shot":
		if strings.Contains(expr, ".") {
			return "must have whole seconds"
		}
		t, err := time.Parse(time.RFC3339, expr)
		if err != nil {
			return "not RFC 3339: " + err.Error()
		}
		if !t.After(time.Now().UTC().Add(-10 * time.Second)) {
			return "not after now"
		}
		return ""
	case "cron":
		s := expr
		if strings.HasPrefix(s, "CRON_TZ=") {
			rest := s[len("CRON_TZ="):]
			sp := strings.IndexByte(rest, ' ')
			if sp <= 0 || sp == len(rest)-1 {
				return "bad CRON_TZ prefix"
			}
			s = rest[sp+1:]
		}
		if f := strings.Fields(s); len(f) != 6 {
			return fmt.Sprintf("needs six fields (sec min hour dom month dow), got %d", len(f))
		}
		return ""
	case "periodic":
		if !strings.HasPrefix(expr, "@every ") {
			return "must be @every <duration>"
		}
		d, err := time.ParseDuration(strings.TrimPrefix(expr, "@every "))
		if err != nil {
			return "bad duration: " + err.Error()
		}
		if d < time.Second {
			return "must be at least 1s"
		}
		if d%time.Second != 0 {
			return "must have whole seconds"
		}
		return ""
	}
	return "unknown sched_type " + typ
}

// checkParamsLite validates params against a JSON schema: harness mirror of
// actio.CheckParams (required, additionalProperties, property types).
func checkParamsLite(schema string, params json.RawMessage) []string {
	var sch struct {
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
		Required             []string `json:"required"`
		AdditionalProperties *bool    `json:"additionalProperties"`
	}
	if json.Unmarshal([]byte(schema), &sch) != nil {
		return nil
	}
	var pm map[string]json.RawMessage
	if json.Unmarshal(params, &pm) != nil {
		return []string{"capability_parameters: not an object"}
	}
	var reasons []string
	for _, r := range sch.Required {
		if _, ok := pm[r]; !ok {
			reasons = append(reasons, fmt.Sprintf("capability_parameters: missing required %q", r))
		}
	}
	for k, v := range pm {
		prop, ok := sch.Properties[k]
		if !ok {
			if sch.AdditionalProperties != nil && !*sch.AdditionalProperties {
				reasons = append(reasons, fmt.Sprintf("capability_parameters: unknown property %q", k))
			}
			continue
		}
		if !typeOK(v, prop.Type) {
			reasons = append(reasons, fmt.Sprintf("capability_parameters: %q must be a %s", k, prop.Type))
		}
	}
	return reasons
}

// typeOK reports whether v decodes as a JSON value of the given schema type.
func typeOK(v json.RawMessage, typ string) bool {
	switch typ {
	case "string":
		var s string
		return json.Unmarshal(v, &s) == nil
	case "number":
		var f float64
		return json.Unmarshal(v, &f) == nil
	case "integer":
		var f float64
		return json.Unmarshal(v, &f) == nil && f == math.Trunc(f)
	case "boolean":
		var b bool
		return json.Unmarshal(v, &b) == nil
	case "object":
		return len(v) > 0 && v[0] == '{'
	case "array":
		return len(v) > 0 && v[0] == '['
	}
	return true
}

// runExtract runs the gate, then extraction rounds 1–2 like set.go.
func runExtract(m *models, prompt string, caps []capInfo) {
	v, err := runOne(m, prompt, caps)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gate error: %v\n", err)
		os.Exit(1)
	}
	printVerdict(v)
	if !v.OK {
		fmt.Println("gate rejected; extraction not run")
		return
	}
	system, user := prompts(prompt, caps, v.Hints)
	reply, reasons, el := extractRound(m, system, user, caps)
	fmt.Printf("round 1 (agents-a1-4b, %s):\n", el.Round(time.Millisecond))
	printReply(reply)
	fmt.Printf("validation: %s\n", verdictText(reasons))
	if forceR2 && len(reasons) == 0 {
		reasons = []string{"sched_expression: (forced by -forceround2) must be six cron fields"}
		fmt.Println("(round 1 forced to fail for the -forceround2 test)")
	}
	if len(reasons) > 0 {
		reply, reasons, el = extractRound2(m, system, user, reply, reasons, caps)
		fmt.Printf("round 2 (agents-a1-4b, %s):\n", el.Round(time.Millisecond))
		printReply(reply)
		fmt.Printf("validation: %s\n", verdictText(reasons))
	}
}

func verdictText(reasons []string) string {
	if len(reasons) == 0 {
		return "ok"
	}
	return "FAILED: " + strings.Join(reasons, "; ")
}

func printReply(reply string) {
	if reply == "" {
		fmt.Println("(empty)")
		return
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, []byte(reply), "", "  "); err != nil {
		buf.WriteString(reply)
	}
	fmt.Println(buf.String())
}

// extractRound2 continues the conversation: the model sees its first reply
// as an assistant message, then a new user message with the validation
// errors (D-6 step 4).
func extractRound2(m *models, system, firstUser, firstReply string, reasons []string, caps []capInfo) (string, []string, time.Duration) {
	msgs := []ai.Message{
		ai.UserMessage{Content: ai.UserText(firstUser)},
		ai.AssistantMessage{Content: []ai.AssistantContentBlock{ai.TextContent{Text: firstReply}}},
		ai.UserMessage{Content: ai.UserText("Your previous reply failed validation: " + strings.Join(reasons, "; ") +
			"\nReply again with the corrected JSON object only.")},
	}
	t0 := time.Now()
	reply, err := m.completeChat(context.Background(), system, msgs)
	el := time.Since(t0)
	if err != nil {
		return "", []string{"model error: " + err.Error()}, el
	}
	if sysmode == "v2" && len(caps) == 1 {
		reply = injectDestCap(reply, caps[0].Name)
	}
	return reply, validateReplyLite(reply, caps), el
}

// extractRound runs one extraction round and validates the reply.
func extractRound(m *models, system, user string, caps []capInfo) (string, []string, time.Duration) {
	t0 := time.Now()
	reply, err := m.complete(context.Background(), system, user)
	el := time.Since(t0)
	if err != nil {
		return "", []string{"model error: " + err.Error()}, el
	}
	if sysmode == "v2" && len(caps) == 1 {
		reply = injectDestCap(reply, caps[0].Name)
	}
	return reply, validateReplyLite(reply, caps), el
}

// runExtractBatch runs gate + extraction rounds over a corpus file.
func runExtractBatch(m *models, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	var rows []corpusRow
	if err := json.Unmarshal(data, &rows); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	okGate, r1pass, r2fix, finalPass, typeOK, nAccept, extractRej := 0, 0, 0, 0, 0, 0, 0
	start := time.Now()
	for i, r := range rows {
		t0 := time.Now()
		if len(r.Capabilities) == 0 {
			fmt.Printf("%02d PRE-REJECT no callable capabilities | %s\n", i+1, r.Prompt)
			continue
		}
		v, err := runOne(m, r.Prompt, r.Capabilities)
		if err != nil {
			fmt.Printf("%02d GATE-ERR %v | %s\n", i+1, err, r.Prompt)
			continue
		}
		got := "reject"
		if v.OK {
			got = "accept"
		}
		wantGate := r.Want
		if r.Want == "reject-extract" {
			wantGate = "accept" // gate should let it through; extraction must reject
		}
		gmark := "BAD"
		if got == wantGate {
			gmark = "ok"
			okGate++
		}
		if !v.OK {
			fmt.Printf("%02d gate=%-4s p=%-5.3f r1=-    r2=-    final=-          type=-    %4ds | %s\n",
				i+1, gmark, v.Score, int(time.Since(t0).Seconds()), r.Prompt)
			continue
		}
		nAccept++
		system, user := prompts(r.Prompt, r.Capabilities, v.Hints)
		reply, reasons, _ := extractRound(m, system, user, r.Capabilities)
		r1ok := len(reasons) == 0
		r1s := "FAIL"
		if r1ok {
			r1s = "ok"
			r1pass++
		}
		r2s := "-"
		if !r1ok {
			reply, reasons, _ = extractRound2(m, system, user, reply, reasons, r.Capabilities)
			if len(reasons) == 0 {
				r2s, r2fix = "ok", r2fix+1
			} else {
				r2s = "FAIL"
			}
		}
		finalOK := len(reasons) == 0
		finalType := "-"
		tmark := "-"
		fmark := "-"
		if finalOK {
			finalPass++
			var ans struct {
				SchedType string `json:"sched_type"`
			}
			if json.Unmarshal([]byte(reply), &ans) == nil {
				finalType = ans.SchedType
			}
			if r.Want == "accept" {
				if finalType == r.Type {
					tmark, typeOK = "ok", typeOK+1
				} else {
					tmark = "BAD"
				}
			}
			if r.Want == "reject-extract" {
				fmark = "BAD" // expected a rejection, got a schedule
			}
		} else {
			finalType = "REJECT"
			if r.Want == "reject-extract" {
				fmark, extractRej = "ok", extractRej+1
			} else {
				fmark = "BAD"
			}
		}
		fmt.Printf("%02d gate=%-4s p=%-5.3f r1=%-4s r2=%-4s final=%-9s type=%-4s fin=%-3s %4ds | %s\n",
			i+1, gmark, v.Score, r1s, r2s, finalType, tmark, fmark, int(time.Since(t0).Seconds()), r.Prompt)
	}
	fmt.Printf("\ngate: %d/%d verdicts | accepted: %d | r1-pass: %d | r2-fixed: %d | final-pass: %d | extract-rej-ok: %d | type-ok: %d | wall: %s\n",
		okGate, len(rows), nAccept, r1pass, r2fix, finalPass, extractRej, typeOK, time.Since(start).Round(time.Second))
}

func printVerdict(v gateVerdict) {
	outcome := "rejected"
	if v.OK {
		outcome = "accepted"
	}
	fmt.Printf("decision: %s\n", outcome)
	fmt.Printf("score:    %.3f\n", v.Score)
	if v.OK {
		fmt.Printf("hints:    type=%s capability=%s\n", v.Hints.Type, v.Hints.Capability)
	}
	fmt.Printf("time:     %s\n", v.t.Round(time.Millisecond))
}

type corpusRow struct {
	Prompt       string    `json:"prompt"`
	Capabilities []capInfo `json:"capabilities"`
	Want         string    `json:"want"`
	Type         string    `json:"type"`
}

func runBatch(m *models, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	var rows []corpusRow
	if err := json.Unmarshal(data, &rows); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	okV, okT, total, failed := 0, 0, len(rows), 0
	var totalT time.Duration
	for _, r := range rows {
		v, err := runOne(m, r.Prompt, r.Capabilities)
		if err != nil {
			failed++
			fmt.Printf("FAIL  %-8s %-9s %s: %v\n", r.Want, r.Type, r.Prompt, err)
			continue
		}
		totalT += v.t
		got := "reject"
		if v.OK {
			got = "accept"
		}
		mark := "ok"
		if got != r.Want {
			mark = "BAD "
		} else {
			okV++
			if r.Want == "accept" {
				if v.Hints.Type == r.Type {
					okT++
					mark += " "
				} else {
					mark = "type"
				}
			}
		}
		fmt.Printf("%s  want=%-8s got=%-8s type want=%-9s got=%-9s [%6.3f %4dms] %s\n",
			mark, r.Want, got, r.Type, v.Hints.Type, v.Score, v.t.Milliseconds(), r.Prompt)
	}
	fmt.Printf("\n%d/%d verdicts, %d/%d types, %d failed, avg %dms\n",
		okV, total, okT, total-failed, failed, totalT.Milliseconds()/int64(total))
}
