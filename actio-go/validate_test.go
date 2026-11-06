// The unit tests of validate.go: the good-document/minimal-break table over every validate*,
// the rules the schemas cannot express, the capability compile (D-19) and its traps, and the
// path form Validate. The conformance vectors live in vectors_test.go.
package actio

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The good documents, built here rather than copied from the vectors: one per validated shape.
// The header values are the same valid ones the vectors use.
const (
	goodSpec = `{"id":"summarizer","actio_version":"0.1","datetime":"2026-10-24T09:00:00Z",` +
		`"description":"Summarizes texts","accepts_commands_from":[],"capabilities":[],` +
		`"security_support":[],"security_posture":"trusted"}`
	goodStatus       = `{"id":"summarizer","status":"idle","datetime":"2026-10-24T09:00:00Z","capability_status":[]}`
	goodAnnounce     = `{"id":"summarizer","datetime":"2026-10-24T09:00:00Z","ts":1792836000,"nuntium":"agent is active"}`
	goodTeamMetadata = `{"actio_version":"0.1","description":"the team","metadata":{"size":1}}`
	goodRequestFile  = `{"rid":"01JB7Q3W8K2M4N6P8R0S2T4V6X","src":"planner","dst":"summarizer",` +
		`"datetime":"2026-10-24T10:00:00Z","type":"req","actio_version":"0.1",` +
		`"request":{"ts":1792836000,"nonce":"52ffe97b3a5e6e2f00a69edb27551465","signature":null,` +
		`"name":"summarize","parameters":null,"timeout":300,"attachments":[]}}`
	goodCancellationFile = `{"rid":"01JB7Q3W8K2M4N6P8R0S2T4V6X","src":"planner","dst":"summarizer",` +
		`"datetime":"2026-10-24T10:00:05Z","type":"req","actio_version":"0.1",` +
		`"cancellation":{"ts":1792836005,"nonce":"52ffe97b3a5e6e2f00a69edb27551465","signature":null,` +
		`"name":"summarize","force":false}}`
	goodResponseFile = `{"rid":"01JB7Q3W8K2M4N6P8R0S2T4V6X","src":"planner","dst":"summarizer",` +
		`"datetime":"2026-10-24T10:01:00Z","type":"req","actio_version":"0.1",` +
		`"response":{"ts":1792836060,"nonce":"52ffe97b3a5e6e2f00a69edb27551465","signature":null,` +
		`"error":false,"cancellation":false,"result":null,"attachments":[]}}`
	goodConfirmationFile = `{"rid":"01JB7Q3W8K2M4N6P8R0S2T4V6X","src":"planner","dst":"summarizer",` +
		`"datetime":"2026-10-24T10:01:02Z","type":"req","actio_version":"0.1",` +
		`"confirmation":{"ts":1792836062,"nonce":"52ffe97b3a5e6e2f00a69edb27551465","signature":null}}`
	goodDiariumLine = `{"id":"summarizer","datetime":"2026-10-24T10:00:03Z","event":"status","event_data":"idle"}`
	goodErrorResult = `{"code":101,"msg":"unknown_capability","details":"no such capability"}`
	goodRequestBody = `{"ts":1792836000,"nonce":"52ffe97b3a5e6e2f00a69edb27551465","signature":null,` +
		`"name":"summarize","parameters":{"text":"x"},"timeout":300,"attachments":[]}`
	goodCancellationBody = `{"ts":1792836005,"nonce":"52ffe97b3a5e6e2f00a69edb27551465","signature":null,` +
		`"name":"summarize","force":true}`
	goodResponseBody = `{"ts":1792836060,"nonce":"52ffe97b3a5e6e2f00a69edb27551465","signature":null,` +
		`"error":true,"cancellation":false,"result":{"code":101,"msg":"unknown_capability"},"attachments":[]}`
	goodConfirmationBody = `{"ts":1792836062,"nonce":"52ffe97b3a5e6e2f00a69edb27551465","signature":null}`
)

// mutate replaces one top-level key of a JSON document, or deletes it when value is nil.
func mutate(t *testing.T, doc, key string, value any) []byte {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(doc), &m); err != nil {
		t.Fatalf("unmarshalling the test document: %v", err)
	}
	if value == nil {
		delete(m, key)
	} else {
		m[key] = value
	}
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshalling the mutated document: %v", err)
	}
	return out
}

// TestValidateAcceptsGoodAndRejectsMinimalBreaks walks every validate* with a good document
// built in the test and a minimal break: a missing required field, a wrong pattern for
// rid/nonce/datetime, or a type that is not req where the schema says const. An invalid input
// is an error, never a zero value, and the error names the schema.
func TestValidateAcceptsGoodAndRejectsMinimalBreaks(t *testing.T) {
	cases := []struct {
		name     string
		validate func(raw []byte) (any, error)
		good     string
		bad      []byte
		badWhy   string
	}{
		{"spec", func(raw []byte) (any, error) { return validateSpec(raw) }, goodSpec, mutate(t, goodSpec, "description", nil), "missing description"},
		{"status", func(raw []byte) (any, error) { return validateStatus(raw) }, goodStatus, mutate(t, goodStatus, "capability_status", nil), "missing capability_status"},
		{"announce", func(raw []byte) (any, error) { return validateAnnounce(raw) }, goodAnnounce, mutate(t, goodAnnounce, "nuntium", nil), "missing nuntium"},
		{"team metadata", func(raw []byte) (any, error) { return validateTeamMetadata(raw) }, goodTeamMetadata,
			mutate(t, goodTeamMetadata, "bogus", 1), "an unknown property"},
		{"request file", func(raw []byte) (any, error) { return validateRequestFile(raw) }, goodRequestFile,
			mutate(t, goodRequestFile, "rid", "not-a-ulid"), "a rid outside the ULID pattern"},
		{"cancellation file", func(raw []byte) (any, error) { return validateCancellationFile(raw) }, goodCancellationFile,
			mutate(t, goodCancellationFile, "datetime", "2026-10-24 10:00:05"), "a datetime outside the pattern"},
		{"response file", func(raw []byte) (any, error) { return validateResponseFile(raw) }, goodResponseFile,
			mutate(t, goodResponseFile, "type", "cmd"), `a type that is not req where the schema says const`},
		{"confirmation file", func(raw []byte) (any, error) { return validateConfirmationFile(raw) }, goodConfirmationFile,
			mutate(t, goodConfirmationFile, "actio_version", "0.1.0"), "a version outside the pattern"},
		{"diarium line", func(raw []byte) (any, error) { return validateDiariumLine(raw) }, goodDiariumLine,
			mutate(t, goodDiariumLine, "event_data", nil), "missing event_data"},
		{"error result", func(raw []byte) (any, error) { return validateErrorResult(raw) }, goodErrorResult,
			mutate(t, goodErrorResult, "msg", nil), "missing msg"},
		{"request body", func(raw []byte) (any, error) { return validateRequestBody(raw) }, goodRequestBody,
			mutate(t, goodRequestBody, "nonce", "xyz"), "a nonce outside the pattern"},
		{"cancellation body", func(raw []byte) (any, error) { return validateCancellationBody(raw) }, goodCancellationBody,
			mutate(t, goodCancellationBody, "attachments", []any{}), "an unknown property"},
		{"response body", func(raw []byte) (any, error) { return validateResponseBody(raw) }, goodResponseBody,
			mutate(t, goodResponseBody, "result", nil), "missing result"},
		{"confirmation body", func(raw []byte) (any, error) { return validateConfirmationBody(raw) }, goodConfirmationBody,
			mutate(t, goodConfirmationBody, "signature", nil), "missing signature"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.validate([]byte(tc.good))
			if err != nil {
				t.Fatalf("a good document was rejected: %v", err)
			}
			if got == nil {
				t.Fatal("a valid document decoded to a nil value")
			}
			if _, err := tc.validate(tc.bad); err == nil {
				t.Fatalf("a broken document was accepted (%s)", tc.badWhy)
			} else if !strings.Contains(err.Error(), "actio:") {
				t.Errorf("the error does not name the schema: %v", err)
			}
		})
	}
}

// TestEnvelopeKind covers each of the four body keys, none, two, unparsable bytes, an array and
// null: it must never panic.
func TestEnvelopeKind(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
		ok   bool
	}{
		{"request", `{"rid":"x","request":{}}`, "request", true},
		{"cancellation", `{"cancellation":{}}`, "cancellation", true},
		{"response", `{"response":{}}`, "response", true},
		{"confirmation", `{"confirmation":{}}`, "confirmation", true},
		{"none", `{"rid":"x"}`, "", false},
		{"two", `{"request":{},"response":{}}`, "", false},
		{"unparsable", `{`, "", false},
		{"array", `[1,2]`, "", false},
		{"null", `null`, "", false},
		{"string", `"request"`, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := envelopeKind([]byte(tc.raw))
			if got != tc.want || ok != tc.ok {
				t.Errorf("envelopeKind(%s) = %q, %v; want %q, %v", tc.raw, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestSameHeader: equal except datetime is the same exchange (D-11); each of the five other
// fields changed is not.
func TestSameHeader(t *testing.T) {
	a := &Header{RID: "01JB7Q3W8K2M4N6P8R0S2T4V6X", Src: "planner", Dst: "summarizer",
		Datetime: "2026-10-24T10:00:00Z", Type: "req", ActioVersion: "0.1"}

	b := *a
	b.Datetime = "2026-10-24T10:01:00Z"
	if !sameHeader(a, &b) {
		t.Error("headers equal except datetime must match")
	}

	breaks := []struct {
		name   string
		change func(h *Header)
	}{
		{"rid", func(h *Header) { h.RID = "01JB7Q3W8K2M4N6P8R0S2T4V6Y" }},
		{"src", func(h *Header) { h.Src = "other" }},
		{"dst", func(h *Header) { h.Dst = "other" }},
		{"type", func(h *Header) { h.Type = "cmd" }},
		{"actio_version", func(h *Header) { h.ActioVersion = "0.2" }},
	}
	for _, tc := range breaks {
		t.Run(tc.name, func(t *testing.T) {
			c := *a
			c.Datetime = "2026-10-24T10:01:00Z"
			tc.change(&c)
			if sameHeader(a, &c) {
				t.Errorf("changing %s must break the match", tc.name)
			}
		})
	}
}

// TestNameMatches: the cancellation names the request's capability.
func TestNameMatches(t *testing.T) {
	req := &RequestBody{Name: "summarize"}
	if !nameMatches(req, &CancellationBody{Name: "summarize"}) {
		t.Error("equal names must match")
	}
	if nameMatches(req, &CancellationBody{Name: "translate"}) {
		t.Error("different names must not match")
	}
}

// TestDuplicateCapabilityName: the first repeated name, "" when unique, "" on an empty list.
func TestDuplicateCapabilityName(t *testing.T) {
	spec := &Spec{Capabilities: []Capability{
		{Name: "a"}, {Name: "b"}, {Name: "a"}, {Name: "b"},
	}}
	if got := duplicateCapabilityName(spec); got != "a" {
		t.Errorf("the first repeated name is %q, want %q", got, "a")
	}
	unique := &Spec{Capabilities: []Capability{{Name: "a"}, {Name: "b"}}}
	if got := duplicateCapabilityName(unique); got != "" {
		t.Errorf("unique names gave %q, want %q", got, "")
	}
	if got := duplicateCapabilityName(&Spec{}); got != "" {
		t.Errorf("an empty list gave %q, want %q", got, "")
	}
}

// capabilityFor builds a capability from its two schema documents.
func capabilityFor(name, params, result string) *Capability {
	return &Capability{
		Name:        name,
		Description: "a test capability",
		Parameters:  json.RawMessage(params),
		Actio:       CapabilityActio{Result: json.RawMessage(result)},
	}
}

// TestCompileCapability walks the D-19 compile: a schema that compiles and accepts/rejects, a
// boolean schema, a $ref to itself, the refused references, the refused lookahead pattern, the
// unknown keyword, and the repeated $id across capabilities.
func TestCompileCapability(t *testing.T) {
	t.Run("compiles and accepts and rejects", func(t *testing.T) {
		cap := capabilityFor("cap",
			`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`,
			`{"type":"string"}`)
		cs, err := compileCapability(cap)
		if err != nil {
			t.Fatalf("a well-formed capability did not compile: %v", err)
		}
		if err := cs.checkParams(json.RawMessage(`{"text":"x"}`)); err != nil {
			t.Errorf("fitting parameters were rejected: %v", err)
		}
		if err := cs.checkParams(json.RawMessage(`{"text":42}`)); err == nil {
			t.Error("wrong-typed parameters were accepted")
		}
		if err := cs.checkParams(json.RawMessage(`{}`)); err == nil {
			t.Error("missing required parameters were accepted")
		}
		if err := cs.checkResult(json.RawMessage(`"done"`)); err != nil {
			t.Errorf("a fitting result was rejected: %v", err)
		}
		if err := cs.checkResult(json.RawMessage(`42`)); err == nil {
			t.Error("a wrong-typed result was accepted")
		}
	})

	t.Run("boolean schemas", func(t *testing.T) {
		cap := capabilityFor("cap", `true`, `true`)
		cs, err := compileCapability(cap)
		if err != nil {
			t.Fatalf("boolean schemas did not compile: %v", err)
		}
		if err := cs.checkParams(json.RawMessage(`{"anything":1}`)); err != nil {
			t.Errorf("a boolean parameters schema accepts everything: %v", err)
		}
		if err := cs.checkResult(json.RawMessage(`null`)); err != nil {
			t.Errorf("a boolean result schema accepts everything: %v", err)
		}
	})

	t.Run("a $ref to itself is accepted", func(t *testing.T) {
		cap := capabilityFor("cap",
			`{"$id":"https://self.example/s","$defs":{"v":{"type":"string"}},"$ref":"https://self.example/s#/$defs/v"}`,
			`true`)
		cs, err := compileCapability(cap)
		if err != nil {
			t.Fatalf("a self-reference did not compile: %v", err)
		}
		if err := cs.checkParams(json.RawMessage(`"x"`)); err != nil {
			t.Errorf("a value the self-referenced schema accepts was rejected: %v", err)
		}
		if err := cs.checkParams(json.RawMessage(`42`)); err == nil {
			t.Error("a value the self-referenced schema rejects was accepted")
		}
	})

	t.Run("a $ref to a URL is refused", func(t *testing.T) {
		cap := capabilityFor("cap", `{"$ref":"http://example.com/x.json"}`, `true`)
		if _, err := compileCapability(cap); err == nil {
			t.Error("a $ref to a URL compiled: the loader must refuse every external reference")
		} else if !strings.Contains(err.Error(), "http://example.com/x.json") {
			t.Errorf("the refusal does not name the reference: %v", err)
		}
	})

	t.Run("a $ref to a file name is refused", func(t *testing.T) {
		cap := capabilityFor("cap", `{"$ref":"other.json"}`, `true`)
		if _, err := compileCapability(cap); err == nil {
			t.Error("a $ref to a file name compiled: the loader must refuse every external reference")
		}
	})

	t.Run("a lookahead pattern is refused", func(t *testing.T) {
		cap := capabilityFor("cap", `{"type":"string","pattern":"^(?!a)"}`, `true`)
		if _, err := compileCapability(cap); err == nil {
			t.Error("a pattern with a lookahead compiled: Go regular expressions are RE2")
		}
	})

	t.Run("an unknown keyword is refused", func(t *testing.T) {
		cap := capabilityFor("cap", `{"propertes":{}}`, `true`)
		_, err := compileCapability(cap)
		if err == nil {
			t.Fatal("a misspelled keyword compiled: unknown keywords must fail the compile")
		}
		if !strings.Contains(err.Error(), "propertes") {
			t.Errorf("the error does not name the unknown keyword: %v", err)
		}
		// the walker reaches nested schemas, in lists and in tables
		nested := capabilityFor("cap",
			`{"type":"object","properties":{"a":{"type":"string","propertes":{}}},"allOf":[{"propertes":{}}]}`,
			`true`)
		if _, err := compileCapability(nested); err == nil {
			t.Error("a misspelled keyword nested in properties and allOf compiled")
		}
		// a draft-04 construct the 2020-12 vocabulary does not define
		draft04 := capabilityFor("cap", `{"dependencies":{"a":["b"]}}`, `true`)
		if _, err := compileCapability(draft04); err == nil {
			t.Error("a draft-04 construct compiled: the 2020-12 vocabulary does not define it")
		}
	})

	t.Run("a repeated $id across capabilities compiles", func(t *testing.T) {
		mk := func(name string) *Capability {
			return capabilityFor(name,
				`{"$id":"https://x/y","type":"object","properties":{"n":{"type":"integer"}}}`,
				`{"$id":"https://x/y","type":"object"}`)
		}
		for _, name := range []string{"one", "two"} {
			cs, err := compileCapability(mk(name))
			if err != nil {
				t.Fatalf("capability %s with a repeated $id did not compile: %v", name, err)
			}
			if err := cs.checkParams(json.RawMessage(`{"n":1}`)); err != nil {
				t.Errorf("capability %s: fitting parameters were rejected: %v", name, err)
			}
		}
	})
}

// TestNilCapabilitySchemasSkipChecks: the D-19 skip path — a nil *capabilitySchemas means the
// checks are skipped, and both methods return nil.
func TestNilCapabilitySchemasSkipChecks(t *testing.T) {
	var cs *capabilitySchemas
	if err := cs.checkParams(json.RawMessage(`{"a":1}`)); err != nil {
		t.Errorf("checkParams on a nil receiver: %v", err)
	}
	if err := cs.checkResult(json.RawMessage(`null`)); err != nil {
		t.Errorf("checkResult on a nil receiver: %v", err)
	}
}

// TestCheckParams: the public form with Go values. A valid value passes, an invalid one names
// where, and a schema that does not compile skips the checks (D-19).
func TestCheckParams(t *testing.T) {
	cap := capabilityFor("cap",
		`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`,
		`true`)

	if err := CheckParams(*cap, map[string]any{"text": "hi"}); err != nil {
		t.Errorf("valid parameters were rejected: %v", err)
	}

	err := CheckParams(*cap, map[string]any{"text": 42})
	if err == nil {
		t.Fatal("invalid parameters were accepted")
	}
	if !strings.Contains(err.Error(), "/text") {
		t.Errorf("the error does not name the instance path: %v", err)
	}

	// a root-level failure says (root), as the reference message form does
	rooted := capabilityFor("cap", `{"type":"string"}`, `true`)
	if err := CheckParams(*rooted, map[string]any{"text": "hi"}); err == nil {
		t.Fatal("a value that is not a string was accepted by a string schema")
	} else if !strings.Contains(err.Error(), "(root)") {
		t.Errorf("the root failure does not say (root): %v", err)
	}

	// D-19: a schema that does not compile skips the checks
	skipped := capabilityFor("cap", `{"$ref":"http://example.com/x.json"}`, `true`)
	if err := CheckParams(*skipped, map[string]any{"text": "hi"}); err != nil {
		t.Errorf("an uncompilable schema must skip the checks, got: %v", err)
	}
}

// TestValidatePath: the path form over temp files — an inferred kind, an explicit kind (the
// --as form), a diarium with a bad third line, and a file whose kind cannot be inferred.
func TestValidatePath(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
		return p
	}

	t.Run("inferred kind from the file name", func(t *testing.T) {
		if err := Validate(write("spec.json", goodSpec), ""); err != nil {
			t.Errorf("spec.json was not inferred as a spec: %v", err)
		}
		if err := Validate(write("status.json", goodStatus), ""); err != nil {
			t.Errorf("status.json was not inferred as a status: %v", err)
		}
		if err := Validate(write("actio.json", goodTeamMetadata), ""); err != nil {
			t.Errorf("actio.json was not inferred as team metadata: %v", err)
		}
		if err := Validate(write("summarizer.12.json", goodAnnounce), ""); err != nil {
			t.Errorf("an announce file name was not inferred as an announce: %v", err)
		}
		if err := Validate(write("nuntium.json", goodAnnounce), ""); err != nil {
			t.Errorf("nuntium.json was not inferred as an announce: %v", err)
		}
		if err := Validate(write("diarium.jsonl", goodDiariumLine+"\n"), ""); err != nil {
			t.Errorf("diarium.jsonl was not inferred as a diarium: %v", err)
		}
		// an envelope name that publishes nothing is inferred from the bytes
		if err := Validate(write("01JB7Q3W8K2M4N6P8R0S2T4V6X.json", goodRequestFile), ""); err != nil {
			t.Errorf("an envelope was not inferred from its bytes: %v", err)
		}
		if err := Validate(write("01JB7Q3W8K2M4N6P8R0S2T4V6X.can.json", goodCancellationFile), ""); err != nil {
			t.Errorf("a cancellation was not inferred from its bytes: %v", err)
		}
	})

	t.Run("explicit kind, the --as form", func(t *testing.T) {
		p := write("some-name.json", goodResponseFile)
		if err := Validate(p, KindResponse); err != nil {
			t.Errorf("an explicit kind was not honored: %v", err)
		}
		// the explicit kind wins over a valid file name of a different kind
		if err := Validate(write("spec.json", goodRequestFile), KindRequest); err != nil {
			t.Errorf("the explicit kind must win over the file name: %v", err)
		}
		if err := Validate(write("spec.json", goodSpec), "no-such-kind"); err == nil {
			t.Error("an unknown explicit kind was accepted")
		}
	})

	t.Run("a diarium is validated line by line", func(t *testing.T) {
		badLine := `{"id":"summarizer","datetime":"2026-10-24T10:00:04Z","event":"status","event_data":"not-a-state"}`
		p := write("diarium.jsonl", goodDiariumLine+"\n"+goodDiariumLine+"\n"+badLine+"\n")
		err := Validate(p, "")
		if err == nil {
			t.Fatal("a diarium with a bad third line was accepted")
		}
		if !strings.Contains(err.Error(), "line 3") {
			t.Errorf("the error does not name line 3: %v", err)
		}
		// a trailing empty line is not a line
		if err := Validate(write("diarium.jsonl", goodDiariumLine+"\n"), ""); err != nil {
			t.Errorf("a trailing newline was treated as a bad line: %v", err)
		}
	})

	t.Run("a kind that cannot be inferred is an error", func(t *testing.T) {
		p := write("mystery.json", `{"whatever":1}`)
		if err := Validate(p, ""); err == nil {
			t.Fatal("a file of unknown kind was accepted")
		} else if !strings.Contains(err.Error(), "infer") {
			t.Errorf("the error does not say the kind cannot be inferred: %v", err)
		}
	})
}

// What a rejection's reason says: it must not depend on the working directory.
func TestPvReasonHasNoFileURI(t *testing.T) {
	for _, raw := range []string{
		`{"id":"a"}`,
		`{"id":"a","status":"nope","datetime":"x","capability_status":[]}`,
	} {
		_, err := validateStatus([]byte(raw))
		if err != nil && strings.Contains(err.Error(), "file://") {
			t.Errorf("a reason names a local path: %s", err.Error())
		}
	}
	_, err := validateDiariumLine([]byte(`{"id":"a","datetime":"2026-01-01T00:00:00Z","event":"log","event_data":5}`))
	if err != nil && strings.Contains(err.Error(), "file://") {
		t.Errorf("a diarium reason names a local path: %s", err.Error())
	}

	// The reasons are single lines that start with "actio: " plus the schema pointer; a newline
	// in a reason is the same bug in another shape.
	statusErr := func(raw string) error { _, e := validateStatus([]byte(raw)); return e }
	diariumErr := func(raw string) error { _, e := validateDiariumLine([]byte(raw)); return e }
	reasons := []struct {
		prefix string
		err    error
	}{
		{"actio: status.schema.json:", statusErr(`{"id":"a"}`)},
		{"actio: status.schema.json:", statusErr(`{"id":"a","status":"nope","datetime":"x","capability_status":[]}`)},
		{"actio: diarium.schema.json:", diariumErr(`{"id":"a","datetime":"2026-01-01T00:00:00Z","event":"log","event_data":5}`)},
	}
	for _, r := range reasons {
		if r.err == nil {
			t.Fatal("a document that must be rejected was accepted")
		}
		if strings.Contains(r.err.Error(), "\n") {
			t.Errorf("a reason is not a single line: %q", r.err.Error())
		}
		if !strings.HasPrefix(r.err.Error(), r.prefix) {
			t.Errorf("a reason does not start with %q: %q", r.prefix, r.err.Error())
		}
	}
}
