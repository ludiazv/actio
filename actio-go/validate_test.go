// The unit tests of validate.go: the good-document/minimal-break table over every validate*,
// the rules the schemas cannot express, the capability compile (D-19) and its traps, and the
// path form Validate. The conformance vectors live in vectors_test.go.
package actio

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
		if err := Validate(write("summarizer.n.json", goodAnnounce), ""); err != nil {
			t.Errorf("the protocol's announce file name <agent_id>.n.json was not inferred as an announce: %v", err)
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

// TestKindOfRoundTripsTheNamesTheLibraryWrites is the guard whose absence once let a wrong name rule through:
// the two file names the library itself writes into nuntium/ — <agent_id>.n.json (publishAnnounce)
// and nuntium.json (Broadcast) — must come back as "announce" from the library's own kind
// inference and must validate under it. If a future change to announceFileNames or KindOf breaks
// this, the message says exactly which name stopped round-tripping.
// TestValidateResolvesNameThenBytes pins the library's own resolution through Validate: the
// published name wins, a name that publishes nothing is settled by the bytes (envelopeKind), and
// when neither resolves the diagnosis names the file. This is the library's export; the CLI has
// its own name rule and does not use it.
func TestValidateResolvesNameThenBytes(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
		return p
	}
	// A valid request envelope (wrapped) body, from the library's own test vectors.
	requestBody, err := os.ReadFile(filepath.Join("testdata", "vectors", "request", "valid", "01-a1-request.json"))
	if err != nil {
		t.Fatal(err)
	}

	// The published name wins over the bytes: a wrapped request body in spec.json is settled as a
	// spec (the name), so the failure is a spec schema reason, not a request one.
	err = Validate(write("spec.json", string(requestBody)), "")
	if err == nil {
		t.Fatal("Validate(spec.json with a request body, \"\") = nil, want a spec schema failure")
	}
	if !strings.Contains(err.Error(), "spec.schema.json") {
		t.Errorf("the name did not decide first: the failure %q is not a spec schema reason", err)
	}

	// A name that publishes nothing is settled by the bytes: a wrapped request body in copy.json
	// is a request (the envelopeKind fallback, the library's own rule).
	if err = Validate(write("copy.json", string(requestBody)), ""); err != nil {
		t.Errorf("Validate(copy.json with a wrapped request body, \"\") = %v, want nil: the bytewise fallback is the library's", err)
	}

	// Neither the name nor the bytes resolve: the library's diagnosis, naming the file.
	err = Validate(write("junk.json", "not json at all"), "")
	if err == nil {
		t.Fatal("Validate(junk.json, \"\") = nil, want an error")
	}
	if !strings.Contains(err.Error(), "not a published name and not an envelope") {
		t.Errorf("the diagnosis %q does not say what it is: not a published name and not an envelope", err)
	}
}

func TestKindOfRoundTripsTheNamesTheLibraryWrites(t *testing.T) {
	root := t.TempDir()
	a := newTestAgent(t, root, "alpha", nil)
	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	announcePath := filepath.Join(root, "nuntium", "alpha.n.json")
	waitFor(t, 5*time.Second, "the agent's announce file", func() bool {
		_, err := os.Stat(announcePath)
		return err == nil
	})
	if kind := KindOf(announcePath); kind != KindAnnounce {
		t.Fatalf("KindOf(%q) = %q, want %q: the announce file name the library writes (<agent_id>.n.json, protocol :83) no longer parses as an announce", announcePath, kind, KindAnnounce)
	}
	if err := Validate(announcePath, ""); err != nil {
		t.Fatalf("Validate(%q, \"\") = %v: the announce the library wrote does not validate under its own inference", announcePath, err)
	}
	cfg := testConfig
	cfg.Root = root
	cfg, err := LoadConfig(cfg) // the timers and the clock
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if err := Broadcast(cfg, "a broadcast"); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	bcastPath := filepath.Join(root, "nuntium", "nuntium.json")
	if kind := KindOf(bcastPath); kind != KindAnnounce {
		t.Fatalf("KindOf(%q) = %q, want %q: nuntium/nuntium.json uses the announce format (protocol :193)", bcastPath, kind, KindAnnounce)
	}
	if err := Validate(bcastPath, ""); err != nil {
		t.Fatalf("Validate(%q, \"\") = %v: the broadcast the library wrote does not validate under its own inference", bcastPath, err)
	}
}

// TestKindOfRoundTripsTheEnvelopeNamesTheLibraryWrites is the guard for the protocol's envelope
// table: the four names <rid>.json, <rid>.can.json, <rid>.res.json and <rid>.con.json
// (doc/actio_protocol.md:97, :99, :101, :103; the kinds at :443-446) must come back as their
// kinds from KindOf and must validate, on the files a real exchange wrote — not on hand-made
// names. The exchange is run for real: a timed-out call produces the cancellation, a confirmed
// call the response and the confirmation.
func TestKindOfRoundTripsTheEnvelopeNamesTheLibraryWrites(t *testing.T) {
	root := t.TempDir()
	peer := newTestAgent(t, root, "peer", nil)
	emptyObject := json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`)
	if err := peer.Capability(Capability{Name: "echo", Description: "echoes", Parameters: emptyObject, Actio: CapabilityActio{Result: json.RawMessage("true")}},
		func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
			return map[string]any{}, nil
		}); err != nil {
		t.Fatal(err)
	}
	if err := peer.Capability(Capability{Name: "block", Description: "blocks until cancelled", Parameters: emptyObject, Actio: CapabilityActio{Result: json.RawMessage("true")}},
		func(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}); err != nil {
		t.Fatal(err)
	}
	if err := peer.Start(context.Background()); err != nil {
		t.Fatalf("Start(peer): %v", err)
	}
	cfg := testConfig
	cfg.Root = root
	cfg, err := LoadConfig(cfg) // the timers and the clock
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	// The exchange archives its own files once the confirmation is seen (A4, protocol :628),
	// and under a slow scheduler the archive can land before a read after the call. So the test
	// takes each envelope from the exchange's A4 record: the receiver's diarium inreq event
	// carries the envelopes as written (the archive's first step, :628), the json.RawMessage
	// fields keep the library's own bytes, and the files are written under the names the
	// protocol gives them (the rid of the request's header).
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
				RID RID `json:"rid"`
			}
			if json.Unmarshal(e.Data.Req, &h) != nil || string(h.RID) != rid {
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

	// A timed-out call: the requestor writes the request and the cancellation into the
	// receiver's inbox and gives up; the exchange is archived as cancelled (:739).
	if _, err := Call(context.Background(), cfg, AgentID("src"), AgentID("peer"), "block", map[string]any{}, RequestOptions{Timeout: 50 * time.Millisecond}); err != nil {
		t.Fatalf("Call(block): %v", err)
	}
	// A confirmed call: the response and the confirmation join the exchange in the
	// receiver's inbox (protocol :508); it is archived as confirmed.
	outcome, err := Call(context.Background(), cfg, AgentID("src"), AgentID("peer"), "echo", map[string]any{}, RequestOptions{Timeout: 5 * time.Second})
	if err != nil || outcome.Response == nil {
		t.Fatalf("Call(echo) = (%v, %v): no confirmed response", outcome, err)
	}
	waitFor(t, 5*time.Second, "both exchanges to be archived in the receiver's diarium", func() bool {
		data, err := os.ReadFile(filepath.Join(root, "peer", "diarium.jsonl"))
		if err != nil {
			return false
		}
		ridCancelled, ridConfirmed = "", ""
		for _, line := range strings.Split(string(data), "\n") {
			var e inreq
			if json.Unmarshal([]byte(line), &e) != nil || e.Event != "inreq" {
				continue
			}
			var h struct {
				RID RID `json:"rid"`
			}
			if json.Unmarshal(e.Data.Req, &h) != nil {
				continue
			}
			switch e.Data.FinalStatus {
			case "cancelled":
				ridCancelled = string(h.RID)
			case "confirmed":
				ridConfirmed = string(h.RID)
			}
		}
		return ridCancelled != "" && ridConfirmed != ""
	})

	for path, want := range map[string]string{
		writeEnv(ridCancelled, "cancelled", "req", ".json"):     KindRequest,      // <rid>.json, protocol :97
		writeEnv(ridCancelled, "cancelled", "can", ".can.json"): KindCancellation, // <rid>.can.json, :99
		writeEnv(ridConfirmed, "confirmed", "res", ".res.json"): KindResponse,     // <rid>.res.json, :101
		writeEnv(ridConfirmed, "confirmed", "con", ".con.json"): KindConfirmation, // <rid>.con.json, :103
	} {
		if got := KindOf(path); got != want {
			t.Errorf("KindOf(%q) = %q, want %q: a protocol envelope name (doc/actio_protocol.md:97, :99, :101, :103) no longer parses as its kind", path, got, want)
		}
		if err := Validate(path, ""); err != nil {
			t.Errorf("Validate(%q, \"\") = %v: the envelope the exchange wrote does not validate under its own inference", path, err)
		}
	}
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

// The read path must hand back the json.RawMessage fields with the bytes exactly as the file
// wrote them: the envelopes are stored as exchanged, and an audit that diffs a diarium envelope
// against the inbox files byte-for-byte depends on it. The write path was always byte-exact; the
// read path used to re-marshal the parsed document, and json.Marshal sorts every object's keys.

// writtenRequestVector is the real request vector as one JSON document, its surrounding
// whitespace removed. The diarium test compacts it further, because a diarium line is one line.
func writtenRequestVector(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "vectors", "request", "valid", "01-a1-request.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	compact, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return compact
}

// A diarium line whose keys are not alphabetical at any level reads back through ReadDiarium
// with the event_data and the envelope bytes exactly as written.
func TestDiariumReadKeepsEnvelopesAsWritten(t *testing.T) {
	vector := writtenRequestVector(t)
	eventData := `{"final_status":"sent","req":` + string(vector) + `,"can":null,"res":null,"con":null}`
	line := `{"id":"agent-a","datetime":"2026-10-24T10:05:00Z","event":"outreq","event_data":` + eventData + `}`
	folder := t.TempDir()
	if err := os.WriteFile(filepath.Join(folder, "diarium.jsonl"), []byte(line+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	read, err := ReadDiarium(folder)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Lines) != 1 || len(read.Ignored) != 0 {
		t.Fatalf("the line was not read back: lines=%d ignored=%+v", len(read.Lines), read.Ignored)
	}
	got := read.Lines[0].EventData
	if !bytes.Equal(got, []byte(eventData)) {
		t.Errorf("event_data came back reordered:\n got %s\nwant %s", got, eventData)
	}
	var envelopes ExchangeEnvelopes
	if err := json.Unmarshal(got, &envelopes); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(envelopes.Req, vector) {
		t.Errorf("the req envelope came back reordered:\n got %s\nwant %s", envelopes.Req, vector)
	}
}

// A request file read through validateRequestFile hands back the envelope with the bytes as
// written, since the same decodeInto serves it. The file is the real vector's values with its
// keys not alphabetical at any level.
func TestRequestFileKeepsTheEnvelopeAsWritten(t *testing.T) {
	body := `{"ts":1792836000,"nonce":"52ffe97b3a5e6e2f00a69edb27551465","signature":null,"name":"summarize","parameters":{"text":"Actio is a simple file/object-based agent coordination protocol ...","instructions":"one sentence"},"timeout":300,"attachments":[]}`
	fileBytes := []byte(`{"rid":"01JB7Q3W8K2M4N6P8R0S2T4V6X","src":"planner","dst":"summarizer","datetime":"2026-10-24T10:00:00Z","type":"req","actio_version":"0.1","request":` + body + `}`)
	file, err := validateRequestFile(fileBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(file.Request, []byte(body)) {
		t.Errorf("the request envelope came back reordered:\n got %s\nwant %s", file.Request, body)
	}
}
