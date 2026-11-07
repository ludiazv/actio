// Tests for the spec builder: the registration-order guarantee, the core profile's security
// fields, the optional-field semantics (absent, null and empty are distinct), and the validation
// that keeps an invalid spec out of the tree.
package actio

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func testOptions() Options {
	return Options{
		ID:                  "alpha",
		Description:         "a test agent",
		AcceptsCommandsFrom: []string{},
	}
}

func testCaps() []Capability {
	return []Capability{
		{
			Name:        "one",
			Description: "first",
			Parameters:  json.RawMessage(`{}`),
			Actio:       CapabilityActio{Result: json.RawMessage(`{}`)},
		},
		{
			Name:        "two",
			Description: "second",
			Parameters:  json.RawMessage(`{}`),
			Actio:       CapabilityActio{Result: json.RawMessage(`{}`)},
		},
	}
}

// TestBuildSpecDuplicateCapabilityRejected: a duplicate name is an error, because the schema cannot
// express uniqueness and a duplicate would make one capability disappear from every peer's view.
func TestBuildSpecDuplicateCapabilityRejected(t *testing.T) {
	caps := testCaps()
	caps[1] = caps[0]
	if _, err := buildSpec(testOptions(), caps, time.Now()); err == nil {
		t.Fatal("a duplicate capability name was accepted")
	}
}

// TestBuildSpecDatetimeFromClock: the datetime comes from the agent's clock, not the wall clock.
func TestBuildSpecDatetimeFromClock(t *testing.T) {
	fixed := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	spec, err := buildSpec(testOptions(), testCaps(), fixed)
	if err != nil {
		t.Fatalf("buildSpec: %v", err)
	}
	if spec.Datetime != "2025-06-01T12:00:00Z" {
		t.Errorf("datetime: %q, want the clock's value", spec.Datetime)
	}
}

// TestBuildSpecSecurityFields: the core profile's security fields are published — no support,
// trusted posture — and the capabilities come out in registration order.
func TestBuildSpecSecurityFields(t *testing.T) {
	spec, err := buildSpec(testOptions(), testCaps(), time.Now())
	if err != nil {
		t.Fatalf("buildSpec: %v", err)
	}
	if len(spec.SecuritySupport) != 0 {
		t.Errorf("security_support: %v, want an empty list", spec.SecuritySupport)
	}
	if spec.SecurityPosture != "trusted" {
		t.Errorf("security_posture: %q, want trusted", spec.SecurityPosture)
	}
	if len(spec.Capabilities) != 2 || spec.Capabilities[0].Name != "one" || spec.Capabilities[1].Name != "two" {
		t.Errorf("capabilities out of registration order: %+v", spec.Capabilities)
	}
	// An empty accepts_commands_from is written as an empty list, not as null.
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["accepts_commands_from"] == nil {
		t.Error("accepts_commands_from is null; an empty list means nobody, and it must be written")
	}
	if m["security_support"] == nil {
		t.Error("security_support is null; it must be an empty list")
	}
}

// TestBuildSpecOptionalFieldsAbsent: with no options, the optional keys are absent from the JSON —
// not null, not an empty list.
func TestBuildSpecOptionalFieldsAbsent(t *testing.T) {
	spec, err := buildSpec(testOptions(), testCaps(), time.Now())
	if err != nil {
		t.Fatalf("buildSpec: %v", err)
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{"namespaces", "depends", "wants", "info"} {
		if strings.Contains(string(raw), `"`+key+"`") {
			t.Errorf("the key %q is present: %s", key, raw)
		}
	}
	if spec.Namespaces != nil || spec.Depends != nil || spec.Wants != nil || spec.Info != nil {
		t.Errorf("the builder invented optional fields: %+v", spec)
	}
}

// TestBuildSpecOptionalFieldsDistinct: absent, null and an empty list are all legal for the
// namespaces, and all mean the null namespace; the builder keeps a listed null item a listed null
// item, and it copies the caller's slices so a later change cannot alter what was built.
func TestBuildSpecOptionalFieldsDistinct(t *testing.T) {
	// An empty list of namespaces: the null namespace, legal on its own.
	opts := testOptions()
	opts.Namespaces = []*string{}
	spec, err := buildSpec(opts, testCaps(), time.Now())
	if err != nil {
		t.Fatalf("buildSpec(empty namespaces): %v", err)
	}
	if spec.Namespaces == nil || len(spec.Namespaces) != 0 {
		t.Errorf("an empty namespace list became %v", spec.Namespaces)
	}

	// A null item: the void namespace listed explicitly, written as null.
	opts.Namespaces = []*string{nil}
	spec, err = buildSpec(opts, testCaps(), time.Now())
	if err != nil {
		t.Fatalf("buildSpec(null namespace): %v", err)
	}
	if spec.Namespaces == nil || len(spec.Namespaces) != 1 || spec.Namespaces[0] != nil {
		t.Errorf("the void namespace item: %v", spec.Namespaces)
	}
	raw, _ := json.Marshal(spec)
	if !strings.Contains(string(raw), `"namespaces":[null]`) {
		t.Errorf("the void namespace is not written as null: %s", raw)
	}

	// Deps and wants are copied, so a later change to the options cannot alter what was built.
	dep := "gamma"
	opts.Namespaces = nil
	opts.Depends = []string{dep}
	opts.Wants = []string{"delta"}
	spec, err = buildSpec(opts, testCaps(), time.Now())
	if err != nil {
		t.Fatalf("buildSpec(depends/wants): %v", err)
	}
	opts.Depends[0] = "changed"
	if spec.Depends[0] != "gamma" {
		t.Error("the builder shares the caller's slice; a later change altered the spec")
	}
}

// TestBuildSpecInvalidRejected: a spec that fails validation is an error, not a file in the tree.
func TestBuildSpecInvalidRejected(t *testing.T) {
	caps := testCaps()
	caps[0].Name = "" // the schema requires a name
	if _, err := buildSpec(testOptions(), caps, time.Now()); err == nil {
		t.Fatal("a spec with a nameless capability was accepted")
	}
}
