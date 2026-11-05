// The pin of the protocol contract at the level of the validator: every embedded schema must compile
// as it stands, with the dependency the plan approved. A schema that stopped parsing, or a dependency
// that changed under us, fails here rather than in a protocol test weeks later.
package actio

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"actio-go/internal/schema"
)

func TestEmbeddedSchemasCompile(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	names := []string{}

	entries, err := schema.Files.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the embedded schemas: %v", err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		names = append(names, entry.Name())
		raw, err := schema.Files.ReadFile(entry.Name())
		if err != nil {
			t.Fatalf("reading %s: %v", entry.Name(), err)
		}
		doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
		if err != nil {
			t.Fatalf("parsing %s: %v", entry.Name(), err)
		}
		if err := compiler.AddResource(entry.Name(), doc); err != nil {
			t.Fatalf("adding %s: %v", entry.Name(), err)
		}
	}

	if len(names) != 11 {
		t.Fatalf("%d embedded schemas, want 11: %v", len(names), names)
	}
	for _, name := range names {
		if _, err := compiler.Compile(name); err != nil {
			t.Errorf("compiling %s: %v", name, err)
		}
	}
}

// TestBadSchemaFailsAtCompile is the control: a schema with a keyword the validator cannot make
// sense of must fail the compile. That is how a peer's malformed capability schema is found
// (D-19), so the behaviour is pinned here rather than assumed.
func TestBadSchemaFailsAtCompile(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	var doc any
	if err := json.Unmarshal([]byte(`{"type":"objekt"}`), &doc); err != nil {
		t.Fatalf("building the control schema: %v", err)
	}
	if err := compiler.AddResource("control.schema.json", doc); err != nil {
		return // refused when added: refused early, which is what we need
	}
	if _, err := compiler.Compile("control.schema.json"); err == nil {
		t.Errorf("a schema whose type is \"objekt\" compiled: an unknown type would silently accept everything")
	}
}
