// model_test.go pins findGate's registry lookup (D-9) with a test-only
// registry: each row saves gates, installs its own, and restores it.
package main

import (
	"testing"

	"github.com/MichaelKinsy/PiG/ai"
)

// mkGate builds a gate whose state marks its identity with mark.
func mkGate(name, mark string) gate {
	return gate{
		name:  name,
		state: func(prompt, capContext string) ai.JsonObject { return ai.JsonObject{"mark": mark} },
	}
}

func TestFindGate(t *testing.T) {
	rows := []struct {
		name     string
		query    string
		registry []gate
		wantMark string
		wantOK   bool
	}{
		{name: "present", query: "alpha", registry: []gate{mkGate("alpha", "a"), mkGate("beta", "b")}, wantMark: "a", wantOK: true},
		{name: "absent", query: "nope", registry: []gate{mkGate("alpha", "a"), mkGate("beta", "b")}, wantOK: false},
		{name: "duplicate name returns the first", query: "dup",
			registry: []gate{mkGate("dup", "first"), mkGate("dup", "second"), mkGate("gamma", "g")}, wantMark: "first", wantOK: true},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			saved := gates
			gates = row.registry
			t.Cleanup(func() { gates = saved })

			got, ok := findGate(row.query)
			if ok != row.wantOK {
				t.Fatalf("findGate(%q) ok = %v, want %v", row.query, ok, row.wantOK)
			}
			if !ok {
				return
			}
			if mark := got.state("", "")["mark"]; mark != row.wantMark {
				t.Fatalf("findGate(%q) returned the gate marked %v, want %v", row.query, mark, row.wantMark)
			}
		})
	}
}
