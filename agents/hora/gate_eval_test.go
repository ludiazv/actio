//go:build live

// gate_eval_test.go is the gate harness: it runs every registered gate (or
// only the names in HORA_GATES, comma-separated) over
// testdata/gate_corpus.json, through the same decide the agent uses,
// against d1-2b at HORA_LIVE_URL. It asserts the mechanics, not the
// accuracy: the table is read at the gate review and after the build. One
// copy at a time, never in the standing check. The decision goes through
// the same capInfo list set builds for the row's capabilities, so the
// measurement is of what the pipeline really sends.
package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// evalRow is one row of testdata/gate_corpus.json.
type evalRow struct {
	Prompt       string           `json:"prompt"`
	Capabilities []evalCapability `json:"capabilities"`
	Want         string           `json:"want"`
	Type         string           `json:"type"`
}

// evalCapability is one visible capability of a corpus row.
type evalCapability struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// gateEvalDeadline is the per-call deadline the harness gives decide (30 s,
// the shape the live rows use).
const gateEvalDeadline = 30 * time.Second

// TestGateEval measures the gates over the corpus (the plan's gate harness).
func TestGateEval(t *testing.T) {
	url := os.Getenv("HORA_LIVE_URL")
	if url == "" {
		t.Fatal("HORA_LIVE_URL is not set; the harness measures the live server and never skips")
	}

	// The corpus: it must parse and meet the minimums (16 rows; at least the
	// three typical requests plus 5 more good; at least 8 rejects).
	data, err := os.ReadFile("testdata/gate_corpus.json")
	if err != nil {
		t.Fatalf("read the corpus: %v", err)
	}
	var rows []evalRow
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatalf("the corpus does not parse: %v", err)
	}
	good, rejects := 0, 0
	for _, r := range rows {
		switch r.Want {
		case "accept":
			good++
		case "reject":
			rejects++
		default:
			t.Fatalf("row %q has want %q, want accept or reject", r.Prompt, r.Want)
		}
	}
	if len(rows) < 16 {
		t.Fatalf("the corpus has %d rows, want at least 16", len(rows))
	}
	if good < 8 {
		t.Fatalf("the corpus has %d good rows, want at least 8 (the three typical requests plus 5)", good)
	}
	if rejects < 8 {
		t.Fatalf("the corpus has %d reject rows, want at least 8", rejects)
	}

	// The gates: every registered one, or the names in HORA_GATES; a name
	// that is not in the registry fails the test.
	names := make([]string, 0, len(gates))
	for _, g := range gates {
		names = append(names, g.name)
	}
	if wanted := os.Getenv("HORA_GATES"); wanted != "" {
		names = nil
		for _, n := range strings.Split(wanted, ",") {
			if _, ok := findGate(n); !ok {
				t.Fatalf("HORA_GATES names %q, which is not in the registry", n)
			}
			names = append(names, n)
		}
	}

	// One gate at a time, one row at a time: nothing runs more than one
	// request at a time.
	for _, name := range names {
		m := newModels(Config{DecisionModel: "d1-2b", DecisionURL: url, DecisionGate: name})
		goodAccepted, rejectsRejected, typeRight, typeTotal, failed := 0, 0, 0, 0, 0
		var latency time.Duration
		for i, row := range rows {
			caps := make([]capInfo, 0, len(row.Capabilities))
			for _, c := range row.Capabilities {
				caps = append(caps, capInfo{Name: c.Name, Description: c.Description})
			}
			if i == 0 {
				t.Logf("caps row=01: %d capabilities", len(caps))
			}
			ctx, cancel := context.WithTimeout(context.Background(), gateEvalDeadline)
			start := time.Now()
			verdict, err := m.decide(ctx, row.Prompt, caps)
			cancel()
			elapsed := time.Since(start)
			latency += elapsed
			if err != nil {
				failed++
				t.Logf("gate=%s row=%02d want=%s ok=false score=0.00 type=- latency=%s error=%v", name, i+1, row.Want, elapsed, err)
				continue
			}
			t.Logf("gate=%s row=%02d want=%s ok=%v score=%.2f type=%s latency=%s", name, i+1, row.Want, verdict.OK, verdict.Score, verdict.Hints.Type, elapsed)
			if row.Want == "accept" {
				if verdict.OK {
					goodAccepted++
				}
				if row.Type != "" {
					typeTotal++
					if verdict.OK && verdict.Hints.Type == row.Type {
						typeRight++
					}
				}
			} else if !verdict.OK {
				rejectsRejected++
			}
		}
		t.Logf("SUMMARY gate=%s good=%d/%d rejects=%d/%d typeRight=%d/%d failed=%d meanLatency=%s",
			name, goodAccepted, good, rejectsRejected, rejects, typeRight, typeTotal, failed, latency/time.Duration(len(rows)))
	}
}
