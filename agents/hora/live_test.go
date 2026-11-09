//go:build live

// live_test.go is the one live run against the real models: the three
// typical requests come back scheduled with the right type, and a question
// that is not a scheduling request comes back rejected. It needs
// HORA_LIVE_URL and fails without it; it never joins the standing check.
package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"actio-go"
)

// TestLive drives set's own path, with a real destination view, against the
// real chat and decision models.
func TestLive(t *testing.T) {
	url := os.Getenv("HORA_LIVE_URL")
	if url == "" {
		t.Fatal("HORA_LIVE_URL is not set: the live run needs the model server's URL")
	}
	root := t.TempDir()
	newTestAgent(t, root, "dest", "the live destination", func(a *actio.Agent) {
		if err := a.Capability(echoCap, func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
			var p struct {
				Text string `json:"text"`
			}
			_ = json.Unmarshal(params, &p)
			return map[string]any{"text": p.Text}, nil
		}); err != nil {
			t.Fatalf("Capability(echo): %v", err)
		}
	})
	caller := newTestAgent(t, root, "caller", "the live caller", nil)
	t.Setenv("ACTIO_DIR", root)
	cfg := `{"agent_id":"hora","TZ":"UTC","coordinators":["caller"],"db":"db.json","model":"lfm3-1.7b","url":"` + url +
		`","env_apikey":"","decision_model":"d1-2b","decision_url":"` + url + `","decision_env_apikey":"","decision_gate":"v1"}`
	cfgPath := filepath.Join(root, "cfg.json")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfgPath) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(40 * time.Second):
			t.Error("run did not return within 40 s")
		}
	})
	select {
	case err := <-done:
		t.Fatalf("run failed: %v", err)
	default:
	}
	see(t, caller, "hora")

	// The three typical requests, verbatim from Hora.md's typical use case,
	// and the question that is not a scheduling request.
	rows := []struct {
		prompt   string
		want     string
		wantType string
	}{
		{`Remind me to do a full network scan tomorrow at noon`, "scheduled", "one-shot"},
		{`Call for do a full back up on sundays 13h`, "scheduled", "cron"},
		{`send this prompt every 30 min 'Check workers status and if done start the final stage and unset the scheduler'`, "scheduled", "periodic"},
		{`what is the capital of France?`, "rejected", ""},
	}
	diarium := filepath.Join(root, "hora", "diarium.jsonl")
	for i, r := range rows {
		outcome, err := caller.Request(context.Background(), "hora", "set", map[string]any{"prompt": r.prompt, "dest_id": "dest"}, actio.RequestOptions{Timeout: 120 * time.Second})
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		res := map[string]any{}
		if outcome.Response != nil {
			_ = json.Unmarshal(outcome.Response.Result, &res)
		}
		got, _ := res["result"].(string)
		expr, _ := res["sched_expression"].(string)
		if got != r.want {
			t.Fatalf("row %d: %q came back %s, want %s: %v", i, r.prompt, got, r.want, res)
		}
		if r.want == "scheduled" && !exprHasType(expr, r.wantType) {
			t.Fatalf("row %d: %q stored %q, want type %s", i, r.prompt, expr, r.wantType)
		}
		// the decision's diarium line, so the gate's score is on record
		if g := lastGateLine(diarium); g != "" {
			t.Logf("row %d: %s", i, g)
		}
		t.Logf("row %d: prompt=%q result=%s sched_type=%s expression=%q", i, r.prompt, got, r.wantType, expr)
	}
}

// lastGateLine is the newest "gate" line of one diarium file, if any.
func lastGateLine(path string) string {
	lines := diariumLog(path)
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.HasPrefix(lines[i], "gate ") {
			return lines[i]
		}
	}
	return ""
}

// exprHasType reports whether the stored expression is in the stored form
// of the given type.
func exprHasType(expr, typ string) bool {
	switch typ {
	case "one-shot":
		_, err := time.Parse(time.RFC3339, expr)
		return err == nil
	case "periodic":
		return strings.HasPrefix(expr, "@every ")
	case "cron":
		f := strings.Fields(expr)
		return len(f) == 7 && strings.Contains(f[0], "=")
	}
	return false
}
