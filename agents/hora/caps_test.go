// caps_test.go pins the published capabilities and the list/unset
// permission matrix (D-2, D-13).
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

// TestPublishedCapabilities pins the three capabilities as published
// them: the names, the parameter schemas verbatim, and set's
// recommended_timeout.
func TestPublishedCapabilities(t *testing.T) {
	rows := []struct {
		name       string
		cap        actio.Capability
		wantParams string
	}{
		{
			name:       "list",
			cap:        listCap,
			wantParams: `{"type":"object","properties":{},"additionalProperties":false}`,
		},
		{
			name:       "unset",
			cap:        unsetCap,
			wantParams: `{"type":"object","properties":{"sched_id":{"type":"string","pattern":"^SCH-[0-9a-f]{8}$"}},"required":["sched_id"],"additionalProperties":false}`,
		},
		{
			name:       "set",
			cap:        setCap,
			wantParams: `{"type":"object","properties":{"prompt":{"type":"string","minLength":1},"dest_id":{"type":"string","minLength":1}},"required":["prompt"],"additionalProperties":false}`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			if row.cap.Name != row.name {
				t.Fatalf("name = %q, want %q", row.cap.Name, row.name)
			}
			if got := string(row.cap.Parameters); got != row.wantParams {
				t.Fatalf("parameters = %s, want the published schema verbatim", got)
			}
		})
	}
	t.Run("set publishes recommended_timeout 180", func(t *testing.T) {
		if got := setCap.Actio.RecommendedTimeout; got != 180 {
			t.Fatalf("recommended_timeout = %v, want 180", got)
		}
	})
}

// listHora builds a hora over the given store for the list/unset tests.
func listHora(t *testing.T, s *store) *hora {
	t.Helper()
	return &hora{
		cfg:   Config{Coordinators: []string{"coord"}},
		store: s,
		run:   newRunner(func(string) {}),
		now:   func() time.Time { return testNow },
	}
}

// seededStore is a store with the three entries the list/unset tests use:
// two of "own" and one of "other", with datetimes out of order on purpose.
func seededStore(t *testing.T) *store {
	t.Helper()
	s := mustStore(t)
	entries := []Entry{
		{SchedID: "SCH-00000002", Datetime: "2026-11-08T10:00:00Z", SchedType: "periodic", SchedExpression: "@every 30m", DestID: "own", DestCapability: "echo", CapabilityParameters: json.RawMessage(`{"text":"a"}`)},
		{SchedID: "SCH-00000001", Datetime: "2026-11-08T09:00:00Z", SchedType: "periodic", SchedExpression: "@every 10m", DestID: "own", DestCapability: "echo", CapabilityParameters: json.RawMessage(`{"text":"b"}`)},
		{SchedID: "SCH-00000003", Datetime: "2026-11-08T08:00:00Z", SchedType: "one-shot", SchedExpression: "2026-12-01T09:00:00Z", DestID: "other", DestCapability: "echo", CapabilityParameters: json.RawMessage(`{"text":"c"}`)},
	}
	for _, e := range entries {
		if _, err := s.add(e); err != nil {
			t.Fatalf("seed %s: %v", e.SchedID, err)
		}
	}
	return s
}

// TestList is D-2's read rule: the caller sees its entries; a coordinator
// sees every entry; sorted by datetime, then sched_id; [] when empty.
func TestList(t *testing.T) {
	ctx := context.Background()
	h := listHora(t, seededStore(t))

	t.Run("the owner sees its own entries, sorted by datetime", func(t *testing.T) {
		out, err := h.list(ctx, []byte(`{}`), &actio.CallContext{Src: "own"})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		b, _ := json.Marshal(out)
		want := `[{"sched_id":"SCH-00000001","dest_id":"own","sched_type":"periodic","dest_capability":"echo","sched_expression":"@every 10m"},{"sched_id":"SCH-00000002","dest_id":"own","sched_type":"periodic","dest_capability":"echo","sched_expression":"@every 30m"}]`
		if string(b) != want {
			t.Fatalf("list = %s, want %s", b, want)
		}
	})

	t.Run("the coordinator sees every entry, sorted", func(t *testing.T) {
		out, err := h.list(ctx, []byte(`{}`), &actio.CallContext{Src: "coord"})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		b, _ := json.Marshal(out)
		want := `[{"sched_id":"SCH-00000003","dest_id":"other","sched_type":"one-shot","dest_capability":"echo","sched_expression":"2026-12-01T09:00:00Z"},{"sched_id":"SCH-00000001","dest_id":"own","sched_type":"periodic","dest_capability":"echo","sched_expression":"@every 10m"},{"sched_id":"SCH-00000002","dest_id":"own","sched_type":"periodic","dest_capability":"echo","sched_expression":"@every 30m"}]`
		if string(b) != want {
			t.Fatalf("list = %s, want %s", b, want)
		}
	})

	t.Run("a caller with no entries sees an empty list", func(t *testing.T) {
		out, err := h.list(ctx, []byte(`{}`), &actio.CallContext{Src: "nobody"})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		b, _ := json.Marshal(out)
		if string(b) != "[]" {
			t.Fatalf("list = %s, want []", b)
		}
	})

	t.Run("an empty store answers an empty list", func(t *testing.T) {
		empty := listHora(t, mustStore(t))
		out, err := empty.list(ctx, []byte(`{}`), &actio.CallContext{Src: "own"})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		b, _ := json.Marshal(out)
		if string(b) != "[]" {
			t.Fatalf("list = %s, want []", b)
		}
	})
}

// TestUnset is D-2's write rule and D-13's order: the owner or a
// coordinator removes the entry; the same 1001 for the missing and the
// not-yours; the DB save before the answer.
func TestUnset(t *testing.T) {
	ctx := context.Background()

	run := func(t *testing.T, src, id string) (any, error) {
		h := listHora(t, seededStore(t))
		return h.unset(ctx, []byte(`{"sched_id":"`+id+`"}`), &actio.CallContext{Src: actio.AgentID(src)})
	}

	t.Run("the owner removes its entry", func(t *testing.T) {
		h := listHora(t, seededStore(t))
		// The entry is in the runner, so the unset's removal is what
		// takes it out.
		if err := h.run.add(Entry{SchedID: "SCH-00000002", SchedType: "periodic", SchedExpression: "@every 30m"}); err != nil {
			t.Fatalf("run.add: %v", err)
		}
		out, err := h.unset(ctx, []byte(`{"sched_id":"SCH-00000002"}`), &actio.CallContext{Src: "own"})
		if err != nil {
			t.Fatalf("unset: %v", err)
		}
		b, _ := json.Marshal(out)
		if string(b) != `{"result":"unset","sched_id":"SCH-00000002"}` {
			t.Fatalf("unset = %s, want the unset answer", b)
		}
		if _, ok := h.store.get("SCH-00000002"); ok {
			t.Fatalf("the entry is still in the store")
		}
		// fire reads the store first, so a job left in the runner would
		// fire nothing and nothing observable would change: the removal
		// is only visible from inside the hora.
		h.run.mu.Lock()
		_, inRunner := h.run.ids["SCH-00000002"]
		h.run.mu.Unlock()
		if inRunner {
			t.Fatal("the entry is still in the runner")
		}
	})

	t.Run("the coordinator removes someone else's entry", func(t *testing.T) {
		out, err := run(t, "coord", "SCH-00000003")
		if err != nil {
			t.Fatalf("unset: %v", err)
		}
		if out.(map[string]any)["result"] != "unset" {
			t.Fatalf("unset = %v, want the unset answer", out)
		}
	})

	t.Run("a stranger gets the same 1001 as a missing id", func(t *testing.T) {
		_, err := run(t, "nobody", "SCH-00000001")
		aerr, ok := err.(*actio.Error)
		if !ok {
			t.Fatalf("err = %v, want an actio error", err)
		}
		if aerr.Code != 1001 || aerr.Message != "unknown sched_id: SCH-00000001" {
			t.Fatalf("err = %+v, want 1001 unknown sched_id: SCH-00000001", aerr)
		}
	})

	t.Run("a missing id gets the same 1001", func(t *testing.T) {
		_, err := run(t, "own", "SCH-ffffffff")
		aerr, ok := err.(*actio.Error)
		if !ok {
			t.Fatalf("err = %v, want an actio error", err)
		}
		if aerr.Code != 1001 || aerr.Message != "unknown sched_id: SCH-ffffffff" {
			t.Fatalf("err = %+v, want 1001 unknown sched_id: SCH-ffffffff", aerr)
		}
	})

	t.Run("the DB save is before the answer", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "db.json")
		s, _, err := openStore(dbPath, testNow)
		if err != nil {
			t.Fatalf("openStore: %v", err)
		}
		if _, err := s.add(Entry{SchedID: "SCH-00000001", Datetime: "2026-11-08T09:00:00Z", SchedType: "periodic", SchedExpression: "@every 10m", DestID: "own", DestCapability: "echo", CapabilityParameters: json.RawMessage(`{"text":"b"}`)}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		h := listHora(t, s)
		if _, err := h.unset(ctx, []byte(`{"sched_id":"SCH-00000001"}`), &actio.CallContext{Src: "own"}); err != nil {
			t.Fatalf("unset: %v", err)
		}
		b, err := os.ReadFile(dbPath)
		if err != nil {
			t.Fatalf("read the DB: %v", err)
		}
		if strings.Contains(string(b), "SCH-00000001") {
			t.Fatalf("the DB still carries the removed entry: %s", b)
		}
	})
}
