// store_test.go pins the load, clean, check and save rules of Hora.md's
// Internal DB section (D-3): a bad DB fails the start and is left
// byte-for-byte as it was; a past one-shot is cleaned, not a failure.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// storeNow is the fixed clock of these tests.
var storeNow = time.Date(2026, 11, 8, 18, 0, 0, 0, time.UTC)

// writeDB writes body as a DB file in a temp directory and returns its path.
func writeDB(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db.json")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// sha256sum returns the hex digest of the file at path.
func sha256sum(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// entryJSON builds one DB row; the datetime is before storeNow on purpose, so
// callers that keep a one-shot must set its own expression.
func entryJSON(id, typ, expr string) string {
	return `{"datetime":"2026-11-08T17:00:00Z","sched_id":"` + id + `","sched_type":"` + typ +
		`","sched_expression":"` + expr + `","dest_id":"d","dest_capability":"c","capability_parameters":{"k":"v"}}`
}

// assertUnchanged fails the test if the file at path is not byte-for-byte the
// same as before.
func assertUnchanged(t *testing.T, path, before string) {
	t.Helper()
	if after := sha256sum(t, path); after != before {
		t.Fatalf("the DB file changed: %s -> %s", before, after)
	}
}

func TestOpenStoreMissingFileIsEmpty(t *testing.T) {
	s, cleaned, err := openStore(filepath.Join(t.TempDir(), "nope.json"), storeNow)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	if len(s.all()) != 0 || cleaned != nil {
		t.Fatalf("not empty: %v %v", s.all(), cleaned)
	}
}

func TestLoadNotAnArrayFailsFileUnchanged(t *testing.T) {
	for _, body := range []string{
		`{"datetime":"2026-11-08T17:00:00Z"}`,
		`hello`,
	} {
		path := writeDB(t, body)
		before := sha256sum(t, path)
		_, _, err := openStore(path, storeNow)
		if err == nil || !strings.Contains(err.Error(), "not a JSON array") {
			t.Fatalf("%s: %v", body, err)
		}
		assertUnchanged(t, path, before)
	}
}

func TestLoadBadFieldFailsFileUnchanged(t *testing.T) {
	rows := []struct {
		name string
		body string
		want string
	}{
		{"bad datetime", `{"datetime":"2026-11-08 17:00:00","sched_id":"SCH-00000001","sched_type":"periodic","sched_expression":"@every 30s","dest_id":"d","dest_capability":"c","capability_parameters":{}}`, "datetime"},
		{"bad sched_id", entryJSON("XYZ", "periodic", "@every 30s"), "sched_id"},
		{"unknown sched_type", entryJSON("SCH-00000001", "hourly", "@every 30s"), "unknown schedule type"},
		{"expression not for its type", entryJSON("SCH-00000001", "periodic", "0 0 1 * * *"), "periodic"},
		{"params not an object", `{"datetime":"2026-11-08T17:00:00Z","sched_id":"SCH-00000001","sched_type":"periodic","sched_expression":"@every 30s","dest_id":"d","dest_capability":"c","capability_parameters":[1]}`, "capability_parameters"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			path := writeDB(t, "["+row.body+"]")
			before := sha256sum(t, path)
			_, _, err := openStore(path, storeNow)
			if err == nil || !strings.Contains(err.Error(), "entry 0") || !strings.Contains(err.Error(), row.want) {
				t.Fatalf("error %v does not name the index and the reason (%s)", err, row.want)
			}
			assertUnchanged(t, path, before)
		})
	}
}

func TestLoadDuplicateIDFailsFileUnchanged(t *testing.T) {
	body := "[" + entryJSON("SCH-00000001", "periodic", "@every 30s") + "," +
		entryJSON("SCH-00000001", "periodic", "@every 60s") + "]"
	path := writeDB(t, body)
	before := sha256sum(t, path)
	_, _, err := openStore(path, storeNow)
	if err == nil || !strings.Contains(err.Error(), "duplicate sched_id") || !strings.Contains(err.Error(), "entry 1") {
		t.Fatalf("error %v does not name the index", err)
	}
	assertUnchanged(t, path, before)
}

// a stored cron expression must carry its zone: the loader has no config zone to fall back on.
func TestLoadCronWithoutPrefixFailsAndFileUnchanged(t *testing.T) {
	path := writeDB(t, "["+entryJSON("SCH-00000001", "cron", "0 0 9 * * *")+"]")
	before := sha256sum(t, path)
	_, _, err := openStore(path, storeNow)
	if err == nil {
		t.Fatal("a prefix-less cron entry loaded")
	}
	if !strings.Contains(err.Error(), "entry 0") || !strings.Contains(err.Error(), "CRON_TZ=<zone>") {
		t.Fatalf("error %q does not name the index and say to add CRON_TZ=<zone>", err)
	}
	assertUnchanged(t, path, before)
	t.Logf("error: %v", err)
	t.Logf("sha256 before=%s after=%s", before, sha256sum(t, path))
}

// CRON_TZ= and TZ= entries load.
func TestLoadCronWithPrefixesLoads(t *testing.T) {
	for _, expr := range []string{"CRON_TZ=Europe/Madrid 0 0 13 * * 0", "TZ=UTC 0 0 9 * * *"} {
		t.Run(expr, func(t *testing.T) {
			path := writeDB(t, "["+entryJSON("SCH-00000001", "cron", expr)+"]")
			s, _, err := openStore(path, storeNow)
			if err != nil {
				t.Fatalf("openStore: %v", err)
			}
			all := s.all()
			if len(all) != 1 || all[0].SchedExpression != expr {
				t.Fatalf("entries: %v", all)
			}
		})
	}
}

// a past one-shot is cleaned, not rejected: the start succeeds and the DB is saved.
func TestCleanPastOneShotSavesAndStarts(t *testing.T) {
	body := "[" + entryJSON("SCH-aaaaaaaa", "one-shot", "2026-11-08T17:00:00Z") + "," +
		entryJSON("SCH-bbbbbbbb", "one-shot", "2026-11-08T19:00:00Z") + "]"
	path := writeDB(t, body)
	s, cleaned, err := openStore(path, storeNow)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	if len(cleaned) != 1 || cleaned[0].SchedID != "SCH-aaaaaaaa" {
		t.Fatalf("cleaned: %v", cleaned)
	}
	all := s.all()
	if len(all) != 1 || all[0].SchedID != "SCH-bbbbbbbb" {
		t.Fatalf("entries: %v", all)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte("SCH-aaaaaaaa")) {
		t.Fatal("the past entry is still in the saved file")
	}
	if !bytes.Contains(b, []byte("SCH-bbbbbbbb")) {
		t.Fatal("the future entry is missing from the saved file")
	}
}

// a one-shot that is not RFC 3339 is a load error.
func TestLoadNonRFC3339OneShotFails(t *testing.T) {
	path := writeDB(t, "["+entryJSON("SCH-cccccccc", "one-shot", "tomorrow at nine")+"]")
	before := sha256sum(t, path)
	_, _, err := openStore(path, storeNow)
	if err == nil || !strings.Contains(err.Error(), "entry 0") || !strings.Contains(err.Error(), "not RFC 3339") {
		t.Fatalf("error: %v", err)
	}
	assertUnchanged(t, path, before)
	t.Logf("error: %v", err)
	t.Logf("sha256 before=%s after=%s", before, sha256sum(t, path))
}

func TestCleanOneShotAtNowIsRemoved(t *testing.T) {
	path := writeDB(t, "["+entryJSON("SCH-aaaaaaaa", "one-shot", "2026-11-08T18:00:00Z")+"]")
	s, cleaned, err := openStore(path, storeNow)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	if len(cleaned) != 1 || len(s.all()) != 0 {
		t.Fatalf("cleaned %v entries %v", cleaned, s.all())
	}
}

func TestLoadOneShotFractionalSecondFails(t *testing.T) {
	path := writeDB(t, "["+entryJSON("SCH-dddddddd", "one-shot", "2026-11-08T19:00:00.5Z")+"]")
	before := sha256sum(t, path)
	_, _, err := openStore(path, storeNow)
	if err == nil || !strings.Contains(err.Error(), "entry 0") || !strings.Contains(err.Error(), "fractional second") {
		t.Fatalf("error: %v", err)
	}
	assertUnchanged(t, path, before)
}

func TestLoadNullParamsBecomeEmptyObject(t *testing.T) {
	body := `{"datetime":"2026-11-08T17:00:00Z","sched_id":"SCH-00000001","sched_type":"periodic","sched_expression":"@every 30s","dest_id":"d","dest_capability":"c","capability_parameters":null}`
	path := writeDB(t, "["+body+"]")
	s, _, err := openStore(path, storeNow)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	if got := string(s.all()[0].CapabilityParameters); got != "{}" {
		t.Fatalf("params: %s", got)
	}
}

func TestAddGeneratesIDAndSaves(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.json")
	s, _, err := openStore(path, storeNow)
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.add(Entry{Datetime: "2026-07-22T00:00:00Z", SchedType: "cron", SchedExpression: "CRON_TZ=UTC 0 0 * * *", DestID: "d", DestCapability: "c"})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if !schedIDRe.MatchString(e.SchedID) {
		t.Fatalf("generated id %q does not match the format", e.SchedID)
	}
	if e.Datetime != "2026-07-22T00:00:00Z" {
		t.Fatalf("add changed the datetime: %q", e.Datetime)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(e.SchedID)) {
		t.Fatalf("the file has no %s: %s", e.SchedID, b)
	}
	if !bytes.Contains(b, []byte(`"capability_parameters":{}`)) {
		t.Fatalf("null params were written, not {}: %s", b)
	}
}

func TestAddKeepsAPresetID(t *testing.T) {
	s, _, err := openStore(filepath.Join(t.TempDir(), "db.json"), storeNow)
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.add(Entry{SchedID: "SCH-12345678", SchedType: "periodic", SchedExpression: "@every 30s", DestID: "d", DestCapability: "c"})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if e.SchedID != "SCH-12345678" {
		t.Fatalf("preset id not kept: %q", e.SchedID)
	}
	if got, ok := s.get("SCH-12345678"); !ok || got.SchedID != "SCH-12345678" {
		t.Fatalf("get: %v %v", got, ok)
	}
	t.Logf("add kept the preset id: %s", e.SchedID)
}

func TestAddTakenIDConflicts(t *testing.T) {
	path := writeDB(t, "["+entryJSON("SCH-12345678", "periodic", "@every 30s")+"]")
	s, _, err := openStore(path, storeNow)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.add(Entry{SchedID: "SCH-12345678", SchedType: "periodic", SchedExpression: "@every 60s", DestID: "d", DestCapability: "c"})
	if err == nil || !strings.Contains(err.Error(), "taken") {
		t.Fatalf("conflict: %v", err)
	}
	t.Logf("conflict: %v", err)
	if len(s.all()) != 1 {
		t.Fatalf("the failed add changed the store: %v", s.all())
	}
	b, _ := os.ReadFile(path)
	if bytes.Count(b, []byte("SCH-12345678")) != 1 {
		t.Fatalf("the file changed: %s", b)
	}
}

func TestRemove(t *testing.T) {
	body := "[" + entryJSON("SCH-11111111", "periodic", "@every 30s") + "," +
		entryJSON("SCH-22222222", "periodic", "@every 60s") + "]"
	path := writeDB(t, body)
	s, _, err := openStore(path, storeNow)
	if err != nil {
		t.Fatal(err)
	}
	e, ok, err := s.remove("SCH-11111111")
	if err != nil || !ok || e.SchedID != "SCH-11111111" {
		t.Fatalf("remove: %v %v %v", e, ok, err)
	}
	b, _ := os.ReadFile(path)
	if bytes.Contains(b, []byte("SCH-11111111")) || !bytes.Contains(b, []byte("SCH-22222222")) {
		t.Fatalf("the file after remove: %s", b)
	}
	if _, ok, err := s.remove("SCH-11111111"); err != nil || ok {
		t.Fatalf("second remove: %v %v", ok, err)
	}
}

func TestGetAndAll(t *testing.T) {
	body := "[" + entryJSON("SCH-11111111", "periodic", "@every 30s") + "," +
		entryJSON("SCH-22222222", "periodic", "@every 60s") + "]"
	s, _, err := openStore(writeDB(t, body), storeNow)
	if err != nil {
		t.Fatal(err)
	}
	e, ok := s.get("SCH-22222222")
	if !ok || e.DestID != "d" || e.CapabilityParameters[0] != '{' {
		t.Fatalf("get: %v %v", e, ok)
	}
	if _, ok := s.get("SCH-99999999"); ok {
		t.Fatal("get found a missing id")
	}
	all := s.all()
	if len(all) != 2 {
		t.Fatalf("all: %v", all)
	}
	all[0].SchedID = "touched"
	if s.all()[0].SchedID != "SCH-11111111" {
		t.Fatal("all returned the internal slice")
	}
}

func TestSaveModeAndFieldOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.json")
	s, _, err := openStore(path, storeNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.add(Entry{SchedID: "SCH-00000001", SchedType: "periodic", SchedExpression: "@every 30s", DestID: "d", DestCapability: "c", CapabilityParameters: json.RawMessage(`{"k":"v"}`)}); err != nil {
		t.Fatalf("add: %v", err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0600 {
		t.Fatalf("mode: %v", st.Mode())
	}
	b, _ := os.ReadFile(path)
	keys := []string{"\"datetime\"", "\"sched_id\"", "\"sched_type\"", "\"sched_expression\"", "\"dest_id\"", "\"dest_capability\"", "\"capability_parameters\""}
	last := -1
	for _, k := range keys {
		i := bytes.Index(b, []byte(k))
		if i < 0 || i < last {
			t.Fatalf("field order in the file: %s", b)
		}
		last = i
	}
}

// TestSaveFailsOnReadOnlyDB is the tamper row: with the DB file chmod 0400,
// the save must fail, and the file and the store must be left as they were.
func TestSaveFailsOnReadOnlyDB(t *testing.T) {
	path := writeDB(t, "[]")
	s, _, err := openStore(path, storeNow)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	_, err = s.add(Entry{SchedType: "periodic", SchedExpression: "@every 30s", DestID: "d", DestCapability: "c"})
	if err == nil {
		t.Fatal("the save succeeded into a read-only DB")
	}
	t.Logf("save error: %v", err)
	if !strings.Contains(err.Error(), "not writable") {
		t.Fatalf("error %q does not say the DB is not writable", err)
	}
	b, _ := os.ReadFile(path)
	if !bytes.Equal(bytes.TrimSpace(b), []byte("[]")) {
		t.Fatalf("the read-only DB changed: %s", b)
	}
	if len(s.all()) != 0 {
		t.Fatalf("the failed add was kept in memory: %v", s.all())
	}
}

func TestSaveWritesEmptyArray(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.json")
	s, _, err := openStore(path, storeNow)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	b, _ := os.ReadFile(path)
	if !bytes.Equal(bytes.TrimSpace(b), []byte("[]")) {
		t.Fatalf("file: %s", b)
	}
}
