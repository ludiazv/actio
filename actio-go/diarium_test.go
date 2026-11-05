// Tests for the diarium: the five event constructors, the validated append, and the reader's
// contract — the valid lines in order, the unusable ones reported with a reason, and the in-flight
// append invisible.
package actio

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDiariumEventShapes(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if got := statusEvent("agent", now, StatusBusy); got.ID != "agent" || got.Event != "status" || got.Datetime != formatISO(now) || string(got.EventData) != `"busy"` {
		t.Fatalf("statusEvent = %+v", got)
	}
	if got := announceEvent("agent", now); got.Event != "announce" || string(got.EventData) != "null" {
		t.Fatalf("announceEvent = %+v", got)
	}
	if got := logEvent("agent", now, "hi"); got.Event != "log" || string(got.EventData) != `"hi"` {
		t.Fatalf("logEvent = %+v", got)
	}

	req := json.RawMessage(`{"rid":"x","src":"a","dst":"b","type":"req"}`)
	env := ExchangeEnvelopes{Req: req}
	in := inreqEvent("agent", now, "dropped", env)
	if in.Event != "inreq" {
		t.Fatalf("inreqEvent = %+v", in)
	}
	var inData ExchangeEnvelopes
	if err := json.Unmarshal(in.EventData, &inData); err != nil {
		t.Fatal(err)
	}
	if inData.FinalStatus != "dropped" || !bytes.Equal(inData.Req, req) {
		t.Fatalf("inreq data = %+v (the envelopes must stay as exchanged)", inData)
	}
	if env.FinalStatus != "" {
		t.Fatal("inreqEvent mutated the caller's envelopes")
	}
	out := outreqEvent("agent", now, "sent", env)
	var outData ExchangeEnvelopes
	if err := json.Unmarshal(out.EventData, &outData); err != nil {
		t.Fatal(err)
	}
	if out.Event != "outreq" || outData.FinalStatus != "sent" || !bytes.Equal(outData.Req, req) {
		t.Fatalf("outreq = %+v, data %+v", out, outData)
	}
}

func TestDiariaAppendFiveEvents(t *testing.T) {
	dir := t.TempDir()
	files := testFiles(t)
	log := newDiaria(files, dir, "agent")
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	// A request envelope valid per request.schema.json: the diarium schema validates the
	// envelopes of an inreq/outreq line against the envelope schemas they are.
	req := json.RawMessage(`{"rid":"01ARZ3NDEKTSV4RRFFQ69C5FA6","src":"a","dst":"b","datetime":"2026-01-02T03:04:05Z","type":"req","actio_version":"1.0","request":{"ts":1767380645,"nonce":"0123456789abcdef0123456789abcdef","signature":null,"name":"echo","parameters":null,"timeout":60,"attachments":[]}}`)
	env := ExchangeEnvelopes{Req: req, Res: json.RawMessage(`null`)}
	lines := []DiariumLine{
		statusEvent("agent", now, StatusIdle),
		announceEvent("agent", now),
		logEvent("agent", now, "a message"),
		inreqEvent("agent", now, "confirmed", env),
		outreqEvent("agent", now, "sent", env),
	}
	for _, line := range lines {
		if err := log.append(line); err != nil {
			t.Fatalf("%s: %v", line.Event, err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "diarium.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(raw), "\n"); n != 5 {
		t.Fatalf("the file has %d newlines, want 5 (one line per event)", n)
	}
	read, err := ReadDiarium(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Lines) != 5 || len(read.Ignored) != 0 {
		t.Fatalf("read = %d lines, %d ignored: %+v", len(read.Lines), len(read.Ignored), read.Ignored)
	}
	wantEvents := []string{"status", "announce", "log", "inreq", "outreq"}
	for i, name := range wantEvents {
		if read.Lines[i].Event != name {
			t.Fatalf("line %d = %q, want %q (file order)", i+1, read.Lines[i].Event, name)
		}
	}
	// The inreq line carries the request envelope as it was exchanged: byte for byte in the file
	// (the append writes what it was given), and content for content when read back — the reader
	// validates every line, and validation normalises key order, so the read bytes are canonical,
	// not original.
	if !strings.Contains(string(raw), string(req)) {
		t.Fatalf("the req envelope was not kept raw in the file:\n%s", raw)
	}
	var data ExchangeEnvelopes
	if err := json.Unmarshal(read.Lines[3].EventData, &data); err != nil {
		t.Fatal(err)
	}
	if data.FinalStatus != "confirmed" {
		t.Fatalf("final_status = %q, want confirmed", data.FinalStatus)
	}
	var wantReq, gotReq any
	if err := json.Unmarshal(req, &wantReq); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data.Req, &gotReq); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantReq, gotReq) {
		t.Fatalf("the inreq envelope content changed on the read: %s", data.Req)
	}
}

func TestDiariaAppendInvalid(t *testing.T) {
	dir := t.TempDir()
	files := testFiles(t)
	log := newDiaria(files, dir, "agent")
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	bad := DiariumLine{ID: "agent", Datetime: formatISO(now), Event: "status", EventData: json.RawMessage(`"bogus"`)}
	if err := log.append(bad); err == nil {
		t.Fatal("an invalid status event was accepted")
	}
	if _, err := os.Stat(filepath.Join(dir, "diarium.jsonl")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("the invalid line created the diarium file")
	}
	// A valid line afterwards still works: the rejection changed nothing.
	if err := log.append(logEvent("agent", now, "after the rejection")); err != nil {
		t.Fatal(err)
	}
	read, err := ReadDiarium(dir)
	if err != nil || len(read.Lines) != 1 || read.Lines[0].Event != "log" {
		t.Fatalf("after the rejection: %+v (%v)", read.Lines, err)
	}
}

func TestReadDiarium(t *testing.T) {
	dir := t.TempDir()
	// A missing file gives an empty result, not an error.
	read, err := ReadDiarium(filepath.Join(dir, "nope"))
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Lines) != 0 || len(read.Ignored) != 0 {
		t.Fatalf("missing file: %+v, want an empty result", read)
	}

	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	dt := formatISO(now)
	valid1 := `{"id":"agent","datetime":"` + dt + `","event":"log","event_data":"one"}`
	valid2 := `{"id":"other","datetime":"` + dt + `","event":"status","event_data":"idle"}`
	badJSON := `{not json`
	badShape := `{"id":"agent","event":"log","event_data":"no datetime"}`
	// Line 6 has no trailing newline: an append may be in progress.
	content := valid1 + "\n\n" + badJSON + "\n" + badShape + "\n" + valid2 + "\npartial"
	if err := os.WriteFile(filepath.Join(dir, "diarium.jsonl"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	read, err = ReadDiarium(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Lines) != 2 {
		t.Fatalf("lines = %d, want 2 (the valid ones, in file order): %+v", len(read.Lines), read.Lines)
	}
	if read.Lines[0].Event != "log" || read.Lines[1].ID != "other" {
		t.Fatalf("order or content: %+v", read.Lines)
	}
	// A line of another agent's is valid protocol in this file's reader: it is returned, not
	// ignored — the schema does not tie the id to the folder, and neither does the reader.
	if len(read.Ignored) != 2 {
		t.Fatalf("ignored = %+v, want exactly the two bad lines", read.Ignored)
	}
	if read.Ignored[0].Line != 3 || read.Ignored[0].Reason != "not valid JSON" {
		t.Fatalf("ignored[0] = %+v, want line 3, \"not valid JSON\"", read.Ignored[0])
	}
	if read.Ignored[1].Line != 4 || !strings.Contains(read.Ignored[1].Reason, "diarium.schema.json") {
		t.Fatalf("ignored[1] = %+v, want line 4 with the schema's first failure", read.Ignored[1])
	}

	// A file that ends with its newline has no ghost last line.
	clean := t.TempDir()
	if err := os.WriteFile(filepath.Join(clean, "diarium.jsonl"), []byte(valid1+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	read, err = ReadDiarium(clean)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Lines) != 1 || len(read.Ignored) != 0 {
		t.Fatalf("clean file: %+v, want one line and nothing ignored", read)
	}
}
