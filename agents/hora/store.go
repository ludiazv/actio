// store.go is the persistent schedule database (Hora.md, Internal DB): a JSON
// array of entries, loaded, cleaned and saved atomically.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Entry is one schedule, in the DB's field order (Hora.md, Internal DB).
type Entry struct {
	Datetime             string          `json:"datetime"`
	SchedID              string          `json:"sched_id"`
	SchedType            string          `json:"sched_type"`
	SchedExpression      string          `json:"sched_expression"`
	DestID               string          `json:"dest_id"`
	DestCapability       string          `json:"dest_capability"`
	CapabilityParameters json.RawMessage `json:"capability_parameters"`
}

// store is the in-memory DB and the only file writer.
type store struct {
	mu      sync.Mutex
	path    string
	entries []Entry
}

// schedIDRe is the id format of Hora.md's Internal DB section.
var schedIDRe = regexp.MustCompile(`^SCH-[0-9a-f]{8}$`)

// openStore loads the DB at path, cleaning expired one-shot entries before
// checking the rest (D-3). A missing file is an empty DB. It returns the
// store and the entries that were cleaned.
func openStore(path string, now time.Time) (*store, []Entry, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &store{path: path, entries: []Entry{}}, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("db %s: %v", path, err)
	}
	entries, cleaned, err := loadEntries(data, now)
	if err != nil {
		return nil, nil, err
	}
	s := &store{path: path, entries: entries}
	if len(cleaned) > 0 {
		if err = s.saveLocked(entries); err != nil {
			return nil, nil, err
		}
	}
	return s, cleaned, nil
}

// loadEntries decodes the DB file, cleaning an expired one-shot before the
// checks run on it (D-3). A load error names the entry's index.
func loadEntries(data []byte, now time.Time) (entries, cleaned []Entry, err error) {
	if !strings.HasPrefix(strings.TrimSpace(string(data)), "[") {
		return nil, nil, fmt.Errorf("db: not a JSON array")
	}
	var raws []json.RawMessage
	if err = json.Unmarshal(data, &raws); err != nil {
		return nil, nil, fmt.Errorf("db: not a JSON array: %v", err)
	}
	seen := map[string]int{}
	for i, raw := range raws {
		var e Entry
		if err = json.Unmarshal(raw, &e); err != nil {
			return nil, nil, fmt.Errorf("db entry %d: %v", i, err)
		}
		if len(e.CapabilityParameters) == 0 || string(e.CapabilityParameters) == "null" {
			e.CapabilityParameters = json.RawMessage("{}")
		}
		if e.SchedType == "one-shot" {
			t, perr := time.Parse(time.RFC3339, e.SchedExpression)
			if perr != nil {
				return nil, nil, fmt.Errorf("db entry %d: one-shot %s: not RFC 3339", i, e.SchedExpression)
			}
			if !t.After(now) {
				cleaned = append(cleaned, e)
				continue
			}
		}
		if j, dup := seen[e.SchedID]; dup {
			return nil, nil, fmt.Errorf("db entry %d: duplicate sched_id %s (also entry %d)", i, e.SchedID, j)
		}
		if err = checkEntry(e, now); err != nil {
			return nil, nil, fmt.Errorf("db entry %d: %v", i, err)
		}
		seen[e.SchedID] = i
		entries = append(entries, e)
	}
	return entries, cleaned, nil
}

// checkEntry applies the field rules and the expression check (D-3). The
// store has no config zone, so the check runs in UTC and a stored cron
// expression must already carry its zone.
func checkEntry(e Entry, now time.Time) error {
	if _, err := time.Parse("2006-01-02T15:04:05Z", e.Datetime); err != nil {
		return fmt.Errorf("datetime %s: not UTC RFC 3339 with whole seconds", e.Datetime)
	}
	if !schedIDRe.MatchString(e.SchedID) {
		return fmt.Errorf("sched_id %s: not SCH- plus 8 lowercase hex digits", e.SchedID)
	}
	if p := e.CapabilityParameters; !json.Valid(p) || p[0] != '{' {
		return fmt.Errorf("capability_parameters: not an object")
	}
	if e.SchedType == "cron" && !strings.HasPrefix(e.SchedExpression, "CRON_TZ=") && !strings.HasPrefix(e.SchedExpression, "TZ=") {
		return fmt.Errorf("cron expression %s: without a CRON_TZ= or TZ= prefix: add CRON_TZ=<zone> ", e.SchedExpression)
	}
	_, _, err := parseSchedule(e.SchedType, e.SchedExpression, time.UTC, now)
	return err
}

// add keeps a non-empty SchedID, or generates one, and saves the DB (D-13).
// It returns the stored entry. The caller sets the datetime (the creation
// time, Hora.md, Internal DB).
func (s *store) add(e Entry) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(e.CapabilityParameters) == 0 || string(e.CapabilityParameters) == "null" {
		e.CapabilityParameters = json.RawMessage("{}")
	}
	id := e.SchedID
	var err error
	if id == "" {
		if id, err = s.freshID(); err != nil {
			return Entry{}, err
		}
	} else if s.index(id) >= 0 {
		return Entry{}, fmt.Errorf("sched_id %s: the id is taken", id)
	}
	e.SchedID = id
	next := append(append([]Entry{}, s.entries...), e)
	if err = s.saveLocked(next); err != nil {
		return Entry{}, err
	}
	s.entries = next
	return e, nil
}

// freshID generates SCH- plus 8 lowercase hex digits from crypto/rand,
// regenerating on a collision.
func (s *store) freshID() (string, error) {
	for {
		b := make([]byte, 4)
		if _, err := rand.Read(b); err != nil {
			return "", err
		}
		if id := "SCH-" + hex.EncodeToString(b); s.index(id) < 0 {
			return id, nil
		}
	}
}

// remove deletes the entry with id and saves the DB (D-13).
func (s *store) remove(id string) (Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.index(id)
	if i < 0 {
		return Entry{}, false, nil
	}
	e := s.entries[i]
	next := append(append([]Entry{}, s.entries[:i]...), s.entries[i+1:]...)
	if err := s.saveLocked(next); err != nil {
		return Entry{}, false, err
	}
	s.entries = next
	return e, true, nil
}

// get returns the entry with id.
func (s *store) get(id string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i := s.index(id); i >= 0 {
		return s.entries[i], true
	}
	return Entry{}, false
}

// all returns every entry, as a copy.
func (s *store) all() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Entry{}, s.entries...)
}

// save writes the current entries to the DB file.
func (s *store) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked(s.entries)
}

// saveLocked writes entries as the DB file: a temp file in the same
// directory, fsynced, renamed over the DB with mode 0600 (Hora.md, Internal
// DB). The DB must be writable, and the caller holds s.mu.
func (s *store) saveLocked(entries []Entry) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	if _, aerr := os.Stat(s.path); aerr == nil {
		if f, oerr := os.OpenFile(s.path, os.O_WRONLY, 0); oerr != nil {
			return fmt.Errorf("db %s: not writable: %v", s.path, oerr)
		} else {
			f.Close()
		}
	}
	name := fmt.Sprintf("%s.%d.tmp", s.path, time.Now().UnixNano())
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	if serr := f.Sync(); werr == nil {
		werr = serr
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		os.Remove(name)
		return werr
	}
	if err := os.Rename(name, s.path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}

// index returns the position of the entry with id, or -1. The caller holds
// s.mu.
func (s *store) index(id string) int {
	for i, e := range s.entries {
		if e.SchedID == id {
			return i
		}
	}
	return -1
}
