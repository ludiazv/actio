// store.go is the persistent schedule database (Hora.md, Internal DB): a JSON
// array of entries, loaded, cleaned and saved atomically.
package main

import (
	"encoding/json"
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

// openStore loads the DB at path, cleaning expired one-shot entries (D-3). It
// returns the store and the entries that were cleaned.
func openStore(path string, now time.Time) (*store, []Entry, error) {
	panic("not implemented: openStore")
}

// add assigns a fresh SchedID, saves the DB, and returns the stored entry (D-13).
func (s *store) add(e Entry) (Entry, error) {
	panic("not implemented: add")
}

// remove deletes the entry with id and saves the DB (D-13).
func (s *store) remove(id string) (Entry, bool, error) {
	panic("not implemented: remove")
}

// get returns the entry with id.
func (s *store) get(id string) (Entry, bool) {
	panic("not implemented: get")
}

// all returns every entry.
func (s *store) all() []Entry {
	panic("not implemented: all")
}
