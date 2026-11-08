// The diarium: the agent's own protocol log. Every line is validated before it is appended, and the
// reader of any agent's file reports the lines it could not use instead of failing.
//
// An inreq or outreq line carries the four envelopes of the exchange as they were exchanged, as raw
// bytes: an encrypted body stays encrypted, and a re-marshalled envelope would no longer be what
// passed between the two agents.
package actio

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// diaria writes one agent's diarium file.
type diaria struct {
	files  *fileIO
	folder string
	id     AgentID
}

// newDiaria binds a diarium file (<folder>/diarium.jsonl) to an agent id and the file primitives.
func newDiaria(files *fileIO, folder string, id AgentID) *diaria {
	return &diaria{files: files, folder: folder, id: id}
}

// append validates the line and appends it. A line we produced ourselves that fails is our bug: the
// error surfaces to whoever triggered it.
func (d *diaria) append(line DiariumLine) error {
	raw, err := marshalJSON(line)
	if err != nil {
		return err
	}
	if _, err := validateDiariumLine(raw); err != nil {
		return err // G-5: the invalid line is not written
	}
	return d.files.appendJSONL(filepath.Join(d.folder, "diarium.jsonl"), raw)
}

// diariumLine builds one line: id, a datetime from now, the event name and its event_data. data is
// one of the five protocol shapes (a state string, null, a message string, the exchange); a marshal
// failure is our bug, not a peer's.
func diariumLine(id AgentID, now time.Time, event string, data any) DiariumLine {
	raw, err := json.Marshal(data)
	if err != nil {
		panic("actio: " + event + " event_data: " + err.Error())
	}
	return DiariumLine{ID: id, Datetime: formatISO(now), Event: event, EventData: raw}
}

// The five protocol events (*Diarium*). Other event names are allowed by the schema but never
// checked, so this implementation writes only these.
func statusEvent(id AgentID, now time.Time, state StatusState) DiariumLine {
	return diariumLine(id, now, "status", state)
}
func announceEvent(id AgentID, now time.Time) DiariumLine {
	return diariumLine(id, now, "announce", nil)
}
func logEvent(id AgentID, now time.Time, message string) DiariumLine {
	return diariumLine(id, now, "log", message)
}
func inreqEvent(id AgentID, now time.Time, finalStatus string, envelopes ExchangeEnvelopes) DiariumLine {
	envelopes.FinalStatus = finalStatus // pass by value: the caller's copy is untouched
	return diariumLine(id, now, "inreq", envelopes)
}
func outreqEvent(id AgentID, now time.Time, finalStatus string, envelopes ExchangeEnvelopes) DiariumLine {
	envelopes.FinalStatus = finalStatus
	return diariumLine(id, now, "outreq", envelopes)
}

// ReadDiarium reads the diarium of one agent folder (the folder of an AgentView, never
// "<root>/<id>" by assumption). A missing file gives an empty result. Blank lines are skipped, and a
// last line without its newline is skipped without being reported: an append may be in progress.
// Filtering is the caller's job.
//
// os.ReadFile is the one os call this file makes: the reader takes a folder, not a *fileIO, so a
// tool can read any agent's diarium without an agent.
func ReadDiarium(folder string) (DiariumRead, error) {
	raw, err := os.ReadFile(filepath.Join(folder, "diarium.jsonl"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return DiariumRead{Lines: []DiariumEntry{}, Ignored: []IgnoredLine{}}, nil
		}
		return DiariumRead{}, err
	}
	read := DiariumRead{Lines: []DiariumEntry{}, Ignored: []IgnoredLine{}}
	parts := strings.Split(string(raw), "\n")
	complete := len(raw) > 0 && raw[len(raw)-1] == '\n'
	for i, text := range parts {
		if i == len(parts)-1 && !complete {
			continue // the partial last line: an append in progress, not an error
		}
		if strings.TrimSpace(text) == "" {
			continue // blank lines carry no event
		}
		if !json.Valid([]byte(text)) {
			read.Ignored = append(read.Ignored, IgnoredLine{Line: i + 1, Reason: "not valid JSON"})
			continue
		}
		line, err := validateDiariumLine([]byte(text))
		if err != nil {
			read.Ignored = append(read.Ignored, IgnoredLine{Line: i + 1, Reason: err.Error()})
			continue
		}
		read.Lines = append(read.Lines, *line)
	}
	return read, nil
}
