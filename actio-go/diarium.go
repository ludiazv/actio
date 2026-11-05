// The diarium: the agent's own protocol log. Every line is validated before it is appended, and the
// reader of any agent's file reports the lines it could not use instead of failing.
//
// An inreq or outreq line carries the four envelopes of the exchange as they were exchanged, as raw
// bytes: an encrypted body stays encrypted, and a re-marshalled envelope would no longer be what
// passed between the two agents.
package actio

import "time"

// diaria writes one agent's diarium file.
type diaria struct{}

// newDiaria binds a diarium file (<folder>/diarium.jsonl) to an agent id and the file primitives.
func newDiaria(files *fileIO, folder string, id AgentID) *diaria { panic("actio: not implemented") }

// append validates the line and appends it. A line we produced ourselves that fails is our bug: the
// error surfaces to whoever triggered it.
func (d *diaria) append(line DiariumLine) error { panic("actio: not implemented") }

// The five protocol events (*Diarium*). Other event names are allowed by the schema but never
// checked, so this implementation writes only these.
func statusEvent(id AgentID, now time.Time, state StatusState) DiariumLine {
	panic("actio: not implemented")
}
func announceEvent(id AgentID, now time.Time) DiariumLine            { panic("actio: not implemented") }
func logEvent(id AgentID, now time.Time, message string) DiariumLine { panic("actio: not implemented") }
func inreqEvent(id AgentID, now time.Time, finalStatus string, envelopes ExchangeEnvelopes) DiariumLine {
	panic("actio: not implemented")
}
func outreqEvent(id AgentID, now time.Time, finalStatus string, envelopes ExchangeEnvelopes) DiariumLine {
	panic("actio: not implemented")
}

// ReadDiarium reads the diarium of one agent folder (the folder of an AgentView, never
// "<root>/<id>" by assumption). A missing file gives an empty result. Blank lines are skipped, and a
// last line without its newline is skipped without being reported: an append may be in progress.
// Filtering is the caller's job.
func ReadDiarium(folder string) (DiariumRead, error) { panic("actio: not implemented") }
