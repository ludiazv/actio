// Protocol shapes: the JSON the protocol exchanges, hand-written and pinned to the schemas in
// internal/schema by a reflection drift test (protocol_drift_test.go). There is no generator: the
// protocol's unions do not map to Go, and the schemas stay authoritative at runtime — every protocol
// file is validated against them before it is used or written.
//
// Field rules that the drift test enforces:
//
//   - every schema property has a field with that JSON name, and every field has a property;
//   - a required property never has "omitempty";
//   - a nullable or absent field is a pointer, json.RawMessage or a slice, as the schema needs
//     (a nil *string marshals as null; a nil json.RawMessage marshals as null; a nil required
//     slice marshals as null, which the self-validation before each write catches).
package actio

import "encoding/json"

// AgentID is an agent id: 1–32 characters of [a-zA-Z0-9_-] (common.schema.json $defs/agentId).
type AgentID string

// RID is a request id: a ULID in canonical upper-case Crockford base32 ($defs/rid).
type RID string

// ProtocolVersion is the only protocol version this implementation speaks (*Version*).
const ProtocolVersion = "0.1"

// The two envelope types (*Issue a request*).
const (
	TypeCommand = "cmd"
	TypeRequest = "req"
)

// The four envelope body keys (*Issue a request*); exactly one per envelope file.
const (
	BodyRequest      = "request"
	BodyCancellation = "cancellation"
	BodyResponse     = "response"
	BodyConfirmation = "confirmation"
)

// The final statuses of an exchange, as the diarium names them (diarium.schema.json): an
// incoming exchange ends confirmed, completed, cancelled or dropped; an outgoing one confirmed,
// cancelled, abandoned or sent.
const (
	FinalConfirmed = "confirmed"
	FinalCompleted = "completed"
	FinalCancelled = "cancelled"
	FinalDropped   = "dropped"
	FinalAbandoned = "abandoned"
	FinalSent      = "sent"
)

// The five events the protocol defines (diarium.schema.json). An implementation may define others;
// this one writes only these, so that a peer reading them knows what event_data holds.
const (
	EventStatus   = "status"
	EventAnnounce = "announce"
	EventInreq    = "inreq"
	EventOutreq   = "outreq"
	EventLog      = "log"
)

// Names of the published files and folders (*Actio directory structure*).
const (
	FileSpec      = "spec.json"
	FileStatus    = "status.json"
	FileDiarium   = "diarium.jsonl"
	FileActio     = "actio.json"
	FolderInbox   = "inbox"
	FolderForum   = "forum"
	FolderNuntium = "nuntium"
	BroadcastID   = "broadcast" // the id in nuntium/nuntium.json: only a tool writes that file
)

// Acceptance error codes (*Acceptance errors*). Required checks: 100, 101, 102, 103. 105, 106, 107
// and 108 are optional and this implementation makes them (104 is not made). Capability errors are
// the agent's own, 1000 and up.
const (
	CodeAgentDisabled      = 100
	CodeUnknownCapability  = 101
	CodeCapabilityDisabled = 102
	CodeRequestorNotNamed  = 103
	CodeInvalidParameters  = 105
	CodeNamespaceMismatch  = 106
	CodeCapacityExceeded   = 107
	CodeRequestExpired     = 108
	CodeAgentStopping      = 300 // work error (*Acceptance errors*, work error codes)
)

// Header is the envelope header: the top-level fields of an envelope file, not a nested "header"
// key (common.schema.json $defs/header). All files of an exchange share it; only Datetime changes.
// The header is public: it is never encrypted.
type Header struct {
	RID          RID     `json:"rid"`
	Src          AgentID `json:"src"`
	Dst          AgentID `json:"dst"`
	Datetime     string  `json:"datetime"`
	Type         string  `json:"type"`
	ActioVersion string  `json:"actio_version"`
}

// RequestFile is <rid>.json, written once by the requestor in the inbox of dst
// (request.schema.json). Request holds a RequestBody object, or a base64 string when a peer
// encrypts: it stays raw so the file can be validated before it is decoded.
type RequestFile struct {
	Header
	Request json.RawMessage `json:"request"`
}

// CancellationFile is <rid>.can.json, written once by the requestor (cancellation.schema.json).
type CancellationFile struct {
	Header
	Cancellation json.RawMessage `json:"cancellation"`
}

// ResponseFile is <rid>.res.json, written once by the receiver in its own inbox
// (response.schema.json).
type ResponseFile struct {
	Header
	Response json.RawMessage `json:"response"`
}

// ConfirmationFile is <rid>.con.json, written once by the requestor (confirmation.schema.json).
type ConfirmationFile struct {
	Header
	Confirmation json.RawMessage `json:"confirmation"`
}

// RequestBody is the request body of a request or a command (request.schema.json $defs/body).
// Every field is required: Parameters is null when there are none, and Attachments is never null.
type RequestBody struct {
	TS          int64           `json:"ts"`
	Nonce       string          `json:"nonce"`
	Signature   *string         `json:"signature"` // null in the core profile
	Name        string          `json:"name"`
	Parameters  json.RawMessage `json:"parameters"`
	Timeout     float64         `json:"timeout"` // seconds; 0 = no timeout
	Attachments []string        `json:"attachments"`
}

// CancellationBody is the cancellation body (cancellation.schema.json $defs/body). Name must equal
// the request's Name (*Schemas* rules).
type CancellationBody struct {
	TS        int64   `json:"ts"`
	Nonce     string  `json:"nonce"`
	Signature *string `json:"signature"`
	Name      string  `json:"name"`
	Force     bool    `json:"force"`
}

// ResponseBody is the response body (response.schema.json $defs/body). When Cancellation is true,
// Error is false, Result is null and Attachments is empty. When Error is true, Result follows
// error.schema.json.
type ResponseBody struct {
	TS           int64           `json:"ts"`
	Nonce        string          `json:"nonce"`
	Signature    *string         `json:"signature"`
	Error        bool            `json:"error"`
	Cancellation bool            `json:"cancellation"`
	Result       json.RawMessage `json:"result"`
	Attachments  []string        `json:"attachments"`
	WorkTime     *float64        `json:"work_time,omitempty"` // seconds of actual work
}

// ConfirmationBody is the confirmation body (confirmation.schema.json $defs/body).
type ConfirmationBody struct {
	TS        int64   `json:"ts"`
	Nonce     string  `json:"nonce"`
	Signature *string `json:"signature"`
}

// ErrorResult is the result of an error response (error.schema.json). Code is absent unless the
// protocol defines one; capability errors are 1000 and up.
type ErrorResult struct {
	Code    *int   `json:"code,omitempty"`
	Msg     string `json:"msg"`
	Details string `json:"details,omitempty"`
}

// Spec is spec.json, the agent's publication of its identity, capabilities and security features
// (spec.schema.json). The owning agent is its only writer.
type Spec struct {
	ID           AgentID `json:"id"`
	ActioVersion string  `json:"actio_version"`
	Datetime     string  `json:"datetime"`
	Description  string  `json:"description"`
	// Namespaces: absent, null and [] all mean the null namespace (*Agent Spec*). Go keeps one of
	// them: a nil slice is absent (Options.Namespaces nil), and a nil item is the void namespace.
	Namespaces          []*string      `json:"namespaces,omitempty"`
	Depends             []string       `json:"depends,omitempty"`
	Wants               []string       `json:"wants,omitempty"`
	Disabled            *bool          `json:"disabled,omitempty"`    // absent means false
	AcceptsCommandsFrom []string       `json:"accepts_commands_from"` // an empty list means nobody
	Capabilities        []Capability   `json:"capabilities"`
	SecuritySupport     []string       `json:"security_support"` // core profile: always empty
	SecurityPosture     string         `json:"security_posture"` // core profile: always "trusted"
	Security            []SecurityAttr `json:"security,omitempty"`
	Info                map[string]any `json:"info,omitempty"` // free content, not schema-checked
}

// Capability is one OpenAI tool definition plus the actio object (spec.schema.json $defs/capability).
// Capability names are unique within an agent: a rule the schema cannot express, checked by the
// agents.
type Capability struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"` // a JSON object; {} when there are none
	Actio       CapabilityActio `json:"actio"`
}

// CapabilityActio is the actio object of a capability (spec.schema.json $defs/capabilityActio).
// Result is a JSON schema: an object or a boolean.
type CapabilityActio struct {
	Result             json.RawMessage `json:"result"`
	Skill              string          `json:"skill,omitempty"`
	Tool               *bool           `json:"tool,omitempty"`
	ToolName           string          `json:"tool_name,omitempty"`
	Sync               *bool           `json:"sync,omitempty"`
	AcceptsFrom        []string        `json:"accepts_from,omitempty"` // absent or empty: everybody
	MaxParallel        int             `json:"max_parallel,omitempty"` // 0: unbounded
	RecommendedTimeout float64         `json:"recommended_timeout,omitempty"`
	Disabled           *bool           `json:"disabled,omitempty"` // absent means false; a written false stays written
}

// SecurityAttr is one key-value pair of spec.json's security array (spec.schema.json). The core
// profile publishes none.
type SecurityAttr struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Status is status.json, written by the agent on every change and at least every 90 seconds
// (status.schema.json).
type Status struct {
	ID       AgentID     `json:"id"`
	Status   StatusState `json:"status"`
	Datetime string      `json:"datetime"`
	Msg      string      `json:"msg,omitempty"`
	Load     *float64    `json:"load,omitempty"`
	// CapabilityStatus is mandatory even when empty: one entry per capability with the requests
	// running and in queue.
	CapabilityStatus []CapabilityStatus `json:"capability_status"`
	Tasks            []StatusTask       `json:"tasks,omitempty"`
	Contention       *bool              `json:"contention,omitempty"`
}

// CapabilityStatus is one entry of status.json's capability_status (status.schema.json).
type CapabilityStatus struct {
	Name    string `json:"name"`
	Running int    `json:"running"`
	Queue   int    `json:"queue"`
}

// StatusTask is one entry of status.json's tasks array, disclosed at the agent's discretion
// (status.schema.json).
type StatusTask struct {
	Capability string   `json:"capability"`
	Status     string   `json:"status"`
	Name       string   `json:"name,omitempty"`
	Title      string   `json:"title,omitempty"`
	Progress   *float64 `json:"progress,omitempty"`
	Eta        string   `json:"eta,omitempty"` // hh:mm:ss
}

// Announce is an announce file nuntium/<id>.n.json, or the reserved nuntium/nuntium.json written
// only by a tool (announce.schema.json). It is a signal: any change triggers a full scan.
type Announce struct {
	ID       AgentID `json:"id"`
	Datetime string  `json:"datetime"`
	TS       int64   `json:"ts"`
	Nuntium  string  `json:"nuntium"`
}

// DiariumLine is one line of diarium.jsonl (diarium.schema.json). EventData is any JSON value: a
// string for status and log, null for announce, an ExchangeEnvelopes object for inreq and outreq.
// The schema's per-event "if"/"then" checks it, and its own event names are not checked at all.
type DiariumLine struct {
	ID        AgentID         `json:"id"`
	Datetime  string          `json:"datetime"`
	Event     string          `json:"event"`
	EventData json.RawMessage `json:"event_data"`
}

// ExchangeEnvelopes is the event_data of inreq and outreq (diarium.schema.json $defs/exchange): the
// final status and the four envelopes as exchanged, each raw so the bytes stay unchanged, null when
// the envelope was never written.
type ExchangeEnvelopes struct {
	FinalStatus string          `json:"final_status"`
	Req         json.RawMessage `json:"req"`
	Can         json.RawMessage `json:"can"`
	Res         json.RawMessage `json:"res"`
	Con         json.RawMessage `json:"con"`
}

// TeamMetadata is the optional .actio/actio.json, read-only for agents (actio.schema.json). An
// agent never writes it.
type TeamMetadata struct {
	ActioVersion string         `json:"actio_version,omitempty"`
	Description  string         `json:"description,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"` // free content, no schema
}
