// Runtime validation: the embedded schemas, the rules they cannot express, and the capability
// schemas compiled on demand. No schema is re-implemented by hand.
//
// Every protocol file goes through here twice: a reader validates the bytes before decoding them,
// and a writer validates what it just marshalled before the write. A value we produced ourselves
// that fails is our bug, not a peer's; a peer's value that fails is ignored, and said so.
//
// Capability schemas (parameters and result) are compiled by a second, separate compiler, so a
// peer's $id never mixes with the protocol's, and each capability compiles on its own, so a repeated
// $id across capabilities is harmless. External references — a file or a network fetch — are
// refused: a peer's schema may only refer to itself.
package actio

import "encoding/json"

// The kinds Validate knows, and the ones the CLI's --as accepts.
const (
	KindActio        = "actio"
	KindSpec         = "spec"
	KindStatus       = "status"
	KindAnnounce     = "announce"
	KindRequest      = "request"
	KindCancellation = "cancellation"
	KindResponse     = "response"
	KindConfirmation = "confirmation"
	KindDiarium      = "diarium"
)

// Validate reports whether the file at path conforms to the schema of kind. kind "" infers the kind
// from the file name, as the CLI does. A diarium file is validated line by line.
func Validate(path string, kind string) error { panic("actio: not implemented") }

// The publication files and the four envelopes: raw bytes in, decoded value out, and the first
// schema failures as the error. A file that does not conform is never used and never written.
func validateSpec(raw []byte) (*Spec, error)                         { panic("actio: not implemented") }
func validateStatus(raw []byte) (*Status, error)                     { panic("actio: not implemented") }
func validateAnnounce(raw []byte) (*Announce, error)                 { panic("actio: not implemented") }
func validateTeamMetadata(raw []byte) (*TeamMetadata, error)         { panic("actio: not implemented") }
func validateRequestFile(raw []byte) (*RequestFile, error)           { panic("actio: not implemented") }
func validateCancellationFile(raw []byte) (*CancellationFile, error) { panic("actio: not implemented") }
func validateResponseFile(raw []byte) (*ResponseFile, error)         { panic("actio: not implemented") }
func validateConfirmationFile(raw []byte) (*ConfirmationFile, error) { panic("actio: not implemented") }
func validateDiariumLine(raw []byte) (*DiariumLine, error)           { panic("actio: not implemented") }
func validateErrorResult(raw []byte) (*ErrorResult, error)           { panic("actio: not implemented") }

// The four envelope bodies, validated on their own when a file's body key holds an object rather
// than an encrypted string.
func validateRequestBody(raw []byte) (*RequestBody, error)           { panic("actio: not implemented") }
func validateCancellationBody(raw []byte) (*CancellationBody, error) { panic("actio: not implemented") }
func validateResponseBody(raw []byte) (*ResponseBody, error)         { panic("actio: not implemented") }
func validateConfirmationBody(raw []byte) (*ConfirmationBody, error) { panic("actio: not implemented") }

// envelopeKind names the body key of an envelope file: exactly one of the four, or ok false when the
// object has none or more than one.
func envelopeKind(raw []byte) (string, bool) { panic("actio: not implemented") }

// sameHeader reports whether two envelopes belong to one exchange: every header field equal except
// Datetime, which each file writes at its own time.

func sameHeader(a, b *Header) bool { panic("actio: not implemented") }

// nameMatches reports whether a cancellation names the same capability as its request.
func nameMatches(req *RequestBody, can *CancellationBody) bool { panic("actio: not implemented") }

// duplicateCapabilityName is the first name a spec repeats, or "" when the names are unique. The
// schema cannot express uniqueness across an array, so the agents check it (*Schemas* rules).
func duplicateCapabilityName(spec *Spec) string { panic("actio: not implemented") }

// capabilitySchemas holds the compiled parameters and result validators of one capability.
type capabilitySchemas struct{}

// compileCapability compiles a capability's parameters and result schemas. For one of our own
// capabilities a compile failure, or a keyword this validator does not know, is a registration error;
// for a peer's capability the caller logs once and skips the checks (D-19).
func compileCapability(cap *Capability) (*capabilitySchemas, error) { panic("actio: not implemented") }

// checkParams validates request parameters against the capability's published parameters schema.
func (c *capabilitySchemas) checkParams(params json.RawMessage) error {
	panic("actio: not implemented")
}

// checkResult validates a result against the capability's published result schema.
func (c *capabilitySchemas) checkResult(result json.RawMessage) error {
	panic("actio: not implemented")
}

// CheckParams is the public form: nil when params fits the capability's parameters schema, or when
// the schema does not compile (D-19 skips the checks), or an error naming the first failure.
func CheckParams(cap Capability, params any) error { panic("actio: not implemented") }
