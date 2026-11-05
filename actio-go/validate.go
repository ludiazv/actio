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

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"actio-go/internal/schema"
)

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

// compiledSchemas is the one piece of package-level state this file may hold: the compiled
// protocol schema set, built once on first use and immutable after — the compiler is discarded
// when the build finishes and the *Schema values are only ever validated against, never changed.
// A build failure is a contract bug (the embedded schemas are pinned and compile-proven by
// TestEmbeddedSchemasCompile): it surfaces as the error of every validator, never as a silent
// "valid".
var compiledSchemas = sync.OnceValues(compileProtocolSchemas)

// protocolSchemas is the compiled protocol set: one schema per pointer the validators use, the
// ten file roots and the four body definitions (*Schemas*).
type protocolSchemas struct {
	schemas map[string]*jsonschema.Schema
}

// refusingLoader is a URLLoader that refuses everything: no file, no network. The protocol set
// is fully embedded, and a capability schema may only refer to itself.
type refusingLoader struct{ what string }

func (l refusingLoader) Load(uri string) (any, error) {
	return nil, fmt.Errorf("actio: %s; the reference %q is refused", l.what, uri)
}

// compileProtocolSchemas compiles the eleven embedded schemas once. Every schema goes in under
// its own $id (the file name), so common.schema.json#/$defs/header resolves inside the embedded
// set. There is no file or network loader for the protocol set: everything is embedded, and a
// $ref that leaves the set is a contract bug, not something to load.
func compileProtocolSchemas() (*protocolSchemas, error) {
	c := jsonschema.NewCompiler()
	c.UseLoader(refusingLoader{what: "the protocol schema set is embedded"})

	entries, err := schema.Files.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("actio: reading the embedded schemas: %w", err)
	}
	for _, entry := range entries {
		raw, err := schema.Files.ReadFile(entry.Name())
		if err != nil {
			return nil, fmt.Errorf("actio: reading the embedded %s: %w", entry.Name(), err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("actio: parsing the embedded %s: %w", entry.Name(), err)
		}
		if err := c.AddResource(entry.Name(), doc); err != nil {
			return nil, fmt.Errorf("actio: adding the embedded %s to the protocol set: %w", entry.Name(), err)
		}
	}

	set := &protocolSchemas{schemas: map[string]*jsonschema.Schema{}}
	// The file shapes are the root of their file; the four bodies are their $defs/body.
	pointer := []string{
		"spec.schema.json",
		"status.schema.json",
		"announce.schema.json",
		"actio.schema.json",
		"request.schema.json",
		"cancellation.schema.json",
		"response.schema.json",
		"confirmation.schema.json",
		"diarium.schema.json",
		"error.schema.json",
		"request.schema.json#/$defs/body",
		"cancellation.schema.json#/$defs/body",
		"response.schema.json#/$defs/body",
		"confirmation.schema.json#/$defs/body",
	}
	for _, p := range pointer {
		sch, err := c.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("actio: compiling the protocol set at %s: %w", p, err)
		}
		set.schemas[p] = sch
	}
	return set, nil
}

// checkRaw is the shared core of every validate*: validate the bytes against the compiled schema
// at pointer, then decode the validated document into the pinned struct. The error names the
// schema and the first failure.
func checkRaw(pointer string, raw []byte, out any) error {
	set, err := compiledSchemas()
	if err != nil {
		return err
	}
	sch := set.schemas[pointer]
	if sch == nil {
		return fmt.Errorf("actio: no compiled schema for %q: a contract bug", pointer)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("actio: %s: %v", pointer, err)
	}
	if err := sch.Validate(doc); err != nil {
		return fmt.Errorf("actio: %s: %v", pointer, err)
	}
	return decodeInto(doc, out)
}

// decodeInto turns a validated document (numbers kept as json.Number) into the pinned struct. The
// document is re-marshalled and read back with UseNumber so the any and json.RawMessage fields
// keep the numbers and the raw bytes exactly as the file wrote them. DisallowUnknownFields is
// deliberately not set: the schema decides what is allowed, not the decoder.
func decodeInto(doc any, out any) error {
	enc, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("actio: %v", err)
	}
	dec := json.NewDecoder(bytes.NewReader(enc))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("actio: %v", err)
	}
	return nil
}

// The publication files and the four envelopes: raw bytes in, decoded value out, and the first
// schema failures as the error. A file that does not conform is never used and never written.
func validateSpec(raw []byte) (*Spec, error) {
	var out Spec
	if err := checkRaw("spec.schema.json", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateStatus(raw []byte) (*Status, error) {
	var out Status
	if err := checkRaw("status.schema.json", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateAnnounce(raw []byte) (*Announce, error) {
	var out Announce
	if err := checkRaw("announce.schema.json", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateTeamMetadata(raw []byte) (*TeamMetadata, error) {
	var out TeamMetadata
	if err := checkRaw("actio.schema.json", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateRequestFile(raw []byte) (*RequestFile, error) {
	var out RequestFile
	if err := checkRaw("request.schema.json", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateCancellationFile(raw []byte) (*CancellationFile, error) {
	var out CancellationFile
	if err := checkRaw("cancellation.schema.json", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateResponseFile(raw []byte) (*ResponseFile, error) {
	var out ResponseFile
	if err := checkRaw("response.schema.json", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateConfirmationFile(raw []byte) (*ConfirmationFile, error) {
	var out ConfirmationFile
	if err := checkRaw("confirmation.schema.json", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateDiariumLine(raw []byte) (*DiariumLine, error) {
	var out DiariumLine
	if err := checkRaw("diarium.schema.json", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateErrorResult(raw []byte) (*ErrorResult, error) {
	var out ErrorResult
	if err := checkRaw("error.schema.json", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// The four envelope bodies, validated on their own when a file's body key holds an object rather
// than an encrypted string.
func validateRequestBody(raw []byte) (*RequestBody, error) {
	var out RequestBody
	if err := checkRaw("request.schema.json#/$defs/body", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateCancellationBody(raw []byte) (*CancellationBody, error) {
	var out CancellationBody
	if err := checkRaw("cancellation.schema.json#/$defs/body", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateResponseBody(raw []byte) (*ResponseBody, error) {
	var out ResponseBody
	if err := checkRaw("response.schema.json#/$defs/body", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateConfirmationBody(raw []byte) (*ConfirmationBody, error) {
	var out ConfirmationBody
	if err := checkRaw("confirmation.schema.json#/$defs/body", raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Validate reports whether the file at path conforms to the schema of kind. kind "" infers the
// kind from the file name, as the CLI does. A diarium file is validated line by line.
//
// The file read below is the single os file call this function makes, and the granted exception
// to the rule that only the file-io files touch the os file functions: this signature is frozen
// for the CLI, which is its only caller, and it needs no sync hooks, so the read is granted here
// rather than routed through a fileIO. Nothing else in this file reads or writes a file.
func Validate(path string, kind string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if kind == "" {
		kind = kindFromName(path)
		if kind == "" {
			k, ok := envelopeKind(raw)
			if !ok {
				return fmt.Errorf("actio: cannot infer the kind of %s: not a published name and not an envelope", path)
			}
			kind = k
		}
	}
	switch kind {
	case KindActio:
		_, err = validateTeamMetadata(raw)
	case KindSpec:
		_, err = validateSpec(raw)
	case KindStatus:
		_, err = validateStatus(raw)
	case KindAnnounce:
		_, err = validateAnnounce(raw)
	case KindRequest:
		_, err = validateRequestFile(raw)
	case KindCancellation:
		_, err = validateCancellationFile(raw)
	case KindResponse:
		_, err = validateResponseFile(raw)
	case KindConfirmation:
		_, err = validateConfirmationFile(raw)
	case KindDiarium:
		return validateDiariumFile(path, raw)
	default:
		return fmt.Errorf("actio: unknown kind %q for %s", kind, path)
	}
	return err
}

// announceFileNames matches the announced file names: nuntium/<agent_id>.n.json (the agent id has
// no dots) and the reserved nuntium/nuntium.json (*Announcement and Discovery*).
var announceFileNames = regexp.MustCompile(`^[a-zA-Z0-9_-]+\.\d+\.json$`)

// kindFromName names the kind from a published file name, "" when the name publishes nothing.
func kindFromName(path string) string {
	switch filepath.Base(path) {
	case FileSpec:
		return KindSpec
	case FileStatus:
		return KindStatus
	case FileActio:
		return KindActio
	case FileDiarium:
		return KindDiarium
	case "nuntium.json":
		return KindAnnounce
	}
	if announceFileNames.MatchString(filepath.Base(path)) {
		return KindAnnounce
	}
	return ""
}

// validateDiariumFile validates a diarium line by line; the error names the line number.
func validateDiariumFile(path string, raw []byte) error {
	for n, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if _, err := validateDiariumLine([]byte(line)); err != nil {
			return fmt.Errorf("actio: %s: line %d: %v", path, n+1, err)
		}
	}
	return nil
}

// envelopeKind names the body key of an envelope file: exactly one of the four, or ok false when
// the object has none or more than one, or the bytes are not a JSON object. It never panics.
func envelopeKind(raw []byte) (string, bool) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", false
	}
	var kind string
	found := 0
	for _, k := range []string{BodyRequest, BodyCancellation, BodyResponse, BodyConfirmation} {
		if _, ok := m[k]; ok {
			kind = k
			found++
		}
	}
	if found != 1 {
		return "", false
	}
	return kind, true
}

// sameHeader reports whether two envelopes belong to one exchange: every header field equal
// except Datetime, which each file writes at its own time (D-11).

func sameHeader(a, b *Header) bool {
	return a.RID == b.RID &&
		a.Src == b.Src &&
		a.Dst == b.Dst &&
		a.Type == b.Type &&
		a.ActioVersion == b.ActioVersion
}

// nameMatches reports whether a cancellation names the same capability as its request.
func nameMatches(req *RequestBody, can *CancellationBody) bool {
	return req.Name == can.Name
}

// duplicateCapabilityName is the first name a spec repeats, or "" when the names are unique. The
// schema cannot express uniqueness across an array, so the agents check it (*Schemas* rules).
func duplicateCapabilityName(spec *Spec) string {
	seen := map[string]struct{}{}
	for _, cap := range spec.Capabilities {
		if _, ok := seen[cap.Name]; ok {
			return cap.Name
		}
		seen[cap.Name] = struct{}{}
	}
	return ""
}

// capabilitySchemas holds the compiled parameters and result validators of one capability.
//
// A nil *capabilitySchemas is valid and means "the checks are skipped": a capability whose
// schemas do not compile (an external $ref, an unknown keyword, a pattern the RE2 engine
// rejects) is handled by the caller, which logs once and skips (D-19), and checkParams and
// checkResult on a nil receiver return nil.
type capabilitySchemas struct {
	params *jsonschema.Schema
	result *jsonschema.Schema
}

// capabilityParamsLoc and capabilityResultLoc are the $ids a capability's two schemas are added
// under. A fresh compiler per capability means the two locations are reused across capabilities
// and never mix with the protocol set.
const (
	capabilityParamsLoc = "actio://capability/parameters"
	capabilityResultLoc = "actio://capability/result"
)

// compileCapability compiles a capability's parameters and result schemas. For one of our own
// capabilities a compile failure, or a keyword this validator does not know, is a registration
// error; for a peer's capability the caller logs once and skips the checks (D-19).
//
// The compile runs on a compiler separate from the protocol one and used for one capability at
// a time, so a peer's $id never mixes with the protocol schemas and a repeated $id across
// capabilities is harmless. The loader refuses every external reference: no file, no network, no
// $ref to another URL — a peer schema may only refer to itself. A schema may be a boolean
// ({"parameters": true}); the library accepts it and the checks pass everything.
func compileCapability(cap *Capability) (*capabilitySchemas, error) {
	params, err := jsonschema.UnmarshalJSON(bytes.NewReader(cap.Parameters))
	if err != nil {
		return nil, fmt.Errorf("actio: capability %s: the parameters schema is not a JSON document: %v", cap.Name, err)
	}
	result, err := jsonschema.UnmarshalJSON(bytes.NewReader(cap.Actio.Result))
	if err != nil {
		return nil, fmt.Errorf("actio: capability %s: the result schema is not a JSON document: %v", cap.Name, err)
	}
	if kw, ok := unknownKeyword(params); ok {
		return nil, fmt.Errorf("actio: capability %s: the parameters schema uses the unknown keyword %q", cap.Name, kw)
	}
	if kw, ok := unknownKeyword(result); ok {
		return nil, fmt.Errorf("actio: capability %s: the result schema uses the unknown keyword %q", cap.Name, kw)
	}

	c := jsonschema.NewCompiler()
	c.UseLoader(refusingLoader{what: fmt.Sprintf("capability %s: a capability schema may only refer to itself", cap.Name)})
	if err := c.AddResource(capabilityParamsLoc, params); err != nil {
		return nil, fmt.Errorf("actio: capability %s: adding the parameters schema: %v", cap.Name, err)
	}
	if err := c.AddResource(capabilityResultLoc, result); err != nil {
		return nil, fmt.Errorf("actio: capability %s: adding the result schema: %v", cap.Name, err)
	}
	ps, err := c.Compile(capabilityParamsLoc)
	if err != nil {
		return nil, fmt.Errorf("actio: capability %s: the parameters schema does not compile: %v", cap.Name, err)
	}
	rs, err := c.Compile(capabilityResultLoc)
	if err != nil {
		return nil, fmt.Errorf("actio: capability %s: the result schema does not compile: %v", cap.Name, err)
	}
	return &capabilitySchemas{params: ps, result: rs}, nil
}

// checkParams validates request parameters against the capability's published parameters schema.
// The bytes are validated as the peer wrote them, without re-marshalling. A nil receiver skips
// the checks (D-19).
func (c *capabilitySchemas) checkParams(params json.RawMessage) error {
	if c == nil {
		return nil
	}
	return c.check("parameters", c.params, params)
}

// checkResult validates a result against the capability's published result schema. The bytes are
// validated as the peer wrote them, without re-marshalling. A nil receiver skips the checks (D-19).
func (c *capabilitySchemas) checkResult(result json.RawMessage) error {
	if c == nil {
		return nil
	}
	return c.check("result", c.result, result)
}

func (c *capabilitySchemas) check(what string, sch *jsonschema.Schema, raw json.RawMessage) error {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("actio: the %s are not a JSON document: %v", what, err)
	}
	if err := sch.Validate(doc); err != nil {
		return fmt.Errorf("actio: %s: %s", what, failureText(err))
	}
	return nil
}

// failureText renders the first (most specific) validation failure in the reference's message
// form: the instance path — "(root)" at the root — then the message.
func failureText(err error) string {
	ve, ok := err.(*jsonschema.ValidationError)
	if !ok {
		return err.Error()
	}
	text := firstCause(ve).Error()
	// The library renders the root as `at ''`; the reference form says (root).
	if rest, ok := strings.CutPrefix(text, "at '': "); ok {
		return "(root) " + rest
	}
	return text
}

// firstCause descends single-cause chains (a $ref hop, a branch with one failing schema) to the
// most specific failure, as the reference validator hands back its first error.
func firstCause(ve *jsonschema.ValidationError) *jsonschema.ValidationError {
	for len(ve.Causes) == 1 {
		ve = ve.Causes[0]
	}
	return ve
}

// knownKeywords is the set of JSON Schema keywords this library version (jsonschema v6.0.3)
// recognizes under draft 2020-12: the properties of the 2020-12 meta-schema embedded in that
// version, across the core, applicator, unevaluated, validation, meta-data,
// format-annotation and content vocabularies. The library silently ignores a keyword it does
// not know (unlike the reference validator's strict mode, which rejects it), so
// compileCapability walks a capability schema and refuses any keyword outside this list: a
// misspelling such as "propertes", and a draft-04 construct the 2020-12 vocabulary does not
// define ("dependencies", "definitions", "additionalItems"), must fail the compile, not the
// validation.
var knownKeywords = map[string]struct{}{
	// core
	"$schema": {}, "$id": {}, "$ref": {}, "$anchor": {}, "$dynamicRef": {}, "$dynamicAnchor": {},
	"$vocabulary": {}, "$defs": {},
	// meta-data and content (annotation)
	"$comment": {}, "title": {}, "description": {}, "default": {}, "deprecated": {}, "readOnly": {},
	"writeOnly": {}, "examples": {}, "contentEncoding": {}, "contentMediaType": {}, "contentSchema": {},
	// applicator
	"allOf": {}, "anyOf": {}, "oneOf": {}, "not": {}, "if": {}, "then": {}, "else": {},
	"prefixItems": {}, "items": {}, "contains": {}, "properties": {}, "patternProperties": {},
	"additionalProperties": {}, "unevaluatedProperties": {}, "unevaluatedItems": {},
	"propertyNames": {}, "dependentSchemas": {}, "dependentRequired": {},
	// validation
	"type": {}, "enum": {}, "const": {}, "multipleOf": {}, "maximum": {}, "exclusiveMaximum": {},
	"minimum": {}, "exclusiveMinimum": {}, "maxLength": {}, "minLength": {}, "pattern": {},
	"maxItems": {}, "minItems": {}, "uniqueItems": {}, "maxContains": {}, "minContains": {},
	"maxProperties": {}, "minProperties": {}, "required": {},
	// format-annotation
	"format": {},
}

// schemaValueKeywords names the 2020-12 keywords whose value is itself a schema: a single
// schema, a list of schemas, or a map of name to schema. The walker descends only through these,
// so data (an enum value, a default) is never mistaken for a schema.
var schemaValueKeywords = map[string]struct{ list, table bool }{
	"not": {}, "if": {}, "then": {}, "else": {}, "items": {}, "contains": {},
	"additionalProperties": {}, "unevaluatedProperties": {}, "unevaluatedItems": {},
	"propertyNames": {}, "contentSchema": {},
	"allOf":             {list: true},
	"anyOf":             {list: true},
	"oneOf":             {list: true},
	"prefixItems":       {list: true},
	"properties":        {table: true},
	"patternProperties": {table: true},
	"dependentSchemas":  {table: true},
}

// unknownKeyword walks a 2020-12 schema document (an object or a boolean) and reports the first
// keyword outside knownKeywords, in name order. It returns ok false for a boolean schema and for
// a value that is neither an object nor a boolean: such a document never reaches the walk's
// keyword checks, and the compile rejects it for what it is.
func unknownKeyword(doc any) (string, bool) {
	switch v := doc.(type) {
	case bool:
		return "", false
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, ok := knownKeywords[key]; !ok {
				return key, true
			}
			if spec, isSchema := schemaValueKeywords[key]; isSchema {
				if kw, bad := walkSchemaValue(spec, v[key]); bad {
					return kw, true
				}
			}
		}
		return "", false
	default:
		return "", false
	}
}

func walkSchemaValue(spec struct{ list, table bool }, v any) (string, bool) {
	switch {
	case spec.list:
		items, ok := v.([]any)
		if !ok {
			return "", false // a non-list value is the compile's to reject
		}
		for _, item := range items {
			if kw, bad := unknownKeyword(item); bad {
				return kw, true
			}
		}
	case spec.table:
		table, ok := v.(map[string]any)
		if !ok {
			return "", false // a non-object value is the compile's to reject
		}
		for _, value := range table {
			if kw, bad := unknownKeyword(value); bad {
				return kw, true
			}
		}
	default:
		return unknownKeyword(v)
	}
	return "", false
}

// CheckParams is the public form: nil when params fits the capability's parameters schema, or
// when the schema does not compile (D-19 skips the checks), or an error naming the first failure.
//
// params is encoded with json.Marshal: the writer's encoder (marshalJSON in io.go) belongs to
// another file and this path only validates, so its HTML-escaping difference cannot change the
// answer. One shared encoder for both paths is a later decision, not a protocol rule.
func CheckParams(cap Capability, params any) error {
	enc, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("actio: capability %s: encoding the parameters: %v", cap.Name, err)
	}
	cs, err := compileCapability(&cap)
	if err != nil {
		return nil // D-19: a schema that does not compile skips the checks
	}
	return cs.checkParams(enc)
}
