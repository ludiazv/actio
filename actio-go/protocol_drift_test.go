// The struct drift test: the protocol structs are hand-written, so this is what pins them to the
// schemas. Every property of every object shape has a field with that JSON name, every field has a
// property, a required property never has "omitempty", and a property that may be null has a field
// that can hold null.
//
// The schemas themselves stay authoritative at runtime: this test proves the structs did not drift,
// it does not replace validation.
package actio

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"actio-go/internal/schema"
)

// driftContract pairs each object shape in the schemas with the Go type that stands for it. The
// pointer locates the shape inside its file: "" is the file's root, then "$defs/<name>",
// "properties/<name>" and "items" step through it.
var driftContract = []struct {
	file    string
	pointer string
	typ     any
}{
	{"actio.schema.json", "", TeamMetadata{}},
	{"announce.schema.json", "", Announce{}},
	{"cancellation.schema.json", "", CancellationFile{}},
	{"cancellation.schema.json", "$defs/body", CancellationBody{}},
	{"common.schema.json", "$defs/header", Header{}},
	{"confirmation.schema.json", "", ConfirmationFile{}},
	{"confirmation.schema.json", "$defs/body", ConfirmationBody{}},
	{"diarium.schema.json", "", DiariumLine{}},
	{"diarium.schema.json", "$defs/exchange", ExchangeEnvelopes{}},
	{"error.schema.json", "", ErrorResult{}},
	{"request.schema.json", "", RequestFile{}},
	{"request.schema.json", "$defs/body", RequestBody{}},
	{"response.schema.json", "", ResponseFile{}},
	{"response.schema.json", "$defs/body", ResponseBody{}},
	{"spec.schema.json", "", Spec{}},
	{"spec.schema.json", "$defs/capability", Capability{}},
	{"spec.schema.json", "$defs/capabilityActio", CapabilityActio{}},
	{"spec.schema.json", "properties/security/items", SecurityAttr{}},
	{"status.schema.json", "", Status{}},
	{"status.schema.json", "properties/capability_status/items", CapabilityStatus{}},
	{"status.schema.json", "properties/tasks/items", StatusTask{}},
}

func TestProtocolStructsMatchTheSchemas(t *testing.T) {
	entries, err := schema.Files.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the embedded schemas: %v", err)
	}
	loaded := map[string]*schemaNode{} // schema file name -> its root
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, err := schema.Files.ReadFile(entry.Name())
		if err != nil {
			t.Fatalf("reading %s: %v", entry.Name(), err)
		}
		var root schemaNode
		if err := json.Unmarshal(raw, &root); err != nil {
			t.Fatalf("parsing %s: %v", entry.Name(), err)
		}
		loaded[entry.Name()] = &root
	}

	covered := map[string]bool{}
	for _, item := range driftContract {
		covered[item.file] = true
		root, ok := loaded[item.file]
		if !ok {
			t.Errorf("%s is in the contract table but not embedded", item.file)
			continue
		}
		node, err := walkPointer(root, item.pointer)
		if err != nil {
			t.Errorf("%s %s: %v", item.file, item.pointer, err)
			continue
		}
		props, required, err := schemaObject(item.file, node, loaded)
		if err != nil {
			t.Errorf("%s %s: %v", item.file, item.pointer, err)
			continue
		}
		checkOne(t, item.file+" "+item.pointer, props, required, reflect.TypeOf(item.typ), loaded)
	}

	for name := range loaded {
		if !covered[name] {
			t.Errorf("%s is embedded but no Go type is pinned to it", name)
		}
	}
}

// checkOne compares the properties of one schema shape with the fields of one Go struct.
func checkOne(t *testing.T, what string, props map[string]schemaLocation, required map[string]bool, typ reflect.Type, loaded map[string]*schemaNode) {
	t.Helper()
	if typ.Kind() != reflect.Struct {
		t.Errorf("%s: %s is not a struct", what, typ)
		return
	}
	fields, err := goFields(typ)
	if err != nil {
		t.Errorf("%s: %v", what, err)
		return
	}

	var missing, extra []string
	for name, prop := range props {
		field, ok := fields[name]
		if !ok {
			missing = append(missing, name)
			continue
		}
		if required[name] && field.omitEmpty {
			t.Errorf("%s: %q is required, so its Go field must not have omitempty", what, name)
		}
		// The property may be declared in another file: an envelope inherits its header from
		// common.schema.json, and the header's own $ref points inside that file.
		declared, err := derefProp(prop.file, prop.node, loaded)
		if err != nil {
			t.Errorf("%s: %q: %v", what, name, err)
			continue
		}
		if mayBeNull(declared) && !canHoldNull(field.field.Type) {
			t.Errorf("%s: %q may be null, but %s of type %s cannot hold it", what, name, field.field.Name, field.field.Type)
		}
	}
	for name := range fields {
		if _, ok := props[name]; !ok {
			extra = append(extra, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 {
		t.Errorf("%s: schema properties with no Go field: %s", what, strings.Join(missing, ", "))
	}
	if len(extra) > 0 {
		t.Errorf("%s: Go fields with no schema property: %s", what, strings.Join(extra, ", "))
	}
}

// derefProp follows the $ref of a property definition: the protocol declares its nullable fields
// through common.schema.json, so the type to check is the one the reference points at.
func derefProp(file string, prop *schemaNode, loaded map[string]*schemaNode) (*schemaNode, error) {
	for depth := 0; prop != nil && prop.Ref != ""; depth++ {
		if depth > 10 {
			return nil, fmt.Errorf("reference chain too long at %q", prop.Ref)
		}
		target, err := resolveRef(file, prop.Ref, loaded)
		if err != nil {
			return nil, err
		}
		file, prop = target.file, target.node
	}
	return prop, nil
}

// canHoldNull reports whether a Go type marshals as null when it holds its zero value: a pointer, a
// slice, a map or an interface. json.RawMessage is a slice, so it holds null as nil.
func canHoldNull(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
		return true
	default:
		return false
	}
}

// mayBeNull reports whether a property's declared types include null. A property with no "type" is
// unconstrained, and its Go field must therefore hold anything: json.RawMessage.
func mayBeNull(prop *schemaNode) bool {
	return len(prop.Type) > 0 && strings.Contains(string(prop.Type), `"null"`)
}

// goFields maps the JSON name of every field of a struct, following the rule encoding/json uses: an
// embedded struct without a tag contributes its own fields.
func goFields(typ reflect.Type) (map[string]*goField, error) {
	fields := map[string]*goField{}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag, tagged := field.Tag.Lookup("json")
		if field.Anonymous && !tagged {
			if field.Type.Kind() != reflect.Struct {
				return nil, fmt.Errorf("embedded %s is not a struct", field.Name)
			}
			inner, err := goFields(field.Type)
			if err != nil {
				return nil, err
			}
			for name, found := range inner {
				fields[name] = found
			}
			continue
		}
		if !tagged {
			return nil, fmt.Errorf("field %s has no json tag", field.Name)
		}
		name, options, _ := strings.Cut(tag, ",")
		if name == "" || name == "-" {
			return nil, fmt.Errorf("field %s is not marshalled (json tag %q)", field.Name, tag)
		}
		fields[name] = &goField{field: field, omitEmpty: strings.Contains(options, "omitempty")}
	}
	return fields, nil
}

type goField struct {
	field     reflect.StructField
	omitEmpty bool
}

// schemaNode is the part of a JSON Schema this test reads: the shape of an object, not its rules.
type schemaNode struct {
	Ref        string                 `json:"$ref"`
	Type       json.RawMessage        `json:"type"`
	Properties map[string]*schemaNode `json:"properties"`
	Required   []string               `json:"required"`
	AllOf      []*schemaNode          `json:"allOf"`
	Defs       map[string]*schemaNode `json:"$defs"`
	Items      *schemaNode            `json:"items"`
}

// schemaObject returns the properties and required names one node enforces, following $ref and
// every allOf branch: the envelopes take their header from common.schema.json that way.
func schemaObject(file string, node *schemaNode, loaded map[string]*schemaNode) (map[string]schemaLocation, map[string]bool, error) {
	if node == nil {
		return nil, nil, fmt.Errorf("empty schema node")
	}
	if node.Ref != "" {
		target, err := resolveRef(file, node.Ref, loaded)
		if err != nil {
			return nil, nil, err
		}
		return schemaObject(target.file, target.node, loaded)
	}

	props := map[string]schemaLocation{}
	required := map[string]bool{}
	for name, child := range node.Properties {
		props[name] = schemaLocation{file: file, node: child}
	}
	for _, name := range node.Required {
		required[name] = true
	}
	for _, branch := range node.AllOf {
		sub, subRequired, err := schemaObject(file, branch, loaded)
		if err != nil {
			return nil, nil, err
		}
		for name, child := range sub {
			props[name] = child
		}
		for name := range subRequired {
			required[name] = true
		}
	}
	return props, required, nil
}

type schemaLocation struct {
	file string
	node *schemaNode
}

// resolveRef follows one $ref, which may name another schema file. An external reference beyond the
// embedded set is a contract change, not a drift: the test reports it as one.
func resolveRef(from, ref string, loaded map[string]*schemaNode) (schemaLocation, error) {
	name, fragment := from, strings.TrimPrefix(ref, "#")
	if before, after, found := strings.Cut(ref, "#"); found {
		name = before
		fragment = after
	}
	if name == "" {
		name = from
	}
	root, ok := loaded[name]
	if !ok {
		return schemaLocation{}, fmt.Errorf("%q refers to %s, which is not embedded", ref, name)
	}
	node, err := walkPointer(root, strings.Trim(fragment, "/"))
	if err != nil {
		return schemaLocation{}, fmt.Errorf("%q: %w", ref, err)
	}
	return schemaLocation{file: name, node: node}, nil
}

// walkPointer steps through "$defs/<name>", "properties/<name>" and "items".
func walkPointer(node *schemaNode, pointer string) (*schemaNode, error) {
	if pointer == "" {
		return node, nil
	}
	steps := strings.Split(pointer, "/")
	for i := 0; i < len(steps); i++ {
		if node == nil {
			return nil, fmt.Errorf("nothing at %q", pointer)
		}
		switch steps[i] {
		case "$defs", "properties":
			i++
			if i >= len(steps) {
				return nil, fmt.Errorf("%s needs a name", steps[i-1])
			}
			branch := node.Defs
			if steps[i-1] == "properties" {
				branch = node.Properties
			}
			child, ok := branch[steps[i]]
			if !ok {
				return nil, fmt.Errorf("no %s %q", steps[i-1], steps[i])
			}
			node = child
		case "items":
			if node.Items == nil {
				return nil, fmt.Errorf("no items")
			}
			node = node.Items
		default:
			return nil, fmt.Errorf("unknown pointer step %q", steps[i])
		}
	}
	return node, nil
}
