// The conformance vector runner: walks testdata/vectors/<kind>/{valid,invalid} and asserts each
// file against the matching validator, mirroring the reference harness. One subtest per kind and
// per file, so a failure names the file. The folder must hold exactly the ten known kind
// folders, and the valid vectors must round-trip through the pinned structs.
package actio

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// vectorsDir is the committed copy of the reference vectors.
const vectorsDir = "testdata/vectors"

// vectorKinds is the ten kind folders the vector tree must hold, each mapped to the validator of
// its file shape. A diarium vector is one line, validated with validateDiariumLine, not a whole
// JSONL file.
var vectorKinds = map[string]func([]byte) (any, error){
	"actio":        func(raw []byte) (any, error) { return validateTeamMetadata(raw) },
	"announce":     func(raw []byte) (any, error) { return validateAnnounce(raw) },
	"cancellation": func(raw []byte) (any, error) { return validateCancellationFile(raw) },
	"confirmation": func(raw []byte) (any, error) { return validateConfirmationFile(raw) },
	"diarium":      func(raw []byte) (any, error) { return validateDiariumLine(raw) },
	"error":        func(raw []byte) (any, error) { return validateErrorResult(raw) },
	"request":      func(raw []byte) (any, error) { return validateRequestFile(raw) },
	"response":     func(raw []byte) (any, error) { return validateResponseFile(raw) },
	"spec":         func(raw []byte) (any, error) { return validateSpec(raw) },
	"status":       func(raw []byte) (any, error) { return validateStatus(raw) },
}

// sortedVectorKinds is the kind names in order, so the subtests run deterministically.
func sortedVectorKinds() []string {
	kinds := make([]string, 0, len(vectorKinds))
	for kind := range vectorKinds {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

// listVectorFiles lists the .json files of a vector folder, in name order.
func listVectorFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		files = append(files, e.Name())
	}
	sort.Strings(files)
	return files
}

// TestVectorFoldersAreTheKnownKinds: the tree holds exactly the ten known kind folders; an
// unknown folder (or a missing one) fails the suite, so a new schema cannot slip in untested.
func TestVectorFoldersAreTheKnownKinds(t *testing.T) {
	entries, err := os.ReadDir(vectorsDir)
	if err != nil {
		t.Fatalf("reading %s: %v", vectorsDir, err)
	}
	folders := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() {
			folders[e.Name()] = true
		}
	}
	for kind := range vectorKinds {
		if !folders[kind] {
			t.Errorf("missing vector folder: %s", kind)
		}
	}
	for name := range folders {
		if _, known := vectorKinds[name]; !known {
			t.Errorf("unknown vector folder: %s", name)
		}
	}
}

// TestVectors: per kind and per valid/invalid, at least 3 valid and 5 invalid files; every valid
// file passes, every invalid file fails with at least one error message; the total is 111, so a
// copy that lost a file is visible here too.
func TestVectors(t *testing.T) {
	total := 0
	for _, kind := range sortedVectorKinds() {
		kind := kind
		validate := vectorKinds[kind]
		t.Run(kind, func(t *testing.T) {
			for _, dir := range []string{"valid", "invalid"} {
				files := listVectorFiles(t, filepath.Join(vectorsDir, kind, dir))
				min := 3
				if dir == "invalid" {
					min = 5
				}
				if len(files) < min {
					t.Fatalf("%s/%s: %d files, need at least %d", kind, dir, len(files), min)
				}
				total += len(files)
				for _, file := range files {
					file := file
					t.Run(file, func(t *testing.T) {
						raw, err := os.ReadFile(filepath.Join(vectorsDir, kind, dir, file))
						if err != nil {
							t.Fatalf("reading the vector: %v", err)
						}
						_, verr := validate(raw)
						if dir == "valid" {
							if verr != nil {
								t.Errorf("expected valid, validator said: %v", verr)
							}
							return
						}
						if verr == nil {
							t.Error("expected invalid, validator said ok")
							return
						}
						if strings.TrimSpace(verr.Error()) == "" {
							t.Error("an invalid vector must yield a non-empty error")
						}
					})
				}
			}
		})
	}
	if total != 111 {
		t.Errorf("the vector tree holds %d files, want 111", total)
	}
}

// TestVectorsRoundTrip: for every valid vector, decode into the pinned struct, re-marshal, and
// validate the result: it must still validate, and re-decoding it must give the same JSON as the
// original (the two parsed trees compared, not the bytes — json.Number keeps them comparable).
// This is the test that catches a field we forgot to model, an omitempty that ate a value, and
// the absent/null/[] confusion. A valid vector that does not round-trip is a finding: the test
// fails and the report says which vector and the difference.
func TestVectorsRoundTrip(t *testing.T) {
	for _, kind := range sortedVectorKinds() {
		kind := kind
		validate := vectorKinds[kind]
		files := listVectorFiles(t, filepath.Join(vectorsDir, kind, "valid"))
		for _, file := range files {
			file := file
			t.Run(kind+"/"+file, func(t *testing.T) {
				raw, err := os.ReadFile(filepath.Join(vectorsDir, kind, "valid", file))
				if err != nil {
					t.Fatalf("reading the vector: %v", err)
				}
				decoded, err := validate(raw)
				if err != nil {
					t.Fatalf("a valid vector does not validate: %v", err)
				}
				enc, err := json.Marshal(decoded)
				if err != nil {
					t.Fatalf("re-marshalling the decoded value: %v", err)
				}
				if _, err := validate(enc); err != nil {
					t.Errorf("the re-marshalled value does not validate: %v", err)
				}
				orig, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
				if err != nil {
					t.Fatalf("re-decoding the original: %v", err)
				}
				re, err := jsonschema.UnmarshalJSON(bytes.NewReader(enc))
				if err != nil {
					t.Fatalf("re-decoding the re-marshalled value: %v", err)
				}
				if !reflect.DeepEqual(orig, re) {
					t.Errorf("the round trip changed the document:\noriginal:\n%s\nre-coded:\n%s", raw, enc)
				}
			})
		}
	}
}
