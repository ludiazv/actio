// The pins: the schemas and the test vectors inside this module must be byte-identical to the
// sources they were copied from. They are committed copies, not generated ones, so this test is what
// keeps them honest; if the protocol changes, the copy changes with it in the same commit.
package actio

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"actio-go/internal/schema"
)

// The sources of the copies, relative to this module. Both are in the same repository.
const (
	upstreamSchemas = "../doc/schemas"
	upstreamVectors = "../actio-ts/test/vectors"
)

func TestEmbeddedSchemasAreTheProtocolSchemas(t *testing.T) {
	upstream := readTree(t, upstreamSchemas)
	if len(upstream) != 11 {
		t.Fatalf("%s holds %d files, want 11", upstreamSchemas, len(upstream))
	}

	embedded, err := schema.Files.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the embedded schemas: %v", err)
	}
	if len(embedded) != len(upstream) {
		t.Fatalf("%d embedded schemas, want %d", len(embedded), len(upstream))
	}

	for name, want := range upstream {
		got, err := schema.Files.ReadFile(name)
		if err != nil {
			t.Errorf("embedded %s: %v", name, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s differs from %s/%s: the copy must be byte-identical (%d bytes here, %d there)",
				name, upstreamSchemas, name, len(got), len(want))
		}
	}
	for _, entry := range embedded {
		if _, ok := upstream[entry.Name()]; !ok {
			t.Errorf("embedded %s has no counterpart in %s", entry.Name(), upstreamSchemas)
		}
	}
}

func TestVectorCopiesAreTheReferenceVectors(t *testing.T) {
	upstream := readTree(t, upstreamVectors)
	if len(upstream) != 111 {
		t.Fatalf("%s holds %d files, want 111", upstreamVectors, len(upstream))
	}

	copied := readTree(t, filepath.Join("testdata", "vectors"))
	if len(copied) != len(upstream) {
		t.Fatalf("testdata/vectors holds %d files, want %d", len(copied), len(upstream))
	}
	for name, want := range upstream {
		got, ok := copied[name]
		if !ok {
			t.Errorf("testdata/vectors/%s is missing", name)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("testdata/vectors/%s differs from the reference vector", name)
		}
	}
	for name := range copied {
		if _, ok := upstream[name]; !ok {
			t.Errorf("testdata/vectors/%s has no counterpart in %s", name, upstreamVectors)
		}
	}
}

// readTree maps every file path under root (relative to root) to its bytes.
func readTree(t *testing.T, root string) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = data
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	return files
}
