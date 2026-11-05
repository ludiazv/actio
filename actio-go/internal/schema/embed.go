// Package schema holds the Actio protocol schemas, embedded in the binary.
//
// The JSON files here are a byte-identical copy of the ones in the protocol's doc/schemas folder:
// they are committed, not generated, and a test fails if a copy differs or a file is missing. The
// structs of the protocol package are hand-written and pinned to these schemas by reflection, and
// these schemas stay authoritative at runtime: every protocol file is validated against them before
// it is used or written.
package schema

import "embed"

// Files holds the protocol schemas, one JSON file each, named by their $id.
//
//go:embed *.json
var Files embed.FS
