// caps.go implements the list and unset capabilities (Hora.md, Capabilities;
// D-2, D-13).
package main

import (
	"context"
	"encoding/json"

	"actio-go"
)

// list returns the caller's entries, or every entry for a coordinator (D-2).
func (h *hora) list(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	panic("not implemented: list")
}

// unset removes the entry, when the caller is its owner or a coordinator (D-13).
func (h *hora) unset(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
	panic("not implemented: unset")
}
