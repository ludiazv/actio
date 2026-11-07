// The spec builder: registrations in, spec.json out (spec.schema.json).
//
// It publishes the core profile's security fields (no security features, trusted posture), the
// capabilities in registration order, and optional fields only when they were given: absent, null
// and an empty list are three legal ways to say "the null namespace", so the builder must not invent
// a value the operator did not write. A spec that fails validation here is our bug, and it is an
// error rather than a file in the tree.
package actio

import (
	"fmt"
	"time"
)

// buildSpec assembles the spec of an agent from its options and its registered capabilities, with a
// fresh datetime from the agent's clock. A capability name repeated is an error: the schema cannot
// express uniqueness, and a duplicate would make one capability disappear from every peer's view.
func buildSpec(opts Options, capabilities []Capability, now time.Time) (*Spec, error) {
	seen := make(map[string]struct{}, len(capabilities))
	for _, cap := range capabilities {
		if _, dup := seen[cap.Name]; dup {
			return nil, fmt.Errorf("actio: the capability %q of agent %s is registered twice", cap.Name, opts.ID)
		}
		seen[cap.Name] = struct{}{}
	}
	spec := Spec{
		ID:           opts.ID,
		ActioVersion: ProtocolVersion,
		Datetime:     formatISO(now),
		Description:  opts.Description,
		// Copied, and non-nil even when empty: an empty list means nobody may command us, and a nil
		// slice would marshal to null.
		AcceptsCommandsFrom: append([]string{}, opts.AcceptsCommandsFrom...),
		// Registration order, copied: a later change to the caller's slice cannot alter what we
		// publish.
		Capabilities:    append([]Capability{}, capabilities...),
		SecuritySupport: []string{}, // core profile: no signatures, no encryption
		SecurityPosture: "trusted",  // core profile: trust the shared tree
	}
	// Optional fields appear only when given: the builder must not invent a value the operator did
	// not write. The copies are deep where the items are pointers, so a later change to the
	// options cannot alter what we publish.
	if opts.Namespaces != nil {
		spec.Namespaces = make([]*string, len(opts.Namespaces))
		for i, ns := range opts.Namespaces {
			if ns == nil {
				continue // the void namespace, listed explicitly
			}
			s := *ns
			spec.Namespaces[i] = &s
		}
	}
	if opts.Depends != nil {
		spec.Depends = append([]string{}, opts.Depends...)
	}
	if opts.Wants != nil {
		spec.Wants = append([]string{}, opts.Wants...)
	}
	if opts.Info != nil {
		spec.Info = opts.Info // free content: the schema only requires an object
	}
	// `disabled` is never written here: it lives in the spec object the agent keeps, the `enable`
	// command rewrites it during a run, and a fresh start clears it again (D-10).

	raw, err := marshalJSON(spec)
	if err != nil {
		return nil, err
	}
	if _, err := validateSpec(raw); err != nil {
		return nil, fmt.Errorf("actio: the spec of agent %s is invalid: %w", opts.ID, err)
	}
	return &spec, nil
}
