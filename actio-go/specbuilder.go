// The spec builder: registrations in, spec.json out (spec.schema.json).
//
// It publishes the core profile's security fields (no security features, trusted posture), the
// capabilities in registration order, and optional fields only when they were given: absent, null
// and an empty list are three legal ways to say "the null namespace", so the builder must not invent
// a value the operator did not write. A spec that fails validation here is our bug, and it is an
// error rather than a file in the tree.
package actio

import "time"

// buildSpec assembles the spec of an agent from its options and its registered capabilities, with a
// fresh datetime from the agent's clock. A capability name repeated is an error: the schema cannot
// express uniqueness, and a duplicate would make one capability disappear from every peer's view.
func buildSpec(opts Options, capabilities []Capability, now time.Time) (*Spec, error) {
	panic("actio: not implemented")
}
