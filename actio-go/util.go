// Small pure helpers: the protocol's time format, request ids and nonces.
package actio

import "time"

// formatISO renders t in the protocol's one form: "YYYY-MM-DDThh:mm:ssZ", second resolution, UTC.
// Every datetime the implementation writes comes from here, never from a formatting string elsewhere.
func formatISO(t time.Time) string { panic("actio: not implemented") }

// parseISO reads that exact form. ok is false for any other text, and for a date the pattern accepts
// but the calendar does not (month 13, or a leap second).
func parseISO(text string) (time.Time, bool) { panic("actio: not implemented") }

// newULID is the protocol's request id: 48 bits of millisecond time then 80 random bits, in
// canonical upper-case Crockford base32.
func newULID(now time.Time) RID { panic("actio: not implemented") }

// newNonce is a 128-bit random nonce as 32 hexadecimal characters.
func newNonce() string { panic("actio: not implemented") }
