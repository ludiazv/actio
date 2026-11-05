// Small pure helpers: the protocol's time format, request ids and nonces.
package actio

import (
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"time"
)

// isoPattern is the one shape a protocol datetime has (the datetime pattern in common.schema.json).
var isoPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$`)

// formatISO renders t in the protocol's one form: "YYYY-MM-DDThh:mm:ssZ", second resolution, UTC.
// Every datetime the implementation writes comes from here, never from a formatting string elsewhere.
// Sub-second digits are truncated (floor), never rounded: a peer reads the moment the write
// started, not a rounded one.
func formatISO(t time.Time) string {
	return t.UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z")
}

// parseISO reads that exact form. ok is false for any other text, and for a date the pattern accepts
// but the calendar does not (month 13, or a leap second).
// A text that parses but does not round-trip through formatISO is rejected: a calendar-invalid date
// like 2025-02-30 is an error, not a silent normalisation.
func parseISO(text string) (time.Time, bool) {
	if !isoPattern.MatchString(text) {
		return time.Time{}, false
	}
	t, err := time.Parse("2006-01-02T15:04:05Z", text)
	if err != nil {
		return time.Time{}, false
	}
	if formatISO(t) != text {
		return time.Time{}, false
	}
	return t, true
}

// crockford is the Crockford base32 alphabet, without I, L, O and U.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// newULID is the protocol's request id: 48 bits of millisecond time then 80 random bits, in
// canonical upper-case Crockford base32 (the rid pattern in common.schema.json).
// The first 10 characters encode the millisecond time, so ids taken at increasing instants compare
// in lexical order on those 10 characters.
func newULID(now time.Time) RID {
	var out [26]byte

	// The 48-bit millisecond time, five bits per character, most significant first: the first
	// character carries the top 3 bits and is therefore in 0-7.
	ms := uint64(now.UnixMilli()) & 0x0000FFFFFFFFFFFF
	for i := 9; i >= 0; i-- {
		out[i] = crockford[ms&0x1F]
		ms >>= 5
	}

	// The 80 random bits, five bits per character.
	var buf [10]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err) // no crypto/rand, no ids: this cannot recover
	}
	n, bits, val := 10, 0, 0
	for _, b := range buf {
		val = (val << 8) | int(b)
		bits += 8
		for bits >= 5 {
			bits -= 5
			out[n] = crockford[(val>>bits)&0x1F]
			n++
		}
	}
	return RID(out[:])
}

// newNonce is a 128-bit random nonce as 32 hexadecimal characters (the nonce pattern in
// common.schema.json).
func newNonce() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // no crypto/rand, no nonces: this cannot recover
	}
	return hex.EncodeToString(b[:])
}
