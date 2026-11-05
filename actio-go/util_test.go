// Tests for the small pure helpers: the protocol's time form, ULIDs and nonces.
package actio

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

var (
	ridPattern   = regexp.MustCompile(`^[0-7][0-9A-HJKMNP-TV-Z]{25}$`)
	noncePattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

func TestFormatISOShapeAndTruncation(t *testing.T) {
	cases := []struct {
		name string
		in   time.Time
		want string
	}{
		{"plain", time.Date(2025, 6, 15, 8, 7, 6, 0, time.UTC), "2025-06-15T08:07:06Z"},
		{"subsecond truncated, not rounded", time.Date(2025, 6, 15, 8, 7, 6, 999_999_999, time.UTC), "2025-06-15T08:07:06Z"},
		{"half a second truncated", time.Date(2025, 6, 15, 8, 7, 6, 500_000_000, time.UTC), "2025-06-15T08:07:06Z"},
		{"non-UTC location rendered in UTC", time.Date(2025, 6, 15, 10, 7, 6, 0, time.FixedZone("+0200", 2*3600)), "2025-06-15T08:07:06Z"},
		{"last second of a year", time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC), "1999-12-31T23:59:59Z"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := formatISO(c.in)
			if got != c.want {
				t.Fatalf("formatISO(%v) = %q, want %q", c.in, got, c.want)
			}
			if !isoPattern.MatchString(got) {
				t.Fatalf("formatISO(%v) = %q does not match the datetime pattern", c.in, got)
			}
		})
	}
}

func TestParseISO(t *testing.T) {
	accepted := []struct {
		text string
		want time.Time
	}{
		{"2025-06-15T08:07:06Z", time.Date(2025, 6, 15, 8, 7, 6, 0, time.UTC)},
		{"2000-01-01T00:00:00Z", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"9999-12-31T23:59:59Z", time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)},
	}
	for _, c := range accepted {
		tt, ok := parseISO(c.text)
		if !ok {
			t.Errorf("parseISO(%q) rejected, want accepted", c.text)
			continue
		}
		if !tt.Equal(c.want) {
			t.Errorf("parseISO(%q) = %v, want %v", c.text, tt, c.want)
		}
		// The round-trip rule: what parseISO accepts, formatISO renders back byte for byte.
		if got := formatISO(tt); got != c.text {
			t.Errorf("round trip: formatISO(parseISO(%q)) = %q, want %q", c.text, got, c.text)
		}
	}

	rejected := []string{
		"2025-06-15T08:07:06.123Z",  // fractional seconds
		"2025-06-15T08:07:06+02:00", // an offset
		"2025-06-15T08:07:06z",      // lowercase z
		"2025-06-15T08:07:06",       // no zone
		"2025-06-15 08:07:06Z",      // a space
		"25-06-15T08:07:06Z",        // two-digit year
		"2025-02-30T00:00:00Z",      // a date the calendar rejects
		"2025-13-01T00:00:00Z",      // month 13
		"2025-06-15T24:00:00Z",      // hour 24
		"2025-06-15T08:07:60Z",      // a leap second
		"",                          // nothing
	}
	for _, text := range rejected {
		if _, ok := parseISO(text); ok {
			t.Errorf("parseISO(%q) accepted, want rejected", text)
		}
	}
}

func TestNewULID(t *testing.T) {
	now := time.Date(2025, 6, 15, 8, 7, 6, 123_000_000, time.UTC)

	id := newULID(now)
	if len(id) != 26 {
		t.Fatalf("len(ULID) = %d, want 26", len(id))
	}
	if !ridPattern.MatchString(string(id)) {
		t.Fatalf("ULID %q does not match the rid pattern", id)
	}

	// Two calls at the same instant differ, and share the 10 time characters.
	a, b := newULID(now), newULID(now)
	if a == b {
		t.Fatalf("two ULIDs at the same instant are equal: %q", a)
	}
	if a[:10] != b[:10] {
		t.Fatalf("same instant, different time prefix: %q vs %q", a, b)
	}

	// Increasing instants compare in lexical order on the first 10 characters.
	earlier, later := newULID(now), newULID(now.Add(time.Second))
	if strings.Compare(string(earlier[:10]), string(later[:10])) >= 0 {
		t.Fatalf("time prefix not in lexical order: %q vs %q", earlier[:10], later[:10])
	}

	// 100 calls are distinct.
	seen := make(map[string]bool, 100)
	for i := 0; i < 100; i++ {
		u := string(newULID(now))
		if seen[u] {
			t.Fatalf("duplicate ULID %q in 100 calls", u)
		}
		seen[u] = true
	}
}

func TestNewNonce(t *testing.T) {
	seen := make(map[string]bool, 100)
	for i := 0; i < 100; i++ {
		n := newNonce()
		if !noncePattern.MatchString(n) {
			t.Fatalf("nonce %q does not match the nonce pattern", n)
		}
		if seen[n] {
			t.Fatalf("duplicate nonce %q in 100 calls", n)
		}
		seen[n] = true
	}
}
