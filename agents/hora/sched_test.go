// sched_test.go pins parseSchedule's three types and their stored forms (D-4,
// D-5, D-6), including robfig's traps that Hora must reject itself.
package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseSchedule(t *testing.T) {
	madrid, err := time.LoadLocation("Europe/Madrid")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 3, 25, 6, 0, 0, 0, time.UTC)
	ten := time.Date(2026, 3, 25, 10, 0, 0, 0, time.UTC)
	madridNine := time.Date(2026, 3, 25, 8, 0, 0, 0, time.UTC) // 09:00 Madrid = 08:00Z (CET)

	rows := []struct {
		name     string
		typ      string
		expr     string
		want     string    // the stored form, when there is no error
		wantErr  string    // a substring of the error, when one is wanted
		wantNext time.Time // the first Next, when set
		once     bool      // the second Next must be the zero time
	}{
		// one-shot (D-4)
		{name: "one-shot plain RFC3339", typ: "one-shot", expr: "2026-03-25T10:00:00Z",
			want: "2026-03-25T10:00:00Z", wantNext: ten, once: true},
		{name: "one-shot +02:00 offset stored as UTC", typ: "one-shot", expr: "2026-03-25T12:00:00+02:00",
			want: "2026-03-25T10:00:00Z", wantNext: ten, once: true},
		{name: "one-shot fractional second", typ: "one-shot", expr: "2026-03-25T10:00:00.500Z",
			wantErr: "fractional second"},
		{name: "one-shot equal to now", typ: "one-shot", expr: "2026-03-25T06:00:00Z",
			wantErr: "not after now"},
		{name: "one-shot before now", typ: "one-shot", expr: "2026-03-25T05:59:59Z",
			wantErr: "not after now"},
		{name: "one-shot malformed", typ: "one-shot", expr: "not a time",
			wantErr: "not RFC 3339"},
		// periodic (D-5)
		{name: "periodic 30s stored as written", typ: "periodic", expr: "@every 30s",
			want: "@every 30s"},
		{name: "periodic 500ms", typ: "periodic", expr: "@every 500ms",
			wantErr: "less than 1s"},
		{name: "periodic 0s", typ: "periodic", expr: "@every 0s",
			wantErr: "less than 1s"},
		{name: "periodic -5s", typ: "periodic", expr: "@every -5s",
			wantErr: "less than 1s"},
		{name: "periodic 1500ms", typ: "periodic", expr: "@every 1500ms",
			wantErr: "whole number of seconds"},
		{name: "periodic bare @every", typ: "periodic", expr: "@every",
			wantErr: "must be @every"},
		{name: "periodic no @", typ: "periodic", expr: "every 30s",
			wantErr: "must be @every"},
		{name: "periodic bad duration", typ: "periodic", expr: "@every 30x",
			wantErr: "bad duration"},
		// cron (D-6)
		{name: "cron bare 6 fields stored with zone", typ: "cron", expr: "0 0 9 * * *",
			want: "CRON_TZ=Europe/Madrid 0 0 9 * * *"},
		{name: "cron CRON_TZ prefix kept", typ: "cron", expr: "CRON_TZ=Europe/Madrid 0 0 9 * * *",
			want: "CRON_TZ=Europe/Madrid 0 0 9 * * *"},
		{name: "cron TZ prefix kept", typ: "cron", expr: "TZ=Europe/Madrid 0 0 9 * * *",
			want: "TZ=Europe/Madrid 0 0 9 * * *"},
		{name: "cron @daily", typ: "cron", expr: "@daily",
			wantErr: "does not accept descriptors"},
		{name: "cron @every 5s", typ: "cron", expr: "@every 5s",
			wantErr: "does not accept descriptors"},
		{name: "cron 5 fields", typ: "cron", expr: "0 0 9 * *",
			wantErr: "expected exactly 6 fields, found 5"},
		{name: "cron 7 fields", typ: "cron", expr: "0 0 9 * * * 1",
			wantErr: "expected exactly 6 fields, found 7"},
		{name: "cron Feb 30 never fires", typ: "cron", expr: "0 0 0 30 2 *",
			wantErr: "never fires"},
		{name: "cron Madrid next is the UTC instant", typ: "cron", expr: "CRON_TZ=Europe/Madrid 0 0 9 * * *",
			want: "CRON_TZ=Europe/Madrid 0 0 9 * * *", wantNext: madridNine},
		// unknown type
		{name: "unknown type", typ: "weekly", expr: "@every 30s",
			wantErr: "unknown schedule type"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			s, stored, err := parseSchedule(row.typ, row.expr, madrid, now)
			if row.wantErr != "" {
				if err == nil {
					t.Fatalf("want error %q, got none (stored %q)", row.wantErr, stored)
				}
				if !strings.Contains(err.Error(), row.wantErr) {
					t.Fatalf("error %q does not contain %q", err, row.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if stored != row.want {
				t.Fatalf("stored %q, want %q", stored, row.want)
			}
			if !row.wantNext.IsZero() {
				if got := s.Next(now); !got.Equal(row.wantNext) {
					t.Fatalf("Next = %v, want %v", got, row.wantNext)
				}
			}
			if row.once {
				if got := s.Next(now); !got.IsZero() {
					t.Fatalf("second Next = %v, want the zero time", got)
				}
			}
		})
	}
}
