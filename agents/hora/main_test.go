// main_test.go pins run's start failures (D-3, D-12; Hora.md,
// Configuration): each one names its reason, and none leaves a
// half-started state behind.
package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// validConfig passes loadConfig; its db file need not exist, because a
// missing file is an empty DB.
const validConfig = `{"agent_id":"hora-test","model":"m","url":"http://127.0.0.1:1","db":"db.json","env_apikey":""}`

func TestRunStartFailure(t *testing.T) {
	rows := []struct {
		name    string
		config  string // "" leaves the config file unwritten
		db      string // content to write to the db file, "" for none
		wantErr string
	}{
		{
			name:    "a bad config names the key",
			config:  `{"agent_id":"hora-test"}`,
			wantErr: "model",
		},
		{
			name:    "a missing config file",
			wantErr: "no such file",
		},
		{
			name:    "a db whose directory does not exist",
			config:  `{"agent_id":"hora-test","model":"m","url":"http://127.0.0.1:1","db":"nodir/db.json","env_apikey":""}`,
			wantErr: "db",
		},
		{
			name:    "an unparseable db",
			config:  validConfig,
			db:      "not a json array",
			wantErr: "not a JSON array",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("ACTIO_DIR", dir)
			before := runtime.NumGoroutine()
			configPath := filepath.Join(dir, "config.json")
			if row.config != "" {
				if err := os.WriteFile(configPath, []byte(row.config), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if row.db != "" {
				if err := os.WriteFile(filepath.Join(dir, "db.json"), []byte(row.db), 0600); err != nil {
					t.Fatal(err)
				}
			}
			err := run(context.Background(), configPath)
			if err == nil {
				t.Fatal("run succeeded, want an error")
			}
			if !strings.Contains(err.Error(), row.wantErr) {
				t.Fatalf("error %q does not contain %q", err, row.wantErr)
			}
			// No half-started state: the temp root holds only the files the
			// row wrote, and no goroutine the run left behind.
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				if name := e.Name(); name != "config.json" && name != "db.json" {
					t.Fatalf("unexpected %q left in the temp root", name)
				}
			}
			settled(t, before)
			if row.db != "" {
				data, err := os.ReadFile(filepath.Join(dir, "db.json"))
				if err != nil {
					t.Fatal(err)
				}
				if string(data) != row.db {
					t.Fatalf("db file rewritten to %q", data)
				}
			}
		})
	}
}

// settled polls, with a real-time deadline, until the goroutine count is
// back at the baseline the row measured before run.
func settled(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline+2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("goroutine count %d did not settle to %d", runtime.NumGoroutine(), baseline)
}
