// Tests for config.go: the agent's timers and their protocol units (*Timers and thresholds*),
// the environment overrides given in seconds, the precedence defaults < environment <
// overrides, and the two-file contract that a bad ACTIO_SYNC survives LoadConfig and is
// rejected by newSyncProvider at Start.
package actio

import (
	"os"
	"strings"
	"testing"
	"time"
)

// The team environment legitimately sets ACTIO_DIR, so the "no environment" cases need the
// ACTIO_* names genuinely unset for their duration. t.Setenv cannot unset a variable, so this
// helper unsets by hand and restores on cleanup; every other environment change in these tests
// uses t.Setenv. The tests never run in parallel, so the process environment is safe to mutate.
func withCleanEnv(t *testing.T) {
	t.Helper()
	names := []string{EnvRoot, EnvSync, EnvHeartbeat, EnvConfirmTimeout, EnvCancelWait}
	saved := make(map[string]string, len(names))
	for _, n := range names {
		if v, ok := os.LookupEnv(n); ok {
			saved[n] = v
			os.Unsetenv(n)
		}
	}
	t.Cleanup(func() {
		for n, v := range saved {
			os.Setenv(n, v)
		}
	})
}

// Claim 1: the six protocol timers, their defaults, and the protocol's units. The protocol names
// the timers in seconds, so a default is that many seconds of time.Duration — not milliseconds.
func TestConfigDefaultsMatchProtocolUnits(t *testing.T) {
	withCleanEnv(t)
	cfg, err := LoadConfig(Config{})
	if err != nil {
		t.Fatalf("LoadConfig with no environment: %v", err)
	}
	want := map[string]time.Duration{
		"HeartbeatInterval": 30 * time.Second,  // protocol: at least every 90 s
		"ConfirmTimeout":    120 * time.Second, // protocol: 120 s
		"CancelWaitTimeout": 60 * time.Second,  // B4, seconds
		"AnnounceDelay":     100 * time.Millisecond,
		"NuntiumDebounce":   1 * time.Second,
		"StopGrace":         30 * time.Second,
	}
	got := map[string]time.Duration{
		"HeartbeatInterval": cfg.HeartbeatInterval,
		"ConfirmTimeout":    cfg.ConfirmTimeout,
		"CancelWaitTimeout": cfg.CancelWaitTimeout,
		"AnnounceDelay":     cfg.AnnounceDelay,
		"NuntiumDebounce":   cfg.NuntiumDebounce,
		"StopGrace":         cfg.StopGrace,
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s = %v, want the protocol's %v (seconds, not milliseconds)", name, got[name], w)
		}
	}
	if cfg.Sync != "none" {
		t.Errorf("Sync = %q, want the default %q", cfg.Sync, "none")
	}
	if home := os.Getenv(EnvHome); cfg.Root != home+"/.actio" {
		t.Errorf("Root = %q, want $HOME/.actio = %q", cfg.Root, home+"/.actio")
	}
}

// Claim 2: the precedence defaults < environment < overrides, with all three layers differing.
func TestConfigPrecedenceDefaultEnvOverride(t *testing.T) {
	withCleanEnv(t)
	// Layer 1: the default, with no environment and no override.
	cfg, err := LoadConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HeartbeatInterval != 30*time.Second {
		t.Fatalf("default layer: HeartbeatInterval = %v, want 30s", cfg.HeartbeatInterval)
	}
	// Layer 2: the environment beats the default.
	t.Setenv(EnvHeartbeat, "5")
	cfg, err = LoadConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HeartbeatInterval != 5*time.Second {
		t.Fatalf("env layer: HeartbeatInterval = %v, want 5s", cfg.HeartbeatInterval)
	}
	// Layer 3: the override beats the environment.
	cfg, err = LoadConfig(Config{HeartbeatInterval: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HeartbeatInterval != 10*time.Second {
		t.Fatalf("override layer: HeartbeatInterval = %v, want 10s", cfg.HeartbeatInterval)
	}
}

// Claim 3: the five environment names, exactly. Setting all five must move all five results.
func TestConfigAllFiveEnvNamesAreRead(t *testing.T) {
	withCleanEnv(t)
	t.Setenv(EnvRoot, "/x/actio-root")
	t.Setenv(EnvSync, "delay:50")
	t.Setenv(EnvHeartbeat, "11")
	t.Setenv(EnvConfirmTimeout, "12")
	t.Setenv(EnvCancelWait, "13")
	cfg, err := LoadConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Root != "/x/actio-root" {
		t.Errorf("ACTIO_DIR not read: Root = %q", cfg.Root)
	}
	if cfg.Sync != "delay:50" {
		t.Errorf("ACTIO_SYNC not read: Sync = %q", cfg.Sync)
	}
	if cfg.HeartbeatInterval != 11*time.Second {
		t.Errorf("ACTIO_HEARTBEAT not read: HeartbeatInterval = %v", cfg.HeartbeatInterval)
	}
	if cfg.ConfirmTimeout != 12*time.Second {
		t.Errorf("ACTIO_CONFIRM_TIMEOUT not read: ConfirmTimeout = %v", cfg.ConfirmTimeout)
	}
	if cfg.CancelWaitTimeout != 13*time.Second {
		t.Errorf("ACTIO_CANCEL_WAIT not read: CancelWaitTimeout = %v", cfg.CancelWaitTimeout)
	}
}

// Claim 3 (negative half): a name with a typo in the environment is not read; the results stay
// at the defaults.
func TestConfigEnvNameWithTypoIsNotRead(t *testing.T) {
	withCleanEnv(t)
	typo := map[string]string{
		"ACTIO_DIRECTORY":           "/x/typo",
		"ACTIO_SYNC_MODE":           "delay:50",
		"ACTIO_HEARTBEAT_INTERVAL":  "5",
		"ACTIO_HEATBEAT":            "5",
		"ACTIO_CONFIRM_TIMEOUT_SEC": "5",
		"ACTIO_CANCEL_WAIT_TIMEOUT": "5",
		"ACTIO_ANNOUNCE_DELAY":      "5",
		"ACTIO_NUNTIUM_DEBOUNCE":    "5",
		"ACTIO_STOP_GRACE":          "5",
	}
	for name, value := range typo {
		t.Setenv(name, value)
	}
	cfg, err := LoadConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if home := os.Getenv(EnvHome); cfg.Root != home+"/.actio" {
		t.Errorf("a typo'd root name was read: Root = %q", cfg.Root)
	}
	if cfg.Sync != "none" {
		t.Errorf("a typo'd sync name was read: Sync = %q", cfg.Sync)
	}
	defaults := map[string]time.Duration{
		"HeartbeatInterval": 30 * time.Second,
		"ConfirmTimeout":    120 * time.Second,
		"CancelWaitTimeout": 60 * time.Second,
	}
	for name, d := range map[string]time.Duration{
		"HeartbeatInterval": cfg.HeartbeatInterval,
		"ConfirmTimeout":    cfg.ConfirmTimeout,
		"CancelWaitTimeout": cfg.CancelWaitTimeout,
	} {
		if d != defaults[name] {
			t.Errorf("a typo'd timer name was read: %s = %v (want the default %v)", name, d, defaults[name])
		}
	}
}

// Seconds in, duration out: the environment names the three protocol timers in seconds.
func TestConfigEnvSecondsBecomeDurations(t *testing.T) {
	cases := []struct {
		env  string
		set  string
		want time.Duration
	}{
		{EnvHeartbeat, "1.5", 1500 * time.Millisecond},
		{EnvConfirmTimeout, "2", 2 * time.Second},
		{EnvCancelWait, "0.25", 250 * time.Millisecond},
	}
	for _, c := range cases {
		t.Run(c.env, func(t *testing.T) {
			withCleanEnv(t)
			t.Setenv(c.env, c.set)
			cfg, err := LoadConfig(Config{})
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]time.Duration{
				EnvHeartbeat:      cfg.HeartbeatInterval,
				EnvConfirmTimeout: cfg.ConfirmTimeout,
				EnvCancelWait:     cfg.CancelWaitTimeout,
			}[c.env]
			if got != c.want {
				t.Fatalf("%s=%s -> %v, want %v", c.env, c.set, got, c.want)
			}
		})
	}
}

// A fraction of a second below nanosecond precision cannot survive the conversion to
// time.Duration: the code truncates it (a direct float to int64 conversion), it does not report
// it. 1.0000000001 s is 0.1 ns above 1 s, and the 0.1 ns is dropped.
func TestConfigEnvFractionBelowNanosecondIsTruncatedNotReported(t *testing.T) {
	withCleanEnv(t)
	t.Setenv(EnvHeartbeat, "1.0000000001")
	cfg, err := LoadConfig(Config{})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.HeartbeatInterval != 1*time.Second {
		t.Fatalf("HeartbeatInterval = %v, want exactly 1s (the 0.1 ns fraction is truncated)", cfg.HeartbeatInterval)
	}
}

// Claim 4: a bad value for each of the three timer variables is an error that names the variable
// and the value it rejected; nothing is silently defaulted.
func TestConfigBadEnvValueIsAnErrorNamingVariableAndValue(t *testing.T) {
	vars := []string{EnvHeartbeat, EnvConfirmTimeout, EnvCancelWait}
	bad := []string{"abc", "0", "-5", "1e400"}
	for _, key := range vars {
		for _, value := range bad {
			t.Run(key+"="+value, func(t *testing.T) {
				withCleanEnv(t)
				t.Setenv(key, value)
				_, err := LoadConfig(Config{})
				if err == nil {
					t.Fatalf("%s=%s: no error, want one naming the variable and the value", key, value)
				}
				if !strings.Contains(err.Error(), key) {
					t.Errorf("error %q does not name the variable %s", err, key)
				}
				if !strings.Contains(err.Error(), `"`+value+`"`) {
					t.Errorf("error %q does not name the rejected value %q", err, value)
				}
			})
		}
	}
}

// The reference accepts a surrounded value — config.ts's Number(" 5 ") is 5 — while Go's
// ParseFloat rejects it. This test pins the difference: " 5 " is an error in Go.
func TestConfigEnvValueWithWhitespaceIsRejectedUnlikeReference(t *testing.T) {
	withCleanEnv(t)
	t.Setenv(EnvHeartbeat, " 5 ")
	_, err := LoadConfig(Config{})
	if err == nil {
		t.Fatal(`ACTIO_HEARTBEAT=" 5 ": no error, want one (Go rejects the whitespace the reference accepts)`)
	}
	if !strings.Contains(err.Error(), EnvHeartbeat) || !strings.Contains(err.Error(), `" 5 "`) {
		t.Errorf("error %q does not name the variable and the value", err)
	}
}

// Set but empty: ACTIO_DIR="" is the empty root, which LoadConfig reports — it does not fall
// back to the default, because guessing a root the operator meant to set would put the agent's
// files in the wrong folder.
func TestConfigSetButEmptyDirIsReportedAsEmptyRoot(t *testing.T) {
	withCleanEnv(t)
	t.Setenv(EnvRoot, "")
	_, err := LoadConfig(Config{})
	if err == nil {
		t.Fatal(`ACTIO_DIR="": no error, want the empty root reported`)
	}
	if !strings.Contains(err.Error(), "Root must not be empty") {
		t.Errorf("error %q does not name the empty root", err)
	}
}

// Set but empty: ACTIO_SYNC="" survives LoadConfig untouched; the provider parser is what
// rejects it, at Start. (newSyncProvider("") is the rejection; see the sync contract test.)
func TestConfigSetButEmptySyncSurvivesLoadConfig(t *testing.T) {
	withCleanEnv(t)
	t.Setenv(EnvSync, "")
	cfg, err := LoadConfig(Config{})
	if err != nil {
		t.Fatalf(`ACTIO_SYNC="": LoadConfig must not reject it, got %v`, err)
	}
	if cfg.Sync != "" {
		t.Fatalf("Sync = %q, want the untouched empty string", cfg.Sync)
	}
}

// Set but empty: an empty timer variable is not an override (as in the reference); the default
// stands.
func TestConfigSetButEmptyTimerIsNotAnOverride(t *testing.T) {
	withCleanEnv(t)
	t.Setenv(EnvHeartbeat, "")
	cfg, err := LoadConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HeartbeatInterval != 30*time.Second {
		t.Fatalf("HeartbeatInterval = %v, want the default 30s", cfg.HeartbeatInterval)
	}
}

// A zero override means "not set": it does not clear an environment value or a default.
func TestConfigZeroOverrideMeansNotSet(t *testing.T) {
	withCleanEnv(t)
	t.Setenv(EnvHeartbeat, "5")
	cfg, err := LoadConfig(Config{HeartbeatInterval: 0})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HeartbeatInterval != 5*time.Second {
		t.Fatalf("HeartbeatInterval = %v, want 5s (a zero override must not clear the env value)", cfg.HeartbeatInterval)
	}
	cfg, err = LoadConfig(Config{AnnounceDelay: 0, NuntiumDebounce: 0, StopGrace: 0})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AnnounceDelay != 100*time.Millisecond || cfg.NuntiumDebounce != time.Second || cfg.StopGrace != 30*time.Second {
		t.Fatalf("zero overrides cleared defaults: %v %v %v", cfg.AnnounceDelay, cfg.NuntiumDebounce, cfg.StopGrace)
	}
}

// A nil Clock means the wall clock: two Now() calls of the result differ. A provided clock is
// kept as is.
func TestConfigNilClockMeansWallClock(t *testing.T) {
	withCleanEnv(t)
	cfg, err := LoadConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Clock == nil {
		t.Fatal("Clock is nil after LoadConfig; it must default to the wall clock")
	}
	first := cfg.Clock()
	time.Sleep(2 * time.Millisecond)
	second := cfg.Clock()
	if !first.Before(second) {
		t.Fatalf("two wall-clock readings are equal or inverted: %v, %v", first, second)
	}
	fixed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	cfg, err = LoadConfig(Config{Clock: func() time.Time { return fixed }})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Clock(); !got.Equal(fixed) || !cfg.Clock().Equal(fixed) {
		t.Fatalf("the provided clock was not kept: %v", got)
	}
}

// Claim 5: the sync string is not parsed by LoadConfig. Unknown values pass it untouched, and
// newSyncProvider — which runs at Start — is what rejects them. That pair is the plan's "a bad
// sync value surfaces at Start".
func TestConfigSyncSurvivesLoadConfigAndIsRejectedAtProviderParse(t *testing.T) {
	withCleanEnv(t)
	rejected := []struct{ name, value string }{
		{"unknown", "bogus"},
		{"empty", ""},
		{"delay-zero", "delay:0"},
		{"mutagen-no-session", "mutagen:"},
	}
	for _, c := range rejected {
		t.Run("rejected/"+c.name, func(t *testing.T) {
			t.Setenv(EnvSync, c.value)
			cfg, err := LoadConfig(Config{})
			if err != nil {
				t.Fatalf("LoadConfig must not parse the sync value, got %v", err)
			}
			if cfg.Sync != c.value {
				t.Fatalf("Sync = %q, want the untouched %q", cfg.Sync, c.value)
			}
			if _, perr := newSyncProvider(cfg.Sync, nil); perr == nil {
				t.Fatalf("newSyncProvider(%q) must reject the value", c.value)
			}
		})
	}
	for _, value := range []string{"none", "delay:50", "fsync+delay:50", "mutagen:sess"} {
		t.Run("accepted/"+value, func(t *testing.T) {
			t.Setenv(EnvSync, value)
			cfg, err := LoadConfig(Config{})
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Sync != value {
				t.Fatalf("Sync = %q, want %q", cfg.Sync, value)
			}
			p, perr := newSyncProvider(cfg.Sync, nil)
			if perr != nil {
				t.Fatalf("newSyncProvider(%q) must accept the value: %v", value, perr)
			}
			p.Close()
		})
	}
}

// With HOME empty, the default root is "/.actio" — a non-empty string at the file system root,
// which LoadConfig accepts. The reference throws instead (its root becomes ""). This pins the
// difference; see the report's Open points.
func TestConfigEmptyHomeYieldsRootSlashActioUnlikeReference(t *testing.T) {
	withCleanEnv(t)
	t.Setenv(EnvHome, "")
	cfg, err := LoadConfig(Config{})
	if err != nil {
		t.Fatalf("LoadConfig with HOME empty: %v", err)
	}
	if cfg.Root != "/.actio" {
		t.Fatalf("Root = %q, want the file system root .actio", cfg.Root)
	}
}
