// Configuration: the timers of one agent, their defaults, their environment overrides and the
// injectable clock (*Timers and thresholds*). Durations are Go durations in the struct and seconds
// in the environment, as the protocol names them.
package actio

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
)

// Config holds every timer of one agent. A zero field means "not set" and takes its default in
// LoadConfig; no key needs an explicit zero. Clock nil means the wall clock.
type Config struct {
	// Root is the actio folder shared by the team: <root>/nuntium, <root>/forum, <root>/<agent>.
	Root string
	// Sync is the raw ACTIO_SYNC value: "none", "delay[:<ms>]", "fsync+delay[:<ms>]" or
	// "mutagen:<session>". It is parsed into a provider when the agent starts.
	Sync string

	HeartbeatInterval time.Duration // publish the status again after this (protocol: at least 90 s)
	ConfirmTimeout    time.Duration // the receiver's wait for a confirmation (protocol: 120 s)
	CancelWaitTimeout time.Duration // after our own cancellation, keep waiting for the response (B4)
	AnnounceDelay     time.Duration // between a status write and the announce file
	NuntiumDebounce   time.Duration // one debounced scan per burst of nuntium changes
	StopGrace         time.Duration // how long Stop waits for the work in progress

	Clock func() time.Time // tests replace it; never nil after LoadConfig
}

// Defaults equal to the protocol's and to the reference implementation's.
const (
	defaultHeartbeatInterval = 30 * time.Second
	defaultConfirmTimeout    = 120 * time.Second
	defaultCancelWaitTimeout = 60 * time.Second
	defaultAnnounceDelay     = 100 * time.Millisecond
	defaultNuntiumDebounce   = 1 * time.Second
	defaultStopGrace         = 30 * time.Second
	defaultSync              = "none"
)

// Environment overrides (*Timers and thresholds*): the three protocol timers are given in seconds.
const (
	EnvRoot           = "ACTIO_DIR"
	EnvSync           = "ACTIO_SYNC"
	EnvHeartbeat      = "ACTIO_HEARTBEAT"
	EnvConfirmTimeout = "ACTIO_CONFIRM_TIMEOUT"
	EnvCancelWait     = "ACTIO_CANCEL_WAIT"
	EnvMutagenBin     = "ACTIO_MUTAGEN_BIN" // the mutagen provider's binary path
	EnvHome           = "HOME"
)

// LoadConfig resolves the configuration: defaults, then the environment, then overrides, where a
// zero field of overrides means "not set". It reports a bad environment value rather than starting
// an agent on a wrong timer. The sync string is not parsed here: an unknown provider surfaces at
// Start, as in the reference implementation.
func LoadConfig(overrides Config) (Config, error) {
	cfg := Config{
		Root:              defaultRoot(),
		Sync:              defaultSync,
		HeartbeatInterval: defaultHeartbeatInterval,
		ConfirmTimeout:    defaultConfirmTimeout,
		CancelWaitTimeout: defaultCancelWaitTimeout,
		AnnounceDelay:     defaultAnnounceDelay,
		NuntiumDebounce:   defaultNuntiumDebounce,
		StopGrace:         defaultStopGrace,
		Clock:             time.Now,
	}
	// An empty value is different from an unset one: ACTIO_DIR="" is the empty root, which
	// the check at the end reports, because guessing a root the operator meant to set would put the
	// agent's files in the wrong folder. ACTIO_SYNC="" survives LoadConfig untouched: the provider
	// parser is what rejects a sync value, and it runs at Start, as in the reference.
	if v, ok := os.LookupEnv(EnvRoot); ok {
		cfg.Root = v
	}
	if v, ok := os.LookupEnv(EnvSync); ok {
		cfg.Sync = v
	}
	for _, env := range []struct {
		key  string
		into *time.Duration
	}{
		{EnvHeartbeat, &cfg.HeartbeatInterval},
		{EnvConfirmTimeout, &cfg.ConfirmTimeout},
		{EnvCancelWait, &cfg.CancelWaitTimeout},
	} {
		seconds, err := envSeconds(env.key)
		if err != nil {
			return Config{}, err
		}
		if seconds > 0 {
			*env.into = seconds
		}
	}

	if overrides.Root != "" {
		cfg.Root = overrides.Root
	}
	if overrides.Sync != "" {
		cfg.Sync = overrides.Sync
	}
	for _, over := range []struct {
		value time.Duration
		into  *time.Duration
	}{
		{overrides.HeartbeatInterval, &cfg.HeartbeatInterval},
		{overrides.ConfirmTimeout, &cfg.ConfirmTimeout},
		{overrides.CancelWaitTimeout, &cfg.CancelWaitTimeout},
		{overrides.AnnounceDelay, &cfg.AnnounceDelay},
		{overrides.NuntiumDebounce, &cfg.NuntiumDebounce},
		{overrides.StopGrace, &cfg.StopGrace},
	} {
		if over.value != 0 {
			*over.into = over.value
		}
	}
	if overrides.Clock != nil {
		cfg.Clock = overrides.Clock
	}

	if cfg.Root == "" {
		return Config{}, errors.New("actio: config Root must not be empty")
	}
	for _, timed := range []struct {
		key   string
		value time.Duration
	}{
		{"HeartbeatInterval", cfg.HeartbeatInterval},
		{"ConfirmTimeout", cfg.ConfirmTimeout},
		{"CancelWaitTimeout", cfg.CancelWaitTimeout},
		{"AnnounceDelay", cfg.AnnounceDelay},
		{"NuntiumDebounce", cfg.NuntiumDebounce},
		{"StopGrace", cfg.StopGrace},
	} {
		if timed.value <= 0 {
			return Config{}, fmt.Errorf("actio: config %s must be above zero, got %s", timed.key, timed.value)
		}
	}
	return cfg, nil
}

// defaultRoot is the protocol's default location: <home>/.actio (*Actio directory structure*).
func defaultRoot() string {
	return os.Getenv(EnvHome) + "/.actio"
}

// envSeconds reads one environment override given in seconds; unset or empty is not an override.
// A non-number, an infinite or NaN one, or a non-positive one is a configuration error, never a
// silent default. ParseFloat accepts "Inf" and "NaN" without an error, so finiteness is checked
// here; a timer of infinite length is a timer that never fires.
func envSeconds(key string) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return 0, nil
	}
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds <= 0 {
		return 0, fmt.Errorf("actio: %s must be a positive number of seconds, got %q", key, raw)
	}
	return time.Duration(seconds * float64(time.Second)), nil
}
