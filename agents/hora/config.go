// config.go loads and validates the hora configuration file (Hora.md,
// Configuration). A config error names the offending key.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Config is the decoded configuration file. The last three fields are
// resolved by loadConfig from the file's values.
type Config struct {
	AgentID           string         `json:"agent_id"`
	Namespaces        []*string      `json:"namespaces"`
	TZ                string         `json:"TZ"`
	Coordinators      []string       `json:"coordinators"`
	DB                string         `json:"db"`
	Model             string         `json:"model"`
	URL               string         `json:"url"`
	EnvAPIKey         string         `json:"env_apikey"`
	DecisionModel     string         `json:"decision_model"`
	DecisionURL       string         `json:"decision_url"`
	DecisionEnvAPIKey string         `json:"decision_env_apikey"`
	DecisionGate      string         `json:"decision_gate"` // default v1
	Loc               *time.Location `json:"-"`
	APIKey            string         `json:"-"`
	DecisionAPIKey    string         `json:"-"`
}

// loadConfig reads the config file at path, checks every rule of Hora.md's
// Configuration section, and resolves the zone and the API keys.
func loadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: %v", err)
	}
	var c Config
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("config: %v", err)
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return Config{}, fmt.Errorf("config: a second JSON value after the object")
	}
	var present map[string]json.RawMessage
	if err := json.Unmarshal(data, &present); err != nil {
		return Config{}, fmt.Errorf("config: %v", err)
	}

	if c.AgentID == "" {
		return Config{}, fmt.Errorf("config: agent_id: required")
	}
	if c.Model == "" {
		return Config{}, fmt.Errorf("config: model: required")
	}
	if c.URL == "" {
		return Config{}, fmt.Errorf("config: url: required")
	}
	if c.DB == "" {
		return Config{}, fmt.Errorf("config: db: required")
	}
	if !filepath.IsAbs(c.DB) {
		c.DB = filepath.Join(filepath.Dir(path), c.DB)
	}
	if st, err := os.Stat(filepath.Dir(c.DB)); err != nil || !st.IsDir() {
		return Config{}, fmt.Errorf("config: db: the directory %s does not exist", filepath.Dir(c.DB))
	}
	if len(c.Namespaces) == 0 {
		c.Namespaces = nil
	}
	if c.TZ == "" {
		c.TZ = "UTC"
	}
	c.Loc, err = time.LoadLocation(c.TZ)
	if err != nil {
		return Config{}, fmt.Errorf("config: TZ: %v", err)
	}
	if _, ok := present["env_apikey"]; !ok {
		return Config{}, fmt.Errorf("config: env_apikey: required")
	}
	c.APIKey, err = resolveKey(c.EnvAPIKey, "env_apikey")
	if err != nil {
		return Config{}, err
	}
	nDecision := 0
	for _, key := range []string{"decision_model", "decision_url", "decision_env_apikey"} {
		if _, ok := present[key]; ok {
			nDecision++
		}
	}
	if nDecision != 0 && nDecision != 3 {
		return Config{}, fmt.Errorf("config: decision_model, decision_url and decision_env_apikey: all three keys present, or none")
	}
	if nDecision == 3 && c.DecisionModel == "" {
		return Config{}, fmt.Errorf("config: decision_model: must be non-empty when present")
	}
	if nDecision == 3 && c.DecisionURL == "" {
		return Config{}, fmt.Errorf("config: decision_url: must be non-empty when present")
	}
	c.DecisionAPIKey, err = resolveKey(c.DecisionEnvAPIKey, "decision_env_apikey")
	if err != nil {
		return Config{}, err
	}
	if _, ok := present["decision_gate"]; ok && nDecision != 3 {
		return Config{}, fmt.Errorf("config: decision_gate: requires the decision_model, decision_url and decision_env_apikey keys")
	}
	if c.DecisionGate == "" {
		c.DecisionGate = "v1"
	}
	if _, ok := findGate(c.DecisionGate); !ok {
		return Config{}, fmt.Errorf("config: decision_gate: unknown gate %q", c.DecisionGate)
	}
	return c, nil
}

// resolveKey resolves an env_apikey value: the empty name means no key, and a
// named variable must be set and non-empty at start.
func resolveKey(name, key string) (string, error) {
	if name == "" {
		return "", nil
	}
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return "", fmt.Errorf("config: %s: the environment variable %s is not set or is empty", key, name)
	}
	return v, nil
}
