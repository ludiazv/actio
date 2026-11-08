// config.go loads and validates the hora configuration file (Hora.md, Configuration).
package main

import "time"

// Config is the decoded configuration file. The last three fields are
// resolved by loadConfig from the env_apikey and decision_env_apikey names.
type Config struct {
	AgentID           string    `json:"agent_id"`
	Namespaces        []*string `json:"namespaces"`
	TZ                string    `json:"TZ"`
	Coordinators      []string  `json:"coordinators"`
	DB                string    `json:"db"`
	Model             string    `json:"model"`
	URL               string    `json:"url"`
	EnvAPIKey         string    `json:"env_apikey"`
	DecisionModel     string    `json:"decision_model"`
	DecisionURL       string    `json:"decision_url"`
	DecisionEnvAPIKey string    `json:"decision_env_apikey"`
	DecisionGate      string    `json:"decision_gate"` // default v1
	Loc               *time.Location
	APIKey            string
	DecisionAPIKey    string
}

// loadConfig reads the config file at path, validates it (D-6), and resolves
// the API keys. A config error names the offending key.
func loadConfig(path string) (Config, error) {
	panic("not implemented: loadConfig")
}
