// config_test.go pins every rule of Hora.md's Configuration section: the
// strict decoding and each key row, with the API keys resolved. Each test
// installs a test-only gate registry (saving and restoring gates) so it never
// depends on the shipped registry; none of these tests runs in parallel,
// because they mutate a package variable.
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testRegistry installs a test-only gate registry and restores the real one.
func testRegistry(t *testing.T, gs ...gate) {
	t.Helper()
	saved := gates
	gates = gs
	t.Cleanup(func() { gates = saved })
}

// loadWith sets the test environment and gate registry, writes body as a
// config file in a temp directory, and loads it.
func loadWith(t *testing.T, body string) (Config, error) {
	t.Helper()
	testRegistry(t, gate{name: "v1"}, gate{name: "other"})
	t.Setenv("HORA_TEST_KEY", "sk-1")
	t.Setenv("HORA_TEST_DKEY", "sk-d")
	path := filepath.Join(t.TempDir(), "cfg.json")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return loadConfig(path)
}

// minBody is a fragment that passes every rule with no decision triple and
// no key; it has no closing brace, so test bodies extend it.
const minBody = `{"agent_id":"hora-test","db":"db.json","model":"m","url":"http://u","env_apikey":""`

// minConfig is minBody closed into a whole object.
const minConfig = minBody + "}"

// decisionTriple is the three decision keys, as a JSON fragment.
const decisionTriple = `,"decision_model":"dm","decision_url":"http://du","decision_env_apikey":"HORA_TEST_DKEY"`

func TestLoadConfigHappyPath(t *testing.T) {
	body := `{"agent_id":"hora-test","namespaces":["ns-a",null],"TZ":"Europe/Madrid",` +
		`"coordinators":["c1"],"db":"db.json","model":"m","url":"http://u","env_apikey":"HORA_TEST_KEY",` +
		`"decision_model":"dm","decision_url":"http://du","decision_env_apikey":"HORA_TEST_DKEY","decision_gate":"v1"}`
	c, err := loadWith(t, body)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if c.AgentID != "hora-test" || c.Model != "m" || c.URL != "http://u" {
		t.Fatalf("scalar fields: %+v", c)
	}
	if len(c.Namespaces) != 2 || c.Namespaces[1] != nil || *c.Namespaces[0] != "ns-a" {
		t.Fatalf("namespaces: %v", c.Namespaces)
	}
	if c.TZ != "Europe/Madrid" || c.Loc.String() != "Europe/Madrid" {
		t.Fatalf("TZ: %q loc %v", c.TZ, c.Loc)
	}
	if len(c.Coordinators) != 1 || c.Coordinators[0] != "c1" {
		t.Fatalf("coordinators: %v", c.Coordinators)
	}
	if !filepath.IsAbs(c.DB) {
		t.Fatalf("db not resolved to an absolute path: %q", c.DB)
	}
	if c.APIKey != "sk-1" || c.DecisionAPIKey != "sk-d" {
		t.Fatalf("keys: %q %q", c.APIKey, c.DecisionAPIKey)
	}
	if c.DecisionGate != "v1" {
		t.Fatalf("decision_gate: %q", c.DecisionGate)
	}
}

func TestLoadConfigStrictDecoding(t *testing.T) {
	if _, err := loadWith(t, `{"bogus":1,`+minConfig[1:]); err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("unknown key: %v", err)
	}
	if _, err := loadWith(t, minConfig+` {"b":2}`); err == nil || !strings.Contains(err.Error(), "second JSON value") {
		t.Fatalf("second value: %v", err)
	}
}

func TestLoadConfigAgentIDRequired(t *testing.T) {
	if _, err := loadWith(t, `{}`); err == nil || !strings.Contains(err.Error(), "agent_id") {
		t.Fatalf("missing agent_id: %v", err)
	}
}

func TestLoadConfigNamespaces(t *testing.T) {
	c, err := loadWith(t, minConfig)
	if err != nil || c.Namespaces != nil {
		t.Fatalf("absent: %v %v", c.Namespaces, err)
	}
	c, err = loadWith(t, `{"agent_id":"a","db":"d","model":"m","url":"u","env_apikey":"","namespaces":null}`)
	if err != nil || c.Namespaces != nil {
		t.Fatalf("null: %v %v", c.Namespaces, err)
	}
	c, err = loadWith(t, `{"agent_id":"a","db":"d","model":"m","url":"u","env_apikey":"","namespaces":[]}`)
	if err != nil || c.Namespaces != nil {
		t.Fatalf("empty: %v %v", c.Namespaces, err)
	}
	c, err = loadWith(t, `{"agent_id":"a","db":"d","model":"m","url":"u","env_apikey":"","namespaces":["a",null]}`)
	if err != nil || len(c.Namespaces) != 2 || c.Namespaces[0] == nil || *c.Namespaces[0] != "a" || c.Namespaces[1] != nil {
		t.Fatalf("items: %v %v", c.Namespaces, err)
	}
}

func TestLoadConfigTZ(t *testing.T) {
	c, err := loadWith(t, minConfig)
	if err != nil || c.TZ != "UTC" || c.Loc.String() != "UTC" {
		t.Fatalf("default: %q %v %v", c.TZ, c.Loc, err)
	}
	c, err = loadWith(t, `{"agent_id":"a","db":"d","model":"m","url":"u","env_apikey":"","TZ":"Europe/Madrid"}`)
	if err != nil || c.TZ != "Europe/Madrid" || c.Loc.String() != "Europe/Madrid" {
		t.Fatalf("madrid: %q %v %v", c.TZ, c.Loc, err)
	}
	if _, err := loadWith(t, `{"agent_id":"a","db":"d","model":"m","url":"u","env_apikey":"","TZ":"Not/AZone"}`); err == nil || !strings.Contains(err.Error(), "TZ") {
		t.Fatalf("bad zone: %v", err)
	}
}

func TestLoadConfigCoordinators(t *testing.T) {
	c, err := loadWith(t, minConfig)
	if err != nil || len(c.Coordinators) != 0 {
		t.Fatalf("default: %v %v", c.Coordinators, err)
	}
	c, err = loadWith(t, `{"agent_id":"a","db":"d","model":"m","url":"u","env_apikey":"","coordinators":["x","y"]}`)
	if err != nil || len(c.Coordinators) != 2 || c.Coordinators[0] != "x" || c.Coordinators[1] != "y" {
		t.Fatalf("values: %v %v", c.Coordinators, err)
	}
}

func TestLoadConfigDB(t *testing.T) {
	if _, err := loadWith(t, `{"agent_id":"a","model":"m","url":"u","env_apikey":""}`); err == nil || !strings.Contains(err.Error(), "db") {
		t.Fatalf("missing: %v", err)
	}
	c, err := loadWith(t, minConfig)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(c.DB) == "." || !strings.HasSuffix(c.DB, "db.json") {
		t.Fatalf("relative db not resolved against the config file's directory: %q", c.DB)
	}
	if _, err := loadWith(t, `{"agent_id":"a","db":"nodir/x.json","model":"m","url":"u","env_apikey":""}`); err == nil || !strings.Contains(err.Error(), "db") {
		t.Fatalf("missing directory: %v", err)
	}
}

func TestLoadConfigChatRequired(t *testing.T) {
	if _, err := loadWith(t, `{"agent_id":"a","db":"d","url":"u","env_apikey":""}`); err == nil || !strings.Contains(err.Error(), "model") {
		t.Fatalf("model: %v", err)
	}
	if _, err := loadWith(t, `{"agent_id":"a","db":"d","model":"m","env_apikey":""}`); err == nil || !strings.Contains(err.Error(), "url") {
		t.Fatalf("url: %v", err)
	}
	if _, err := loadWith(t, `{"agent_id":"a","db":"d","model":"m","url":"u"}`); err == nil || !strings.Contains(err.Error(), "env_apikey") {
		t.Fatalf("env_apikey: %v", err)
	}
}

func TestLoadConfigEnvAPIKey(t *testing.T) {
	c, err := loadWith(t, minConfig)
	if err != nil || c.APIKey != "" {
		t.Fatalf("empty name: %q %v", c.APIKey, err)
	}
	c, err = loadWith(t, `{"agent_id":"a","db":"d","model":"m","url":"u","env_apikey":"HORA_TEST_KEY"}`)
	if err != nil || c.APIKey != "sk-1" {
		t.Fatalf("set variable: %q %v", c.APIKey, err)
	}
	if _, err := loadWith(t, `{"agent_id":"a","db":"d","model":"m","url":"u","env_apikey":"HORA_UNSET_VAR"}`); err == nil || !strings.Contains(err.Error(), "env_apikey") {
		t.Fatalf("unset variable: %v", err)
	}
	t.Setenv("HORA_TEST_EMPTY", "")
	if _, err := loadWith(t, `{"agent_id":"a","db":"d","model":"m","url":"u","env_apikey":"HORA_TEST_EMPTY"}`); err == nil || !strings.Contains(err.Error(), "env_apikey") {
		t.Fatalf("empty variable: %v", err)
	}
}

func TestLoadConfigDecisionTriple(t *testing.T) {
	c, err := loadWith(t, minBody+decisionTriple+"}")
	if err != nil || c.DecisionModel != "dm" || c.DecisionURL != "http://du" || c.DecisionAPIKey != "sk-d" {
		t.Fatalf("all three: %+v %v", c, err)
	}
	if _, err := loadWith(t, minConfig); err != nil {
		t.Fatalf("none: %v", err)
	}
	for _, frag := range []string{
		`,"decision_model":"dm"`,
		`,"decision_model":"dm","decision_url":"http://du"`,
		`,"decision_model":"dm","decision_env_apikey":"HORA_TEST_DKEY"`,
		`,"decision_url":"http://du"`,
		`,"decision_env_apikey":"HORA_TEST_DKEY"`,
	} {
		if _, err := loadWith(t, minBody+frag+"}"); err == nil || !strings.Contains(err.Error(), "decision") {
			t.Fatalf("partial triple %s: %v", frag, err)
		}
	}
}

func TestLoadConfigDecisionGate(t *testing.T) {
	c, err := loadWith(t, minConfig)
	if err != nil || c.DecisionGate != "v1" {
		t.Fatalf("absent key: %q %v", c.DecisionGate, err)
	}
	c, err = loadWith(t, minBody+decisionTriple+"}")
	if err != nil || c.DecisionGate != "v1" {
		t.Fatalf("absent key with triple: %q %v", c.DecisionGate, err)
	}
	c, err = loadWith(t, minBody+decisionTriple+`,"decision_gate":"other"}`)
	if err != nil || c.DecisionGate != "other" {
		t.Fatalf("known name: %q %v", c.DecisionGate, err)
	}
	if _, err := loadWith(t, minBody+decisionTriple+`,"decision_gate":"unknown"}`); err == nil || !strings.Contains(err.Error(), "decision_gate") {
		t.Fatalf("unknown name: %v", err)
	}
	if _, err := loadWith(t, minBody+`,"decision_gate":"v1"}`); err == nil || !strings.Contains(err.Error(), "decision_gate") {
		t.Fatalf("key without the triple: %v", err)
	}
}
