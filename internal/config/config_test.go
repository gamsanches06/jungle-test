package config_test

import (
	"strings"
	"testing"

	"github.com/gamsanches06/jungle-test/internal/config"
)

func lookup(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func base() map[string]string {
	return map[string]string{
		"DATABASE_URL": "postgres://u:p@localhost/db", "OIDC_ISSUER": "http://localhost:8081/realms/jungle",
		"OIDC_JWKS_URL": "http://localhost:8081/realms/jungle/protocol/openid-connect/certs",
	}
}

func TestDefaults(t *testing.T) {
	c, err := config.FromLookup(lookup(base()))
	if err != nil {
		t.Fatal(err)
	}
	if !c.Roles.API || !c.Roles.Consumer || !c.Roles.Outbox || !c.Roles.Pending || c.InputQueueName != "wager-transactions.fifo" {
		t.Fatalf("defaults = %+v", c)
	}
}

func TestValidation(t *testing.T) {
	cases := map[string]map[string]string{
		"missing db":          {"DATABASE_URL": ""},
		"bad duration":        {"SQS_VISIBILITY_TIMEOUT": "soon"},
		"process>=visibility": {"SQS_PROCESS_TIMEOUT": "40s"},
		"bad role":            {"APP_ROLES": "api,teleport"},
		"relative issuer":     {"OIDC_ISSUER": "/realms/x"},
		"too many messages":   {"SQS_MAX_MESSAGES": "11"},
	}
	for name, override := range cases {
		env := base()
		for k, v := range override {
			env[k] = v
		}
		if _, err := config.FromLookup(lookup(env)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	env := base()
	env["APP_ROLES"] = "consumer"
	delete(env, "OIDC_ISSUER")
	delete(env, "OIDC_JWKS_URL")
	c, err := config.FromLookup(lookup(env))
	if err != nil || c.Roles.API {
		t.Fatalf("consumer-only without OIDC = %v %v", c.Roles, err)
	}
	if err := (config.Config{}).Validate(); err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("empty config = %v", err)
	}
}
