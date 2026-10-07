package app_test

import (
	"testing"

	"go.uber.org/fx"

	"github.com/gamsanches06/jungle-test/internal/app"
	"github.com/gamsanches06/jungle-test/internal/config"
)

// TestGraphIsComplete validates the Fx dependency graph of every role
// combination without starting any component.
func TestGraphIsComplete(t *testing.T) {
	for _, roles := range []string{"api,consumer,outbox,pending", "api", "consumer", "outbox", "pending"} {
		env := map[string]string{
			"APP_ROLES": roles, "DATABASE_URL": "postgres://u:p@localhost:1/db",
			"OIDC_ISSUER": "http://localhost:1/realms/x", "OIDC_JWKS_URL": "http://localhost:1/certs",
		}
		cfg, err := config.FromLookup(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
		if err != nil {
			t.Fatal(err)
		}
		if err := fx.ValidateApp(app.Options(cfg)); err != nil {
			t.Fatalf("roles %s: %v", roles, err)
		}
	}
}
