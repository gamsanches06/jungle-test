// Package auth validates OAuth 2.0 access tokens issued by the external
// OIDC provider (Keycloak) and maps them to a Principal.
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Realm roles used for authorization.
const (
	// RoleProvider allows submitting and reading the caller's own operations.
	RoleProvider = "wagering-provider"
	// RoleWalletOperator is held by internal services: wallet operations and
	// reading any transaction.
	RoleWalletOperator = "wallet-operator"
)

var (
	// ErrUnauthenticated: missing, malformed, invalid or expired credentials.
	ErrUnauthenticated = errors.New("unauthenticated")
	// ErrForbidden: authenticated but not allowed.
	ErrForbidden = errors.New("forbidden")
)

// Principal is the authenticated caller.
type Principal struct {
	Subject    string
	ClientID   string
	ProviderID string
	Roles      map[string]bool
}

// HasRole reports whether the principal holds role.
func (p Principal) HasRole(role string) bool { return p.Roles[role] }

// IsInternal reports whether the caller is an internal service.
func (p Principal) IsInternal() bool { return p.HasRole(RoleWalletOperator) }

// IsProvider reports whether the caller is a game provider bound to a
// provider id. The provider id comes only from the token.
func (p Principal) IsProvider() bool { return p.HasRole(RoleProvider) && p.ProviderID != "" }

// CanAccessProvider reports whether the caller may read operations of providerID.
func (p Principal) CanAccessProvider(providerID string) bool {
	return p.IsInternal() || (p.IsProvider() && p.ProviderID == providerID)
}

// Verifier validates a raw bearer token.
type Verifier interface {
	Verify(ctx context.Context, rawToken string) (Principal, error)
}

// OIDCConfig configures token validation.
type OIDCConfig struct {
	// Issuer is the expected "iss" claim.
	Issuer string
	// JWKSURL is where signing keys are fetched (may use an internal host
	// name different from the public issuer URL).
	JWKSURL string
	// Audience must be present in "aud".
	Audience string
}

// OIDCVerifier validates RS256 JWT access tokens: signature against the
// IdP's JWKS (cached, refreshed on unknown key id), issuer, audience,
// expiry and token type.
type OIDCVerifier struct {
	cfg      OIDCConfig
	verifier *oidc.IDTokenVerifier
	client   *http.Client
}

// NewOIDCVerifier builds a verifier. Keys are fetched lazily.
func NewOIDCVerifier(cfg OIDCConfig) *OIDCVerifier {
	client := &http.Client{Timeout: 5 * time.Second}
	ctx := oidc.ClientContext(context.Background(), client)
	keys := oidc.NewRemoteKeySet(ctx, cfg.JWKSURL)
	v := oidc.NewVerifier(cfg.Issuer, keys, &oidc.Config{
		ClientID:             cfg.Audience,
		SupportedSigningAlgs: []string{oidc.RS256},
	})
	return &OIDCVerifier{cfg: cfg, verifier: v, client: client}
}

type claims struct {
	Type        string `json:"typ"`
	AZP         string `json:"azp"`
	ClientID    string `json:"client_id"`
	ProviderID  string `json:"provider_id"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Verify validates the token and extracts the principal.
func (v *OIDCVerifier) Verify(ctx context.Context, raw string) (Principal, error) {
	if raw == "" {
		return Principal{}, fmt.Errorf("%w: missing bearer token", ErrUnauthenticated)
	}
	tok, err := v.verifier.Verify(ctx, raw)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	var c claims
	if err := tok.Claims(&c); err != nil {
		return Principal{}, fmt.Errorf("%w: claims: %v", ErrUnauthenticated, err)
	}
	// Keycloak access tokens carry typ=Bearer; ID tokens are not accepted.
	if !strings.EqualFold(c.Type, "Bearer") {
		return Principal{}, fmt.Errorf("%w: not an access token", ErrUnauthenticated)
	}
	p := Principal{Subject: tok.Subject, ClientID: c.AZP, ProviderID: c.ProviderID, Roles: map[string]bool{}}
	if p.ClientID == "" {
		p.ClientID = c.ClientID
	}
	for _, r := range c.RealmAccess.Roles {
		p.Roles[r] = true
	}
	return p, nil
}

// CheckReachable fetches the JWKS once to validate the IdP dependency at startup.
func (v *OIDCVerifier) CheckReachable(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.cfg.JWKSURL, nil)
	if err != nil {
		return err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks endpoint returned %d", resp.StatusCode)
	}
	return nil
}

// BearerToken extracts the token from an Authorization header.
func BearerToken(h string) (string, bool) {
	const prefix = "bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", false
	}
	t := strings.TrimSpace(h[len(prefix):])
	return t, t != ""
}

type principalKey struct{}

// WithPrincipal stores the principal in the context.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext returns the authenticated principal.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
