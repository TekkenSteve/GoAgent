package config

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/authn"
)

func TestAuthRejectsAConfigurationThatCannotAuthenticate(t *testing.T) {
	t.Parallel()

	cases := map[string]Auth{
		"nothing configured": {AccountClaim: "sub"},
		"two key sources": {
			JWKSURL:    "https://issuer.test/jwks",
			HMACSecret: "0123456789abcdef0123456789abcdef",
			Issuer:     "https://issuer.test",
		},
		"unbound token": {HMACSecret: "0123456789abcdef0123456789abcdef"},
	}
	for name, auth := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if err := auth.Validate(); !errors.Is(err, authn.ErrInvalidConfig) {
				t.Fatalf("Validate error = %v, want authn.ErrInvalidConfig", err)
			}
		})
	}
}

func TestAuthAcceptsAUsableConfiguration(t *testing.T) {
	t.Parallel()

	auth := Auth{
		HMACSecret: "0123456789abcdef0123456789abcdef",
		Issuer:     "https://issuer.test",
	}

	if err := auth.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestAuthVerifierConfigCarriesTheDeploymentDecisions(t *testing.T) {
	t.Parallel()

	auth := Auth{
		JWKSURL:       "https://issuer.test/jwks",
		Issuer:        "https://issuer.test",
		Audience:      "goagent",
		LeewaySeconds: 30,
		AccountClaim:  "tenant_id",
		ActorClaim:    "act",
	}

	verifierConfig := auth.VerifierConfig()

	if verifierConfig.JWKSURL != auth.JWKSURL ||
		verifierConfig.Issuer != auth.Issuer ||
		verifierConfig.Audience != auth.Audience ||
		verifierConfig.AccountClaim != auth.AccountClaim ||
		verifierConfig.ActorClaim != auth.ActorClaim {
		t.Fatalf("VerifierConfig lost a setting: %#v", verifierConfig)
	}

	if verifierConfig.Leeway.Seconds() != 30 {
		t.Fatalf("leeway = %v, want 30s", verifierConfig.Leeway)
	}
}

// TestAuthPublicPathsAreMinimalByDefault pins the fail-closed default: only the
// liveness probe is reachable without a credential until a deployment says
// otherwise.
func TestAuthPublicPathsAreMinimalByDefault(t *testing.T) {
	t.Parallel()

	empty := Auth{}

	paths := empty.ResolvedPublicPaths()
	if len(paths) != 1 || paths[0] != "/healthz" {
		t.Fatalf("default public paths = %v, want [\"/healthz\"]", paths)
	}

	configured := Auth{PublicPaths: []string{"/healthz", "/metrics", "/swagger/*"}}

	if got := configured.ResolvedPublicPaths(); len(got) != 3 {
		t.Fatalf("configured public paths = %v, want the configured list", got)
	}
}
