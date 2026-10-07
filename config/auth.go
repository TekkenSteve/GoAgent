package config

import (
	"time"

	"github.com/TekkenSteve/GoAgent/internal/authn"
)

// VerifierConfig maps the Auth settings onto the authenticator's own config.
//
// The mapping lives here so the deployment-facing rules (which key source,
// which claims, how much clock skew) have exactly one definition: authn.Config
// validates them, and the loader refuses to start when they do not hold.
func (a *Auth) VerifierConfig() authn.Config {
	return authn.Config{
		JWKSURL:      a.JWKSURL,
		HMACSecret:   a.HMACSecret,
		Issuer:       a.Issuer,
		Audience:     a.Audience,
		Leeway:       time.Duration(a.LeewaySeconds) * time.Second,
		AccountClaim: a.AccountClaim,
		ActorClaim:   a.ActorClaim,
	}
}

// Validate reports whether the authentication settings can authenticate
// anyone. It delegates to authn.Config so the rules cannot drift from the
// component that enforces them at runtime.
func (a *Auth) Validate() error {
	verifierConfig := a.VerifierConfig()

	return verifierConfig.Validate()
}

// ResolvedPublicPaths returns the configured public paths, defaulting to the
// liveness probe alone.
//
// The default is deliberately minimal: authentication is the posture, and a
// path is reachable without a credential only when a deployment says so.
// Metrics and API documentation are operator decisions (they carry topology and
// contract information), so enabling them means adding "/metrics" or
// "/swagger/*" explicitly. An entry ending in "/*" matches by prefix.
func (a *Auth) ResolvedPublicPaths() []string {
	if len(a.PublicPaths) > 0 {
		return a.PublicPaths
	}

	return []string{"/healthz"}
}
