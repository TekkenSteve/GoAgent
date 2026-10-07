// Package authn verifies request credentials and turns them into an
// authenticated core.Principal.
//
// Verification itself is delegated: golang-jwt verifies signatures and
// registered claims, MicahParks/keyfunc maintains the JWK Set (fetch, cache,
// refresh, rotation). This package owns only the deployment-facing decisions —
// which key source is trusted, which claims carry the account and the actor,
// and how a verified token becomes a Principal. It contains no cryptography.
//
// Two key sources are supported, and they map onto the two ways a deployment
// terminates authentication:
//
//   - JWKSURL: the token was minted and signed by an identity provider or by a
//     gateway that terminates authentication in front of this service (Ory
//     Oathkeeper and Hydra are the reference deployment). Public keys are
//     discovered from the provider's JWK Set.
//   - HMACSecret: a shared secret, for local development and integration
//     tests. Not a production posture — it cannot be rotated per environment
//     and every holder can mint identities.
package authn

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	jwtlib "github.com/golang-jwt/jwt/v5"
)

// Default claim names. The account defaults to the standard subject claim,
// which is what an identity provider puts the authenticated principal in; the
// actor defaults to RFC 8693's acting-party claim, which is present only when
// one principal acts on behalf of another.
const (
	DefaultAccountClaim = "sub"
	DefaultActorClaim   = "act"
)

// ErrInvalidConfig reports a verifier configuration that cannot authenticate
// anyone. It is returned at construction so a misconfigured deployment fails to
// start instead of accepting anonymous requests.
var ErrInvalidConfig = errors.New("authn: invalid config")

// Config declares how tokens are trusted.
type Config struct {
	// JWKSURL is the issuer's JWK Set endpoint. Mutually exclusive with
	// HMACSecret.
	JWKSURL string
	// HMACSecret is the shared secret for HS256 tokens. Mutually exclusive
	// with JWKSURL.
	HMACSecret string
	// Issuer, when set, must match the token's iss claim.
	Issuer string
	// Audience, when set, must be present in the token's aud claim.
	Audience string
	// Leeway absorbs small clock differences when validating exp/nbf.
	Leeway time.Duration
	// AccountClaim names the claim holding the tenant boundary. Defaults to
	// DefaultAccountClaim.
	AccountClaim string
	// ActorClaim names the claim identifying the acting party. Defaults to
	// DefaultActorClaim. When absent, the account acts for itself.
	ActorClaim string
}

// Validate reports whether the configuration can authenticate anyone.
//
// Exactly one key source must be configured, and the token must be bound to
// this service by at least an issuer or an audience: a verifier that accepts
// any correctly signed token from the key source is a confused deputy.
func (c *Config) Validate() error {
	hasJWKS := strings.TrimSpace(c.JWKSURL) != ""
	hasSecret := strings.TrimSpace(c.HMACSecret) != ""

	switch {
	case hasJWKS && hasSecret:
		return fmt.Errorf("%w: configure either jwks_url or hmac_secret, not both", ErrInvalidConfig)
	case !hasJWKS && !hasSecret:
		return fmt.Errorf("%w: one of jwks_url or hmac_secret is required", ErrInvalidConfig)
	}

	if hasSecret && len(c.HMACSecret) < minHMACSecretLength {
		return fmt.Errorf("%w: hmac_secret must be at least %d bytes", ErrInvalidConfig, minHMACSecretLength)
	}

	if strings.TrimSpace(c.Issuer) == "" && strings.TrimSpace(c.Audience) == "" {
		return fmt.Errorf("%w: set issuer or audience so tokens are bound to this service", ErrInvalidConfig)
	}

	return nil
}

// minHMACSecretLength is the shortest shared secret accepted. HS256 keys
// shorter than the hash output weaken the signature to brute force.
const minHMACSecretLength = 32

func (c *Config) withDefaults() {
	if strings.TrimSpace(c.AccountClaim) == "" {
		c.AccountClaim = DefaultAccountClaim
	}

	if strings.TrimSpace(c.ActorClaim) == "" {
		c.ActorClaim = DefaultActorClaim
	}
}

func (c *Config) validMethods() []string {
	if strings.TrimSpace(c.HMACSecret) != "" {
		return []string{"HS256"}
	}

	return []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512", "EdDSA"}
}

// Verifier turns a bearer token into an authenticated principal.
type Verifier struct {
	config   Config
	keyfunc  keyfunc.Keyfunc
	parseOps []jwtlib.ParserOption
}

// New builds a verifier from the configuration, failing when it could not
// authenticate anyone. The context governs the JWK Set refresh goroutine.
func New(ctx context.Context, config *Config) (*Verifier, error) {
	if config == nil {
		return nil, fmt.Errorf("%w: config is required", ErrInvalidConfig)
	}

	config.withDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	verifier := &Verifier{config: *config}

	if strings.TrimSpace(config.JWKSURL) != "" {
		keySet, err := keyfunc.NewDefaultCtx(ctx, []string{strings.TrimSpace(config.JWKSURL)})
		if err != nil {
			return nil, fmt.Errorf("%w: load jwk set: %w", ErrInvalidConfig, err)
		}

		verifier.keyfunc = keySet
	}

	parseOps := []jwtlib.ParserOption{
		jwtlib.WithValidMethods(config.validMethods()),
		// A token without an expiry is a permanent credential.
		jwtlib.WithExpirationRequired(),
	}

	if issuer := strings.TrimSpace(config.Issuer); issuer != "" {
		parseOps = append(parseOps, jwtlib.WithIssuer(issuer))
	}

	if audience := strings.TrimSpace(config.Audience); audience != "" {
		parseOps = append(parseOps, jwtlib.WithAudience(audience))
	}

	if config.Leeway > 0 {
		parseOps = append(parseOps, jwtlib.WithLeeway(config.Leeway))
	}

	verifier.parseOps = parseOps

	return verifier, nil
}

// Verify validates the token and returns the principal it asserts. Every
// failure wraps agentoscore.ErrUnauthenticated so transports map them to one
// status without inspecting error text.
func (v *Verifier) Verify(ctx context.Context, token string) (agentoscore.Principal, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return agentoscore.Principal{}, fmt.Errorf("%w: empty token", agentoscore.ErrUnauthenticated)
	}

	claims := jwtlib.MapClaims{}

	parsed, err := jwtlib.ParseWithClaims(token, claims, v.keyfuncFor(ctx), v.parseOps...)
	if err != nil {
		return agentoscore.Principal{}, fmt.Errorf("%w: %w", agentoscore.ErrUnauthenticated, err)
	}

	if !parsed.Valid {
		return agentoscore.Principal{}, fmt.Errorf("%w: token is not valid", agentoscore.ErrUnauthenticated)
	}

	principal, err := agentoscore.NewPrincipal(
		stringClaim(claims, v.config.AccountClaim),
		stringClaim(claims, v.config.ActorClaim),
		agentoscore.PrincipalSourceJWT,
	)
	if err != nil {
		// A signed token that carries no usable tenant is still unusable: the
		// failure is authentication, not authorization.
		return agentoscore.Principal{}, fmt.Errorf("%w: %w", agentoscore.ErrUnauthenticated, err)
	}

	return principal, nil
}

// keyfuncFor returns the key lookup for the configured source. The HMAC branch
// is a constant-time comparison done by the JWT library.
func (v *Verifier) keyfuncFor(ctx context.Context) jwtlib.Keyfunc {
	if v.keyfunc != nil {
		return v.keyfunc.KeyfuncCtx(ctx)
	}

	secret := []byte(v.config.HMACSecret)

	return func(*jwtlib.Token) (any, error) { return secret, nil }
}

// stringClaim reads one claim as a trimmed string. Numeric or structured
// claims are rejected rather than coerced: an account id that is not a string
// is a malformed token, not a tenant.
func stringClaim(claims jwtlib.MapClaims, name string) string {
	value, ok := claims[name]
	if !ok {
		return ""
	}

	text, ok := value.(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(text)
}
