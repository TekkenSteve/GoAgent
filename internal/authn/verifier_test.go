package authn

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
)

const (
	testSecret   = "test-secret-that-is-at-least-32-bytes-long"
	testIssuer   = "https://issuer.test"
	testAudience = "goagent"
)

func hmacVerifier(t *testing.T, config *Config) *Verifier {
	t.Helper()

	provided := *config

	// Defaults only fill gaps: a test that deliberately configures a wrong
	// issuer or audience must keep it.
	if provided.HMACSecret == "" {
		provided.HMACSecret = testSecret
	}

	if provided.Issuer == "" {
		provided.Issuer = testIssuer
	}

	if provided.Audience == "" {
		provided.Audience = testAudience
	}

	verifier, err := New(context.Background(), &provided)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	return verifier
}

func hmacToken(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()

	base := jwt.MapClaims{
		"iss": testIssuer,
		"aud": testAudience,
		"sub": "account-1",
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	for name, value := range claims {
		base[name] = value
	}

	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, base).SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	return signed
}

func TestConfigRejectsUnusableSetups(t *testing.T) {
	t.Parallel()

	cases := map[string]Config{
		"no key source":         {Issuer: testIssuer},
		"both key sources":      {JWKSURL: "https://issuer.test/jwks", HMACSecret: testSecret, Issuer: testIssuer},
		"short shared secret":   {HMACSecret: "too-short", Issuer: testIssuer},
		"no issuer or audience": {HMACSecret: testSecret},
	}
	for name, config := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if err := config.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestConfigAcceptsEitherKeySource(t *testing.T) {
	t.Parallel()

	valid := map[string]Config{
		"hmac with issuer":     {HMACSecret: testSecret, Issuer: testIssuer},
		"hmac with audience":   {HMACSecret: testSecret, Audience: testAudience},
		"jwks with audience":   {JWKSURL: "https://issuer.test/jwks", Audience: testAudience},
		"hmac with both":       {HMACSecret: testSecret, Issuer: testIssuer, Audience: testAudience},
		"hmac exactly 32 byte": {HMACSecret: "0123456789abcdef0123456789abcdef", Issuer: testIssuer},
	}
	for name, config := range valid {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if err := config.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

func TestNewFailsWhenItCannotAuthenticateAnyone(t *testing.T) {
	t.Parallel()

	unusable := Config{Issuer: testIssuer}

	if _, err := New(context.Background(), &unusable); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New error = %v, want ErrInvalidConfig", err)
	}
}

func TestVerifyAcceptsATokenFromTheConfiguredIssuer(t *testing.T) {
	t.Parallel()

	principal, err := hmacVerifier(t, &Config{}).Verify(context.Background(), hmacToken(t, nil))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if principal.AccountID != "account-1" {
		t.Fatalf("account = %q, want the subject claim", principal.AccountID)
	}

	if principal.Source != agentoscore.PrincipalSourceJWT {
		t.Fatalf("source = %q, want jwt", principal.Source)
	}
}

func TestVerifyReadsTheConfiguredAccountClaim(t *testing.T) {
	t.Parallel()

	verifier := hmacVerifier(t, &Config{AccountClaim: "tenant_id"})

	principal, err := verifier.Verify(context.Background(), hmacToken(t, jwt.MapClaims{"tenant_id": "tenant-9"}))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if principal.AccountID != "tenant-9" {
		t.Fatalf("account = %q, want the configured claim", principal.AccountID)
	}
}

func TestVerifyReadsTheActingPartyClaim(t *testing.T) {
	t.Parallel()

	principal, err := hmacVerifier(t, &Config{}).Verify(context.Background(), hmacToken(t, jwt.MapClaims{"act": "service-7"}))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if principal.ActorID != "service-7" {
		t.Fatalf("actor = %q, want the act claim", principal.ActorID)
	}

	if got := principal.EffectiveActor(); got != "service-7" {
		t.Fatalf("EffectiveActor() = %q", got)
	}
}

func TestVerifyRejectsUnusableTokens(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		token  string
		config Config
	}{
		"empty":              {token: ""},
		"not a jwt":          {token: "not-a-token"},
		"wrong audience":     {token: "", config: Config{Audience: "another-service"}},
		"wrong issuer":       {token: "", config: Config{Issuer: "https://elsewhere.test"}},
		"missing expiry":     {token: ""},
		"account not string": {token: ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			verifier := hmacVerifier(t, &tc.config)

			token := tc.token

			switch name {
			case "missing expiry":
				token = signHMAC(t, jwt.MapClaims{"iss": testIssuer, "aud": testAudience, "sub": "account-1"})
			case "account not string":
				token = signHMAC(t, jwt.MapClaims{
					"iss": testIssuer, "aud": testAudience, "sub": 42,
					"exp": time.Now().Add(time.Hour).Unix(),
				})
			case "wrong audience", "wrong issuer":
				token = hmacToken(t, nil)
			}

			if _, err := verifier.Verify(context.Background(), token); !errors.Is(err, agentoscore.ErrUnauthenticated) {
				t.Fatalf("Verify error = %v, want ErrUnauthenticated", err)
			}
		})
	}
}

func signHMAC(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()

	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	return signed
}

// jwksTestSetup serves a JWK Set and returns a verifier bound to it plus the
// private key that owns the published key.
func jwksTestSetup(t *testing.T) (*Verifier, *rsa.PrivateKey) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
		Key:       key.Public(),
		KeyID:     "key-1",
		Algorithm: string(jose.RS256),
		Use:       "sig",
	}}}

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(writer).Encode(jwks); err != nil {
			t.Errorf("encode jwk set: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	jwksConfig := Config{
		JWKSURL:  server.URL,
		Issuer:   testIssuer,
		Audience: testAudience,
	}

	verifier, err := New(context.Background(), &jwksConfig)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	return verifier, key
}

// TestVerifyUsesThePublishersJWKSet covers the production path: keys come from
// the issuer's JWK Set, maintained by keyfunc, and never from a shared secret.
func TestVerifyUsesThePublishersJWKSet(t *testing.T) {
	t.Parallel()

	verifier, key := jwksTestSetup(t)

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": testIssuer,
		"aud": testAudience,
		"sub": "account-1",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	token.Header["kid"] = "key-1"

	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	principal, err := verifier.Verify(context.Background(), signed)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if principal.AccountID != "account-1" {
		t.Fatalf("account = %q", principal.AccountID)
	}
}

// TestVerifyRejectsATokenSignedByAnotherKey pins that discovery does not widen
// trust: holding a key id from the set is not enough.
func TestVerifyRejectsATokenSignedByAnotherKey(t *testing.T) {
	t.Parallel()

	verifier, _ := jwksTestSetup(t)

	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate second key: %v", err)
	}

	forged := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": testIssuer, "aud": testAudience, "sub": "account-1",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	forged.Header["kid"] = "key-1"

	forgedSigned, err := forged.SignedString(otherKey)
	if err != nil {
		t.Fatalf("sign forged: %v", err)
	}

	if _, err := verifier.Verify(context.Background(), forgedSigned); !errors.Is(err, agentoscore.ErrUnauthenticated) {
		t.Fatalf("forged token error = %v, want ErrUnauthenticated", err)
	}
}
