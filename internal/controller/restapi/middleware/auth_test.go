package middleware_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/authn"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/middleware"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testSecret signs the tokens this suite mints. It is a fixture, not a
// deployment credential.
const testSecret = "test-secret-that-is-at-least-32-bytes-long"

const (
	testIssuer    = "https://issuer.test"
	testAudience  = "goagent"
	protectedPath = "/v1/agentos/runs"
)

func newVerifier(t *testing.T) *authn.Verifier {
	t.Helper()

	config := authn.Config{
		HMACSecret: testSecret,
		Issuer:     testIssuer,
		Audience:   testAudience,
	}

	verifier, err := authn.New(context.Background(), &config)
	require.NoError(t, err)

	return verifier
}

// tokenFor mints a token the verifier accepts, with claim overrides applied.
func tokenFor(t *testing.T, claims jwt.MapClaims) string {
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
	require.NoError(t, err)

	return signed
}

// request performs one request and returns its status and body. The body is
// read here, so no response outlives the call.
func request(t *testing.T, app *fiber.App, path, authorization string) (status int, body string) {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), fiber.MethodGet, path, http.NoBody)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}

	resp, err := app.Test(req)
	require.NoError(t, err)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return resp.StatusCode, string(raw)
}

// echoPrincipal mounts a route that answers with the principal it was given, so
// tests can assert what the middleware established.
func echoPrincipal(app *fiber.App, verifier *authn.Verifier, publicPaths []string) {
	app.Use(middleware.RequireAuth(verifier, publicPaths))
	app.Get(protectedPath, func(ctx *fiber.Ctx) error {
		principal, ok := middleware.PrincipalFromCtx(ctx)
		if !ok {
			return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "no principal"})
		}

		return ctx.JSON(fiber.Map{
			"account_id": principal.AccountID,
			"actor_id":   principal.EffectiveActor(),
			"source":     string(principal.Source),
		})
	})
}

func TestRequireAuthRejectsMissingCredential(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	echoPrincipal(app, newVerifier(t), nil)

	status, _ := request(t, app, protectedPath, "")

	assert.Equal(t, fiber.StatusUnauthorized, status)
}

func TestRequireAuthRejectsMalformedAuthorization(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"not bearer":  "Token abc",
		"no scheme":   "abc",
		"empty token": "Bearer ",
	}
	for name, header := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			app := fiber.New()
			echoPrincipal(app, newVerifier(t), nil)

			status, _ := request(t, app, protectedPath, header)

			assert.Equal(t, fiber.StatusUnauthorized, status)
		})
	}
}

func TestRequireAuthRejectsTokensItCannotTrust(t *testing.T) {
	t.Parallel()

	cases := map[string]jwt.MapClaims{
		"wrong issuer":       {"iss": "https://elsewhere.test"},
		"wrong audience":     {"aud": "another-service"},
		"expired":            {"exp": time.Now().Add(-time.Minute).Unix()},
		"no expiry":          {"exp": nil},
		"missing account":    {"sub": ""},
		"non string account": {"sub": 42},
	}
	for name, claims := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			app := fiber.New()
			echoPrincipal(app, newVerifier(t), nil)

			status, _ := request(t, app, protectedPath, "Bearer "+tokenFor(t, claims))

			assert.Equal(t, fiber.StatusUnauthorized, status)
		})
	}
}

// TestRequireAuthRejectsAForeignSignature pins that a well-formed token is not
// enough: the signature must verify against the configured key.
func TestRequireAuthRejectsAForeignSignature(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	echoPrincipal(app, newVerifier(t), nil)

	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": testIssuer, "aud": testAudience, "sub": "account-1",
		"exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte("a-different-secret-that-is-long-enough"))
	require.NoError(t, err)

	status, _ := request(t, app, protectedPath, "Bearer "+signed)

	assert.Equal(t, fiber.StatusUnauthorized, status)
}

func TestRequireAuthEstablishesThePrincipal(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	echoPrincipal(app, newVerifier(t), nil)

	status, body := request(t, app, protectedPath, "Bearer "+tokenFor(t, nil))
	require.Equal(t, fiber.StatusOK, status)

	assert.Contains(t, body, `"account_id":"account-1"`)
	assert.Contains(t, body, string(agentoscore.PrincipalSourceJWT))
}

func TestRequireAuthCarriesTheActingParty(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	echoPrincipal(app, newVerifier(t), nil)

	status, body := request(t, app, protectedPath, "Bearer "+tokenFor(t, jwt.MapClaims{"act": "service-7"}))
	require.Equal(t, fiber.StatusOK, status)

	assert.Contains(t, body, `"account_id":"account-1"`)
	assert.Contains(t, body, `"actor_id":"service-7"`)
}

func TestRequireAuthHonoursPublicPaths(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		public []string
		path   string
		status int
	}{
		"exact match":            {public: []string{"/healthz"}, path: "/healthz", status: fiber.StatusOK},
		"prefix match":           {public: []string{"/swagger/*"}, path: "/swagger/index.html", status: fiber.StatusOK},
		"unlisted path":          {public: []string{"/healthz"}, path: "/metrics", status: fiber.StatusUnauthorized},
		"prefix does not spread": {public: []string{"/health/*"}, path: "/healthz", status: fiber.StatusUnauthorized},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			app := fiber.New()
			echoPrincipal(app, newVerifier(t), tc.public)
			app.Get("/healthz", func(ctx *fiber.Ctx) error { return ctx.SendStatus(fiber.StatusOK) })
			app.Get("/metrics", func(ctx *fiber.Ctx) error { return ctx.SendStatus(fiber.StatusOK) })
			app.Get("/swagger/index.html", func(ctx *fiber.Ctx) error { return ctx.SendStatus(fiber.StatusOK) })

			status, _ := request(t, app, tc.path, "")

			assert.Equal(t, tc.status, status)
		})
	}
}

func TestPrincipalFromCtxReportsAbsence(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get(protectedPath, func(ctx *fiber.Ctx) error {
		if _, ok := middleware.PrincipalFromCtx(ctx); ok {
			return ctx.SendStatus(fiber.StatusInternalServerError)
		}

		return ctx.SendStatus(fiber.StatusNoContent)
	})

	status, _ := request(t, app, protectedPath, "")

	assert.Equal(t, fiber.StatusNoContent, status)
}

func TestStashPrincipalRefusesAnInvalidIdentity(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get(protectedPath, func(ctx *fiber.Ctx) error {
		if err := middleware.StashPrincipal(ctx, agentoscore.Principal{}); err == nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "accepted an empty principal"})
		}

		return ctx.SendStatus(fiber.StatusNoContent)
	})

	status, _ := request(t, app, protectedPath, "")

	assert.Equal(t, fiber.StatusNoContent, status)
}

func TestRequireAuthHidesWhyATokenFailed(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	echoPrincipal(app, newVerifier(t), nil)

	status, body := request(t, app, protectedPath, "Bearer "+tokenFor(t, jwt.MapClaims{"iss": "https://elsewhere.test"}))
	require.Equal(t, fiber.StatusUnauthorized, status)

	// The response must not tell an unauthenticated caller which check failed.
	assert.NotContains(t, strings.ToLower(body), "issuer")
	assert.NotContains(t, strings.ToLower(body), "audience")
}
