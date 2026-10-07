package restapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/config"
	"github.com/TekkenSteve/GoAgent/internal/authn"
	"github.com/TekkenSteve/GoAgent/internal/authz"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// routerTestSigningKey is a fixture this suite signs with; no deployment
	// uses it.
	routerTestSigningKey = "router-test-secret-that-is-at-least-32-bytes"
	routerTestIssuer     = "https://issuer.test"
	routerTestAudience   = "goagent"
	routerTestPath       = "/v1/agentos/runs"
)

func newTestAuthDeps(t *testing.T, publicPaths []string) AuthDeps {
	t.Helper()

	authConfig := authn.Config{
		HMACSecret: routerTestSigningKey,
		Issuer:     routerTestIssuer,
		Audience:   routerTestAudience,
	}

	verifier, err := authn.New(context.Background(), &authConfig)
	require.NoError(t, err)

	return AuthDeps{
		Verifier:    verifier,
		Authorizer:  authz.TenantAuthorizer{},
		PublicPaths: publicPaths,
	}
}

func newTestRouter(t *testing.T, auth AuthDeps) *fiber.App {
	t.Helper()

	app := fiber.New()
	cfg := &config.Config{}
	cfg.App.Name = "goagent-test"
	cfg.Metrics.Enabled = false
	cfg.Swagger.Enabled = false

	NewRouter(app, cfg, logger.New("error"), nil, nil, nil, nil, nil, nil, nil, nil, auth)

	return app
}

func routerTestToken(t *testing.T) string {
	t.Helper()

	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": routerTestIssuer,
		"aud": routerTestAudience,
		"sub": "account-1",
		"exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(routerTestSigningKey))
	require.NoError(t, err)

	return signed
}

func doRouterRequest(t *testing.T, app *fiber.App, path, authorization string) int {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), fiber.MethodGet, path, http.NoBody)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return resp.StatusCode
}

// TestNewRouterRequiresAuthenticationOnAPIRoutes is the wiring guard: the
// control plane shipped anonymous because NewRouter never installed the
// middleware it was given. A behavior test on the middleware cannot catch
// that, so this asserts through the assembled router.
func TestNewRouterRequiresAuthenticationOnAPIRoutes(t *testing.T) {
	t.Parallel()

	app := newTestRouter(t, newTestAuthDeps(t, nil))

	assert.Equal(t, fiber.StatusUnauthorized, doRouterRequest(t, app, routerTestPath, ""))
}

func TestNewRouterKeepsProbesPublic(t *testing.T) {
	t.Parallel()

	app := newTestRouter(t, newTestAuthDeps(t, []string{"/healthz"}))

	assert.Equal(t, fiber.StatusOK, doRouterRequest(t, app, "/healthz", ""))
	assert.Equal(t, fiber.StatusUnauthorized, doRouterRequest(t, app, routerTestPath, ""))
}

// TestNewRouterAcceptsAVerifiedToken proves the assembled router reaches the
// handlers it registers: the request is no longer rejected as unauthenticated.
func TestNewRouterAcceptsAVerifiedToken(t *testing.T) {
	t.Parallel()

	app := newTestRouter(t, newTestAuthDeps(t, nil))

	status := doRouterRequest(t, app, routerTestPath, "Bearer "+routerTestToken(t))

	assert.NotEqual(t, fiber.StatusUnauthorized, status)
}

func TestNewRouterRefusesToAssembleWithoutSecurityDependencies(t *testing.T) {
	t.Parallel()

	cases := map[string]AuthDeps{
		"no verifier":   {Authorizer: authz.TenantAuthorizer{}},
		"no authorizer": {Verifier: newTestAuthDeps(t, nil).Verifier},
	}
	for name, deps := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Panics(t, func() {
				newTestRouter(t, deps)
			})
		})
	}
}
