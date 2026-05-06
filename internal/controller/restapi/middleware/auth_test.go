package middleware_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/middleware"
	"github.com/TekkenSteve/GoAgent/pkg/jwt"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newJWTManager(t *testing.T) *jwt.Manager {
	t.Helper()

	return jwt.New("test-secret", time.Hour)
}

func TestAuth_SkipRegister(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	jwtMgr := newJWTManager(t)
	app.Use(middleware.Auth(jwtMgr))
	app.Get("/v1/auth/register", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	_, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/v1/auth/register", nil))
	require.NoError(t, err)
}

func TestAuth_SkipLogin(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	jwtMgr := newJWTManager(t)
	app.Use(middleware.Auth(jwtMgr))
	app.Get("/v1/auth/login", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	_, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/v1/auth/login", nil))
	require.NoError(t, err)
}

func TestAuth_MissingAuthHeader(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	jwtMgr := newJWTManager(t)
	app.Use(middleware.Auth(jwtMgr))
	app.Get("/v1/agent/execute", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	resp, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/v1/agent/execute", nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
}

func TestAuth_InvalidToken(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	jwtMgr := newJWTManager(t)
	app.Use(middleware.Auth(jwtMgr))
	app.Get("/v1/agent/execute", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(fiber.MethodGet, "/v1/agent/execute", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
}

func TestAuth_ValidToken(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	jwtMgr := newJWTManager(t)
	app.Use(middleware.Auth(jwtMgr))
	app.Get("/v1/agent/execute", func(c *fiber.Ctx) error {
		userID := c.Locals("userID")
		return c.JSON(fiber.Map{"user_id": userID})
	})

	token, err := jwtMgr.GenerateToken("user-123")
	require.NoError(t, err)

	req := httptest.NewRequest(fiber.MethodGet, "/v1/agent/execute", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
}
