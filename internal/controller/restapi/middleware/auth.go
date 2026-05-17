package middleware

import (
	"strings"

	"github.com/TekkenSteve/GoAgent/pkg/jwt"
	"github.com/gofiber/fiber/v2"
)

const bearerPartsCount = 2

// Auth returns a Fiber middleware that validates JWT tokens.
func Auth(jwtManager *jwt.Manager) func(*fiber.Ctx) error {
	skipAuthPaths := map[string]bool{
		"/v1/auth/register": true,
		"/v1/auth/login":    true,
	}

	return func(ctx *fiber.Ctx) error {
		path := strings.ToLower(ctx.Path())
		if skipAuthPaths[path] {
			return ctx.Next()
		}

		authHeader := ctx.Get("Authorization")
		if authHeader == "" {
			return ctx.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "missing authorization token",
			})
		}

		// Extract token from "Bearer <token>"
		parts := strings.SplitN(authHeader, " ", bearerPartsCount)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
			return ctx.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "invalid authorization format",
			})
		}

		userID, err := jwtManager.ParseToken(parts[1])
		if err != nil {
			return ctx.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "invalid or expired token",
			})
		}

		ctx.Locals("userID", userID)

		return ctx.Next()
	}
}
