// Package middleware provides HTTP middleware for the REST API.
package middleware

import (
	"errors"
	"strings"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/authn"
	"github.com/gofiber/fiber/v2"
)

// Credential-shape failures. They are distinct so tests (and logs) can tell a
// missing credential from a malformed one; both are answered with 401.
var (
	errMissingAuthorization   = errors.New("missing authorization token")
	errMalformedAuthorization = errors.New("invalid authorization format")
)

const (
	bearerPartsCount = 2
	// principalLocalsKey is the Fiber locals key the authenticated principal
	// is stored under for the lifetime of the request.
	principalLocalsKey = "agentos.principal"
)

// RequireAuth authenticates every request except the configured public paths
// and stores the resulting principal for downstream handlers.
//
// Public paths are listed explicitly rather than inferred: a path is
// reachable without a credential only when a deployment says so. An entry
// ending in "/*" matches by prefix (documentation trees); every other entry
// must match exactly, so widening the surface takes an explicit edit.
//
// Requests are never authorized here: this middleware does not know which
// object a route acts on. Handlers resolve the object and consult the
// authorization port with it.
func RequireAuth(verifier *authn.Verifier, publicPaths []string) func(*fiber.Ctx) error {
	isPublic := newPublicPathMatcher(publicPaths)

	return func(ctx *fiber.Ctx) error {
		if isPublic.matches(ctx.Path()) {
			return ctx.Next()
		}

		token, err := bearerToken(ctx.Get(fiber.HeaderAuthorization))
		if err != nil {
			return unauthorized(ctx, err.Error())
		}

		principal, err := verifier.Verify(ctx.UserContext(), token)
		if err != nil {
			// The reason is deliberately not echoed: which part of a token
			// failed is information an unauthenticated caller should not get.
			return unauthorized(ctx, "invalid or expired token")
		}

		ctx.Locals(principalLocalsKey, principal)

		return ctx.Next()
	}
}

// PrincipalFromCtx returns the principal RequireAuth stored for this request.
//
// The bool is the point: a handler must not be able to fall back to a default
// tenant when authentication is missing, so callers are forced to handle the
// unauthenticated case explicitly.
func PrincipalFromCtx(ctx *fiber.Ctx) (agentoscore.Principal, bool) {
	principal, ok := ctx.Locals(principalLocalsKey).(agentoscore.Principal)
	if !ok || principal.IsZero() {
		return agentoscore.Principal{}, false
	}

	return principal, true
}

// StashPrincipal records an already-verified identity on the request.
//
// RequireAuth is the normal source. This exists for the two cases that
// legitimately establish identity elsewhere: a deployment that terminates
// authentication in its own transport (and therefore does not use RequireAuth),
// and tests that exercise handlers without minting tokens. It validates the
// principal, so it cannot be used to stash an anonymous or malformed identity.
func StashPrincipal(ctx *fiber.Ctx, principal agentoscore.Principal) error {
	if err := principal.Validate(); err != nil {
		return err
	}

	ctx.Locals(principalLocalsKey, principal)

	return nil
}

// bearerToken extracts the credential from an Authorization header.
func bearerToken(header string) (string, error) {
	header = strings.TrimSpace(header)
	if header == "" {
		return "", errMissingAuthorization
	}

	parts := strings.SplitN(header, " ", bearerPartsCount)
	if len(parts) != bearerPartsCount || !strings.EqualFold(parts[0], "bearer") {
		return "", errMalformedAuthorization
	}

	token := strings.TrimSpace(parts[1])
	if token == "" {
		return "", errMissingAuthorization
	}

	return token, nil
}

func unauthorized(ctx *fiber.Ctx, message string) error {
	return ctx.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": message})
}

// publicPathMatcher decides which paths skip authentication. Entries are exact
// matches except those ending in "*", which match by prefix.
type publicPathMatcher struct {
	exact    map[string]struct{}
	prefixes []string
}

func newPublicPathMatcher(publicPaths []string) *publicPathMatcher {
	matcher := &publicPathMatcher{exact: make(map[string]struct{}, len(publicPaths))}

	for _, path := range publicPaths {
		trimmed := strings.ToLower(strings.TrimSpace(path))
		if trimmed == "" {
			continue
		}

		prefix, isPrefix := strings.CutSuffix(trimmed, "*")
		if isPrefix {
			matcher.prefixes = append(matcher.prefixes, prefix)

			continue
		}

		matcher.exact[trimmed] = struct{}{}
	}

	return matcher
}

func (m *publicPathMatcher) matches(path string) bool {
	path = strings.ToLower(path)

	if _, ok := m.exact[path]; ok {
		return true
	}

	for _, prefix := range m.prefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}
