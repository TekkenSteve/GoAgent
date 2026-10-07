package v1

import (
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosplatform "github.com/TekkenSteve/GoAgent/agentos/platform"
	"github.com/TekkenSteve/GoAgent/internal/authz"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/middleware"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/gofiber/fiber/v2"
)

// testAccountID is the account every handler test is authenticated as.
const testAccountID = "acct-1"

// testPrincipal is the identity handler tests act as. Authentication itself is
// covered by the middleware and authn suites; handler tests stand in for it so
// they exercise behavior rather than token plumbing.
func testPrincipal(t *testing.T) agentoscore.Principal {
	t.Helper()

	principal, err := agentoscore.NewPrincipal(testAccountID, "", agentoscore.PrincipalSourceJWT)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}

	return principal
}

// withTestPrincipal installs the authenticated identity on every request,
// standing in for the RequireAuth middleware.
func withTestPrincipal(t *testing.T, app *fiber.App) {
	t.Helper()

	principal := testPrincipal(t)

	app.Use(func(ctx *fiber.Ctx) error {
		if err := middleware.StashPrincipal(ctx, principal); err != nil {
			t.Fatalf("StashPrincipal: %v", err)
		}

		return ctx.Next()
	})
}

// newTestRoutes registers the agentOS routes under a group as an authenticated
// caller, so a test names only the runtime it exercises. Authentication itself
// is covered by the middleware and router suites.
func newTestRoutes(
	t *testing.T,
	app *fiber.App,
	agentOSRuntime agentos.Runtime,
	planRuntime agentos.PlanRuntime,
	platformRuntime agentosplatform.Runtime,
	runEventReader RunEventReader,
) {
	t.Helper()

	withTestPrincipal(t, app)
	registerTestRoutes(app, agentOSRuntime, planRuntime, platformRuntime, runEventReader)
}

// newTestRoutesUnauthenticated registers the same routes with no identity, for
// the tests that assert a handler fails closed.
func newTestRoutesUnauthenticated(
	app *fiber.App,
	agentOSRuntime agentos.Runtime,
	planRuntime agentos.PlanRuntime,
	platformRuntime agentosplatform.Runtime,
	runEventReader RunEventReader,
) {
	registerTestRoutes(app, agentOSRuntime, planRuntime, platformRuntime, runEventReader)
}

func registerTestRoutes(
	app *fiber.App,
	agentOSRuntime agentos.Runtime,
	planRuntime agentos.PlanRuntime,
	platformRuntime agentosplatform.Runtime,
	runEventReader RunEventReader,
) {
	NewRoutes(app.Group("/v1"), logger.New("error"), nil, nil, nil, nil,
		agentOSRuntime, planRuntime, platformRuntime, runEventReader, authz.TenantAuthorizer{})
}
