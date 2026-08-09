// Package platform implements the AgentOS platform runtime used by the app entrypoint.
package platform

import (
	"github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/agentos/process"
)

// Runtime is the AgentOS platform facade for applications that need both the
// agent control plane and the durable process platform.
type Runtime interface {
	control.Runtime
	control.PlanRuntime
	process.Runtime
	process.LedgerRuntime
	process.GovernedActionRuntime
	process.BatchRuntime
	process.ProjectionRuntime
}
