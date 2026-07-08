package app

import (
	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentosplatform "github.com/TekkenSteve/GoAgent/agentos/platform"
	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
)

type agentOSControlRuntime struct {
	agentos.Runtime
}

type agentOSProcessRuntime struct {
	agentosproc.Runtime
}

type agentOSPlatformRuntime struct {
	agentOSControlRuntime
	agentos.PlanRuntime
	agentOSProcessRuntime
	agentosproc.LedgerRuntime
	agentosproc.GovernedActionRuntime
	agentosproc.BatchRuntime
	agentosproc.ProjectionRuntime
}

func newAgentOSPlatformRuntime(
	control agentos.Runtime,
	plans agentos.PlanRuntime,
	processes agentosproc.Runtime,
	ledger agentosproc.LedgerRuntime,
	actions agentosproc.GovernedActionRuntime,
	worksets agentosproc.BatchRuntime,
	projections agentosproc.ProjectionRuntime,
) agentosplatform.Runtime {
	return &agentOSPlatformRuntime{
		agentOSControlRuntime: agentOSControlRuntime{Runtime: control},
		PlanRuntime:           plans,
		agentOSProcessRuntime: agentOSProcessRuntime{Runtime: processes},
		LedgerRuntime:         ledger,
		GovernedActionRuntime: actions,
		BatchRuntime:          worksets,
		ProjectionRuntime:     projections,
	}
}

func (r *agentOSProcessPlatformRuntimes) platformRuntime(control agentos.Runtime, plans agentos.PlanRuntime) agentosplatform.Runtime {
	return newAgentOSPlatformRuntime(
		control,
		plans,
		r.processRuntime,
		r.ledgerRuntime,
		r.actionRuntime,
		r.worksetRuntime,
		r.projectionRuntime,
	)
}

var _ agentosplatform.Runtime = (*agentOSPlatformRuntime)(nil)
