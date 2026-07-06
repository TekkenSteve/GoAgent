package v1

import (
	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentosplatform "github.com/TekkenSteve/GoAgent/agentos/platform"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/eventing"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v2"
)

// NewRoutes registers all v1 API routes under the given router group.
// Matches go-clean-template pattern: single entry point, all usecase interfaces
// passed as parameters, all route groups registered inside.
func NewRoutes(apiV1Group fiber.Router, t usecase.AgentExecutor, o usecase.OrchestrationExecutor, l logger.Interface,
	cancelWorkflow CancelWorkflowFn, signalWorkflow SignalWorkflowFn,
	m usecase.TemplateManager, eh usecase.TriggerEventHandler,
	eventIngest *eventing.Service,
	agentOSRuntime agentos.Runtime,
	planRuntime agentos.PlanRuntime,
	platformRuntime agentosplatform.Runtime,
) {
	r := newV1(t, o, l, cancelWorkflow, signalWorkflow, eventIngest, agentOSRuntime, planRuntime, platformRuntime)
	registerTemplateRoutes(apiV1Group, m, l)
	registerTriggerRoutes(apiV1Group, eh, t, l)
	registerOrchestrationRoutes(apiV1Group, r, o)
	registerAgentOSRoutes(apiV1Group, r, eventIngest, agentOSRuntime, planRuntime, platformRuntime)
}

func newV1(
	t usecase.AgentExecutor,
	o usecase.OrchestrationExecutor,
	l logger.Interface,
	cancelWorkflow CancelWorkflowFn,
	signalWorkflow SignalWorkflowFn,
	eventIngest *eventing.Service,
	agentOSRuntime agentos.Runtime,
	planRuntime agentos.PlanRuntime,
	platformRuntime agentosplatform.Runtime,
) *V1 {
	return &V1{
		t:              t,
		o:              o,
		l:              l,
		v:              validator.New(validator.WithRequiredStructEnabled()),
		cancelWorkflow: cancelWorkflow, signalWorkflow: signalWorkflow,
		eventIngest:     eventIngest,
		agentOSRuntime:  agentOSRuntime,
		planRuntime:     planRuntime,
		platformRuntime: platformRuntime,
	}
}

func registerTemplateRoutes(apiV1Group fiber.Router, m usecase.TemplateManager, l logger.Interface) {
	if m != nil {
		tpl := &templateHandler{m: m, l: l}
		tplGroup := apiV1Group.Group("/templates")
		tplGroup.Post("/import", tpl.importYAML)
		tplGroup.Get("/", tpl.list)
		tplGroup.Get("/:template_id", tpl.get)
		tplGroup.Delete("/:template_id", tpl.delete)
	}
}

func registerTriggerRoutes(apiV1Group fiber.Router, eh usecase.TriggerEventHandler, t usecase.AgentExecutor, l logger.Interface) {
	if eh != nil {
		th := &triggerWebhookHandler{eh: eh, t: t, l: l}
		apiV1Group.Post("/triggers/events", th.handleEvent)
	}
}

func registerOrchestrationRoutes(apiV1Group fiber.Router, r *V1, o usecase.OrchestrationExecutor) {
	if o != nil {
		apiV1Group.Post("/orchestration/execute", r.orchestrate)
		apiV1Group.Get("/orchestration/status/:run_id", r.orchestrationStatus)
	}
}

func registerAgentOSRoutes(
	apiV1Group fiber.Router,
	r *V1,
	eventIngest *eventing.Service,
	agentOSRuntime agentos.Runtime,
	planRuntime agentos.PlanRuntime,
	platformRuntime agentosplatform.Runtime,
) {
	registerAgentOSRunRoutes(apiV1Group, r, eventIngest, agentOSRuntime)
	registerAgentOSPlanRoutes(apiV1Group, r, planRuntime)
	registerAgentOSProcessPlatformRoutes(apiV1Group, r, platformRuntime)
}

func registerAgentOSRunRoutes(apiV1Group fiber.Router, r *V1, eventIngest *eventing.Service, agentOSRuntime agentos.Runtime) {
	if eventIngest != nil {
		apiV1Group.Post("/agentos/runs/:run_id/events", r.ingestAgentOSEvent)
	}

	if agentOSRuntime != nil {
		apiV1Group.Post("/agentos/runs", r.startAgentOSRun)
		apiV1Group.Get("/agentos/runs/:run_id/status", r.statusAgentOSRun)
		apiV1Group.Post("/agentos/runs/:run_id/signals", r.signalAgentOSRun)
		apiV1Group.Post("/agentos/runs/:run_id/control", r.controlAgentOSRun)
	}
}

func registerAgentOSPlanRoutes(apiV1Group fiber.Router, r *V1, planRuntime agentos.PlanRuntime) {
	apiV1Group.Get("/agentos/plans/schemas/:kind", r.agentOSPlanSchema)
	apiV1Group.Get("/agentos/plans/author", r.agentOSPlanAuthor)

	if planRuntime != nil {
		apiV1Group.Post("/agentos/plans", r.startAgentOSPlan)
		apiV1Group.Get("/agentos/plans/:plan_id/status", r.statusAgentOSPlan)
		apiV1Group.Get("/agentos/plans/:plan_id/description", r.describeAgentOSPlan)
		apiV1Group.Get("/agentos/plans/:plan_id/console", r.agentOSPlanConsole)
		apiV1Group.Post("/agentos/plans/:plan_id/signals", r.signalAgentOSPlan)
		apiV1Group.Post("/agentos/plans/:plan_id/control", r.controlAgentOSPlan)
		apiV1Group.Get("/agentos/plans/:plan_id/events/history", r.listAgentOSPlanEvents)
		apiV1Group.Get("/agentos/plans/:plan_id/debug/traces", r.listAgentOSPlanDebugTraces)
		apiV1Group.Get("/agentos/plans/:plan_id/events", r.streamAgentOSPlanEvents)
		apiV1Group.Get("/agentos/plans/:plan_id/audits", r.listAgentOSPlanAudits)
		apiV1Group.Get("/agentos/plans/:plan_id/artifacts", r.listAgentOSPlanArtifacts)
		apiV1Group.Get("/agentos/plans/:plan_id/artifacts/:artifact_id", r.getAgentOSPlanArtifact)
	}
}

func registerAgentOSProcessPlatformRoutes(apiV1Group fiber.Router, r *V1, platformRuntime agentosplatform.Runtime) {
	if platformRuntime != nil {
		apiV1Group.Post("/agentos/processes", r.startAgentOSProcess)
		apiV1Group.Get("/agentos/processes", r.listAgentOSProcesses)
		apiV1Group.Get("/agentos/processes/:process_id/status", r.statusAgentOSProcess)
		apiV1Group.Get("/agentos/processes/:process_id", r.describeAgentOSProcess)

		apiV1Group.Post("/agentos/ledger", r.appendAgentOSLedgerEntry)
		apiV1Group.Get("/agentos/ledger", r.listAgentOSLedgerEntries)

		apiV1Group.Post("/agentos/actions", r.requestAgentOSAction)
		apiV1Group.Get("/agentos/actions", r.listAgentOSActions)
		apiV1Group.Get("/agentos/actions/:action_id", r.statusAgentOSAction)
		apiV1Group.Post("/agentos/actions/:action_id/dry-run", r.recordAgentOSActionDryRun)
		apiV1Group.Post("/agentos/actions/:action_id/approval", r.resolveAgentOSActionApproval)
		apiV1Group.Post("/agentos/actions/:action_id/execution", r.completeAgentOSAction)
		apiV1Group.Post("/agentos/actions/:action_id/cancel", r.cancelAgentOSAction)

		apiV1Group.Post("/agentos/worksets", r.startAgentOSWorkset)
		apiV1Group.Get("/agentos/worksets", r.listAgentOSWorksets)
		apiV1Group.Get("/agentos/worksets/:workset_id", r.statusAgentOSWorkset)

		apiV1Group.Get("/agentos/resources", r.getOrListAgentOSResources)
	}
}
