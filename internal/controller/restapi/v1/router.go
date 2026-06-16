package v1

import (
	"github.com/TekkenSteve/GoAgent/agentos"
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
) {
	r := &V1{
		t: t, o: o, l: l, v: validator.New(validator.WithRequiredStructEnabled()),
		cancelWorkflow: cancelWorkflow, signalWorkflow: signalWorkflow,
		eventIngest:    eventIngest,
		agentOSRuntime: agentOSRuntime,
	}

	// Template routes (optional — requires TemplateManager)
	if m != nil {
		tpl := &templateHandler{m: m, l: l}
		tplGroup := apiV1Group.Group("/templates")
		{
			tplGroup.Post("/import", tpl.importYAML)
			tplGroup.Get("/", tpl.list)
			tplGroup.Get("/:template_id", tpl.get)
			tplGroup.Delete("/:template_id", tpl.delete)
		}
	}

	// Trigger event webhook routes (optional — requires TriggerEventHandler)
	if eh != nil {
		th := &triggerWebhookHandler{eh: eh, t: t, l: l}
		apiV1Group.Post("/triggers/events", th.handleEvent)
	}

	// Orchestration route (optional — requires OrchestrationExecutor)
	if o != nil {
		apiV1Group.Post("/orchestration/execute", r.orchestrate)
		apiV1Group.Get("/orchestration/status/:run_id", r.orchestrationStatus)
	}

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
