package v1

import (
	"errors"
	"net/http"
	"reflect"

	agentosprocess "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/gofiber/fiber/v2"
)

var errRESTQueryRequestRequired = errors.New("rest api query request is required")

type restQueryValidator interface {
	Validate() error
}

// @Summary     Start AgentOS process
// @Description Start a durable business process for a tenant-scoped resource.
// @ID          agentos-start-process
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       request body agentosprocess.Spec true "AgentOS process request"
// @Success     202 {object} agentosprocess.Status
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/processes [post]
func (r *V1) startAgentOSProcess(ctx *fiber.Ctx) error {
	if r.platformRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos platform runtime is not configured")
	}

	var spec agentosprocess.Spec
	if err := ctx.BodyParser(&spec); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}

	status, err := r.platformRuntime.StartProcess(ctx.UserContext(), &spec)
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusAccepted).JSON(status)
}

// @Summary     List AgentOS processes
// @Description Query durable process projections for a tenant, resource, kind, or lifecycle state.
// @ID          agentos-list-processes
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       resource_kind query string false "Resource kind"
// @Param       resource_id query string false "Resource ID"
// @Param       kind query string false "Process kind"
// @Param       lifecycle_state query string false "Lifecycle state"
// @Param       limit query int false "Maximum processes"
// @Success     200 {array} agentosprocess.Status
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/processes [get]
func (r *V1) listAgentOSProcesses(ctx *fiber.Ctx) error {
	return withPlatformQueryScope(r, ctx, "process scope", func(req request.AgentOSProcessScope) (any, error) {
		return r.platformRuntime.ListProcesses(ctx.UserContext(), &agentosprocess.Scope{
			AccountID:      req.AccountID,
			ProjectID:      req.ProjectID,
			Resource:       agentOSProcessListResourceRef(req.AccountID, req.ProjectID, req.ResourceKind, req.ResourceID),
			ResourceKind:   req.ResourceKind,
			Kind:           req.Kind,
			LifecycleState: req.LifecycleState,
			Limit:          req.Limit,
		})
	})
}

// @Summary     Describe AgentOS process
// @Description Query the public description of one durable business process.
// @ID          agentos-describe-process
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       process_id path string true "Process ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Success     200 {object} agentosprocess.Description
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/processes/{process_id} [get]
func (r *V1) describeAgentOSProcess(ctx *fiber.Ctx) error {
	return r.withProcessRef(ctx, func(ref agentosprocess.Ref) error {
		description, err := r.platformRuntime.DescribeProcess(ctx.UserContext(), ref)
		if err != nil {
			return agentOSError(ctx, err)
		}

		return ctx.Status(http.StatusOK).JSON(description)
	})
}

// @Summary     Get AgentOS process status
// @Description Query the current status of one durable business process.
// @ID          agentos-process-status
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       process_id path string true "Process ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Success     200 {object} agentosprocess.Status
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/processes/{process_id}/status [get]
func (r *V1) statusAgentOSProcess(ctx *fiber.Ctx) error {
	return r.withProcessRef(ctx, func(ref agentosprocess.Ref) error {
		status, err := r.platformRuntime.StatusProcess(ctx.UserContext(), ref)
		if err != nil {
			return agentOSError(ctx, err)
		}

		return ctx.Status(http.StatusOK).JSON(status)
	})
}

// @Summary     Append AgentOS ledger entry
// @Description Append one durable process or resource ledger entry.
// @ID          agentos-append-ledger-entry
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       request body agentosprocess.LedgerEntrySpec true "Ledger entry request"
// @Success     201 {object} agentosprocess.LedgerEntry
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/ledger [post]
func (r *V1) appendAgentOSLedgerEntry(ctx *fiber.Ctx) error {
	if r.platformRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos platform runtime is not configured")
	}

	var spec agentosprocess.LedgerEntrySpec
	if err := ctx.BodyParser(&spec); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}

	entry, err := r.platformRuntime.AppendLedgerEntry(ctx.UserContext(), &spec)
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusCreated).JSON(entry)
}

// @Summary     List AgentOS ledger entries
// @Description Query durable ledger entries for a tenant, process, resource, or kind.
// @ID          agentos-list-ledger-entries
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       process_id query string false "Process ID"
// @Param       resource_kind query string false "Resource kind"
// @Param       resource_id query string false "Resource ID"
// @Param       kind query string false "Ledger entry kind"
// @Param       after_sequence query int false "Only return entries after this sequence"
// @Param       limit query int false "Maximum entries"
// @Success     200 {array} agentosprocess.LedgerEntry
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/ledger [get]
func (r *V1) listAgentOSLedgerEntries(ctx *fiber.Ctx) error {
	var req request.AgentOSLedgerScope

	return withPlatformListScope(r, ctx, "ledger scope", &req, func(selector *processPlatformSelector) (any, error) {
		scope := selector.ledgerScope(req.Kind, req.AfterSequence)

		return r.platformRuntime.ListLedgerEntries(ctx.UserContext(), scope)
	})
}

// @Summary     Request AgentOS governed action
// @Description Request one governed action with durable dry-run, approval, and execution state.
// @ID          agentos-request-action
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       request body agentosprocess.GovernedActionSpec true "Governed action request"
// @Success     202 {object} agentosprocess.GovernedActionStatus
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/actions [post]
func (r *V1) requestAgentOSAction(ctx *fiber.Ctx) error {
	if r.platformRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos platform runtime is not configured")
	}

	var spec agentosprocess.GovernedActionSpec
	if err := ctx.BodyParser(&spec); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}

	status, err := r.platformRuntime.RequestAction(ctx.UserContext(), &spec)
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusAccepted).JSON(status)
}

// @Summary     List AgentOS governed actions
// @Description Query governed action projections for a tenant, process, resource, kind, or lifecycle state.
// @ID          agentos-list-actions
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       process_id query string false "Process ID"
// @Param       resource_kind query string false "Resource kind"
// @Param       resource_id query string false "Resource ID"
// @Param       kind query string false "Action kind"
// @Param       lifecycle_state query string false "Lifecycle state"
// @Param       limit query int false "Maximum actions"
// @Success     200 {array} agentosprocess.GovernedActionStatus
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/actions [get]
func (r *V1) listAgentOSActions(ctx *fiber.Ctx) error {
	var req request.AgentOSActionScope

	return withPlatformListScope(r, ctx, "action scope", &req, func(selector *processPlatformSelector) (any, error) {
		scope := selector.actionScope(req.Kind, req.LifecycleState)

		return r.platformRuntime.ListActions(ctx.UserContext(), scope)
	})
}

// @Summary     Get AgentOS governed action status
// @Description Query one governed action projection.
// @ID          agentos-action-status
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       action_id path string true "Action ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Success     200 {object} agentosprocess.GovernedActionStatus
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/actions/{action_id} [get]
func (r *V1) statusAgentOSAction(ctx *fiber.Ctx) error {
	return r.withActionRef(ctx, func(ref agentosprocess.ActionRef) error {
		status, err := r.platformRuntime.StatusAction(ctx.UserContext(), ref)
		if err != nil {
			return agentOSError(ctx, err)
		}

		return ctx.Status(http.StatusOK).JSON(status)
	})
}

// @Summary     Record AgentOS action dry-run
// @Description Record one governed action dry-run result.
// @ID          agentos-record-action-dry-run
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       action_id path string true "Action ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       request body agentosprocess.ActionDryRunResult true "Dry-run result"
// @Success     200 {object} agentosprocess.GovernedActionStatus
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/actions/{action_id}/dry-run [post]
func (r *V1) recordAgentOSActionDryRun(ctx *fiber.Ctx) error {
	return withAgentOSActionTransition[agentosprocess.ActionDryRunResult](r, ctx, func(ref agentosprocess.ActionRef, result *agentosprocess.ActionDryRunResult) (agentosprocess.GovernedActionStatus, error) {
		return r.platformRuntime.RecordActionDryRun(ctx.UserContext(), ref, result)
	})
}

// @Summary     Resolve AgentOS action approval
// @Description Record a governed action approval decision.
// @ID          agentos-resolve-action-approval
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       action_id path string true "Action ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       request body agentosprocess.ActionApprovalDecision true "Approval decision"
// @Success     200 {object} agentosprocess.GovernedActionStatus
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/actions/{action_id}/approval [post]
func (r *V1) resolveAgentOSActionApproval(ctx *fiber.Ctx) error {
	return withAgentOSActionTransition[agentosprocess.ActionApprovalDecision](r, ctx, func(ref agentosprocess.ActionRef, decision *agentosprocess.ActionApprovalDecision) (agentosprocess.GovernedActionStatus, error) {
		return r.platformRuntime.ResolveActionApproval(ctx.UserContext(), ref, decision)
	})
}

// @Summary     Complete AgentOS action execution
// @Description Record the execution result for one governed action.
// @ID          agentos-complete-action
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       action_id path string true "Action ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       request body agentosprocess.ActionExecutionResult true "Execution result"
// @Success     200 {object} agentosprocess.GovernedActionStatus
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/actions/{action_id}/execution [post]
func (r *V1) completeAgentOSAction(ctx *fiber.Ctx) error {
	return withAgentOSActionTransition[agentosprocess.ActionExecutionResult](r, ctx, func(ref agentosprocess.ActionRef, result *agentosprocess.ActionExecutionResult) (agentosprocess.GovernedActionStatus, error) {
		return r.platformRuntime.CompleteAction(ctx.UserContext(), ref, result)
	})
}

// @Summary     Cancel AgentOS action
// @Description Cancel one governed action.
// @ID          agentos-cancel-action
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       action_id path string true "Action ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       request body agentosprocess.ActionCancelRequest true "Cancel request"
// @Success     200 {object} agentosprocess.GovernedActionStatus
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/actions/{action_id}/cancel [post]
func (r *V1) cancelAgentOSAction(ctx *fiber.Ctx) error {
	return withAgentOSActionTransition[agentosprocess.ActionCancelRequest](r, ctx, func(ref agentosprocess.ActionRef, req *agentosprocess.ActionCancelRequest) (agentosprocess.GovernedActionStatus, error) {
		return r.platformRuntime.CancelAction(ctx.UserContext(), ref, req)
	})
}

// @Summary     Start AgentOS workset
// @Description Start one durable coarse-grained batch workset.
// @ID          agentos-start-workset
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       request body agentosprocess.WorksetSpec true "Workset request"
// @Success     202 {object} agentosprocess.WorksetStatus
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/worksets [post]
func (r *V1) startAgentOSWorkset(ctx *fiber.Ctx) error {
	if r.platformRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos platform runtime is not configured")
	}

	var spec agentosprocess.WorksetSpec
	if err := ctx.BodyParser(&spec); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
	}

	status, err := r.platformRuntime.StartWorkset(ctx.UserContext(), &spec)
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusAccepted).JSON(status)
}

// @Summary     List AgentOS worksets
// @Description Query batch workset projections for a tenant, process, resource, kind, or lifecycle state.
// @ID          agentos-list-worksets
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       process_id query string false "Process ID"
// @Param       resource_kind query string false "Resource kind"
// @Param       resource_id query string false "Resource ID"
// @Param       kind query string false "Workset kind"
// @Param       lifecycle_state query string false "Lifecycle state"
// @Param       limit query int false "Maximum worksets"
// @Success     200 {array} agentosprocess.WorksetStatus
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/worksets [get]
func (r *V1) listAgentOSWorksets(ctx *fiber.Ctx) error {
	var req request.AgentOSWorksetScope

	return withPlatformListScope(r, ctx, "workset scope", &req, func(selector *processPlatformSelector) (any, error) {
		scope := selector.worksetScope(req.Kind, req.LifecycleState)

		return r.platformRuntime.ListWorksets(ctx.UserContext(), scope)
	})
}

// @Summary     Get AgentOS workset status
// @Description Query one workset projection.
// @ID          agentos-workset-status
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       workset_id path string true "Workset ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Success     200 {object} agentosprocess.WorksetStatus
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/worksets/{workset_id} [get]
func (r *V1) statusAgentOSWorkset(ctx *fiber.Ctx) error {
	return r.withWorksetRef(ctx, func(ref agentosprocess.WorksetRef) error {
		status, err := r.platformRuntime.StatusWorkset(ctx.UserContext(), ref)
		if err != nil {
			return agentOSError(ctx, err)
		}

		return ctx.Status(http.StatusOK).JSON(status)
	})
}

// @Summary     Get or list AgentOS resource projections
// @Description Query durable process-platform projections for application-owned resources.
// @ID          agentos-resources
// @Tags        agentos
// @Accept      json
// @Produce     json
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       resource_kind query string false "Resource kind"
// @Param       resource_id query string false "Resource ID"
// @Param       lifecycle_state query string false "Lifecycle state"
// @Param       limit query int false "Maximum projection rows or nested records"
// @Success     200 {object} agentosprocess.ResourceProjection
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/resources [get]
func (r *V1) getOrListAgentOSResources(ctx *fiber.Ctx) error {
	return withPlatformQueryScope(r, ctx, "resource scope", func(req request.AgentOSResourceScope) (any, error) {
		if req.ResourceID != "" {
			return r.platformRuntime.GetResourceProjection(ctx.UserContext(), &agentosprocess.ResourceProjectionScope{
				Resource: agentOSResourceRef(req.AccountID, req.ProjectID, req.ResourceKind, req.ResourceID),
				Limit:    req.Limit,
			})
		}

		return r.platformRuntime.ListResourceProjections(ctx.UserContext(), &agentosprocess.ResourceProjectionListScope{
			AccountID:      req.AccountID,
			ProjectID:      req.ProjectID,
			ResourceKind:   req.ResourceKind,
			LifecycleState: req.LifecycleState,
			Limit:          req.Limit,
		})
	})
}

func (r *V1) withProcessRef(ctx *fiber.Ctx, fn func(agentosprocess.Ref) error) error {
	var req request.AgentOSProcessRef

	return withPlatformRef(r, ctx, "process scope", &req, func() error {
		return fn(agentosprocess.Ref{
			ProcessID: ctx.Params("process_id"),
			AccountID: req.AccountID,
			ProjectID: req.ProjectID,
		})
	})
}

func (r *V1) withActionRef(ctx *fiber.Ctx, fn func(agentosprocess.ActionRef) error) error {
	var req request.AgentOSActionRef

	return withPlatformRef(r, ctx, "action scope", &req, func() error {
		return fn(agentosprocess.ActionRef{
			ActionID:  ctx.Params("action_id"),
			AccountID: req.AccountID,
			ProjectID: req.ProjectID,
		})
	})
}

func withAgentOSActionTransition[T any](
	r *V1,
	ctx *fiber.Ctx,
	fn func(agentosprocess.ActionRef, *T) (agentosprocess.GovernedActionStatus, error),
) error {
	return r.withActionRef(ctx, func(ref agentosprocess.ActionRef) error {
		var body T
		if err := ctx.BodyParser(&body); err != nil {
			return errorResponse(ctx, http.StatusBadRequest, "invalid request body")
		}

		status, err := fn(ref, &body)
		if err != nil {
			return agentOSError(ctx, err)
		}

		return ctx.Status(http.StatusOK).JSON(status)
	})
}

func (r *V1) withWorksetRef(ctx *fiber.Ctx, fn func(agentosprocess.WorksetRef) error) error {
	var req request.AgentOSWorksetRef

	return withPlatformRef(r, ctx, "workset scope", &req, func() error {
		return fn(agentosprocess.WorksetRef{
			WorksetID: ctx.Params("workset_id"),
			AccountID: req.AccountID,
			ProjectID: req.ProjectID,
		})
	})
}

func withPlatformQueryScope[T any](r *V1, ctx *fiber.Ctx, scopeName string, fn func(T) (any, error)) error {
	var req T

	return withRuntimeQueryScope(r, ctx, scopeName, r.platformRuntime != nil, "agentos platform runtime is not configured", &req, fn)
}

func withPlatformListScope[T processPlatformScopeRequest](r *V1, ctx *fiber.Ctx, scopeName string, req *T, fn func(*processPlatformSelector) (any, error)) error {
	if err := parseRuntimeScopedQuery(r, ctx, scopeName, r.platformRuntime != nil, "agentos platform runtime is not configured", req); err != nil {
		return err
	}

	result, err := fn(newProcessPlatformSelector(*req))
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusOK).JSON(result)
}

func agentOSResourceRef(accountID, projectID string, kind agentosprocess.ResourceKind, resourceID string) agentosprocess.ResourceRef {
	if kind == "" && resourceID == "" {
		return agentosprocess.ResourceRef{}
	}

	return agentosprocess.ResourceRef{
		Kind:       kind,
		ResourceID: resourceID,
		AccountID:  accountID,
		ProjectID:  projectID,
	}
}

func agentOSProcessListResourceRef(accountID, projectID string, kind agentosprocess.ResourceKind, resourceID string) agentosprocess.ResourceRef {
	if resourceID == "" {
		return agentosprocess.ResourceRef{}
	}

	return agentOSResourceRef(accountID, projectID, kind, resourceID)
}

func withPlatformRef[T any](r *V1, ctx *fiber.Ctx, scopeName string, req *T, fn func() error) error {
	if err := parseRuntimeScopedQuery(r, ctx, scopeName, r.platformRuntime != nil, "agentos platform runtime is not configured", req); err != nil {
		return err
	}

	if err := fn(); err != nil {
		return agentOSError(ctx, err)
	}

	return nil
}

func withRuntimeQueryScope[T any](r *V1, ctx *fiber.Ctx, scopeName string, runtimeConfigured bool, runtimeMessage string, req *T, fn func(T) (any, error)) error {
	if err := parseRuntimeScopedQuery(r, ctx, scopeName, runtimeConfigured, runtimeMessage, req); err != nil {
		return err
	}

	result, err := fn(*req)
	if err != nil {
		return agentOSError(ctx, err)
	}

	return ctx.Status(http.StatusOK).JSON(result)
}

func parseRuntimeScopedQuery(r *V1, ctx *fiber.Ctx, scopeName string, runtimeConfigured bool, runtimeMessage string, req any) error {
	if !runtimeConfigured {
		return errorResponse(ctx, http.StatusNotFound, runtimeMessage)
	}

	if req == nil {
		return errRESTQueryRequestRequired
	}

	if err := ctx.QueryParser(req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid "+scopeName)
	}

	if err := validateRESTQuery(r, req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	return nil
}

func validateRESTQuery(r *V1, req any) error {
	if validator, ok := req.(restQueryValidator); ok {
		return validator.Validate()
	}

	if target := queryValue(req); target.IsValid() && target.CanInterface() {
		if validator, ok := target.Interface().(restQueryValidator); ok {
			return validator.Validate()
		}
	}

	return r.v.Struct(req)
}

func queryValue(req any) reflect.Value {
	value := reflect.ValueOf(req)
	if value.Kind() == reflect.Pointer && !value.IsNil() {
		return value.Elem()
	}

	return value
}

type processPlatformSelector struct {
	accountID    string
	projectID    string
	processID    string
	resourceKind agentosprocess.ResourceKind
	resourceID   string
	limit        int
}

type processPlatformScopeRequest interface {
	request.AgentOSLedgerScope | request.AgentOSActionScope | request.AgentOSWorksetScope
}

func newProcessPlatformSelector[T processPlatformScopeRequest](req T) *processPlatformSelector {
	switch scope := any(req).(type) {
	case request.AgentOSLedgerScope:
		return &processPlatformSelector{
			accountID:    scope.AccountID,
			projectID:    scope.ProjectID,
			processID:    scope.ProcessID,
			resourceKind: scope.ResourceKind,
			resourceID:   scope.ResourceID,
			limit:        scope.Limit,
		}
	case request.AgentOSActionScope:
		return &processPlatformSelector{
			accountID:    scope.AccountID,
			projectID:    scope.ProjectID,
			processID:    scope.ProcessID,
			resourceKind: scope.ResourceKind,
			resourceID:   scope.ResourceID,
			limit:        scope.Limit,
		}
	case request.AgentOSWorksetScope:
		return &processPlatformSelector{
			accountID:    scope.AccountID,
			projectID:    scope.ProjectID,
			processID:    scope.ProcessID,
			resourceKind: scope.ResourceKind,
			resourceID:   scope.ResourceID,
			limit:        scope.Limit,
		}
	default:
		return &processPlatformSelector{}
	}
}

func (s *processPlatformSelector) ledgerScope(kind agentosprocess.LedgerEntryKind, afterSequence int64) *agentosprocess.LedgerScope {
	scope := agentosprocess.LedgerScope{
		Kind:          kind,
		AfterSequence: afterSequence,
	}
	s.apply(&scope.AccountID, &scope.ProjectID, &scope.ProcessID, &scope.Resource, &scope.Limit)

	return &scope
}

func (s *processPlatformSelector) actionScope(kind agentosprocess.ActionKind, lifecycleState string) *agentosprocess.ActionScope {
	scope := agentosprocess.ActionScope{
		Kind:           kind,
		LifecycleState: lifecycleState,
	}
	s.apply(&scope.AccountID, &scope.ProjectID, &scope.ProcessID, &scope.Resource, &scope.Limit)

	return &scope
}

func (s *processPlatformSelector) worksetScope(kind agentosprocess.WorksetKind, lifecycleState string) *agentosprocess.WorksetScope {
	scope := agentosprocess.WorksetScope{
		Kind:           kind,
		LifecycleState: lifecycleState,
	}
	s.apply(&scope.AccountID, &scope.ProjectID, &scope.ProcessID, &scope.Resource, &scope.Limit)

	return &scope
}

func (s *processPlatformSelector) apply(accountID, projectID, processID *string, resource *agentosprocess.ResourceRef, limit *int) {
	*accountID = s.accountID
	*projectID = s.projectID
	*processID = s.processID
	*resource = s.resourceRef()
	*limit = s.limit
}

func (s *processPlatformSelector) resourceRef() agentosprocess.ResourceRef {
	return agentOSResourceRef(s.accountID, s.projectID, s.resourceKind, s.resourceID)
}
