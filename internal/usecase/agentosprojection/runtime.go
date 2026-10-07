// Package agentosprojection implements AgentOS resource projections.
package agentosprojection

import (
	"context"
	"errors"
	"slices"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosaction"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosbatch"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosledger"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosprocess"
)

var (
	errProcessIndexRequired = errors.New("agentos projection: process index is required")
	errLedgerStoreRequired  = errors.New("agentos projection: ledger store is required")
	errActionStoreRequired  = errors.New("agentos projection: action store is required")
	errWorksetStoreRequired = errors.New("agentos projection: workset store is required")
	errRuntimeNotConfigured = errors.New("agentos projection: runtime is not configured")
)

// Runtime assembles generic AgentOS resource projections from durable
// read-model stores. It deliberately stays outside Temporal adapters.
type Runtime struct {
	processes agentosprocess.ProcessIndex
	ledger    agentosledger.Store
	actions   agentosaction.Store
	worksets  agentosbatch.Store
}

// Config declares the durable stores used by ProjectionRuntime.
type Config struct {
	Processes agentosprocess.ProcessIndex
	Ledger    agentosledger.Store
	Actions   agentosaction.Store
	Worksets  agentosbatch.Store
}

// NewRuntime creates a projection runtime over existing durable stores.
func NewRuntime(config Config) (*Runtime, error) {
	if config.Processes == nil {
		return nil, errProcessIndexRequired
	}

	if config.Ledger == nil {
		return nil, errLedgerStoreRequired
	}

	if config.Actions == nil {
		return nil, errActionStoreRequired
	}

	if config.Worksets == nil {
		return nil, errWorksetStoreRequired
	}

	return &Runtime{
		processes: config.Processes,
		ledger:    config.Ledger,
		actions:   config.Actions,
		worksets:  config.Worksets,
	}, nil
}

// GetResourceProjection assembles the full read model for one resource.
func (r *Runtime) GetResourceProjection(ctx context.Context, scope *agentos.ResourceProjectionScope) (agentos.ResourceProjection, error) {
	if r == nil {
		return agentos.ResourceProjection{}, errRuntimeNotConfigured
	}

	if err := agentos.ValidateResourceProjectionScope(scope); err != nil {
		return agentos.ResourceProjection{}, err
	}

	processes, err := r.processes.ListProcesses(ctx, processScope(scope.Resource, scope.Limit))
	if err != nil {
		return agentos.ResourceProjection{}, err
	}

	ledger, err := r.ledger.ListLedgerEntries(ctx, ledgerScope(scope.Resource, scope.Limit))
	if err != nil {
		return agentos.ResourceProjection{}, err
	}

	actions, err := r.actions.ListActions(ctx, actionScope(scope.Resource, scope.Limit))
	if err != nil {
		return agentos.ResourceProjection{}, err
	}

	worksets, err := r.worksets.ListWorksets(ctx, worksetScope(scope.Resource, scope.Limit))
	if err != nil {
		return agentos.ResourceProjection{}, err
	}

	return agentos.ResourceProjection{
		Resource:  scope.Resource,
		Processes: processes,
		Ledger:    ledger,
		Actions:   actions,
		Worksets:  worksets,
		UpdatedAt: projectionUpdatedAt(processes, ledger, actions, worksets),
	}, nil
}

// ListResourceProjections returns resource summaries derived from process
// projections. A resource becomes visible after at least one durable process
// exists for it.
func (r *Runtime) ListResourceProjections(ctx context.Context, scope *agentos.ResourceProjectionListScope) ([]agentos.ResourceProjectionSummary, error) {
	if r == nil {
		return nil, errRuntimeNotConfigured
	}

	if err := agentos.ValidateResourceProjectionListScope(scope); err != nil {
		return nil, err
	}

	processes, err := r.processes.ListProcesses(ctx, &agentos.Scope{
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		ResourceKind:   scope.ResourceKind,
		LifecycleState: scope.LifecycleState,
		Limit:          scope.Limit,
	})
	if err != nil {
		return nil, err
	}

	return summarizeResources(processes), nil
}

func processScope(resource agentos.ResourceRef, limit int) *agentos.Scope {
	return &agentos.Scope{
		AccountID: resource.AccountID,
		ProjectID: resource.ProjectID,
		Resource:  resource,
		Limit:     limit,
	}
}

func ledgerScope(resource agentos.ResourceRef, limit int) *agentos.LedgerScope {
	return &agentos.LedgerScope{
		AccountID: resource.AccountID,
		ProjectID: resource.ProjectID,
		Resource:  resource,
		Limit:     limit,
	}
}

func actionScope(resource agentos.ResourceRef, limit int) *agentos.ActionScope {
	return &agentos.ActionScope{
		AccountID: resource.AccountID,
		ProjectID: resource.ProjectID,
		Resource:  resource,
		Limit:     limit,
	}
}

func worksetScope(resource agentos.ResourceRef, limit int) *agentos.WorksetScope {
	return &agentos.WorksetScope{
		AccountID: resource.AccountID,
		ProjectID: resource.ProjectID,
		Resource:  resource,
		Limit:     limit,
	}
}

func projectionUpdatedAt(processes []agentos.Status, ledger []agentos.LedgerEntry, actions []agentos.GovernedActionStatus, worksets []agentos.WorksetStatus) time.Time {
	updatedAt := time.Time{}

	for i := range processes {
		status := &processes[i]
		updatedAt = maxTime(updatedAt, status.UpdatedAt)
	}

	for i := range ledger {
		entry := &ledger[i]
		updatedAt = maxTime(updatedAt, entry.CreatedAt)
		updatedAt = maxTime(updatedAt, entry.OccurredAt)
	}

	for i := range actions {
		status := &actions[i]
		updatedAt = maxTime(updatedAt, status.UpdatedAt)
	}

	for i := range worksets {
		status := &worksets[i]
		updatedAt = maxTime(updatedAt, status.UpdatedAt)
	}

	return updatedAt
}

func summarizeResources(processes []agentos.Status) []agentos.ResourceProjectionSummary {
	summaries := make(map[agentos.ResourceRef]agentos.ResourceProjectionSummary)

	for i := range processes {
		status := &processes[i]
		summary := summaries[status.Resource]
		summary.Resource = status.Resource
		summary.ProcessCount++

		if status.UpdatedAt.After(summary.UpdatedAt) {
			summary.LatestProcessID = status.ProcessID
			summary.LatestLifecycleState = status.LifecycleState
			summary.UpdatedAt = status.UpdatedAt
		}

		summaries[status.Resource] = summary
	}

	result := make([]agentos.ResourceProjectionSummary, 0, len(summaries))
	for resource := range summaries {
		result = append(result, summaries[resource])
	}

	slices.SortFunc(result, func(left, right agentos.ResourceProjectionSummary) int {
		return right.UpdatedAt.Compare(left.UpdatedAt)
	})

	return result
}

func maxTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}

	return left
}

var _ agentos.ProjectionRuntime = (*Runtime)(nil)
