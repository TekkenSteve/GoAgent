package orchestration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Custom search attributes shared by the agent workflows. Every key must be
// registered with the Temporal cluster BEFORE deploying workers that set it —
// an unregistered key fails the workflow task ("search attribute not
// defined"). Register with:
//
//	temporal operator search-attribute create --name goagent.run_id --type Keyword
//	temporal operator search-attribute create --name goagent.lifecycle_state --type Keyword
//	temporal operator search-attribute create --name goagent.model --type Keyword
//	temporal operator search-attribute create --name goagent.iteration --type Int
//	temporal operator search-attribute create --name goagent.last_tool --type Keyword
//
// With these, operators can answer "which runs are stuck / waiting for user
// input / on which model" with a visibility query instead of scanning history:
//
//	temporal workflow list -q "WorkflowType='AgentWorkflow' AND goagent.lifecycle_state='waiting_input'"
const (
	SearchAttrRunID          = "goagent.run_id"
	SearchAttrLifecycleState = "goagent.lifecycle_state"
	SearchAttrModel          = "goagent.model"
	SearchAttrIteration      = "goagent.iteration"
	SearchAttrLastTool       = "goagent.last_tool"
)

// SearchAttributesForRun returns the start-time visibility attributes for a
// run, set on StartWorkflowOptions by every executor.
func SearchAttributesForRun(runID, lifecycleState string) map[string]any {
	return map[string]any{
		SearchAttrRunID:          runID,
		SearchAttrLifecycleState: lifecycleState,
	}
}

// syncRunSearchAttributes upserts the visibility attributes that keep the
// execution observable: current lifecycle state, model, iteration, and the
// last tool invoked. UpsertSearchAttributes is merge semantics — absent keys
// are left untouched. Guarded by a workflow.GetVersion marker because it
// emits history events; in-flight runs started before the marker replay
// without the calls.
func syncRunSearchAttributes(ctx workflow.Context, enabled workflow.Version, runID, model, lifecycleState string, iteration int, lastTool string) {
	if enabled < 1 {
		return
	}

	updates := []temporal.SearchAttributeUpdate{
		temporal.NewSearchAttributeKeyKeyword(SearchAttrRunID).ValueSet(runID),
		temporal.NewSearchAttributeKeyKeyword(SearchAttrLifecycleState).ValueSet(lifecycleState),
	}
	if model != "" {
		updates = append(updates, temporal.NewSearchAttributeKeyKeyword(SearchAttrModel).ValueSet(model))
	}

	if iteration > 0 {
		updates = append(updates, temporal.NewSearchAttributeKeyInt64(SearchAttrIteration).ValueSet(int64(iteration)))
	}

	if lastTool != "" {
		updates = append(updates, temporal.NewSearchAttributeKeyKeyword(SearchAttrLastTool).ValueSet(lastTool))
	}

	// Visibility is best-effort observability: a failed upsert (e.g. an
	// unregistered key) must not fail the run, but should be visible to
	// operators.
	if err := workflow.UpsertTypedSearchAttributes(ctx, updates...); err != nil {
		workflow.GetLogger(ctx).Warn("failed to upsert search attributes", "error", err)
	}
}

// errStuckRunsClientRequired is returned by FindStuckRuns when no Temporal
// client is configured.
var errStuckRunsClientRequired = errors.New("FindStuckRuns - client is required")

// FindStuckRuns lists open workflows of the given type that have been running
// longer than olderThan — candidates for runs stuck without progress (dead
// activity worker, unresolved dependency) that would otherwise be invisible
// until a timeout fires. Intended for operator tooling and alerting; the
// threshold should be the expected maximum run duration (e.g. P95 + margin).
func FindStuckRuns(ctx context.Context, c client.Client, workflowType string, olderThan time.Duration) ([]string, error) {
	if c == nil {
		return nil, errStuckRunsClientRequired
	}

	var stuck []string

	request := &workflowservice.ListWorkflowExecutionsRequest{
		Query: fmt.Sprintf("WorkflowType='%s' AND ExecutionStatus='Running'", workflowType),
	}

	now := time.Now()

	for {
		resp, err := c.ListWorkflow(ctx, request)
		if err != nil {
			return nil, fmt.Errorf("FindStuckRuns - ListWorkflow: %w", err)
		}

		for _, info := range resp.Executions {
			if info.StartTime == nil {
				continue
			}

			if now.Sub(info.StartTime.AsTime()) > olderThan {
				stuck = append(stuck, info.Execution.GetWorkflowId())
			}
		}

		if len(resp.NextPageToken) == 0 {
			break
		}

		request.NextPageToken = resp.NextPageToken
	}

	return stuck, nil
}
