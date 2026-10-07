package temporalrouter

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// ErrTemporalClientRequired reports an ingress without a Temporal to call.
var ErrTemporalClientRequired = errors.New("temporal router: workflow client is required")

// WorkflowStarter is the slice of the Temporal client this bridge uses.
//
// It is declared here, one method wide, for two reasons: the adapter becomes
// testable without a server, and the single call the ingress is allowed to make
// is visible in the type rather than buried in an implementation.
type WorkflowStarter interface {
	SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg any,
		options client.StartWorkflowOptions, workflow any, workflowArgs ...any) (client.WorkflowRun, error)
}

// TemporalIngress delivers facts into Temporal as signal-with-start calls.
//
// Signal-with-start rather than start-then-signal is what makes redelivery
// harmless. One call either starts the entity's execution or attaches to the one
// already running, so the two halves cannot come apart with a crash in between —
// a start that succeeded and a signal that never arrived would leave a workflow
// waiting forever for a fact that is already in the log.
type TemporalIngress struct {
	client    WorkflowStarter
	taskQueue string
}

var _ Ingress = (*TemporalIngress)(nil)

// NewTemporalIngress creates the bridge onto one task queue.
func NewTemporalIngress(workflows WorkflowStarter, taskQueue string) (*TemporalIngress, error) {
	if workflows == nil {
		return nil, ErrTemporalClientRequired
	}

	if taskQueue == "" {
		return nil, ErrTaskQueueRequired
	}

	return &TemporalIngress{client: workflows, taskQueue: taskQueue}, nil
}

// SignalWithStart delivers one routed fact.
//
// The workflow ID is the delivery's, which the router derived from the record's
// entity key: the same entity always maps to the same execution, so a town
// cannot end up with two workflows racing over one business object. The input
// carries the fact's token, which is what lets the receiving workflow discard a
// duplicate signal instead of acting on the fact twice.
func (t *TemporalIngress) SignalWithStart(ctx context.Context, delivery *Delivery) error {
	options := client.StartWorkflowOptions{
		ID:        delivery.WorkflowID,
		TaskQueue: t.taskQueue,
		// A redelivery must attach to the running execution; a fresh one is
		// allowed once the previous has completed, because a later fact about the
		// same entity is new work rather than a duplicate of the old.
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}

	if _, err := t.client.SignalWithStartWorkflow(ctx,
		delivery.WorkflowID,
		delivery.SignalName,
		delivery.Input,
		options,
		delivery.WorkflowType,
		delivery.Input,
	); err != nil {
		return fmt.Errorf("temporal ingress - signal-with-start %s %s: %w",
			delivery.WorkflowID, delivery.SignalName, err)
	}

	return nil
}
