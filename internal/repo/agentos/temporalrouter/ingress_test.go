package temporalrouter

import (
	"context"
	"testing"

	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// starterCall is one SignalWithStartWorkflow invocation, captured whole so the
// assertion covers the options rather than only the arguments.
type starterCall struct {
	workflowID string
	signalName string
	signalArg  any
	options    client.StartWorkflowOptions
	workflow   any
	args       []any
}

type fakeWorkflowStarter struct {
	calls []starterCall
	err   error
}

//nolint:gocritic // the parameter list must match the SDK method this interface exists to accept.
func (s *fakeWorkflowStarter) SignalWithStartWorkflow(_ context.Context, workflowID, signalName string, signalArg any,
	options client.StartWorkflowOptions, workflow any, workflowArgs ...any,
) (client.WorkflowRun, error) {
	if s.err != nil {
		return nil, s.err
	}

	s.calls = append(s.calls, starterCall{
		workflowID: workflowID,
		signalName: signalName,
		signalArg:  signalArg,
		options:    options,
		workflow:   workflow,
		args:       workflowArgs,
	})

	return nil, nil
}

func testDelivery() *Delivery {
	return &Delivery{
		WorkflowID:   "run-a",
		WorkflowType: "RunSupervisorWorkflow",
		SignalName:   "run.requested",
		Input: Input{
			Token:  "agentos.commands/2/11",
			Domain: eventlog.DomainCommands,
			Type:   "run.requested",
			Key:    "run-a",
		},
	}
}

func TestNewTemporalIngress_ValidatesConfiguration(t *testing.T) {
	t.Parallel()

	_, err := NewTemporalIngress(nil, "q")
	require.ErrorIs(t, err, ErrTemporalClientRequired)

	_, err = NewTemporalIngress(&fakeWorkflowStarter{}, "")
	require.ErrorIs(t, err, ErrTaskQueueRequired)
}

// TestSignalWithStart_MapsTheDeliveryToTheSDKCall locks the one call this bridge
// is allowed to make.
//
// Signal-with-start is the point: a start followed by a separate signal can come
// apart on a crash, leaving a workflow waiting for a fact that is already in the
// log. One call cannot.
func TestSignalWithStart_MapsTheDeliveryToTheSDKCall(t *testing.T) {
	t.Parallel()

	starter := &fakeWorkflowStarter{}

	ingress, err := NewTemporalIngress(starter, "agentos-run-supervisor")
	require.NoError(t, err)

	delivery := testDelivery()
	require.NoError(t, ingress.SignalWithStart(t.Context(), delivery))

	require.Len(t, starter.calls, 1)
	call := starter.calls[0]

	assert.Equal(t, "run-a", call.workflowID, "the entity key is the workflow ID")
	assert.Equal(t, "run.requested", call.signalName)
	assert.Equal(t, "RunSupervisorWorkflow", call.workflow)
	assert.Equal(t, delivery.Input, call.signalArg, "the signal carries the fact identity")

	// The started execution is given the same input as the signal, so a workflow
	// that this call started sees the fact's token too and can dedupe it the same
	// way a running one does.
	require.Len(t, call.args, 1)
	assert.Equal(t, delivery.Input, call.args[0])

	assert.Equal(t, "agentos-run-supervisor", call.options.TaskQueue)
	assert.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, call.options.WorkflowIDConflictPolicy,
		"a redelivery attaches to the running execution instead of failing")
	assert.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, call.options.WorkflowIDReusePolicy,
		"a later fact about the same entity is new work once the previous execution completed")
}

// TestSignalWithStart_ReportsTemporalFailures locks that a refused delivery
// surfaces: the router only withholds the offset if it learns the call failed.
func TestSignalWithStart_ReportsTemporalFailures(t *testing.T) {
	t.Parallel()

	ingress, err := NewTemporalIngress(&fakeWorkflowStarter{err: errTestTemporalUnavailable}, "q")
	require.NoError(t, err)

	err = ingress.SignalWithStart(t.Context(), testDelivery())
	require.ErrorIs(t, err, errTestTemporalUnavailable)
}
