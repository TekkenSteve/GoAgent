package validation

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/runtimeops"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/tool"
)

type noopValidator struct{}

func (noopValidator) Validate(_ context.Context, _ tool.ToolRequest) error { return nil }

type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(_ context.Context, _ tool.ToolRequest) error { return nil }

type staticPolicy struct{ p tool.ToolPolicy }

func (s staticPolicy) GetPolicy(_ string) tool.ToolPolicy { return s.p }

type profileExecutor struct {
	mu      sync.Mutex
	calls   int
	profile []error
}

func (e *profileExecutor) Execute(_ context.Context, _ tool.ToolRequest) (tool.RawResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	idx := (e.calls - 1) % len(e.profile)
	if err := e.profile[idx]; err != nil {
		return tool.RawResult{}, err
	}
	return tool.RawResult{Payload: map[string]any{"ok": true}}, nil
}

type transientErr struct{ error }

type transientClassifier struct{}

func (transientClassifier) IsTransient(err error) bool {
	var te transientErr
	return errors.As(err, &te)
}

func TestLoadSuiteShortTaskThroughput(t *testing.T) {
	const (
		totalRequests = 1500
		workers       = 32
	)

	executor := &profileExecutor{profile: []error{nil}}
	pipeline := tool.Pipeline{
		Validator:  noopValidator{},
		Authorizer: allowAllAuthorizer{},
		Executor:   executor,
		Policies: staticPolicy{p: tool.ToolPolicy{
			Timeout:     2 * time.Second,
			MaxAttempts: 1,
		}},
	}

	jobs := make(chan int)
	var errs atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()

	for range workers {
		wg.Go(func() {
			for id := range jobs {
				_, err := pipeline.Execute(context.Background(), tool.ToolRequest{
					RunID:      "load-short",
					ToolCallID: "call",
					ToolName:   "noop",
					Args:       map[string]any{"task_id": id},
				})
				if err != nil {
					errs.Add(1)
				}
			}
		})
	}

	for i := range totalRequests {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	duration := time.Since(start)

	require.Zero(t, errs.Load())
	require.Equal(t, totalRequests, executor.calls)
	require.Less(t, duration, 15*time.Second)
}

func TestLoadSuiteLongSessionContinuationStability(t *testing.T) {
	policy := orchestration.ContinueAsNewPolicy{
		StepThreshold:    50,
		MaxContinuations: 10,
	}

	input := orchestration.WorkflowInput{
		Request: orchestration.ExecuteRequest{
			RunID:       "run-cont",
			ThreadID:    "thread-1",
			RequestedAt: time.Unix(1, 0).UTC(),
		},
		Continuation: orchestration.ContinuationPayload{RunID: "run-cont"},
	}

	var continuations int32
	totalSteps := int32(500)
	for step := int32(1); step <= totalSteps; step++ {
		status := orchestration.RunStatus{RunID: "run-cont", Step: step}
		decision := orchestration.EvaluateContinueAsNew(policy, orchestration.ContinueAsNewSnapshot{
			Step:            step,
			ContinuationCnt: input.Continuation.ContinuationCount,
		})
		if !decision.ShouldContinue {
			continue
		}
		payload, err := orchestration.BuildContinuationPayload(
			input,
			status,
			"wf-id",
			time.Unix(1+int64(step), 0).UTC(),
		)
		require.NoError(t, err)
		require.Equal(t, "run-cont", payload.RunID)
		require.Equal(t, step, payload.CarriedStep)
		require.Equal(t, input.Request.RequestedAt, payload.InitialRequestedAt)
		input.Continuation = payload
		continuations++
	}

	require.Equal(t, int32(10), continuations)
	require.Equal(t, continuations, input.Continuation.ContinuationCount)
}

func TestLoadSuiteProviderInstabilityProfile(t *testing.T) {
	profile := []error{
		transientErr{error: errors.New("provider timeout")},
		nil,
		transientErr{error: errors.New("provider 429")},
		nil,
		nil,
	}
	executor := &profileExecutor{profile: profile}

	pipeline := tool.Pipeline{
		Validator:  noopValidator{},
		Authorizer: allowAllAuthorizer{},
		Executor:   executor,
		Policies: staticPolicy{p: tool.ToolPolicy{
			Timeout:      2 * time.Second,
			MaxAttempts:  3,
			RetryBackoff: time.Millisecond,
		}},
	}

	const total = 300
	var success int
	for i := range total {
		_, err := pipeline.Execute(context.Background(), tool.ToolRequest{
			RunID:      "load-provider",
			ToolCallID: "call",
			ToolName:   "provider-tool",
			Args:       map[string]any{"i": i},
		})
		if err == nil {
			success++
		}
	}

	ratio := float64(success) / float64(total)
	require.GreaterOrEqual(t, ratio, 0.95)

	thresholds := runtimeops.DefaultV1SLOThresholds()
	violations := runtimeops.EvaluateSLO(runtimeops.SLOSnapshot{
		AdmissionLatencyP95Ms:    150,
		StepLatencyP95Ms:         1400,
		CompletionRate:           ratio,
		RecoveryTimeP95Seconds:   80,
		ContinuationSuccessRatio: 0.999,
	}, thresholds)
	require.Empty(t, violations)
}
