package tool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/agentfw/runtimeops"
	"github.com/stretchr/testify/require"
)

var (
	errTestProviderTimeout = errors.New("provider timeout")
	errTestProvider429     = errors.New("provider 429")
)

type loadSuiteTransientErr struct{ error }

type loadSuiteNoopValidator struct{}

func (loadSuiteNoopValidator) Validate(_ context.Context, _ *Request) error { return nil }

type loadSuiteAllowAllAuthorizer struct{}

func (loadSuiteAllowAllAuthorizer) Authorize(_ context.Context, _ *Request) error { return nil }

type loadSuiteStaticPolicy struct{ p Policy }

func (s loadSuiteStaticPolicy) GetPolicy(_ string) Policy { return s.p }

type loadSuiteProfileExecutor struct {
	mu      sync.Mutex
	calls   int
	profile []error
}

func (e *loadSuiteProfileExecutor) Execute(_ context.Context, _ *Request) (RawResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.calls++

	idx := (e.calls - 1) % len(e.profile)
	if err := e.profile[idx]; err != nil {
		return RawResult{}, err
	}

	return RawResult{Payload: map[string]any{"ok": true}}, nil
}

func TestLoadSuiteShortTaskThroughput(t *testing.T) {
	t.Parallel()

	const (
		totalRequests = 1500
		workers       = 32
	)

	executor := &loadSuiteProfileExecutor{profile: []error{nil}}
	pipeline := Pipeline{
		Validator:  loadSuiteNoopValidator{},
		Authorizer: loadSuiteAllowAllAuthorizer{},
		Executor:   executor,
		Policies: loadSuiteStaticPolicy{p: Policy{
			Timeout:     2 * time.Second,
			MaxAttempts: 1,
		}},
	}

	jobs := make(chan int)

	var (
		errs atomic.Int64
		wg   sync.WaitGroup
	)

	start := time.Now()

	for range workers {
		wg.Go(func() {
			for id := range jobs {
				_, err := pipeline.Execute(context.Background(), &Request{
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
	t.Parallel()

	policy := orchestration.ContinueAsNewPolicy{
		StepThreshold:    50,
		MaxContinuations: 10,
	}

	input := orchestration.AgentWorkflowInput{
		RunID: "run-cont",
		Continuation: orchestration.ContinuationPayload{
			RunID:              "run-cont",
			InitialRequestedAt: time.Unix(1, 0).UTC(),
		},
	}

	var continuations int32

	totalSteps := int32(500)

	for step := int32(1); step <= totalSteps; step++ {
		decision := orchestration.EvaluateContinueAsNew(policy, orchestration.ContinueAsNewSnapshot{
			Step:            step,
			ContinuationCnt: input.Continuation.ContinuationCount,
		})
		if !decision.ShouldContinue {
			continue
		}

		payload := orchestration.ContinuationPayload{
			RunID:              "run-cont",
			ContinuationCount:  input.Continuation.ContinuationCount + 1,
			PreviousWorkflowID: "wf-id",
			CarriedStep:        step,
			CarriedAt:          time.Unix(1+int64(step), 0).UTC(),
			InitialRequestedAt: input.Continuation.InitialRequestedAt,
		}
		require.Equal(t, step, payload.CarriedStep)
		require.Equal(t, input.Continuation.InitialRequestedAt, payload.InitialRequestedAt)
		input.Continuation = payload
		continuations++
	}

	require.Equal(t, int32(10), continuations)
	require.Equal(t, continuations, input.Continuation.ContinuationCount)
}

func TestLoadSuiteProviderInstabilityProfile(t *testing.T) {
	t.Parallel()

	profile := []error{
		loadSuiteTransientErr{error: errTestProviderTimeout},
		nil,
		loadSuiteTransientErr{error: errTestProvider429},
		nil,
		nil,
	}
	executor := &loadSuiteProfileExecutor{profile: profile}

	pipeline := Pipeline{
		Validator:  loadSuiteNoopValidator{},
		Authorizer: loadSuiteAllowAllAuthorizer{},
		Executor:   executor,
		Policies: loadSuiteStaticPolicy{p: Policy{
			Timeout:      2 * time.Second,
			MaxAttempts:  3,
			RetryBackoff: time.Millisecond,
		}},
	}

	const total = 300

	var success int

	for i := range total {
		_, err := pipeline.Execute(context.Background(), &Request{
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
