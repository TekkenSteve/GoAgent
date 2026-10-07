package tool

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const defaultToolTimeout = 10 * time.Second

// Pipeline runs a tool call through its stages: validate, authorize, resolve
// idempotency, execute, redact, record.
//
// Build it with NewPipeline. The stages that protect the host are not optional
// — an authorization check that is silently absent, or a side-effecting call
// that silently loses its idempotency, is worse than a service that refuses to
// start, because nothing about a running service says the check is missing.
type Pipeline struct {
	Validator   Validator
	Authorizer  Authorizer
	Executor    Executor
	Redactor    SecretRedactor
	Policies    PolicyProvider
	Idempotency IdempotencyStore
}

// PipelineConfig is the assembly input for NewPipeline.
type PipelineConfig struct {
	// Validator checks the request's arguments. Optional: the tool's own
	// schema is the primary check, and a deployment without extra rules
	// passes nil.
	Validator Validator
	// Authorizer decides whether this call may run. Required.
	Authorizer Authorizer
	// Executor performs the call. Required.
	Executor Executor
	// Redactor masks credentials in the recorded output. Required.
	Redactor SecretRedactor
	// Policies supplies per-tool rules. Required; DefaultPolicyProvider is
	// the conservative baseline.
	Policies PolicyProvider
	// Idempotency resolves a repeated call to its first result. Required:
	// without it, a retried side effect runs twice.
	Idempotency IdempotencyStore
}

// pipelineRequiredStages is how many protective stages NewPipeline insists on.
const pipelineRequiredStages = 5

// ErrPipelineAssembly reports a pipeline built without a stage that protects
// the host.
var ErrPipelineAssembly = errors.New("tool pipeline assembly incomplete")

// NewPipeline assembles the execution pipeline, refusing to build one whose
// protective stages are missing.
func NewPipeline(cfg *PipelineConfig) (*Pipeline, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrPipelineAssembly)
	}

	// Five stages must be present; the slice is sized for them.
	missing := make([]string, 0, pipelineRequiredStages)

	if cfg.Authorizer == nil {
		missing = append(missing, "Authorizer")
	}

	if cfg.Executor == nil {
		missing = append(missing, "Executor")
	}

	if cfg.Redactor == nil {
		missing = append(missing, "Redactor")
	}

	if cfg.Policies == nil {
		missing = append(missing, "Policies")
	}

	if cfg.Idempotency == nil {
		missing = append(missing, "Idempotency")
	}

	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: %v", ErrPipelineAssembly, missing)
	}

	return &Pipeline{
		Validator:   cfg.Validator,
		Authorizer:  cfg.Authorizer,
		Executor:    cfg.Executor,
		Redactor:    cfg.Redactor,
		Policies:    cfg.Policies,
		Idempotency: cfg.Idempotency,
	}, nil
}

// Execute runs validate -> authorize -> idempotency -> execute -> redact -> record.
func (p *Pipeline) Execute(ctx context.Context, req *Request) (Result, error) {
	if p.Validator != nil {
		if err := p.Validator.Validate(ctx, req); err != nil {
			return Result{}, fmt.Errorf("%w: %w", ErrValidation, err)
		}
	}

	if err := p.Authorizer.Authorize(ctx, req); err != nil {
		return Result{}, err
	}

	if result, err := p.checkIdempotency(ctx, req); err != nil {
		return Result{}, err
	} else if result != nil {
		return *result, nil
	}

	raw, attempts, err := p.executeStep(ctx, req)
	if err != nil {
		return Result{}, err
	}

	result := Result{
		RunID:      req.RunID,
		ToolCallID: req.ToolCallID,
		ToolName:   req.ToolName,
		Output:     p.Redactor.Redact(raw.Payload),
		Attempts:   attempts,
	}

	if err := p.cacheResultIfNeeded(ctx, req, &result); err != nil {
		return Result{}, err
	}

	return result, nil
}

// checkIdempotency returns the recorded result of a repeated call, so the
// executor does not run again.
func (p *Pipeline) checkIdempotency(ctx context.Context, req *Request) (*Result, error) {
	if !p.Policies.GetPolicy(req.ToolName).EnableIdempotent || req.IdempotencyKey == "" {
		return nil, nil
	}

	cached, ok, err := p.Idempotency.Get(ctx, req.IdempotencyKey)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	cached.FromIdempotent = true

	return &cached, nil
}

func (p *Pipeline) executeStep(ctx context.Context, req *Request) (RawResult, int, error) {
	policy := p.Policies.GetPolicy(req.ToolName)
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}

	if policy.Timeout <= 0 {
		policy.Timeout = defaultToolTimeout
	}

	var (
		raw     RawResult
		lastErr error
	)

	attempts := 0

	for i := 0; i < policy.MaxAttempts; i++ {
		attempts = i + 1
		attemptCtx, cancel := context.WithTimeout(ctx, policy.Timeout)

		raw, lastErr = p.Executor.Execute(attemptCtx, req)

		cancel()

		if lastErr == nil {
			break
		}

		if policy.RetryBackoff > 0 {
			select {
			case <-time.After(policy.RetryBackoff):
			case <-ctx.Done():
				return RawResult{}, attempts, fmt.Errorf("%w: %w", ErrExecution, ctx.Err())
			}
		}
	}

	// Exhausted attempts is a failure, not a zero-valued success: whatever
	// raw holds is the corpse of the last attempt, and letting it flow on
	// would record a tool result that never happened.
	if lastErr != nil {
		return RawResult{}, attempts, fmt.Errorf("%w after %d attempt(s): %w", ErrExecution, attempts, lastErr)
	}

	return raw, attempts, nil
}

// cacheResultIfNeeded records the result so a repeated key answers with it.
func (p *Pipeline) cacheResultIfNeeded(ctx context.Context, req *Request, result *Result) error {
	if !p.Policies.GetPolicy(req.ToolName).EnableIdempotent || req.IdempotencyKey == "" {
		return nil
	}

	return p.Idempotency.Put(ctx, req.IdempotencyKey, result)
}

// DefaultPolicyProvider is the conservative per-tool baseline.
//
// One attempt, always: the pipeline does not re-run a call that may already
// have had its side effect. A tool that is safe to repeat — or that the
// deployment has declared so — carries the same idempotency key across
// attempts, and enabling idempotency is what makes a retry safe for it.
type DefaultPolicyProvider struct {
	// Policies overrides the baseline per tool name.
	Policies map[string]Policy
	// Timeout bounds one attempt when a tool has no entry.
	Timeout time.Duration
}

// GetPolicy returns the tool's policy, or the baseline.
func (p *DefaultPolicyProvider) GetPolicy(toolName string) Policy {
	if policy, ok := p.Policies[toolName]; ok {
		return policy
	}

	timeout := p.Timeout
	if timeout <= 0 {
		timeout = defaultToolTimeout
	}

	return Policy{Timeout: timeout, MaxAttempts: 1}
}
