package tool

import (
	"context"
	"fmt"
	"time"
)

const defaultToolTimeout = 10 * time.Second

// Pipeline executes standardized tool stages.
type Pipeline struct {
	Validator   Validator
	Authorizer  Authorizer
	Executor    Executor
	Normalizer  Normalizer
	Persister   Persister
	Policies    PolicyProvider
	Idempotency IdempotencyStore
	Isolation   IsolationPolicy
	Redactor    SecretRedactor
}

// Execute runs validate -> authorize -> execute -> normalize -> persist.
func (p *Pipeline) Execute(ctx context.Context, req *Request) (Result, error) {
	if p.Validator != nil {
		if err := p.Validator.Validate(ctx, req); err != nil {
			return Result{}, fmt.Errorf("%w: %w", ErrValidation, err)
		}
	}

	if p.Authorizer != nil {
		if err := p.Authorizer.Authorize(ctx, req); err != nil {
			return Result{}, err
		}
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

	return p.buildResult(ctx, req, raw, attempts)
}

func (p *Pipeline) checkIdempotency(ctx context.Context, req *Request) (*Result, error) {
	if p.Policies == nil {
		return nil, nil
	}

	policy := p.Policies.GetPolicy(req.ToolName)
	if !policy.EnableIdempotent || req.IdempotencyKey == "" || p.Idempotency == nil {
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
	policy := Policy{Timeout: defaultToolTimeout, MaxAttempts: 1}
	if p.Policies != nil {
		policy = p.Policies.GetPolicy(req.ToolName)
	}

	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}

	if policy.Timeout <= 0 {
		policy.Timeout = defaultToolTimeout
	}

	var raw RawResult

	attempts := 0
	for i := 0; i < policy.MaxAttempts; i++ {
		attempts = i + 1
		attemptCtx, cancel := context.WithTimeout(ctx, policy.Timeout)

		var execErr error

		raw, execErr = p.Executor.Execute(attemptCtx, req)

		cancel()

		if execErr == nil {
			break
		}

		if policy.RetryBackoff > 0 {
			select {
			case <-time.After(policy.RetryBackoff):
			case <-ctx.Done():
				return RawResult{}, 0, fmt.Errorf("%w: %w", ErrExecution, ctx.Err())
			}
		}
	}

	return raw, attempts, nil
}

func (p *Pipeline) buildResult(ctx context.Context, req *Request, raw RawResult, attempts int) (Result, error) {
	normalized, err := p.normalizeResult(ctx, req, raw)
	if err != nil {
		return Result{}, err
	}

	ref, err := p.persistResult(ctx, req, normalized)
	if err != nil {
		return Result{}, err
	}

	isolation := p.resolveIsolation(req)

	result := Result{
		RunID:              req.RunID,
		ToolCallID:         req.ToolCallID,
		ToolName:           req.ToolName,
		Output:             normalized,
		PersistedRef:       ref,
		FromIdempotent:     false,
		Attempts:           attempts,
		ExecutionIsolation: isolation,
	}

	if err := p.cacheResultIfNeeded(ctx, req, &result); err != nil {
		return Result{}, err
	}

	return result, nil
}

// normalizeResult applies the normalizer and redactor stages sequentially.
func (p *Pipeline) normalizeResult(ctx context.Context, req *Request, raw RawResult) (map[string]any, error) {
	normalized := raw.Payload
	if p.Normalizer != nil {
		norm, err := p.Normalizer.Normalize(ctx, req, raw)
		if err != nil {
			return nil, err
		}

		normalized = norm
	}

	if p.Redactor != nil {
		normalized = p.Redactor.Redact(normalized)
	}

	return normalized, nil
}

// persistResult persists the normalized output and returns the reference.
func (p *Pipeline) persistResult(ctx context.Context, req *Request, normalized map[string]any) (string, error) {
	if p.Persister == nil {
		return "", nil
	}

	ref, err := p.Persister.Persist(ctx, req, normalized)
	if err != nil {
		return "", err
	}

	return ref, nil
}

// resolveIsolation returns the isolation policy for the request, or the shared default.
func (p *Pipeline) resolveIsolation(req *Request) ExecutionIsolation {
	if p.Isolation != nil {
		return p.Isolation.Resolve(req)
	}

	return ExecutionIsolationShared
}

// cacheResultIfNeeded stores the result in the idempotency store if the policy requires it.
func (p *Pipeline) cacheResultIfNeeded(ctx context.Context, req *Request, result *Result) error {
	policy := Policy{Timeout: defaultToolTimeout, MaxAttempts: 1}
	if p.Policies != nil {
		policy = p.Policies.GetPolicy(req.ToolName)
	}

	if policy.EnableIdempotent && req.IdempotencyKey != "" && p.Idempotency != nil {
		if err := p.Idempotency.Put(ctx, req.IdempotencyKey, result); err != nil {
			return err
		}
	}

	return nil
}
