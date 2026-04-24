package tool

import (
	"context"
	"fmt"
	"time"
)

// Pipeline executes standardized tool stages.
type Pipeline struct {
	Validator           Validator
	Authorizer          Authorizer
	Executor            Executor
	Normalizer          Normalizer
	Persister           Persister
	Policies            PolicyProvider
	Idempotency         IdempotencyStore
	TransientClassifier TransientClassifier
	Isolation           IsolationPolicy
	Redactor            SecretRedactor
}

// Execute runs validate -> authorize -> execute -> normalize -> persist.
func (p Pipeline) Execute(ctx context.Context, req ToolRequest) (Result, error) {
	if p.Validator != nil {
		if err := p.Validator.Validate(ctx, req); err != nil {
			return Result{}, fmt.Errorf("%w: %v", ErrValidation, err)
		}
	}

	if p.Authorizer != nil {
		if err := p.Authorizer.Authorize(ctx, req); err != nil {
			return Result{}, err
		}
	}

	policy := ToolPolicy{Timeout: 10 * time.Second, MaxAttempts: 1}
	if p.Policies != nil {
		policy = p.Policies.GetPolicy(req.ToolName)
	}
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}
	if policy.Timeout <= 0 {
		policy.Timeout = 10 * time.Second
	}

	if policy.EnableIdempotent && req.IdempotencyKey != "" && p.Idempotency != nil {
		cached, ok, err := p.Idempotency.Get(ctx, req.IdempotencyKey)
		if err != nil {
			return Result{}, err
		}
		if ok {
			cached.FromIdempotent = true
			return cached, nil
		}
	}

	attempts := 0
	var raw RawResult
	var execErr error
	for i := 0; i < policy.MaxAttempts; i++ {
		attempts = i + 1
		attemptCtx, cancel := context.WithTimeout(ctx, policy.Timeout)
		raw, execErr = p.Executor.Execute(attemptCtx, req)
		cancel()
		if execErr == nil {
			break
		}

		transient := p.TransientClassifier != nil && p.TransientClassifier.IsTransient(execErr)
		if !transient || attempts >= policy.MaxAttempts {
			return Result{}, fmt.Errorf("%w: %v", ErrExecution, execErr)
		}
		if policy.RetryBackoff > 0 {
			select {
			case <-time.After(policy.RetryBackoff):
			case <-ctx.Done():
				return Result{}, fmt.Errorf("%w: %v", ErrExecution, ctx.Err())
			}
		}
	}

	normalized := raw.Payload
	if p.Normalizer != nil {
		norm, err := p.Normalizer.Normalize(ctx, req, raw)
		if err != nil {
			return Result{}, err
		}
		normalized = norm
	}
	if p.Redactor != nil {
		normalized = p.Redactor.Redact(normalized)
	}

	ref := ""
	if p.Persister != nil {
		persistRef, err := p.Persister.Persist(ctx, req, normalized)
		if err != nil {
			return Result{}, err
		}
		ref = persistRef
	}

	isolation := ExecutionIsolationShared
	if p.Isolation != nil {
		isolation = p.Isolation.Resolve(req)
	}

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

	if policy.EnableIdempotent && req.IdempotencyKey != "" && p.Idempotency != nil {
		if err := p.Idempotency.Put(ctx, req.IdempotencyKey, result); err != nil {
			return Result{}, err
		}
	}

	return result, nil
}
