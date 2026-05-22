// Package billing provides use-case operations for credit management and cost calculation.
package billing

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo"
	"github.com/google/uuid"
)

// UseCase implements billing and credit management.
type UseCase struct {
	credits repo.CreditManager
	costs   repo.CostCalculator
	usage   repo.UsageRecordRepo
}

// New creates a billing usecase.
// usageRepo may be nil; usage recording is skipped when nil.
func New(credits repo.CreditManager, costs repo.CostCalculator, usageRepo repo.UsageRecordRepo) *UseCase {
	return &UseCase{
		credits: credits,
		costs:   costs,
		usage:   usageRepo,
	}
}

// GetBalance returns the current credit balance for an account.
func (uc *UseCase) GetBalance(ctx context.Context, accountID string) (entity.CreditAccount, error) {
	return uc.credits.GetBalance(ctx, accountID)
}

// DeductUsage calculates the cost of LLM usage and deducts from the account balance.
// Uses Usage.Cost if available (populated by Bifrost gateway), otherwise
// falls back to the CostCalculator for model-based pricing.
// Records a usage record if a UsageRecordRepo is configured.
// Returns the resulting transaction and the calculated cost.
func (uc *UseCase) DeductUsage(ctx context.Context, accountID, modelID string, usage entity.Usage) (entity.CreditTransaction, entity.Money, error) {
	cost := entity.Money(usage.Cost)
	if cost <= 0 && uc.costs != nil {
		// Not provided by gateway — calculate from pricing catalog
		var err error

		c, err := uc.costs.Calculate(ctx, modelID, usage)
		if err != nil {
			return entity.CreditTransaction{}, 0, fmt.Errorf("billing - DeductUsage - calculate cost: %w", err)
		}

		cost = c
	}

	if cost <= 0 {
		// Cost calculation unavailable for this model — skip deduction.
		// This is a graceful fallback when pricing data is missing.
		return entity.CreditTransaction{
			AccountID: accountID,
			Amount:    0,
			Type:      entity.TxnUsage,
		}, 0, nil
	}

	txn, err := uc.credits.Deduct(ctx, accountID, cost, fmt.Sprintf("LLM usage: model=%s tokens=%d+%d", modelID, usage.PromptTokens, usage.CompletionTokens))
	if err != nil {
		return entity.CreditTransaction{}, cost, fmt.Errorf("billing - DeductUsage - deduct: %w", err)
	}

	// Record usage for audit trail (best-effort, non-fatal)
	if uc.usage != nil {
		rec := entity.UsageRecord{
			RecordID:         uuid.New().String(),
			RunID:            "",
			AccountID:        accountID,
			ModelID:          modelID,
			PromptTokens:     usage.PromptTokens,
			CompletionTokens: usage.CompletionTokens,
			TotalTokens:      usage.TotalTokens,
			Cost:             cost,
		}
		if err := uc.usage.CreateUsageRecord(ctx, &rec); err != nil {
			// Non-fatal: billing already succeeded, audit record is best-effort
			return txn, cost, nil
		}
	}

	return txn, cost, nil
}

// AddCredits adds credits to an account (purchase, grant, adjustment).
func (uc *UseCase) AddCredits(ctx context.Context, accountID string, amount entity.Money, description string) (entity.CreditTransaction, error) {
	return uc.credits.AddCredits(ctx, accountID, amount, description)
}

// GetHistory retrieves the transaction history for an account.
func (uc *UseCase) GetHistory(ctx context.Context, accountID string, limit, offset int) ([]entity.CreditTransaction, error) {
	if limit <= 0 {
		limit = 50
	}

	if offset < 0 {
		offset = 0
	}

	return uc.credits.GetHistory(ctx, accountID, limit, offset)
}
