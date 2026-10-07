package persistent

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
)

// Sentinel errors.
var (
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrNegativeLimitOffset = errors.New("limit and offset must be non-negative")
)

// BillingRepo implements repo.CreditManager with Postgres. Statements and
// bindings come from queries/billing.sql; NUMERIC columns bind as float64 to
// match the Go money types.
type BillingRepo struct {
	*postgres.Postgres

	queries *sqlcgen.Queries
}

// NewBillingRepo creates a Postgres-backed credit manager.
func NewBillingRepo(pg *postgres.Postgres) *BillingRepo {
	return &BillingRepo{Postgres: pg, queries: sqlcgen.New(pg.Pool)}
}

// GetBalance returns the current credit balance for an account.
// Returns zero balance if the account has no row yet (soft-create).
func (r *BillingRepo) GetBalance(ctx context.Context, accountID string) (entity.CreditAccount, error) {
	row, err := r.queries.GetCreditAccount(ctx, accountID)
	if missingRow(err) {
		return entity.CreditAccount{AccountID: accountID, Balance: 0, Currency: "USD"}, nil
	}

	if err != nil {
		return entity.CreditAccount{}, fmt.Errorf("BillingRepo - GetBalance - query: %w", err)
	}

	return entity.CreditAccount{
		AccountID: row.AccountID,
		Balance:   entity.Money(row.Balance),
		Currency:  row.Currency,
		UpdatedAt: row.UpdatedAt,
	}, nil
}

// Deduct decreases the account balance by amount. Returns the transaction record.
// Returns an error if the balance is insufficient.
func (r *BillingRepo) Deduct(ctx context.Context, accountID string, amount entity.Money, description string) (entity.CreditTransaction, error) {
	amountFloat := float64(amount)

	// The WHERE clause is the sufficiency guard: zero affected rows is the
	// insufficient-balance case, so the check cannot race with the debit.
	deducted, err := r.queries.DeductCreditBalance(ctx, sqlcgen.DeductCreditBalanceParams{
		AccountID: accountID,
		Balance:   amountFloat,
	})
	if err != nil {
		return entity.CreditTransaction{}, fmt.Errorf("BillingRepo - Deduct - update: %w", err)
	}

	if deducted == 0 {
		return entity.CreditTransaction{}, fmt.Errorf("BillingRepo - Deduct - %w: %s", ErrInsufficientBalance, accountID)
	}

	// Record in ledger with the direction carried by the sign.
	row, err := r.queries.InsertCreditLedger(ctx, sqlcgen.InsertCreditLedgerParams{
		AccountID:   accountID,
		Amount:      -amountFloat,
		Type:        entity.TxnUsage,
		Description: description,
	})
	if err != nil {
		return entity.CreditTransaction{}, fmt.Errorf("BillingRepo - Deduct - insert ledger: %w", err)
	}

	return creditTransactionFromRow(&row), nil
}

// AddCredits increases the account balance by amount. Returns the transaction record.
// Creates the account row if it does not exist (upsert).
func (r *BillingRepo) AddCredits(ctx context.Context, accountID string, amount entity.Money, description string) (entity.CreditTransaction, error) {
	amountFloat := float64(amount)

	if err := r.queries.UpsertCreditAccount(ctx, sqlcgen.UpsertCreditAccountParams{
		AccountID: accountID,
		Balance:   amountFloat,
	}); err != nil {
		return entity.CreditTransaction{}, fmt.Errorf("BillingRepo - AddCredits - upsert: %w", err)
	}

	// Determine transaction type
	txnType := entity.TxnPurchase
	if amount < 0 {
		txnType = entity.TxnAdjust
	}

	row, err := r.queries.InsertCreditLedger(ctx, sqlcgen.InsertCreditLedgerParams{
		AccountID:   accountID,
		Amount:      amountFloat,
		Type:        txnType,
		Description: description,
	})
	if err != nil {
		return entity.CreditTransaction{}, fmt.Errorf("BillingRepo - AddCredits - insert ledger: %w", err)
	}

	return creditTransactionFromRow(&row), nil
}

// GetHistory retrieves credit transactions for an account, newest first.
func (r *BillingRepo) GetHistory(ctx context.Context, accountID string, limit, offset int) ([]entity.CreditTransaction, error) {
	if limit < 0 || offset < 0 {
		return nil, ErrNegativeLimitOffset
	}

	rows, err := r.queries.ListCreditHistory(ctx, sqlcgen.ListCreditHistoryParams{
		AccountID: accountID,
		Limit:     clampInt32(limit),
		Offset:    clampInt32(offset),
	})
	if err != nil {
		return nil, fmt.Errorf("BillingRepo - GetHistory - query: %w", err)
	}

	txns := make([]entity.CreditTransaction, 0, len(rows))

	for i := range rows {
		txns = append(txns, entity.CreditTransaction{
			TransactionID: strconv.FormatInt(rows[i].ID, 10),
			AccountID:     rows[i].AccountID,
			Amount:        entity.Money(rows[i].Amount),
			Type:          rows[i].Type,
			Description:   rows[i].Description,
			CreatedAt:     rows[i].CreatedAt,
		})
	}

	return txns, nil
}

// CreateUsageRecord persists an LLM usage record for audit and billing history.
func (r *BillingRepo) CreateUsageRecord(ctx context.Context, record *entity.UsageRecord) error {
	err := r.queries.InsertUsageRecord(ctx, sqlcgen.InsertUsageRecordParams{
		RecordID:         record.RecordID,
		RunID:            record.RunID,
		AccountID:        record.AccountID,
		ModelID:          record.ModelID,
		PromptTokens:     clampInt32(record.PromptTokens),
		CompletionTokens: clampInt32(record.CompletionTokens),
		TotalTokens:      clampInt32(record.TotalTokens),
		Cost:             float64(record.Cost),
	})
	if err != nil {
		return fmt.Errorf("BillingRepo - CreateUsageRecord - insert: %w", err)
	}

	return nil
}

// creditTransactionFromRow shapes a ledger row into the domain transaction.
// The generated row type is the one the RETURNING clause produces, so the
// mapping lives once here instead of in every scan call site the builder
// version repeated.
func creditTransactionFromRow(row *sqlcgen.CreditLedger) entity.CreditTransaction {
	return entity.CreditTransaction{
		TransactionID: strconv.FormatInt(row.ID, 10),
		AccountID:     row.AccountID,
		Amount:        entity.Money(row.Amount),
		Type:          row.Type,
		Description:   row.Description,
		CreatedAt:     row.CreatedAt,
	}
}
