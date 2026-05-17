package persistent

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/postgres"
	"github.com/jackc/pgx/v5"
)

// Sentinel errors.
var (
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrNegativeLimitOffset = errors.New("limit and offset must be non-negative")
)

// BillingRepo implements repo.CreditManager with Postgres.
type BillingRepo struct {
	*postgres.Postgres
}

// NewBillingRepo creates a Postgres-backed credit manager.
func NewBillingRepo(pg *postgres.Postgres) *BillingRepo {
	return &BillingRepo{pg}
}

// GetBalance returns the current credit balance for an account.
// Returns zero balance if the account has no row yet (soft-create).
func (r *BillingRepo) GetBalance(ctx context.Context, accountID string) (entity.CreditAccount, error) {
	sql, args, err := r.Builder.
		Select("account_id", "balance", "currency", "version", "updated_at").
		From("credit_accounts").
		Where(sq.Eq{"account_id": accountID}).
		ToSql()
	if err != nil {
		return entity.CreditAccount{}, fmt.Errorf("BillingRepo - GetBalance - builder: %w", err)
	}

	var (
		acct    entity.CreditAccount
		version int64
	)

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&acct.AccountID, &acct.Balance, &acct.Currency, &version, &acct.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.CreditAccount{
				AccountID: accountID,
				Balance:   0,
				Currency:  "USD",
			}, nil
		}

		return entity.CreditAccount{}, fmt.Errorf("BillingRepo - GetBalance - query: %w", err)
	}

	return acct, nil
}

// Deduct decreases the account balance by amount. Returns the transaction record.
// Returns an error if the balance is insufficient.
func (r *BillingRepo) Deduct(ctx context.Context, accountID string, amount entity.Money, description string) (entity.CreditTransaction, error) {
	amountFloat := float64(amount)

	// Optimistic-lock update: balance -= amount, version++
	tag, err := r.Pool.Exec(ctx, `
		UPDATE credit_accounts
		SET balance = balance - $2, version = version + 1, updated_at = NOW()
		WHERE account_id = $1 AND balance >= $2
	`, accountID, amountFloat)
	if err != nil {
		return entity.CreditTransaction{}, fmt.Errorf("BillingRepo - Deduct - update: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return entity.CreditTransaction{}, fmt.Errorf("BillingRepo - Deduct - %w: %s", ErrInsufficientBalance, accountID)
	}

	// Record in ledger
	var txn entity.CreditTransaction

	err = r.Pool.QueryRow(ctx, `
		INSERT INTO credit_ledger (account_id, amount, type, description)
		VALUES ($1, $2, $3, $4)
		RETURNING id, account_id, amount, type, description, created_at
	`, accountID, -amountFloat, entity.TxnUsage, description).Scan(
		&txn.TransactionID, &txn.AccountID, &txn.Amount, &txn.Type, &txn.Description, &txn.CreatedAt,
	)
	if err != nil {
		return entity.CreditTransaction{}, fmt.Errorf("BillingRepo - Deduct - insert ledger: %w", err)
	}

	return txn, nil
}

// AddCredits increases the account balance by amount. Returns the transaction record.
// Creates the account row if it does not exist (upsert).
func (r *BillingRepo) AddCredits(ctx context.Context, accountID string, amount entity.Money, description string) (entity.CreditTransaction, error) {
	amountFloat := float64(amount)

	// Upsert: insert if not exists, otherwise update balance
	_, err := r.Pool.Exec(ctx, `
		INSERT INTO credit_accounts (account_id, balance, currency)
		VALUES ($1, $2, 'USD')
		ON CONFLICT (account_id)
		DO UPDATE SET balance = credit_accounts.balance + $2,
		              version = credit_accounts.version + 1,
		              updated_at = NOW()
	`, accountID, amountFloat)
	if err != nil {
		return entity.CreditTransaction{}, fmt.Errorf("BillingRepo - AddCredits - upsert: %w", err)
	}

	// Determine transaction type
	txnType := entity.TxnPurchase
	if amount < 0 {
		txnType = entity.TxnAdjust
	}

	var txn entity.CreditTransaction

	err = r.Pool.QueryRow(ctx, `
		INSERT INTO credit_ledger (account_id, amount, type, description)
		VALUES ($1, $2, $3, $4)
		RETURNING id, account_id, amount, type, description, created_at
	`, accountID, amountFloat, txnType, description).Scan(
		&txn.TransactionID, &txn.AccountID, &txn.Amount, &txn.Type, &txn.Description, &txn.CreatedAt,
	)
	if err != nil {
		return entity.CreditTransaction{}, fmt.Errorf("BillingRepo - AddCredits - insert ledger: %w", err)
	}

	return txn, nil
}

// GetHistory retrieves credit transactions for an account, newest first.
func (r *BillingRepo) GetHistory(ctx context.Context, accountID string, limit, offset int) ([]entity.CreditTransaction, error) {
	if limit < 0 || offset < 0 {
		return nil, ErrNegativeLimitOffset
	}

	sql, args, err := r.Builder.
		Select("id", "account_id", "amount", "type", "description", "created_at").
		From("credit_ledger").
		Where(sq.Eq{"account_id": accountID}).
		OrderBy("created_at DESC").
		Limit(uint64(limit)).
		Offset(uint64(offset)).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("BillingRepo - GetHistory - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("BillingRepo - GetHistory - query: %w", err)
	}
	defer rows.Close()

	var txns []entity.CreditTransaction

	for rows.Next() {
		var txn entity.CreditTransaction
		if err := rows.Scan(
			&txn.TransactionID, &txn.AccountID, &txn.Amount, &txn.Type, &txn.Description, &txn.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("BillingRepo - GetHistory - scan: %w", err)
		}

		txns = append(txns, txn)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("BillingRepo - GetHistory - rows: %w", err)
	}

	return txns, nil
}

// CreateUsageRecord persists an LLM usage record for audit and billing history.
func (r *BillingRepo) CreateUsageRecord(ctx context.Context, record *entity.UsageRecord) error {
	_, err := r.Pool.Exec(ctx, `
		INSERT INTO usage_records (record_id, run_id, account_id, model_id, prompt_tokens, completion_tokens, total_tokens, cost)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, record.RecordID, record.RunID, record.AccountID, record.ModelID,
		record.PromptTokens, record.CompletionTokens, record.TotalTokens, float64(record.Cost))
	if err != nil {
		return fmt.Errorf("BillingRepo - CreateUsageRecord - insert: %w", err)
	}

	return nil
}
