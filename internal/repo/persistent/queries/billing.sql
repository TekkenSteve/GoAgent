-- Prepaid credit accounts and their ledger. Money moves in whole
-- transactions: the balance changes and the ledger row is written by the same
-- repo call, with the amount's sign carrying the direction.

-- GetCreditAccount reads an account's balance. Absence is not an error: an
-- account with no row has a zero balance.
-- name: GetCreditAccount :one
SELECT account_id, balance, currency, version, updated_at
FROM credit_accounts
WHERE account_id = $1;

-- DeductCreditBalance decrements the balance under its own guard: the WHERE
-- clause is the sufficiency check, so zero affected rows means the balance
-- was too low. The version bump keeps optimistic locking honest for readers.
-- name: DeductCreditBalance :execrows
UPDATE credit_accounts
SET balance = balance - $2, version = version + 1, updated_at = NOW()
WHERE account_id = $1 AND balance >= $2;

-- UpsertCreditAccount adds credits, creating the account on first top-up.
-- name: UpsertCreditAccount :exec
INSERT INTO credit_accounts (account_id, balance, currency)
VALUES ($1, $2, 'USD')
ON CONFLICT (account_id)
DO UPDATE SET balance = credit_accounts.balance + $2,
              version = credit_accounts.version + 1,
              updated_at = NOW();

-- InsertCreditLedger records one money movement; the caller passes the amount
-- with its sign (negative for usage, positive for top-up).
-- name: InsertCreditLedger :one
INSERT INTO credit_ledger (account_id, amount, type, description)
VALUES ($1, $2, $3, $4)
RETURNING id, account_id, amount, type, description, created_at;

-- ListCreditHistory pages an account's ledger newest-first.
-- name: ListCreditHistory :many
SELECT id, account_id, amount, type, description, created_at
FROM credit_ledger
WHERE account_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- InsertUsageRecord persists one LLM usage record for audit and billing
-- history.
-- name: InsertUsageRecord :exec
INSERT INTO usage_records (record_id, run_id, account_id, model_id, prompt_tokens, completion_tokens, total_tokens, cost)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
