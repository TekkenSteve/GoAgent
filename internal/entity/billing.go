package entity

import "time"

// Money represents a monetary amount in USD.
// Precision is float64 for initial implementation; migrate to a decimal type
// (e.g. shopspring/decimal) if rounding accuracy becomes critical.
type Money float64

// CreditAccount represents a user's credit/prepaid balance.
type CreditAccount struct {
	AccountID string    `json:"account_id"`
	Balance   Money     `json:"balance"`
	Currency  string    `json:"currency"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CreditTransaction records a single credit movement for audit.
type CreditTransaction struct {
	TransactionID string    `json:"transaction_id"`
	AccountID     string    `json:"account_id"`
	Amount        Money     `json:"amount"`
	Type          string    `json:"type"` // usage, purchase, grant, refund, adjustment
	Description   string    `json:"description"`
	CreatedAt     time.Time `json:"created_at"`
}

// UsageRecord is the billed usage for a single LLM invocation or agent run.
type UsageRecord struct {
	RecordID         string    `json:"record_id"`
	RunID            string    `json:"run_id"`
	AccountID        string    `json:"account_id"`
	ModelID          string    `json:"model_id"`
	PromptTokens     int       `json:"prompt_tokens"`
	CompletionTokens int       `json:"completion_tokens"`
	TotalTokens      int       `json:"total_tokens"`
	Cost             Money     `json:"cost"`
	CreatedAt        time.Time `json:"created_at"`
}

// Transaction types for CreditTransaction.Type.
const (
	TxnUsage    = "usage"
	TxnPurchase = "purchase"
	TxnGrant    = "grant"
	TxnRefund   = "refund"
	TxnAdjust   = "adjustment"
)
