package persistent

// Column names shared by the Squirrel builders in this package. Centralized so
// goconst stays satisfied and a rename reflects across every query.
const (
	_colAccountID      = "account_id"
	_colProjectID      = "project_id"
	_colIDempotencyKey = "idempotency_key"
	_colPlanID         = "plan_id"
	_colProcessID      = "process_id"
	_colRunID          = "run_id"
	_colContent        = "content"
	_colToolCallID     = "tool_call_id"
)
