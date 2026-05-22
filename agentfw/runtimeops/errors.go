package runtimeops

// ErrorCategory is the runtime taxonomy category.
type ErrorCategory string

const (
	UserError        ErrorCategory = "UserError"
	ToolError        ErrorCategory = "ToolError"
	ProviderError    ErrorCategory = "ProviderError"
	InfraError       ErrorCategory = "InfraError"
	DeterminismError ErrorCategory = "DeterminismError"
)

// ErrorPolicy defines retry/display/billing behavior by category.
type ErrorPolicy struct {
	Retryable   bool
	UserVisible bool
	Billable    bool
}

// CategorizedError is a stable error payload.
type CategorizedError struct {
	Category ErrorCategory
	Code     string
	Message  string
}

func (e CategorizedError) Error() string { return e.Code + ": " + e.Message }

// PolicyForCategory returns default category policy.
func PolicyForCategory(category ErrorCategory) ErrorPolicy {
	switch category {
	case UserError:
		return ErrorPolicy{Retryable: false, UserVisible: true, Billable: false}
	case ToolError:
		return ErrorPolicy{Retryable: false, UserVisible: true, Billable: true}
	case ProviderError:
		return ErrorPolicy{Retryable: true, UserVisible: true, Billable: false}
	case InfraError:
		return ErrorPolicy{Retryable: true, UserVisible: false, Billable: false}
	case DeterminismError:
		return ErrorPolicy{Retryable: false, UserVisible: false, Billable: false}
	default:
		return ErrorPolicy{}
	}
}
