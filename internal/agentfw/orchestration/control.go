package orchestration

import "fmt"

// DomainErrorCode represents deterministic domain error categories.
type DomainErrorCode string

const (
	ErrCodeInvalidControlOperation DomainErrorCode = "invalid_control_operation"
)

// DomainError is returned for deterministic control-plane validation failures.
type DomainError struct {
	Code    DomainErrorCode
	Message string
}

func (e *DomainError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// ValidateControlOperation checks whether a control operation is valid for the
// current lifecycle state.
func ValidateControlOperation(lifecycle string, op ControlOperation) error {
	switch op {
	case ControlPause:
		if lifecycle == "running" || lifecycle == "resumed" {
			return nil
		}
	case ControlResume:
		if lifecycle == "paused" {
			return nil
		}
	case ControlCancel:
		if lifecycle == "created" || lifecycle == "running" || lifecycle == "resumed" || lifecycle == "paused" {
			return nil
		}
	default:
		return &DomainError{
			Code:    ErrCodeInvalidControlOperation,
			Message: fmt.Sprintf("unsupported control operation: %q", op),
		}
	}

	return &DomainError{
		Code:    ErrCodeInvalidControlOperation,
		Message: fmt.Sprintf("operation %q is not allowed while run is %q", op, lifecycle),
	}
}
