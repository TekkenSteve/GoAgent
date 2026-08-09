package runtimeops

import (
	"errors"
)

// Sentinel errors for identity validation.
var (
	ErrRunIDRequired          = errors.New("run_id is required")
	ErrWorkflowIDRequired     = errors.New("workflow_id is required")
	ErrThreadRunIDRequired    = errors.New("thread_run_id is required")
	ErrIdempotencyKeyRequired = errors.New("idempotency_key is required")
	ErrEventCannotBeNil       = errors.New("event cannot be nil")
	ErrRecordCannotBeNil      = errors.New("record cannot be nil")
)

// RunIdentity is the run identity propagation tuple.
type RunIdentity struct {
	RunID          string
	WorkflowID     string
	ThreadRunID    string
	IdempotencyKey string
}

// ValidateRunIdentity checks required run identity fields.
func ValidateRunIdentity(id RunIdentity) error {
	if id.RunID == "" {
		return ErrRunIDRequired
	}

	if id.WorkflowID == "" {
		return ErrWorkflowIDRequired
	}

	if id.ThreadRunID == "" {
		return ErrThreadRunIDRequired
	}

	if id.IdempotencyKey == "" {
		return ErrIdempotencyKeyRequired
	}

	return nil
}

// AttachIdentityToEvent injects identity into stream event payload.
func AttachIdentityToEvent(event *StreamEvent, id RunIdentity) error {
	if event == nil {
		return ErrEventCannotBeNil
	}

	if err := ValidateRunIdentity(id); err != nil {
		return err
	}

	if event.Payload == nil {
		event.Payload = map[string]any{}
	}

	event.RunID = id.RunID
	event.Payload["workflow_id"] = id.WorkflowID
	event.Payload["thread_run_id"] = id.ThreadRunID
	event.Payload["idempotency_key"] = id.IdempotencyKey

	return nil
}

// AttachIdentityToUsage injects identity into a usage record.
func AttachIdentityToUsage(record *UsageRecord, id RunIdentity) error {
	if record == nil {
		return ErrRecordCannotBeNil
	}

	if err := ValidateRunIdentity(id); err != nil {
		return err
	}

	record.RunID = id.RunID
	record.ThreadRunID = id.ThreadRunID
	record.IdempotencyKey = id.IdempotencyKey

	return nil
}
