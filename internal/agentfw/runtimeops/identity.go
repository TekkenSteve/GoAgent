package runtimeops

import "fmt"

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
		return fmt.Errorf("run_id is required")
	}
	if id.WorkflowID == "" {
		return fmt.Errorf("workflow_id is required")
	}
	if id.ThreadRunID == "" {
		return fmt.Errorf("thread_run_id is required")
	}
	if id.IdempotencyKey == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	return nil
}

// AttachIdentityToEvent injects identity into stream event payload.
func AttachIdentityToEvent(event *StreamEvent, id RunIdentity) error {
	if event == nil {
		return fmt.Errorf("event cannot be nil")
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
func AttachIdentityToUsage(record *UsageRecord, id RunIdentity) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}
	if err := ValidateRunIdentity(id); err != nil {
		return err
	}
	record.RunID = id.RunID
	record.ThreadRunID = id.ThreadRunID
	record.IdempotencyKey = id.IdempotencyKey
	return nil
}
