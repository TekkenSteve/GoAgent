package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

type planCommandReconciler struct {
	temporalClient planTemporalClient
	taskQueues     *TaskQueues
	commandStore   agentosplan.PlanCommandStore
	auditStore     agentosplan.AuditStore
	planIndex      agentosplan.PlanIndex
}

func newPlanCommandReconciler(
	temporalClient planTemporalClient,
	taskQueues *TaskQueues,
	commandStore agentosplan.PlanCommandStore,
	auditStore agentosplan.AuditStore,
	planIndex agentosplan.PlanIndex,
) *planCommandReconciler {
	return &planCommandReconciler{
		temporalClient: temporalClient,
		taskQueues:     taskQueues,
		commandStore:   commandStore,
		auditStore:     auditStore,
		planIndex:      planIndex,
	}
}

func (r *planCommandReconciler) Recover(ctx context.Context, limit int) (PlanCommandRecoveryResult, error) {
	if r.temporalClient == nil {
		return PlanCommandRecoveryResult{}, errPlanRuntimeTemporalClientNotConfigured
	}

	if r.commandStore == nil {
		return PlanCommandRecoveryResult{}, errPlanRuntimeCommandStoreRequired
	}

	if r.auditStore == nil {
		return PlanCommandRecoveryResult{}, errPlanRuntimeAuditStoreRequired
	}

	if r.planIndex == nil {
		return PlanCommandRecoveryResult{}, errPlanRuntimePlanIndexRequired
	}

	scope := agentosplan.PlanCommandScope{Limit: limit}

	commands, err := r.commandStore.ListRecoverablePlanCommands(ctx, &scope)
	if err != nil {
		return PlanCommandRecoveryResult{}, err
	}

	result := PlanCommandRecoveryResult{Scanned: len(commands)}

	var errs []error

	for i := range commands {
		command := commands[i]
		if err := r.deliver(ctx, &command); err != nil {
			result.Failed++

			errs = append(errs, err)

			continue
		}

		result.Delivered++
	}

	return result, errors.Join(errs...)
}

func (r *planCommandReconciler) deliver(ctx context.Context, command *agentosplan.PlanCommandRecord) error {
	if command.Status == agentosplan.PlanCommandDelivered {
		return nil
	}

	if command.Action == agentosplan.AuditActionPlanStart {
		return r.deliverPlanStart(ctx, command)
	}

	signalName, payload, auditRecord, err := commandDeliveryPayload(command)
	if err != nil {
		if _, markErr := r.commandStore.MarkPlanCommandFailed(ctx, agentosplan.PlanCommandRefFromRecord(command), err.Error()); markErr != nil {
			return errors.Join(err, markErr)
		}

		return err
	}

	if err := r.temporalClient.SignalWorkflow(ctx, planWorkflowID(command.PlanID), "", signalName, payload); err != nil {
		_, markErr := r.commandStore.MarkPlanCommandFailed(ctx, agentosplan.PlanCommandRefFromRecord(command), err.Error())

		return errors.Join(fmt.Errorf("agentos temporal plan command reconciler - signal workflow: %w", err), markErr)
	}

	if _, _, err := r.auditStore.RecordAudit(ctx, &auditRecord); err != nil {
		return err
	}

	if _, err := r.commandStore.MarkPlanCommandDelivered(ctx, agentosplan.PlanCommandRefFromRecord(command)); err != nil {
		return err
	}

	return nil
}

func (r *planCommandReconciler) deliverPlanStart(ctx context.Context, command *agentosplan.PlanCommandRecord) error {
	spec, _, exists, err := r.planIndex.GetPlanByRef(ctx, planRefFromCommand(command))
	if err != nil {
		return r.markCommandFailed(ctx, command, err)
	}

	if !exists {
		return r.markCommandFailed(ctx, command, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, command.PlanID))
	}

	if command.IdempotencyKey != spec.IdempotencyKey {
		err := fmt.Errorf("%w: plan.start command idempotency key does not match plan start request", agentos.ErrInvalidRunPlan)

		return r.markCommandFailed(ctx, command, err)
	}

	auditRecord := planStartAuditRecord(&spec)
	if err := executePlanWorkflow(ctx, r.temporalClient, r.taskQueues.PlanControl, r.taskQueues, &spec); err != nil {
		return r.markCommandFailed(ctx, command, err)
	}

	if _, _, err := r.auditStore.RecordAudit(ctx, auditRecord); err != nil {
		return err
	}

	if _, err := r.commandStore.MarkPlanCommandDelivered(ctx, agentosplan.PlanCommandRefFromRecord(command)); err != nil {
		return err
	}

	return nil
}

func (r *planCommandReconciler) markCommandFailed(ctx context.Context, command *agentosplan.PlanCommandRecord, cause error) error {
	_, markErr := r.commandStore.MarkPlanCommandFailed(ctx, agentosplan.PlanCommandRefFromRecord(command), cause.Error())

	return errors.Join(cause, markErr)
}

func commandDeliveryPayload(command *agentosplan.PlanCommandRecord) (
	signalName string,
	payload any,
	audit agentosplan.AuditRecord,
	err error,
) {
	switch command.Action {
	case agentosplan.AuditActionPlanSignal:
		signal, err := signalFromPlanCommand(command)
		if err != nil {
			return "", nil, agentosplan.AuditRecord{}, err
		}

		audit := planSignalAuditRecord(planRefFromCommand(command), &signal)

		return PlanSignalName, signal, *audit, nil
	case agentosplan.AuditActionPlanControl:
		control, err := controlFromPlanCommand(command)
		if err != nil {
			return "", nil, agentosplan.AuditRecord{}, err
		}

		audit := planControlAuditRecord(planRefFromCommand(command), &control)

		return PlanControlSignalName, control, *audit, nil
	case agentosplan.AuditActionPlanStart:
		return "", nil, agentosplan.AuditRecord{}, fmt.Errorf("%w: plan start is handled by deliverPlanStart", agentos.ErrInvalidRunPlan)
	default:
		return "", nil, agentosplan.AuditRecord{}, fmt.Errorf("%w: unsupported plan command action %q", agentos.ErrInvalidRunPlan, command.Action)
	}
}

func signalFromPlanCommand(command *agentosplan.PlanCommandRecord) (agentos.Signal, error) {
	signalType, err := signalTypePayload(command.Payload, planCommandPayloadSignalType)
	if err != nil {
		return agentos.Signal{}, err
	}

	payload, err := mapPayload(command.Payload, planCommandPayloadPayload)
	if err != nil {
		return agentos.Signal{}, err
	}

	sentAt, err := optionalTimePayload(command.Payload, planCommandPayloadSentAt)
	if err != nil {
		return agentos.Signal{}, err
	}

	signal := agentos.Signal{
		Type:           signalType,
		IdempotencyKey: command.IdempotencyKey,
		ActorID:        command.ActorID,
		Payload:        payload,
		SentAt:         sentAt,
	}
	if err := agentosplan.ValidatePlanSignal(&signal); err != nil {
		return agentos.Signal{}, err
	}

	return signal, nil
}

func controlFromPlanCommand(command *agentosplan.PlanCommandRecord) (agentos.ControlRequest, error) {
	operation, err := controlOperationPayload(command.Payload, planCommandPayloadOperation)
	if err != nil {
		return agentos.ControlRequest{}, err
	}

	metadata, err := stringMapPayload(command.Payload, planCommandPayloadMetadata)
	if err != nil {
		return agentos.ControlRequest{}, err
	}

	requestedAt, err := optionalTimePayload(command.Payload, planCommandPayloadRequestedAt)
	if err != nil {
		return agentos.ControlRequest{}, err
	}

	control := agentos.ControlRequest{
		Operation:      operation,
		IdempotencyKey: command.IdempotencyKey,
		RequestedAt:    requestedAt,
		ActorID:        command.ActorID,
		Metadata:       metadata,
	}
	if err := agentos.ValidateControlRequest(&control); err != nil {
		return agentos.ControlRequest{}, err
	}

	if control.ActorID == "" {
		return agentos.ControlRequest{}, fmt.Errorf("%w: control actor id is required", agentos.ErrInvalidControlOperation)
	}

	return control, nil
}

func signalTypePayload(payload map[string]any, key string) (agentos.SignalType, error) {
	value, exists := payload[key]
	if !exists {
		return "", fmt.Errorf("%w: command payload.%s is required", agentos.ErrInvalidRunPlan, key)
	}

	switch typed := value.(type) {
	case agentos.SignalType:
		return typed, nil
	case string:
		return agentos.SignalType(typed), nil
	default:
		return "", fmt.Errorf("%w: command payload.%s must be a string", agentos.ErrInvalidRunPlan, key)
	}
}

func controlOperationPayload(payload map[string]any, key string) (agentos.ControlOperation, error) {
	value, exists := payload[key]
	if !exists {
		return "", fmt.Errorf("%w: command payload.%s is required", agentos.ErrInvalidRunPlan, key)
	}

	switch typed := value.(type) {
	case agentos.ControlOperation:
		return typed, nil
	case string:
		return agentos.ControlOperation(typed), nil
	default:
		return "", fmt.Errorf("%w: command payload.%s must be a string", agentos.ErrInvalidRunPlan, key)
	}
}

func mapPayload(payload map[string]any, key string) (map[string]any, error) {
	value, exists := payload[key]
	if !exists || value == nil {
		return map[string]any{}, nil
	}

	switch typed := value.(type) {
	case map[string]any:
		return typed, nil
	case map[string]string:
		converted := make(map[string]any, len(typed))
		for key, value := range typed {
			converted[key] = value
		}

		return converted, nil
	default:
		return nil, fmt.Errorf("%w: command payload.%s must be an object", agentos.ErrInvalidRunPlan, key)
	}
}

func stringMapPayload(payload map[string]any, key string) (map[string]string, error) {
	value, exists := payload[key]
	if !exists || value == nil {
		return nil, nil
	}

	switch typed := value.(type) {
	case map[string]string:
		return typed, nil
	case map[string]any:
		converted := make(map[string]string, len(typed))
		for key, value := range typed {
			stringValue, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("%w: command payload.%s.%s must be a string", agentos.ErrInvalidRunPlan, planCommandPayloadMetadata, key)
			}

			converted[key] = stringValue
		}

		return converted, nil
	default:
		return nil, fmt.Errorf("%w: command payload.%s must be an object", agentos.ErrInvalidRunPlan, key)
	}
}

func optionalTimePayload(payload map[string]any, key string) (time.Time, error) {
	value, exists := payload[key]
	if !exists || value == nil {
		return time.Time{}, nil
	}

	switch typed := value.(type) {
	case time.Time:
		return typed, nil
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: command payload.%s must be RFC3339 time", agentos.ErrInvalidRunPlan, key)
		}

		return parsed, nil
	default:
		return time.Time{}, fmt.Errorf("%w: command payload.%s must be RFC3339 time", agentos.ErrInvalidRunPlan, key)
	}
}

func planRefFromCommand(command *agentosplan.PlanCommandRecord) agentos.PlanRef {
	return agentos.PlanRef{
		PlanID:    command.PlanID,
		AccountID: command.AccountID,
		ProjectID: command.ProjectID,
	}
}

func planControlAuditRecord(ref agentos.PlanRef, control *agentos.ControlRequest) *agentosplan.AuditRecord {
	payload := map[string]any{
		planCommandPayloadOperation: control.Operation,
		planCommandPayloadMetadata:  control.Metadata,
	}
	if !control.RequestedAt.IsZero() {
		payload[planCommandPayloadRequestedAt] = control.RequestedAt
	}

	return &agentosplan.AuditRecord{
		PlanID:         ref.PlanID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		ActorID:        control.ActorID,
		Action:         agentosplan.AuditActionPlanControl,
		IdempotencyKey: control.IdempotencyKey,
		Payload:        payload,
	}
}
