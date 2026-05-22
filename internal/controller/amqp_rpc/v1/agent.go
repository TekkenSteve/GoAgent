package v1

import (
	"context"
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/internal/controller/amqp_rpc/v1/request"
	"github.com/TekkenSteve/GoAgent/internal/controller/amqp_rpc/v1/response"
	"github.com/TekkenSteve/GoAgent/pkg/rabbitmq/rmq_rpc/server"
	"github.com/goccy/go-json"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (r *V1) execute() server.CallHandler {
	return func(d *amqp.Delivery) (any, error) {
		var req request.Execute
		if err := json.Unmarshal(d.Body, &req); err != nil {
			r.l.Error(err, "amqp_rpc - V1 - execute")

			return nil, fmt.Errorf("amqp_rpc - V1 - execute: %w", err)
		}

		status, err := r.t.Execute(context.Background(), &entity.ExecuteRequest{
			RunID:            req.RunID,
			ThreadID:         req.ThreadID,
			ProjectID:        req.ProjectID,
			AccountID:        req.AccountID,
			ModelRef:         req.ModelRef,
			AgentID:          req.AgentID,
			AgentVersionID:   req.AgentVersionID,
			ToolSchemaVer:    req.ToolSchemaVer,
			UserMessage:      req.UserMessage,
			IsNewThread:      req.IsNewThread,
			BypassAdmission:  req.BypassAdmission,
			IdempotencyKey:   req.IdempotencyKey,
			EventSchemaVer:   req.EventSchemaVer,
			WorkflowVersion:  req.WorkflowVersion,
			MCPServerConfigs: req.MCPServerConfigs,
		})
		if err != nil {
			r.l.Error(err, "amqp_rpc - V1 - execute")

			return nil, fmt.Errorf("amqp_rpc - V1 - execute: %w", err)
		}

		return response.NewRunStatus(&status), nil
	}
}

var ErrMissingRunID = errors.New("amqp_rpc - v1 - missing or invalid run_id header")

func (r *V1) getStatus() server.CallHandler {
	return func(d *amqp.Delivery) (any, error) {
		runID, ok := d.Headers["run_id"].(string)
		if !ok {
			return nil, fmt.Errorf("%w", ErrMissingRunID)
		}

		status, err := r.t.GetStatus(context.Background(), runID)
		if err != nil {
			r.l.Error(err, "amqp_rpc - V1 - getStatus")

			return nil, fmt.Errorf("amqp_rpc - V1 - getStatus: %w", err)
		}

		return response.NewRunStatus(&status), nil
	}
}
