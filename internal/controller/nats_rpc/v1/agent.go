package v1

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/controller/nats_rpc/v1/request"
	"github.com/TekkenSteve/GoAgent/internal/controller/nats_rpc/v1/response"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/nats/nats_rpc/server"
	"github.com/goccy/go-json"
	"github.com/nats-io/nats.go"
)

func (r *V1) execute() server.CallHandler {
	return func(m *nats.Msg) (any, error) {
		var req request.Execute
		if err := json.Unmarshal(m.Data, &req); err != nil {
			r.l.Error(err, "nats_rpc - V1 - execute")

			return nil, fmt.Errorf("nats_rpc - V1 - execute: %w", err)
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
			r.l.Error(err, "nats_rpc - V1 - execute")

			return nil, fmt.Errorf("nats_rpc - V1 - execute: %w", err)
		}

		return response.NewRunStatus(&status), nil
	}
}

func (r *V1) getStatus() server.CallHandler {
	return func(m *nats.Msg) (any, error) {
		runID := string(m.Data)

		status, err := r.t.GetStatus(context.Background(), runID)
		if err != nil {
			r.l.Error(err, "nats_rpc - V1 - getStatus")

			return nil, fmt.Errorf("nats_rpc - V1 - getStatus: %w", err)
		}

		return response.NewRunStatus(&status), nil
	}
}
