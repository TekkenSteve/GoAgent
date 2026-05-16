package v1

import (
	"context"
	"time"

	v1 "github.com/TekkenSteve/GoAgent/docs/proto/v1"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// V1 implements the AgentService gRPC server.
type V1 struct {
	v1.UnimplementedAgentServiceServer

	t usecase.AgentExecutor
	l logger.Interface
}

// Execute starts a new agent workflow run.
func (s *V1) Execute(ctx context.Context, req *v1.ExecuteRequest) (*v1.RunStatus, error) {
	statusResp, err := s.t.Execute(ctx, entity.ExecuteRequest{
		RunID:           req.GetRunId(),
		AccountID:       req.GetAccountId(),
		AgentID:         req.GetAgentId(),
		UserMessage:     req.GetUserMessage(),
		ModelRef:        req.GetModelRef(),
		SystemPrompt:    req.GetSystemPrompt(),
		ThreadID:        req.GetThreadId(),
		ProjectID:       req.GetProjectId(),
		BypassAdmission: req.GetBypassAdmission(),
		IdempotencyKey:  req.GetIdempotencyKey(),
	})
	if err != nil {
		s.l.Error(err, "grpc - v1 - Execute")
		return nil, status.Error(codes.Internal, "execution failed")
	}

	return &v1.RunStatus{
		RunId:          statusResp.RunID,
		LifecycleState: statusResp.LifecycleState,
		Step:           statusResp.Step,
		Reason:         statusResp.Reason,
		UpdatedAt:      statusResp.UpdatedAt.Unix(),
	}, nil
}

// GetStatus queries the current status of an agent run.
func (s *V1) GetStatus(ctx context.Context, req *v1.GetStatusRequest) (*v1.RunStatus, error) {
	statusResp, err := s.t.GetStatus(ctx, req.GetRunId())
	if err != nil {
		s.l.Error(err, "grpc - v1 - GetStatus")
		return nil, status.Error(codes.Internal, "query failed")
	}

	return &v1.RunStatus{
		RunId:          statusResp.RunID,
		LifecycleState: statusResp.LifecycleState,
		Step:           statusResp.Step,
		Reason:         statusResp.Reason,
		UpdatedAt:      time.Now().Unix(),
	}, nil
}
