package v1

import (
	"context"
	"fmt"

	v1 "github.com/TekkenSteve/GoAgent/docs/proto/v1"
	"github.com/TekkenSteve/GoAgent/internal/controller/grpc/v1/response"
)

func (r *V1) GetHistory(ctx context.Context, _ *v1.GetHistoryRequest) (*v1.GetHistoryResponse, error) {
	translationHistory, err := r.t.History(ctx)
	if err != nil {
		r.l.Error(err, "grpc - v1 - GetHistory")

		return nil, fmt.Errorf("grpc - v1 - GetHistory: %w", err)
	}

	return response.NewTranslationHistory(translationHistory), nil
}
