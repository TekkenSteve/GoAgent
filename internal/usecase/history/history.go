package history

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo"
)

// UseCase -.
type UseCase struct {
	store repo.WarmStateRepo
}

// New -.
func New(r repo.WarmStateRepo) *UseCase {
	return &UseCase{store: r}
}

// ListMessages returns paginated message records for a run.
func (uc *UseCase) ListMessages(ctx context.Context, runID string, limit, offset uint64) ([]entity.MessageRecord, error) {
	records, err := uc.store.ListMessagesByRun(ctx, runID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("HistoryUseCase - ListMessages - uc.store.ListMessagesByRun: %w", err)
	}

	return records, nil
}

// ListToolResults returns paginated tool result records for a run.
func (uc *UseCase) ListToolResults(ctx context.Context, runID string, limit, offset uint64) ([]entity.ToolResultRecord, error) {
	records, err := uc.store.ListToolResultsByRun(ctx, runID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("HistoryUseCase - ListToolResults - uc.store.ListToolResultsByRun: %w", err)
	}

	return records, nil
}
