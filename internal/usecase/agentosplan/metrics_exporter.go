package agentosplan

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// PlanMetricsExporterConfig wires the durable RunPlan metric exporter.
type PlanMetricsExporterConfig struct {
	ExporterID  string
	PlanRefs    PlanRefStore
	Plans       PlanIndex
	PlanEvents  PlanEventStore
	Checkpoints PlanMetricCheckpointStore
	Sink        PlanMetricsSink
	BatchSize   int
}

// PlanMetricsExporter projects durable PlanEvents into an idempotent metrics
// sink. It is intentionally outside Temporal workflows so replay and activity
// retries cannot double-count metrics.
type PlanMetricsExporter struct {
	exporterID  string
	planRefs    PlanRefStore
	plans       PlanIndex
	planEvents  PlanEventStore
	checkpoints PlanMetricCheckpointStore
	sink        PlanMetricsSink
	batchSize   int
}

// PlanMetricsExportResult summarizes one exporter catch-up pass.
type PlanMetricsExportResult struct {
	PlansScanned     int
	EventsScanned    int
	SamplesRecorded  int
	CheckpointsSaved int
}

// NewPlanMetricsExporter creates a metrics exporter with explicit durable
// dependencies. BatchSize must be positive; callers should size it according to
// their event history and sink throughput.
func NewPlanMetricsExporter(cfg PlanMetricsExporterConfig) (*PlanMetricsExporter, error) {
	if cfg.ExporterID == "" {
		return nil, fmt.Errorf("%w: metrics exporter id is required", agentos.ErrInvalidRunPlan)
	}
	if cfg.PlanRefs == nil {
		return nil, fmt.Errorf("%w: metrics plan ref store is required", agentos.ErrInvalidRunPlan)
	}
	if cfg.Plans == nil {
		return nil, fmt.Errorf("%w: metrics plan index is required", agentos.ErrInvalidRunPlan)
	}
	if cfg.PlanEvents == nil {
		return nil, fmt.Errorf("%w: metrics plan event store is required", agentos.ErrInvalidRunPlan)
	}
	if cfg.Checkpoints == nil {
		return nil, fmt.Errorf("%w: metrics checkpoint store is required", agentos.ErrInvalidRunPlan)
	}
	if cfg.Sink == nil {
		return nil, fmt.Errorf("%w: metrics sink is required", agentos.ErrInvalidRunPlan)
	}
	if cfg.BatchSize <= 0 {
		return nil, fmt.Errorf("%w: metrics batch size must be positive", agentos.ErrInvalidRunPlan)
	}

	return &PlanMetricsExporter{
		exporterID:  cfg.ExporterID,
		planRefs:    cfg.PlanRefs,
		plans:       cfg.Plans,
		planEvents:  cfg.PlanEvents,
		checkpoints: cfg.Checkpoints,
		sink:        cfg.Sink,
		batchSize:   cfg.BatchSize,
	}, nil
}

// Export catches up all plans selected by scope. A returned error means the
// failing plan checkpoint was not advanced beyond the last fully recorded
// sample batch.
func (e *PlanMetricsExporter) Export(ctx context.Context, scope PlanRefScope) (PlanMetricsExportResult, error) {
	if scope.Limit < 0 {
		return PlanMetricsExportResult{}, fmt.Errorf("%w: plan ref limit must be non-negative", agentos.ErrInvalidPlanScope)
	}
	refs, err := e.planRefs.ListPlanRefs(ctx, scope)
	if err != nil {
		return PlanMetricsExportResult{}, err
	}

	result := PlanMetricsExportResult{PlansScanned: len(refs)}
	for _, ref := range refs {
		planResult, err := e.exportPlan(ctx, ref)
		result.EventsScanned += planResult.EventsScanned
		result.SamplesRecorded += planResult.SamplesRecorded
		result.CheckpointsSaved += planResult.CheckpointsSaved
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

func (e *PlanMetricsExporter) exportPlan(ctx context.Context, ref agentos.PlanRef) (PlanMetricsExportResult, error) {
	if err := ValidatePlanRef(ref); err != nil {
		return PlanMetricsExportResult{}, err
	}
	spec, _, exists, err := e.plans.GetPlan(ctx, ref.PlanID)
	if err != nil {
		return PlanMetricsExportResult{}, err
	}
	if !exists {
		return PlanMetricsExportResult{}, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, ref.PlanID)
	}
	if err := ValidatePlanTenantAccess(ref, spec); err != nil {
		return PlanMetricsExportResult{}, err
	}

	checkpoint, exists, err := e.checkpoints.GetPlanMetricCheckpoint(ctx, e.exporterID, ref)
	if err != nil {
		return PlanMetricsExportResult{}, err
	}
	if !exists {
		checkpoint = PlanMetricCheckpoint{
			ExporterID: e.exporterID,
			PlanID:     ref.PlanID,
			AccountID:  ref.AccountID,
			ProjectID:  ref.ProjectID,
		}
	}
	if err := ValidatePlanMetricCheckpointRef(checkpoint, e.exporterID, ref); err != nil {
		return PlanMetricsExportResult{}, err
	}

	result := PlanMetricsExportResult{}
	for {
		events, err := e.planEvents.ListPlanEvents(ctx, agentos.PlanStreamScope{
			PlanID:        ref.PlanID,
			AccountID:     ref.AccountID,
			ProjectID:     ref.ProjectID,
			AfterSequence: checkpoint.Sequence,
		}, e.batchSize)
		if err != nil {
			return result, err
		}
		if len(events) == 0 {
			return result, nil
		}

		samples, projection, err := BuildPlanMetricSamplesFromState(ctx, spec, checkpoint.Projection, events)
		if err != nil {
			return result, err
		}
		for _, sample := range samples {
			if err := e.sink.RecordPlanMetric(ctx, sample); err != nil {
				return result, err
			}
			result.SamplesRecorded++
		}

		checkpoint.Sequence = lastMetricEventSequence(events)
		checkpoint.Projection = projection
		if err := e.checkpoints.SavePlanMetricCheckpoint(ctx, checkpoint); err != nil {
			return result, err
		}
		result.EventsScanned += len(events)
		result.CheckpointsSaved++

		if len(events) < e.batchSize {
			return result, nil
		}
	}
}

// ValidatePlanMetricCheckpointRef verifies checkpoint ownership and tenant
// scope before exporter state is trusted.
func ValidatePlanMetricCheckpointRef(checkpoint PlanMetricCheckpoint, exporterID string, ref agentos.PlanRef) error {
	if exporterID == "" {
		return fmt.Errorf("%w: metrics exporter id is required", agentos.ErrInvalidRunPlan)
	}
	if err := ValidatePlanRef(ref); err != nil {
		return err
	}
	if checkpoint.ExporterID != exporterID {
		return fmt.Errorf("%w: metrics checkpoint belongs to exporter %q", agentos.ErrInvalidRunPlan, checkpoint.ExporterID)
	}
	if checkpoint.PlanID != ref.PlanID {
		return fmt.Errorf("%w: metrics checkpoint belongs to plan %q", agentos.ErrInvalidRunPlan, checkpoint.PlanID)
	}
	if checkpoint.AccountID != ref.AccountID {
		return fmt.Errorf("%w: metrics checkpoint belongs to account %q", agentos.ErrInvalidRunPlan, checkpoint.AccountID)
	}
	if checkpoint.ProjectID != ref.ProjectID {
		return fmt.Errorf("%w: metrics checkpoint belongs to project %q", agentos.ErrInvalidRunPlan, checkpoint.ProjectID)
	}
	if checkpoint.Sequence < 0 {
		return fmt.Errorf("%w: metrics checkpoint sequence must be non-negative", agentos.ErrInvalidRunPlan)
	}

	return nil
}

func lastMetricEventSequence(events []agentos.PlanEvent) int64 {
	var sequence int64
	for _, event := range events {
		if event.Sequence > sequence {
			sequence = event.Sequence
		}
	}

	return sequence
}
