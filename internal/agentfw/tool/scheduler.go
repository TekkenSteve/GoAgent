package tool

// ExecutionBatch is a deterministic scheduling unit. Requests in one batch can
// run in parallel while batch order stays serialized.
type ExecutionBatch struct {
	Requests []ToolRequest
}

// BuildExecutionPlan builds deterministic batches with conflict-domain
// serialization and bounded per-batch concurrency.
func BuildExecutionPlan(calls []ToolRequest, maxParallel int) []ExecutionBatch {
	if maxParallel <= 0 {
		maxParallel = 1
	}

	remaining := append([]ToolRequest(nil), calls...)
	var batches []ExecutionBatch

	for len(remaining) > 0 {
		usedDomain := map[string]struct{}{}
		batch := ExecutionBatch{Requests: make([]ToolRequest, 0, maxParallel)}
		nextRemaining := make([]ToolRequest, 0, len(remaining))

		for _, call := range remaining {
			if len(batch.Requests) >= maxParallel {
				nextRemaining = append(nextRemaining, call)
				continue
			}

			domain := call.ConflictDomain
			if domain == "" {
				batch.Requests = append(batch.Requests, call)
				continue
			}

			if _, exists := usedDomain[domain]; exists {
				nextRemaining = append(nextRemaining, call)
				continue
			}

			usedDomain[domain] = struct{}{}
			batch.Requests = append(batch.Requests, call)
		}

		batches = append(batches, batch)
		remaining = nextRemaining
	}

	return batches
}
