package tool

// ExecutionBatch is a deterministic scheduling unit. Requests in one batch can
// run in parallel while batch order stays serialized.
type ExecutionBatch struct {
	Requests []Request
}

// BuildExecutionPlan builds deterministic batches with conflict-domain
// serialization and bounded per-batch concurrency.
func BuildExecutionPlan(calls []Request, maxParallel int) []ExecutionBatch {
	if maxParallel <= 0 {
		maxParallel = 1
	}

	remaining := append([]Request(nil), calls...)

	var batches []ExecutionBatch

	for len(remaining) > 0 {
		usedDomain := map[string]struct{}{}
		batch := ExecutionBatch{Requests: make([]Request, 0, maxParallel)}
		nextRemaining := make([]Request, 0, len(remaining))

		for i := range remaining {
			if len(batch.Requests) >= maxParallel {
				nextRemaining = append(nextRemaining, remaining[i])

				continue
			}

			domain := remaining[i].ConflictDomain
			if domain == "" {
				batch.Requests = append(batch.Requests, remaining[i])

				continue
			}

			if _, exists := usedDomain[domain]; exists {
				nextRemaining = append(nextRemaining, remaining[i])

				continue
			}

			usedDomain[domain] = struct{}{}

			batch.Requests = append(batch.Requests, remaining[i])
		}

		batches = append(batches, batch)
		remaining = nextRemaining
	}

	return batches
}
