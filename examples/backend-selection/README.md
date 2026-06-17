# AgentOS Backend Selection

GoAgent accepts an explicit `BackendRef` on every `RunSpec`. For product-facing services that want policy-based routing, configure ordered backend selection rules instead of hardcoding task-specific branches in application code.

Example:

```sh
export AGENTFW_BACKEND_SELECTION_RULES_JSON='[
  {
    "name": "card-template",
    "backend": {"kind": "temporal_external", "name": "kardcraft-agent-workflow"},
    "input": {"task_type": "card_template"}
  },
  {
    "name": "opencode",
    "backend": {"kind": "grpc", "name": "opencode"},
    "input": {"agent": "opencode"}
  }
]'
```

Then a caller can omit `RunSpec.Backend` only when the run matches one configured rule:

```go
status, err := rt.Start(ctx, agentos.RunSpec{
    RunID: "run-1",
    Input: map[string]any{
        "task_type": "card_template",
        "query": "generate cards from this document",
    },
})
```

If no rule matches, `Start` fails. There is no implicit native fallback.
