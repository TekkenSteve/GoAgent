# AgentOS gRPC Backend

GoAgent can route runs to an external gRPC agent backend with the same control-plane model as HTTP backends:

```text
agentos.Runtime
  -> grpc backend
      -> external runtime service
  <- event ingest through POST /v1/agentos/runs/{run_id}/events
```

The built-in adapter uses JSON-encoded unary gRPC calls. The default service and methods are:

```text
/agentos.v1.AgentBackend/StartRun
/agentos.v1.AgentBackend/SignalRun
/agentos.v1.AgentBackend/ControlRun
/agentos.v1.AgentBackend/StatusRun
```

Configure it with:

```sh
export AGENTFW_GRPC_BACKENDS_JSON='[
  {
    "name": "opencode",
    "target": "opencode-runtime:9090",
    "insecure": true,
    "service": "agentos.v1.AgentBackend"
  }
]'
```

Start a run by selecting the backend explicitly:

```go
status, err := rt.Start(ctx, agentos.RunSpec{
    RunID: "run-1",
    Backend: agentos.BackendRef{
        Kind: agentos.BackendKindGRPC,
        Name: "opencode",
    },
    Input: map[string]any{
        "prompt": "implement the requested change",
    },
})
```

`Signal`, `Control`, `Status`, and `Subscribe` continue through the AgentOS runtime. The external backend should write normalized events back through the AgentOS event ingest endpoint.
