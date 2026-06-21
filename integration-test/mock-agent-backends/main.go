package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

const (
	defaultTemporalAddress   = "temporal:7233"
	defaultTemporalNamespace = "default"
	defaultTemporalTaskQueue = "mock-agent-backends"
	defaultHTTPAddress       = ":8080"
	defaultGRPCAddress       = ":9090"
	mockWorkflowType         = "MockAgentWorkflow"
	mockStatusQuery          = "agentos.run.status"
	mockCancelSignal         = "agentos.control.cancel"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := newRunStore()
	httpServer := &http.Server{
		Addr:              envString("HTTP_ADDR", defaultHTTPAddress),
		Handler:           newHTTPHandler(store),
		ReadHeaderTimeout: 5 * time.Second,
	}
	grpcServer, listener, err := newGRPCServer(envString("GRPC_ADDR", defaultGRPCAddress), store)
	if err != nil {
		log.Fatalf("mock backend grpc: %v", err)
	}

	temporalClient, err := client.Dial(client.Options{
		HostPort:  envString("TEMPORAL_ADDRESS", defaultTemporalAddress),
		Namespace: envString("TEMPORAL_NAMESPACE", defaultTemporalNamespace),
	})
	if err != nil {
		log.Fatalf("mock backend temporal client: %v", err)
	}
	defer temporalClient.Close()

	temporalWorker := worker.New(temporalClient, envString("TEMPORAL_TASK_QUEUE", defaultTemporalTaskQueue), worker.Options{})
	temporalWorker.RegisterWorkflowWithOptions(mockTemporalWorkflow, workflow.RegisterOptions{Name: mockWorkflowType})
	if err := temporalWorker.Start(); err != nil {
		log.Fatalf("mock backend temporal worker: %v", err)
	}
	defer temporalWorker.Stop()

	errCh := make(chan error, 2)
	go func() {
		log.Printf("mock backend http listening on %s", httpServer.Addr)
		errCh <- ignoreClosed(httpServer.ListenAndServe())
	}()
	go func() {
		log.Printf("mock backend grpc listening on %s", listener.Addr().String())
		errCh <- ignoreStopped(grpcServer.Serve(listener))
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			log.Fatalf("mock backend serve: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	grpcServer.GracefulStop()
}

func envString(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func ignoreClosed(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}

	return err
}

func ignoreStopped(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "use of closed network connection") {
		return nil
	}

	return err
}

type runStore struct {
	mu       sync.Mutex
	statuses map[string]agentos.RunStatus
}

func newRunStore() *runStore {
	return &runStore{statuses: make(map[string]agentos.RunStatus)}
}

func (s *runStore) start(spec agentos.RunSpec) agentos.RunStatus {
	status := agentos.RunStatus{
		RunID:          spec.RunID,
		LifecycleState: "completed",
		BudgetUsage:    agentos.PlanBudgetUsage{SpentCents: 1},
		UpdatedAt:      time.Now().UTC(),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[spec.RunID] = status

	return status
}

func (s *runStore) status(runID string) agentos.RunStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if status, ok := s.statuses[runID]; ok {
		return status
	}

	status := agentos.RunStatus{
		RunID:          runID,
		LifecycleState: "failed",
		Reason:         "run not found",
		UpdatedAt:      time.Now().UTC(),
	}
	s.statuses[runID] = status

	return status
}

func (s *runStore) control(runID string, op agentos.ControlOperation) {
	if op != agentos.ControlCancel {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.statuses[runID]
	status.RunID = runID
	status.LifecycleState = "canceled"
	status.Reason = "cancel requested"
	status.UpdatedAt = time.Now().UTC()
	s.statuses[runID] = status
}

func newHTTPHandler(store *runStore) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/runs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		var spec agentos.RunSpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, store.start(spec))
	})
	mux.HandleFunc("/runs/", func(w http.ResponseWriter, r *http.Request) {
		runID, action, ok := parseRunAction(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		switch {
		case r.Method == http.MethodGet && action == "status":
			writeJSON(w, store.status(runID))
		case r.Method == http.MethodPost && action == "signals":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && action == "control":
			var control agentos.ControlRequest
			if err := json.NewDecoder(r.Body).Decode(&control); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			store.control(runID, control.Operation)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	})

	return mux
}

func parseRunAction(path string) (string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 3 || parts[0] != "runs" || parts[1] == "" || parts[2] == "" {
		return "", "", false
	}

	return parts[1], parts[2], true
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (jsonCodec) Name() string {
	return "json"
}

func newGRPCServer(addr string, store *runStore) (*grpc.Server, net.Listener, error) {
	encoding.RegisterCodec(jsonCodec{})
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}

	server := grpc.NewServer(grpc.ForceServerCodec(jsonCodec{}))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "agentos.v1.AgentBackend",
		HandlerType: (*mockGRPCServiceContract)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "StartRun", Handler: grpcUnaryHandler(func(_ context.Context, svc *mockGRPCService, spec agentos.RunSpec) (agentos.RunStatus, error) {
				return svc.store.start(spec), nil
			})},
			{MethodName: "SignalRun", Handler: grpcUnaryHandler(func(context.Context, *mockGRPCService, grpcSignalRequest) (emptyResponse, error) {
				return emptyResponse{}, nil
			})},
			{MethodName: "ControlRun", Handler: grpcUnaryHandler(func(_ context.Context, svc *mockGRPCService, req grpcControlRequest) (emptyResponse, error) {
				svc.store.control(req.RunID, req.Operation)
				return emptyResponse{}, nil
			})},
			{MethodName: "StatusRun", Handler: grpcUnaryHandler(func(_ context.Context, svc *mockGRPCService, req grpcStatusRequest) (agentos.RunStatus, error) {
				return svc.store.status(req.RunID), nil
			})},
		},
	}, &mockGRPCService{store: store})

	return server, listener, nil
}

type mockGRPCService struct {
	store *runStore
}

type mockGRPCServiceContract interface {
	mustEmbedMockGRPCService()
}

func (*mockGRPCService) mustEmbedMockGRPCService() {}

type grpcSignalRequest struct {
	RunID          string             `json:"run_id"`
	Type           agentos.SignalType `json:"type"`
	IdempotencyKey string             `json:"idempotency_key,omitempty"`
	Payload        map[string]any     `json:"payload,omitempty"`
	SentAt         time.Time          `json:"sent_at"`
}

type grpcControlRequest struct {
	RunID          string                   `json:"run_id"`
	Operation      agentos.ControlOperation `json:"operation"`
	IdempotencyKey string                   `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time                `json:"requested_at"`
}

type grpcStatusRequest struct {
	RunID string `json:"run_id"`
}

type emptyResponse struct{}

func grpcUnaryHandler[Req any, Resp any](
	fn func(context.Context, *mockGRPCService, Req) (Resp, error),
) grpc.MethodHandler {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		var request Req
		if err := dec(&request); err != nil {
			return nil, err
		}
		handler := func(handlerCtx context.Context, request any) (any, error) {
			return fn(handlerCtx, srv.(*mockGRPCService), request.(Req))
		}
		if interceptor == nil {
			return handler(ctx, request)
		}

		return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: srv}, handler)
	}
}

type temporalStartInput struct {
	RunID     string `json:"run_id"`
	AccountID string `json:"account_id,omitempty"`
	ProjectID string `json:"project_id,omitempty"`
}

func mockTemporalWorkflow(ctx workflow.Context, input temporalStartInput) (agentos.RunStatus, error) {
	status := agentos.RunStatus{
		RunID:          input.RunID,
		LifecycleState: "completed",
		BudgetUsage:    agentos.PlanBudgetUsage{SpentCents: 1},
		UpdatedAt:      workflow.Now(ctx),
	}
	if err := workflow.SetQueryHandler(ctx, mockStatusQuery, func() (agentos.RunStatus, error) {
		return status, nil
	}); err != nil {
		return status, err
	}

	cancelCh := workflow.GetSignalChannel(ctx, mockCancelSignal)
	timer := workflow.NewTimer(ctx, 24*time.Hour)
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(cancelCh, func(workflow.ReceiveChannel, bool) {
		status.LifecycleState = "canceled"
		status.Reason = "cancel requested"
		status.UpdatedAt = workflow.Now(ctx)
	})
	selector.AddFuture(timer, func(workflow.Future) {})
	selector.Select(ctx)

	return status, nil
}
