package temporal

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/grpcbackend"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/httpbackend"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/temporalexternal"
	"github.com/TekkenSteve/GoAgent/internal/repo/memory"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	agentosruntime "github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
)

func TestPlanWorkflowRunsThroughMixedBackendAdapters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const (
		planID          = "plan-real-mixed-adapters"
		httpNodeID      = "http"
		summaryArtifact = "summary"
	)
	nativeRef := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	temporalRef := agentos.BackendRef{Kind: agentos.BackendKindTemporalExternal, Name: "langgraph"}
	httpRef := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "http-agent"}
	grpcRef := agentos.BackendRef{Kind: agentos.BackendKindGRPC, Name: "grpc-agent"}

	nativeBackend := &mixedAdapterNativeBackend{}
	temporalClient := &mixedAdapterTemporalClient{
		runID: "temporal-run-id",
		queryValue: mixedAdapterEncodedStatus{status: agentos.RunStatus{
			RunID:          "run-temporal",
			LifecycleState: "completed",
			UpdatedAt:      time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
		}},
	}
	temporalBackend, err := temporalexternal.NewBackend(temporalClient, nil, temporalexternal.Config{
		Name:         temporalRef.Name,
		TaskQueue:    "langgraph-task-queue",
		WorkflowType: "langgraph.agent.v1",
		QueryType:    "agentos_status",
		Signals: temporalexternal.SignalNames{
			Cancel: "cancel",
			Defaults: map[agentos.SignalType]string{
				agentos.SignalUserMessage: "user_input",
			},
		},
	})
	require.NoError(t, err)

	artifactStore := agentosplan.NewMemoryArtifactStore()
	httpServer := newMixedAdapterHTTPServer(t, artifactStore, mixedAdapterArtifactOutput{
		PlanID:       planID,
		NodeID:       httpNodeID,
		ArtifactName: summaryArtifact,
		ArtifactID:   "artifact-http-summary",
		Payload: map[string]any{
			"body": map[string]any{"title": "mixed backend artifact"},
		},
	})
	httpBackend, err := httpbackend.NewBackend(httpServer.Client(), nil, httpbackend.Config{
		Name:     httpRef.Name,
		Endpoint: httpServer.URL,
	})
	require.NoError(t, err)

	grpcServer := newMixedAdapterGRPCServer(t)
	grpcBackend, err := grpcbackend.NewBackend(nil, grpcbackend.Config{
		Name:     grpcRef.Name,
		Target:   grpcServer.target,
		Insecure: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = grpcBackend.Close() })

	registry := agentosruntime.NewRegistry()
	require.NoError(t, registry.Register(nativeRef, nativeBackend))
	require.NoError(t, registry.Register(temporalRef, temporalBackend))
	require.NoError(t, registry.Register(httpRef, httpBackend))
	require.NoError(t, registry.Register(grpcRef, grpcBackend))
	runIndex := memory.NewAgentOSRunIndex()
	router, err := agentosruntime.NewRouter(registry, runIndex)
	require.NoError(t, err)
	runtime := routerRuntime{router: router}

	planStore := agentosplan.NewMemoryPlanStore()
	activities, err := NewPlanActivitiesWithStores(
		runtime,
		[]agentos.Capability{
			{Backend: nativeRef, Name: "run"},
			{Backend: temporalRef, Name: "run"},
			{Backend: httpRef, Name: "run"},
			{Backend: grpcRef, Name: "run"},
		},
		planStore,
		planStore,
		nil,
		artifactStore,
	)
	require.NoError(t, err)

	spec := agentos.RunPlanSpec{
		PlanID:         planID,
		AccountID:      "acct-" + planID,
		ProjectID:      "proj-" + planID,
		IdempotencyKey: "plan-start-" + planID,
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "native", Capability: "run", Run: agentos.RunSpec{RunID: "run-native", Backend: nativeRef}},
			{NodeID: "temporal", Capability: "run", Run: agentos.RunSpec{RunID: "run-temporal", Backend: temporalRef}},
			{
				NodeID:     httpNodeID,
				Capability: "run",
				Run:        agentos.RunSpec{RunID: "run-http", Backend: httpRef},
				Outputs: []agentos.ArtifactSpec{
					{Name: summaryArtifact, Kind: agentos.ArtifactKindObject, Required: true},
				},
			},
			{NodeID: "grpc", Capability: "run", Run: agentos.RunSpec{RunID: "run-grpc", Backend: grpcRef}},
		},
		Edges: []agentos.PlanEdgeSpec{
			{EdgeID: "native-temporal", From: "native", To: "temporal", On: agentos.EdgeOnSuccess},
			{EdgeID: "temporal-http", From: "temporal", To: "http", On: agentos.EdgeOnSuccess},
			{
				EdgeID: "http-grpc",
				From:   httpNodeID,
				To:     "grpc",
				On:     agentos.EdgeOnSuccess,
				InputMapping: []agentos.InputMapping{
					{
						Target:         "summary_title",
						SourceNodeID:   httpNodeID,
						SourceArtifact: summaryArtifact,
						SourcePath:     "body.title",
						Required:       true,
					},
				},
			},
		},
	}

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(PlanWorkflow, workflow.RegisterOptions{Name: PlanWorkflowName})
	env.RegisterActivityWithOptions(activities.ValidatePlanActivity, activity.RegisterOptions{Name: ValidatePlanActivityName})
	env.RegisterActivityWithOptions(activities.PersistPlanStateActivity, activity.RegisterOptions{Name: PersistPlanStateActivityName})
	env.RegisterActivityWithOptions(activities.ResolvePlanNodeInputActivity, activity.RegisterOptions{Name: ResolvePlanNodeInputActivityName})
	env.RegisterActivityWithOptions(activities.StartPlanNodeActivity, activity.RegisterOptions{Name: StartPlanNodeActivityName})
	env.RegisterActivityWithOptions(activities.StatusPlanNodeActivity, activity.RegisterOptions{Name: StatusPlanNodeActivityName})
	env.RegisterActivityWithOptions(activities.ControlPlanNodeActivity, activity.RegisterOptions{Name: ControlPlanNodeActivityName})
	env.RegisterActivityWithOptions(activities.PublishPlanArtifactsActivity, activity.RegisterOptions{Name: PublishPlanArtifactsActivityName})

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInput{Spec: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var status agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&status))
	require.Equal(t, agentos.PlanLifecycleSucceeded, status.LifecycleState)
	require.Equal(t, []string{"run-native"}, nativeBackend.startedRunIDs())
	require.Equal(t, "agentos-external-run-temporal", temporalClient.startOptions.ID)
	require.Equal(t, "langgraph.agent.v1", temporalClient.workflow)
	require.Equal(t, []string{"run-http"}, httpServer.startedRunIDs())
	require.Equal(t, []string{"run-grpc"}, grpcServer.service.startedRunIDs())
	require.Equal(t, []string{"mixed backend artifact"}, grpcServer.service.summaryTitles())
	require.Len(t, status.Artifacts, 1)
	require.Equal(t, summaryArtifact, status.Artifacts[0].Name)

	expectedRoutes := map[string]agentos.BackendRef{
		"run-native":   nativeRef,
		"run-temporal": temporalRef,
		"run-http":     httpRef,
		"run-grpc":     grpcRef,
	}
	for runID, expectedRef := range expectedRoutes {
		ref, err := runIndex.Resolve(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, expectedRef, ref)
	}
}

type routerRuntime struct {
	router *agentosruntime.Router
}

func (r routerRuntime) Start(ctx context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	return r.router.Start(ctx, spec)
}

func (r routerRuntime) StartPlanNode(ctx context.Context, planID, nodeID string, spec agentos.RunSpec) (agentos.RunStatus, error) {
	return r.router.StartPlanNode(ctx, planID, nodeID, spec)
}

func (r routerRuntime) Signal(ctx context.Context, runID string, signal agentos.Signal) error {
	return r.router.Signal(ctx, runID, signal)
}

func (r routerRuntime) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	return r.router.Status(ctx, runID)
}

func (r routerRuntime) Control(ctx context.Context, runID string, control agentos.ControlRequest) error {
	return r.router.Control(ctx, runID, control)
}

func (r routerRuntime) Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error) {
	return r.router.Subscribe(ctx, scope)
}

func (r routerRuntime) Close() error {
	return nil
}

type mixedAdapterNativeBackend struct {
	mu      sync.Mutex
	started []string
}

func (b *mixedAdapterNativeBackend) Start(_ context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.started = append(b.started, spec.RunID)

	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "completed"}, nil
}

func (b *mixedAdapterNativeBackend) Signal(context.Context, string, agentos.Signal) error {
	return nil
}

func (b *mixedAdapterNativeBackend) Control(context.Context, string, agentos.ControlRequest) error {
	return nil
}

func (b *mixedAdapterNativeBackend) Status(_ context.Context, runID string) (agentos.RunStatus, error) {
	return agentos.RunStatus{RunID: runID, LifecycleState: "completed"}, nil
}

func (b *mixedAdapterNativeBackend) Subscribe(context.Context, agentos.StreamScope) (agentos.Subscription, error) {
	return nil, nil
}

func (b *mixedAdapterNativeBackend) startedRunIDs() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]string(nil), b.started...)
}

type mixedAdapterHTTPServer struct {
	*httptest.Server
	mu      sync.Mutex
	started []string
}

type mixedAdapterArtifactOutput struct {
	PlanID       string
	NodeID       string
	ArtifactName string
	ArtifactID   string
	Payload      any
}

func newMixedAdapterHTTPServer(t *testing.T, artifactStore agentosplan.ArtifactStore, output mixedAdapterArtifactOutput) *mixedAdapterHTTPServer {
	t.Helper()
	server := &mixedAdapterHTTPServer{}
	server.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/runs":
			var request struct {
				RunID string `json:"run_id"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			server.mu.Lock()
			server.started = append(server.started, request.RunID)
			server.mu.Unlock()
			key, err := agentosplan.ArtifactPublishIdempotencyKey(output.PlanID, output.NodeID, request.RunID, output.ArtifactName)
			require.NoError(t, err)
			ref, err := artifactStore.Put(r.Context(), agentos.ArtifactRef{
				ArtifactID: output.ArtifactID,
				PlanID:     output.PlanID,
				NodeID:     output.NodeID,
				RunID:      request.RunID,
				Name:       output.ArtifactName,
				Kind:       agentos.ArtifactKindObject,
			}, output.Payload, key)
			require.NoError(t, err)
			_ = json.NewEncoder(w).Encode(agentos.RunStatus{
				RunID:          request.RunID,
				LifecycleState: "completed",
				Artifacts:      []agentos.ArtifactRef{ref},
			})
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/status"):
			runID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/runs/"), "/status")
			_ = json.NewEncoder(w).Encode(agentos.RunStatus{RunID: runID, LifecycleState: "completed"})
		case r.Method == http.MethodPost && (strings.HasSuffix(r.URL.Path, "/signals") || strings.HasSuffix(r.URL.Path, "/control")):
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	return server
}

func (s *mixedAdapterHTTPServer) startedRunIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.started...)
}

type mixedAdapterGRPCServer struct {
	target  string
	server  *grpc.Server
	service *mixedAdapterGRPCService
}

type mixedAdapterGRPCService struct {
	mu                 sync.Mutex
	started            []string
	summaryTitleInputs []string
}

type mixedAdapterGRPCServiceContract interface {
	mustEmbedMixedAdapterGRPCService()
}

func (*mixedAdapterGRPCService) mustEmbedMixedAdapterGRPCService() {}

func newMixedAdapterGRPCServer(t *testing.T) *mixedAdapterGRPCServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	service := &mixedAdapterGRPCService{}
	server := grpc.NewServer()
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "agentos.v1.AgentBackend",
		HandlerType: (*mixedAdapterGRPCServiceContract)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "StartRun", Handler: mixedAdapterUnaryHandler(func(ctx context.Context, svc *mixedAdapterGRPCService, req map[string]any) (agentos.RunStatus, error) {
				runID, _ := req["run_id"].(string)
				input, _ := req["input"].(map[string]any)
				summaryTitle, _ := input["summary_title"].(string)
				svc.mu.Lock()
				svc.started = append(svc.started, runID)
				svc.summaryTitleInputs = append(svc.summaryTitleInputs, summaryTitle)
				svc.mu.Unlock()

				return agentos.RunStatus{RunID: runID, LifecycleState: "completed"}, nil
			})},
			{MethodName: "SignalRun", Handler: mixedAdapterUnaryHandler(func(context.Context, *mixedAdapterGRPCService, map[string]any) (agentos.RunStatus, error) {
				return agentos.RunStatus{}, nil
			})},
			{MethodName: "ControlRun", Handler: mixedAdapterUnaryHandler(func(context.Context, *mixedAdapterGRPCService, map[string]any) (agentos.RunStatus, error) {
				return agentos.RunStatus{}, nil
			})},
			{MethodName: "StatusRun", Handler: mixedAdapterUnaryHandler(func(_ context.Context, _ *mixedAdapterGRPCService, req map[string]any) (agentos.RunStatus, error) {
				runID, _ := req["run_id"].(string)

				return agentos.RunStatus{RunID: runID, LifecycleState: "completed"}, nil
			})},
		},
	}, service)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	return &mixedAdapterGRPCServer{target: listener.Addr().String(), server: server, service: service}
}

func mixedAdapterUnaryHandler(
	fn func(context.Context, *mixedAdapterGRPCService, map[string]any) (agentos.RunStatus, error),
) grpc.MethodHandler {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		req := make(map[string]any)
		if err := dec(&req); err != nil {
			return nil, err
		}
		handler := func(handlerCtx context.Context, request any) (any, error) {
			return fn(handlerCtx, srv.(*mixedAdapterGRPCService), request.(map[string]any))
		}
		if interceptor == nil {
			return handler(ctx, req)
		}

		return interceptor(ctx, req, &grpc.UnaryServerInfo{Server: srv}, handler)
	}
}

func (s *mixedAdapterGRPCService) startedRunIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.started...)
}

func (s *mixedAdapterGRPCService) summaryTitles() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.summaryTitleInputs...)
}

type mixedAdapterTemporalClient struct {
	runID        string
	startOptions client.StartWorkflowOptions
	workflow     interface{}
	queryValue   converter.EncodedValue
}

func (c *mixedAdapterTemporalClient) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, workflow interface{}, _ ...interface{}) (client.WorkflowRun, error) {
	c.startOptions = options
	c.workflow = workflow

	return mixedAdapterWorkflowRun{id: options.ID, runID: c.runID}, nil
}

func (c *mixedAdapterTemporalClient) SignalWorkflow(context.Context, string, string, string, interface{}) error {
	return nil
}

func (c *mixedAdapterTemporalClient) QueryWorkflow(context.Context, string, string, string, ...interface{}) (converter.EncodedValue, error) {
	return c.queryValue, nil
}

type mixedAdapterWorkflowRun struct {
	id    string
	runID string
}

func (r mixedAdapterWorkflowRun) GetID() string {
	return r.id
}

func (r mixedAdapterWorkflowRun) GetRunID() string {
	return r.runID
}

func (r mixedAdapterWorkflowRun) Get(context.Context, interface{}) error {
	return nil
}

func (r mixedAdapterWorkflowRun) GetWithOptions(context.Context, interface{}, client.WorkflowRunGetOptions) error {
	return nil
}

type mixedAdapterEncodedStatus struct {
	status agentos.RunStatus
}

func (e mixedAdapterEncodedStatus) HasValue() bool {
	return true
}

func (e mixedAdapterEncodedStatus) Get(valuePtr interface{}) error {
	status, ok := valuePtr.(*agentos.RunStatus)
	if !ok {
		return nil
	}
	*status = e.status

	return nil
}
