package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
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
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
)

var errMixedAdapterUnexpectedGRPCRequest = errors.New("unexpected mixed adapter grpc request")

func TestPlanWorkflowRunsThroughMixedBackendAdapters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	planID, httpNodeID, summaryArtifact := "plan-real-mixed-adapters", "http", "summary"
	adapter := newMixedBackendAdapter(t, planID, httpNodeID, summaryArtifact)

	env := newAgentOSTemporalWorkflowTestEnv()
	env.RegisterWorkflowWithOptions(PlanWorkflow, workflow.RegisterOptions{Name: PlanWorkflowName})
	env.RegisterActivityWithOptions(adapter.activities.ValidatePlanActivity, activity.RegisterOptions{Name: ValidatePlanActivityName})
	env.RegisterActivityWithOptions(adapter.activities.PersistPlanStateActivity, activity.RegisterOptions{Name: PersistPlanStateActivityName})
	env.RegisterActivityWithOptions(adapter.activities.ResolvePlanNodeInputActivity, activity.RegisterOptions{Name: ResolvePlanNodeInputActivityName})
	env.RegisterActivityWithOptions(adapter.activities.StartPlanNodeActivity, activity.RegisterOptions{Name: StartPlanNodeActivityName})
	env.RegisterActivityWithOptions(adapter.activities.StatusPlanNodeActivity, activity.RegisterOptions{Name: StatusPlanNodeActivityName})
	env.RegisterActivityWithOptions(adapter.activities.ControlPlanNodeActivity, activity.RegisterOptions{Name: ControlPlanNodeActivityName})
	env.RegisterActivityWithOptions(adapter.activities.PublishPlanArtifactsActivity, activity.RegisterOptions{Name: PublishPlanArtifactsActivityName})

	createPlanForWorkflowTest(t, adapter.planStore, &adapter.spec)
	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&adapter.spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var status agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&status))
	require.Equal(t, agentos.PlanLifecycleSucceeded, status.LifecycleState)
	require.Equal(t, []string{"run-native"}, adapter.nativeBackend.startedRunIDs())
	require.Equal(t, "agentos-external-run-temporal", adapter.temporalClient.startOptions.ID)
	require.Equal(t, "langgraph.agent.v1", adapter.temporalClient.workflow)
	require.Equal(t, []string{"run-http"}, adapter.httpServer.startedRunIDs())
	require.Equal(t, []string{"run-grpc"}, adapter.grpcServer.service.startedRunIDs())
	require.Equal(t, []string{"mixed backend artifact"}, adapter.grpcServer.service.summaryTitles())
	require.Len(t, status.Artifacts, 1)
	require.Equal(t, summaryArtifact, status.Artifacts[0].Name)

	expectedRoutes := map[string]agentos.BackendRef{
		"run-native":   adapter.nativeRef,
		"run-temporal": adapter.temporalRef,
		"run-http":     adapter.httpRef,
		"run-grpc":     adapter.grpcRef,
	}
	for runID, expectedRef := range expectedRoutes {
		ref, err := adapter.runIndex.Resolve(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, expectedRef, ref)
	}
}

type mixedBackendAdapter struct {
	nativeBackend  *mixedAdapterNativeBackend
	temporalClient *mixedAdapterTemporalClient
	temporalRef    agentos.BackendRef
	httpRef        agentos.BackendRef
	grpcRef        agentos.BackendRef
	nativeRef      agentos.BackendRef
	httpServer     *mixedAdapterHTTPServer
	grpcServer     *mixedAdapterGRPCServer
	planStore      *agentosplan.MemoryPlanStore
	activities     *PlanActivities
	spec           agentos.RunPlanSpec
	runIndex       *memory.AgentOSRunIndex
}

func newMixedBackendAdapter(t *testing.T, planID, httpNodeID, summaryArtifact string) *mixedBackendAdapter {
	t.Helper()

	nativeRef, temporalRef := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}, agentos.BackendRef{Kind: agentos.BackendKindTemporalExternal, Name: "langgraph"}
	httpRef := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "http-agent"}
	grpcRef := agentos.BackendRef{Kind: agentos.BackendKindGRPC, Name: "grpc-agent"}
	nativeBackend := &mixedAdapterNativeBackend{}
	temporalClient := &mixedAdapterTemporalClient{runID: "temporal-run-id", queryValue: &mixedAdapterEncodedStatus{status: agentos.RunStatus{RunID: "run-temporal", LifecycleState: "completed", UpdatedAt: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)}}}
	temporalConfig := temporalexternal.Config{Name: temporalRef.Name, TaskQueue: "langgraph-task-queue", WorkflowType: "langgraph.agent.v1", QueryType: "agentos_status", Signals: temporalexternal.SignalNames{Cancel: "cancel", Defaults: map[agentoscore.SignalType]string{agentoscore.SignalUserMessage: "user_input"}}}
	temporalBackend, err := temporalexternal.NewBackend(temporalClient, nil, &temporalConfig)
	require.NoError(t, err)

	artifactStore := agentosplan.NewMemoryArtifactStore()
	httpServer := newMixedAdapterHTTPServer(t, artifactStore, &mixedAdapterArtifactOutput{PlanID: planID, NodeID: httpNodeID, ArtifactName: summaryArtifact, ArtifactID: "artifact-http-summary", Payload: map[string]any{"body": map[string]any{"title": "mixed backend artifact"}}})
	httpBackend, err := httpbackend.NewBackend(httpServer.Client(), nil, httpbackend.Config{Name: httpRef.Name, Endpoint: httpServer.URL})
	require.NoError(t, err)
	grpcServer := newMixedAdapterGRPCServer(t)
	grpcBackend, err := grpcbackend.NewBackend(nil, &grpcbackend.Config{Name: grpcRef.Name, Target: grpcServer.target, Insecure: true})
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
	activities, err := NewPlanActivitiesWithStores(runtime, []agentos.Capability{{Backend: nativeRef, Name: "run"}, {Backend: temporalRef, Name: "run"}, {Backend: httpRef, Name: "run"}, {Backend: grpcRef, Name: "run"}}, planStore, nil, artifactStore)
	require.NoError(t, err)

	spec := specForMixedBackend(planID, httpNodeID, summaryArtifact, nativeRef, temporalRef, httpRef, grpcRef)

	return &mixedBackendAdapter{
		nativeBackend: nativeBackend, temporalClient: temporalClient, temporalRef: temporalRef,
		httpRef: httpRef, grpcRef: grpcRef, nativeRef: nativeRef, httpServer: httpServer,
		grpcServer: grpcServer, planStore: planStore, activities: activities, spec: spec, runIndex: runIndex,
	}
}

func specForMixedBackend(planID, httpNodeID, summaryArtifact string, nativeRef, temporalRef, httpRef, grpcRef agentos.BackendRef) agentos.RunPlanSpec {
	return agentos.RunPlanSpec{
		PlanID: planID, AccountID: "acct-" + planID, ProjectID: "proj-" + planID, IdempotencyKey: "plan-start-" + planID,
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "native", Capability: "run", Run: agentos.RunSpec{RunID: "run-native", Backend: nativeRef}},
			{NodeID: "temporal", Capability: "run", Run: agentos.RunSpec{RunID: "run-temporal", Backend: temporalRef}},
			{
				NodeID: httpNodeID, Capability: "run", Run: agentos.RunSpec{RunID: "run-http", Backend: httpRef},
				Outputs: []agentos.ArtifactSpec{{Name: summaryArtifact, Kind: agentoscore.ArtifactKindObject, Required: true}},
			},
			{NodeID: "grpc", Capability: "run", Run: agentos.RunSpec{RunID: "run-grpc", Backend: grpcRef}},
		},
		Edges: []agentos.PlanEdgeSpec{
			{EdgeID: "native-temporal", From: "native", To: "temporal", On: agentos.EdgeOnSuccess},
			{EdgeID: "temporal-http", From: "temporal", To: "http", On: agentos.EdgeOnSuccess},
			{
				EdgeID: "http-grpc", From: httpNodeID, To: "grpc", On: agentos.EdgeOnSuccess,
				InputMapping: []agentos.InputMapping{
					{Target: "summary_title", SourceNodeID: httpNodeID, SourceArtifact: summaryArtifact, SourcePath: "body.title", Required: true},
				},
			},
		},
	}
}

type routerRuntime struct {
	router *agentosruntime.Router
}

func (r routerRuntime) Start(ctx context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	return r.router.Start(ctx, spec)
}

func (r routerRuntime) StartPlanNode(ctx context.Context, planID, nodeID string, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	return r.router.StartPlanNode(ctx, planID, nodeID, spec)
}

func (r routerRuntime) Signal(ctx context.Context, runID string, signal *agentoscore.Signal) error {
	return r.router.Signal(ctx, runID, signal)
}

func (r routerRuntime) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	return r.router.Status(ctx, runID)
}

func (r routerRuntime) Control(ctx context.Context, runID string, control *agentoscore.ControlRequest) error {
	return r.router.Control(ctx, runID, control)
}

func (r routerRuntime) Subscribe(ctx context.Context, scope agentoscore.StreamScope) (agentoscore.Subscription, error) {
	return r.router.Subscribe(ctx, scope)
}

func (r routerRuntime) Close() error {
	return nil
}

type mixedAdapterNativeBackend struct {
	mu      sync.Mutex
	started []string
}

func (b *mixedAdapterNativeBackend) Start(_ context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.started = append(b.started, spec.RunID)

	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "completed"}, nil
}

func (b *mixedAdapterNativeBackend) Signal(context.Context, string, *agentoscore.Signal) error {
	return nil
}

func (b *mixedAdapterNativeBackend) Control(context.Context, string, *agentoscore.ControlRequest) error {
	return nil
}

func (b *mixedAdapterNativeBackend) Status(_ context.Context, runID string) (agentos.RunStatus, error) {
	return agentos.RunStatus{RunID: runID, LifecycleState: "completed"}, nil
}

func (b *mixedAdapterNativeBackend) Subscribe(context.Context, agentoscore.StreamScope) (agentoscore.Subscription, error) {
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

func newMixedAdapterHTTPServer(t *testing.T, artifactStore agentosplan.ArtifactStore, output *mixedAdapterArtifactOutput) *mixedAdapterHTTPServer {
	t.Helper()

	server := &mixedAdapterHTTPServer{}
	server.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleMixedAdapterHTTPRequest(t, w, r, server, artifactStore, output)
	}))
	t.Cleanup(server.Close)

	return server
}

func handleMixedAdapterHTTPRequest(t *testing.T, w http.ResponseWriter, r *http.Request, server *mixedAdapterHTTPServer, artifactStore agentosplan.ArtifactStore, output *mixedAdapterArtifactOutput) {
	t.Helper()

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/runs":
		handleMixedAdapterRunStart(t, w, r, server, artifactStore, output)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/status"):
		runID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/runs/"), "/status")
		if err := json.NewEncoder(w).Encode(agentos.RunStatus{RunID: runID, LifecycleState: "completed"}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}
	case r.Method == http.MethodPost && (strings.HasSuffix(r.URL.Path, "/signals") || strings.HasSuffix(r.URL.Path, "/control")):
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func handleMixedAdapterRunStart(t *testing.T, w http.ResponseWriter, r *http.Request, server *mixedAdapterHTTPServer, artifactStore agentosplan.ArtifactStore, output *mixedAdapterArtifactOutput) {
	t.Helper()

	var request struct {
		RunID string `json:"run_id"`
	}
	require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
	server.mu.Lock()
	server.started = append(server.started, request.RunID)
	server.mu.Unlock()

	key, err := agentosplan.ArtifactPublishIdempotencyKey(output.PlanID, output.NodeID, request.RunID, output.ArtifactName)
	require.NoError(t, err)
	ref, err := putArtifact(r.Context(), artifactStore, &agentoscore.ArtifactRef{
		ArtifactID: output.ArtifactID,
		PlanID:     output.PlanID,
		NodeID:     output.NodeID,
		RunID:      request.RunID,
		Name:       output.ArtifactName,
		Kind:       agentoscore.ArtifactKindObject,
	}, output.Payload, key)
	require.NoError(t, err)

	if err := json.NewEncoder(w).Encode(agentos.RunStatus{
		RunID:          request.RunID,
		LifecycleState: "completed",
		Artifacts:      []agentoscore.ArtifactRef{ref},
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}
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

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	service := &mixedAdapterGRPCService{}
	server := grpc.NewServer(grpcbackend.JSONServerCodecOption())
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "agentos.v1.AgentBackend",
		HandlerType: (*mixedAdapterGRPCServiceContract)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "StartRun", Handler: mixedAdapterUnaryHandler(func(_ context.Context, svc *mixedAdapterGRPCService, req map[string]any) (agentos.RunStatus, error) {
				runID, ok := req["run_id"].(string)
				_ = ok

				input, inputOK := req["input"].(map[string]any)

				var summaryTitle string

				if inputOK {
					if t, stOK := input["summary_title"].(string); stOK {
						summaryTitle = t
					}
				}

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
				runID, ok := req["run_id"].(string)
				_ = ok

				return agentos.RunStatus{RunID: runID, LifecycleState: "completed"}, nil
			})},
		},
	}, service)

	go func() {
		if err := server.Serve(listener); err != nil {
			panic(err)
		}
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
			service, ok := srv.(*mixedAdapterGRPCService)
			if !ok {
				return nil, errMixedAdapterUnexpectedGRPCRequest
			}

			typedRequest, ok := request.(map[string]any)
			if !ok {
				return nil, errMixedAdapterUnexpectedGRPCRequest
			}

			return fn(handlerCtx, service, typedRequest)
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
	workflow     any
	queryValue   converter.EncodedValue
}

func (c *mixedAdapterTemporalClient) ExecuteWorkflow(_ context.Context, options *client.StartWorkflowOptions, workflowType any, _ ...any) (client.WorkflowRun, error) {
	c.startOptions = *options
	c.workflow = workflowType

	return mixedAdapterWorkflowRun{id: options.ID, runID: c.runID}, nil
}

func (c *mixedAdapterTemporalClient) SignalWorkflow(context.Context, string, string, string, any) error {
	return nil
}

func (c *mixedAdapterTemporalClient) QueryWorkflow(context.Context, string, string, string, ...any) (converter.EncodedValue, error) {
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

func (r mixedAdapterWorkflowRun) Get(context.Context, any) error {
	return nil
}

func (r mixedAdapterWorkflowRun) GetWithOptions(context.Context, any, client.WorkflowRunGetOptions) error {
	return nil
}

type mixedAdapterEncodedStatus struct {
	status agentos.RunStatus
}

func (e *mixedAdapterEncodedStatus) HasValue() bool {
	return true
}

func (e *mixedAdapterEncodedStatus) Get(valuePtr any) error {
	status, ok := valuePtr.(*agentos.RunStatus)
	if !ok {
		return nil
	}

	*status = e.status

	return nil
}
