package grpcbackend

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosruntimetest "github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime/agentosruntimetest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const Run1 = "run-1"

var errUnexpectedGRPCRequest = errors.New("unexpected grpc request type")

func TestBackendConformance(t *testing.T) {
	t.Parallel()
	server := newTestServer(t)
	subscriber := &agentosruntimetest.SubscriberProbe{}

	config := Config{
		Name:     "grpc-test",
		Target:   server.target,
		Insecure: true,
	}

	backend, err := NewBackend(subscriber, nil, &config)
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}

	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close backend: %v", err)
		}
	})

	agentosruntimetest.RunBackendConformance(t, &agentosruntimetest.BackendConformanceCase{
		Name:            "grpc",
		Backend:         backend,
		Ref:             backend.config.Ref(),
		RunID:           Run1,
		StatusState:     "running",
		SubscriberProbe: subscriber,
	})

	if server.service.start.RunID != Run1 ||
		server.service.signal.RunID != Run1 ||
		server.service.signal.Type != agentoscore.SignalUserMessage ||
		server.service.control.Operation != agentoscore.ControlCancel ||
		server.service.status.RunID != Run1 {
		t.Fatalf("unexpected grpc calls: %#v", server.service)
	}
}

func TestBackendRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	config := Config{}

	_, err := NewBackend(nil, nil, &config)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBackendRejectsNilConfig(t *testing.T) {
	t.Parallel()

	_, err := NewBackend(nil, nil, nil)
	if !errors.Is(err, agentoscore.ErrInvalidBackendRef) {
		t.Fatalf("NewBackend nil config error = %v, want ErrInvalidBackendRef", err)
	}
}

func TestBackendRejectsNilRunInputs(t *testing.T) {
	t.Parallel()

	backend := &Backend{config: Config{Name: "grpc-test"}}

	if _, err := backend.Start(context.Background(), nil); !errors.Is(err, agentoscore.ErrInvalidRunSpec) {
		t.Fatalf("Start nil error = %v, want ErrInvalidRunSpec", err)
	}

	if err := backend.Signal(context.Background(), Run1, nil); !errors.Is(err, agentoscore.ErrInvalidSignal) {
		t.Fatalf("Signal nil error = %v, want ErrInvalidSignal", err)
	}
}

type testServer struct {
	target  string
	server  *grpc.Server
	service *testAgentBackendService
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	service := &testAgentBackendService{}
	server := grpc.NewServer()
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: defaultService,
		HandlerType: (*testAgentBackendServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "StartRun", Handler: unaryHandler(func(_ context.Context, svc *testAgentBackendService, req *startRequest) (*agentos.RunStatus, error) {
				svc.start = *req

				return &agentos.RunStatus{RunID: req.RunID, LifecycleState: "created", UpdatedAt: time.Now().UTC()}, nil
			})},
			{MethodName: "SignalRun", Handler: unaryHandler(func(_ context.Context, svc *testAgentBackendService, req *signalRequest) (*emptyResponse, error) {
				svc.signal = *req

				return &emptyResponse{}, nil
			})},
			{MethodName: "ControlRun", Handler: unaryHandler(func(_ context.Context, svc *testAgentBackendService, req *controlRequest) (*emptyResponse, error) {
				svc.control = *req

				return &emptyResponse{}, nil
			})},
			{MethodName: "StatusRun", Handler: unaryHandler(func(_ context.Context, svc *testAgentBackendService, req *statusRequest) (*agentos.RunStatus, error) {
				svc.status = *req

				return &agentos.RunStatus{RunID: req.RunID, LifecycleState: "running", UpdatedAt: time.Now().UTC()}, nil
			})},
		},
	}, service)

	go func() {
		if err := server.Serve(listener); err != nil {
			panic(err)
		}
	}()

	t.Cleanup(server.Stop)

	return &testServer{
		target:  listener.Addr().String(),
		server:  server,
		service: service,
	}
}

type testAgentBackendService struct {
	start   startRequest
	signal  signalRequest
	control controlRequest
	status  statusRequest
}

type testAgentBackendServer interface {
	mustEmbedTestAgentBackendServer()
}

func (*testAgentBackendService) mustEmbedTestAgentBackendServer() {}

func unaryHandler[Req, Resp any](
	fn func(context.Context, *testAgentBackendService, *Req) (*Resp, error),
) grpc.MethodHandler {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		req := new(Req)
		if err := dec(req); err != nil {
			return nil, err
		}

		svc, ok := srv.(*testAgentBackendService)
		if !ok {
			return nil, errUnexpectedGRPCRequest
		}

		if interceptor == nil {
			return fn(ctx, svc, req)
		}

		info := &grpc.UnaryServerInfo{
			Server:     srv,
			FullMethod: defaultService,
		}
		handler := func(ctx context.Context, request any) (any, error) {
			req, ok := request.(*Req)
			if !ok {
				return nil, errUnexpectedGRPCRequest
			}

			return fn(ctx, svc, req)
		}

		return interceptor(ctx, req, info, handler)
	}
}

// TestBackendPublishesLifecycleToDataPlane verifies Start and Status forward
// their observations to the data-plane lifecycle adapter, matching the shared
// agentosruntime.LifecyclePublisher contract used by all one-shot backends.
func TestBackendPublishesLifecycleToDataPlane(t *testing.T) {
	t.Parallel()

	probe := &agentosruntimetest.LifecycleProbe{}
	server := newTestServer(t)

	backend, err := NewBackend(nil, probe, &Config{
		Name:     "grpc-lifecycle",
		Target:   server.target,
		Insecure: true,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close backend: %v", err)
		}
	})

	spec := agentos.RunSpec{RunID: Run1, ThreadID: "thread-1", AccountID: "acme", Backend: backend.config.Ref()}

	started, err := backend.Start(context.Background(), &spec)
	require.NoError(t, err)
	require.Equal(t, "created", started.LifecycleState)
	require.Equal(t, spec, probe.LastStartedSpec())
	require.Equal(t, started, probe.LastStartedStatus())

	status, err := backend.Status(context.Background(), Run1)
	require.NoError(t, err)
	require.Equal(t, "running", status.LifecycleState)
	require.Equal(t, []agentos.RunStatus{status}, probe.Statuses())
}
