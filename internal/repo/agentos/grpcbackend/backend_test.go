package grpcbackend

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentosruntimetest "github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime/agentosruntimetest"
	"google.golang.org/grpc"
)

func TestBackendConformance(t *testing.T) {
	server := newTestServer(t)
	subscriber := &agentosruntimetest.SubscriberProbe{}
	backend, err := NewBackend(subscriber, Config{
		Name:     "grpc-test",
		Target:   server.target,
		Insecure: true,
	})
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	agentosruntimetest.RunBackendConformance(t, agentosruntimetest.BackendConformanceCase{
		Name:            "grpc",
		Backend:         backend,
		Ref:             backend.config.Ref(),
		RunID:           "run-1",
		StatusState:     "running",
		SubscriberProbe: subscriber,
	})

	if server.service.start.RunID != "run-1" ||
		server.service.signal.RunID != "run-1" ||
		server.service.signal.Type != agentos.SignalUserMessage ||
		server.service.control.Operation != agentos.ControlCancel ||
		server.service.status.RunID != "run-1" {
		t.Fatalf("unexpected grpc calls: %#v", server.service)
	}
}

func TestBackendRejectsInvalidConfig(t *testing.T) {
	_, err := NewBackend(nil, Config{})
	if err == nil {
		t.Fatal("expected error")
	}
}

type testServer struct {
	target  string
	server  *grpc.Server
	service *testAgentBackendService
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	service := &testAgentBackendService{}
	server := grpc.NewServer()
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: defaultService,
		HandlerType: (*testAgentBackendServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "StartRun", Handler: unaryHandler(func(ctx context.Context, svc *testAgentBackendService, req *startRequest) (*agentos.RunStatus, error) {
				svc.start = *req

				return &agentos.RunStatus{RunID: req.RunID, LifecycleState: "created", UpdatedAt: time.Now().UTC()}, nil
			})},
			{MethodName: "SignalRun", Handler: unaryHandler(func(ctx context.Context, svc *testAgentBackendService, req *signalRequest) (*emptyResponse, error) {
				svc.signal = *req

				return &emptyResponse{}, nil
			})},
			{MethodName: "ControlRun", Handler: unaryHandler(func(ctx context.Context, svc *testAgentBackendService, req *controlRequest) (*emptyResponse, error) {
				svc.control = *req

				return &emptyResponse{}, nil
			})},
			{MethodName: "StatusRun", Handler: unaryHandler(func(ctx context.Context, svc *testAgentBackendService, req *statusRequest) (*agentos.RunStatus, error) {
				svc.status = *req

				return &agentos.RunStatus{RunID: req.RunID, LifecycleState: "running", UpdatedAt: time.Now().UTC()}, nil
			})},
		},
	}, service)

	go func() {
		_ = server.Serve(listener)
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

func unaryHandler[Req any, Resp any](
	fn func(context.Context, *testAgentBackendService, *Req) (*Resp, error),
) grpc.MethodHandler {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		req := new(Req)
		if err := dec(req); err != nil {
			return nil, err
		}
		if interceptor == nil {
			return fn(ctx, srv.(*testAgentBackendService), req)
		}

		info := &grpc.UnaryServerInfo{
			Server:     srv,
			FullMethod: defaultService,
		}
		handler := func(ctx context.Context, request any) (any, error) {
			return fn(ctx, srv.(*testAgentBackendService), request.(*Req))
		}

		return interceptor(ctx, req, info, handler)
	}
}
