package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/TekkenSteve/GoAgent/config"
	eventlognats "github.com/TekkenSteve/GoAgent/pkg/eventlog/nats"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/stretchr/testify/require"
)

// ingressTestConfig builds a config the way the environment layer does, so a
// test starts from the same defaults a deployment gets.
func ingressTestConfig() *config.Config {
	cfg := &config.Config{}
	cfg.AgentFW.IngressTaskQueue = "town-b-work"
	cfg.AgentFW.IngressConsumerPrefix = "agentos-ingress"
	cfg.AgentFW.NatsShards = 16

	return cfg
}

func TestIngressConfigFromAppDisabledWithoutRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		routes string
	}{
		{name: "empty array", routes: "[]"},
		{name: "empty string", routes: ""},
		{name: "null", routes: "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := ingressTestConfig()
			cfg.AgentFW.IngressRoutesJSON = tt.routes

			ingress, err := ingressConfigFromApp(cfg)
			if err != nil {
				t.Fatalf("ingressConfigFromApp: %v", err)
			}

			if ingress.enabled() {
				t.Fatalf("ingress is enabled without routes: %#v", ingress)
			}
		})
	}
}

func TestIngressConfigFromAppReadsRoutes(t *testing.T) {
	t.Parallel()

	cfg := ingressTestConfig()
	cfg.AgentFW.IngressRoutesJSON = `[
		{"domain":"run.timeline","type":"run.requested",
		 "workflow_type":"agentfw.agent-workflow.v1","signal_name":"fact"},
		{"domain":"conversation.events","type":"message.posted",
		 "workflow_type":"agentfw.agent-workflow.v1","signal_name":"fact"}
	]`
	cfg.AgentFW.NatsShards = 4

	ingress, err := ingressConfigFromApp(cfg)
	if err != nil {
		t.Fatalf("ingressConfigFromApp: %v", err)
	}

	if !ingress.enabled() {
		t.Fatal("ingress is disabled with routes configured")
	}

	if ingress.taskQueue != "town-b-work" {
		t.Fatalf("task queue = %q", ingress.taskQueue)
	}

	if len(ingress.routes) != 2 || ingress.routes[0].Type != "run.requested" {
		t.Fatalf("routes = %#v", ingress.routes)
	}

	if len(ingress.shards) != 4 {
		t.Fatalf("shards = %#v", ingress.shards)
	}

	if ingress.consumerPrefix != "agentos-ingress" {
		t.Fatalf("consumer prefix = %q", ingress.consumerPrefix)
	}
}

// invalidIngressCase is one rejected configuration: the deployment settings
// and the error the bridge must report for them.
type invalidIngressCase struct {
	name     string
	mutate   func(*config.Config)
	expected error
}

func invalidIngressCases() []invalidIngressCase {
	return []invalidIngressCase{
		{
			name: "routes without a task queue",
			mutate: func(cfg *config.Config) {
				cfg.AgentFW.IngressTaskQueue = ""
				cfg.AgentFW.IngressRoutesJSON = `[{"domain":"run.timeline","type":"t","workflow_type":"w","signal_name":"s"}]`
			},
			expected: errIngressTaskQueueRequired,
		},
		{
			name: "route without a domain",
			mutate: func(cfg *config.Config) {
				cfg.AgentFW.IngressRoutesJSON = `[{"type":"t","workflow_type":"w","signal_name":"s"}]`
			},
			expected: errIngressRouteInvalid,
		},
		{
			name: "route without a signal",
			mutate: func(cfg *config.Config) {
				cfg.AgentFW.IngressRoutesJSON = `[{"domain":"run.timeline","type":"t","workflow_type":"w"}]`
			},
			expected: errIngressRouteInvalid,
		},
		{
			name: "route on a domain the log does not declare",
			mutate: func(cfg *config.Config) {
				cfg.AgentFW.IngressRoutesJSON = `[{"domain":"messages","type":"t","workflow_type":"w","signal_name":"s"}]`
			},
			expected: errIngressDomainUnknown,
		},
		{
			name: "routes that are not JSON",
			mutate: func(cfg *config.Config) {
				cfg.AgentFW.IngressRoutesJSON = `{`
			},
		},
		{
			name: "shard outside the domain",
			mutate: func(cfg *config.Config) {
				cfg.AgentFW.IngressRoutesJSON = `[{"domain":"run.timeline","type":"t","workflow_type":"w","signal_name":"s"}]`
				cfg.AgentFW.IngressShards = "16"
			},
			expected: errIngressShardsOutOfRange,
		},
		{
			name: "consumer prefix that is not a subject token",
			mutate: func(cfg *config.Config) {
				cfg.AgentFW.IngressRoutesJSON = `[{"domain":"run.timeline","type":"t","workflow_type":"w","signal_name":"s"}]`
				cfg.AgentFW.IngressConsumerPrefix = "AgentOS.Ingress"
			},
			expected: errIngressConsumerTokenInvalid,
		},
	}
}

func TestIngressConfigFromAppRejectsBadConfig(t *testing.T) {
	t.Parallel()

	for _, tt := range invalidIngressCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := ingressTestConfig()

			tt.mutate(cfg)

			_, err := ingressConfigFromApp(cfg)
			if tt.expected == nil {
				if err == nil {
					t.Fatal("expected an error")
				}

				return
			}

			if !errors.Is(err, tt.expected) {
				t.Fatalf("error = %v, want %v", err, tt.expected)
			}
		})
	}
}

func TestParseIngressShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		spec     string
		total    uint32
		expected []uint32
		wantErr  error
	}{
		{name: "empty owns every shard", spec: "", total: 3, expected: []uint32{0, 1, 2}},
		{name: "single shard", spec: "2", total: 4, expected: []uint32{2}},
		{name: "range", spec: "0-3", total: 8, expected: []uint32{0, 1, 2, 3}},
		{name: "list and range", spec: "5,0-1", total: 8, expected: []uint32{0, 1, 5}},
		{name: "duplicates collapse", spec: "1,1,0-0", total: 4, expected: []uint32{0, 1}},
		{name: "spaces are ignored", spec: " 0 , 2 ", total: 4, expected: []uint32{0, 2}},
		{name: "backwards range", spec: "3-1", total: 8, wantErr: errIngressShardsInvalid},
		{name: "not a number", spec: "x", total: 8, wantErr: errIngressShardsInvalid},
		{name: "empty entry", spec: "0,,1", total: 8, wantErr: errIngressShardsInvalid},
		{name: "beyond the shard count", spec: "0-8", total: 8, wantErr: errIngressShardsOutOfRange},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			shards, err := parseIngressShards(tt.spec, tt.total)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("error = %v, want %v", err, tt.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("parseIngressShards: %v", err)
			}

			if !slices.Equal(shards, tt.expected) {
				t.Fatalf("shards = %#v, want %#v", shards, tt.expected)
			}
		})
	}
}

func TestIngressConsumerName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		domain string
		shards []uint32
		total  uint32
		want   string
	}{
		{
			name:   "every shard is the default name",
			domain: "run.timeline",
			shards: []uint32{0, 1, 2, 3},
			total:  4,
			want:   "agentos-ingress-run-timeline-all",
		},
		{
			name:   "a shard group is part of the name",
			domain: "run.timeline",
			shards: []uint32{0, 1, 2, 3},
			total:  16,
			want:   "agentos-ingress-run-timeline-0-3",
		},
		{
			name:   "ranges and singles stay readable",
			domain: "plan.events",
			shards: []uint32{0, 1, 2, 5, 9, 10},
			total:  16,
			want:   "agentos-ingress-plan-events-0-2.5.9-10",
		},
		{
			name:   "an unordered spec still derives one name",
			domain: "run.timeline",
			shards: []uint32{3, 1, 2, 0},
			total:  16,
			want:   "agentos-ingress-run-timeline-0-3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ingressConsumerName("agentos-ingress", tt.domain, tt.shards, tt.total)
			if got != tt.want {
				t.Fatalf("consumer name = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGroupRoutesByDomain(t *testing.T) {
	t.Parallel()

	routes := []ingressRoute{
		{Domain: "run.timeline", Type: "b", WorkflowType: "w", SignalName: "s"},
		{Domain: "conversation.events", Type: "c", WorkflowType: "w", SignalName: "s"},
		{Domain: "run.timeline", Type: "a", WorkflowType: "w", SignalName: "s"},
	}

	grouped := groupRoutesByDomain(routes)
	if len(grouped) != 2 {
		t.Fatalf("domains = %d, want 2", len(grouped))
	}

	// Deterministic order matters: one consumer per domain is created in it.
	if grouped[0].domain != "conversation.events" || grouped[1].domain != "run.timeline" {
		t.Fatalf("domain order = %q, %q", grouped[0].domain, grouped[1].domain)
	}

	if len(grouped[1].routes) != 2 {
		t.Fatalf("run.timeline routes = %#v", grouped[1].routes)
	}

	if grouped[1].routes[0].Type != "b" || grouped[1].routes[1].Type != "a" {
		t.Fatalf("route order within a domain must follow the table: %#v", grouped[1].routes)
	}
}

func TestIngressBridgeStopIsSafeWithoutRoutes(t *testing.T) {
	t.Parallel()

	cfg := ingressTestConfig()

	bridge, err := startAgentOSIngress(t.Context(), logger.New("error"), cfg, nil)
	if err != nil {
		t.Fatalf("startAgentOSIngress: %v", err)
	}

	bridge.Stop()
	bridge.Stop()
}

func TestStartAgentOSIngressRequiresRuntimeWithRoutes(t *testing.T) {
	t.Parallel()

	cfg := ingressTestConfig()
	cfg.AgentFW.IngressRoutesJSON = `[{"domain":"run.timeline","type":"t","workflow_type":"w","signal_name":"s"}]`

	_, err := startAgentOSIngress(t.Context(), logger.New("error"), cfg, nil)
	if !errors.Is(err, errIngressRuntimeRequired) {
		t.Fatalf("error = %v, want %v", err, errIngressRuntimeRequired)
	}
}

// errTestReaderUnavailable stands in for a failure that is not "the stream is
// not provisioned yet" — the kind that must not be retried.
var errTestReaderUnavailable = errors.New("test: reader unavailable")

// TestRetryUntilStreamExistsWaitsForProvisioning locks the startup policy: a
// stream that has not been provisioned yet is waited for, anything else fails
// at once.
func TestRetryUntilStreamExistsWaitsForProvisioning(t *testing.T) {
	t.Parallel()

	t.Run("waits then succeeds", func(t *testing.T) {
		t.Parallel()

		attempts := 0
		expected := &eventlognats.Reader{}

		reader, err := retryUntilStreamExists(t.Context(), logger.New("error"), func() (*eventlognats.Reader, error) {
			attempts++

			if attempts < 3 {
				return nil, fmt.Errorf("%w: STREAM", eventlognats.ErrStreamNotFound)
			}

			return expected, nil
		})
		require.NoError(t, err)
		require.Same(t, expected, reader)
		require.Equal(t, 3, attempts)
	})

	t.Run("any other failure is immediate", func(t *testing.T) {
		t.Parallel()

		attempts := 0
		failure := errTestReaderUnavailable

		_, err := retryUntilStreamExists(t.Context(), logger.New("error"), func() (*eventlognats.Reader, error) {
			attempts++

			return nil, failure
		})
		require.ErrorIs(t, err, failure)
		require.Equal(t, 1, attempts, "a configuration failure must not be retried")
	})

	t.Run("a canceled parent stops the wait", func(t *testing.T) {
		t.Parallel()

		parent, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := retryUntilStreamExists(parent, logger.New("error"), func() (*eventlognats.Reader, error) {
			return nil, fmt.Errorf("%w: STREAM", eventlognats.ErrStreamNotFound)
		})
		require.ErrorIs(t, err, context.Canceled)
	})
}
