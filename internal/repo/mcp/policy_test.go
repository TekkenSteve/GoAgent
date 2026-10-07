package mcp

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The policy is the boundary between request-shaped server configs and the
// host. These tests state its contract: nothing launches unless the operator
// listed it, subprocesses see no secrets, PATH cannot be hijacked, and dials
// to the inside of the perimeter are refused at connect time.

func TestPolicyAuthorizeStdioCommandAllowlist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		commands   []string
		cfgCommand string
		wantErr    error
	}{
		{name: "listed command connects", commands: []string{"npx"}, cfgCommand: "npx"},
		{
			name:       "unlisted command is refused",
			commands:   []string{"npx"},
			cfgCommand: "curl",
			wantErr:    ErrMCPCommandNotAllowed,
		},
		{
			name:       "empty allowlist runs nothing",
			commands:   nil,
			cfgCommand: "npx",
			wantErr:    ErrMCPCommandNotAllowed,
		},
		{
			name:       "a path is not its command",
			commands:   []string{"sh"},
			cfgCommand: "/bin/sh",
			wantErr:    ErrMCPCommandNotAllowed,
		},
		{
			name:       "a command is not its path",
			commands:   []string{"/bin/sh"},
			cfgCommand: "sh",
			wantErr:    ErrMCPCommandNotAllowed,
		},
		{
			name:       "a parent directory is not the command",
			commands:   []string{"npx"},
			cfgCommand: "../npx",
			wantErr:    ErrMCPCommandNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			policy := NewTransportPolicy(&TransportPolicyOptions{AllowedCommands: tt.commands})

			err := policy.Authorize(&ServerConfig{Name: "srv", Transport: TransportStdio, Command: tt.cfgCommand})

			assertPolicyError(t, err, tt.wantErr)
		})
	}
}

func TestPolicyAuthorizeEnvIsOperatorScoped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		wantErr error
	}{
		{name: "an opened key is settable", env: map[string]string{"WEATHER_API_KEY": "k"}},
		{
			name:    "a closed key is refused",
			env:     map[string]string{"NODE_OPTIONS": "--require=/tmp/evil.js"},
			wantErr: ErrMCPEnvKeyNotAllowed,
		},
		{
			name:    "PATH cannot be redirected by a config",
			env:     map[string]string{"PATH": "/tmp/evildir"},
			wantErr: ErrMCPEnvKeyNotConfigurable,
		},
		{
			name:    "HOME cannot be redirected by a config",
			env:     map[string]string{"HOME": "/tmp/evilhome"},
			wantErr: ErrMCPEnvKeyNotConfigurable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			policy := NewTransportPolicy(&TransportPolicyOptions{
				AllowedCommands: []string{"npx"},
				ConfigEnvKeys:   []string{"WEATHER_API_KEY"},
			})

			err := policy.Authorize(&ServerConfig{Name: "srv", Transport: TransportStdio, Command: "npx", Env: tt.env})

			assertPolicyError(t, err, tt.wantErr)
		})
	}
}

func TestPolicyAuthorizeURLScheme(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		wantErr error
	}{
		{name: "https is a network transport", url: "https://mcp.example.com/sse"},
		{name: "http is a network transport", url: "http://mcp.example.com/sse"},
		{name: "file is not", url: "file:///etc/passwd", wantErr: ErrMCPSchemeNotAllowed},
		{name: "ftp is not", url: "ftp://mcp.example.com", wantErr: ErrMCPSchemeNotAllowed},
		{name: "gopher is not", url: "gopher://127.0.0.1:70", wantErr: ErrMCPSchemeNotAllowed},
		{name: "hostless is not", url: "http:///sse", wantErr: ErrMCPSchemeNotAllowed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			policy := NewTransportPolicy(&TransportPolicyOptions{})

			err := policy.Authorize(&ServerConfig{Name: "srv", Transport: TransportSSE, URL: tt.url})

			assertPolicyError(t, err, tt.wantErr)
		})
	}
}

// A subprocess must receive the host keys the operator listed and nothing
// else. The process environment here carries stand-ins for what a real one
// would: a database URL, an auth secret — none of which belong in a tool
// server's environment.
// Mutates the process environment, so it cannot run in parallel.
func TestPolicyBuildEnvInheritsOnlyListedKeys(t *testing.T) {
	t.Setenv("MCP_TEST_DB_URL", "postgres://secret")
	t.Setenv("MCP_TEST_AUTH_SECRET", "hmac-secret")
	t.Setenv("PATH", "/usr/bin:/bin")

	policy := NewTransportPolicy(&TransportPolicyOptions{
		AllowedCommands: []string{"npx"},
		ConfigEnvKeys:   []string{"WEATHER_API_KEY"},
	})

	cfg := &ServerConfig{
		Name:      "srv",
		Transport: TransportStdio,
		Command:   "npx",
		Env:       map[string]string{"WEATHER_API_KEY": "wk"},
	}

	if err := policy.Authorize(cfg); err != nil {
		t.Fatal(err)
	}

	env := policy.BuildEnv(cfg)
	joined := strings.Join(env, "\n")

	if !strings.Contains(joined, "PATH=") {
		t.Errorf("subprocess lost PATH: %v", env)
	}

	for _, leaked := range []string{"MCP_TEST_DB_URL", "MCP_TEST_AUTH_SECRET"} {
		if strings.Contains(joined, leaked) {
			t.Errorf("subprocess environment leaked %s", leaked)
		}
	}

	if !strings.Contains(joined, "WEATHER_API_KEY=wk") {
		t.Errorf("opened config key was dropped: %v", env)
	}
}

// The dial guard is the SSRF boundary: a request-chosen URL may reach the
// public internet or a host the operator named, and nothing else.
func TestPolicyDialRefusesThePerimeter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		address string
	}{
		{name: "loopback v4", address: "127.0.0.1:8080"},
		{name: "loopback v6", address: "[::1]:8080"},
		{name: "rfc1918 10/8", address: "10.0.0.5:8080"},
		{name: "rfc1918 172.16/12", address: "172.16.0.5:8080"},
		{name: "rfc1918 192.168/16", address: "192.168.1.5:8080"},
		{name: "link-local", address: "169.254.169.254:80"},
		{name: "carrier-grade nat", address: "100.64.0.5:8080"},
		{name: "unspecified", address: "0.0.0.0:8080"},
		{name: "unique-local v6", address: "[fd00::5]:8080"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			policy := NewTransportPolicy(&TransportPolicyOptions{})

			conn, err := guardedDial(t.Context(), t, policy, tt.address)
			if err == nil {
				if closeErr := conn.Close(); closeErr != nil {
					t.Logf("closing refused connection: %v", closeErr)
				}

				t.Fatalf("dial to %s was allowed by the policy", tt.address)
			}

			if !strings.Contains(err.Error(), "refused by transport policy") &&
				!strings.Contains(err.Error(), ErrMCPAddressRefused.Error()) {
				t.Fatalf("dial to %s failed for the wrong reason: %v", tt.address, err)
			}
		})
	}
}

// An operator-named host is trusted even when it sits inside the perimeter:
// an internal MCP server is a legitimate deployment.
func TestPolicyDialAllowsOperatorNamedHost(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	policy := NewTransportPolicy(&TransportPolicyOptions{AllowedHTTPHosts: []string{"127.0.0.1"}})

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := policy.httpClient().Do(req)
	if err != nil {
		t.Fatalf("operator-named host was refused: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from the trusted host, got %d", resp.StatusCode)
	}
}

// A public host resolves and dials through the guard. The assertion is on the
// guard's decision, not on the internet: resolve a name that cannot exist and
// require the refusal to name the policy, not the resolver.
func TestPolicyDialRefusalNamesThePolicy(t *testing.T) {
	t.Parallel()

	policy := NewTransportPolicy(&TransportPolicyOptions{})

	_, err := guardedDial(t.Context(), t, policy, "nonexistent-policy-test.invalid:80")
	if err == nil {
		t.Fatal("expected a resolution failure")
	}

	if !strings.Contains(err.Error(), ErrMCPAddressRefused.Error()) {
		t.Fatalf("refusal did not come from the policy: %v", err)
	}
}

func TestPolicyIsPublicAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ip   string
		want bool
	}{
		{ip: "8.8.8.8", want: true},
		{ip: "1.1.1.1", want: true},
		{ip: "2606:4700:4700::1111", want: true},
		{ip: "127.0.0.1", want: false},
		{ip: "10.1.2.3", want: false},
		{ip: "192.168.0.1", want: false},
		{ip: "169.254.1.1", want: false},
		{ip: "100.64.1.1", want: false},
		{ip: "100.127.255.255", want: false},
		{ip: "100.128.0.1", want: true}, // just outside RFC6598
		{ip: "0.0.0.0", want: false},
		{ip: "::1", want: false},
		{ip: "fd12::1", want: false},
		{ip: "fe80::1", want: false},
	}

	for _, tt := range tests {
		ip := net.ParseIP(tt.ip)
		if ip == nil {
			t.Fatalf("test address %q does not parse", tt.ip)
		}

		if got := isPublicAddress(ip); got != tt.want {
			t.Errorf("isPublicAddress(%s) = %v, want %v", tt.ip, got, tt.want)
		}
	}
}

// The manager holds the policy at every entry a request-shaped config can
// reach. Registration is the cheapest place to observe it.
func TestManagerRegisterServerEnforcesPolicy(t *testing.T) {
	t.Parallel()

	mgr := NewManager(NewTransportPolicy(&TransportPolicyOptions{AllowedCommands: []string{"npx"}}))

	err := mgr.RegisterServer(&ServerConfig{Name: "evil", Transport: TransportStdio, Command: "curl", Args: []string{"http://attacker.example"}})
	assertPolicyError(t, err, ErrMCPCommandNotAllowed)

	if err := mgr.RegisterServer(&ServerConfig{Name: "ok", Transport: TransportStdio, Command: "npx"}); err != nil {
		t.Fatalf("listed command refused: %v", err)
	}
}

func TestManagerNilPolicyFailsClosed(t *testing.T) {
	t.Parallel()

	mgr := NewManager(nil)

	err := mgr.RegisterServer(&ServerConfig{Name: "any", Transport: TransportStdio, Command: "npx"})
	assertPolicyError(t, err, ErrMCPCommandNotAllowed)

	_, dynamicErr := mgr.ConnectDynamic(t.Context(), &ServerConfig{Name: "any2", Transport: TransportSSE, URL: "https://mcp.example.com/sse"})
	if dynamicErr == nil {
		t.Fatal("a nil policy must not admit network transports either")
	}
}

// BuildEnv with no config keys still yields the inherited baseline, so the
// subprocess can find its executable.
// Mutates the process environment, so it cannot run in parallel.
func TestPolicyBuildEnvBaselineIsPathAndHome(t *testing.T) {
	t.Setenv("PATH", "/usr/bin:/bin")

	policy := NewTransportPolicy(&TransportPolicyOptions{AllowedCommands: []string{"npx"}})

	env := policy.BuildEnv(&ServerConfig{Name: "srv", Transport: TransportStdio, Command: "npx"})

	if len(env) == 0 {
		t.Fatal("subprocess environment is empty; it cannot even resolve PATH")
	}

	if !strings.Contains(strings.Join(env, "\n"), "PATH=") {
		t.Errorf("PATH missing from baseline: %v", env)
	}
}

// guardedDial runs the policy's dial guard directly.
func guardedDial(ctx context.Context, t *testing.T, policy *TransportPolicy, addr string) (net.Conn, error) {
	t.Helper()

	transport, ok := policy.httpClient().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("policy http client does not carry a guarded *http.Transport")
	}

	return transport.DialContext(ctx, "tcp", addr)
}

func assertPolicyError(t *testing.T, got, want error) {
	t.Helper()

	switch {
	case want == nil:
		if got != nil {
			t.Fatalf("policy refused an allowed config: %v", got)
		}
	case got == nil:
		t.Fatalf("policy allowed a forbidden config; wanted %v", want)
	case !errors.Is(got, want):
		t.Fatalf("refusal = %v, wanted %v", got, want)
	}
}
