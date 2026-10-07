package mcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// TransportPolicy is the boundary between request-shaped MCP server
// configurations and this host.
//
// Every ServerConfig the Manager sees was authored outside this process — in
// an agent definition, a template, or a run request. Before the policy
// existed, such a configuration could name any executable to run as a
// subprocess, inherit the entire process environment into it, and dial any
// URL. Each of those is a capability of the host handed to request data:
// arbitrary code execution, secret disclosure, and internal-network reach
// (SSRF).
//
// The policy restores the direction of trust: the operator declares what may
// run and where connections may go, and everything else is refused.
//
//   - A stdio command must be listed verbatim (MCP_ALLOWED_COMMANDS). An
//     empty list runs nothing. Allowing a command delegates its argument
//     handling to it, so only binaries that treat their arguments as input
//     belong on the list.
//   - A subprocess inherits only the listed host environment keys
//     (MCP_INHERITED_ENV, PATH and HOME by default) — never secrets. Keys the
//     host owns, PATH above all, cannot be overridden by a server config, or
//     a config would redirect a sanctioned binary to its own.
//   - A server config may set only the env keys the operator explicitly
//     opened (MCP_CONFIG_ENV, none by default).
//   - A network transport may use http and https, and dials are verified at
//     connect time: a host is either listed (MCP_ALLOWED_HTTP_HOSTS — for
//     servers on the internal network), or every address it resolves to must
//     be public. Checking at dial time rather than at configuration time is
//     what makes DNS rebanding during the run ineffective.
type TransportPolicy struct {
	allowedCommands  map[string]struct{}
	inheritedEnvKeys map[string]struct{}
	configEnvKeys    map[string]struct{}
	allowedHTTPHosts map[string]struct{}
}

// TransportPolicyOptions states the operator's declarations. Nil TransportPolicy
// (a wiring omission) behaves like the zero options: nothing runs, nothing
// dials anywhere private.
type TransportPolicyOptions struct {
	// AllowedCommands lists the exact executables a stdio config may launch.
	AllowedCommands []string
	// InheritedEnvKeys lists host environment keys a subprocess receives.
	// Defaults to PATH and HOME.
	InheritedEnvKeys []string
	// ConfigEnvKeys lists environment keys a server config may itself set.
	// Defaults to none.
	ConfigEnvKeys []string
	// AllowedHTTPHosts lists hosts (name or address, port not included) a
	// network transport may reach regardless of address class.
	AllowedHTTPHosts []string
}

// dialTimeout bounds one connection attempt; the MCP handshake applies its
// own deadlines above it.
const dialTimeout = 10 * time.Second

// defaultInheritedEnvKeys is the subprocess baseline: enough to resolve the
// executable and the user's home, nothing that carries a secret.
func defaultInheritedEnvKeys() []string {
	return []string{"PATH", "HOME"}
}

var (
	// ErrMCPCommandNotAllowed is returned when a stdio command is not on the allowlist.
	ErrMCPCommandNotAllowed = errors.New("mcp command not allowed")
	// ErrMCPEnvKeyNotConfigurable is returned when a config tries to set an env key the host owns.
	ErrMCPEnvKeyNotConfigurable = errors.New("mcp env key is inherited from the host and cannot be set by a server config")
	// ErrMCPEnvKeyNotAllowed is returned when a config sets an env key the operator did not open.
	ErrMCPEnvKeyNotAllowed = errors.New("mcp env key not allowed")
	// ErrMCPSchemeNotAllowed is returned when a network transport URL is not http(s).
	ErrMCPSchemeNotAllowed = errors.New("mcp url scheme not allowed")
	// ErrMCPAddressRefused is returned when a dial would reach an address the policy forbids.
	ErrMCPAddressRefused = errors.New("mcp address refused by transport policy")
)

// NewTransportPolicy builds a policy from the operator's declarations.
func NewTransportPolicy(opts *TransportPolicyOptions) *TransportPolicy {
	if opts == nil {
		opts = &TransportPolicyOptions{}
	}

	inherited := opts.InheritedEnvKeys
	if inherited == nil {
		inherited = defaultInheritedEnvKeys()
	}

	return &TransportPolicy{
		allowedCommands:  keySet(opts.AllowedCommands),
		inheritedEnvKeys: keySet(inherited),
		configEnvKeys:    keySet(opts.ConfigEnvKeys),
		allowedHTTPHosts: keySet(opts.AllowedHTTPHosts),
	}
}

// Authorize decides whether a server configuration may be connected at all.
// Structural validity (Validate) is separate: shape first, privilege here.
func (p *TransportPolicy) Authorize(cfg *ServerConfig) error {
	switch cfg.Transport {
	case TransportStdio:
		if _, ok := p.allowedCommands[cfg.Command]; !ok {
			return fmt.Errorf("%w: %q for server %q", ErrMCPCommandNotAllowed, cfg.Command, cfg.Name)
		}
	case TransportSSE, TransportStreamableHTTP:
		host, scheme, err := urlHostAndScheme(cfg.URL)
		if err != nil {
			return err
		}

		if scheme != "http" && scheme != "https" {
			return fmt.Errorf("%w: %q for server %q", ErrMCPSchemeNotAllowed, scheme, cfg.Name)
		}

		if host == "" {
			return fmt.Errorf("%w: url has no host, server %q", ErrMCPSchemeNotAllowed, cfg.Name)
		}
	default:
		// Validate already rejects unknown transports; reaching here means the
		// caller skipped it, so refuse rather than guess.
		return fmt.Errorf("%w: %q", ErrMCPUnsupportedTransport, cfg.Transport)
	}

	return p.authorizeEnv(cfg)
}

// BuildEnv assembles the subprocess environment: the host keys the operator
// listed, then the keys this config is allowed to set. Authorize must have
// accepted the config; BuildEnv filters rather than fails so a nil check can
// never reintroduce the whole environment.
func (p *TransportPolicy) BuildEnv(cfg *ServerConfig) []string {
	env := make([]string, 0, len(p.inheritedEnvKeys)+len(cfg.Env))

	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, ok := p.inheritedEnvKeys[key]; ok {
			env = append(env, entry)
		}
	}

	for key, value := range cfg.Env {
		if _, ok := p.configEnvKeys[key]; ok {
			env = append(env, key+"="+value)
		}
	}

	return env
}

// httpClient is shared by both network transports: connections are verified
// by the policy at dial time, and no ambient proxy is honored (egress goes
// where the policy says, not where the environment points).
func (p *TransportPolicy) httpClient() *http.Client {
	dialer := &net.Dialer{Timeout: dialTimeout}

	// No overall client timeout: both transports hold long-lived
	// streams, and a client-level deadline would cut them mid-session.
	return &http.Client{
		Transport: &http.Transport{
			DialContext: p.dialVerified(dialer),
		},
	}
}

// dialVerified resolves the host, requires every address to satisfy the policy,
// and dials the verified address — resolution and dial cannot be swapped for a
// different answer in between.
func (p *TransportPolicy) dialVerified(base *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("%w: %q: %w", ErrMCPAddressRefused, addr, err)
		}

		if p.hostAllowed(host) {
			return base.DialContext(ctx, network, addr)
		}

		resolved, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("%w: resolve %q: %w", ErrMCPAddressRefused, host, err)
		}

		if len(resolved) == 0 {
			return nil, fmt.Errorf("%w: %q resolves to nothing", ErrMCPAddressRefused, host)
		}

		for _, ip := range resolved {
			if !isPublicAddress(ip.IP) {
				return nil, fmt.Errorf("%w: %q resolves to %s, which is not a public address; list the host in MCP_ALLOWED_HTTP_HOSTS to trust it",
					ErrMCPAddressRefused, host, ip.IP)
			}
		}

		return base.DialContext(ctx, network, net.JoinHostPort(resolved[0].IP.String(), port))
	}
}

func (p *TransportPolicy) hostAllowed(host string) bool {
	for allowed := range p.allowedHTTPHosts {
		if strings.EqualFold(allowed, host) {
			return true
		}
	}

	return false
}

func (p *TransportPolicy) authorizeEnv(cfg *ServerConfig) error {
	for key := range cfg.Env {
		if _, inherited := p.inheritedEnvKeys[key]; inherited {
			return fmt.Errorf("%w: %q for server %q", ErrMCPEnvKeyNotConfigurable, key, cfg.Name)
		}

		if _, ok := p.configEnvKeys[key]; !ok {
			return fmt.Errorf("%w: %q for server %q", ErrMCPEnvKeyNotAllowed, key, cfg.Name)
		}
	}

	return nil
}

// isPublicAddress reports whether an address is reachable from the public
// internet. Loopback, RFC1918 and ULA privates, link-local, multicast,
// unspecified, and RFC6598 carrier-grade space are all inside the perimeter.
func isPublicAddress(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}

	if v4 := ip.To4(); v4 != nil && v4[0] == 100 && v4[1]&0xC0 == 64 {
		return false
	}

	return ip.IsGlobalUnicast()
}

func urlHostAndScheme(raw string) (host, scheme string, err error) {
	u, parseErr := url.Parse(raw)
	if parseErr != nil {
		return "", "", fmt.Errorf("%w: %q: %w", ErrMCPSchemeNotAllowed, raw, parseErr)
	}

	return u.Hostname(), u.Scheme, nil
}

func keySet(keys []string) map[string]struct{} {
	set := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		set[key] = struct{}{}
	}

	return set
}
