package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/TekkenSteve/GoAgent/config"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/temporalrouter"
	"github.com/TekkenSteve/GoAgent/pkg/eventlog"
	eventlognats "github.com/TekkenSteve/GoAgent/pkg/eventlog/nats"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

var (
	// errIngressRouteInvalid reports a route that cannot be delivered.
	errIngressRouteInvalid = errors.New("agentos ingress: route needs a domain, a type, a workflow type and a signal name")
	// errIngressDomainUnknown reports a route on a domain the fact log does
	// not declare: nothing publishes there, so the consumer would never fire.
	errIngressDomainUnknown = errors.New("agentos ingress: route names a domain the fact log does not declare")
	// errIngressTaskQueueRequired reports routes without a queue to deliver to.
	errIngressTaskQueueRequired = errors.New("agentos ingress: routes require AGENTFW_INGRESS_TASK_QUEUE")
	// errIngressShardsInvalid reports a shard specification that cannot be read.
	errIngressShardsInvalid = errors.New("agentos ingress: shards")
	// errIngressShardsOutOfRange reports a shard the domain does not have.
	errIngressShardsOutOfRange = errors.New("agentos ingress: shard is outside AGENTFW_NATS_SHARDS")
	// errIngressRuntimeRequired reports routes without a Temporal to call.
	errIngressRuntimeRequired = errors.New("agentos ingress: routes are configured but there is no Temporal runtime to deliver into")
	// errIngressConsumerTokenInvalid reports a consumer name that would not be
	// a safe single subject token.
	errIngressConsumerTokenInvalid = errors.New("agentos ingress: consumer name must be lowercase letters, digits and dashes")
)

const (
	// _ingressAllShards labels a consumer that owns every shard. It is the name
	// every deployment starts with, so it stays stable across a scale-out only
	// when the owned set does not change.
	_ingressAllShards = "all"
	// _ingressShardRangeSeparator joins the ends of a shard range.
	_ingressShardRangeSeparator = "-"
	// _ingressShardListSeparator separates shards and ranges in a spec.
	_ingressShardListSeparator = ","
	// _ingressNameSeparator separates the parts of a derived consumer name.
	_inressNameSeparator = "-"
)

// ingressRoute is one deployment-declared route: the fact type this town
// reacts to, and the Temporal work it becomes.
//
// The JSON tags are the deployment contract: which towns react to which facts
// is policy an operator sets, not something the core decides.
type ingressRoute struct {
	Domain       string `json:"domain"`
	Type         string `json:"type"`
	WorkflowType string `json:"workflow_type"`
	SignalName   string `json:"signal_name"`
}

// route converts the declared route into the router's own shape. The workflow
// ID is deliberately not part of the contract: the router derives it from the
// fact's entity key, which is the key that has to agree across towns.
func (r ingressRoute) route() temporalrouter.Route {
	return temporalrouter.Route{
		Domain:       r.Domain,
		Type:         r.Type,
		WorkflowType: r.WorkflowType,
		SignalName:   r.SignalName,
	}
}

// ingressConfig is the cross-town bridge as the deployment declares it.
type ingressConfig struct {
	taskQueue      string
	consumerPrefix string
	shards         []uint32
	routes         []ingressRoute
}

// enabled reports whether the deployment asked for a bridge at all.
func (c *ingressConfig) enabled() bool {
	return len(c.routes) > 0
}

// ingressConfigFromApp reads and checks the ingress settings. An empty route
// table is not an error: it is a deployment that reacts to nobody's facts, and
// the bridge stays off.
func ingressConfigFromApp(cfg *config.Config) (ingressConfig, error) {
	table := strings.TrimSpace(cfg.AgentFW.IngressRoutesJSON)
	if table == "" || table == "null" {
		return ingressConfig{}, nil
	}

	var routes []ingressRoute
	if err := json.Unmarshal([]byte(table), &routes); err != nil {
		return ingressConfig{}, fmt.Errorf("app - ingressConfigFromApp - parse routes: %w", err)
	}

	if len(routes) == 0 {
		return ingressConfig{}, nil
	}

	if cfg.AgentFW.IngressTaskQueue == "" {
		return ingressConfig{}, errIngressTaskQueueRequired
	}

	shards, err := parseIngressShards(cfg.AgentFW.IngressShards, cfg.AgentFW.NatsShards)
	if err != nil {
		return ingressConfig{}, err
	}

	if err := validateIngressRoutes(routes); err != nil {
		return ingressConfig{}, err
	}

	prefix := cfg.AgentFW.IngressConsumerPrefix
	if err := validateConsumerToken(prefix); err != nil {
		return ingressConfig{}, err
	}

	return ingressConfig{
		taskQueue:      cfg.AgentFW.IngressTaskQueue,
		consumerPrefix: prefix,
		shards:         shards,
		routes:         routes,
	}, nil
}

// validateIngressRoutes rejects a route that could never be delivered: it
// would consume a domain nothing declares, or name no workflow to hand the
// fact to.
func validateIngressRoutes(routes []ingressRoute) error {
	for _, route := range routes {
		if route.Domain == "" || route.Type == "" || route.WorkflowType == "" || route.SignalName == "" {
			return fmt.Errorf("%w: %#v", errIngressRouteInvalid, route)
		}

		if !eventlog.AgentOSDomain(route.Domain) {
			return fmt.Errorf("%w: %q", errIngressDomainUnknown, route.Domain)
		}
	}

	return nil
}

// parseIngressShards reads a shard specification ("0-7,12") into the shards a
// process owns. An empty spec owns every shard, which is the default and the
// only shape a single-process ingress needs.
func parseIngressShards(spec string, total uint32) ([]uint32, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return allIngressShards(total), nil
	}

	var shards []uint32

	for part := range strings.SplitSeq(spec, _ingressShardListSeparator) {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("%w: empty entry in %q", errIngressShardsInvalid, spec)
		}

		bounds, err := parseShardRange(part)
		if err != nil {
			return nil, err
		}

		if bounds.last < bounds.first {
			return nil, fmt.Errorf("%w: range %q goes backwards", errIngressShardsInvalid, part)
		}

		if bounds.last >= total {
			return nil, fmt.Errorf("%w: %d >= %d", errIngressShardsOutOfRange, bounds.last, total)
		}

		for shard := bounds.first; shard <= bounds.last; shard++ {
			shards = append(shards, shard)
		}
	}

	slices.Sort(shards)
	shards = slices.Compact(shards)

	return shards, nil
}

// shardRange is one entry of a shard specification: a single shard when first
// equals last. The pair is a type rather than two bare results so the ends
// cannot be swapped at a call site.
type shardRange struct {
	first uint32
	last  uint32
}

// parseShardRange reads one entry of the specification: either a number or a
// "first-last" range.
func parseShardRange(part string) (shardRange, error) {
	firstText, lastText, ranged := strings.Cut(part, _ingressShardRangeSeparator)
	if !ranged {
		shard, err := parseShard(part)
		if err != nil {
			return shardRange{}, err
		}

		return shardRange{first: shard, last: shard}, nil
	}

	first, err := parseShard(firstText)
	if err != nil {
		return shardRange{}, err
	}

	last, err := parseShard(lastText)
	if err != nil {
		return shardRange{}, err
	}

	return shardRange{first: first, last: last}, nil
}

// parseShard reads one shard number.
func parseShard(value string) (uint32, error) {
	shard, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%w: %q is not a shard number", errIngressShardsInvalid, value)
	}

	return uint32(shard), nil
}

// allIngressShards is every shard of a domain, in order.
func allIngressShards(total uint32) []uint32 {
	shards := make([]uint32, 0, total)

	for shard := range total {
		shards = append(shards, shard)
	}

	return shards
}

// validateConsumerToken rejects a prefix that would not be one safe subject
// token: the derived consumer name is assembled from it, and a stray dot or
// uppercase letter would make two different consumers look like one.
func validateConsumerToken(prefix string) error {
	if prefix == "" {
		return fmt.Errorf("%w: empty", errIngressConsumerTokenInvalid)
	}

	for _, r := range prefix {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-'
		if !valid {
			return fmt.Errorf("%w: %q", errIngressConsumerTokenInvalid, prefix)
		}
	}

	return nil
}

// ingressConsumerName derives the durable consumer one process owns for a
// domain.
//
// The shard group is part of the name on purpose. Two processes that share a
// consumer name form one JetStream consumer whose workers split the deliveries,
// and a worker pool does not preserve order — so the entities of one shard
// could be processed out of order. Deriving the name from the owned shards makes
// that impossible by construction, at the cost of a rebalanced deployment
// starting fresh consumers (they replay from the oldest retained record, which
// delivery tolerates because every fact carries its identity).
func ingressConsumerName(prefix, domain string, shards []uint32, total uint32) string {
	label := _ingressAllShards
	if len(shards) != int(total) {
		label = shardLabel(shards)
	}

	return strings.Join([]string{prefix, consumerDomainToken(domain), label}, _inressNameSeparator)
}

// consumerDomainToken turns a domain into a single subject token: the log's
// domain names carry dots ("run.timeline"), which are separators in the API
// subject a consumer name is used in.
func consumerDomainToken(domain string) string {
	return strings.ReplaceAll(domain, ".", _inressNameSeparator)
}

// shardLabel writes a shard set compactly ("0-3.5") so the derived name stays
// readable and stable for the same set however it was specified.
func shardLabel(shards []uint32) string {
	sorted := slices.Clone(shards)
	slices.Sort(sorted)

	var parts []string

	for index := 0; index < len(sorted); {
		last := index
		for last+1 < len(sorted) && sorted[last+1] == sorted[last]+1 {
			last++
		}

		if last == index {
			parts = append(parts, strconv.FormatUint(uint64(sorted[index]), 10))
		} else {
			parts = append(parts, strconv.FormatUint(uint64(sorted[index]), 10)+
				_ingressShardRangeSeparator+strconv.FormatUint(uint64(sorted[last]), 10))
		}

		index = last + 1
	}

	return strings.Join(parts, ".")
}

// groupRoutesByDomain buckets the routes by the log domain they consume, in a
// deterministic order: one consumer per domain is what the bridge starts.
func groupRoutesByDomain(routes []ingressRoute) []domainRoutes {
	byDomain := make(map[string][]temporalrouter.Route, len(routes))

	for _, route := range routes {
		byDomain[route.Domain] = append(byDomain[route.Domain], route.route())
	}

	domains := make([]string, 0, len(byDomain))
	for domain := range byDomain {
		domains = append(domains, domain)
	}

	slices.Sort(domains)

	grouped := make([]domainRoutes, 0, len(domains))
	for _, domain := range domains {
		grouped = append(grouped, domainRoutes{domain: domain, routes: byDomain[domain]})
	}

	return grouped
}

// domainRoutes is one domain's slice of the route table.
type domainRoutes struct {
	domain string
	routes []temporalrouter.Route
}

// ingressBridge owns the running bridge: one routing loop per routed domain,
// and the consumers they read from.
type ingressBridge struct {
	logger  logger.Interface
	cancel  context.CancelFunc
	done    chan struct{}
	readers []*eventlognats.Reader
	once    sync.Once
}

// startAppIngress starts the cross-town bridge and returns the function that
// stops it, so Run reads as "start, defer stop" like every other component it
// owns. A failure to build it is fatal: routes that cannot be consumed are a
// deployment mistake, and a town that silently ignores friend-cell facts is
// worse than one that refuses to start.
func startAppIngress(parent context.Context, l logger.Interface, cfg *config.Config, workflows temporalrouter.WorkflowStarter) func() {
	bridge, err := startAgentOSIngress(parent, l, cfg, workflows)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - start agentos ingress: %w", err))
	}

	return bridge.Stop
}

// startAgentOSIngress starts one routing loop per routed domain. Each loop
// reads its domain from the fact log on its own consumer and delivers every
// fact a route claims into Temporal, acknowledging only what Temporal took.
//
// A domain with no routes is not consumed at all: a consumer with an empty
// route table would acknowledge every record it read that no route matched,
// and a route added later could then never see the facts it was meant for.
func startAgentOSIngress(
	parent context.Context,
	l logger.Interface,
	cfg *config.Config,
	workflows temporalrouter.WorkflowStarter,
) (*ingressBridge, error) {
	ingressCfg, err := ingressConfigFromApp(cfg)
	if err != nil {
		return nil, err
	}

	if !ingressCfg.enabled() {
		l.Info("app - startAgentOSIngress - cross-town ingress disabled: no routes configured")

		return &ingressBridge{logger: l}, nil
	}

	if workflows == nil {
		return nil, errIngressRuntimeRequired
	}

	ingress, err := temporalrouter.NewTemporalIngress(workflows, ingressCfg.taskQueue)
	if err != nil {
		return nil, fmt.Errorf("app - startAgentOSIngress - temporalrouter.NewTemporalIngress: %w", err)
	}

	ctx, cancel := context.WithCancel(parent)

	bridge := &ingressBridge{
		logger: l,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	var loops sync.WaitGroup

	if err := bridge.openDomains(parent, ctx, eventBackboneConfig(cfg), &ingressCfg, ingress, l, &loops); err != nil {
		bridge.releaseReaders()
		cancel()

		return nil, err
	}

	go func() {
		loops.Wait()
		close(bridge.done)
	}()

	l.Info("app - startAgentOSIngress - cross-town ingress started: %d domains, task queue %s",
		len(bridge.readers), ingressCfg.taskQueue)

	return bridge, nil
}

// openDomains opens one consumer per routed domain and launches its routing
// loop. A domain whose consumer cannot be opened fails the whole bridge: a town
// that hears only some of the facts it declared is worse than one that refuses
// to start.
func (b *ingressBridge) openDomains(
	parent context.Context,
	ctx context.Context,
	backboneCfg eventlognats.Config,
	ingressCfg *ingressConfig,
	ingress *temporalrouter.TemporalIngress,
	log logger.Interface,
	loops *sync.WaitGroup,
) error {
	for _, grouped := range groupRoutesByDomain(ingressCfg.routes) {
		consumer := ingressConsumerName(ingressCfg.consumerPrefix, grouped.domain, ingressCfg.shards, backboneCfg.Shards)

		reader, err := openBackboneReader(parent, log, &eventlognats.ReaderConfig{
			Config:   backboneCfg,
			Domain:   grouped.domain,
			Consumer: consumer,
			Shards:   ingressCfg.shards,
			// This consumer's failures are Temporal's or the record's, and the
			// fact keeps existing in the log and in the dead-letter stream, so
			// an unprocessable fact is parked instead of blocking its shard.
			OnPoison: eventlognats.PoisonDeadLetter,
			Logger:   log,
		})
		if err != nil {
			return fmt.Errorf("app - startAgentOSIngress - eventlog/nats.NewReader(%s): %w", grouped.domain, err)
		}

		b.readers = append(b.readers, reader)

		router, err := temporalrouter.NewRouter(temporalrouter.Config{
			Reader:  reader,
			Ingress: ingress,
			Routes:  grouped.routes,
			Logger:  log,
		})
		if err != nil {
			return fmt.Errorf("app - startAgentOSIngress - temporalrouter.NewRouter(%s): %w", grouped.domain, err)
		}

		loops.Add(1)

		go func(domain string, router *temporalrouter.Router) {
			defer loops.Done()

			if err := router.Run(ctx); err != nil {
				log.Error(fmt.Errorf("app - startAgentOSIngress - route %s: %w", domain, err))
			}
		}(grouped.domain, router)

		log.Info("app - startAgentOSIngress - consuming %s on consumer %s", grouped.domain, consumer)
	}

	return nil
}

// Stop cancels the routing loops, waits for them to leave their poll, then
// releases the consumers. It is idempotent, and a no-op on a bridge that never
// started.
func (b *ingressBridge) Stop() {
	if b == nil {
		return
	}

	b.once.Do(func() {
		if b.cancel == nil {
			return
		}

		b.cancel()

		if b.done != nil {
			<-b.done
		}

		b.releaseReaders()
	})
}

// releaseReaders closes the consumers this bridge opened, reporting a failure
// instead of hiding it: a consumer that cannot be released leaves the
// connection behind.
func (b *ingressBridge) releaseReaders() {
	for _, reader := range b.readers {
		if err := reader.Close(); err != nil && b.logger != nil {
			b.logger.Error(fmt.Errorf("app - ingressBridge.Stop - close reader: %w", err))
		}
	}

	b.readers = nil
}

// startAppIngressForComponents starts the bridge against the Temporal client
// the runtime owns, and returns its stop function. Declaring routes without a
// Temporal runtime is a configuration mistake the bridge reports as fatal.
func startAppIngressForComponents(
	parent context.Context,
	l logger.Interface,
	cfg *config.Config,
	components *temporalComponents,
) func() {
	var workflows temporalrouter.WorkflowStarter
	if components != nil {
		workflows = components.runtime.Client
	}

	return startAppIngress(parent, l, cfg, workflows)
}
