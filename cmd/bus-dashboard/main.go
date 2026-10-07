package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/memstream"
)

const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2

	defaultAddr        = ":8613"
	defaultRate        = 25 * time.Millisecond
	defaultConcurrency = 3

	readHeaderTimeout = 5 * time.Second
	shutdownTimeout   = 5 * time.Second
	heartbeatInterval = 15 * time.Second

	transportMemstream = "memstream"

	modeBoth      = "both"
	modeNative    = "native"
	modeLifecycle = "lifecycle"

	memstreamHistory = 5000
	memstreamBuffer  = 1024
)

var (
	errInvalidMode = errors.New("invalid mode")
	errConcurrency = errors.New("concurrency must be >= 1")
	errRate        = errors.New("rate must be positive")
)

//go:embed dashboard.html
var dashboardHTML embed.FS

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// dashboardConfig is the resolved command-line configuration.
type dashboardConfig struct {
	addr        string
	rate        time.Duration
	concurrency int
	runs        int
	seed        int64
	mode        pathMode
	pgURL       string
}

// parseFlags reads and validates the command line. Flag and validation errors
// return the wrapped cause; flag.ErrHelp bubbles up so run() can exit quietly.
func parseFlags(args []string, stderr io.Writer) (*dashboardConfig, error) {
	fs := flag.NewFlagSet("bus-dashboard", flag.ContinueOnError)
	fs.SetOutput(stderr)

	addr := fs.String("addr", defaultAddr, "HTTP listen address")
	rate := fs.Duration("rate", defaultRate, "delay between token events")
	concurrency := fs.Int("concurrency", defaultConcurrency, "parallel synthetic runs")
	runs := fs.Int("runs", 0, "number of runs to generate (0 = until interrupted)")
	seed := fs.Int64("seed", 0, "random seed (0 = time-based)")
	mode := fs.String("mode", modeBoth, "publishing path: both | native | lifecycle")
	pgURL := fs.String("pg-url", "", "optional Postgres URL enabling the writer→PG projection check")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if *concurrency < 1 {
		return nil, errConcurrency
	}

	if *rate <= 0 {
		return nil, errRate
	}

	pathMode, err := parseMode(*mode)
	if err != nil {
		return nil, err
	}

	return &dashboardConfig{
		addr:        *addr,
		rate:        *rate,
		concurrency: *concurrency,
		runs:        *runs,
		seed:        *seed,
		mode:        pathMode,
		pgURL:       *pgURL,
	}, nil
}

func run(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}

		fmt.Fprintf(stderr, "bus-dashboard: %v\n", err)

		return exitUsage
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The data plane is a single in-process bus: Publisher and Subscriber are
	// the same instance, exactly as the shipped app assembles them when no
	// Centrifugo is configured.
	bus := memstream.New(memstream.WithHistory(memstreamHistory), memstream.WithBuffer(memstreamBuffer))

	recorder, repo, pg, err := wireProjection(cfg.pgURL)
	if err != nil {
		fmt.Fprintf(stderr, "bus-dashboard: %v\n", err)

		return exitError
	}

	// The recorder shares the Postgres pool, so closing the pool releases
	// everything the durable path holds.
	if repo != nil {
		defer pg.Close()
	}

	obs := NewObserver(ctx, bus, repo, transportMemstream)
	gen := NewGenerator(bus, cfg.rate, cfg.mode, recorder, cfg.seed)

	// Generator workers publish runs continuously (or a fixed count) until the
	// process is interrupted. Each run flows through the real adapter paths.
	stopWorkers := make(chan struct{})

	var started atomic.Int64

	for i := 0; i < cfg.concurrency; i++ {
		go worker(ctx, stopWorkers, gen, obs, &cfg.runs, &started, stderr)
	}

	fmt.Fprintf(stdout, "bus-dashboard: listening on http://%s transport=%s rate=%s concurrency=%d projection=%v\n",
		cfg.addr, transportMemstream, cfg.rate, cfg.concurrency, repo != nil)

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           newServer(ctx, obs, gen, stdout).routes(),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	if err := serve(srv, stopWorkers); err != nil {
		fmt.Fprintf(stderr, "bus-dashboard: http server: %v\n", err)

		return exitError
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(ctx, shutdownTimeout)
	defer cancelShutdown()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		fmt.Fprintf(stderr, "bus-dashboard: shutdown: %v\n", err)

		return exitError
	}

	return exitOK
}

// serve runs the HTTP server until it fails or the process is interrupted,
// then signals the generator workers to stop exactly once.
func serve(srv *http.Server, stopWorkers chan struct{}) error {
	serveErr := make(chan error, 1)

	go func() {
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			close(stopWorkers)

			return err
		}
	case <-waitInterrupt():
	}

	close(stopWorkers)

	return nil
}

// wireProjection builds the optional writer→PG fact path. With no URL it
// returns nil values, leaving the dashboard in pure bus mode. The recorder is
// the same one the shipped app installs on every writer: each milestone is
// appended to Postgres before its bus publish, so the durable projection and
// the live stream observe the same timeline without any subscription.
func wireProjection(pgURL string) (*streamadapter.MilestoneRecorder, *persistent.AgentOSRunEventRepo, *postgres.Postgres, error) {
	if pgURL == "" {
		return nil, nil, nil, nil
	}

	pg, err := postgres.New(pgURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("postgres: %w", err)
	}

	repo := persistent.NewAgentOSRunEventRepo(pg)

	recorder, err := streamadapter.NewMilestoneRecorder(repo, nil)
	if err != nil {
		pg.Close()

		return nil, nil, nil, fmt.Errorf("milestone recorder: %w", err)
	}

	return recorder, repo, pg, nil
}

// worker drives one generator loop: publish a run, then immediately start the
// next, until the fixed count is reached or the worker is stopped.
func worker(ctx context.Context, stop <-chan struct{}, gen *Generator, obs *Observer, runs *int, started *atomic.Int64, stderr io.Writer) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		if *runs > 0 && started.Load() >= int64(*runs) {
			return
		}

		started.Add(1)

		if err := gen.runOne(ctx, obs); err != nil {
			fmt.Fprintf(stderr, "bus-dashboard: generate run: %v\n", err)

			return
		}
	}
}

// waitInterrupt returns a channel that fires on SIGINT/SIGTERM.
func waitInterrupt() <-chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

	return ch
}

func parseMode(s string) (pathMode, error) {
	switch s {
	case modeBoth:
		return pathBoth, nil
	case modeNative:
		return pathNative, nil
	case modeLifecycle:
		return pathLifecycle, nil
	default:
		return pathBoth, fmt.Errorf("%w: %q (want %s | %s | %s)", errInvalidMode, s, modeBoth, modeNative, modeLifecycle)
	}
}

// server wires the dashboard HTTP endpoints.
type server struct {
	ctx    context.Context
	obs    *Observer
	gen    *Generator
	stdout io.Writer
}

func newServer(ctx context.Context, obs *Observer, gen *Generator, stdout io.Writer) *server {
	return &server{ctx: ctx, obs: obs, gen: gen, stdout: stdout}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", s.handlePage)
	mux.HandleFunc("/api/state", s.handleState)
	mux.HandleFunc("/events", s.handleEvents)
	mux.HandleFunc("/api/run", s.handleStartRun)

	return mux
}

func (s *server) handlePage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)

		return
	}

	page, err := dashboardHTML.ReadFile("dashboard.html")
	if err != nil {
		http.Error(w, "dashboard page unavailable", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if _, err := w.Write(page); err != nil {
		fmt.Fprintf(s.stdout, "bus-dashboard: write page: %v\n", err)
	}
}

func (s *server) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(s.obs.snapshot()); err != nil {
		fmt.Fprintf(s.stdout, "bus-dashboard: encode state: %v\n", err)
	}
}

// handleStartRun triggers one synthetic run on demand through the same real
// publishing paths the background workers use.
func (s *server) handleStartRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}

	go func() {
		if err := s.gen.runOne(s.ctx, s.obs); err != nil {
			fmt.Fprintf(s.stdout, "bus-dashboard: on-demand run: %v\n", err)
		}
	}()

	w.WriteHeader(http.StatusAccepted)
}

// handleEvents streams dashboard snapshots over Server-Sent Events. Every bus
// delivery wakes the subscribers via the observer broadcaster; a periodic
// comment keeps the connection alive between bursts.
func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	notify := s.obs.bcast.subscribe()
	defer s.obs.bcast.unsubscribe(notify)

	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	ctx := r.Context()
	enc := json.NewEncoder(w)

	for {
		select {
		case <-ctx.Done():
			return
		case <-notify:
			if err := sendSSEState(w, enc, s.obs, flusher); err != nil {
				return
			}
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}

			flusher.Flush()
		}
	}
}

// sendSSEState writes one `data: <json>` frame and flushes it.
func sendSSEState(w io.Writer, enc *json.Encoder, obs *Observer, flusher http.Flusher) error {
	if _, err := io.WriteString(w, "data: "); err != nil {
		return err
	}

	if err := enc.Encode(obs.snapshot()); err != nil {
		return err
	}

	if _, err := io.WriteString(w, "\n"); err != nil {
		return err
	}

	flusher.Flush()

	return nil
}
