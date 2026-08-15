package v1

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/gofiber/fiber/v2"
)

// fakeRunEventReader records the forwarded scope and returns a fixed timeline.
type fakeRunEventReader struct {
	runID  string
	after  int64
	limit  int
	events []agentoscore.Event
}

func (r *fakeRunEventReader) ListRunEvents(_ context.Context, runID string, after int64, limit int) ([]agentoscore.Event, error) {
	r.runID = runID
	r.after = after
	r.limit = limit

	return r.events, nil
}

func TestAgentOSRunEventHistoryRoute(t *testing.T) {
	t.Parallel()

	reader := &fakeRunEventReader{events: []agentoscore.Event{
		{EventID: "run-1:4", EventType: agentoscore.EventRunStarted, RunID: "run-1", Sequence: 4},
	}}
	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, nil, nil, reader)

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/runs/run-1/events/history?after_sequence=3&limit=10", "")

	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d", resp.StatusCode)
	}

	var events []agentoscore.Event
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		t.Fatalf("decode events: %v", err)
	}

	if len(events) != 1 || events[0].EventID != "run-1:4" || events[0].EventType != agentoscore.EventRunStarted {
		t.Fatalf("unexpected events: %#v", events)
	}

	if reader.runID != "run-1" || reader.after != 3 || reader.limit != 10 {
		t.Fatalf("forwarded scope = run=%q after=%d limit=%d, want run-1/3/10", reader.runID, reader.after, reader.limit)
	}
}

func TestAgentOSRunEventHistoryUnconfigured(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, nil, nil, nil)

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/runs/run-1/events/history", "")

	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status code = %d, want 404", resp.StatusCode)
	}
}
