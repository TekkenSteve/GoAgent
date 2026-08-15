package centrifugo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/centrifugal/centrifuge"
	client "github.com/centrifugal/centrifuge-go"
	"github.com/stretchr/testify/require"
)

const (
	apiKey     = "spike-key"
	channel    = "$agentos:run:acme:spike-run"
	historySz  = 1000
	historyTTL = time.Hour
)

// publishShim is a thin stand-in for Centrifugo's server API: POST /api/publish
// with X-API-Key, body {"channel","data"}, published WITH history so the memory
// broker assigns offsets.
type publishShim struct {
	node *centrifuge.Node
}

func (s *publishShim) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-API-Key") != apiKey {
		http.Error(w, `{"error":{"code":401,"message":"unauthorized"}}`, http.StatusUnauthorized)

		return
	}

	var req struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":{"code":400,"message":"bad request"}}`, http.StatusBadRequest)

		return
	}

	_, err := s.node.Publish(req.Channel, req.Data, centrifuge.WithHistory(historySz, historyTTL))
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":{"code":500,"message":%q}}`, err.Error()), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json")

	if _, err = w.Write([]byte(`{"ok":true}`)); err != nil {
		return
	}
}

func startTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	node, err := centrifuge.New(centrifuge.Config{LogLevel: centrifuge.LogLevelNone})
	require.NoError(t, err)

	node.OnConnecting(func(_ context.Context, _ centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		// The server rejects connections with no Credentials ("client
		// credentials not found"), so hand every anonymous client a user id.
		return centrifuge.ConnectReply{Credentials: &centrifuge.Credentials{UserID: "spike"}}, nil
	})

	node.OnConnect(func(c *centrifuge.Client) {
		c.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			_ = e

			cb(centrifuge.SubscribeReply{Options: centrifuge.SubscribeOptions{
				EnablePositioning: true,
				EnableRecovery:    true,
			}}, nil)
		})

		// History is a per-client RPC: without a registered OnHistory handler
		// the server answers ErrorNotAvailable (108). The empty reply lets the
		// default path serve real history from the broker.
		c.OnHistory(func(e centrifuge.HistoryEvent, cb centrifuge.HistoryCallback) {
			_ = e

			cb(centrifuge.HistoryReply{}, nil)
		})
	})

	require.NoError(t, node.Run())

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{}))
	mux.Handle("/api/publish", &publishShim{node: node})

	server := httptest.NewServer(mux)

	t.Cleanup(func() {
		server.Close()
		require.NoError(t, node.Shutdown(context.Background()))
	})

	return server
}

func wsURL(server *httptest.Server) string {
	return "ws" + strings.TrimPrefix(server.URL, "http") + "/connection/websocket"
}

// connectClient dials the embedded node over a real websocket.
func connectClient(t *testing.T, server *httptest.Server) *client.Client {
	t.Helper()

	c := client.NewJsonClient(wsURL(server), client.Config{})

	connected := make(chan struct{})

	c.OnConnected(func(client.ConnectedEvent) {
		close(connected)
	})

	errCh := make(chan error, 1)

	c.OnError(func(e client.ErrorEvent) {
		errCh <- e.Error
	})

	// Register the connect handlers BEFORE Connect(): connection completes
	// asynchronously and the read pump can fire OnConnected at any moment.
	require.NoError(t, c.Connect())

	select {
	case <-connected:
	case err := <-errCh:
		t.Fatalf("connect: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("connect timed out")
	}

	t.Cleanup(func() {
		c.Close()
	})

	return c
}

func publishViaAPI(t *testing.T, server *httptest.Server, handle *stream.Handle, ev *stream.Event) {
	t.Helper()

	body, err := json.Marshal(struct {
		Channel string `json:"channel"`
		Data    any    `json:"data"`
	}{Channel: handle.Channel, Data: ev})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/api/publish", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode, "publish rejected")
}

func TestSpikeProtocolPairing(t *testing.T) {
	t.Parallel()

	server := startTestServer(t)
	handle := stream.NewHandle(channel)

	// Publish 3 events before any subscription.
	for i := range 3 {
		publishViaAPI(t, server, handle, stream.NewTextMessageContent("t", "r", "m", fmt.Sprintf("delta-%d", i)))
	}

	c := connectClient(t, server)

	sub, err := c.NewSubscription(channel, client.SubscriptionConfig{
		Positioned:  true,
		Recoverable: true,
	})
	require.NoError(t, err)

	live := make(chan client.PublicationEvent, 16)

	sub.OnPublication(func(e client.PublicationEvent) {
		live <- e
	})

	subscribed := make(chan struct{})

	sub.OnSubscribed(func(client.SubscribedEvent) {
		close(subscribed)
	})

	require.NoError(t, sub.Subscribe())

	select {
	case <-subscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("subscribe timed out")
	}

	// History replay from the start: all 3 events, offsets 1..3.
	hctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hres, err := sub.History(hctx, client.WithHistoryLimit(historySz), client.WithHistorySince(&client.StreamPosition{Offset: 0}))
	require.NoError(t, err)
	require.Len(t, hres.Publications, 3, "history from 0")
	require.EqualValues(t, 1, hres.Publications[0].Offset, "first offset")
	require.EqualValues(t, 3, hres.Publications[2].Offset, "third offset")

	// History from cursor after=1: only events with offset > 1 (2, 3).
	hres2, err := sub.History(hctx, client.WithHistoryLimit(historySz), client.WithHistorySince(&client.StreamPosition{Offset: 1}))
	require.NoError(t, err)
	require.Len(t, hres2.Publications, 2, "history after 1")
	require.EqualValues(t, 2, hres2.Publications[0].Offset, "after=1 first offset")

	// Live delivery: publish one more, must arrive with offset 4.
	publishViaAPI(t, server, handle, stream.NewRunStarted("t", "r"))

	select {
	case ev := <-live:
		require.EqualValues(t, 4, ev.Offset, "live offset")

		var got stream.Event

		require.NoError(t, json.Unmarshal(ev.Data, &got))
		require.Equal(t, stream.EventRunStarted, got.Type)
	case <-time.After(5 * time.Second):
		t.Fatal("live publication not delivered")
	}
}

// TestSpikeLiveThenHistoryOrdering simulates the subscriber's replay→live
// bridge: history first (older), then live (newer), never interleaved.
func TestSpikeLiveThenHistoryOrdering(t *testing.T) {
	t.Parallel()

	server := startTestServer(t)
	handle := stream.NewHandle(channel)

	// No prefix history: subscribe first (LiveOnly), then publish.
	c := connectClient(t, server)

	sub, err := c.NewSubscription(channel, client.SubscriptionConfig{
		Positioned:  true,
		Recoverable: true,
	})
	require.NoError(t, err)

	live := make(chan client.PublicationEvent, 16)

	sub.OnPublication(func(e client.PublicationEvent) {
		live <- e
	})

	require.NoError(t, sub.Subscribe())

	// Wait for the subscription to be live before publishing.
	time.Sleep(300 * time.Millisecond)

	publishViaAPI(t, server, handle, stream.NewRunStarted("t", "r"))

	select {
	case ev := <-live:
		require.EqualValues(t, 1, ev.Offset, "live-only first offset")
	case <-time.After(5 * time.Second):
		t.Fatal("live publication not delivered")
	}

	// A second subscriber on a fresh connection replays from 0.
	c2 := connectClient(t, server)

	sub2, err := c2.NewSubscription(channel, client.SubscriptionConfig{
		Positioned:  true,
		Recoverable: true,
	})
	require.NoError(t, err)

	sub2.OnSubscribed(func(e client.SubscribedEvent) {
		require.True(t, e.Recoverable, "server should mark subscription recoverable")
	})
	require.NoError(t, sub2.Subscribe())

	hctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hres, err := sub2.History(hctx, client.WithHistoryLimit(historySz), client.WithHistorySince(&client.StreamPosition{Offset: 0}))
	require.NoError(t, err)
	require.Len(t, hres.Publications, 1, "history has the one live pub")
	require.EqualValues(t, 1, hres.Publications[0].Offset)
}

// TestSpikeRecoveryFlag verifies the server responds with Recoverable=true when
// the subscription asks for recovery on a history channel.
func TestSpikeRecoveryFlag(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex

	gotRecoverable := false

	server := startTestServer(t)
	handle := stream.NewHandle(channel)

	publishViaAPI(t, server, handle, stream.NewRunStarted("t", "r"))

	c := connectClient(t, server)

	sub, err := c.NewSubscription(channel, client.SubscriptionConfig{
		Positioned:  true,
		Recoverable: true,
	})
	require.NoError(t, err)

	sub.OnSubscribed(func(e client.SubscribedEvent) {
		mu.Lock()
		gotRecoverable = e.Recoverable
		mu.Unlock()
	})

	require.NoError(t, sub.Subscribe())

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return gotRecoverable
	}, 5*time.Second, 10*time.Millisecond, "server never marked subscription recoverable")
}
