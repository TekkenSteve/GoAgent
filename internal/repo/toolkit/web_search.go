package toolkit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// Package-level sentinel errors for web search.
var (
	ErrQueryRequired     = errors.New("query is required")
	ErrNoValidQueries    = errors.New("at least one valid search query is required")
	ErrInvalidQueryType  = errors.New("query must be a string or array of strings")
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
)

const defaultProvider = "tavily"

// defaultHTTPTimeout is the HTTP client timeout for web search requests.
const defaultHTTPTimeout = 30 * time.Second

// defaultSearchResults is the default number of search results per query.
const defaultSearchResults = 5

// WebSearchConfig configures the web_search tool.
type WebSearchConfig struct {
	// Provider selects the search backend: "tavily", "google", "bing", or "" for mock.
	Provider string
	// APIKey for the search provider.
	APIKey string
	// BaseURL overrides the default search API endpoint.
	BaseURL string
	// RatePerMinute limits the number of searches per minute. 0 = no limit.
	RatePerMinute int
}

// WebSearchResult is the structured output for a single search query.
type WebSearchResult struct {
	Query   string           `json:"query"`
	Success bool             `json:"success"`
	Results []WebSearchHit   `json:"results"`
	Answer  string           `json:"answer,omitempty"`
	Images  []WebSearchImage `json:"images,omitempty"`
	Error   string           `json:"error,omitempty"`
}

// WebSearchHit is a single search result item.
type WebSearchHit struct {
	Title   string  `json:"title"`
	URL     string  `json:"url"`
	Content string  `json:"content"`
	Score   float64 `json:"score,omitempty"`
}

// WebSearchImage is an image result from Tavily.
type WebSearchImage struct {
	URL    string `json:"url"`
	Width  int    `json:"width,omitempty"`
	Height int    `json:"height,omitempty"`
}

// WebSearchBatchResult wraps multiple query results for batch mode.
type WebSearchBatchResult struct {
	BatchMode    bool              `json:"batch_mode"`
	TotalQueries int               `json:"total_queries"`
	ElapsedMs    int64             `json:"elapsed_ms"`
	Results      []WebSearchResult `json:"results"`
}

// WebSearch executes web searches through configurable backends.
// Supports both single queries (string) and batch queries ([]string) via
// the "query" parameter.
type WebSearch struct {
	cfg   WebSearchConfig
	mu    sync.Mutex
	slots []time.Time
	http  *http.Client
}

// NewWebSearch creates a web search tool.
func NewWebSearch(cfg WebSearchConfig) *WebSearch {
	if cfg.Provider == "" {
		cfg.Provider = defaultProvider
	}

	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.tavily.com"
	}

	return &WebSearch{
		cfg:  cfg,
		http: &http.Client{Timeout: defaultHTTPTimeout},
	}
}

// Meta implements tool.Tool.
func (w *WebSearch) Meta() ToolMeta {
	return ToolMeta{
		Name: "web_search",
		Description: `Search the web for up-to-date information.

Allows searching the web and using the results to inform responses. Provides up-to-date information for current events and recent data. Returns search results including titles, URLs, publication dates, direct answers, and images.

### Batch Mode
ALWAYS batch multiple queries into ONE call for efficiency:
- ❌ WRONG: 3 separate web_search calls
- ✅ CORRECT: One call with query=["topic 1", "topic 2", "topic 3"]

### Sources Requirement
After answering, you MUST include a "Sources:" section listing all relevant URLs as markdown hyperlinks.

### Search Query Best Practice
Use the current year in search queries when searching for recent information.`,
		Parameters: map[string]any{
			_schemaKeyType: "object",
			"properties": map[string]any{
				"query": map[string]any{
					"oneOf": []map[string]any{
						{_schemaKeyType: _schemaTypeString, _schemaKeyDescription: "A single search query."},
						{
							_schemaKeyType:        "array",
							"items":               map[string]any{_schemaKeyType: _schemaTypeString},
							_schemaKeyDescription: "Multiple search queries to execute concurrently.",
						},
					},
					_schemaKeyDescription: "**REQUIRED** - The search query. Either a single string or an array of strings for batch execution.",
				},
				"max_results": map[string]any{
					_schemaKeyType:        "integer",
					_schemaKeyDescription: "Number of search results to return per query (1-50). Default: 5.",
					"default":             defaultSearchResults,
				},
			},
			"required": []any{"query"},
		},
	}
}

// Execute implements tool.Tool.
func (w *WebSearch) Execute(ctx context.Context, args map[string]any) (any, error) {
	maxResults := parseMaxResults(args)

	switch q := args["query"].(type) {
	case string:
		if q == "" {
			return nil, fmt.Errorf("%w", ErrQueryRequired)
		}

		if err := w.rateLimit(ctx); err != nil {
			return nil, err
		}

		return w.executeSingle(ctx, q, maxResults)

	case []any:
		queries := extractQueries(q)
		if len(queries) == 0 {
			return nil, fmt.Errorf("%w", ErrNoValidQueries)
		}

		return w.executeBatch(ctx, queries, maxResults)

	default:
		return nil, fmt.Errorf("%w", ErrInvalidQueryType)
	}
}

// parseMaxResults safely extracts and clamps the max_results argument.
func parseMaxResults(args map[string]any) int {
	mr, ok := args["max_results"].(float64)
	if !ok {
		return defaultSearchResults
	}

	if n := int(mr); n > 0 && n <= 50 {
		return n
	}

	return defaultSearchResults
}

// extractQueries extracts non-empty string queries from a []any batch.
func extractQueries(q []any) []string {
	queries := make([]string, 0, len(q))
	for _, v := range q {
		if s, ok := v.(string); ok && s != "" {
			queries = append(queries, s)
		}
	}

	return queries
}

func (w *WebSearch) executeSingle(ctx context.Context, query string, maxResults int) (any, error) {
	start := time.Now()
	result := w.search(ctx, query, maxResults)
	elapsed := time.Since(start).Milliseconds()

	// For single query, return the result directly (not wrapped in batch)
	// But attach elapsed time via the result structure
	out := map[string]any{
		"query":      result.Query,
		"success":    result.Success,
		"results":    result.Results,
		"answer":     result.Answer,
		"images":     result.Images,
		"elapsed_ms": elapsed,
	}
	if result.Error != "" {
		out["error"] = result.Error
	}

	return out, nil
}

func (w *WebSearch) executeBatch(ctx context.Context, queries []string, maxResults int) (any, error) {
	start := time.Now()

	type searchOut struct {
		result WebSearchResult
		index  int
	}

	ch := make(chan searchOut, len(queries))

	var wg sync.WaitGroup

	for i, q := range queries {
		wg.Add(1)

		go func(idx int, query string) {
			defer wg.Done()

			r := w.search(ctx, query, maxResults)
			ch <- searchOut{result: r, index: idx}
		}(i, q)
	}

	wg.Wait()
	close(ch)

	results := make([]WebSearchResult, len(queries))
	for r := range ch {
		results[r.index] = r.result
	}

	return WebSearchBatchResult{
		BatchMode:    true,
		TotalQueries: len(queries),
		ElapsedMs:    time.Since(start).Milliseconds(),
		Results:      results,
	}, nil
}

func (w *WebSearch) search(ctx context.Context, query string, maxResults int) WebSearchResult {
	switch w.cfg.Provider {
	case defaultProvider:
		return w.searchTavily(ctx, query, maxResults)
	case "google":
		return w.searchGoogle(ctx, query, maxResults)
	case "bing":
		return w.searchBing(ctx, query, maxResults)
	default:
		return w.searchMock(ctx, query, maxResults)
	}
}

// -- Tavily provider --

type tavilyResponse struct {
	Results []tavilyResult `json:"results"`
	Answer  string         `json:"answer"`
	Images  []tavilyImage  `json:"images"`
}

type tavilyResult struct {
	Title   string  `json:"title"`
	URL     string  `json:"url"`
	Content string  `json:"content"`
	Score   float64 `json:"score"`
}

type tavilyImage struct {
	URL string `json:"url"`
}

func (w *WebSearch) searchTavily(ctx context.Context, query string, maxResults int) WebSearchResult {
	if w.cfg.APIKey == "" {
		return WebSearchResult{
			Query:   query,
			Success: false,
			Error:   "Web Search is not available. TAVILY_API_KEY is not configured.",
		}
	}

	data, err := json.Marshal(map[string]any{
		"api_key":        w.cfg.APIKey,
		"query":          query,
		"max_results":    maxResults,
		"include_images": true,
		"include_answer": true,
		"search_depth":   "advanced",
	})
	if err != nil {
		return WebSearchResult{Query: query, Success: false, Error: fmt.Sprintf("marshal search request: %v", err)}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.BaseURL+"/search", bytes.NewReader(data))
	if err != nil {
		return WebSearchResult{Query: query, Success: false, Error: err.Error()}
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := w.http.Do(req)
	if err != nil {
		return WebSearchResult{Query: query, Success: false, Error: err.Error()}
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("web search: close response body: %v", err)
		}
	}()

	var tavilyResp tavilyResponse
	if err := json.NewDecoder(resp.Body).Decode(&tavilyResp); err != nil {
		return WebSearchResult{Query: query, Success: false, Error: err.Error()}
	}

	hits := make([]WebSearchHit, 0, len(tavilyResp.Results))
	for _, r := range tavilyResp.Results {
		hits = append(hits, WebSearchHit(r))
	}

	images := make([]WebSearchImage, 0, len(tavilyResp.Images))
	for _, img := range tavilyResp.Images {
		images = append(images, WebSearchImage{URL: img.URL})
	}

	success := len(hits) > 0 || tavilyResp.Answer != ""

	return WebSearchResult{
		Query:   query,
		Success: success,
		Results: hits,
		Answer:  tavilyResp.Answer,
		Images:  images,
	}
}

// -- Mock provider (fallback when no API key) --

func (w *WebSearch) searchMock(_ context.Context, query string, _ int) WebSearchResult {
	return WebSearchResult{
		Query:   query,
		Success: false,
		Results: []WebSearchHit{},
		Error:   "Web search is not configured. Set TAVILY_API_KEY to enable.",
	}
}

// -- Placeholder providers (for future multi-backend support) --

func (w *WebSearch) searchGoogle(_ context.Context, query string, _ int) WebSearchResult {
	return WebSearchResult{
		Query:   query,
		Success: false,
		Error:   "google search provider not yet implemented",
	}
}

func (w *WebSearch) searchBing(_ context.Context, query string, _ int) WebSearchResult {
	return WebSearchResult{
		Query:   query,
		Success: false,
		Error:   "bing search provider not yet implemented",
	}
}

// -- Rate limiting --

func (w *WebSearch) rateLimit(_ context.Context) error {
	if w.cfg.RatePerMinute <= 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	window := now.Add(-1 * time.Minute)

	j := 0

	for _, t := range w.slots {
		if t.After(window) {
			w.slots[j] = t
			j++
		}
	}

	w.slots = w.slots[:j]

	if len(w.slots) >= w.cfg.RatePerMinute {
		return fmt.Errorf("%w: max %d requests per minute", ErrRateLimitExceeded, w.cfg.RatePerMinute)
	}

	w.slots = append(w.slots, now)

	return nil
}
