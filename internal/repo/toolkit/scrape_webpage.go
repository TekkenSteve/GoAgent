package toolkit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// scrapeHTTPTimeout is the HTTP client timeout for web scraping.
const scrapeHTTPTimeout = 60 * time.Second

// ErrURLsRequired is returned when no URLs are provided to the scraper.
var ErrURLsRequired = errors.New("urls is required")

// ScrapeWebpageConfig configures the scrape_webpage tool.
type ScrapeWebpageConfig struct {
	// APIKey for the Firecrawl API.
	APIKey string
	// BaseURL overrides the default Firecrawl API endpoint.
	BaseURL string
}

// ScrapeResult is the output from scraping a single URL.
type ScrapeResult struct {
	URL     string `json:"url"`
	Success bool   `json:"success"`
	Title   string `json:"title,omitempty"`
	Content string `json:"content,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ScrapeBatchResult wraps multiple scrape results.
type ScrapeBatchResult struct {
	Total      int            `json:"total"`
	Successful int            `json:"successful"`
	Failed     int            `json:"failed"`
	Results    []ScrapeResult `json:"results"`
}

// ScrapeWebpage fetches and extracts content from web pages using Firecrawl.
// Supports batch URLs in a single call for efficiency.
type ScrapeWebpage struct {
	cfg  ScrapeWebpageConfig
	http *http.Client
}

// NewScrapeWebpage creates a webpage scraping tool.
func NewScrapeWebpage(cfg ScrapeWebpageConfig) *ScrapeWebpage {
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.firecrawl.dev"
	}

	return &ScrapeWebpage{
		cfg:  cfg,
		http: &http.Client{Timeout: scrapeHTTPTimeout},
	}
}

// Meta implements tool.Tool.
func (s *ScrapeWebpage) Meta() ToolMeta {
	return ToolMeta{
		Name: "scrape_webpage",
		Description: `Fetch and extract content from web pages. Use this to retrieve the full content of web pages found via web_search.

### Usage
- Batch multiple URLs in a single call for efficiency
- URLs should be comma-separated
- Returns page content as markdown text
- For GitHub URLs, prefer using gh CLI via bash instead

### Best Practice
Always collect multiple relevant URLs from web-search results and scrape them all in a single call rather than making separate calls.`,
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"urls": map[string]any{
					"type":        "string",
					"description": "**REQUIRED** - URLs to scrape, separated by commas. Example: 'https://example.com/page1,https://example.com/page2'",
				},
			},
			"required": []any{"urls"},
		},
	}
}

// Execute implements tool.Tool.
func (s *ScrapeWebpage) Execute(ctx context.Context, args map[string]any) (any, error) {
	urlsRaw, ok := args["urls"].(string)
	if !ok {
		urlsRaw = ""
	}

	if urlsRaw == "" {
		return nil, fmt.Errorf("%w", ErrURLsRequired)
	}

	urlList := strings.Split(urlsRaw, ",")
	for i := range urlList {
		urlList[i] = strings.TrimSpace(urlList[i])
	}

	if s.cfg.APIKey == "" {
		results := make([]ScrapeResult, len(urlList))
		for i, u := range urlList {
			results[i] = ScrapeResult{
				URL:     u,
				Success: false,
				Error:   "Web scraping is not available. FIRECRAWL_API_KEY is not configured.",
			}
		}

		return ScrapeBatchResult{
			Total:   len(urlList),
			Failed:  len(urlList),
			Results: results,
		}, nil
	}

	type scrapeOut struct {
		result ScrapeResult
		index  int
	}

	ch := make(chan scrapeOut, len(urlList))

	for i, u := range urlList {
		go func(idx int, url string) {
			r := s.scrape(ctx, url)
			ch <- scrapeOut{result: r, index: idx}
		}(i, u)
	}

	results := make([]ScrapeResult, len(urlList))
	successful := 0

	for range urlList {
		r := <-ch

		results[r.index] = r.result
		if r.result.Success {
			successful++
		}
	}

	return ScrapeBatchResult{
		Total:      len(urlList),
		Successful: successful,
		Failed:     len(urlList) - successful,
		Results:    results,
	}, nil
}

type firecrawlReq struct {
	URL     string   `json:"url"`
	Formats []string `json:"formats"`
}

type firecrawlResp struct {
	Data *firecrawlData `json:"data"`
}

type firecrawlData struct {
	Markdown string         `json:"markdown"`
	Metadata map[string]any `json:"metadata"`
}

func (s *ScrapeWebpage) scrape(ctx context.Context, url string) ScrapeResult {
	body := firecrawlReq{
		URL:     url,
		Formats: []string{"markdown"},
	}

	data, err := json.Marshal(body)
	if err != nil {
		return ScrapeResult{URL: url, Success: false, Error: fmt.Sprintf("marshal scrape request: %v", err)}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.BaseURL+"/v1/scrape", bytes.NewReader(data))
	if err != nil {
		return ScrapeResult{URL: url, Success: false, Error: err.Error()}
	}

	req.Header.Set("Authorization", "Bearer "+s.cfg.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.http.Do(req)
	if err != nil {
		return ScrapeResult{URL: url, Success: false, Error: err.Error()}
	}
	defer resp.Body.Close()

	var fcResp firecrawlResp
	if err := json.NewDecoder(resp.Body).Decode(&fcResp); err != nil {
		return ScrapeResult{URL: url, Success: false, Error: err.Error()}
	}

	if fcResp.Data == nil {
		return ScrapeResult{URL: url, Success: false, Error: "empty response from Firecrawl"}
	}

	title := ""

	if fcResp.Data.Metadata != nil {
		if t, ok := fcResp.Data.Metadata["title"].(string); ok {
			title = t
		}
	}

	return ScrapeResult{
		URL:     url,
		Success: true,
		Title:   title,
		Content: fcResp.Data.Markdown,
	}
}
