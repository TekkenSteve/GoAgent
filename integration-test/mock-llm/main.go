package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type chatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
	Usage   usage        `json:"usage"`
}

type chatChoice struct {
	Index        int         `json:"index"`
	Message      chatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

const (
	readHeaderTimeout = 5 * time.Second
	writeTimeout      = 10 * time.Second
	mockPromptTokens  = 15
	mockCompTokens    = 10
	mockTotalTokens   = 25
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", handleChat)

	addr := ":8080"
	log.Printf("mock-llm listening on %s", addr)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		WriteTimeout:      writeTimeout,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}

	// Log request for debugging
	bodyLen := r.ContentLength

	msg := fmt.Sprintf("received chat request: model=%s content-length=%d", r.URL.Query().Get("model"), bodyLen)
	log.Print(msg) //nolint:gosec // mock server, controlled input

	w.Header().Set("Content-Type", "application/json")

	resp := chatResponse{
		ID:      fmt.Sprintf("chatcmpl-mock-%d", time.Now().UnixNano()),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   "gpt-4.1-mini",
		Choices: []chatChoice{
			{
				Index: 0,
				Message: chatMessage{
					Role:    "assistant",
					Content: "Hello! I'm a mock LLM. This is a test response for E2E integration testing.",
				},
				FinishReason: "stop",
			},
		},
		Usage: usage{
			PromptTokens:     mockPromptTokens,
			CompletionTokens: mockCompTokens,
			TotalTokens:      mockTotalTokens,
		},
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("error encoding response: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}
