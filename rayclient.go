package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// RayClient talks to the Ray Serve vLLM endpoint and the Ray dashboard.
type RayClient struct {
	dashboardURL string // e.g. http://head-svc:8265
	serveURL     string // e.g. http://serve-svc:8000
	httpClient   *http.Client
}

func NewRayClient(dashboardURL, serveURL string) *RayClient {
	return &RayClient{
		dashboardURL: dashboardURL,
		serveURL:     serveURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute,
		},
	}
}

// --- vLLM Completion types ---

// CompletionRequest is sent to the Ray Serve vLLM endpoint.
type CompletionRequest struct {
	Model     string   `json:"model"`
	Prompt    []string `json:"prompt"`
	MaxTokens int      `json:"max_tokens"`
}

// CompletionUsage contains token usage information from vLLM.
type CompletionUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// CompletionResponse from the Ray Serve vLLM endpoint.
type CompletionResponse struct {
	Choices []CompletionChoice `json:"choices"`
	Usage   CompletionUsage    `json:"usage"`
}

type CompletionChoice struct {
	Index int    `json:"index"`
	Text  string `json:"text"`
}

// InferenceRequest is what the queue stores per job.
type InferenceRequest struct {
	Model     string
	Prompts   []string
	MaxTokens int
}

// Complete sends a batch completion request to the vLLM serve endpoint.
func (c *RayClient) Complete(ctx context.Context, req CompletionRequest) (*CompletionResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal completion request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.serveURL+"/v1/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create completion request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send completion request to vLLM: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("vLLM returned %d: %s", resp.StatusCode, string(b))
	}

	var result CompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode completion response: %w", err)
	}
	return &result, nil
}

// ServeHealthz checks if the vLLM serve endpoint is reachable.
func (c *RayClient) ServeHealthz() error {
	resp, err := c.httpClient.Get(c.serveURL + "/-/healthz")
	if err != nil {
		return fmt.Errorf("vLLM serve unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("vLLM serve returned status %d", resp.StatusCode)
	}
	return nil
}

// Healthz pings the Ray dashboard health endpoint.
func (c *RayClient) Healthz() error {
	resp, err := c.httpClient.Get(c.dashboardURL + "/api/version")
	if err != nil {
		return fmt.Errorf("ray dashboard unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ray dashboard returned status %d", resp.StatusCode)
	}
	return nil
}
