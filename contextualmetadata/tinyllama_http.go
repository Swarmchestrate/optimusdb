//go:build !tinyllama_local

package contextualmetadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type LLMRequest struct {
	Prompt      string   `json:"prompt"`
	MaxTokens   int      `json:"max_tokens"`
	Temperature float64  `json:"temperature"`
	Stop        []string `json:"stop,omitempty"`
}

type LLMResponse struct {
	Text    string  `json:"text"`
	Tokens  int     `json:"tokens,omitempty"`
	Latency float64 `json:"latency_ms,omitempty"`
}

type HTTPClient struct {
	Endpoint   string
	Timeout    time.Duration
	MaxRetries int
	RetryDelay time.Duration
	client     *http.Client
}

func NewTinyLlamaHTTP() (*HTTPClient, error) {
	ep := os.Getenv("TINYLLAMA_ENDPOINT")
	if ep == "" {
		// llama.cpp server supports several routes; keep your endpoint in env.
		// Default to text completion style compatible wrapper:
		ep = "http://localhost:8080/v1/completions"
	}

	return &HTTPClient{
		Endpoint:   ep,
		Timeout:    60 * time.Second,
		MaxRetries: 3,
		RetryDelay: 2 * time.Second,
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
	}, nil
}

func (c *HTTPClient) Generate(prompt string, maxTokens int) (string, error) {
	return c.GenerateWithContext(context.Background(), prompt, maxTokens)
}

func (c *HTTPClient) GenerateWithContext(ctx context.Context, prompt string, maxTokens int) (string, error) {
	req := LLMRequest{
		Prompt:      prompt,
		MaxTokens:   maxTokens,
		Temperature: 0.2,
		Stop:        []string{"\n\n", "###"},
	}

	var lastErr error
	for attempt := 0; attempt <= c.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(c.RetryDelay):
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}

		resp, err := c.doRequest(ctx, req)
		if err != nil {
			lastErr = err
			continue
		}
		return resp.Text, nil
	}
	return "", fmt.Errorf("tinyllama: failed after %d attempts: %w", c.MaxRetries+1, lastErr)
}

func (c *HTTPClient) doRequest(ctx context.Context, req LLMRequest) (*LLMResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("llm http %d: %s", resp.StatusCode, string(b))
	}

	// Two formats are commonly seen:
	// 1) Simple text: { "text": "...", "tokens": ... }
	// 2) OpenAI-like: { "choices": [ { "text": "..." } ], ... }
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	var out LLMResponse
	// Try simple first
	if err := json.Unmarshal(raw, &out); err == nil && out.Text != "" {
		out.Latency = float64(time.Since(start).Milliseconds())
		return &out, nil
	}

	// Try OpenAI-ish
	var alt struct {
		Choices []struct {
			Text string `json:"text"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &alt); err == nil && len(alt.Choices) > 0 {
		return &LLMResponse{
			Text:    alt.Choices[0].Text,
			Latency: float64(time.Since(start).Milliseconds()),
		}, nil
	}

	return nil, fmt.Errorf("unexpected LLM response: %s", string(raw))
}

// HealthCheck verifies TinyLlama service is reachable
func (c *HTTPClient) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := c.GenerateWithContext(ctx, "Test", 8)
	return err
}
