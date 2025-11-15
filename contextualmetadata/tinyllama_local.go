package contextualmetadata

import (
	"fmt"
	llama "optimusdb/binding/go-llama.cpp"
	"os"
	"sync"
)

type TinyLlamaClient interface {
	Generate(prompt string, maxTokens int) (string, error)
}

type LocalClient struct {
	mu    sync.Mutex
	model *llama.LLama
}

func NewTinyLlamaLocal() (*LocalClient, error) {
	modelPath := os.Getenv("TINYLLAMA_MODEL") // e.g. /models/TinyLlama-1.1B-Chat-v1.0.Q4_K_M.gguf
	if modelPath == "" {
		return nil, fmt.Errorf("TINYLLAMA_MODEL not set")
	}
	m, err := llama.New(
		modelPath,
		llama.SetContext(2048),
		llama.SetMMap(true), // Memory-mapped IO (reduces RAM)
		llama.EnabelLowVRAM, // Use more CPU/less GPU
	)
	if err != nil {
		return nil, err
	}
	return &LocalClient{model: m}, nil
}

func (c *LocalClient) Generate(prompt string, maxTokens int) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.model.Predict(prompt,
		llama.SetTokens(maxTokens),
		llama.SetTemperature(0.2),
		llama.SetTopP(0.9),
		llama.SetSeed(42), // Deterministic output
	)
}
