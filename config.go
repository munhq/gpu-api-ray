package main

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port             string
	APIKey           string
	RayDashboardURL  string // Ray dashboard at :8265 (also used to derive serve URL at :8000)
	DefaultModel     string
	DefaultMaxTokens int
	VLLMModel        string // model loaded by the Ray Serve vLLM deployment
	MaxConcurrent    int    // max concurrent inference requests to vLLM
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		Port:             envOrDefault("PORT", "8000"),
		APIKey:           os.Getenv("API_KEY"),
		RayDashboardURL:  envOrDefault("RAY_DASHBOARD_URL", "http://raycluster-batch-inference-head-svc:8265"),
		DefaultModel:     envOrDefault("DEFAULT_MODEL", "Qwen/Qwen2.5-0.5B-Instruct"),
		DefaultMaxTokens: envOrDefaultInt("DEFAULT_MAX_TOKENS", 50),
		VLLMModel:        envOrDefault("VLLM_MODEL", "Qwen/Qwen2.5-0.5B-Instruct"),
		MaxConcurrent:    envOrDefaultInt("MAX_CONCURRENT", 4),
	}

	if cfg.APIKey == "" {
		return nil, fmt.Errorf("API_KEY environment variable is required")
	}

	return cfg, nil
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envOrDefaultInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return i
}
