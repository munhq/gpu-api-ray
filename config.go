package main

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port             string
	APIKey           string
	RayDashboardURL  string // Ray dashboard at :8265
	RayServeURL      string // Ray Serve vLLM endpoint at :8000
	RedisURL         string // Dragonfly/Redis for job persistence
	DefaultModel     string
	DefaultMaxTokens int
	MaxConcurrent    int // max concurrent inference requests to vLLM
	JobTTLSeconds    int // TTL for completed jobs in Redis
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		Port:             envOrDefault("PORT", "8000"),
		APIKey:           os.Getenv("API_KEY"),
		RayDashboardURL:  envOrDefault("RAY_DASHBOARD_URL", "http://raycluster-batch-inference-head-svc:8265"),
		RayServeURL:      envOrDefault("RAY_SERVE_URL", "http://raycluster-batch-inference-serve-svc:8000"),
		RedisURL:         envOrDefault("REDIS_URL", "dragonfly.gpu-workloads.svc.cluster.local:6379"),
		DefaultModel:     envOrDefault("DEFAULT_MODEL", "Qwen/Qwen2.5-0.5B-Instruct"),
		DefaultMaxTokens: envOrDefaultInt("DEFAULT_MAX_TOKENS", 50),
		MaxConcurrent:    envOrDefaultInt("MAX_CONCURRENT", 4),
		JobTTLSeconds:    envOrDefaultInt("JOB_TTL_SECONDS", 3600),
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
