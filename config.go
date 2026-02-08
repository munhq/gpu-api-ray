package main

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port             string
	APIKey           string
	RayDashboardURL  string // Deprecated: kept for backward compat
	DefaultModel     string
	DefaultMaxTokens int
	Namespace        string
	RayClusterName   string
	KueueQueueName   string
	JobTTLSeconds    int
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		Port:             envOrDefault("PORT", "8000"),
		APIKey:           os.Getenv("API_KEY"),
		RayDashboardURL:  envOrDefault("RAY_DASHBOARD_URL", "http://raycluster-batch-inference-head-svc:8265"),
		DefaultModel:     envOrDefault("DEFAULT_MODEL", "Qwen/Qwen2.5-0.5B-Instruct"),
		DefaultMaxTokens: envOrDefaultInt("DEFAULT_MAX_TOKENS", 50),
		Namespace:        envOrDefault("NAMESPACE", "gpu-workloads"),
		RayClusterName:   envOrDefault("RAY_CLUSTER_NAME", "raycluster-batch-inference"),
		KueueQueueName:   envOrDefault("KUEUE_QUEUE_NAME", "user-queue"),
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
