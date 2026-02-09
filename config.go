package main

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port             string
	APIKey           string
	RayDashboardURL  string // Ray dashboard at :8265
	RayServeURL      string // Ray Serve vLLM endpoint at :8000
	RedisURL         string // Dragonfly/Redis for job persistence
	DefaultModel     string
	DefaultMaxTokens int
	MaxMaxTokens     int // upper bound for max_tokens in requests
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
		DefaultModel:     envOrDefault("DEFAULT_MODEL", "qwen2.5-0.5b-instruct"),
		DefaultMaxTokens: envOrDefaultInt("DEFAULT_MAX_TOKENS", 512),
		MaxMaxTokens:     envOrDefaultInt("MAX_MAX_TOKENS", 131072),
		MaxConcurrent:    envOrDefaultInt("MAX_CONCURRENT", 4),
		JobTTLSeconds:    envOrDefaultInt("JOB_TTL_SECONDS", 604800),
	}

	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

func validateConfig(cfg *Config) error {
	// Required fields
	if cfg.APIKey == "" {
		return fmt.Errorf("API_KEY environment variable is required")
	}
	if len(cfg.APIKey) < 8 {
		return fmt.Errorf("API_KEY must be at least 8 characters long")
	}

	// Port validation
	port, err := strconv.Atoi(cfg.Port)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("PORT must be a valid port number (1-65535), got: %s", cfg.Port)
	}

	// URL validation
	if err := validateURL("RAY_DASHBOARD_URL", cfg.RayDashboardURL); err != nil {
		return err
	}
	if err := validateURL("RAY_SERVE_URL", cfg.RayServeURL); err != nil {
		return err
	}

	// Redis URL validation (basic format check)
	if cfg.RedisURL != "" {
		parts := strings.Split(cfg.RedisURL, ":")
		if len(parts) != 2 {
			return fmt.Errorf("REDIS_URL must be in format host:port, got: %s", cfg.RedisURL)
		}
		if port, err := strconv.Atoi(parts[1]); err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("REDIS_URL port must be valid (1-65535), got: %s", parts[1])
		}
	}

	// Numeric range validation
	if cfg.MaxMaxTokens < 1 || cfg.MaxMaxTokens > 131072 {
		return fmt.Errorf("MAX_MAX_TOKENS must be between 1 and 131072, got: %d", cfg.MaxMaxTokens)
	}
	if cfg.DefaultMaxTokens < 1 || cfg.DefaultMaxTokens > cfg.MaxMaxTokens {
		return fmt.Errorf("DEFAULT_MAX_TOKENS must be between 1 and %d (MAX_MAX_TOKENS), got: %d", cfg.MaxMaxTokens, cfg.DefaultMaxTokens)
	}
	if cfg.MaxConcurrent < 1 || cfg.MaxConcurrent > 1000 {
		return fmt.Errorf("MAX_CONCURRENT must be between 1 and 1000, got: %d", cfg.MaxConcurrent)
	}
	if cfg.JobTTLSeconds < 3600 || cfg.JobTTLSeconds > 2592000 { // 1 hour to 30 days
		return fmt.Errorf("JOB_TTL_SECONDS must be between 3600 and 2592000, got: %d", cfg.JobTTLSeconds)
	}

	// Model name validation (basic)
	if strings.TrimSpace(cfg.DefaultModel) == "" {
		return fmt.Errorf("DEFAULT_MODEL cannot be empty")
	}

	return nil
}

func validateURL(name, urlStr string) error {
	u, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("%s must be a valid URL: %w", name, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%s must use http or https scheme, got: %s", name, u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("%s must have a valid host", name)
	}
	return nil
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
