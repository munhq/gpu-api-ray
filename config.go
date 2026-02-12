package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// DefaultPerGPUVRAM is the reference GPU size used for auto-calculating
// GPU count when no specific GPU type is targeted. 24GB covers RTX 3090/4090.
const DefaultPerGPUVRAM = 24

// ModelConfig describes a model the system can serve, with rich provisioning metadata.
// VRAM is the primary input — GPU count, RAM, disk are auto-calculated.
type ModelConfig struct {
	ID     string `json:"id"`     // API request name (e.g., "glm-4-7")
	Source string `json:"source"` // HuggingFace source (e.g., "hf:zai-org/GLM-4.7")

	// GPU requirements — vramRequired is the primary input
	VRAMRequired int `json:"vramRequired"` // GB total VRAM needed to load this model
	MaxGPUCount  int `json:"maxGpuCount"`  // optional upper bound on GPU count (0 = no limit)
	MaxModelLen  int `json:"maxModelLen"`  // vLLM max_model_len

	// Scaling behavior
	AlwaysActive          bool `json:"alwaysActive"`          // true = keep warm, never scale to zero
	MinReplicas           int  `json:"minReplicas"`           // Ray Serve min replicas (0 = cold)
	MaxReplicas           int  `json:"maxReplicas"`           // Ray Serve max replicas
	TargetOngoingRequests int  `json:"targetOngoingRequests"` // Ray Serve autoscale target

	// Instance mode: "ray-worker" (default) joins Ray cluster, "full-node" joins K8s via VPN
	NodeType string `json:"nodeType"`

	// Provider preferences (per-model override; falls back to global)
	CapacityType   string  `json:"capacityType"`   // "spot" or "on-demand"
	MaxPricePerGPU float64 `json:"maxPricePerGPU"` // $/hr per GPU, 0 = no limit

	// Optional model pre-caching from object storage
	CacheURL string `json:"cacheUrl"` // rclone-compatible URL (e.g., "r2:model-cache/glm-4-7")

	// Computed fields (populated by ComputeRequirements)
	GPUCount          int `json:"-"` // ceil(vramRequired / perGPUVRAM)
	MinRAM            int `json:"-"` // vramRequired * 1.1
	MinDisk           int `json:"-"` // vramRequired * 2.5
	TensorParallelSize int `json:"-"` // same as GPUCount
}

// ComputeRequirements derives GPU count, RAM, disk from vramRequired and
// the given per-GPU VRAM size. Call this after loading config.
func (m *ModelConfig) ComputeRequirements(perGPUVRAM int) {
	if perGPUVRAM <= 0 {
		perGPUVRAM = DefaultPerGPUVRAM
	}
	if m.VRAMRequired <= 0 {
		m.GPUCount = 1
		m.TensorParallelSize = 1
		m.MinRAM = 8
		m.MinDisk = 20
		return
	}
	m.GPUCount = int(math.Ceil(float64(m.VRAMRequired) / float64(perGPUVRAM)))
	if m.GPUCount < 1 {
		m.GPUCount = 1
	}
	if m.MaxGPUCount > 0 && m.GPUCount > m.MaxGPUCount {
		m.GPUCount = m.MaxGPUCount
	}
	m.TensorParallelSize = m.GPUCount
	m.MinRAM = int(math.Ceil(float64(m.VRAMRequired) * 1.1))
	m.MinDisk = int(math.Ceil(float64(m.VRAMRequired) * 2.5))
}

type Config struct {
	Port             string
	APIKey           string
	RayDashboardURL  string // Ray dashboard at :8265
	RayServeURL      string // Ray Serve vLLM endpoint at :8000
	RedisURL         string // Dragonfly/Redis for job persistence
	DefaultModel     string
	DefaultMaxTokens int
	MaxMaxTokens     int // upper bound for max_tokens in requests
	JobTTLSeconds    int // TTL for completed jobs in Redis
	SessionTTLSeconds    int    // TTL for chat sessions in Redis (DB 2)
	WorkerHealthInterval int    // Worker health check interval in seconds
	ModelCatalog     []ModelConfig            // ordered list of known models
	Models           map[string]*ModelConfig  // keyed by model ID for fast lookup
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		Port:             envOrDefault("PORT", "8000"),
		APIKey:           os.Getenv("API_KEY"),
		RayDashboardURL:  envOrDefault("RAY_DASHBOARD_URL", "http://rayservice-head-svc:8265"),
		RayServeURL:      envOrDefault("RAY_SERVE_URL", "http://rayservice-serve-svc:8000"),
		RedisURL:         envOrDefault("REDIS_URL", "dragonfly.gpu-workloads.svc.cluster.local:6379"),
		DefaultModel:     envOrDefault("DEFAULT_MODEL", "qwen2.5-0.5b-instruct"),
		DefaultMaxTokens: envOrDefaultInt("DEFAULT_MAX_TOKENS", 512),
		MaxMaxTokens:     envOrDefaultInt("MAX_MAX_TOKENS", 131072),
		JobTTLSeconds:        envOrDefaultInt("JOB_TTL_SECONDS", 604800),
		SessionTTLSeconds:    envOrDefaultInt("SESSION_TTL_SECONDS", 172800),
		WorkerHealthInterval: envOrDefaultInt("WORKER_HEALTH_INTERVAL", 15),
	}

	// Parse model catalog from JSON env var (optional).
	// Accepts the new ModelConfig format (vramRequired, alwaysActive, etc).
	if mc := os.Getenv("MODELS_CONFIG"); mc != "" {
		var models []ModelConfig
		if err := json.Unmarshal([]byte(mc), &models); err != nil {
			return nil, fmt.Errorf("MODELS_CONFIG must be valid JSON array: %w", err)
		}
		cfg.ModelCatalog = models
	}

	// Build the Models map and compute derived requirements.
	cfg.Models = make(map[string]*ModelConfig, len(cfg.ModelCatalog))
	for i := range cfg.ModelCatalog {
		m := &cfg.ModelCatalog[i]
		if m.NodeType == "" {
			m.NodeType = "ray-worker" // default: join Ray cluster directly
		}
		m.ComputeRequirements(DefaultPerGPUVRAM)
		cfg.Models[m.ID] = m
	}

	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

func validateConfig(cfg *Config) error {
	if cfg.APIKey == "" {
		return fmt.Errorf("API_KEY environment variable is required")
	}
	if len(cfg.APIKey) < 8 {
		return fmt.Errorf("API_KEY must be at least 8 characters long")
	}

	port, err := strconv.Atoi(cfg.Port)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("PORT must be a valid port number (1-65535), got: %s", cfg.Port)
	}

	if err := validateURL("RAY_DASHBOARD_URL", cfg.RayDashboardURL); err != nil {
		return err
	}
	if err := validateURL("RAY_SERVE_URL", cfg.RayServeURL); err != nil {
		return err
	}

	if cfg.RedisURL != "" {
		parts := strings.Split(cfg.RedisURL, ":")
		if len(parts) != 2 {
			return fmt.Errorf("REDIS_URL must be in format host:port, got: %s", cfg.RedisURL)
		}
		if port, err := strconv.Atoi(parts[1]); err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("REDIS_URL port must be valid (1-65535), got: %s", parts[1])
		}
	}

	if cfg.MaxMaxTokens < 1 || cfg.MaxMaxTokens > 131072 {
		return fmt.Errorf("MAX_MAX_TOKENS must be between 1 and 131072, got: %d", cfg.MaxMaxTokens)
	}
	if cfg.DefaultMaxTokens < 1 || cfg.DefaultMaxTokens > cfg.MaxMaxTokens {
		return fmt.Errorf("DEFAULT_MAX_TOKENS must be between 1 and %d (MAX_MAX_TOKENS), got: %d", cfg.MaxMaxTokens, cfg.DefaultMaxTokens)
	}
	if cfg.JobTTLSeconds < 3600 || cfg.JobTTLSeconds > 2592000 {
		return fmt.Errorf("JOB_TTL_SECONDS must be between 3600 and 2592000, got: %d", cfg.JobTTLSeconds)
	}

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
