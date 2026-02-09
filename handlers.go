package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

// --- Request / Response types ---

type BatchRequest struct {
	Model     string              `json:"model,omitempty"`
	Input     []map[string]string `json:"input"`
	MaxTokens int                 `json:"max_tokens,omitempty"`
	Priority  string              `json:"priority,omitempty"`
}

type BatchSubmitResponse struct {
	JobID    string `json:"job_id"`
	Status   string `json:"status"`
	Priority string `json:"priority"`
}

type BatchStatusResponse struct {
	JobID    string              `json:"job_id"`
	Status   string              `json:"status"`
	Priority string              `json:"priority,omitempty"`
	Results  []map[string]string `json:"results,omitempty"`
	Message  string              `json:"message,omitempty"`
}

type BatchListItem struct {
	JobID           string     `json:"job_id"`
	Status          string     `json:"status"`
	Priority        string     `json:"priority"`
	Model           string     `json:"model"`
	PromptCount     int        `json:"prompt_count"`
	DurationSeconds float64    `json:"duration_seconds"`
	Message         string     `json:"message,omitempty"`
	EnqueuedAt      time.Time  `json:"enqueued_at"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
}

type BatchListResponse struct {
	Jobs []BatchListItem `json:"jobs"`
}

// --- Handlers ---

type Handlers struct {
	cfg   *Config
	ray   *RayClient
	queue *JobQueue
}

func NewHandlers(cfg *Config, ray *RayClient, queue *JobQueue) *Handlers {
	return &Handlers{cfg: cfg, ray: ray, queue: queue}
}

func (h *Handlers) submitBatch(w http.ResponseWriter, r *http.Request) {
	var req BatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("invalid request body: %v", err)})
		return
	}

	// Validate input
	if len(req.Input) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "input is required and must be non-empty"})
		return
	}
	if len(req.Input) > 100 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "input exceeds maximum of 100 items"})
		return
	}
	for i, item := range req.Input {
		if _, ok := item["prompt"]; !ok {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("input[%d] missing required 'prompt' field", i)})
			return
		}
	}

	// Defaults
	model := req.Model
	if model == "" {
		model = h.cfg.DefaultModel
	}
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = h.cfg.DefaultMaxTokens
	}
	if maxTokens > h.cfg.MaxMaxTokens {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("max_tokens must be between 1 and %d", h.cfg.MaxMaxTokens)})
		return
	}
	priority := req.Priority
	if priority == "" {
		priority = "medium"
	}
	if priority != "high" && priority != "medium" && priority != "low" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "priority must be one of: high, medium, low"})
		return
	}

	// Extract prompts from input
	prompts := make([]string, len(req.Input))
	for i, item := range req.Input {
		prompts[i] = item["prompt"]
	}

	job := h.queue.Submit(InferenceRequest{
		Model:     model,
		Prompts:   prompts,
		MaxTokens: maxTokens,
	}, priority)

	log.Printf("enqueued job %s (model=%s, prompts=%d, priority=%s)", job.ID, model, len(prompts), priority)

	writeJSON(w, http.StatusAccepted, BatchSubmitResponse{
		JobID:    job.ID,
		Status:   job.State,
		Priority: priority,
	})
}

func (h *Handlers) getBatchStatus(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")
	if jobID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id is required"})
		return
	}

	job := h.queue.GetJob(jobID)
	if job == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}

	resp := BatchStatusResponse{
		JobID:    jobID,
		Status:   job.State,
		Priority: job.PriorityName,
		Message:  job.Message,
		Results:  job.Results,
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handlers) listBatches(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 200 {
		limit = 200
	}

	jobs := h.queue.ListJobs(limit)

	items := make([]BatchListItem, 0, len(jobs))
	for _, job := range jobs {
		item := BatchListItem{
			JobID:       job.ID,
			Status:      job.State,
			Priority:    job.PriorityName,
			Model:       job.InferenceReq.Model,
			PromptCount: len(job.InferenceReq.Prompts),
			Message:     job.Message,
			EnqueuedAt:  job.EnqueuedAt,
		}
		if !job.StartedAt.IsZero() {
			t := job.StartedAt
			item.StartedAt = &t
		}
		if !job.CompletedAt.IsZero() {
			t := job.CompletedAt
			item.CompletedAt = &t
			item.DurationSeconds = job.CompletedAt.Sub(job.StartedAt).Seconds()
		} else if !job.StartedAt.IsZero() {
			// Still running — duration is time since start
			item.DurationSeconds = time.Since(job.StartedAt).Seconds()
		}
		items = append(items, item)
	}

	writeJSON(w, http.StatusOK, BatchListResponse{Jobs: items})
}

func (h *Handlers) getQueueStatus(w http.ResponseWriter, r *http.Request) {
	active := h.queue.QueueInfo()
	writeJSON(w, http.StatusOK, map[string]int{
		"active_jobs": active,
	})
}

func (h *Handlers) healthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}

func (h *Handlers) deepHealthCheck(w http.ResponseWriter, r *http.Request) {
	rayStatus := "reachable"
	if err := h.ray.Healthz(); err != nil {
		rayStatus = fmt.Sprintf("unreachable: %v", err)
	}

	serveStatus := "ready"
	if err := h.ray.ServeHealthz(); err != nil {
		serveStatus = fmt.Sprintf("not ready: %v", err)
	}

	redisStatus := "not configured"
	if h.queue.store != nil {
		if err := h.queue.store.Healthz(r.Context()); err != nil {
			redisStatus = fmt.Sprintf("unhealthy: %v", err)
		} else {
			redisStatus = "healthy"
		}
	}

	// Overall status is unhealthy if any critical component is down
	overallStatus := "healthy"
	if rayStatus != "reachable" || serveStatus != "ready" {
		overallStatus = "degraded"
	}

	statusCode := http.StatusOK
	if overallStatus == "degraded" {
		statusCode = http.StatusServiceUnavailable
	}

	writeJSON(w, statusCode, map[string]string{
		"status":        overallStatus,
		"ray_dashboard": rayStatus,
		"vllm_serve":    serveStatus,
		"redis":         redisStatus,
	})
}

// --- Middleware ---

func (h *Handlers) apiKeyAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		if key == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing X-API-Key header"})
			return
		}
		if subtle.ConstantTimeCompare([]byte(key), []byte(h.cfg.APIKey)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid API key"})
			return
		}
		next(w, r)
	}
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
