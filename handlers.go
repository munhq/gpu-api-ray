package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"text/template"
)

// --- Request / Response types ---

type BatchRequest struct {
	Model     string              `json:"model,omitempty"`
	Input     []map[string]string `json:"input"`
	MaxTokens int                 `json:"max_tokens,omitempty"`
	Priority  string              `json:"priority,omitempty"`
}

type BatchSubmitResponse struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

type BatchStatusResponse struct {
	JobID   string              `json:"job_id"`
	Status  string              `json:"status"`
	Results []map[string]string `json:"results,omitempty"`
	Message string              `json:"message,omitempty"`
}

// --- Python entrypoint template ---

const pythonEntrypointTmpl = `import json
from vllm import LLM, SamplingParams

model = {{.Model | printf "%q"}}
prompts = json.loads({{.PromptsJSON | printf "%q"}})
max_tokens = {{.MaxTokens}}

llm = LLM(model=model, trust_remote_code=True)
params = SamplingParams(max_tokens=max_tokens)
outputs = llm.generate([p["prompt"] for p in prompts], params)

results = []
for prompt, output in zip(prompts, outputs):
    results.append({"prompt": prompt["prompt"], "output": output.outputs[0].text})

print("RESULTS_START")
print(json.dumps(results))
print("RESULTS_END")
`

var entrypointTemplate = template.Must(template.New("entrypoint").Parse(pythonEntrypointTmpl))

type entrypointData struct {
	Model       string
	PromptsJSON string
	MaxTokens   int
}

// --- Handlers ---

type Handlers struct {
	cfg *Config
	ray *RayClient
}

func NewHandlers(cfg *Config, ray *RayClient) *Handlers {
	return &Handlers{cfg: cfg, ray: ray}
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
	if maxTokens > 4096 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max_tokens must be between 1 and 4096"})
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

	// Build Python entrypoint via template (safe interpolation)
	promptsJSON, err := json.Marshal(req.Input)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to serialize prompts"})
		return
	}

	var scriptBuf bytes.Buffer
	if err := entrypointTemplate.Execute(&scriptBuf, entrypointData{
		Model:       model,
		PromptsJSON: string(promptsJSON),
		MaxTokens:   maxTokens,
	}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to build entrypoint script"})
		return
	}

	// Submit to Ray Jobs API
	// Base64-encode script to avoid shell escaping issues with python -c
	scriptB64 := base64.StdEncoding.EncodeToString(scriptBuf.Bytes())
	jobID, err := h.ray.SubmitJob(SubmitJobRequest{
		Entrypoint:        fmt.Sprintf("echo %s | base64 -d > /tmp/job.py && python3 /tmp/job.py", scriptB64),
		EntrypointNumGpus: 1,
		RuntimeEnv: map[string]any{
			"pip": []string{"vllm"},
			"env_vars": map[string]string{
				"VLLM_WORKER_MULTIPROC_METHOD": "spawn",
				"HF_HOME":                      "/opt/models/huggingface",
				"TRANSFORMERS_CACHE":            "/opt/models/huggingface",
			},
		},
		Metadata: map[string]string{
			"model":    model,
			"priority": priority,
		},
	})
	if err != nil {
		log.Printf("failed to submit job to Ray: %v", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": fmt.Sprintf("failed to submit job: %v", err)})
		return
	}

	jobsSubmitted.Inc()
	jobsActive.Inc()

	log.Printf("submitted job %s (model=%s, prompts=%d, priority=%s)", jobID, model, len(req.Input), priority)

	writeJSON(w, http.StatusOK, BatchSubmitResponse{
		JobID:  jobID,
		Status: "PENDING",
	})
}

func (h *Handlers) getBatchStatus(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")
	if jobID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id is required"})
		return
	}

	status, err := h.ray.GetJobStatus(jobID)
	if err != nil {
		log.Printf("failed to get job status for %s: %v", jobID, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": fmt.Sprintf("failed to get job status: %v", err)})
		return
	}
	if status == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}

	resp := BatchStatusResponse{
		JobID:   jobID,
		Status:  status.Status,
		Message: status.Message,
	}

	// If succeeded, extract results from logs
	if status.Status == "SUCCEEDED" {
		jobsByStatus.WithLabelValues("SUCCEEDED").Inc()
		jobsActive.Dec()

		if status.StartTime > 0 && status.EndTime > 0 {
			dur := float64(status.EndTime-status.StartTime) / 1000.0
			jobDuration.Observe(dur)
		}

		logs, err := h.ray.GetJobLogs(jobID)
		if err != nil {
			log.Printf("failed to get logs for job %s: %v", jobID, err)
			resp.Message = "Job succeeded but failed to retrieve results."
		} else {
			results := extractResults(logs)
			if results != nil {
				resp.Results = results
			} else {
				resp.Message = "Job succeeded but results could not be parsed from logs."
			}
		}
	} else if status.Status == "FAILED" || status.Status == "STOPPED" {
		jobsByStatus.WithLabelValues(status.Status).Inc()
		jobsActive.Dec()
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handlers) healthCheck(w http.ResponseWriter, r *http.Request) {
	// Always return healthy for liveness/readiness probes.
	// Ray dashboard reachability is informational only — we don't want
	// a slow VPN hop to the GPU node to kill our pod.
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}

// deepHealthCheck includes Ray dashboard reachability (for manual debugging, not probes).
func (h *Handlers) deepHealthCheck(w http.ResponseWriter, r *http.Request) {
	rayStatus := "reachable"
	if err := h.ray.Healthz(); err != nil {
		rayStatus = fmt.Sprintf("unreachable: %v", err)
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":        "healthy",
		"ray_dashboard": rayStatus,
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
		if key != h.cfg.APIKey {
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

// extractResults parses JSON between RESULTS_START and RESULTS_END markers in logs.
func extractResults(logs string) []map[string]string {
	startMarker := "RESULTS_START"
	endMarker := "RESULTS_END"

	startIdx := strings.Index(logs, startMarker)
	if startIdx == -1 {
		return nil
	}
	startIdx += len(startMarker)

	endIdx := strings.Index(logs[startIdx:], endMarker)
	if endIdx == -1 {
		return nil
	}

	jsonStr := strings.TrimSpace(logs[startIdx : startIdx+endIdx])

	var results []map[string]string
	if err := json.Unmarshal([]byte(jsonStr), &results); err != nil {
		return nil
	}
	return results
}
