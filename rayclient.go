package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// RayClient talks to the Ray Jobs API and Ray Serve on the head node.
type RayClient struct {
	dashboardURL string // e.g. http://head-svc:8265
	serveURL     string // e.g. http://head-svc:8000
	httpClient   *http.Client
}

func NewRayClient(dashboardURL string) *RayClient {
	// Derive serve URL from dashboard URL (replace port 8265 with 8000)
	serveURL := strings.Replace(dashboardURL, ":8265", ":8000", 1)

	return &RayClient{
		dashboardURL: dashboardURL,
		serveURL:     serveURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute, // long timeout for inference + model loading
		},
	}
}

// --- Ray Jobs API types (kept for serve deployment) ---

type SubmitJobRequest struct {
	Entrypoint        string            `json:"entrypoint"`
	RuntimeEnv        map[string]any    `json:"runtime_env,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
	EntrypointNumCpus float64           `json:"entrypoint_num_cpus,omitempty"`
	EntrypointNumGpus float64           `json:"entrypoint_num_gpus,omitempty"`
}

type SubmitJobResponse struct {
	JobID string `json:"job_id"`
}

type JobStatusResponse struct {
	JobID     string            `json:"job_id"`
	Status    string            `json:"status"`
	Message   string            `json:"message,omitempty"`
	StartTime int64             `json:"start_time,omitempty"`
	EndTime   int64             `json:"end_time,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// --- vLLM Completion types ---

// CompletionRequest is sent to the Ray Serve vLLM endpoint.
type CompletionRequest struct {
	Model     string   `json:"model"`
	Prompt    []string `json:"prompt"`
	MaxTokens int      `json:"max_tokens"`
}

// CompletionResponse from the Ray Serve vLLM endpoint.
type CompletionResponse struct {
	Choices []CompletionChoice `json:"choices"`
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

// --- Ray Serve deployment ---

const vllmServeScript = `
from ray import serve
from vllm import LLM, SamplingParams
import json

@serve.deployment(
    num_replicas=1,
    ray_actor_options={"num_gpus": 1, "runtime_env": {
        "pip": ["vllm", "numpy<2.0", "scipy>=1.14"],
        "env_vars": {
            "VLLM_WORKER_MULTIPROC_METHOD": "spawn",
            "HF_HOME": "/opt/models/huggingface",
            "TRANSFORMERS_CACHE": "/opt/models/huggingface",
        }
    }},
)
class VLLMDeployment:
    def __init__(self, model):
        self.llm = LLM(model=model, trust_remote_code=True)

    async def __call__(self, request):
        data = await request.json()
        prompts = data.get("prompt", [])
        if isinstance(prompts, str):
            prompts = [prompts]
        max_tokens = data.get("max_tokens", 50)
        model = data.get("model", "")
        params = SamplingParams(max_tokens=max_tokens)
        outputs = self.llm.generate(prompts, params)
        choices = []
        for i, output in enumerate(outputs):
            choices.append({"index": i, "text": output.outputs[0].text})
        return {"choices": choices, "model": model}

import sys
model = sys.argv[1] if len(sys.argv) > 1 else "Qwen/Qwen2.5-0.5B-Instruct"
app = VLLMDeployment.bind(model=model)
serve.run(app, name="vllm", route_prefix="/v1/completions", blocking=False)
print("SERVE_DEPLOYED")
`

// IsServeReady checks if the vLLM serve app is already deployed and running.
func (c *RayClient) IsServeReady() bool {
	resp, err := c.httpClient.Get(c.dashboardURL + "/api/serve/applications/")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false
	}

	// Check if "vllm" application exists and is RUNNING
	apps, ok := result["applications"].(map[string]any)
	if !ok {
		return false
	}
	vllmApp, ok := apps["vllm"].(map[string]any)
	if !ok {
		return false
	}
	status, _ := vllmApp["status"].(string)
	return status == "RUNNING"
}

// DeployServeApp deploys the vLLM Ray Serve application by submitting a Ray Job.
func (c *RayClient) DeployServeApp(model string) error {
	scriptB64 := base64.StdEncoding.EncodeToString([]byte(vllmServeScript))
	entrypoint := fmt.Sprintf(
		"printf '%%s' '%s' | base64 -d > /tmp/serve_vllm.py && python3 /tmp/serve_vllm.py '%s'",
		scriptB64, model,
	)

	req := SubmitJobRequest{
		Entrypoint: entrypoint,
		RuntimeEnv: map[string]any{
			"pip": []string{"vllm", "numpy<2.0", "scipy>=1.14"},
		},
		Metadata: map[string]string{
			"purpose": "deploy-vllm-serve",
			"model":   model,
		},
	}

	jobID, err := c.submitJob(req)
	if err != nil {
		return fmt.Errorf("submit serve deployment job: %w", err)
	}

	log.Printf("submitted serve deployment job %s, waiting for completion...", jobID)

	// Poll for job completion (model loading can take a while)
	for i := 0; i < 120; i++ { // up to 10 minutes
		time.Sleep(5 * time.Second)

		status, err := c.getJobStatus(jobID)
		if err != nil {
			log.Printf("serve deploy: failed to get job status: %v", err)
			continue
		}

		switch status.Status {
		case "SUCCEEDED":
			log.Printf("serve deployment job %s completed successfully", jobID)
			return nil
		case "FAILED", "STOPPED":
			return fmt.Errorf("serve deployment job %s failed: %s", jobID, status.Message)
		default:
			if i%6 == 0 { // log every 30s
				log.Printf("serve deployment job %s status: %s", jobID, status.Status)
			}
		}
	}

	return fmt.Errorf("serve deployment job %s timed out after 10 minutes", jobID)
}

// WaitForServeReady polls the serve endpoint until it's ready to accept requests.
func (c *RayClient) WaitForServeReady(ctx context.Context) error {
	log.Println("waiting for vLLM serve endpoint to be ready...")
	for i := 0; i < 60; i++ { // up to 5 minutes
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if c.IsServeReady() {
			log.Println("vLLM serve endpoint is ready")
			return nil
		}

		if i%6 == 0 {
			log.Println("vLLM serve not ready yet, waiting...")
		}
		time.Sleep(5 * time.Second)
	}

	return fmt.Errorf("vLLM serve endpoint not ready after 5 minutes")
}

// EnsureServeApp checks if vLLM serve is running and redeploys if not.
// Safe to call repeatedly — no-op if already healthy.
func (c *RayClient) EnsureServeApp(model string) {
	if c.IsServeReady() {
		return
	}
	log.Println("vLLM serve app not running, redeploying...")
	if err := c.DeployServeApp(model); err != nil {
		log.Printf("failed to redeploy vLLM serve app: %v", err)
	}
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

// --- Internal helpers ---

func (c *RayClient) submitJob(req SubmitJobRequest) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("marshal submit request: %w", err)
	}

	resp, err := c.httpClient.Post(c.dashboardURL+"/api/jobs/", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("submit job to Ray: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("Ray Jobs API returned %d: %s", resp.StatusCode, string(b))
	}

	var result SubmitJobResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode submit response: %w", err)
	}
	return result.JobID, nil
}

func (c *RayClient) getJobStatus(jobID string) (*JobStatusResponse, error) {
	resp, err := c.httpClient.Get(c.dashboardURL + "/api/jobs/" + jobID)
	if err != nil {
		return nil, fmt.Errorf("get job status from Ray: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Ray Jobs API returned %d: %s", resp.StatusCode, string(b))
	}

	var result JobStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode status response: %w", err)
	}
	return &result, nil
}
