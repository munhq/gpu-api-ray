package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// RayClient talks to the Ray Jobs API on the head node.
type RayClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewRayClient(dashboardURL string) *RayClient {
	return &RayClient{
		baseURL: dashboardURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// SubmitJobRequest is the payload for POST /api/jobs/
type SubmitJobRequest struct {
	Entrypoint  string            `json:"entrypoint"`
	RuntimeEnv  map[string]any    `json:"runtime_env,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	EntrypointNumCpus float64    `json:"entrypoint_num_cpus,omitempty"`
	EntrypointNumGpus float64    `json:"entrypoint_num_gpus,omitempty"`
}

// SubmitJobResponse is the response from POST /api/jobs/
type SubmitJobResponse struct {
	JobID string `json:"job_id"` // e.g. "raysubmit_abc123"
}

// JobStatusResponse is the response from GET /api/jobs/{id}
type JobStatusResponse struct {
	JobID     string            `json:"job_id"`
	Status    string            `json:"status"` // PENDING, RUNNING, SUCCEEDED, FAILED, STOPPED
	Message   string            `json:"message,omitempty"`
	StartTime int64             `json:"start_time,omitempty"`
	EndTime   int64             `json:"end_time,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// JobLogsResponse is the response from GET /api/jobs/{id}/logs
type JobLogsResponse struct {
	Logs string `json:"logs"`
}

// SubmitJob submits a job to the Ray cluster.
func (c *RayClient) SubmitJob(req SubmitJobRequest) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("marshal submit request: %w", err)
	}

	resp, err := c.httpClient.Post(c.baseURL+"/api/jobs/", "application/json", bytes.NewReader(body))
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

// GetJobStatus retrieves the status of a Ray job.
func (c *RayClient) GetJobStatus(jobID string) (*JobStatusResponse, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/api/jobs/" + jobID)
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

// GetJobLogs retrieves the stdout/stderr logs of a Ray job.
func (c *RayClient) GetJobLogs(jobID string) (string, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/api/jobs/" + jobID + "/logs")
	if err != nil {
		return "", fmt.Errorf("get job logs from Ray: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("Ray Jobs API returned %d: %s", resp.StatusCode, string(b))
	}

	var result JobLogsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode logs response: %w", err)
	}
	return result.Logs, nil
}

// Healthz pings the Ray dashboard health endpoint.
func (c *RayClient) Healthz() error {
	resp, err := c.httpClient.Get(c.baseURL + "/api/version")
	if err != nil {
		return fmt.Errorf("ray dashboard unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ray dashboard returned status %d", resp.StatusCode)
	}
	return nil
}
