# GPU API

REST API for submitting batch inference jobs to a persistent vLLM instance running on a KubeRay RayService. Includes an embedded web dashboard for monitoring jobs in real time.

The container image is `ghcr.io/munhq/gpu-api`. The Kubernetes platform that deploys it — Ansible, ArgoCD and the Helm chart — lives in [munhq/kubernetes_gpu](https://github.com/munhq/kubernetes_gpu).

## How it works

1. Accepts batch inference requests via `POST /v1/batches`
2. Fires them immediately as HTTP requests to the vLLM serve endpoint (Ray Serve)
3. Persists job state to Dragonfly (Redis-compatible) on submission and completion
4. Returns results via `GET /v1/batches/{job_id}`

No queue, no dispatcher. Ray Serve handles load balancing across vLLM replicas, and vLLM handles continuous batching internally. Priority (high/medium/low) is stored as metadata but doesn't affect execution order.

## Dashboard

A built-in web dashboard is served at `GET /`. Port-forward and open in a browser:

```bash
kubectl port-forward svc/gpu-api -n gpu-workloads 8000:8000
# Open http://localhost:8000/
```

Features:
- Summary cards: total, running, succeeded, failed jobs, avg duration, throughput
- Charts: status distribution (doughnut), priority distribution (bar), duration histogram
- Clickable jobs table with inline detail expansion (prompts, outputs, timestamps)
- Auto-refresh every 5s with smooth in-place updates
- API key prompted on first load, stored in localStorage

The dashboard is a single HTML file (`dashboard.html`) embedded in the Go binary at compile time via `go:embed`. No separate build step or deployment required.

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `API_KEY` | Yes | - | API key for authentication (X-API-Key header) |
| `PORT` | No | `8000` | HTTP listen port |
| `RAY_SERVE_URL` | No | `http://raycluster-batch-inference-serve-svc:8000` | Ray Serve vLLM endpoint |
| `RAY_DASHBOARD_URL` | No | `http://raycluster-batch-inference-head-svc:8265` | Ray head node dashboard URL |
| `REDIS_URL` | No | `dragonfly.gpu-workloads.svc.cluster.local:6379` | Dragonfly/Redis for job persistence |
| `DEFAULT_MODEL` | No | `qwen2.5-0.5b-instruct` | Default model for inference |
| `DEFAULT_MAX_TOKENS` | No | `512` | Default max tokens per request |
| `JOB_TTL_SECONDS` | No | `604800` | TTL for completed jobs in Redis (7 days) |

## API Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/` | None | Web dashboard |
| POST | `/v1/batches` | API Key | Submit batch inference job — returns immediately with RUNNING status |
| GET | `/v1/batches/{job_id}` | API Key | Get job status and results |
| GET | `/v1/batches` | None | List recent jobs (cluster-internal, read-only) |
| GET | `/v1/queue` | API Key | Get number of active (in-flight) jobs |
| GET | `/health` | None | Basic health check |
| GET | `/health/deep` | None | Health check including Ray dashboard, vLLM serve, and Redis |
| GET | `/metrics` | None | Prometheus metrics |

## Prometheus Metrics

| Metric | Type | Description |
|---|---|---|
| `gpu_api_jobs_submitted_total` | Counter | Total jobs submitted |
| `gpu_api_jobs_active` | Gauge | In-flight jobs right now |
| `gpu_api_jobs_by_status_total` | Counter | Jobs by terminal status (SUCCEEDED/FAILED) |
| `gpu_api_job_duration_seconds` | Histogram | End-to-end job duration |
| `gpu_api_inference_duration_seconds` | Histogram | vLLM HTTP call latency (inference only) |
| `gpu_api_batch_size` | Histogram | Prompts per batch job |
| `gpu_api_tokens_total` | Counter | Tokens by type (prompt/completion) and model |
| `gpu_api_tokens_per_request` | Histogram | Total tokens per job |
| `gpu_api_jobs_submitted_by_priority_total` | Counter | Jobs by priority level |
| `gpu_api_http_requests_total` | Counter | HTTP requests by method, path, status |
| `gpu_api_http_request_duration_seconds` | Histogram | HTTP request latency |

## Build

```bash
go build -o gpu-api .
```

## Docker

```bash
docker build -t gpu-api .
```

Image is built automatically via GitHub Actions on push to `main` (triggered by changes in `gpu-api/`). Published to `ghcr.io/munhq/gpu-api:latest`.

## Load Testing

```bash
# Port-forward first
kubectl port-forward svc/gpu-api -n gpu-workloads 8000:8000

# Run 99-job load test
GPU_API_KEY=<key> python3 scripts/test_gpu_api_load.py
```
