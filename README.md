# GPU API

REST API for submitting batch inference jobs to a persistent RayCluster via the Ray Jobs API.

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `API_KEY` | Yes | - | API key for authentication (X-API-Key header) |
| `PORT` | No | `8000` | HTTP listen port |
| `RAY_DASHBOARD_URL` | No | `http://raycluster-batch-inference-head-svc:8265` | Ray head node dashboard URL |
| `DEFAULT_MODEL` | No | `Qwen/Qwen2.5-0.5B-Instruct` | Default model for inference |
| `DEFAULT_MAX_TOKENS` | No | `50` | Default max tokens per request |
| `NAMESPACE` | No | `gpu-workloads` | Kubernetes namespace |
| `JOB_TTL_SECONDS` | No | `3600` | Job TTL after completion |

## API Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/v1/batches` | API Key | Submit batch inference job |
| GET | `/v1/batches/{job_id}` | API Key | Get job status and results |
| GET | `/health` | None | Health check (includes Ray dashboard reachability) |
| GET | `/metrics` | None | Prometheus metrics |

## Build

```bash
go build -o gpu-api .
```

## Docker

```bash
docker build -t gpu-api .
```
