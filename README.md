# GPU API

REST API for submitting batch inference jobs to a persistent vLLM instance running on a KubeRay RayService.

## Architecture

The GPU API is a stateless HTTP service that:
1. Accepts batch inference requests via `POST /v1/batches`
2. Queues them in an in-process priority queue (high/medium/low)
3. Dispatches them as HTTP requests to the vLLM serve endpoint managed by a RayService CRD
4. Returns results via `GET /v1/batches/{job_id}`

The vLLM model lifecycle is managed entirely by the KubeRay operator via the RayService CRD — no serve deployment code in the Go API.

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `API_KEY` | Yes | - | API key for authentication (X-API-Key header) |
| `PORT` | No | `8000` | HTTP listen port |
| `RAY_DASHBOARD_URL` | No | `http://raycluster-batch-inference-head-svc:8265` | Ray head node dashboard URL |
| `RAY_SERVE_URL` | No | `http://raycluster-batch-inference-serve-svc:8000` | Ray Serve vLLM endpoint |
| `DEFAULT_MODEL` | No | `Qwen/Qwen2.5-0.5B-Instruct` | Default model for inference |
| `DEFAULT_MAX_TOKENS` | No | `50` | Default max tokens per request |
| `MAX_CONCURRENT` | No | `4` | Max concurrent inference requests |

## API Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/v1/batches` | API Key | Submit batch inference job |
| GET | `/v1/batches/{job_id}` | API Key | Get job status and results |
| GET | `/v1/queue` | API Key | Get queue depth and active slots |
| GET | `/health` | None | Basic health check |
| GET | `/health/deep` | None | Health check including Ray dashboard and vLLM serve |
| GET | `/metrics` | None | Prometheus metrics |

## Build

```bash
go build -o gpu-api .
```

## Docker

```bash
docker build -t gpu-api .
```
