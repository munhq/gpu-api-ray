package main

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Job states
const (
	JobStateRunning   = "RUNNING"
	JobStateSucceeded = "SUCCEEDED"
	JobStateFailed    = "FAILED"
)

// Job represents a batch inference job.
type Job struct {
	ID           string
	PriorityName string
	State        string
	InferenceReq InferenceRequest
	Results      []map[string]string
	Message      string
	EnqueuedAt   time.Time
	StartedAt    time.Time
	CompletedAt  time.Time
}

// JobQueue manages job submission, execution, and persistence.
// Inference requests are sent directly to vLLM via Ray Serve —
// Ray handles load balancing and vLLM handles continuous batching.
type JobQueue struct {
	mu    sync.Mutex
	ray   *RayClient
	store *JobStore // nil if Redis unavailable
	jobs  map[string]*Job // only active (running) jobs
	ctx   context.Context
}

func NewJobQueue(ray *RayClient, store *JobStore) *JobQueue {
	return &JobQueue{
		ray:   ray,
		store: store,
		jobs:  make(map[string]*Job),
	}
}

// SetContext sets the context for Redis operations.
func (q *JobQueue) SetContext(ctx context.Context) {
	q.ctx = ctx
}

// Submit creates a job and fires it to vLLM immediately.
// Ray Serve distributes requests across vLLM replicas.
func (q *JobQueue) Submit(req InferenceRequest, priority string) *Job {
	job := &Job{
		ID:           uuid.New().String(),
		PriorityName: priority,
		State:        JobStateRunning,
		InferenceReq: req,
		EnqueuedAt:   time.Now(),
		StartedAt:    time.Now(),
	}

	q.mu.Lock()
	q.jobs[job.ID] = job
	q.mu.Unlock()

	q.persistJob(job)

	jobsSubmitted.Inc()
	jobsActive.Inc()
	jobsSubmittedByPriority.WithLabelValues(priority).Inc()
	batchSize.Observe(float64(len(req.Prompts)))

	log.Printf("submitted job %s (model=%s, prompts=%d, priority=%s)",
		job.ID, req.Model, len(req.Prompts), priority)

	go q.executeJob(job)
	return job
}

// GetJob returns the job state. Checks in-memory for active jobs, Redis for completed jobs.
func (q *JobQueue) GetJob(id string) *Job {
	q.mu.Lock()
	job := q.jobs[id]
	q.mu.Unlock()

	if job != nil {
		return job
	}

	if q.store != nil {
		return q.store.Load(q.ctx, id)
	}
	return nil
}

// ListJobs returns recent jobs, prioritizing Redis data for completed jobs.
func (q *JobQueue) ListJobs(limit int) []*Job {
	var result []*Job
	seen := make(map[string]bool)

	if q.store != nil {
		redisJobs := q.store.ListRecent(q.ctx, limit)
		for _, job := range redisJobs {
			result = append(result, job)
			seen[job.ID] = true
		}
	}

	q.mu.Lock()
	for _, job := range q.jobs {
		if !seen[job.ID] {
			result = append(result, job)
			seen[job.ID] = true
		}
	}
	q.mu.Unlock()

	sort.Slice(result, func(i, j int) bool {
		return result[i].EnqueuedAt.After(result[j].EnqueuedAt)
	})

	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

// QueueInfo returns current active job count.
func (q *JobQueue) QueueInfo() (active int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}

// executeJob sends the inference request to vLLM and updates the job state.
func (q *JobQueue) executeJob(job *Job) {
	start := time.Now()

	resp, err := q.ray.Complete(q.ctx, CompletionRequest{
		Model:     job.InferenceReq.Model,
		Prompt:    job.InferenceReq.Prompts,
		MaxTokens: job.InferenceReq.MaxTokens,
	})

	q.mu.Lock()
	defer q.mu.Unlock()

	duration := time.Since(start).Seconds()
	job.CompletedAt = time.Now()

	model := job.InferenceReq.Model
	inferenceDuration.WithLabelValues(model).Observe(duration)

	if err != nil {
		job.State = JobStateFailed
		job.Message = err.Error()
		jobsByStatus.WithLabelValues("FAILED").Inc()
		log.Printf("job %s FAILED after %.1fs: %v", job.ID, duration, err)
	} else {
		job.State = JobStateSucceeded
		job.Message = "completed"

		results := make([]map[string]string, len(job.InferenceReq.Prompts))
		for _, choice := range resp.Choices {
			if choice.Index < 0 || choice.Index >= len(results) {
				log.Printf("job %s: vLLM returned out-of-range choice index %d (prompts=%d), skipping", job.ID, choice.Index, len(results))
				continue
			}
			prompt := job.InferenceReq.Prompts[choice.Index]
			results[choice.Index] = map[string]string{
				"prompt": prompt,
				"output": choice.Text,
			}
		}
		job.Results = results

		tokensTotal.WithLabelValues("prompt", model).Add(float64(resp.Usage.PromptTokens))
		tokensTotal.WithLabelValues("completion", model).Add(float64(resp.Usage.CompletionTokens))
		tokensPerRequest.Observe(float64(resp.Usage.TotalTokens))

		jobsByStatus.WithLabelValues("SUCCEEDED").Inc()
		log.Printf("job %s SUCCEEDED in %.1fs (%d prompts, %d tokens)", job.ID, duration, len(resp.Choices), resp.Usage.TotalTokens)
	}

	q.persistJob(job)

	if job.State == JobStateSucceeded || job.State == JobStateFailed {
		delete(q.jobs, job.ID)
	}

	jobDuration.Observe(duration)
	jobsActive.Dec()
}

// persistJob saves job state to Redis (non-blocking, best-effort).
func (q *JobQueue) persistJob(job *Job) {
	if q.store == nil {
		return
	}
	q.store.Save(q.ctx, job)
}
