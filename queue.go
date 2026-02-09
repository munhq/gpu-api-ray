package main

import (
	"container/heap"
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Job states
const (
	JobStateQueued    = "QUEUED"
	JobStateRunning   = "RUNNING"
	JobStateSucceeded = "SUCCEEDED"
	JobStateFailed    = "FAILED"
)

// Priority values — higher runs first
const (
	PriorityHigh   = 1000
	PriorityMedium = 500
	PriorityLow    = 100
)

func parsePriority(s string) int {
	switch s {
	case "high":
		return PriorityHigh
	case "low":
		return PriorityLow
	default:
		return PriorityMedium
	}
}

// Job represents a batch inference job tracked by the queue.
type Job struct {
	ID           string
	Priority     int
	PriorityName string
	State        string
	InferenceReq InferenceRequest
	Results      []map[string]string
	Message      string
	EnqueuedAt   time.Time
	StartedAt    time.Time
	CompletedAt  time.Time

	// heap bookkeeping
	seqNum int // monotonic counter for FIFO within same priority
	index  int // index in the heap slice
}

// --- Priority heap implementation ---

type jobHeap []*Job

func (h jobHeap) Len() int { return len(h) }

func (h jobHeap) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority // higher priority first
	}
	return h[i].seqNum < h[j].seqNum // FIFO within same priority
}

func (h jobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *jobHeap) Push(x any) {
	job := x.(*Job)
	job.index = len(*h)
	*h = append(*h, job)
}

func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	job := old[n-1]
	old[n-1] = nil
	job.index = -1
	*h = old[:n-1]
	return job
}

// --- JobQueue ---

type JobQueue struct {
	mu            sync.Mutex
	ray           *RayClient
	store         *JobStore // nil if Redis unavailable (fallback to in-memory only)
	maxConcurrent int
	activeSlots   int
	seqCounter    int
	pq            jobHeap
	jobs          map[string]*Job // only active and queued jobs
	dispatch      chan struct{}
	ctx           context.Context
}

func NewJobQueue(ray *RayClient, store *JobStore, maxConcurrent int) *JobQueue {
	q := &JobQueue{
		ray:           ray,
		store:         store,
		maxConcurrent: maxConcurrent,
		pq:            make(jobHeap, 0),
		jobs:          make(map[string]*Job),
		dispatch:      make(chan struct{}, 1),
	}
	heap.Init(&q.pq)
	gpusTotal.Set(float64(maxConcurrent))
	return q
}

// Enqueue adds a job to the priority queue and signals the dispatcher.
func (q *JobQueue) Enqueue(req InferenceRequest, priority string) *Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	job := &Job{
		ID:           uuid.New().String(),
		Priority:     parsePriority(priority),
		PriorityName: priority,
		State:        JobStateQueued,
		InferenceReq: req,
		EnqueuedAt:   time.Now(),
		seqNum:       q.seqCounter,
	}
	q.seqCounter++

	q.jobs[job.ID] = job
	heap.Push(&q.pq, job)
	queueDepth.Set(float64(q.pq.Len()))

	q.persistJob(job)
	q.signalDispatch()
	return job
}

// GetJob returns the job state. Checks in-memory for active/queued jobs, Redis for completed jobs.
func (q *JobQueue) GetJob(id string) *Job {
	q.mu.Lock()
	job := q.jobs[id]
	q.mu.Unlock()

	if job != nil {
		return job // Active or queued job from memory
	}

	// Check Redis for completed jobs
	if q.store != nil {
		return q.store.Load(q.ctx, id)
	}
	return nil
}

// ListJobs returns recent jobs, prioritizing Redis data for completed jobs.
func (q *JobQueue) ListJobs(limit int) []*Job {
	var result []*Job
	seen := make(map[string]bool)

	// First get from Redis (completed jobs + any still there)
	if q.store != nil {
		redisJobs := q.store.ListRecent(q.ctx, limit)
		for _, job := range redisJobs {
			result = append(result, job)
			seen[job.ID] = true
		}
	}

	// Then add any in-memory jobs not already seen (active/queued)
	q.mu.Lock()
	for _, job := range q.jobs {
		if !seen[job.ID] {
			result = append(result, job)
			seen[job.ID] = true
		}
	}
	q.mu.Unlock()

	// Sort by enqueued time descending
	sort.Slice(result, func(i, j int) bool {
		return result[i].EnqueuedAt.After(result[j].EnqueuedAt)
	})

	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

// QueueInfo returns current queue depth and active slot count.
func (q *JobQueue) QueueInfo() (depth int, active int, max int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pq.Len(), q.activeSlots, q.maxConcurrent
}

// Start launches the dispatcher goroutine. Runs until ctx is cancelled.
func (q *JobQueue) Start(ctx context.Context) {
	q.ctx = ctx
	go q.dispatchLoop(ctx)
	log.Printf("queue started: max_concurrent=%d", q.maxConcurrent)
}

func (q *JobQueue) signalDispatch() {
	select {
	case q.dispatch <- struct{}{}:
	default:
	}
}

func (q *JobQueue) dispatchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.dispatch:
			q.tryDispatch()
		}
	}
}

func (q *JobQueue) tryDispatch() {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.activeSlots < q.maxConcurrent && q.pq.Len() > 0 {
		job := heap.Pop(&q.pq).(*Job)
		queueDepth.Set(float64(q.pq.Len()))

		waitDuration := time.Since(job.EnqueuedAt).Seconds()
		queueWaitSeconds.Observe(waitDuration)

		job.State = JobStateRunning
		job.StartedAt = time.Now()
		q.activeSlots++

		gpusActive.Set(float64(q.activeSlots))
		jobsSubmitted.Inc()
		jobsActive.Inc()
		jobsSubmittedByPriority.WithLabelValues(job.PriorityName).Inc()
		batchSize.Observe(float64(len(job.InferenceReq.Prompts)))

		log.Printf("dispatched job %s (priority=%s, waited=%.1fs, slots=%d/%d)",
			job.ID, job.PriorityName, waitDuration, q.activeSlots, q.maxConcurrent)

		q.persistJob(job)

		// Execute inference in a goroutine — HTTP call blocks until vLLM responds
		go q.executeJob(job)
	}
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

		// Convert vLLM response to our results format (prompt + output pairs)
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

		// Record token metrics from vLLM response
		tokensTotal.WithLabelValues("prompt", model).Add(float64(resp.Usage.PromptTokens))
		tokensTotal.WithLabelValues("completion", model).Add(float64(resp.Usage.CompletionTokens))
		tokensPerRequest.Observe(float64(resp.Usage.TotalTokens))

		jobsByStatus.WithLabelValues("SUCCEEDED").Inc()
		log.Printf("job %s SUCCEEDED in %.1fs (%d prompts, %d tokens)", job.ID, duration, len(resp.Choices), resp.Usage.TotalTokens)
	}

	q.persistJob(job)

	// Remove completed jobs from memory immediately (Redis is source of truth)
	if job.State == JobStateSucceeded || job.State == JobStateFailed {
		delete(q.jobs, job.ID)
		log.Printf("removed completed job %s from memory (state: %s)", job.ID, job.State)
	}

	jobDuration.Observe(duration)
	q.activeSlots--
	gpusActive.Set(float64(q.activeSlots))
	jobsActive.Dec()

	// Signal dispatcher to pick up next queued job
	q.signalDispatch()
}

// persistJob saves job state to Redis (non-blocking, best-effort).
func (q *JobQueue) persistJob(job *Job) {
	if q.store == nil {
		return
	}
	q.store.Save(q.ctx, job)
}
