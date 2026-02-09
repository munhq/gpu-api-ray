package main

import (
	"container/heap"
	"context"
	"log"
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
	maxConcurrent int
	activeSlots   int
	seqCounter    int
	pq            jobHeap
	jobs          map[string]*Job
	dispatch      chan struct{}
	ctx           context.Context
}

func NewJobQueue(ray *RayClient, maxConcurrent int) *JobQueue {
	q := &JobQueue{
		ray:           ray,
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

	q.signalDispatch()
	return job
}

// GetJob returns the job state. Returns nil if not found.
func (q *JobQueue) GetJob(id string) *Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.jobs[id]
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

		log.Printf("dispatched job %s (priority=%s, waited=%.1fs, slots=%d/%d)",
			job.ID, job.PriorityName, waitDuration, q.activeSlots, q.maxConcurrent)

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

	if err != nil {
		job.State = JobStateFailed
		job.Message = err.Error()
		jobsByStatus.WithLabelValues("FAILED").Inc()
		log.Printf("job %s FAILED after %.1fs: %v", job.ID, duration, err)
	} else {
		job.State = JobStateSucceeded
		job.Message = "completed"

		// Convert vLLM response to our results format (prompt + output pairs)
		results := make([]map[string]string, len(resp.Choices))
		for _, choice := range resp.Choices {
			prompt := ""
			if choice.Index < len(job.InferenceReq.Prompts) {
				prompt = job.InferenceReq.Prompts[choice.Index]
			}
			results[choice.Index] = map[string]string{
				"prompt": prompt,
				"output": choice.Text,
			}
		}
		job.Results = results

		jobsByStatus.WithLabelValues("SUCCEEDED").Inc()
		log.Printf("job %s SUCCEEDED in %.1fs (%d prompts)", job.ID, duration, len(resp.Choices))
	}

	jobDuration.Observe(duration)
	q.activeSlots--
	gpusActive.Set(float64(q.activeSlots))
	jobsActive.Dec()

	// Signal dispatcher to pick up next queued job
	q.signalDispatch()
}
