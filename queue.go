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
	JobStateSubmitted = "SUBMITTED"
	JobStateRunning   = "RUNNING"
	JobStateSucceeded = "SUCCEEDED"
	JobStateFailed    = "FAILED"
	JobStateStopped   = "STOPPED"
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
	SubmitReq    SubmitJobRequest
	RayJobID     string // set once submitted to Ray
	Results      []map[string]string
	Message      string
	EnqueuedAt   time.Time
	SubmittedAt  time.Time
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
	old[n-1] = nil // avoid memory leak
	job.index = -1
	*h = old[:n-1]
	return job
}

// --- JobQueue ---

type JobQueue struct {
	mu         sync.Mutex
	ray        JobClient
	maxGPUs    int
	activeGPUs int
	seqCounter int
	pq         jobHeap
	jobs       map[string]*Job // all jobs by ID (queued + active + terminal)
	dispatch   chan struct{}    // signal to attempt dispatch
}

func NewJobQueue(ray JobClient, maxGPUs int) *JobQueue {
	q := &JobQueue{
		ray:      ray,
		maxGPUs:  maxGPUs,
		pq:       make(jobHeap, 0),
		jobs:     make(map[string]*Job),
		dispatch: make(chan struct{}, 1),
	}
	heap.Init(&q.pq)
	gpusTotal.Set(float64(maxGPUs))
	return q
}

// Enqueue adds a job to the priority queue and signals the dispatcher.
func (q *JobQueue) Enqueue(req SubmitJobRequest, priority string) *Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	job := &Job{
		ID:           uuid.New().String(),
		Priority:     parsePriority(priority),
		PriorityName: priority,
		State:        JobStateQueued,
		SubmitReq:    req,
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

// GetJob returns a snapshot of the job state. Returns nil if not found.
func (q *JobQueue) GetJob(id string) *Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.jobs[id]
}

// QueueInfo returns current queue depth and active GPU count.
func (q *JobQueue) QueueInfo() (depth int, active int, maxGPUs int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pq.Len(), q.activeGPUs, q.maxGPUs
}

// Start launches the dispatcher and reconciler goroutines.
// They run until ctx is cancelled.
func (q *JobQueue) Start(ctx context.Context) {
	go q.dispatchLoop(ctx)
	go q.reconcileLoop(ctx)
	log.Printf("queue started: max_gpus=%d", q.maxGPUs)
}

func (q *JobQueue) signalDispatch() {
	select {
	case q.dispatch <- struct{}{}:
	default:
		// already signalled
	}
}

// dispatchLoop dequeues highest-priority jobs whenever GPU slots are available.
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

	for q.activeGPUs < q.maxGPUs && q.pq.Len() > 0 {
		job := heap.Pop(&q.pq).(*Job)
		queueDepth.Set(float64(q.pq.Len()))

		waitDuration := time.Since(job.EnqueuedAt).Seconds()
		queueWaitSeconds.Observe(waitDuration)

		// Submit to Ray outside of the lock would be better for latency,
		// but keeping it simple — Ray submit is fast (HTTP POST).
		rayJobID, err := q.ray.SubmitJob(job.SubmitReq)
		if err != nil {
			log.Printf("failed to submit job %s to Ray: %v", job.ID, err)
			job.State = JobStateFailed
			job.Message = "failed to submit to Ray: " + err.Error()
			job.CompletedAt = time.Now()
			jobsByStatus.WithLabelValues("FAILED").Inc()
			continue
		}

		job.RayJobID = rayJobID
		job.State = JobStateSubmitted
		job.SubmittedAt = time.Now()
		q.activeGPUs++

		gpusActive.Set(float64(q.activeGPUs))
		jobsSubmitted.Inc()
		jobsActive.Inc()

		log.Printf("dispatched job %s → Ray %s (priority=%s, waited=%.1fs, gpus=%d/%d)",
			job.ID, rayJobID, job.PriorityName, waitDuration, q.activeGPUs, q.maxGPUs)
	}
}

// reconcileLoop polls Ray for active job statuses and frees GPU slots on completion.
func (q *JobQueue) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.reconcile()
		}
	}
}

func (q *JobQueue) reconcile() {
	q.mu.Lock()
	// Collect jobs that need status checks
	var active []*Job
	for _, job := range q.jobs {
		if job.State == JobStateSubmitted || job.State == JobStateRunning {
			active = append(active, job)
		}
	}
	q.mu.Unlock()

	if len(active) == 0 {
		return
	}

	freedSlots := 0

	for _, job := range active {
		status, err := q.ray.GetJobStatus(job.RayJobID)
		if err != nil {
			log.Printf("reconcile: failed to get status for job %s (ray=%s): %v", job.ID, job.RayJobID, err)
			continue
		}
		if status == nil {
			continue
		}

		q.mu.Lock()
		switch status.Status {
		case "RUNNING":
			if job.State != JobStateRunning {
				job.State = JobStateRunning
				log.Printf("job %s (ray=%s) now RUNNING", job.ID, job.RayJobID)
			}
		case "SUCCEEDED":
			job.State = JobStateSucceeded
			job.CompletedAt = time.Now()
			job.Message = status.Message

			// Cache results from logs
			logs, err := q.ray.GetJobLogs(job.RayJobID)
			if err != nil {
				log.Printf("reconcile: failed to get logs for job %s: %v", job.ID, err)
				job.Message = "Job succeeded but failed to retrieve results."
			} else {
				results := extractResults(logs)
				if results != nil {
					job.Results = results
				} else {
					job.Message = "Job succeeded but results could not be parsed from logs."
				}
			}

			if status.StartTime > 0 && status.EndTime > 0 {
				dur := float64(status.EndTime-status.StartTime) / 1000.0
				jobDuration.Observe(dur)
			}

			q.activeGPUs--
			freedSlots++
			jobsByStatus.WithLabelValues("SUCCEEDED").Inc()
			jobsActive.Dec()
			log.Printf("job %s (ray=%s) SUCCEEDED, freed GPU slot (%d/%d active)",
				job.ID, job.RayJobID, q.activeGPUs, q.maxGPUs)

		case "FAILED", "STOPPED":
			job.State = status.Status
			job.CompletedAt = time.Now()
			job.Message = status.Message
			q.activeGPUs--
			freedSlots++
			jobsByStatus.WithLabelValues(status.Status).Inc()
			jobsActive.Dec()
			log.Printf("job %s (ray=%s) %s, freed GPU slot (%d/%d active)",
				job.ID, job.RayJobID, status.Status, q.activeGPUs, q.maxGPUs)
		}
		q.mu.Unlock()
	}

	if freedSlots > 0 {
		gpusActive.Set(float64(q.activeGPUs))
		q.signalDispatch()
	}
}
