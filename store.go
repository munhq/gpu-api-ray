package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

// JobStore persists job state to Dragonfly/Redis.
type JobStore struct {
	client *redis.Client
	ttl    time.Duration
}

// jobRecord is the serializable form of a Job for Redis storage.
type jobRecord struct {
	ID           string              `json:"id"`
	Priority     int                 `json:"priority"`
	PriorityName string              `json:"priority_name"`
	State        string              `json:"state"`
	Model        string              `json:"model"`
	Prompts      []string            `json:"prompts"`
	MaxTokens    int                 `json:"max_tokens"`
	Results      []map[string]string `json:"results,omitempty"`
	Message      string              `json:"message,omitempty"`
	EnqueuedAt   time.Time           `json:"enqueued_at"`
	StartedAt    time.Time           `json:"started_at,omitempty"`
	CompletedAt  time.Time           `json:"completed_at,omitempty"`
}

func NewJobStore(addr string, ttlSeconds int) (*JobStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		DB:           1, // DB 0 is used by Ray GCS
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		DialTimeout:  5 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connect to redis at %s: %w", addr, err)
	}

	log.Printf("connected to redis at %s (db=1, ttl=%ds)", addr, ttlSeconds)

	return &JobStore{
		client: client,
		ttl:    time.Duration(ttlSeconds) * time.Second,
	}, nil
}

func jobKey(id string) string {
	return "job:" + id
}

// Save persists a job to Redis. Called after state changes.
func (s *JobStore) Save(ctx context.Context, job *Job) {
	rec := jobRecord{
		ID:           job.ID,
		Priority:     job.Priority,
		PriorityName: job.PriorityName,
		State:        job.State,
		Model:        job.InferenceReq.Model,
		Prompts:      job.InferenceReq.Prompts,
		MaxTokens:    job.InferenceReq.MaxTokens,
		Results:      job.Results,
		Message:      job.Message,
		EnqueuedAt:   job.EnqueuedAt,
		StartedAt:    job.StartedAt,
		CompletedAt:  job.CompletedAt,
	}

	data, err := json.Marshal(rec)
	if err != nil {
		log.Printf("failed to marshal job %s for redis: %v", job.ID, err)
		return
	}

	if err := s.client.Set(ctx, jobKey(job.ID), data, s.ttl).Err(); err != nil {
		log.Printf("failed to save job %s to redis: %v", job.ID, err)
	}
}

// Load retrieves a job from Redis. Returns nil if not found.
func (s *JobStore) Load(ctx context.Context, id string) *Job {
	data, err := s.client.Get(ctx, jobKey(id)).Bytes()
	if err != nil {
		return nil
	}

	var rec jobRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		log.Printf("failed to unmarshal job %s from redis: %v", id, err)
		return nil
	}

	return &Job{
		ID:           rec.ID,
		Priority:     rec.Priority,
		PriorityName: rec.PriorityName,
		State:        rec.State,
		InferenceReq: InferenceRequest{
			Model:     rec.Model,
			Prompts:   rec.Prompts,
			MaxTokens: rec.MaxTokens,
		},
		Results:     rec.Results,
		Message:     rec.Message,
		EnqueuedAt:  rec.EnqueuedAt,
		StartedAt:   rec.StartedAt,
		CompletedAt: rec.CompletedAt,
	}
}

// ListRecent returns up to limit recent jobs from Redis using SCAN + MGET.
func (s *JobStore) ListRecent(ctx context.Context, limit int) []*Job {
	var keys []string
	iter := s.client.Scan(ctx, 0, "job:*", 0).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
		if len(keys) >= limit*2 { // over-fetch since we'll sort and trim
			break
		}
	}
	if err := iter.Err(); err != nil {
		log.Printf("redis SCAN error: %v", err)
		return nil
	}
	if len(keys) == 0 {
		return nil
	}

	vals, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		log.Printf("redis MGET error: %v", err)
		return nil
	}

	var jobs []*Job
	for _, v := range vals {
		str, ok := v.(string)
		if !ok || str == "" {
			continue
		}
		var rec jobRecord
		if err := json.Unmarshal([]byte(str), &rec); err != nil {
			continue
		}
		jobs = append(jobs, &Job{
			ID:           rec.ID,
			Priority:     rec.Priority,
			PriorityName: rec.PriorityName,
			State:        rec.State,
			InferenceReq: InferenceRequest{
				Model:     rec.Model,
				Prompts:   rec.Prompts,
				MaxTokens: rec.MaxTokens,
			},
			Results:     rec.Results,
			Message:     rec.Message,
			EnqueuedAt:  rec.EnqueuedAt,
			StartedAt:   rec.StartedAt,
			CompletedAt: rec.CompletedAt,
		})
	}

	// Sort by enqueued time descending (most recent first)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].EnqueuedAt.After(jobs[j].EnqueuedAt)
	})

	if len(jobs) > limit {
		jobs = jobs[:limit]
	}
	return jobs
}

// Healthz checks if Redis is reachable.
func (s *JobStore) Healthz(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// Close shuts down the Redis connection.
func (s *JobStore) Close() error {
	return s.client.Close()
}
