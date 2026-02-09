package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	ray := NewRayClient(cfg.RayDashboardURL, cfg.RayServeURL)

	// Connect to Dragonfly/Redis for job persistence
	var store *JobStore
	store, err = NewJobStore(cfg.RedisURL, cfg.JobTTLSeconds)
	if err != nil {
		log.Printf("WARNING: redis unavailable, falling back to in-memory only: %v", err)
		store = nil
	}

	queue := NewJobQueue(ray, store, cfg.MaxConcurrent)

	// Graceful shutdown context
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	queue.Start(ctx)

	h := NewHandlers(cfg, ray, queue)

	mux := http.NewServeMux()

	// Public endpoints
	mux.HandleFunc("GET /health", h.healthCheck)
	mux.HandleFunc("GET /health/deep", h.deepHealthCheck)
	mux.Handle("GET /metrics", promhttp.Handler())

	// Authenticated endpoints
	mux.HandleFunc("POST /v1/batches", h.apiKeyAuth(h.submitBatch))
	mux.HandleFunc("GET /v1/batches/{job_id}", h.apiKeyAuth(h.getBatchStatus))
	mux.HandleFunc("GET /v1/batches", h.apiKeyAuth(h.listBatches))
	mux.HandleFunc("GET /v1/queue", h.apiKeyAuth(h.getQueueStatus))

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      metricsMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 5 * time.Minute,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("gpu-api listening on :%s", cfg.Port)
		log.Printf("ray dashboard: %s", cfg.RayDashboardURL)
		log.Printf("ray serve: %s", cfg.RayServeURL)
		log.Printf("max concurrent: %d", cfg.MaxConcurrent)
		if store != nil {
			log.Printf("redis: %s (job ttl=%ds)", cfg.RedisURL, cfg.JobTTLSeconds)
		}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("shutdown error: %v", err)
	}
	if store != nil {
		store.Close()
	}
	log.Println("server stopped")
}
