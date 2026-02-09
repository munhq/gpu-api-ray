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

	ray := NewRayClient(cfg.RayDashboardURL)

	// Deploy vLLM Ray Serve application if not already running
	if ray.IsServeReady() {
		log.Println("vLLM serve app already deployed and running")
	} else {
		log.Printf("deploying vLLM serve app (model=%s)...", cfg.VLLMModel)
		if err := ray.DeployServeApp(cfg.VLLMModel); err != nil {
			log.Fatalf("failed to deploy vLLM serve app: %v", err)
		}
	}

	// Wait for serve endpoint to be ready
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer startupCancel()
	if err := ray.WaitForServeReady(startupCtx); err != nil {
		log.Fatalf("vLLM serve not ready: %v", err)
	}

	queue := NewJobQueue(ray, cfg.MaxConcurrent)

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
	mux.HandleFunc("GET /v1/queue", h.apiKeyAuth(h.getQueueStatus))

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      metricsMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 5 * time.Minute, // allow long inference responses
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("gpu-api listening on :%s", cfg.Port)
		log.Printf("ray dashboard: %s", cfg.RayDashboardURL)
		log.Printf("vLLM model: %s", cfg.VLLMModel)
		log.Printf("max concurrent: %d", cfg.MaxConcurrent)
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
	log.Println("server stopped")
}
