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

	// Use KubeRayClient for Kubernetes-native job submission with Kueue integration
	ray, err := NewKubeRayClient(cfg.Namespace, cfg.RayClusterName, cfg.KueueQueueName)
	if err != nil {
		log.Fatalf("failed to create KubeRay client: %v", err)
	}
	h := NewHandlers(cfg, ray)

	mux := http.NewServeMux()

	// Public endpoints
	mux.HandleFunc("GET /health", h.healthCheck)
	mux.HandleFunc("GET /health/deep", h.deepHealthCheck)
	mux.Handle("GET /metrics", promhttp.Handler())

	// Authenticated endpoints
	mux.HandleFunc("POST /v1/batches", h.apiKeyAuth(h.submitBatch))
	mux.HandleFunc("GET /v1/batches/{job_id}", h.apiKeyAuth(h.getBatchStatus))

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      metricsMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("gpu-api listening on :%s", cfg.Port)
		log.Printf("target RayCluster: %s/%s", cfg.Namespace, cfg.RayClusterName)
		log.Printf("kueue queue: %s", cfg.KueueQueueName)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("shutdown error: %v", err)
	}
	log.Println("server stopped")
}
