package main

import (
	"context"
	_ "embed"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"internal/provider"
	"internal/provider"
	"internal/provider"
	"internal/provider"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//go:embed dashboard.html
var dashboardHTML []byte

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	ray := NewRayClient(cfg.RayDashboardURL, cfg.RayServeURL)

	var store *JobStore
	store, err = NewJobStore(cfg.RedisURL, cfg.JobTTLSeconds)
	if err != nil {
		log.Printf("WARNING: redis unavailable, falling back to in-memory only: %v", err)
		store = nil
	}

	// Session store (Dragonfly DB 2) for chat sessions
	var sessionStore *SessionStore
	sessionStore, err = NewSessionStore(cfg.RedisURL, cfg.SessionTTLSeconds)
	if err != nil {
		log.Printf("WARNING: session store unavailable, chat sessions disabled: %v", err)
		sessionStore = nil
	}

	// Model discovery — polls Ray Serve /v1/models
	discovery := NewModelDiscovery(cfg.RayServeURL)

	// Request queue (Dragonfly DB 3) for chat requests when models are cold
	var reqQueue *RequestQueue
	reqQueue, err = NewRequestQueue(cfg.RedisURL, cfg.RayServeURL, discovery, cfg.JobTTLSeconds)
	if err != nil {
		log.Printf("WARNING: request queue unavailable, 202 queueing disabled: %v", err)
		reqQueue = nil
	}

	// Batch job queue (existing, backward compat)
	queue := NewJobQueue(ray, store)

	// Router for worker monitoring (kept for /v1/workers endpoint)
	router := NewRouter(sessionStore, time.Duration(cfg.WorkerHealthInterval)*time.Second)

	// Provisioner — directly triggers GPU provisioning when models are cold.
	// Replaces the KEDA → demand-pod → the provisioner chain.
	var provisioner *Provisioner
	if len(cfg.Models) > 0 {
		k8sClient := NewClaimClient(os.Getenv("CLAIM_NAMESPACE"))
		rayHeadAddr := os.Getenv("RAY_HEAD_GCS_ADDRESS")
		provisioner = NewProvisioner(
			buildProviderRegistry(),
			cfg.RedisURL,
			cfg.Models,
			discovery,
			k8sClient,
			rayHeadAddr,
		)
		// Wire full-node secrets (optional — only needed if any model uses nodeType: "full-node")
		provisioner.netbirdSetupKey = os.Getenv("NETBIRD_SETUP_KEY")
		provisioner.k8sJoinURL = os.Getenv("K8S_JOIN_URL")
		provisioner.k8sJoinToken = os.Getenv("K8S_JOIN_TOKEN")
	}

	// Chat handler — forwards to RayService, queues when model unavailable
	chatHandler := NewChatHandler(cfg, sessionStore, reqQueue, discovery, provisioner)

	// Graceful shutdown context
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	queue.SetContext(ctx)

	// Start background goroutines
	go discovery.RunDiscovery(ctx, 10*time.Second)
	go router.RunHealthChecks(ctx)
	if reqQueue != nil {
		go reqQueue.RunProcessor(ctx, 5*time.Second)
	}
	if sessionStore != nil {
		go sessionStore.RunSessionCounter(ctx, 30*time.Second)
	}
	if provisioner != nil {
		provisioner.PublishModelConfig(ctx)
		go provisioner.EnsureAlwaysActive(ctx)
	}

	h := NewHandlers(cfg, ray, queue)

	mux := http.NewServeMux()

	// Dashboard
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(dashboardHTML)
	})

	// Public endpoints
	mux.HandleFunc("GET /health", h.healthCheck)
	mux.HandleFunc("GET /health/deep", h.deepHealthCheck)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /v1/batches", h.listBatches)

	// Authenticated batch endpoints
	mux.HandleFunc("POST /v1/batches", h.apiKeyAuth(h.submitBatch))
	mux.HandleFunc("GET /v1/batches/{job_id}", h.apiKeyAuth(h.getBatchStatus))
	mux.HandleFunc("GET /v1/queue", h.apiKeyAuth(h.getQueueStatus))

	// Authenticated chat endpoints
	mux.HandleFunc("POST /v1/chat/completions", h.apiKeyAuth(chatHandler.handleChatCompletions))

	// Request polling (for 202 queued requests)
	mux.HandleFunc("GET /v1/requests/{request_id}", h.apiKeyAuth(chatHandler.handleGetRequest))

	// Model listing
	mux.HandleFunc("GET /v1/models", h.apiKeyAuth(chatHandler.handleListModels))

	// Worker status endpoints (for monitoring the provisioner workers)
	mux.HandleFunc("GET /v1/workers", h.apiKeyAuth(handleListWorkers(router)))
	mux.HandleFunc("GET /v1/workers/metrics", h.apiKeyAuth(handleProxyWorkerMetrics(router)))

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      metricsMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 5 * time.Minute,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("gpu-api listening on :%s", cfg.Port)
		log.Printf("ray serve: %s", cfg.RayServeURL)
		log.Printf("ray dashboard: %s", cfg.RayDashboardURL)
		if store != nil {
			log.Printf("redis: %s (job ttl=%ds)", cfg.RedisURL, cfg.JobTTLSeconds)
		}
		if sessionStore != nil {
			log.Printf("session store: db=2 (session ttl=%ds)", cfg.SessionTTLSeconds)
		}
		if reqQueue != nil {
			log.Printf("request queue: db=3 (202 queueing enabled)")
		}
		log.Printf("model discovery: polling every 10s")
		if len(cfg.ModelCatalog) > 0 {
			log.Printf("model catalog: %d models configured", len(cfg.ModelCatalog))
			for _, m := range cfg.ModelCatalog {
				mode := "cold"
				if m.AlwaysActive {
					mode = "always-active"
				}
				log.Printf("  model %s: vram=%dGB gpus=%d tp=%d %s", m.ID, m.VRAMRequired, m.GPUCount, m.TensorParallelSize, mode)
			}
		}
		if provisioner != nil {
			log.Printf("provisioner: enabled (%d providers)", len(buildProviderRegistry().List()))
		}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	if sessionStore != nil {
		sessionStore.Close()
	}
	if reqQueue != nil {
		reqQueue.Close()
	}
	if provisioner != nil {
		provisioner.Close()
	}
	log.Println("server stopped")
}

// buildProviderRegistry creates a provider registry from environment variables.
func buildProviderRegistry() *provider.Registry {
	registry := provider.NewRegistry()

	if key := os.Getenv("PROVIDER_A_KEY"); key != "" {
		registry.Register(providera.New(key))
		log.Printf("provisioner: registered provider vast.ai")
	}
	if clientID, clientSecret := os.Getenv("PROVIDER_B_ID"), os.Getenv("PROVIDER_B_SECRET"); clientID != "" && clientSecret != "" {
		registry.Register(providerb.New(clientID, clientSecret))
		log.Printf("provisioner: registered provider providerb")
	}
	if key := os.Getenv("PROVIDER_C_KEY"); key != "" {
		registry.Register(providerc.New(key))
		log.Printf("provisioner: registered provider providerc")
	}

	if len(registry.List()) == 0 {
		log.Printf("provisioner: WARNING: no providers configured (set PROVIDER_A_KEY, PROVIDER_B_ID/SECRET, or PROVIDER_C_KEY)")
	}

	return registry
}
