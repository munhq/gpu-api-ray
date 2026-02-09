package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gpu_api_http_requests_total",
		Help: "Total HTTP requests by method, path, and status code.",
	}, []string{"method", "path", "status"})

	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gpu_api_http_request_duration_seconds",
		Help:    "HTTP request latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	jobsSubmitted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gpu_api_jobs_submitted_total",
		Help: "Total number of jobs submitted to Ray.",
	})

	jobsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gpu_api_jobs_active",
		Help: "Number of currently active (non-terminal) jobs.",
	})

	jobsByStatus = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gpu_api_jobs_by_status_total",
		Help: "Jobs observed in terminal states.",
	}, []string{"status"})

	jobDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gpu_api_job_duration_seconds",
		Help:    "Time from job submission to completion as observed by status checks.",
		Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
	})

	queueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gpu_api_queue_depth",
		Help: "Number of jobs waiting in the priority queue.",
	})

	gpusActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gpu_api_gpus_active",
		Help: "Number of GPUs currently in use by running jobs.",
	})

	gpusTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gpu_api_gpus_total",
		Help: "Total GPU slots configured (max concurrent jobs).",
	})

	queueWaitSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gpu_api_queue_wait_seconds",
		Help:    "Time from enqueue to submission to Ray.",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600},
	})
)

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rec, r)
		duration := time.Since(start).Seconds()

		path := r.URL.Path
		httpRequestsTotal.WithLabelValues(r.Method, path, strconv.Itoa(rec.statusCode)).Inc()
		httpRequestDuration.WithLabelValues(r.Method, path).Observe(duration)
	})
}
