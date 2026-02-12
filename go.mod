module github.com/munhq/kubernetes_gpu/gpu-api

go 1.25.0

require (
	internal/provider v0.0.0
	github.com/google/uuid v1.6.0
	github.com/prometheus/client_golang v1.20.5
	github.com/redis/go-redis/v9 v9.17.3
)

replace internal/provider => internal/provider

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	golang.org/x/sys v0.38.0 // indirect
	google.golang.org/protobuf v1.36.8 // indirect
	k8s.io/apimachinery v0.35.1 // indirect
	k8s.io/client-go v0.35.1 // indirect
)
