FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy shared module first (replace directive: internal/provider)
COPY pkg/provider/ /app/pkg/provider/

# Copy gpu-api module
COPY gpu-api/go.mod gpu-api/go.sum /app/gpu-api/
WORKDIR /app/gpu-api
RUN go mod download

COPY gpu-api/*.go gpu-api/*.html /app/gpu-api/

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o gpu-api .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/gpu-api/gpu-api .

EXPOSE 8000

CMD ["./gpu-api"]
