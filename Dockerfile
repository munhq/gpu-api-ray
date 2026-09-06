FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go *.html ./

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o gpu-api .

FROM alpine:latest

# Binds the GHCR package to this repository, so the package page links back here
# and no other repository can claim the name.
LABEL org.opencontainers.image.source="https://github.com/munhq/gpu-api-ray"

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/gpu-api .

EXPOSE 8000

CMD ["./gpu-api"]
