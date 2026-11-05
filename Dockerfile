# syntax=docker/dockerfile:1.7

# Multi-stage build for the Shinzo Network Host.
#
# CGO is required: Lens (lens/host-go) links wasmtime-go, whose
# libwasmtime is statically linked from the Go module cache. No external
# Wasmer/Wasmtime binaries, headers, or shared libraries are needed.

ARG GO_VERSION=1.27.3

# ============================================================
# Playground assets (cached until the download script changes)
# ============================================================

FROM golang:${GO_VERSION}-bookworm AS playground

WORKDIR /src
COPY tools/scripts/download_playground.sh tools/scripts/
RUN mkdir playground && bash tools/scripts/download_playground.sh

# ============================================================
# Build stage
# ============================================================

FROM golang:${GO_VERSION}-bookworm AS builder

ARG VERSION=dev

WORKDIR /app

# Cache Go dependencies separately from source changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download && go mod verify

COPY . .
COPY --from=playground /src/playground/dist ./playground/dist

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build \
      -trimpath \
      -tags=hostplayground \
      -ldflags="-w -s -X main.version=${VERSION}" \
      -o /out/host \
      ./cmd/

# ============================================================
# Runtime stage
# ============================================================

FROM debian:bookworm-slim

# ca-certificates: TLS to RPC/peers; tzdata: log timestamps;
# wget: HEALTHCHECK.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      tzdata \
      wget \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1001 shinzo && \
    useradd --uid 1001 --gid 1001 --create-home --shell /bin/bash shinzo && \
    mkdir -p /app/.defra /app/.lens && \
    chown -R shinzo:shinzo /app

WORKDIR /app

COPY --link --chown=1001:1001 --from=builder /out/host /app/host
COPY --link --chown=1001:1001 --from=builder /app/config/config.yaml /app/config.yaml
COPY --link --chown=1001:1001 --from=playground /src/playground/dist /app/playground/dist

USER shinzo

# Embedded DefraDB logs.
ENV LOG_LEVEL=error \
    LOG_SOURCE=false \
    LOG_STACKTRACE=false

# 9181: DefraDB API
# 9182: GraphQL Playground
# 9171: P2P
# 8080: health / metrics (internal, used by HEALTHCHECK)
EXPOSE 9181 9182 9171

HEALTHCHECK --interval=15s --timeout=30s --start-period=120s --retries=10 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/metrics || exit 1

CMD ["./host"]
