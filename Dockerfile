FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /app/bin/host ./cmd/host

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1001 shinzo && \
    useradd -u 1001 -g shinzo -m -s /usr/sbin/nologin shinzo

WORKDIR /app

COPY --from=builder /app/bin/host /app/host

RUN mkdir -p /data && chown -R shinzo:shinzo /app /data

USER shinzo

EXPOSE 8080 9171

HEALTHCHECK --interval=15s --timeout=10s --start-period=60s --retries=5 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/host"]
CMD ["start", "--config", "/app/config.toml"]
