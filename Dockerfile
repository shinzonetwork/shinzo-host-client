# Multi-stage build like indexer
FROM ubuntu:24.04 AS builder

# Install build dependencies including WASM runtimes
RUN apt-get update && apt-get install -y \
    git \
    ca-certificates \
    tzdata \
    make \
    build-essential \
    pkg-config \
    wget \
    tar \
    xz-utils \
    bash \
    coreutils \
    libgcc-s1 \
    libstdc++6 \
    curl \
    libssl-dev \
    gzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Go (detect architecture like indexer)
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        curl -L https://go.dev/dl/go1.25.4.linux-amd64.tar.gz | tar -xz -C /usr/local; \
    elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
        curl -L https://go.dev/dl/go1.25.4.linux-arm64.tar.gz | tar -xz -C /usr/local; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi
ENV PATH="/usr/local/go/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download && go mod verify

# Install WASM runtimes
RUN set -ex && \
    mkdir -p /usr/local/include /usr/local/lib /usr/local/bin && \
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
        WASMTIME_ARCH="aarch64"; \
    else \
        WASMTIME_ARCH="x86_64"; \
    fi && \
    # Install Wasmtime
    wget -O wasmtime.tar.xz "https://github.com/bytecodealliance/wasmtime/releases/download/v15.0.1/wasmtime-v15.0.1-${WASMTIME_ARCH}-linux.tar.xz" && \
    tar -xf wasmtime.tar.xz && \
    mv "wasmtime-v15.0.1-${WASMTIME_ARCH}-linux/wasmtime" /usr/local/bin/ && \
    chmod +x /usr/local/bin/wasmtime && \
    rm -rf wasmtime* && \
    # Install Wasmer
    if [ "$WASMTIME_ARCH" = "x86_64" ]; then \
        WASMER_URL="https://github.com/wasmerio/wasmer/releases/download/v4.2.5/wasmer-linux-amd64.tar.gz"; \
    else \
        WASMER_URL="https://github.com/wasmerio/wasmer/releases/download/v4.2.5/wasmer-linux-aarch64.tar.gz"; \
    fi && \
    wget -O wasmer.tar.gz "$WASMER_URL" && \
    tar -xf wasmer.tar.gz && \
    mv bin/wasmer /usr/local/bin/ && \
    mv lib/* /usr/local/lib/ && \
    mv include/* /usr/local/include/ && \
    chmod +x /usr/local/bin/wasmer && \
    rm -rf wasmer.tar.gz bin lib include

# Set CGO flags for WASM support
ENV CGO_ENABLED=1
ENV CGO_CFLAGS="-I/usr/local/include"
ENV CGO_LDFLAGS="-L/usr/local/lib"

# Copy source code
COPY . .

# Build the application with WASM support and playground enabled
RUN make build-with-playground

# Runtime stage
FROM ubuntu:24.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    tzdata \
    wget \
    libc6 \
    libgcc-s1 \
    libstdc++6 \
    && apt-get upgrade -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy WASM runtimes from builder stage (binaries and libraries)
COPY --from=builder /usr/local/bin/wasmtime /usr/local/bin/wasmtime
COPY --from=builder /usr/local/bin/wasmer /usr/local/bin/wasmer
COPY --from=builder /usr/local/lib/ /usr/local/lib/

# Set library path for WASM runtimes
ENV LD_LIBRARY_PATH="/usr/local/lib"

# Create non-root user
RUN groupadd -g 1001 shinzo && \
    useradd -u 1001 -g shinzo -m -s /bin/bash shinzo

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/bin/host /app/host

# Copy config and playground assets
COPY --from=builder /app/config.yaml /app/config.yaml
COPY --from=builder /app/playground/dist /app/playground/dist

# Create directories for data persistence
RUN mkdir -p .defra/data .lens && \
    chown -R shinzo:shinzo /app

# Switch to non-root user
USER shinzo

# Expose ports
# 9181: DefraDB API
# 9182: GraphQL Playground (if enabled)
EXPOSE 9181 9182 9171

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:9181/api/v0/graphql || exit 1

# Default command
CMD ["./host"]
