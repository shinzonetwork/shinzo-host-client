.PHONY: build build-playground start deps-playground lint lint-fix
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

build:
	go build -o bin/host cmd/main.go

build-staging:
	go build -ldflags "-X main.version=$(VERSION)" -o bin/staging/host ./cmd/host/

build-playground: deps-playground
	go generate -tags hostplayground ./playground
	go build -tags hostplayground -o ./bin/host cmd/main.go

start:
	./bin/host

start-staging:
	./bin/staging/host

# Download playground static assets
deps-playground:
	cd playground && go generate .

lint:
	@echo "🔍 Running golangci-lint..."
	@golangci-lint run ./...

lint-fix:
	@echo "🔧 Running golangci-lint with auto-fix..."
	@golangci-lint run --fix ./...
