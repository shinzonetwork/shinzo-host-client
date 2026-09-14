.PHONY: build build-quiet start start-quiet lint lint-fix

build:
	go build -o bin/host ./cmd/host

build-quiet:
	go build -tags silent -o bin/host ./cmd/host

start: build
	./bin/host start

start-quiet: build-quiet
	./bin/host start

lint:
	@echo "🔍 Running golangci-lint..."
	@golangci-lint run ./...

lint-fix:
	@echo "🔧 Running golangci-lint with auto-fix..."
	@golangci-lint run --fix ./...
