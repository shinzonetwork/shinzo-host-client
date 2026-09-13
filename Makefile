.PHONY: build start lint lint-fix

build:
	go build -o bin/host ./cmd/host

start: build
	./bin/host start

lint:
	@echo "🔍 Running golangci-lint..."
	@golangci-lint run ./...

lint-fix:
	@echo "🔧 Running golangci-lint with auto-fix..."
	@golangci-lint run --fix ./...
