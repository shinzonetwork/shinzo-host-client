.PHONY: build build-loud start start-loud lint lint-fix deps-console

build:
	go build -tags silent -o bin/host ./cmd/host

build-loud:
	go build -o bin/host ./cmd/host

start: build
	./bin/host start

start-loud: build-loud
	./bin/host start

deps-console:
	go generate ./console

lint:
	@echo "🔍 Running golangci-lint..."
	@golangci-lint run ./...

lint-fix:
	@echo "🔧 Running golangci-lint with auto-fix..."
	@golangci-lint run --fix ./...
