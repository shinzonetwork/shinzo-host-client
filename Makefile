.PHONY: build start

build:
	go build -o bin/host cmd/main.go

start:
	go run cmd/main.go