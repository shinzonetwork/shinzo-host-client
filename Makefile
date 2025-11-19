.PHONY: build build-playground start start-playground deps-playground

build:
	go build -o bin/host cmd/main.go

build-playground: deps-playground
	go build -tags hostplayground -o bin/host cmd/main.go

start:
	go run cmd/main.go

start-playground: deps-playground
	go run -tags hostplayground cmd/main.go

# Download playground static assets
deps-playground:
	cd playground && go generate .

# Build with playground enabled (downloads assets and builds)
build-with-playground: deps-playground build-playground