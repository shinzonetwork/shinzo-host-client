.PHONY: build build-playground build-branchable build-branchable-with-playground start deps-playground

build:
	go build -o bin/host cmd/main.go

build-branchable:
	go build -tags branchable -o bin/host cmd/main.go

build-playground: deps-playground
	go build -tags hostplayground -o bin/host cmd/main.go

start:
	./bin/host

# Download playground static assets
deps-playground:
	cd playground && go generate .

# Build with both branchable tag and playground enabled
build-branchable-with-playground: deps-playground
	go build -tags "branchable,hostplayground" -o bin/host cmd/main.go