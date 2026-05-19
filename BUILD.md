# Build from source

## Prerequisites

- [Go 1.25+](https://go.dev/dl/)
- Make

Wasmtime and Wasmer are installed automatically inside the Docker build. If you are building locally (outside Docker), both WASM runtimes need to be on your `LD_LIBRARY_PATH`. The `Dockerfile` has the exact versions and install steps.

## Steps

```shell
git clone git@github.com:shinzonetwork/shinzo-host-client.git
cd shinzo-host-client
make build
```

The binary lands at `./bin/host`.

## Useful commands

| Command | What it does |
| --- | --- |
| `make build` | Build the binary into `./bin/host`. |
| `make build-playground` | Download playground assets and build with the embedded GraphQL Playground UI. |
| `make build-branchable` | Build with the `branchable` schema variant. |
| `make start` | Run the compiled `./bin/host` binary. |
| `make deps-playground` | Download playground static assets (required before `build-playground`). |
| `go run cmd/main.go` | Run without building. |
| `go test -v ./pkg/...` | Run the test suite. |

## Build tags

| Tag | Effect |
| --- | --- |
| `hostplayground` | Embeds the GraphQL Playground UI (served on port `9182`). |
| `branchable` | Uses the branchable GraphQL schema variant. |

## Docker

`docker-compose.yml` pulls the published image from GHCR. To build locally instead:

```shell
docker build -t shinzo-host-client .
```

To include the Playground UI in the image, add `--build-arg TAGS=hostplayground`.

## Ports

| Port | Service |
| --- | --- |
| `9181` | DefraDB GraphQL + REST API |
| `9182` | GraphQL Playground UI (if enabled) |
| `9171` | libp2p P2P networking |
| `8080` | Health and metrics server |
