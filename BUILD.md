# Build from source

## Prerequisites

- [Go 1.26+](https://go.dev/dl/)
- Make
- A C compiler (`gcc`/`libc6-dev`) — needed for cgo, picked up automatically by the Go toolchain. No Wasmtime or Wasmer install is required: the embedded lens runtime uses `wazero`, a pure-Go implementation.

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
| `make start` | Build and run `./bin/host start`. |
| `go run ./cmd/host start` | Run without building first. |
| `go test ./...` | Run the test suite. |

## Configuration

The node reads a TOML config file, resolved in this order: `--config <path>`, or `XDG_DATA_HOME/shinzo-host/default/config.toml` (`~/.local/share/shinzo-host/default/config.toml` if `XDG_DATA_HOME` isn't set). Running `start` with no config file present writes a default one to that path and continues.

The generated default is enough to boot a node, but `p2p.bootstrap_peers` starts empty, so it won't sync anything until you add real peers. A working reference config:

```toml
[p2p]
enabled = true
listen_addr = "/ip4/0.0.0.0/tcp/9171"
bootstrap_peers = [
  "/ip4/35.254.135.221/tcp/9171/p2p/12D3KooWDUdHSCXBM5Wb7te6ZdWMgqddw7tJ7npWSzXK5tQgBsbT",
  "/ip4/34.57.239.57/tcp/9171/p2p/12D3KooWBAgCEJHYqzuCFEXzjsw2CnV9JqvqMgTKYDww58aCxwW5",
  "/ip4/34.134.119.63/tcp/9171/p2p/12D3KooWQQTuSQaz4HfuvnJHakkQy3PhWbKBBbS3RkmBw4ZsFkyT",
]

[snapshot]
enabled = false
indexer_url = "http://35.254.135.221:8080"
historical_ranges = [
  { start = 24528700, end = 24528999 },
]
```

See `config/config.go` for the full set of fields.

## Docker

```shell
docker build -t shinzo-host-client .
docker compose up -d
```

`docker-compose.yml` builds the image locally and expects a `./config.toml` next to it (mounted read-only into the container). Node data persists in a named volume, so it survives container restarts.

## Ports

Everything — health, metrics, GraphQL, the node console, the GraphQL playground, and node info — is served on one HTTP port. There's no separate playground or DefraDB port anymore, and no reverse proxy is needed to reach any of it.

| Port | Service |
| --- | --- |
| `8080` | HTTP: `/health`, `/metrics`, `/graphql`, `/console`, `/playground`, `/api/node`, `/api/system` |
| `9171` | libp2p P2P networking |

## Debugging

Set `SHINZO_PPROF_ADDR` to expose pprof and expvar endpoints on their own listener (off by default):

```shell
SHINZO_PPROF_ADDR=:6060 ./bin/host start
```

`scripts/monitor_goroutines.sh [interval] [port]` polls that listener on a loop and logs goroutine count, heap usage, and top CPU consumers over time.
