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
| `make build` | Build the binary into `./bin/host`. The embedded DefraDB instance produces no log output — the host's own logs are unaffected. |
| `make build-loud` | Same, but without `-tags silent` — DefraDB's own internal logs are included too. Useful when debugging DefraDB itself. |
| `make start` | Build and run `./bin/host start`. |
| `make start-loud` | Same, with DefraDB's logs included. |
| `go run ./cmd/host start` | Run without building first. Bypasses Make, so this defaults to the loud build (no `-tags silent`) unless you pass it yourself: `go run -tags silent ./cmd/host start`. |
| `go test ./...` | Run the test suite. |

`-tags silent` is a compile-time switch, not a runtime config value — a binary built with it never emits DefraDB logs, one built without it always does. `make build`/`make start` apply it by default; use the `-loud` variants (or a bare `go build`) to get DefraDB's logs back.

## Configuration

The node reads a TOML config file, resolved in this order: `--config <path>`, or `XDG_DATA_HOME/shinzo-host/default/config.toml` (`~/.local/share/shinzo-host/default/config.toml` if `XDG_DATA_HOME` isn't set). Running `start` with no config file present writes a default one to that path and continues.

The generated default is enough to boot a node, but `p2p.bootstrap_peers` starts empty, so it won't sync anything until you add real peers. Two reference configs are checked in:

- [`toml/default.toml`](./toml/default.toml) — every field, heavily commented, no real network values filled in.
- [`toml/testnet.toml`](./toml/testnet.toml) — a working config with real bootstrap peers and a real snapshot indexer.

See `config/config.go` for the full set of fields.

## Local test host with a Cloudflare tunnel

From the repository root, run:

```shell
./scripts/run-testnet-local.sh
```

The script builds this checkout and uses the testnet template. It keeps all local
test data and keys in `.local_tests/cleanup-testnet-host/node`, uses HTTP port
`8080` on loopback and P2P port `9171`, and sets a default Go soft memory limit of
16 GB. Stop any host already using these ports first. The previous
`.local_tests/testnet-host` instance and the default XDG instance are untouched.
An ordinary restart keeps the identity. Configuration is generated from
`toml/testnet.toml` on each run; edits to the generated copy are replaced.

In another terminal, run:

```shell
./scripts/tunnel-testnet-local.sh
```

This requires `cloudflared` and `curl`. The run script also requires Go and `lsof`.
The tunnel prints a public HTTPS hostname. Use that hostname with `/registration`,
`/health`, `/api/node`, and `/api/v0/graphql`; all use the same HTTP port. The tunnel exposes
the HTTP server, including the development console and API. P2P still uses its
separate TCP port. Keep the tunnel terminal open while testing.

`/registration` returns the signed registration document and health data required
by onboarding. It uses the active node and peer keys and advertises GraphQL on
the public request origin, including forwarded tunnel headers. HTTP 503 retains
the signed document when health is not ready. A hostname with only private P2P
addresses still has no public `connection_string`; an HTTP tunnel does not expose
P2P port `9171`.

To start with a new identity, stop the host with Ctrl+C and wait for shutdown:

```shell
./scripts/reset-testnet-local.sh
./scripts/run-testnet-local.sh
```

Reset moves the entire test instance to a private
`.local_tests/cleanup-testnet-backup.XXXXXX/instance` directory. This clears the
active database and keyring together. The next start generates new keys and a
new DID. It does not delete an on-chain registration. Reset refuses to run while
the run script holds its lock. To restore a backup, stop the host, reset any
current instance, and move the saved `instance` directory back to
`.local_tests/cleanup-testnet-host`.

## Docker

```shell
cp toml/testnet.toml config.toml
docker compose up -d
```

`docker-compose.yml` builds the image locally and expects a `./config.toml` next to it (mounted read-only into the container, not tracked by git). Node data persists in a named volume, so it survives container restarts.

## Ports

Everything — health, metrics, GraphQL, the node console, the GraphQL playground, and node info — is served on one HTTP port. There's no separate playground or DefraDB port anymore, and no reverse proxy is needed to reach any of it.

| Port | Service |
| --- | --- |
| `8080` | HTTP: `/health`, `/registration`, `/metrics`, `/api/v0/graphql`, `/console`, `/playground`, `/api/node`, `/api/system` |
| `9171` | libp2p P2P networking |

## Debugging

Set `SHINZO_PPROF_ADDR` to expose pprof and expvar endpoints on their own listener (off by default):

```shell
SHINZO_PPROF_ADDR=:6060 ./bin/host start
```

`scripts/monitor_goroutines.sh [interval] [port]` polls that listener on a loop and logs goroutine count, heap usage, and top CPU consumers over time.
