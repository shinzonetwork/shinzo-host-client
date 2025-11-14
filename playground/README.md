# Playground

The playground provides a web-based GraphQL interface for interacting with the Host's embedded DefraDB instance.

The playground source code can be found at:
https://github.com/sourcenetwork/defradb-playground

## Setup

Download the playground static assets:

```bash
make deps-playground
```

Or from the playground directory:

```bash
cd playground
go generate .
```

## Building with Playground

To enable the playground, build or run the host with the `hostplayground` build tag:

**Build:**
```bash
make build-playground
```

**Run:**
```bash
make start-playground
```

Or manually:
```bash
go build -tags hostplayground -o bin/host cmd/main.go
go run -tags hostplayground cmd/main.go
```

## Using the Playground

Once the host is running with the playground enabled, open your browser and navigate to the URL shown in the startup logs (typically `http://localhost:9181`).

The playground provides:
- An interactive GraphQL query editor
- Schema exploration and documentation
- Query execution and result viewing
- Support for subscriptions

The GraphQL endpoint is available at `/api/v0/graphql` and the playground is served at the root path `/`.
