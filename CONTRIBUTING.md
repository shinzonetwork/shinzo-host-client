# Contributing

## Before you start

Open an issue to discuss your proposed change before submitting a PR. This avoids wasted effort if the change isn't a good fit. PRs without an attached issue will be closed.

## Making changes

The repo is structured as follows:

| Path | Purpose |
| --- | --- |
| `cmd/` | Application entrypoint (`main.go`). |
| `config/` | Config structs, YAML loader, and the reference `config.yaml`. |
| `pkg/host/` | Core `Host` struct, startup logic, and the processing pipeline. |
| `pkg/view/` | View lifecycle management, WASM lens registry, and schema service. |
| `pkg/attestation/` | Attestation record service and signature verification. |
| `pkg/shinzohub/` | WebSocket event subscription and RPC client for the ShinzoHub chain node. |
| `pkg/schema/` | Embedded GraphQL schema files (standard and branchable variants). |
| `pkg/constants/` | Collection name constants and version info. |
| `pkg/graphql/` | Query builder utilities. |
| `pkg/server/` | Health and metrics HTTP server. |
| `pkg/snapshot/` | Historical snapshot download and import. |
| `pkg/playground/` | GraphQL Playground HTTP server (real and no-op implementations). |
| `playground/` | Static assets for the Playground UI (generated; do not edit by hand). |
| `adr/` | Architecture Decision Records. Read these before making structural changes. |
| `scripts/` | GCP VM startup and monitoring scripts. |
| `integration/` | Integration tests. |

Before touching core data-flow logic (host startup, view processing, attestation handling), read the relevant ADRs in `adr/`. They explain the reasoning behind key design decisions.

## Submitting a PR

- Keep PRs focused. One change per PR.
- Describe what you changed and why in the PR description.
- Make sure all tests pass before requesting review: `go test -v ./pkg/...`
- Leave lots of comments. It's best to be verbose in your explanations rather than assuming reviewers know what you're doing.
