#!/bin/bash
# Builds the staging host, inits it in .host/ (once), then starts it.
# Extra args go to `host start`, e.g. scripts/dev-host.sh --http.addr :9090

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOME_DIR="$ROOT/.host"
BIN="$ROOT/bin/staging/host"

cd "$ROOT"
make build-staging

if [ ! -f "$HOME_DIR/config.toml" ]; then
  "$BIN" init --home "$HOME_DIR"
fi

exec "$BIN" start --home "$HOME_DIR" "$@"
