#!/bin/sh
set -eu

REPO_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$REPO_DIR/scripts/testnet-local-common.sh"

for tool in go lsof; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "Install $tool before running this script." >&2
    exit 1
  }
done

lock_test_host
HOST_PID=
cleanup() {
  # Keep the reset lock until the host has completed its shutdown.
  trap '' INT TERM
  if [ -n "$HOST_PID" ]; then
    kill -TERM "$HOST_PID" 2>/dev/null || true
    wait "$HOST_PID" 2>/dev/null || true
  fi
  unlock_test_host
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for port in 8080 9171; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "Port $port is in use. Stop the existing host before starting this one." >&2
    exit 1
  fi
done

# Build this checkout each time, so an old binary cannot use a different config format.
cd "$REPO_DIR"
go build -tags silent -o bin/host-testnet-cleanup ./cmd/host
mkdir -p "$RUN_DIR"

# Regenerate only configuration; keys and data survive an ordinary restart.
# All relative node paths resolve inside RUN_DIR, never in the default XDG instance.
sed \
  -e 's/^name = "default"$/name = "cleanup-testnet-host"/' \
  -e 's/^data_dir = ""$/data_dir = "node"/' \
  -e 's/^addr = ":8080"$/addr = "127.0.0.1:8080"/' \
  "$REPO_DIR/toml/testnet.toml" > "$RUN_DIR/config.toml"

# This is a Go soft memory limit, not a hard process memory limit.
export GOMEMLIMIT="${GOMEMLIMIT:-16000000000B}"
cd "$RUN_DIR"
echo "Test data: $RUN_DIR/node"
echo "Health: http://127.0.0.1:8080/health"
echo "Registration: http://127.0.0.1:8080/registration"
echo "Identity: http://127.0.0.1:8080/api/node"
echo "GraphQL: http://127.0.0.1:8080/api/v0/graphql"
echo "Start the tunnel in another terminal: $REPO_DIR/scripts/tunnel-testnet-local.sh"
"$REPO_DIR/bin/host-testnet-cleanup" start --config "$RUN_DIR/config.toml" &
HOST_PID=$!
wait "$HOST_PID"
HOST_PID=
