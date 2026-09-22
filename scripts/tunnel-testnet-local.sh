#!/bin/sh
set -eu

for tool in cloudflared curl; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "Install $tool before running this script." >&2
    exit 1
  }
done

if ! curl --fail --silent --show-error --max-time 5 \
  http://127.0.0.1:8080/health >/dev/null; then
  echo "Start scripts/run-testnet-local.sh and wait for the health endpoint." >&2
  exit 1
fi

echo "Use the printed HTTPS hostname with /registration, /health, /api/node, or /api/v0/graphql."
exec cloudflared tunnel --url http://127.0.0.1:8080 --no-autoupdate
