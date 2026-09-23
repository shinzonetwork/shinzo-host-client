#!/usr/bin/env bash
set -euo pipefail

# Install node-console static assets into console/dist.
#
# Order:
#   1. Sibling checkout ../node-console (local develop)
#   2. CONSOLE_DIST_URL pointing at a dist.tar.gz GitHub release
#
# Bump CONSOLE_DIST_URL when you publish a new UI version.

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CONSOLE_DIR="$ROOT/console"
UI_DIR="$(cd "$ROOT/.." && pwd)/node-console"
CONSOLE_DIST_URL="${CONSOLE_DIST_URL:-}"

cd "$CONSOLE_DIR"

install_from_dir() {
  local src="$1"
  rm -rf dist
  mkdir -p dist
  cp -R "$src/." dist/
}

if [[ -d "$UI_DIR" ]]; then
  echo "building node-console from $UI_DIR"
  (cd "$UI_DIR" && npm run build)
  install_from_dir "$UI_DIR/dist"
  echo "copied $UI_DIR/dist -> $CONSOLE_DIR/dist"
  exit 0
fi

if [[ -n "$CONSOLE_DIST_URL" ]]; then
  echo "downloading $CONSOLE_DIST_URL"
  curl -fsSL "$CONSOLE_DIST_URL" | tar xzf -
  echo "extracted console assets into $CONSOLE_DIR"
  exit 0
fi

cat >&2 <<EOF
node-console assets not found.

Clone or keep a sibling checkout:
  ../node-console

Or set CONSOLE_DIST_URL to a GitHub release tarball:
  CONSOLE_DIST_URL=https://github.com/<org>/node-console/releases/download/v0.1.0/dist.tar.gz
  go generate ./console
EOF
exit 1
