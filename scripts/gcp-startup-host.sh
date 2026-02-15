#!/bin/bash

set -e

apt-get update
apt-get install -y docker.io

mkdir -p ~/data/defradb
chown -R 1003:1006 ~/data/defradb

docker pull ghcr.io/shinzonetwork/shinzo-host-client:v0.5.0
docker run -d \
  --name shinzo-host \
  --restart unless-stopped \
  --network host \
  -u 1003:1006 \
  -v ~/data/defradb:/app/.defra \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -e DEFRA_URL=0.0.0.0:9181 \
  -e START_HEIGHT=${START_HEIGHT:-} \
  -e BOOTSTRAP_PEERS=${BOOTSTRAP_PEERS:-} \
  -e LOG_LEVEL=error \
  -e LOG_SOURCE=false \
  -e LOG_STACKTRACE=false \
  --health-cmd="wget --no-verbose --tries=1 --spider http://localhost:8080/metrics || exit 1" \
  --health-interval=30s \
  --health-timeout=10s \
  --health-retries=3 \
  --health-start-period=40s \
  --log-opt max-size=50m \
  --log-opt max-file=3 \
  ghcr.io/shinzonetwork/shinzo-host-client:v0.5.0
