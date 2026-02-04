#!/bin/bash

# NOTE: Put config.yaml into the VM before running this script

set -e

apt-get update
apt-get install -y docker.io

mkdir -p ~/data/defradb
chown -R 1003:1006 ~/data/defradb

docker pull ghcr.io/shinzonetwork/shinzo-host-client:v0.4.9
docker run -d \
  --name shinzo-host \
  --network host \
  -u 1003:1006 \
  -v ~/data/defradb:/app/.defra \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -e DEFRA_URL=0.0.0.0:9181 \
  -e LOG_LEVEL=error \
  -e LOG_SOURCE=false \
  -e LOG_STACKTRACE=false \
  --health-cmd="wget --no-verbose --tries=1 --spider http://localhost:8080/metrics || exit 1" \
  --health-interval=30s \
  --health-timeout=10s \
  --health-retries=3 \
  --health-start-period=40s \
  --restart unless-stopped \
  ghcr.io/shinzonetwork/shinzo-host-client:v0.4.9
