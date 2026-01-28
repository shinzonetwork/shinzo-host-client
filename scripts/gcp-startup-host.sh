#!/bin/bash

# NOTE: Put config.yaml into the VM before running this script

set -e

apt-get update
apt-get install -y docker.io

mkdir -p /mnt/defradb-data/logs
mkdir -p ~/data/defradb
sudo chown -R 1001:1001 ~/data/defradb

docker pull ghcr.io/shinzonetwork/shinzo-host-client:v0.4.6
docker run -d \
  --name shinzo-host \
  --network host \
  -v ~/data/defradb:/app/.defra \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -e DEFRA_URL=0.0.0.0:9181 \
  -e DEFRADB_BLOCK_CACHE_MB=2048 \
  -e DEFRADB_MEMTABLE_MB=1024 \
  -e DEFRADB_INDEX_CACHE_MB=2048 \
  -e DEFRADB_NUM_COMPACTORS=4 \
  -e DEFRADB_NUM_LEVEL_ZERO_TABLES=40 \
  -e DEFRADB_NUM_LEVEL_ZERO_TABLES_STALL=80 \
  -e LOG_LEVEL=error \
  -e LOG_SOURCE=false \
  -e LOG_STACKTRACE=false \
  --health-cmd="wget --no-verbose --tries=1 --spider http://localhost:8080/metrics || exit 1" \
  --health-interval=30s \
  --health-timeout=10s \
  --health-retries=3 \
  --health-start-period=40s \
  --restart unless-stopped \
  ghcr.io/shinzonetwork/shinzo-host-client:v0.4.6
