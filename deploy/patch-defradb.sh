#!/usr/bin/env bash
set -euo pipefail

defradb_dir="$(go list -m -f '{{.Dir}}' github.com/sourcenetwork/defradb)"
chmod u+w "${defradb_dir}/internal/db/p2p"
chmod u+w \
  "${defradb_dir}/internal/db/p2p/p2p.go" \
  "${defradb_dir}/internal/db/p2p/replication_filter.go"
patch --batch --forward -p1 -d "${defradb_dir}" < deploy/defradb-car-filter.patch
