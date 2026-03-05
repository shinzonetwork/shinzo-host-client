sudo tee ~/config.yaml <<'EOF'
defradb:
  url: "localhost:9181"
  keyring_secret: "pingpong"
  p2p:
    enabled: true
    bootstrap_peers:
      - '/ip4/35.209.3.27/tcp/9171/p2p/12D3KooWCjuGc2jjFGogai7kwJpYC6BwxPvsZVDsgB5BUNGXofUr'
    listen_addr: "/ip4/0.0.0.0/tcp/9171"
    max_retries: 5                  # Number of connection attempts before marking peer as failed
    retry_base_delay_ms: 1000       # Base delay for exponential backoff (1s, 2s, 4s, 8s, 16s)
    reconnect_interval_ms: 60000    # Check for disconnected peers every 60 seconds
    enable_auto_reconnect: true     # Automatically reconnect to failed/disconnected peers
  store:
    path: "./.defra"
    # Badger memory configuration
    block_cache_mb: 512  # Block cache size
    memtable_mb: 64      # Memtable size
    index_cache_mb: 256   # Index cache size
    # Badger compaction configuration
    num_compactors: 4              # Number of compaction workers
    # num_level_zero_tables: 20      # L0 tables before compaction starts
    # num_level_zero_tables_stall: 40  # L0 tables that trigger write stalls
    # Badger value log configuration
    value_log_file_size_mb: 128     # Size of each vlog file (smaller = faster GC)
shinzo:
  minimum_attestations: 1
  start_height: 0 # Indexer auto-detects from chain tip
  hub_base_url: rpc.devnet.shinzo.network:26657
  # P2P Control Settings
  wait_for_gaps: true         # Wait for gap processing before starting P2P
  max_gap_size: 1000          # Skip P2P if gap is larger than this
  # Queue Settings
  cache_queue_size: 50000        # Job queue size for document processing
  # View Management Settings
  view_inactivity_timeout: "24h"  # Stop updating views after 24h of no access
  view_cleanup_interval: "1h"     # Check for inactive views every hour
  view_worker_count: 20            # Workers for batch lens transformations
  view_queue_size: 5000           # Queue size for batch processing jobs
  batch_processing_enabled: true     # Enable batch processing for efficiency
  batch_max_views_per_job: 50         # Split large batches to prevent memory issues
  batch_query_cache_size: 1000        # Cache query templates for reuse
  # Attestation Processing Settings
  batch_writer_count: 8             # Number of batch writers
  batch_size: 500                  # Max attestations per batch
  batch_flush_interval: 100          # Flush interval in milliseconds
  use_block_signatures: true
  doc_worker_count: 32
  doc_queue_size: 50000
  # Event Replication Filter
  # Controls which P2P documents the host stores. Blocks and BlockSignatures
  # always pass through regardless of filter settings.
  event_filter:
    enabled: false                 # Set to false to accept all documents (no filtering)
pruner:
  enabled: true             # Set to true to enable automatic pruning
  max_blocks: 2000         # Number of blocks to retain
  docs_per_block: 1000      # Average docs per block (~1000 on Ethereum mainnet). Pruning triggers at max_blocks * docs_per_block docs
  interval_seconds: 30      # How often to check and prune
  prune_history: false      # Walk DAG chains to delete historical block versions (2-3x slower)
logger:
  development: false
  level: "error"
host:
  lens_registry_path: "./.defra/lens"
  health_server_port: 8080
  open_browser_on_start: false
  # Snapshot Bootstrap - download historical data from an indexer on first startup.
  # Set enabled: false to skip snapshot import entirely.
  snapshot:
    enabled: false
    indexer_url: "http://35.206.105.60:8080"
    historical_ranges:
      - start: 24528700
        end: 24528999
EOF

&&

sudo tee ~/docker-compose.yml <<'EOF'
networks:
  shinzo-net:
    driver: bridge

services:
  shinzo-host:
    image: ghcr.io/shinzonetwork/shinzo-host-client:v0.5.5
    mem_limit: 16g
    mem_reservation: 13g
    restart: unless-stopped
    container_name: shinzo-host
    networks:
      - shinzo-net
    ports:
      - "9181:9181"  # DefraDB API (internal network access)
      - "444:9182"  # GraphQL Playground 
      - "9171:9171"  # P2P networking (still needs external access)
    volumes:
      - ~/data/defradb:/app/.defra/data
      - ~/data/lens:/app/.lens
      - ~/config.yaml:/app/config.yaml:ro
    environment:
      - DEFRA_URL=0.0.0.0:9181
      - GOMEMLIMIT=14GiB
      - LOG_LEVEL=error
      - LOG_SOURCE=false
      - LOG_STACKTRACE=false
    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:8080/metrics"]
      interval: 15s
      timeout: 30s
      retries: 10
      start_period: 120s

  nginx:
    image: nginx:alpine
    ports:
      - "8080:8080"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - shinzo-host
    networks:
      - shinzo-net
    restart: unless-stopped
EOF

&&

sudo tee ~/nginx.conf <<'EOF'
events { worker_connections 1024; }

http {
  # Only allow this origin for CORS
  map $http_origin $cors_origin {
    default "";
    "https://explorer.shinzo.network" $http_origin;
  }

  server {
    listen 8080;
    server_name _;

    # CORS headers for ALL responses from this server
    add_header 'Access-Control-Allow-Origin' $cors_origin always;
    add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
    add_header 'Access-Control-Allow-Headers' 'Authorization, Content-Type, Accept, Origin' always;
    add_header 'Access-Control-Max-Age' 3600 always;
    add_header 'Vary' 'Origin' always;

    # Generic preflight handler (OPTIONS to any path)
    location / {
      if ($request_method = OPTIONS) {
        return 204;
      }

      proxy_pass http://shinzo-host:9181;
      proxy_set_header Host $host;
    }

    # Metrics endpoint
    location = /metrics {
      if ($request_method = OPTIONS) {
        return 204;
      }

      proxy_pass http://shinzo-host:8080/metrics;
      proxy_set_header Host $host;
    }

    # GraphQL endpoint
    location = /api/v0/graphql {
      if ($request_method = OPTIONS) {
        return 204;
      }

      proxy_pass http://shinzo-host:9181/api/v0/graphql;
      proxy_set_header Host $host;
    }
  }
}
EOF