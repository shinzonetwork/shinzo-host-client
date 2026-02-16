#!/bin/bash

# NOTE: Put config.yaml into the VM before running this script

set -e

# 1) create ssl
# SSL Certificate
# Create SSL directory
sudo mkdir -p ~/ssl

# Generate private key
sudo openssl genrsa -out ~/ssl/nginx.key 2048

# Generate certificate signing request
sudo openssl req -new -key ~/ssl/nginx.key -out /tmp/nginx.csr -subj "/C=US/ST=State/L=City/O=Shinzo/OU=Host Client/CN=$DOMAIN"

# Generate self-signed certificate (valid for 365 days)
sudo openssl x509 -req -days 365 -in /tmp/nginx.csr -signkey ~/ssl/nginx.key -out ~/ssl/nginx.crt

# Clean up CSR file
sudo rm /tmp/nginx.csr

# 2) create nginx.conf
sudo tee ~/nginx.conf <<EOF
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

# 3) create docker-compose.yml
sudo tee ~/docker-compose.yml <<EOF
networks:
  shinzo-net:
    driver: bridge

services:
  shinzo-host:
    image: ghcr.io/shinzonetwork/shinzo-host-client:sha-002270f
    mem_reservation: 30g
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

# Docker + Nginx
sudo apt-get update
sudo apt-get install -y docker.io docker-compose nginx

sudo mkdir -p ~/data/defradb
sudo chown -R 1003:1006 ~/data/defradb


docker compose up -d


# Step 1 - Create ssl
# Step 2 - Copy nginx + docker compose files
# Step 3 - Install docker, docker compose, + nginx
# Steo 4 - Create app repo's and give correct permissions
# Step 5 - run `docker-compose up -d`