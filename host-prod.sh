#!/bin/bash

# NOTE: Put config.yaml into the VM before running this script

set -e

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

sudo tee ~/nginx.conf <<EOF

events {
    worker_connections 1024;
}
http {
    # HTTP server - redirect to HTTPS
    server {
        listen 80;
        server_name _;
        return 301 https://$server_name$request_uri;
    }
    # HTTPS server with SSL termination
    server {
        listen 443 ssl http2;
        server_name _;
        
        # SSL certificate configuration
        ssl_certificate /etc/nginx/ssl/nginx.crt;
        ssl_certificate_key /etc/nginx/ssl/nginx.key;
        
        # SSL settings
        ssl_protocols TLSv1.2 TLSv1.3;
        ssl_ciphers HIGH:!aNULL:!MD5;
        ssl_prefer_server_ciphers off;
        
        # CORS configuration
        add_header 'Access-Control-Allow-Origin' '*' always;
        add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
        add_header 'Access-Control-Allow-Headers' 'DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range,Authorization' always;
        add_header 'Access-Control-Expose-Headers' 'Content-Length,Content-Range' always;
        add_header 'Access-Control-Allow-Credentials' 'true' always;
        
        location / {
            # Handle preflight OPTIONS requests
            if ($request_method = 'OPTIONS') {
                add_header 'Access-Control-Allow-Origin' '*' always;
                add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
                add_header 'Access-Control-Allow-Headers' 'DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range,Authorization' always;
                add_header 'Access-Control-Max-Age' 1728000 always;
                add_header 'Content-Type' 'text/plain; charset=utf-8' always;
                add_header 'Content-Length' 0 always;
                return 204;
            }
            
            # Proxy to shinzo-host container using service name
            proxy_pass http://shinzo-host:9181;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto https;
            
            # Additional proxy settings
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_buffering off;
            proxy_read_timeout 300s;
            proxy_connect_timeout 300s;
            proxy_send_timeout 300s;
            
            # Hide backend CORS headers to avoid conflicts
            proxy_hide_header 'Access-Control-Allow-Origin';
            proxy_hide_header 'Access-Control-Allow-Methods';
            proxy_hide_header 'Access-Control-Allow-Headers';
        }
    }
    # GraphQL Playground server
    server {
        listen 444;
        server_name localhost;
        
        location / {
            # Proxy to shinzo-host container using service name
            proxy_pass http://shinzo-host:9182;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto http;
            
            proxy_read_timeout 300s;
            proxy_connect_timeout 300s;
        }
    }
}
EOF

sudo tee ~/docker-compose.yml <<EOF
networks:
  shinzo-net:
    driver: bridge

services:
  shinzo-host:
    image: ghcr.io/shinzonetwork/shinzo-host-client:sha-5df2442
    mem_reservation: 30g
    restart: unless-stopped
    container_name: shinzo-host
    networks:
      - shinzo-net
    ports:
      - "9181:9181"  # DefraDB API (internal network access)
      - "444:9182"  # GraphQL Playground 
      - "8080:8080"  # Metrics
      - "9171:9171"  # P2P networking (still needs external access)
    volumes:
      - ~/data/defradb:/app/.defra/data
      - ~/data/lens:/app/.lens
      - /home/duncanbrown/config.yaml:/app/config.yaml:ro
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
      - "443:443"  # HTTPS
      - "80:80"    # HTTP (redirects to HTTPS)
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./ssl:/etc/nginx/ssl:ro
    depends_on:
      - shinzo-host
    networks:
      - shinzo-net
    restart: unless-stopped
EOF

# Docker + Nginx
apt-get update
sudo apt-get install -y docker.io docker-compose-plugin nginx

mkdir -p ~/data/defradb
chown -R 1003:1006 ~/data/defradb

docker compose up -d