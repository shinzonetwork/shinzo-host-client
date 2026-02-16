#!/bin/bash

set -e
# SSL Certificate
# Create SSL directory
sudo mkdir -p ~/ssl

# Generate private key
sudo openssl genrsa -out ~/ssl/nginx.key 2048

# Generate certificate signing request
sudo openssl req -new -key ~/ssl/nginx.key -out /tmp/nginx.csr -subj "/C=US/ST=State/L=City/O=Shinzo/OU=Host Client/CN=shinzo.network"

# Generate self-signed certificate (valid for 365 days)
sudo openssl x509 -req -days 365 -in /tmp/nginx.csr -signkey ~/ssl/nginx.key -out ~/ssl/nginx.crt
