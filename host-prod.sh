# Generate private key, certificate signing request, and self-signed certificate
set -e &&
sudo mkdir -p ~/ssl &&
sudo openssl genrsa -out ~/ssl/nginx.key 2048 &&
sudo openssl req -new -key ~/ssl/nginx.key -out /tmp/nginx.csr -subj "/C=US/ST=State/L=City/O=Shinzo/OU=Host Client/CN=shinzo.network" &&
sudo openssl x509 -req -days 365 -in /tmp/nginx.csr -signkey ~/ssl/nginx.key -out ~/ssl/nginx.crt &&
sudo rm /tmp/nginx.csr

# Install dependencies and start the host
sudo apt-get update &&
sudo apt-get install -y docker.io docker-compose nginx &&
sudo mkdir -p ~/data/defradb &&
sudo chown -R 1003:1006 ~/data/defradb &&
docker-compose up -d
