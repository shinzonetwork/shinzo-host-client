# Install dependencies and start the host
sudo apt-get update &&
sudo apt-get install -y docker.io docker-compose nginx &&
sudo mkdir -p ~/data/defradb ~/data/lens ~/data/keys &&
sudo chown -R 1001:1001 ~/data/ ~/data/defradb ~/data/lens ~/data/keys &&
sudo docker-compose up -d
