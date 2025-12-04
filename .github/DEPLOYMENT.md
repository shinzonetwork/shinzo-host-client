# Deployment Configuration

This document describes the CI/CD setup for auto-deploying shinzo-host-client.

## Architecture

```
Push to main → GitHub Actions (test + build) → Push to GHCR → Watchtower pulls → Container restarts
```

**Watchtower** runs on the VM and automatically pulls new `:latest` images every 5 minutes.

## Required GitHub Secrets

| Secret | Required | Description |
|--------|----------|-------------|
| `DEFRA_KEYRING_SECRET` | Yes | DefraDB keyring secret (for tests) |

## Workflow Behavior

### Triggers
- **Push to `main`**: Runs tests, builds image, pushes to GHCR
- **Manual dispatch**: Trigger build via Actions UI

### Deploy Process
1. Run tests
2. Build Docker image with SHA tag
3. Push to GHCR with `:latest` and `:sha-<commit>` tags
4. Watchtower detects new image within ~5 minutes
5. Watchtower stops old container, pulls new image, starts new container

### Image Tags
- `ghcr.io/shinzonetwork/shinzo-host-client:latest` - Most recent main build (Watchtower watches this)
- `ghcr.io/shinzonetwork/shinzo-host-client:sha-<7chars>` - Specific commit for rollback

## VM Setup

### Prerequisites
- Docker installed
- GHCR authentication configured (`docker login ghcr.io`)
- `config.yaml` file with runtime configuration

### Watchtower Setup

```bash
# Login to GHCR (one-time, credentials saved to ~/.docker/config.json)
echo "YOUR_GITHUB_PAT" | docker login ghcr.io -u YOUR_USERNAME --password-stdin

# Start Watchtower
docker run -d \
  --name watchtower \
  --restart unless-stopped \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v ~/.docker/config.json:/config.json:ro \
  -e WATCHTOWER_POLL_INTERVAL=300 \
  -e WATCHTOWER_CLEANUP=true \
  -e WATCHTOWER_LABEL_ENABLE=true \
  containrrr/watchtower
```

### Host Container

```bash
docker run -d \
  --label com.centurylinklabs.watchtower.enable=true \
  --name shinzo-host \
  --restart unless-stopped \
  -p 9181:9181 \
  -p 9182:9182 \
  -p 9171:9171 \
  -v $(pwd)/data/defradb:/app/.defra/data \
  -v $(pwd)/data/lens:/app/.lens \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -e DEFRA_URL=0.0.0.0:9181 \
  -e LOG_LEVEL=error \
  -e LOG_SOURCE=false \
  -e LOG_STACKTRACE=false \
  ghcr.io/shinzonetwork/shinzo-host-client:latest
```

## Troubleshooting

### Check Watchtower logs
```bash
docker logs watchtower --tail 50
```

### Check if Watchtower can pull images
```bash
docker pull ghcr.io/shinzonetwork/shinzo-host-client:latest
```

### Force immediate update
```bash
docker exec watchtower /watchtower --run-once
```

### Manual rollback
```bash
# Stop current container
docker stop shinzo-host && docker rm shinzo-host

# Start with specific SHA tag
docker run -d \
  --label com.centurylinklabs.watchtower.enable=true \
  --name shinzo-host \
  ... \
  ghcr.io/shinzonetwork/shinzo-host-client:sha-abc1234
```

### Container not updating
- Verify the container has the Watchtower label: `docker inspect shinzo-host | grep watchtower`
- Check GHCR credentials: `cat ~/.docker/config.json`
- Verify Watchtower is running: `docker ps | grep watchtower`
