#!/bin/bash
set -euxo pipefail

#############################################
# Config (override by exporting env vars)
#############################################
SWAP_GB="${SWAP_GB:-8}"                      # swap size in GiB
SWAPFILE="${SWAPFILE:-/swapfile}"
SWAPPINESS="${SWAPPINESS:-10}"

# Docker memory guardrails (prevents VM-wide OOM)
# For a ~32GiB VM, 24g is a reasonable starting point.
SHINZO_HOST_MEM="${SHINZO_HOST_MEM:-24g}"
SHINZO_HOST_MEMSWAP="${SHINZO_HOST_MEMSWAP:-24g}"

# Image/tag
SHINZO_IMAGE="${SHINZO_IMAGE:-ghcr.io/shinzonetwork/shinzo-host-client:v0.4.9}"

#############################################
# Base packages
#############################################
apt-get update
apt-get install -y docker.io mdadm rsync wget

#############################################
# Swap (idempotent)
#############################################
if ! swapon --show | awk '{print $1}' | grep -qx "$SWAPFILE"; then
  if [ ! -f "$SWAPFILE" ]; then
    # Create swapfile (fallocate fast-path; fallback to dd)
    if ! fallocate -l "${SWAP_GB}G" "$SWAPFILE"; then
      dd if=/dev/zero of="$SWAPFILE" bs=1M count=$((SWAP_GB * 1024))
    fi
    chmod 600 "$SWAPFILE"
    mkswap "$SWAPFILE"
  fi

  swapon "$SWAPFILE"

  # Persist across reboot
  if ! grep -qE "^[^#]*\s+$SWAPFILE\s+none\s+swap\s" /etc/fstab; then
    echo "$SWAPFILE none swap sw 0 0" >> /etc/fstab
  fi
fi

# Tune swappiness (prefer RAM, but allow swap as safety valve)
cat >/etc/sysctl.d/99-swap-tuning.conf <<EOF
vm.swappiness=${SWAPPINESS}
EOF
sysctl -p /etc/sysctl.d/99-swap-tuning.conf || true

#############################################
# Local SSD detection/RAID
#############################################
# GCP Local SSDs have model "nvme_card" and can appear as multiple namespaces on one controller
# (nvme0n1, nvme0n2) or as separate controllers (nvme0n1, nvme1n1)
DEVICES=()
for dev in /dev/nvme*n*; do
  [[ "$dev" =~ p[0-9]+$ ]] && continue
  CTRL=$(echo "$dev" | sed 's|/dev/\(nvme[0-9]*\)n.*|\1|')
  MODEL=$(cat /sys/class/nvme/$CTRL/model 2>/dev/null | xargs || true)
  echo "Checking $dev - controller: $CTRL, model: '$MODEL'"
  if [[ "$MODEL" == "nvme_card" ]]; then
    DEVICES+=("$dev")
  fi
done

echo "Found ${#DEVICES[@]} Local SSD(s): ${DEVICES[*]}"
if [ "${#DEVICES[@]}" -eq 0 ]; then
  echo "ERROR: No Local SSDs detected! Check that Local SSDs are attached to this VM."
  exit 1
fi

if [ "${#DEVICES[@]}" -ge 2 ]; then
  echo "Creating RAID-0 over ${#DEVICES[@]} Local SSDs"
  RAID_DEV=/dev/md0
  if [ ! -e "$RAID_DEV" ]; then
    mdadm --create "$RAID_DEV" \
      --level=0 \
      --raid-devices="${#DEVICES[@]}" \
      "${DEVICES[@]}"
  fi
  TARGET_DEV="$RAID_DEV"
else
  echo "Only one Local SSD found, using it directly"
  TARGET_DEV="${DEVICES[0]}"
fi

if ! blkid "$TARGET_DEV"; then
  mkfs.ext4 -F "$TARGET_DEV"
fi

MNT=/mnt/localssd
BACKUP_MNT=/mnt/persistent
mkdir -p "$MNT" "$BACKUP_MNT"

mountpoint -q "$MNT" || mount -o noatime,discard "$TARGET_DEV" "$MNT"
chmod 777 "$MNT"

mkdir -p \
  "$MNT/defradb" \
  "$MNT/logs" \
  "$BACKUP_MNT/backup/defradb"

chown -R 1001:1001 \
  "$MNT/defradb" \
  "$BACKUP_MNT/backup"

# Restore if localssd empty and backup exists
if [ -z "$(ls -A "$MNT/defradb" 2>/dev/null || true)" ] && [ -n "$(ls -A "$BACKUP_MNT/backup/defradb" 2>/dev/null || true)" ]; then
  echo "Restoring data from persistent backup to NVMe..."
  rsync -a --delete "$BACKUP_MNT/backup/defradb/" "$MNT/defradb/"
  chown -R 1003:1006 "$MNT/defradb"
  echo "Restore complete."
fi

#############################################
# Backup loop
#############################################
cat > /usr/local/bin/backup-defra.sh << 'BACKUP_SCRIPT'
#!/bin/bash
while true; do
  sleep 300 # 5 minutes
  rsync -a --delete /mnt/localssd/defradb/ /mnt/persistent/backup/defradb/ 2>/dev/null || true
  echo "$(date): Backup sync completed" >> /mnt/localssd/logs/backup.log
done
BACKUP_SCRIPT
chmod +x /usr/local/bin/backup-defra.sh
nohup /usr/local/bin/backup-defra.sh &

#############################################
# Run shinzo-host with memory guardrails
#############################################
docker pull "$SHINZO_IMAGE"
docker rm -f shinzo-host || true

docker run -d \
  --name shinzo-host \
  --restart unless-stopped \
  --network host \
  -u 1003:1006 \
  -v "$MNT/defradb:/app/.defra" \
  -v "$(pwd)/config.yaml:/app/config.yaml:ro" \
  -e DEFRA_URL=0.0.0.0:9181 \
  -e LOG_LEVEL=error \
  -e LOG_SOURCE=false \
  -e LOG_STACKTRACE=false \
  --memory "$SHINZO_HOST_MEM" \
  --memory-swap "$SHINZO_HOST_MEMSWAP" \
  --health-cmd="wget --no-verbose --tries=1 --spider http://localhost:8080/metrics || exit 1" \
  --health-interval=30s \
  --health-timeout=10s \
  --health-retries=3 \
  --health-start-period=40s \
  "$SHINZO_IMAGE"
