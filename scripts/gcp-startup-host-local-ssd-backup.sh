#!/bin/bash
set -euxo pipefail

apt-get update
apt-get install -y docker.io mdadm rsync

# GCP Local SSDs have model "nvme_card" and can appear as multiple namespaces on one controller (nvme0n1, nvme0n2)
# or as separate controllers (nvme0n1, nvme1n1)
DEVICES=()
for dev in /dev/nvme*n*; do
  [[ "$dev" =~ p[0-9]+$ ]] && continue
  CTRL=$(echo "$dev" | sed 's|/dev/\(nvme[0-9]*\)n.*|\1|')
  MODEL=$(cat /sys/class/nvme/$CTRL/model 2>/dev/null | xargs)
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
    mdadm --create $RAID_DEV \
      --level=0 \
      --raid-devices=${#DEVICES[@]} \
      "${DEVICES[@]}"
  fi
  TARGET_DEV=$RAID_DEV
else
  echo "Only one Local SSD found, using it directly"
  TARGET_DEV=${DEVICES[0]}
fi

if ! blkid $TARGET_DEV; then
  mkfs.ext4 -F $TARGET_DEV
fi

MNT=/mnt/localssd
BACKUP_MNT=/mnt/persistent
mkdir -p $MNT
mountpoint -q $MNT || mount -o noatime,discard $TARGET_DEV $MNT
chmod 777 $MNT
mkdir -p \
  $MNT/defradb \
  $MNT/logs
mkdir -p \
  $BACKUP_MNT/backup/defradb
chown -R 1001:1001 \
  $MNT/defradb \
  $BACKUP_MNT/backup

if [ -z "$(ls -A $MNT/defradb 2>/dev/null)" ] && [ -n "$(ls -A $BACKUP_MNT/backup/defradb 2>/dev/null)" ]; then
  echo "Restoring data from persistent backup to NVMe..."
  rsync -a --delete $BACKUP_MNT/backup/defradb/ $MNT/defradb/
  chown -R 1003:1006 $MNT/defradb
  echo "Restore complete."
fi

cat > /usr/local/bin/backup-defra.sh << 'BACKUP_SCRIPT'
#!/bin/bash
while true; do
  sleep 300  # 5 minutes
  rsync -a --delete /mnt/localssd/defradb/ /mnt/persistent/backup/defradb/ 2>/dev/null || true
  echo "$(date): Backup sync completed" >> /mnt/localssd/logs/backup.log
done
BACKUP_SCRIPT
chmod +x /usr/local/bin/backup-defra.sh
nohup /usr/local/bin/backup-defra.sh &

docker pull ghcr.io/shinzonetwork/shinzo-host-client:v0.4.9
docker rm -f shinzo-host || true
docker run -d \
  --name shinzo-host \
  --restart unless-stopped \
  --network host \
  -u 1003:1006 \
  -v $MNT/defradb:/app/.defra \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -e DEFRA_URL=0.0.0.0:9181 \
  -e LOG_LEVEL=error \
  -e LOG_SOURCE=false \
  -e LOG_STACKTRACE=false \
  --health-cmd="wget --no-verbose --tries=1 --spider http://localhost:8080/metrics || exit 1" \
  --health-interval=30s \
  --health-timeout=10s \
  --health-retries=3 \
  --health-start-period=40s \
  ghcr.io/shinzonetwork/shinzo-host-client:v0.4.9
