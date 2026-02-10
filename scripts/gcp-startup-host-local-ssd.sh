#!/bin/bash

# NOTE: Put config.yaml into the VM before running this script

set -euxo pipefail

apt-get update
apt-get install -y docker.io mdadm

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
mkdir -p $MNT
mountpoint -q $MNT || mount -o noatime,discard $TARGET_DEV $MNT
chmod 777 $MNT
mkdir -p \
  $MNT/defradb \
  $MNT/logs
chown -R 1003:1006 $MNT/defradb

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
  --log-opt max-size=50m \
  --log-opt max-file=3 \
  ghcr.io/shinzonetwork/shinzo-host-client:v0.4.9
