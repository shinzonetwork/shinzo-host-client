#!/bin/bash
set -euo pipefail

DATA_DEVICE="/dev/disk/by-id/google-vanishmax-shinzo-host-data"
DATA_MOUNT="/mnt/stateful_partition/vanishmax-shinzo-host-data"

# The container runs as uid/gid 1001. Google Container-Optimized OS mounts
# persistent disks in a separate namespace, so prepare ownership from the host
# before the container is restarted by the container runtime.
for _ in $(seq 1 120); do
    if [[ -b "${DATA_DEVICE}" ]]; then
        break
    fi
    sleep 1
done

if [[ ! -b "${DATA_DEVICE}" ]]; then
    echo "host data disk was not available within 120 seconds" >&2
    exit 1
fi

mkdir -p "${DATA_MOUNT}"
mount "${DATA_DEVICE}" "${DATA_MOUNT}"
chown 1001:1001 "${DATA_MOUNT}"
chmod 0770 "${DATA_MOUNT}"
sync
umount "${DATA_MOUNT}"
