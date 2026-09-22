#!/bin/sh
set -eu

REPO_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$REPO_DIR/scripts/testnet-local-common.sh"

lock_test_host
trap unlock_test_host EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [ ! -d "$RUN_DIR" ]; then
  echo "No local test data exists. The next run will create a new identity."
  exit 0
fi

# Move the whole instance, including its keyring, database, and config. Moving only
# the database would preserve the DID. Keep a private backup for recovery.
BACKUP_DIR=$(mktemp -d "$LOCAL_PARENT/cleanup-testnet-backup.XXXXXX")
mv "$RUN_DIR" "$BACKUP_DIR/instance"
echo "Test data and keys moved to $BACKUP_DIR/instance"
echo "The next run will create new node keys and a new DID."
echo "Existing on-chain registrations remain unchanged."
