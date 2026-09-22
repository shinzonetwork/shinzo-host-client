#!/bin/sh
# Shared paths and lock for the local run and reset commands.
# The caller sets REPO_DIR from its own script location.
LOCAL_PARENT="$REPO_DIR/.local_tests"
RUN_DIR="$LOCAL_PARENT/cleanup-testnet-host"
LOCK_DIR="$LOCAL_PARENT/cleanup-testnet-host.lock"

lock_test_host() {
  umask 077
  if [ -L "$LOCAL_PARENT" ] || [ -L "$RUN_DIR" ]; then
    echo "The local test directory must not be a symbolic link." >&2
    exit 1
  fi
  mkdir -p "$LOCAL_PARENT"
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "The test host is running or another reset is in progress." >&2
    echo "If a previous run was killed, stop its host before removing $LOCK_DIR." >&2
    exit 1
  fi
}

unlock_test_host() {
  rmdir "$LOCK_DIR"
}
