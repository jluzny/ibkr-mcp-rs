#!/usr/bin/env bash
# Deploy the locally built ibkr-mcp-rs binary to /usr/local/bin and restart the
# native systemd unit (WSL2 systemd, NOT the Docker image).
#
# The install + restart are privileged, so the script re-execs itself through
# sudo when it is not already running as root (you will be prompted once).
#
# Usage:
#   cd ~/dev/trading/ibkr-mcp-rs
#   bash scripts/deploy-native.sh                 # deploy dist/ibkr-mcp-rs
#   bash scripts/deploy-native.sh path/to/binary  # deploy an explicit build
#   bash scripts/deploy-native.sh --skip-verify   # install/restart only
#
# What it does:
#   1. checks the source binary exists and prints its sha256
#   2. backs up the currently installed binary to <dest>.bak-<timestamp>
#   3. installs the new binary (0755 root:root) over /usr/local/bin/ibkr-mcp-rs
#   4. systemctl restart ibkr-mcp.service and waits for /health/ready
#   5. best-effort: runs scripts/verify-market-data-tools.py as the invoking user
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DEST=/usr/local/bin/ibkr-mcp-rs
UNIT=ibkr-mcp.service
HEALTH=http://127.0.0.1:8881/health/ready

SKIP_VERIFY=0
ARGS=()
for arg in "$@"; do
  case "$arg" in
    --skip-verify) SKIP_VERIFY=1 ;;
    *) ARGS+=("$arg") ;;
  esac
done

SRC=${ARGS[0]:-$REPO_ROOT/dist/ibkr-mcp-rs}
# resolve relative to the repo root so the sudo re-exec gets an absolute path
[[ $SRC = /* ]] || SRC=$REPO_ROOT/$SRC

if [[ ! -f $SRC ]]; then
  echo "error: binary not found: $SRC" >&2
  echo "build it first:" >&2
  echo "  CARGO_TARGET_DIR=/tmp/ibkr-mcp-target cargo build --release" >&2
  echo "  install -m 755 /tmp/ibkr-mcp-target/release/ibkr-mcp-rs dist/ibkr-mcp-rs" >&2
  exit 1
fi

echo "==> source binary: $SRC"
sha256sum "$SRC"

if [[ $(id -u) -ne 0 ]]; then
  echo "==> re-executing under sudo (install to $DEST + restart $UNIT)"
  if [[ $SKIP_VERIFY -eq 1 ]]; then
    exec sudo -- "$0" --skip-verify "$SRC"
  else
    exec sudo -- "$0" "$SRC"
  fi
fi

OWNER=${SUDO_USER:-root}

if [[ -f $DEST ]]; then
  OLD_HASH=$(sha256sum "$DEST" | cut -d' ' -f1)
  if [[ $OLD_HASH == $(sha256sum "$SRC" | cut -d' ' -f1) ]]; then
    echo "==> note: $DEST already has this exact build (sha256 $OLD_HASH); continuing anyway"
  fi
  BACKUP=$DEST.bak-$(date +%Y%m%d-%H%M%S)
  echo "==> backing up current binary -> $BACKUP"
  cp -a "$DEST" "$BACKUP"
else
  echo "==> no existing $DEST, nothing to back up"
fi

echo "==> installing $SRC -> $DEST"
install -m 0755 -o root -g root "$SRC" "$DEST"

echo "==> restarting $UNIT"
systemctl restart "$UNIT"

echo "==> waiting for $HEALTH (up to 30s)"
ready=0
for _ in $(seq 1 30); do
  if curl -fsS --max-time 3 "$HEALTH" >/tmp/ibkr-health.json 2>/dev/null; then
    ready=1
    break
  fi
  sleep 1
done

echo "--- systemctl is-active ---"
systemctl is-active "$UNIT" || true
echo "--- systemctl status (first lines) ---"
systemctl status "$UNIT" --no-pager | head -8 || true
echo "--- installed binary ---"
sha256sum "$DEST"
echo "--- health ---"
if [[ $ready -eq 1 ]]; then
  cat /tmp/ibkr-health.json; echo
else
  echo "!! $HEALTH did not become ready within 30s - check: journalctl -u $UNIT -n 50" >&2
fi

VERIFY_CMD="cd $REPO_ROOT && IBKR_MCP_URL=http://127.0.0.1:8881/mcp python3 scripts/verify-market-data-tools.py --json /tmp/ibkr-verify-deployed.json"

if [[ $SKIP_VERIFY -eq 1 ]]; then
  echo "--- verification skipped (--skip-verify) ---"
  echo "run it as $OWNER:"
  echo "  $VERIFY_CMD"
  exit 0
fi

echo "==> best-effort verification as $OWNER"
if [[ $OWNER != root ]] && sudo -u "$OWNER" -H bash -lc "$VERIFY_CMD"; then
  echo "==> verification OK (results: /tmp/ibkr-verify-deployed.json)"
else
  echo "!! verification step failed or could not run as $OWNER - run it by hand:" >&2
  echo "   $VERIFY_CMD" >&2
  exit 1
fi
