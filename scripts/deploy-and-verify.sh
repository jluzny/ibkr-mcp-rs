#!/usr/bin/env bash
# Deploy the staged ibkr-mcp-rs build (historical bars / IV rank / scanners / WSH events)
# and verify the DEPLOYED service. One sudo prompt, then a full verdict.
#
# Run:  bash /home/jiri/dev/trading/ibkr-mcp-rs/scripts/deploy-and-verify.sh
#
# Safe by design: it refuses to deploy if the staged binary's sha256 is not the
# build that was independently verified, and it verifies the running service
# (not just the file on disk) at the end.
set -euo pipefail

REPO="/home/jiri/dev/trading/ibkr-mcp-rs"
BIN="$REPO/dist/ibkr-mcp-rs"
VERIFY="$REPO/scripts/verify-market-data-tools.py"
EXPECTED_STAGED_SHA="6b9e39f19098ad52bfc5c27949b95177a392f8d3999cf6a971de7d15beb77ac8"
PREV_DEPLOYED_SHA="dec576df564a11857ff561c8be1494e8a9c1f90bd48188627f5408efdd9805c9"

[ -x "$BIN" ] || { echo "!! staged binary missing: $BIN"; exit 1; }
[ -f "$VERIFY" ] || { echo "!! verify script missing: $VERIFY"; exit 1; }

STAGED_SHA="$(sha256sum "$BIN" | awk '{print $1}')"
echo "== staged binary   : $BIN"
echo "== staged sha256   : $STAGED_SHA"
if [ "$STAGED_SHA" != "$EXPECTED_STAGED_SHA" ]; then
  echo "!! staged hash does NOT match the verified build ($EXPECTED_STAGED_SHA) — aborting."
  exit 1
fi
echo "== deployed sha256 : $(sha256sum /usr/local/bin/ibkr-mcp-rs | awk '{print $1}')  (expected old: $PREV_DEPLOYED_SHA)"

echo
echo "== deploying (single sudo prompt) ..."
sudo bash -c "
  set -e
  install -o root -g root -m 755 '$BIN' /usr/local/bin/ibkr-mcp-rs
  systemctl restart ibkr-mcp
"

sleep 5
echo
echo "== service state   : $(systemctl is-active ibkr-mcp.service)"
echo "== deployed sha256 : $(sha256sum /usr/local/bin/ibkr-mcp-rs | awk '{print $1}')"
echo "== /health/ready   : $(curl -s --max-time 5 http://127.0.0.1:8881/health/ready)"
echo
echo "== verifying the deployed tool surface ..."
IBKR_MCP_URL=http://127.0.0.1:8881/mcp /usr/bin/python3 "$VERIFY" --json /tmp/ibkr-verify-deployed.json
echo
echo "== DONE. Full results: /tmp/ibkr-verify-deployed.json"
