#!/usr/bin/env bash
# Scratch dead-port verification for the broker-outage policy.
#
# Runs the freshly built bridge against a DEAD IBKR port (4999) on MCP port 8899
# and samples it for the whole window. Nothing production-facing is touched: the
# live service owns 8881 and nothing listens on 4999.
#
# Asserts (exit non-zero on failure):
#   1. the process is still alive at the end of the window (it used to exit at
#      ~5 min / 5 canary ticks)
#   2. /health/live answered 200 on every sample
#   3. the log contains backoff lines
#   4. the log contains NO "exiting for systemd restart" line
#   5. get_connection_status answers promptly with brokerConnected:false
#
# Usage: bash scripts/verify-outage-policy.sh [seconds]   (default 700)
set -uo pipefail

DUR=${1:-700}
BIN=${BIN:-/tmp/ibkr-mcp-target/release/ibkr-mcp-rs}
PORT=${PORT:-8899}
DEAD_PORT=${DEAD_PORT:-4999}
OUT=${OUT:-/tmp/ibkr-outage-verify}
mkdir -p "$OUT"
LOG=$OUT/scratch.log
SAMPLES=$OUT/samples.tsv
: >"$LOG"
: >"$SAMPLES"
printf 't\tpid_alive\thealth_live_code\thealth_broker\n' >>"$SAMPLES"

if [[ ! -x $BIN ]]; then
  echo "error: binary not found/executable: $BIN" >&2
  exit 3
fi

REPO_ROOT=${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
cd "$REPO_ROOT" || exit 1

echo "==> binary: $BIN"
sha256sum "$BIN"
echo "==> starting scratch instance ($PORT -> dead $DEAD_PORT), log: $LOG"

IBKR_MCP_MCP__HOST=127.0.0.1 \
IBKR_MCP_MCP__PORT=$PORT \
IBKR_MCP_IBKR__HOST=127.0.0.1 \
IBKR_MCP_IBKR__PORT=$DEAD_PORT \
IBKR_MCP_IBKR__CLIENT_ID=777 \
IBKR_MCP_IBKR__PAPER_TRADING=false \
IBKR_MCP_IBKR__READ_ONLY=true \
IBKR_MCP_HEARTBEAT_PATH=/tmp/ibkr-mcp-heartbeat-$PORT \
IBKR_MCP_PERSIST_PATH=/tmp/ibkr-mcp-last-good-$PORT.json \
RUST_LOG=info \
  "$BIN" >>"$LOG" 2>&1 &
PID=$!
echo "==> scratch pid: $PID"

# Wait for the HTTP listener to bind before starting the observation window,
# otherwise the first sample races process startup and reports a bogus non-200.
READY=0
for _ in $(seq 1 30); do
  if curl -sS -o /dev/null --max-time 2 "http://127.0.0.1:$PORT/health/live" 2>/dev/null; then
    READY=1
    break
  fi
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "!! scratch process died during startup" >&2
    break
  fi
  sleep 1
done
if [[ $READY -ne 1 ]]; then
  echo "!! /health/live never became reachable — aborting" >&2
  kill -TERM "$PID" 2>/dev/null
  exit 3
fi
echo "==> /health/live answering; starting ${DUR}s observation window"

START=$(date +%s)
while :; do
  NOW=$(date +%s); T=$((NOW - START))
  [[ $T -ge $DUR ]] && break

  if kill -0 "$PID" 2>/dev/null; then ALIVE=yes; else ALIVE=NO; fi
  LIVE_CODE=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 3 "http://127.0.0.1:$PORT/health/live" 2>/dev/null || echo ERR)
  BROKER=$(curl -sS --max-time 3 "http://127.0.0.1:$PORT/health/broker" 2>/dev/null | tr -d '\n' | tr '\t' ' ')
  printf '%s\t%s\t%s\t%s\n' "$T" "$ALIVE" "$LIVE_CODE" "${BROKER:-<none>}" >>"$SAMPLES"
  echo "t=${T}s alive=$ALIVE health_live=$LIVE_CODE broker=${BROKER:-<none>}"
  sleep 15
done

if kill -0 "$PID" 2>/dev/null; then FINAL_ALIVE=yes; else FINAL_ALIVE=NO; fi

echo
echo "==> get_connection_status during the outage"
python3 scripts/mcp_connection_status.py --url "http://127.0.0.1:$PORT/mcp" --json "$OUT/connection-status.json" \
  >"$OUT/connection-status.txt" 2>&1
MCP_RC=$?
cat "$OUT/connection-status.txt"

echo
echo "==> stopping scratch instance $PID"
kill -TERM "$PID" 2>/dev/null
wait "$PID" 2>/dev/null

echo
echo "===== RESULT ====="
FAIL=0
echo "1. process survived ${DUR}s: $FINAL_ALIVE"
[[ $FINAL_ALIVE == yes ]] || FAIL=1
BAD_LIVE=$(awk -F'\t' 'NR>1 && $3 != 200 {c++} END {print c+0}' "$SAMPLES")
echo "2. /health/live non-200 samples: $BAD_LIVE (of $(( $(wc -l <"$SAMPLES") - 1 )))"
[[ $BAD_LIVE -eq 0 ]] || FAIL=1
BACKOFF=$(grep -c 'Waiting before IBKR reconnect' "$LOG")
echo "3. backoff lines in log: $BACKOFF"
[[ $BACKOFF -ge 5 ]] || FAIL=1
CANARY_EXIT=$(grep -c 'exiting for systemd restart' "$LOG")
echo "4. 'exiting for systemd restart' lines in log: $CANARY_EXIT (must be 0)"
[[ $CANARY_EXIT -eq 0 ]] || FAIL=1
echo "5. get_connection_status rc=$MCP_RC"
[[ $MCP_RC -eq 0 ]] || FAIL=1
echo "6. canary outage lines:"
grep -c 'Canary: broker not connected' "$LOG" | sed 's/^/   first-failure /'
grep -c 'Canary: broker still not connected' "$LOG" | sed 's/^/   still-down    /'

echo
if [[ $FAIL -eq 0 ]]; then echo "ALL CHECKS PASSED"; else echo "FAILURES PRESENT"; fi
echo "transcript: $LOG"
echo "samples:    $SAMPLES"
exit $FAIL
