#!/usr/bin/env bash
# Phase 0 MCP fault matrix (PLAN-gateway-supervisor-refactor.md §6.2, §13).
#
# Exercises the frozen snapshot's failure behaviour against a SCRATCH instance of
# the bridge. Nothing production-facing is touched: the scratch instance gets its
# own HTTP port, its own heartbeat/persistence paths, a dead or relayed broker
# port and client id 778. The installed service keeps its own port and id, and
# this script never starts, stops or restarts it.
#
# Cases:
#   M1   broker absent from the start      -> delegates to verify-outage-policy.sh
#   M2a  established session is lost       -> paper gateway reached via a socat relay
#   M2b  session recovers when it returns
#   M3   reconnect/backoff/canary units    -> cargo test, isolated target dir
#   M4   HTTP listener bind failure        -> port already taken; the second
#                                             instance must exit NON-ZERO
#   M5   fragmented / partial self-probe   -> cargo, fault_matrix_phase0 units
#   M6   corrupt + clock-shifted snapshot  -> process level, own persist path
#   M7   no broker-triggered process exit  -> aggregate over every log above
#
# Ports are derived at run time from $$ + RANDOM (never the installed service's
# ports) and the paper gateway's raw port is read from its IBC ini, so nothing
# here hardcodes a live endpoint.
#
# Usage:  bash scripts/fault-matrix.sh [--quick] [--keep]
#           --quick  shorten the M1 window to 90 s (default 180 s)
#           --keep   keep the scratch directory (logs, samples, snapshots)
# Env:    BIN, CARGO_TARGET_DIR, WORK, PAPER_RAW_PORT, DISCONNECT_SECS,
#         LOSS_TIMEOUT, RECOVER_TIMEOUT, CLIENT_ID, KEEP
#
# Exit:   0 = every case passed, 1 = at least one case failed, 3 = setup error.
set -uo pipefail

REPO_ROOT=${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
BIN=${BIN:-$REPO_ROOT/target/release/ibkr-mcp-rs}
CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$REPO_ROOT/target}
WORK=${WORK:-$(mktemp -d "${TMPDIR:-/tmp}/mcp-fault-matrix.XXXXXX")}
mkdir -p "$WORK"
KEEP=${KEEP:-0}
DISCONNECT_SECS=${DISCONNECT_SECS:-180}
LOSS_TIMEOUT=${LOSS_TIMEOUT:-150}
RECOVER_TIMEOUT=${RECOVER_TIMEOUT:-180}
CLIENT_ID=${CLIENT_ID:-778}
MCP_HOST=127.0.0.1
IBC_PAPER_INI=${IBC_PAPER_INI:-/tmp/ibc-paper.ini}

while [ $# -gt 0 ]; do
    case "$1" in
        --keep) KEEP=1 ;;
        --quick) DISCONNECT_SECS=90 ;;
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
        *) printf 'unknown argument: %s\n' "$1" >&2; exit 2 ;;
    esac
    shift
done

# ---------------------------------------------------------------- scratch ports
SLOT=$(( (RANDOM + $$) % 1500 ))
PORT_MCP=$(( 21000 + SLOT * 4 ))
PORT_DEAD=$(( PORT_MCP + 1 ))
PORT_RELAY=$(( PORT_MCP + 2 ))
PORT_TAKEN=$(( PORT_MCP + 3 ))
for p in "$PORT_MCP" "$PORT_DEAD" "$PORT_RELAY" "$PORT_TAKEN"; do
    if ss -ltn 2>/dev/null | grep -q ":$p "; then
        printf 'error: scratch port %s is already in use; re-run\n' "$p" >&2
        exit 3
    fi
done

PAPER_RAW_PORT=${PAPER_RAW_PORT:-}
if [ -z "$PAPER_RAW_PORT" ] && [ -r "$IBC_PAPER_INI" ]; then
    PAPER_RAW_PORT=$(sed -n 's/^[[:space:]]*OverrideTwsApiPort[[:space:]]*=[[:space:]]*//p' "$IBC_PAPER_INI" | tail -1)
fi

# ------------------------------------------------------------------ bookkeeping
PASS=0; FAIL=0; SKIP=0
PIDS=(); RELAY_PID=""

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && kill -TERM "$pid" 2>/dev/null
    done
    pkill -f "TCP-LISTEN:$PORT_RELAY" 2>/dev/null
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && kill -KILL "$pid" 2>/dev/null
    done
    if [ "$KEEP" = "1" ]; then
        printf '\nscratch kept at %s\n' "$WORK"
    else
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

ok()   { PASS=$((PASS+1)); printf '  ok    %s\n' "$*"; }
bad()  { FAIL=$((FAIL+1)); printf '  FAIL  %s\n' "$*"; }
skip() { SKIP=$((SKIP+1)); printf '  skip  %s\n' "$*"; }
sec()  { printf '\n== %s ==\n' "$*"; }

# -------------------------------------------------------------------- MCP start
# start_mcp <tag> <broker-port> <http-port> [extra env assignments...]
start_mcp() {
    local tag="$1" bport="$2" hport="$3"; shift 3
    local log="$WORK/$tag.log"
    env IBKR_MCP_MCP__HOST="$MCP_HOST" \
        IBKR_MCP_MCP__PORT="$hport" \
        IBKR_MCP_IBKR__HOST="$MCP_HOST" \
        IBKR_MCP_IBKR__PORT="$bport" \
        IBKR_MCP_IBKR__CLIENT_ID="$CLIENT_ID" \
        IBKR_MCP_IBKR__PAPER_TRADING=true \
        IBKR_MCP_IBKR__READ_ONLY=true \
        IBKR_MCP_HEARTBEAT_PATH="$WORK/$tag.heartbeat" \
        IBKR_MCP_PERSIST_PATH="$WORK/$tag.last-good.json" \
        RUST_LOG=info \
        "$@" \
        "$BIN" >>"$log" 2>&1 &
    LAST_PID=$!
    PIDS+=("$LAST_PID")
    printf '   [%s] pid=%s log=%s\n' "$tag" "$LAST_PID" "$log"
}

wait_http() { # wait_http <port> <pid> <seconds>
    local port="$1" pid="$2" secs="$3" i=0
    while [ "$i" -lt "$secs" ]; do
        if curl -sS -o /dev/null --max-time 2 "http://$MCP_HOST:$port/health/live" 2>/dev/null; then
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || return 1
        sleep 1; i=$((i+1))
    done
    return 1
}

broker_flag() { # broker_flag <port> -> true|false|unknown
    local body
    body=$(curl -sS --max-time 3 "http://$MCP_HOST:$1/health/broker" 2>/dev/null) || { echo unknown; return; }
    if printf '%s' "$body" | grep -q '"brokerConnected":[[:space:]]*true'; then
        echo true
    elif printf '%s' "$body" | grep -q '"brokerConnected":[[:space:]]*false'; then
        echo false
    else
        echo unknown
    fi
}

wait_broker() { # wait_broker <port> <pid> <true|false> <seconds>
    local port="$1" pid="$2" want="$3" secs="$4" i=0
    while [ "$i" -lt "$secs" ]; do
        [ "$(broker_flag "$port")" = "$want" ] && return 0
        kill -0 "$pid" 2>/dev/null || return 1
        sleep 3; i=$((i+3))
    done
    return 1
}

http_code() { curl -sS -o /dev/null -w '%{http_code}' --max-time 3 "http://$MCP_HOST:$1$2" 2>/dev/null || echo ERR; }

# ------------------------------------------------------------------------ setup
if [ ! -x "$BIN" ]; then
    printf 'error: binary not found/executable: %s\n' "$BIN" >&2
    exit 3
fi
printf 'Phase 0 MCP fault matrix\n'
printf '  binary         %s\n' "$BIN"
sha256sum "$BIN"
printf '  repo           %s (%s)\n' "$REPO_ROOT" "$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
printf '  scratch dir    %s\n' "$WORK"
printf '  scratch ports  mcp=%s dead-broker=%s relay=%s taken=%s\n' "$PORT_MCP" "$PORT_DEAD" "$PORT_RELAY" "$PORT_TAKEN"
printf '  paper raw port %s\n' "${PAPER_RAW_PORT:-<not found>}"

# --------------------------------------------------------------------- M1
sec "M1 broker absent from the start (window ${DISCONNECT_SECS}s)"
M1_OUT="$WORK/m1"
if OUT="$M1_OUT" BIN="$BIN" PORT="$PORT_MCP" DEAD_PORT="$PORT_DEAD" REPO_ROOT="$REPO_ROOT" \
        bash "$REPO_ROOT/scripts/verify-outage-policy.sh" "$DISCONNECT_SECS" >"$WORK/m1.console" 2>&1; then
    ok "M1 stale-outage policy held for ${DISCONNECT_SECS}s (see m1.console)"
else
    bad "M1 stale-outage policy FAILED; tail of m1.console: $(tail -3 "$WORK/m1.console" | tr '\n' ' ')"
fi
grep -E '^(1|2|3|4)\.' "$WORK/m1.console" | sed 's/^/   /'
M1_LOG="$M1_OUT/scratch.log"

# --------------------------------------------------------------------- M2
sec "M2 established session loss and recovery (paper gateway via relay)"
if ! command -v socat >/dev/null 2>&1; then
    skip "socat is not installed - cannot inject a session loss"
elif [ -z "$PAPER_RAW_PORT" ]; then
    skip "no OverrideTwsApiPort in $IBC_PAPER_INI - cannot reach the paper gateway"
elif ! ss -ltn 2>/dev/null | grep -q ":$PAPER_RAW_PORT "; then
    skip "paper gateway raw API port $PAPER_RAW_PORT is not listening"
else
    socat "TCP-LISTEN:$PORT_RELAY,bind=$MCP_HOST,fork,reuseaddr" "TCP:$MCP_HOST:$PAPER_RAW_PORT" \
        >>"$WORK/relay.log" 2>&1 &
    RELAY_PID=$!
    PIDS+=("$RELAY_PID")
    sleep 1
    start_mcp m2 "$PORT_RELAY" "$PORT_MCP"
    M2_PID=$LAST_PID
    if ! wait_http "$PORT_MCP" "$M2_PID" 30; then
        bad "M2 scratch instance never answered /health/live"
    elif wait_broker "$PORT_MCP" "$M2_PID" true 120; then
        ok "M2a.0 session established through the relay (brokerConnected=true)"
        pkill -f "TCP-LISTEN:$PORT_RELAY" 2>/dev/null
        RELAY_PID=""
        if wait_broker "$PORT_MCP" "$M2_PID" false "$LOSS_TIMEOUT"; then
            ok "M2a session loss detected within ${LOSS_TIMEOUT}s (brokerConnected=false)"
        else
            bad "M2a session loss NOT detected within ${LOSS_TIMEOUT}s"
        fi
        if kill -0 "$M2_PID" 2>/dev/null; then
            ok "M2 process survived the session loss (pid alive)"
        else
            bad "M2 process exited on session loss"
        fi
        [ "$(http_code "$PORT_MCP" /health/live)" = "200" ] \
            && ok "M2 /health/live still 200 during the loss" \
            || bad "M2 /health/live not 200 during the loss"
        socat "TCP-LISTEN:$PORT_RELAY,bind=$MCP_HOST,fork,reuseaddr" "TCP:$MCP_HOST:$PAPER_RAW_PORT" \
            >>"$WORK/relay.log" 2>&1 &
        RELAY_PID=$!
        PIDS+=("$RELAY_PID")
        if wait_broker "$PORT_MCP" "$M2_PID" true "$RECOVER_TIMEOUT"; then
            ok "M2b session recovered within ${RECOVER_TIMEOUT}s of the broker returning"
        else
            bad "M2b session did NOT recover within ${RECOVER_TIMEOUT}s"
        fi
    else
        skip "M2 could not establish a broker session through the relay (environmental); M2a/M2b not exercised"
    fi
    kill -TERM "$M2_PID" 2>/dev/null
    pkill -f "TCP-LISTEN:$PORT_RELAY" 2>/dev/null
    RELAY_PID=""
fi

# --------------------------------------------------------------------- M3/M5
sec "M3 reconnect/backoff/canary units + M5 self-probe units (cargo, isolated target)"
( cd "$REPO_ROOT" && CARGO_TARGET_DIR="$CARGO_TARGET_DIR" cargo test --locked --lib -- --list ) \
    >"$WORK/m3-list.log" 2>&1
M3_LISTED=$?
( cd "$REPO_ROOT" && CARGO_TARGET_DIR="$CARGO_TARGET_DIR" cargo test --locked --lib ) \
    >"$WORK/m3-test.log" 2>&1
M3_RC=$?
if [ "$M3_LISTED" -eq 0 ]; then
    printf '   unit tests in the frozen snapshot:\n'
    sed -n 's/: test$//p' "$WORK/m3-list.log" | sed 's/^/     /'
else
    printf '   (could not list unit tests; see m3-list.log)\n'
fi
if [ "$M3_RC" -eq 0 ]; then
    ok "M3 all $REPO_ROOT unit tests pass: $(grep -E '^test result' "$WORK/m3-test.log" | tail -1)"
else
    bad "M3 unit tests FAILED: $(grep -E '^test result' "$WORK/m3-test.log" | tail -1)"
fi
# m5a pins the FIXED behaviour of defect F4 (a fragmented 200 is judged
# ALIVE); m5c is its falsifier (a status line that never completes is still
# judged dead), so the probe cannot pass by treating "some bytes arrived" as
# liveness.
for t in m5a_fragmented_status_line_is_judged_alive m5b_headers_only_response_is_alive m5c_truncated_status_line_is_judged_dead; do
    if grep -qE "^test .*fault_matrix_phase0::$t \.\.\. ok" "$WORK/m3-test.log"; then
        ok "M5 $t"
    else
        bad "M5 $t not ok (see m3-test.log)"
    fi
done

# --------------------------------------------------------------------- M4
sec "M4 HTTP listener bind failure is typed and non-silent"
start_mcp m4a "$PORT_DEAD" "$PORT_TAKEN"
M4A_PID=$LAST_PID
if wait_http "$PORT_TAKEN" "$M4A_PID" 30; then
    ok "M4 holder instance owns the port"
    start_mcp m4b "$PORT_DEAD" "$PORT_TAKEN"
    M4B_PID=$LAST_PID
    M4B_EXIT=""
    for _ in $(seq 1 20); do
        if ! kill -0 "$M4B_PID" 2>/dev/null; then break; fi
        sleep 1
    done
    if kill -0 "$M4B_PID" 2>/dev/null; then
        bad "M4 second instance is still running with no listener (silent limbo)"
        kill -TERM "$M4B_PID" 2>/dev/null
    else
        wait "$M4B_PID" 2>/dev/null; M4B_EXIT=$?
        if grep -q 'MCP HTTP server error' "$WORK/m4b.log"; then
            ok "M4 bind failure is surfaced: $(grep -m1 'MCP HTTP server error' "$WORK/m4b.log" | cut -c1-160)"
        else
            bad "M4 bind failure produced no 'MCP HTTP server error' line"
        fi
        if [ "$M4B_EXIT" -ne 0 ]; then
            ok "M4 second instance exited non-zero (exit=$M4B_EXIT)"
        else
            bad "M4 second instance exited 0 without ever serving - systemd cannot tell this apart from a deliberate stop"
            grep -m1 'MCP HTTP server exited' "$WORK/m4b.log" | sed 's/^/        /'
        fi
    fi
    [ "$(http_code "$PORT_TAKEN" /health/live)" = "200" ] \
        && ok "M4 holder instance was unaffected" \
        || bad "M4 holder instance was disturbed"
    kill -TERM "$M4A_PID" 2>/dev/null
else
    bad "M4 holder instance never bound the port"
fi

# --------------------------------------------------------------------- M6
sec "M6 corrupt and clock-shifted persistence"
printf '{ this is not json' >"$WORK/m6a.last-good.json"
start_mcp m6a "$PORT_DEAD" "$PORT_MCP"
M6A_PID=$LAST_PID
if wait_http "$PORT_MCP" "$M6A_PID" 30; then
    if grep -q 'last-good.json corrupt, ignoring' "$WORK/m6a.log"; then
        ok "M6a corrupt snapshot is reported and ignored"
    else
        bad "M6a corrupt snapshot was not reported"
    fi
    [ "$(http_code "$PORT_MCP" /health/last-good)" = "404" ] \
        && ok "M6a /health/last-good is 404 with no usable snapshot" \
        || bad "M6a /health/last-good did not return 404"
    kill -0 "$M6A_PID" 2>/dev/null && ok "M6a process survived the corrupt snapshot" \
        || bad "M6a process died on the corrupt snapshot"
else
    bad "M6a instance never answered /health/live"
fi
kill -TERM "$M6A_PID" 2>/dev/null
sleep 1

FUTURE=$(( $(date +%s) + 3600 ))
{
    printf '{\n  "account": {\n'
    printf '    "account_id": "DU0000000",\n    "net_liquidation": 1.0,\n'
    printf '    "available_funds": 1.0,\n    "excess_liquidity": 1.0,\n'
    printf '    "buying_power": 1.0,\n    "currency": "USD",\n'
    printf '    "daily_pnl": 0.0,\n    "unrealized_pnl": 0.0,\n    "realized_pnl": 0.0\n  },\n'
    printf '  "fetched_at_unix": %s,\n  "loaded_from_disk": false\n}\n' "$FUTURE"
} >"$WORK/m6b.last-good.json"
start_mcp m6b "$PORT_DEAD" "$PORT_MCP"
M6B_PID=$LAST_PID
if wait_http "$PORT_MCP" "$M6B_PID" 30; then
    if grep -q 'Loaded last-good IBKR snapshot from disk' "$WORK/m6b.log"; then
        ok "M6b clock-shifted snapshot is loaded"
    else
        bad "M6b clock-shifted snapshot was not loaded"
    fi
    M6B_BODY=$(curl -sS --max-time 3 "http://$MCP_HOST:$PORT_MCP/health/last-good" 2>/dev/null)
    case "$M6B_BODY" in
        *'"ageSecs":0'*) ok "M6b future-dated snapshot reports ageSecs=0 (no underflow)" ;;
        *) bad "M6b future-dated snapshot reported ${M6B_BODY:0:160}" ;;
    esac
    kill -0 "$M6B_PID" 2>/dev/null && ok "M6b process survived the clock shift" \
        || bad "M6b process died on the clock shift"
else
    bad "M6b instance never answered /health/live"
fi
kill -TERM "$M6B_PID" 2>/dev/null

# --------------------------------------------------------------------- M7
sec "M7 no broker-triggered process exit (aggregate)"
EXIT_LINES=0
for log in "$WORK"/*.log; do
    [ -f "$log" ] || continue
    n=$(grep -c 'exiting for systemd restart' "$log" 2>/dev/null || true)
    if [ "${n:-0}" -gt 0 ]; then
        EXIT_LINES=$((EXIT_LINES + n))
        printf '   %s: %s exit line(s)\n' "$(basename "$log")" "$n"
        grep -m2 'exiting for systemd restart' "$log" | sed 's/^/        /'
    fi
done
if [ "$EXIT_LINES" -eq 0 ]; then
    ok "M7 no 'exiting for systemd restart' line in any scratch log"
else
    bad "M7 $EXIT_LINES 'exiting for systemd restart' line(s) in the scratch logs"
fi
[ -n "$M1_LOG" ] && [ -f "$M1_LOG" ] && {
    printf '   M1 canary lines: first-failure=%s still-down=%s\n' \
        "$(grep -c 'Canary: broker not connected' "$M1_LOG")" \
        "$(grep -c 'Canary: broker still not connected' "$M1_LOG")"
}

# ------------------------------------------------------------------ summary
sec "summary"
printf '  %s ok, %s fail, %s skipped\n' "$PASS" "$FAIL" "$SKIP"
printf '  transcript dir: %s\n' "$WORK"
if [ "$FAIL" -eq 0 ]; then
    printf '  RESULT: PASS\n'
    exit 0
fi
printf '  RESULT: FAIL\n'
printf '  NOTE: a FAIL here means the frozen snapshot does not meet a requirement\n'
printf '        of the plan - it is a FINDING, not a broken harness. See\n'
printf '        docs/mcp-phase0-baseline.md for the graded list.\n'
exit 1
