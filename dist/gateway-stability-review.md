# IB Gateway Stability Review (Aug 19, 2026)

## Current state
- Gateway live: PID 1256301, started Aug 19 18:18, 2FA approved in 7s, port 4001 open
- MCP: PID 1256302, connected, pump running, snapshot cached
- Paper gateway: running since Aug 15 23:45 (may have same issues)

## Defects found (7)

### 1. ReadOnlyLogin=yes is a no-op that clutters logs (HIGH)
**File**: `~/.hermes/ibc-config.ini.tmpl` line ~230
**Problem**: Template hardcodes `ReadOnlyLogin=yes`. IBC tries read-only login to skip 2FA, but IBKR Gateway doesn't support it — logs "Read-only login not supported by Gateway" every login (78 occurrences). The gateway ALWAYS falls through to full 2FA anyway.
**Fix**: Change to `ReadOnlyLogin=no` in both `.tmpl` files. This removes the confusing log line and makes the 2FA path explicit.
**Cannot apply**: file is under `~/.hermes/` (agent-instruction guard blocks writes in single-query mode).

### 2. SecondFactorDevice empty — 2FA device selection risk (MEDIUM)
**File**: `~/.hermes/ibc-config.ini.tmpl` — `SecondFactorDevice=${TWOFA_DEVICE}`
**Problem**: `TWOFA_DEVICE` is not set in `~/.hermes/ibgw.env`. If IBKR ever presents a device-selection list (e.g. after adding a second 2FA method), IBC can't auto-select on the headless display → 180s timeout → zombie.
**Current behavior**: Works because Jiri has only one 2FA device, so IBKR auto-selects it (2FA dialog opens and closes in 7s on Aug 19).
**Fix**: Set `TWOFA_DEVICE=IBKR Mobile` (or whatever the device name is in the IBKR 2FA dialog) in `~/.hermes/ibgw.env`. Check the IBC log or VNC to confirm the exact device name. Also add to `~/.hermes/ibgw-paper.env`.

### 3. No AutoRestartTime — daily restart triggers fresh 2FA (HIGH)
**File**: `~/.hermes/ibc-config.ini.tmpl` — `AutoRestartTime=${AUTO_RESTART_TIME}` (empty)
**Problem**: IBKR Gateway requires a daily restart. Without `AutoRestartTime`, the gateway either uses whatever was last configured in the GUI (may be nothing) or shuts down and lets systemd restart it — which triggers a fresh 2FA. With `AutoRestartTime` set, the gateway restarts internally WITHOUT requiring 2FA re-auth.
**Fix**: Set `AUTO_RESTART_TIME=04:00 AM` (or another time after US market close, in your timezone) in `~/.hermes/ibgw.env`. This makes the gateway auto-restart daily without 2FA. Also add `AUTO_RESTART_TIME` to the envsubst call in `start-gateway.sh` (it's currently not passed — see defect 4).

### 4. envsubst passes only 5 of 16 template variables (MEDIUM)
**File**: `/home/jiri/dev/tools/my-hermes/native/scripts/start-gateway.sh` lines 39-43
**Problem**: The template references 16 `${VAR}` placeholders but the envsubst call only passes 5: `TWS_USERID`, `TWS_PASSWORD`, `TRADING_MODE`, `RELOGIN_AFTER_TWOFA_TIMEOUT`, `TWOFA_DEVICE`, `TWOFA_EXIT_INTERVAL`. The other 11 resolve to empty:
  - `EXISTING_SESSION_DETECTED_ACTION` → empty (defaults to `manual` — blocks on session conflict)
  - `READ_ONLY_API` → empty (doesn't change gateway setting)
  - `AUTO_LOGOFF_TIME` → empty
  - `AUTO_RESTART_TIME` → empty (see defect 3)
  - `TWS_COLD_RESTART` → empty (no Sunday cold restart)
  - `TWS_ACCEPT_INCOMING` → empty (defaults to `manual` — blocks on API connection)
  - `ALLOW_BLIND_TRADING` → empty
  - `BYPASS_WARNING` → empty (all 9 bypass settings)
  - `SAVE_TWS_SETTINGS` → empty
  - `TWS_MASTER_CLIENT_ID` → empty
**Fix**: Add all missing vars to the envsubst call with sensible defaults:
```bash
TWS_USERID="$TWS_USERID" TWS_PASSWORD="$TWS_PASSWORD" \
TRADING_MODE="$TRADING_MODE" \
RELOGIN_AFTER_TWOFA_TIMEOUT="${RELOGIN_AFTER_TWOFA_TIMEOUT:-yes}" \
TWOFA_DEVICE="${TWOFA_DEVICE:-}" \
TWOFA_EXIT_INTERVAL="${TWOFA_EXIT_INTERVAL:-}" \
EXISTING_SESSION_DETECTED_ACTION="${EXISTING_SESSION_DETECTED_ACTION:-primaryoverride}" \
READ_ONLY_API="${READ_ONLY_API:-no}" \
AUTO_LOGOFF_TIME="${AUTO_LOGOFF_TIME:-}" \
AUTO_RESTART_TIME="${AUTO_RESTART_TIME:-04:00 AM}" \
TWS_COLD_RESTART="${TWS_COLD_RESTART:-07:00}" \
TWS_ACCEPT_INCOMING="${TWS_ACCEPT_INCOMING:-accept}" \
ALLOW_BLIND_TRADING="${ALLOW_BLIND_TRADING:-no}" \
BYPASS_WARNING="${BYPASS_WARNING:-no}" \
SAVE_TWS_SETTINGS="${SAVE_TWS_SETTINGS:-}" \
TWS_MASTER_CLIENT_ID="${TWS_MASTER_CLIENT_ID:-}" \
envsubst < "$IBC_CONFIG_TMPL" > "$IBC_CONFIG_RUNTIME"
```
**Key defaults**: `EXISTING_SESSION_DETECTED_ACTION=primaryoverride` (new session wins over stale), `TWS_ACCEPT_INCOMING=accept` (auto-accept API connections), `AUTO_RESTART_TIME=04:00 AM` (daily auto-restart without 2FA), `TWS_COLD_RESTART=07:00` (Sunday cold restart at a time after 01:00 US/Eastern).

### 5. Xvfb display conflict between live and paper (LOW)
**File**: `start-gateway.sh` lines 60-66
**Problem**: Both live and paper use `DISPLAY=:1` and `rm -f /tmp/.X1-lock`. If both start simultaneously (e.g. on boot), the second one kills the first's Xvfb.
**Fix**: Use different displays:
```bash
if [ "$TRADING_MODE" = "live" ]; then
    export DISPLAY=:1
    rm -f /tmp/.X1-lock
else
    export DISPLAY=:2
    rm -f /tmp/.X2-lock
fi
```

### 6. socat orphan processes on restart (LOW)
**File**: `start-gateway.sh` line 86
**Problem**: socat is started with `&` (background). When systemd kills the Java process (SIGTERM), socat children survive as orphans, holding the socat port. The next start's socat may fail silently.
**Fix**: Add cleanup before starting socat:
```bash
# Clean up any orphaned socat from previous run
pkill -f "socat TCP-LISTEN:${SOCAT_PORT}" 2>/dev/null || true
sleep 0.5
socat TCP-LISTEN:${SOCAT_PORT},fork,reuseaddr TCP:127.0.0.1:${API_PORT} &
```

### 7. MCP starts before gateway is ready (LOW)
**File**: `/etc/systemd/system/ibkr-mcp.service`
**Problem**: `Requires=ib-gateway-live.service` with no readiness check. MCP starts immediately when gateway process launches (Type=simple), hammers reconnects while gateway is doing 2FA (5+ min). The MCP's backoff handles this, but it wastes client IDs on the gateway.
**Fix**: Not easily fixable with systemd alone (gateway has no readiness notification). The MCP's reconnect backoff is the correct defense. Consider adding `RestartSec=30` to the MCP unit to delay initial start:
```ini
[Service]
ExecStartPre=/bin/sh -c 'until echo > /dev/tcp/127.0.0.1/4001 2>/dev/null; do sleep 2; done'
```
(This blocks MCP start until port 4001 is open. But it will also block MCP start if gateway is down for 2FA, which may not be desired.)

## Recommended env file additions

Add to `~/.hermes/ibgw.env`:
```bash
# 2FA device name (check VNC/IBC logs for exact name in the device list)
TWOFA_DEVICE=IBKR Mobile
# Daily auto-restart without 2FA (after US market close, in your timezone)
AUTO_RESTART_TIME=04:00 AM
# Sunday cold restart (after 01:00 US/Eastern)
TWS_COLD_RESTART=07:00
# Session conflict: new session overrides stale
EXISTING_SESSION_DETECTED_ACTION=primaryoverride
# Auto-accept incoming API connections
TWS_ACCEPT_INCOMING=accept
# Read-only API: no (allow order placement)
READ_ONLY_API=no
```

Add same to `~/.hermes/ibgw-paper.env` (with `AUTO_RESTART_TIME=04:00 AM` etc).

## Recommended template changes

In `~/.hermes/ibc-config.ini.tmpl` and `~/.hermes/ibc-config-paper.ini.tmpl`:
- `ReadOnlyLogin=yes` → `ReadOnlyLogin=no` (Gateway doesn't support read-only, always falls through to 2FA)

## Recommended start-gateway.sh changes

1. Add all 16 envsubst vars (see defect 4)
2. Use different Xvfb displays for live vs paper (see defect 5)
3. Clean up orphaned socat before starting new one (see defect 6)

## Impact summary

| Fix | 2FA frequency reduced? | Stability improved? |
|-----|----------------------|---------------------|
| AutoRestartTime | YES — daily restart without 2FA | YES — no systemd-triggered restarts |
| ColdRestartTime | YES — Sunday restart is scheduled | YES — no surprise shutdowns |
| ReadOnlyLogin=no | No (was already no-op) | Cleaner logs |
| TWOFA_DEVICE set | No (single device works) | Resilience if 2nd device added |
| envsubst vars | Indirectly (AutoRestart works) | YES — all settings properly configured |
| Xvfb display split | No | YES — no boot race |
| socat cleanup | No | YES — no port conflicts |
| ExistingSession=primaryoverride | No | YES — auto-recovers from stale sessions |