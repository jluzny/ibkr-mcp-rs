# Broker outage policy

How `ibkr-mcp-rs` behaves when the IBKR gateway is unreachable or not logged
in, and why. This is the operator-facing companion to the module docs in
`src/ibkr/client.rs`.

## Principle

**An unreachable broker is not a process fault.**

IB Gateway can sit for hours waiting for a human 2FA approval, or be stopped
deliberately for maintenance. Restarting the MCP bridge cannot change either
condition — it only produces a restart loop, a fresh `client_id` handshake
storm against a gateway that is mid-login, and enough journal noise to hide
real faults. So the bridge stays up, keeps serving MCP, and retries forever
with a bounded backoff.

`std::process::exit(1)` is reserved for cases where the *process itself* is
broken and a systemd restart genuinely fixes it.

## Failure classes and reactions

| Class | Detection | Reaction |
|-------|-----------|----------|
| Broker unreachable (TCP refused / `early eof`) | reconnect pre-flight + `try_connect_once` | Log, sleep backoff, retry. **Never exits.** |
| Broker session stale (TCP up, API dead) | `maintain_connection` probe (`managed_accounts`, 10 s timeout) | Drop the stale client, hand back to the reconnect loop. **Never exits.** |
| Broker session lost (TCP down while connected) | `maintain_connection` fast path | Drop the stale client, hand back to the reconnect loop. **Never exits.** |
| Own HTTP server wedged | canary self-probe of `GET /health/live` (raw socket, 5 s timeout, reads accumulated until the status line is complete) | 3 consecutive failures (~3 min) → `exit(1)` for systemd restart. A `200` split across TCP segments is **not** a false positive. |
| Reconnect task panicked | supervisor on the reconnect `JoinHandle` | `exit(1)` for systemd restart. |
| HTTP server task panicked / ended | `tokio::select!` in `main.rs` | `exit(1)` for systemd restart. |
| HTTP listener cannot bind (port already taken) | `TcpListener::bind` in `main.rs`, before the server task is spawned | typed `MCP HTTP server error` + `exit(1)`, before any traffic is served. Never a clean `exit 0`: a bridge that never came up must not look like a deliberate stop. |

Dropping the stale client matters for correctness, not just tidiness: while the
dead session is still in the client slot, `is_connected()` returns `true` and
`get_connection_status` would describe a corpse as connected. After a drop, the
same call truthfully reports `brokerConnected: false` while the reconnect loop
works in the background.

## Reconnect backoff

`IbkrClient::backoff(attempts)` — bounded, monotonic, never overflows, never
gives up:

| attempts | delay |
|----------|-------|
| 0 | 0 |
| 1 … 11 | `500 ms · 1.6^(n-1)` → 0.5 s, 0.8 s, 1.28 s, 2.05 s, 3.28 s, 5.24 s, 8.39 s, 13.4 s, 21.5 s, 34.4 s, 55.0 s |
| 12 … 14 | capped at **60 s** |
| ≥ 15 (≈10 min of failures) | flat **300 s** |

The flat 300 s tier exists for multi-hour outages: an outage that outlives a
2FA prompt — or runs into a weekend — must not hammer the gateway with a
handshake attempt every minute. Worst-case recovery latency when the broker
comes back is therefore 5 minutes, which is well inside the hourly consumer
cadence.

One exception shortens recovery: when the TCP pre-flight itself was failing
(the gateway's listener was closed) and then succeeds, `attempts` is reset and
the handshake is attempted immediately instead of waiting out the calm tier.

The 20-hour silent-outage regression from 2026-08-26 is still guarded: the
exponent is capped at 40 before `powi`, so `mul_f64` can never overflow and
panic, and the reconnect task remains supervised.

## Canary

`IbkrClient::start_canary` spawns one task that ticks every 60 s and does two
independent jobs.

**Broker probe** — `is_connected()` plus a `managed_accounts()` probe (10 s
timeout). On success it resets the failure counter and writes the heartbeat
file (`IBKR_MCP_HEARTBEAT_PATH`, default `/tmp/ibkr-mcp-heartbeat` — this file
means "the broker link is healthy", *not* "the process is alive"). On failure
it increments `broker_failures` and logs:

* failure #1 — `WARN` `Canary: broker not connected — bridge stays up and keeps serving MCP, reconnecting with backoff`
* failure #10, #20, … — `INFO` `Canary: broker still not connected — bridge still up and serving MCP`
* recovery — `INFO` `Canary: broker link restored`

A multi-hour outage therefore costs about 6 journal lines per hour instead of
one line per minute.

**Failure accounting.** The counter is per canary tick and is not shared with
the reconnect loop: a tick counts exactly one failure whether the client slot
is empty or the probe request failed. Handshake attempts made by `connect()` are
never counted here, so a slow initial login cannot inflate the counter, and a
tick interval cannot double-count a single outage. (Checked against the
2026-09-18 journal: failures advanced exactly 1 per 60 s.)

**Pending startup.** The previous policy exited after 5 consecutive failures
(~5 min) with no grace period, so a process that needed longer than 5 minutes
to complete its first login — routine while IBC waits out a 2FA prompt — was
killed by its own canary. There is now no such deadline: the counter only ever
drives logging and the diagnostic fields below.

**Self-health probe** — HTTP mode only. A raw-socket `GET /health/live` against
our own listener (wildcard binds are mapped to loopback). This is the only
canary result that can exit the process; 3 consecutive failures mean the process
is wedged and the systemd restart is the right answer. Stdio mode passes `None`
and skips the check.

The probe reads **until the status line is complete** (first `\n`, EOF, or 1 KiB)
inside one 5 s budget, instead of requiring the whole status line inside a single
64-byte read. A `200` fragmented across TCP segments is therefore judged alive,
and a status line that never completes (or a non-`200` code) is still judged
dead. The earlier single-read form was a load-dependent false positive that
restarted a perfectly healthy bridge after 3 ticks.

## Exit codes

| code | meaning |
|------|---------|
| `0` | deliberate stop (SIGINT / graceful shutdown): the process stopped because it was asked to. |
| `1` | the process is broken and a systemd restart is the right answer: the HTTP listener could not be bound, the HTTP server task failed or panicked, the server stopped serving without a shutdown request, the reconnect task panicked, or the canary judged the process wedged. |

`Restart=always` plus `StartLimitBurst` in the unit bounds the restart rate; a
non-zero exit is what makes a failed start visible instead of being recorded as
a clean stop.

## What consumers see during an outage

| Surface | Behaviour |
|---------|-----------|
| `GET /health/live` | `200 {"alive":true}` — always, while the process runs. |
| `GET /health/ready` | `503` with `brokerConnected:false` — readiness includes the broker on purpose. |
| `GET /health/broker` | `200` with `brokerConnected`, `consecutiveFailures`, `outageSeconds` (counted from the first failed canary tick; `null` while healthy). |
| `GET /health/last-good` | Last persisted account snapshot with `ageSecs` — consumers decide how stale is too stale. |
| `get_connection_status` (MCP tool) | Returns promptly with `success:true`, `brokerConnected:false`, `consecutiveFailures`, `outageSeconds` and a `message`. Not an error, not a hang. |
| Broker-backed tools | Fail fast with `not connected to IBKR` rather than hanging on a dead session. |

## systemd unit expectations

Two artifacts, because a systemd drop-in can only **add** to a unit — it cannot
remove anything the main unit file declared:

| Artifact | Carries | Install |
|----------|---------|---------|
| `systemd/ibkr-mcp.service` | the whole unit: `Requires=` and `Wants=` removed (`After=` ordering only), restart limits, `RestartSec`, health contract version | `sudo bash scripts/install-mcp-tuple.sh --mode=unit` |
| `systemd/ibkr-mcp.service.d/10-restart-policy.conf` | restart limits + `RestartSec` only | `sudo bash scripts/install-mcp-tuple.sh --mode=dropin` |

Use `--mode=unit` if you want the gateway-coupling change: it replaces
`/etc/systemd/system/ibkr-mcp.service` (a dated copy of what it replaced is
written under `backups/` first) and removes a superseded drop-in.

`scripts/install-mcp-tuple.sh` is the **only** installer of this unit, in either
repository. It installs the whole qualified tuple (binary + unit + config) from
an artifact manifest, never restarts anything, and refuses a unit that carries a
Gateway activation edge. `ibkr-stack/scripts/install.sh` installs gateway units
only - it used to copy every `systemd/*.service`, which silently reinstated the
old `Requires=ib-gateway-live.service` edge (Phase 0 finding F1).

What the policy encodes:

* No `Requires=` and no `Wants=` to the gateway - `After=` ordering only - so the
  bridge can never activate the gateway, and a deliberately stopped gateway
  no longer takes the bridge down with it. The bridge keeps answering "broker not connected".
* `StartLimitIntervalSec=300` / `StartLimitBurst=5` bound the restart count, so
  a long outage can never again produce 60+ restarts.
* `RestartSec=30` — restarts are exceptional now; the old 5 s cadence amplified
  the gateway's handshake storm.

Neither installer restarts the service. A restart drops the current IBKR session
and re-handshakes with a fresh `client_id`, so do it deliberately.

### Why a drop-in is not enough for the dependency change

Measured on this host (systemd 261) with a throwaway user unit that declared
`Requires=` + `After=` and a drop-in that assigned both empty:

```
$ systemctl --user show zz-dep-test.service -p Requires -p Wants -p After
Requires=basic.target zz-dep-a.service      # <- not reset by the drop-in
Wants=zz-dep-a.service
After=basic.target zz-dep-a.service         # <- not reset either
```

Removing the same lines by editing the main unit file did work. Hence the full
unit above, with the drop-in kept only for the directives a drop-in can affect.

## Verifying without touching production

Run a scratch instance against a dead port — no outage required:

```bash
IBKR_MCP_MCP__HOST=127.0.0.1 IBKR_MCP_MCP__PORT=8899 \
IBKR_MCP_IBKR__HOST=127.0.0.1 IBKR_MCP_IBKR__PORT=4999 \
IBKR_MCP_IBKR__CLIENT_ID=777 \
IBKR_MCP_HEARTBEAT_PATH=/tmp/ibkr-mcp-heartbeat-8899 \
IBKR_MCP_PERSIST_PATH=/tmp/ibkr-mcp-last-good-8899.json \
  /tmp/ibkr-mcp-target/release/ibkr-mcp-rs
```

Expected: the process survives indefinitely (it used to exit at ~5 min / 5 canary
ticks), the log shows `IBKR connection failed` → `Waiting before IBKR reconnect`
with growing delays, `Canary: broker not connected — bridge stays up …` on the
first tick, then one line per 10 ticks, and **no** `exiting for systemd restart`
line. Meanwhile:

```bash
curl -sS http://127.0.0.1:8899/health/live     # 200 {"alive":true}
curl -sS http://127.0.0.1:8899/health/broker   # 200 brokerConnected:false + counters
```

## History

* 2026-08-26 — reconnect loop died silently for 20 h on a `backoff()` overflow;
  fixed plus a canary that exits after 5 failures.
* 2026-09-18 — that canary produced **59 systemd restarts in a few hours**
  because the gateway was waiting for a human 2FA approval. Restarting the
  bridge could not fix it, and every restart re-ran the `client_id` handshake
  against a gateway that was mid-login. The exit-on-broker-failure policy was
  removed, the backoff gained a calm tier, the unit gained restart limits, and
  the canary was repurposed to detect a wedged *process* instead.
