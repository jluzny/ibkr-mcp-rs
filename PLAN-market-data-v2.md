# PLAN — historical bars (IV history), scanners, WSH events

Task: `t_08f72de1` — extend `ibkr-mcp-rs` with read-only historical/IV-rank/scanner/WSH-event tools, build, deploy.
Status: plan → implement → deploy in this run (scope pre-approved in-session).

## 1. Decisions (settled before code)

### Where the code goes

Three new modules, each owning an `Arc<IbkrClient>` — the same shape as
`AccountManager` / `OrderManager` / `MarketDataManager`. No `pub(crate)` field
leaks, no duplication of the connection plumbing.

| Module | Type | Responsibility |
|---|---|---|
| `src/ibkr/history.rs` | `HistoricalDataManager` | daily/intraday bars, IV rank, historical ticks |
| `src/ibkr/scanner.rs` | `ScannerManager` | `scanner_subscription` + concurrency/row limits |
| `src/ibkr/events.rs` | `EventManager` | WSH metadata + event data, explicit entitlement state |

Shared bar-fetching core is a free fn in `history.rs`:

```rust
pub struct BarRequest { pub duration: Duration, pub bar_size: BarSize,
                        pub what_to_show: WhatToShow, pub use_rth: bool,
                        pub end: Option<OffsetDateTime> }
pub async fn fetch_bars(client: &Client, contract: &Contract, req: &BarRequest)
    -> Result<HistoricalSeries, IbkrError>;
```

Both `HistoricalDataManager::get_historical_data` and the
`market_data.rs:313` stub (`MarketDataManager::get_historical`) call it — the
stub stops being a TODO without a second implementation.

Managers are constructed once in `mcp/server.rs::start_http_on_with_config`
(shared across sessions → caches/limits survive) and passed through
`IbkrMcpServer::new_shared`. `IbkrMcpServer::new` (stdio/tests) builds its own,
unchanged semantics.

### Tool surface (5 new tools, all read-only)

1. `get_historical_data(symbol, duration="1 Y", bar_size="1 day",
   what_to_show="TRADES", use_rth=true, end_datetime="")`
   - `duration`: IBKR duration string, `"{n} {S|D|W|M|Y}"` (ibapi `Duration::from_str`).
   - `bar_size`: `BarSize::from_str` names — `"1 min"`, `"1 day"`, `"1 hour"`, … .
   - `what_to_show`: `TRADES`, `MIDPOINT`, `BID`, `ASK`, `BID_ASK`, `AGGTRADES`,
     `HISTORICAL_VOLATILITY`, `OPTION_IMPLIED_VOLATILITY`, `ADJUSTED_LAST`.
   - `end_datetime`: `""` (now) | `YYYYMMDD` | RFC3339 (`2026-09-16T00:00:00Z`) →
     `time::OffsetDateTime` (new direct dep `time 0.3`, already in the lock as a
     transitive dep of ibapi — no new download).
   - Bars are **sorted ascending by timestamp** before returning, so "last bar"
     and rank math are order-independent of wire order.
   - Response: `{success, symbol, what_to_show, bar_size, duration, use_rth,
     count, start, end, bars:[{date, open, high, low, close, volume, wap, count}]}`.
   - `volume/wap/count` are only populated for `TRADES` by IBKR; returned as-is.
2. `get_iv_rank(symbol, window="252")`
   - Fetches `1 Y` of `1 day` `OPTION_IMPLIED_VOLATILITY` bars (`use_rth=true`),
     drops non-positive closes, uses the **last `min(window, n)` bars**.
   - `iv_rank_pct = (iv - low)/(high - low) * 100`, `iv_percentile =
     100 * count(v < iv)/n` — the exact formula from the probe that produced
     rank 19.2 / pctile 12.8.
   - Response: `{success, symbol, iv, iv_rank_pct, iv_percentile, iv_52w_low,
     iv_52w_high, bars, window_used, window_requested}`.
   - IBKR caps 1-day bars at `1 Y`, so a window > 252 uses what exists and reports
     `window_used` honestly rather than silently pretending.
3. `run_scanner(scan_code, instrument="STK", location="STK.US.MAJOR",
   rows=25, tag_filters={})`
   - `rows` hard-clamped to `1..=50` (IBKR limit).
   - `tokio::sync::Semaphore(10)` held for the scan's lifetime (IBKR ≤10 concurrent
     scans); a failed acquire returns a clear error instead of queueing forever.
   - `tag_filters` → `Vec<TagValue>` (e.g. `{"underConID": "12345"}` with
     `instrument="NATCOMB"`, `location="NATCOMB.OPT.US"`,
     `scan_code="COMBO_LATEST_TRADE"` for multi-leg trades on one underlying).
   - Subscription is explicitly `.cancel().await`ed after the first data batch
     (subscription-leak discipline).
   - Rows carry `{rank, symbol, contract_id, security_type, currency, exchange,
     local_symbol, leg}` plus an explicit
     `note: "scanner rows carry no market-data fields — fetch quotes separately"`,
     because IBKR does not put bid/ask into scanner rows.
4. `get_events(symbol|conid, start_date="", end_date="")`
   - Resolves `symbol` → conid via `contract_details` (reuses the proven path);
     `conid` alone is accepted.
   - Calls `wsh_metadata()` first as an entitlement probe, then
     `wsh_event_data_by_contract(conid).starting()/.ending()/.fetch()`.
   - Response always carries `entitlement: "ok" | "unavailable"` plus
     `status: "ok" | "no_events" | "not_entitled"` and a human `message` —
     never a silent empty list.
5. `get_historical_ticks(symbol, number_of_ticks=100, tick_type="TRADES",
   start_datetime="", end_datetime="", use_rth=true)`
   - Via `historical_ticks(...).trade()/.mid_point()/.bid_ask(IgnoreSize::No)`,
     `number_of_ticks` clamped `1..=1000`, `TickSubscription::cancel()` on exit.
   - Same explicit `entitlement`/`status` response shape as events; empty →
     `status: "no_data"` with the IBKR reason.

### Compatibility / invariants

- `get_market_data` quote path untouched; only a new `"historical"` arm is added
  (today it returns `Unsupported data_type: historical`, so no byte-compat risk).
- No order/execution capability added. Nothing in `orders.rs` changes.
- Existing tool signatures untouched.
- Single long-lived account-summary pump + shared-manager wiring unchanged.
- Every new subscription gets a `tokio::time::timeout` and an explicit cancel.

## 2. Testing

Unit (no broker):
- duration / bar-size / what-to-show string parsing incl. rejection paths.
- IV rank math on a fixed synthetic series.
- `rows` clamp, tag-filter encoding, date parsing, empty-series handling.

Live e2e (new `tests/live_market_data_v2_test.rs`, `#[ignore]`d, port 4003,
own client id so the running bridge is never disturbed):
- `get_historical_data("WOLF", what_to_show="OPTION_IMPLIED_VOLATILITY")` → ≥200 bars.
- `get_iv_rank("WOLF")` → rank 19.2 ±0.5, pctile 12.8 ±0.5.
- `run_scanner("HOT_BY_OPT_VOLUME")` → ≥5 rows.
- `get_events("WOLF")` → explicit entitlement/status.
- `get_connection_status`, `get_positions`, `get_account_info`, `get_option_quote`
  still work (regression).

## 3. Build & deploy

```
CARGO_TARGET_DIR=/tmp/ibkr-mcp-target cargo build --release
cp /tmp/ibkr-mcp-target/release/ibkr-mcp-rs /usr/local/bin/ibkr-mcp-rs   # needs sudo
systemctl restart ibkr-mcp.service
```

If `sudo` is unavailable: the binary copy is the only privileged step — ask.
`kill -TERM <pid>` (systemd `Restart=always`) restarts the service without sudo.

## 4. Deliverable

Repo root: this plan + `DEPLOY-<date>.md` with the real tool outputs pasted
(bar counts, IV rank, scanner rows, event status, regression outputs,
`systemctl status`).
