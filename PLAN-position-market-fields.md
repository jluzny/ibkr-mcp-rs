# Plan: Populate market_price/market_value/unrealized_pnl/daily_pnl in get_positions

**Date:** 2026-08-21 (updated)  
**Status:** Planning — awaiting Jiri's review before any code changes  
**Goal:** Replace the hardcoded `0.0` values for `marketPrice`, `marketValue`, `unrealizedPnL`, and `dailyPnL` in the `get_positions` MCP tool response with real data from IBKR.

---

## Current State

`get_positions` returns positions with four market-data fields always `0.0`:

```json
{
  "symbol": "AAPL",
  "quantity": 100,
  "averageCost": 150.25,
  "marketPrice": 0.0,
  "marketValue": 0.0,
  "unrealizedPnL": 0.0,
  "dailyPnL": 0.0,
  "securityType": "Stock",
  ...
}
```

### Root Cause (confirmed)

`src/ibkr/account.rs` lines 348-356 — the `Position` struct is built with hardcoded zeros:

```rust
positions.push(Position {
    // ...
    market_price: 0.0,    // <-- hardcoded
    market_value: 0.0,    // <-- hardcoded
    unrealized_pnl: 0.0,  // <-- hardcoded
    daily_pnl: 0.0,       // <-- hardcoded
    // ...
});
```

**Why:** The `ibapi` crate's `PositionUpdate::Position` struct only provides:
- `account: String`
- `contract: Contract` (symbol, security_type, strike, right, expiry, multiplier, **contract_id**)
- `position: f64` (quantity)
- `average_cost: f64`

It does **not** include market price, market value, or PnL fields. The TWS positions stream (`reqPositions`) only sends these four fields per position. Market data must come from a separate subscription.

---

## REVISED RECOMMENDATION: Option B — `pnl_single()` per position (PRIMARY PATH)

**The previous version of this plan recommended Option A (`account_updates()`) as Phase 1, leaving `daily_pnl` at 0.0. That recommendation is now superseded.**

Jiri's Go project (`options-trading-agent-go`) already solved this exact problem using **Option B** (`ReqPnLSingle` per position) and it has been running in production. The "rate limit risk" concern that pushed us toward Option A is overblown for a ~16-position account — the Go project proves it works fine at this scale.

### Why Option B over Option A

| Criterion | Option A (`account_updates`) | Option B (`pnl_single`) |
|-----------|-----|-----|
| `daily_pnl` | Not available | Provided directly |
| `unrealized_pnl` | Yes | Yes |
| `market_value` | Yes (`market_value` field) | Yes (`value` field) |
| `market_price` | Yes (direct) | Yes (computed: `value / position`) |
| `realized_pnl` | Yes | Yes |
| Subscriptions needed | 1 broad subscription | N (one per position) |
| Rate limit risk | Low | **Proven safe for ~16 positions** |
| Proven in production? | No | **Yes — Go project runs this** |

Option B provides **all four missing fields** (including `daily_pnl`, which Option A cannot provide) in a single call per position, and the Go project has proven it scales fine for this account size.

---

## ibapi v3.3.0 API (confirmed from crate source)

### `PnLSingle` struct (ibapi v3.3.0, `ibapi::accounts::PnLSingle`)

```rust
pub struct PnLSingle {
    pub position: f64,       // Current size of the position
    pub daily_pnl: f64,      // Daily PnL for the position
    pub unrealized_pnl: f64, // Total unrealized PnL (since inception)
    pub realized_pnl: f64,   // Realized PnL for the position
    pub value: f64,          // Current market value of the position
}
```

### `pnl_single()` method signature (ibapi v3.3.0 async)

```rust
impl Client {
    pub async fn pnl_single(
        &self,
        account: &AccountId,
        contract_id: ContractId,
        model_code: Option<&ModelCode>,
    ) -> Result<Subscription<PnLSingle>, Error>
}
```

### Required type imports

```rust
use ibapi::accounts::PnLSingle;
use ibapi::accounts::types::{AccountId, ContractId};
```

- `AccountId(pub String)` — construct via `AccountId("U1234567".to_string())` or `AccountId::from("U1234567")`
- `ContractId(pub i32)` — construct via `ContractId(contract.contract_id)` (Contract has `contract_id: i32` field)
- The subscription returns `Subscription<PnLSingle>` — use `subscription.next().await` to get the first update, then cancel.

---

## Go Reference Implementation (the proof)

Source: `/data/dev/trading/options-trading-agent-go/internal/broker/ibkr_tws_wrapper.go`

### Flow: PositionEnd -> ReqPnLSingle per position -> PnlSingle callback

1. **`Position()` callback** (line 259): Builds position structs, stores them in `w.positions` slice, and maps `positionKey{AccountCode, ConID} -> index` in `w.positionMap`.

2. **`PositionEnd()` callback** (line 411): After all positions are received:
   - Creates `pnlDoneCh` channel for completion signaling
   - For each position, calls `findContractIDForIndex(i)` to get the ConID
   - Generates a unique `reqId` and maps `pnlReqMap[reqId] = i` (position index)
   - Calls `w.broker.client.ReqPnLSingle(reqId, account, "", conId)` for EACH position
   - Sets `pendingPnlCallbacks = pnlRequestCount`
   - If no PnL requests were made, closes `pnlDoneCh` immediately

3. **`PnlSingle()` callback** (line 1136): For each PnL response:
   - Filters IBKR sentinel values (cap at +/-1e11)
   - Maps `reqId -> posIndex` via `pnlReqMap`
   - Computes `markPrice = value / quantity` (then `/100` for options)
   - Sets `DailyPnL`, `UnrealizedPnL`, `RealizedPnL`, `MarkPrice`, `DayPnLReady = true`
   - Handles closed positions (qty=0): sets `DailyPnLPct = 0.0`
   - Decrements `pendingPnlCallbacks`; when it hits 0, closes `pnlDoneCh`

4. **`findContractIDForIndex()`** (line 482): Searches `positionMap` by index value, validates account matches (prevents cross-account ConID mismatch).

5. **`GetPositions()` caller** (line 769): The Go project originally waited on `pnlDoneCh` but later **decoupled** it — positions are returned immediately and PnL updates stream asynchronously via `posCallback`. For our Rust MCP server (request/response, not streaming UI), we should **wait** for all PnL callbacks with a bounded timeout.

### Go Position struct (types/models.go line 164)

```go
type Position struct {
    // ...
    CurrentPrice   *float64  `json:"currentPrice"`
    UnrealizedPnL  float64   `json:"unrealizedPnL"`
    RealizedPnL    float64   `json:"realizedPnL"`
    DailyPnL       float64   `json:"dayPnL"`
    DailyPnLPct    float64   `json:"dayPnLPct"`
    DayPnLReady    bool      `json:"dayPnLReady" gorm:"-"`
    MarkPrice      float64   `json:"markPrice"`
    // ...
}
```

---

## Implementation Plan (Rust)

### Data flow

1. Call `client.positions()` and collect positions as before (with `contract_id` captured)
2. After `PositionEnd`, determine the account ID from the first position
3. For each position, call `client.pnl_single(&AccountId, ContractId(contract_id), None)`
4. Read the first `PnLSingle` update from each subscription (with timeout)
5. Populate `market_price`, `market_value`, `unrealized_pnl`, `daily_pnl` from `PnLSingle`
6. Cancel each subscription

### Code changes

**File: `src/ibkr/account.rs`**

1. Add imports:
```rust
use ibapi::accounts::PnLSingle;
use ibapi::accounts::types::{AccountId, ContractId};
```

2. **Add `contract_id: i32` field to the `Position` struct** (line 32):
```rust
pub struct Position {
    pub account_id: String,
    pub symbol: String,
    pub quantity: f64,
    pub average_cost: f64,
    pub contract_id: i32,       // <-- NEW: needed for pnl_single
    pub market_price: f64,
    pub market_value: f64,
    pub unrealized_pnl: f64,
    pub daily_pnl: f64,
    pub security_type: String,
    pub strike: Option<f64>,
    pub right: Option<String>,
    pub expiration: Option<String>,
    pub multiplier: Option<String>,
}
```

And populate it in the position collection loop (line 348):
```rust
positions.push(Position {
    // ...
    contract_id: pos.contract.contract_id,  // <-- NEW
    // ...
});
```

3. Modify `get_positions()` — after collecting positions (line 387, before `subscription.cancel().await`), add a PnL enrichment step:

```rust
// --- PnL Enrichment (Option B: pnl_single per position) ---
// Proven approach from Go project: options-trading-agent-go
if !positions.is_empty() {
    // Determine account ID from first position
    let account_id = positions[0].account_id.clone();
    let ibkr_account = AccountId(account_id);

    // Enrich each position with PnL data
    for pos in &mut positions {
        let contract_id = ContractId(pos.contract_id);

        match client.pnl_single(&ibkr_account, contract_id, None).await {
            Ok(mut pnl_sub) => {
                match timeout(Duration::from_secs(3), pnl_sub.next()).await {
                    Ok(Some(Ok(pnl))) => {
                        // Filter sentinel values (pitfall #1)
                        let value = filter_sentinel(pnl.value);
                        let daily_pnl = filter_sentinel(pnl.daily_pnl);
                        let unrealized_pnl = filter_sentinel(pnl.unrealized_pnl);

                        // Compute mark price (pitfall #2: options multiplier)
                        let mark_price = if pnl.position != 0.0 {
                            let raw = value / pnl.position;
                            // Options: divide by 100 (standard multiplier)
                            if pos.security_type == "OPT" || pos.security_type == "FOP" {
                                raw / 100.0
                            } else {
                                raw
                            }
                        } else {
                            0.0
                        };

                        pos.market_price = mark_price;
                        pos.market_value = value;
                        pos.unrealized_pnl = unrealized_pnl;
                        pos.daily_pnl = daily_pnl;
                    }
                    _ => {
                        // Timeout or error -- leave as 0.0 (degraded)
                        warn!("pnl_single timeout for {}", pos.symbol);
                    }
                }
                pnl_sub.cancel().await;
            }
            Err(e) => {
                warn!("pnl_single failed for {}: {e}", pos.symbol);
            }
        }
    }
}
```

4. Add a sentinel filter helper:
```rust
/// Filter IBKR sentinel/garbage values (MaxFloat64 ~1.79e11).
/// Cap at +/-1e11 -- anything beyond is garbage data from IBKR.
fn filter_sentinel(v: f64) -> f64 {
    const MAX_REASONABLE: f64 = 1e11;
    if v > MAX_REASONABLE || v < -MAX_REASONABLE || v.is_nan() || v.is_infinite() {
        0.0
    } else {
        v
    }
}
```

**File: `src/mcp/tools.rs`** — No changes needed. The JSON response already includes `marketPrice`, `marketValue`, `unrealizedPnL`, `dailyPnL`. Optionally add `contractId` to the JSON if useful for the consumer.

### Caching consideration

For the initial implementation, use a **snapshot approach** (open -> read first update -> cancel per position). The margin monitor cron calls every ~15 min, so per-call is fine.

**Risk**: The `cancel()` no-op issue (same as account_summary). If PnL subscriptions leak, we may hit subscription limits. However, `pnl_single` subscriptions are per-contract (not global like account_summary's 3-slot cap), so the risk is lower. If it proves problematic, we can convert to long-lived subscriptions that stream updates.

### Timeout

Wrap each `pnl_single` read in a 3s timeout (IBKR responds in 1-3s for pnl_single). If it times out, leave that position's market fields at 0.0 (degraded but functional). Total worst-case for 16 positions: 16 x 3s = 48s -- but in practice they respond in parallel (different subscriptions), so it should be ~3-5s total.

### Concurrency optimization (optional, Phase 2)

Instead of sequential `pnl_single` calls, open all subscriptions concurrently using `tokio::spawn` + `JoinSet`, then collect results. This reduces total latency from Nx3s to ~3s:

```rust
use tokio::task::JoinSet;

let mut tasks = JoinSet::new();
for (i, pos) in positions.iter().enumerate() {
    let client = client.clone(); // Client is Arc internally
    let account = AccountId(pos.account_id.clone());
    let contract_id = ContractId(pos.contract_id);
    tasks.spawn(async move {
        let result = client.pnl_single(&account, contract_id, None).await;
        (i, result)
    });
}
while let Some(res) = tasks.join_next().await {
    // populate positions[i] from result
}
```

Note: `ibapi::Client` is `Arc`-based internally, so cloning is cheap. But we need to verify the `client` reference in `AccountManager` can be used this way -- it's obtained via `self.client.get_client().await?` which returns a guard or reference. May need to restructure to hold a clone.

---

## CRITICAL Pitfalls (from Go project -- must carry into Rust)

### 1. IBKR Sentinel Values

IBKR sends `MaxFloat64` / ~1.79e11 as "unset" or "infinity" markers. These must be filtered to avoid garbage in the response:

```rust
const MAX_REASONABLE: f64 = 1e11; // $100 Billion
if v > MAX_REASONABLE || v < -MAX_REASONABLE || v.is_nan() || v.is_infinite() {
    v = 0.0;
}
```

Apply to `daily_pnl`, `unrealized_pnl`, `realized_pnl`, AND `value`.

**Go source**: `ibkr_tws_wrapper.go` line 1154-1167.

### 2. Options Multiplier

`PnLSingle.value` is the **total market value** (not unit price). To get `mark_price`:
- Stocks: `mark_price = value / quantity`
- Options: `mark_price = value / quantity / 100` (standard options multiplier)

The Rust `Position` struct already has `multiplier: Option<String>` -- we could use it for non-standard multipliers, but for now the Go project just hardcodes `/100` for options.

**Go source**: `ibkr_tws_wrapper.go` line 1178-1188.

### 3. Closed Positions (qty=0)

When `position == 0` (closed today), `daily_pnl` percentage is misleading (dividing by zero/negative baseline). The Go project sets `DailyPnLPct = 0.0` for closed positions. For the Rust MCP server, we should keep `daily_pnl` from PnLSingle (it represents today's realized P&L for the closed position), but NOT compute any percentage from it. The current Rust Position struct doesn't have a `daily_pnl_pct` field, so this is a non-issue -- just ensure we don't divide by zero when computing `mark_price`.

**Go source**: `ibkr_tws_wrapper.go` line 1202-1207.

### 4. Wait for All PnlSingle Callbacks

The Go project originally had a bug where positions were emitted immediately at `PositionEnd()` -- before PnL callbacks arrived -- causing the UI to show $0.00 for Mark/PnL. The fix was to wait on `pnlDoneCh` until all `PnlSingle` callbacks completed.

For Rust: we must wait for all `pnl_single` subscriptions to deliver their first update (or timeout) before returning the positions vector. The sequential approach naturally does this. The concurrent approach needs `JoinSet::join_all()`.

**Go source**: `ibkr_tws_wrapper.go` lines 471-478 (commented-out old code) and 800-821.

### 5. ConID Matching by Index, NOT Symbol

The Go project uses `positionMap[accountKey{account, conId}] = positionIndex` and `pnlReqMap[reqId] = positionIndex` to map PnL responses back to positions. This is critical because:
- A stock and an option can share the same symbol (e.g. AAPL stock + AAPL call)
- Matching by symbol would assign PnL to the wrong position
- The `contract_id` (ConID) is the unique identifier

For Rust: since we're calling `pnl_single` sequentially (or with index-keyed tasks), the mapping is implicit -- we call `pnl_single` with the position's own `contract_id` and update that same position. No map needed. But if we use the concurrent approach, the `JoinSet` returns `(index, result)` tuples to maintain the mapping.

**Go source**: `ibkr_tws_wrapper.go` lines 428-436 (PositionEnd), 1170 (PnlSingle), 482-498 (findContractIDForIndex).

---

## Account ID Resolution

`pnl_single()` requires a specific `AccountId`, not "All". Resolution strategy:

1. Use the account ID from the first position in the positions stream (`positions[0].account_id`)
2. All positions in a single-account deployment share the same account ID
3. If positions is empty, return empty vec (no PnL needed)
4. If the caller passes an `account_id` filter, use that instead

---

## Files to Modify

| File | Change |
|------|--------|
| `src/ibkr/account.rs` | Add `contract_id` to `Position` struct; add `filter_sentinel()` helper; add PnL enrichment step in `get_positions()`; add imports for `PnLSingle`, `AccountId`, `ContractId` |
| `src/mcp/tools.rs` | No changes needed -- JSON response already includes the fields. Optionally add `contractId` to JSON. |

---

## Testing

### Unit Tests (run in normal CI, no gateway needed)

**Location**: `src/ibkr/account_tests.rs` (inline `#[cfg(test)]` module, already exists).

1. **`filter_sentinel()` tests** — verify the helper zeros out garbage and passes through valid values:
   - `filter_sentinel(f64::MAX) == 0.0`
   - `filter_sentinel(1.79e11) == 0.0` (IBKR's typical sentinel)
   - `filter_sentinel(-1.79e11) == 0.0`
   - `filter_sentinel(f64::NAN) == 0.0`
   - `filter_sentinel(f64::INFINITY) == 0.0`
   - `filter_sentinel(f64::NEG_INFINITY) == 0.0`
   - `filter_sentinel(150.25) == 150.25` (normal positive)
   - `filter_sentinel(-5000.00) == -5000.00` (normal negative, e.g. unrealized PnL)
   - `filter_sentinel(0.0) == 0.0`

2. **Options multiplier computation tests** — verify `mark_price = value / position / 100` for options and `value / position` for stocks:
   - Stock: value=10927.0, qty=700.0 -> mark_price=15.61
   - Option: value=848.75, qty=-1.0, security_type="OPT" -> mark_price=8.4875 (848.75 / 1 / 100)
   - Zero-qty position: value=0.0, qty=0.0 -> mark_price=0.0 (no division by zero)

3. **JSON serialization tests** (extend existing `position_json_test.rs`) — verify the new `contractId` field (if added to JSON) and that market fields serialize correctly as non-zero values.

**Run**: `cargo test --lib` (unit tests only, no gateway needed)

### E2E / Integration Tests (gated behind LIVE_TEST=true)

**Pattern**: Follows the Go project's `position_update_live_test.go` convention — live IBKR e2e tests gated behind `LIVE_TEST=true` env var, so they are skipped in normal CI but run on-demand against a real gateway.

**Location**: `tests/live_positions_test.rs` (new integration test file in the `tests/` directory, following the existing `live_ibkr_test.rs` and `what_if_test.rs` pattern).

**Gating**: Each test uses `#[ignore = "Requires live IB Gateway; set LIVE_TEST=true"]` (consistent with existing `what_if_test.rs` pattern). Run with: `LIVE_TEST=true cargo test --test live_positions_test -- --ignored --nocapture`

The `#[ignore]` attribute is the Rust-idiomatic equivalent of the Go project's `if os.Getenv("LIVE_TEST") != "true" { t.Skip(...) }`. Both approaches ensure the tests are skipped in normal CI and only run when explicitly requested. The `LIVE_TEST=true` env var serves as an additional runtime guard — even if `--ignored` tests are invoked without it, they can early-return or skip assertions.

**Connection setup** (shared helper, follows `what_if_test.rs` `connect_to_ibkr()` pattern):
```rust
// Reads IBKR_PORT env var (default 4003 for live, 4004 for paper)
// Returns Arc<IbkrClient> after verifying connection within 30s
```

#### E2E Test Cases

**Test 1: `test_live_get_positions_returns_nonzero_market_fields`**

Goal: Verify that `get_positions()` returns non-zero market data for positions with live data.

Steps:
1. Connect to IB Gateway (port from `IBKR_PORT` env var, default 4003).
2. Create `AccountManager::new_shared(Arc::clone(&client))`.
3. Call `get_positions()`.
4. Assert the result is `Ok(positions)` and `!positions.is_empty()`.
5. For each position where `quantity != 0.0`:
   - Assert `market_price != 0.0` (skip if markets are closed — log a warning, don't fail).
   - Assert `market_value != 0.0`.
   - Assert `unrealized_pnl != 0.0` (this is cumulative; should be non-zero for any position held overnight).
   - Assert `daily_pnl` is present (may be 0.0 if markets just opened or it's a new position — log but don't hard-fail if 0.0).
6. Log all positions with their market fields for diagnostic output.
7. Assert no position has `market_price > 1e11` or `market_value > 1e11` (sentinel filter works).

**Test 2: `test_live_get_positions_sentinel_filter`**

Goal: Verify the sentinel-value filter prevents garbage IBKR values from leaking through.

Steps:
1. Connect to IB Gateway.
2. Call `get_positions()`.
3. For every position in the result:
   - Assert `market_price.abs() < 1e11` (not a sentinel).
   - Assert `market_value.abs() < 1e11`.
   - Assert `unrealized_pnl.abs() < 1e11`.
   - Assert `daily_pnl.abs() < 1e11`.
   - Assert none of the fields are `NaN` or `Infinity` (use `is_finite()`).
4. This test passes even with zero positions (sentinel filter is about preventing garbage, not requiring data).

**Test 3: `test_live_get_positions_options_multiplier`**

Goal: Verify options get the `/100` multiplier applied to mark_price.

Steps:
1. Connect to IB Gateway.
2. Call `get_positions()`.
3. Filter positions where `security_type == "OPT"` or `security_type == "FOP"`.
4. If no option positions exist, log "No option positions — skipping" and return (not a failure).
5. For each option position where `quantity != 0.0` and `market_value != 0.0`:
   - Compute `expected_mark_price = market_value / quantity / 100.0`.
   - Assert `market_price` is approximately equal to `expected_mark_price` (within tolerance, e.g. `abs(market_price - expected) < 0.01` or a percentage tolerance like 1%).
   - This verifies the `/100` division was applied for options.
6. For stock positions (control group): compute `expected = market_value / quantity` and verify `market_price` matches (no `/100`).

**Test 4: `test_live_get_positions_degraded_mode`**

Goal: Verify that if `pnl_single` fails or times out for a position, the response still includes that position with zeros (not an error, not a missing position).

Steps:
1. Connect to IB Gateway.
2. Call `get_positions()`.
3. Assert the result is `Ok` (not `Err`) — even if some PnL fetches timed out internally.
4. Assert the number of positions matches the raw positions count (no positions dropped due to PnL failure).
5. Positions that failed PnL enrichment should have `market_price == 0.0`, `market_value == 0.0`, etc. — verify they're present and zeroed, not absent.

**Test 5: `test_live_get_positions_closed_positions`**

Goal: Verify closed positions (qty=0) are handled correctly.

Steps:
1. Connect to IB Gateway.
2. Call `get_positions()`.
3. Filter positions where `quantity == 0.0`.
4. If none exist, log "No closed positions — skipping" and return.
5. For closed positions:
   - Assert `market_price == 0.0` (no division by zero in mark_price computation).
   - Assert `market_value == 0.0` or a small residual.
   - `daily_pnl` may be non-zero (realized PnL for the day) — don't assert it's zero.
   - Assert the position is present in the response (not dropped).

**Test 6 (optional): `test_live_get_positions_mcp_http`**

Goal: Verify the full MCP HTTP endpoint returns market data (end-to-end through the MCP protocol, not just the Rust API).

Steps:
1. Connect to IB Gateway.
2. Start the MCP HTTP server on a random port (follows `live_ibkr_test.rs::test_live_mcp_server_with_ibkr` pattern).
3. Send an MCP `tools/call` request for `get_positions`:
   ```json
   {
     "jsonrpc": "2.0",
     "id": 1,
     "method": "tools/call",
     "params": {
       "name": "get_positions",
       "arguments": {}
     }
   }
   ```
4. Parse the JSON response.
5. Assert the response contains a `positions` array with non-zero market fields for open positions.
6. Assert no sentinel values in any market field.
7. This tests the full MCP JSON serialization path, not just the Rust struct.

### How to Run

**Unit tests (CI, no gateway)**:
```bash
cd /data/dev/trading/ibkr-mcp-rs
cargo test --lib
```

**E2E tests (live gateway required)**:
```bash
# Ensure IB Gateway is running (check: timeout 3 bash -c 'echo > /dev/tcp/127.0.0.1/4003' && echo "OPEN" || echo "CLOSED")
# Live trading:
LIVE_TEST=true IBKR_PORT=4003 cargo test --test live_positions_test -- --ignored --nocapture

# Paper trading:
LIVE_TEST=true IBKR_PORT=4004 cargo test --test live_positions_test -- --ignored --nocapture

# Single test:
LIVE_TEST=true cargo test --test live_positions_test test_live_get_positions_sentinel_filter -- --ignored --nocapture
```

**All tests**:
```bash
cargo test -- --ignored --nocapture  # includes all live/integration tests
cargo test                            # unit + non-ignored integration only
```

### CI Behavior

- `cargo test` (default): runs unit tests only. E2E tests are `#[ignore]`-gated and skipped.
- `cargo test -- --ignored`: runs only the ignored (live) tests. Requires `LIVE_TEST=true` env var (or they early-return).
- No `LIVE_TEST=true` in CI -> all e2e tests skip -> CI passes without a gateway.
- This exactly mirrors the Go project's `LIVE_TEST=true` gating convention.

### Test File Layout

```
tests/
  live_ibkr_test.rs         # existing: market data quote + MCP server smoke
  live_positions_test.rs    # NEW: e2e tests for get_positions PnL enrichment
  mcp_server_test.rs         # existing: MCP server unit tests
  position_json_test.rs     # existing: JSON serialization tests (extend with contractId)
  what_if_test.rs            # existing: what-if order live tests
src/ibkr/
  account.rs                 # main source
  account_tests.rs           # inline unit tests (extend with filter_sentinel + multiplier tests)
```

### Notes

- E2E tests depend on the account having open positions. If the account has no positions, tests that assert non-zero market fields will log a warning and pass (not fail) — consistent with the Go project's approach of not hard-failing when live data is unavailable.
- Tests should use `--nocapture` to see diagnostic `println!` output (position details, warnings, etc.).
- The `connect_to_ibkr()` helper should be shared across test functions (extracted as a module-level async fn, same as `what_if_test.rs`).
- Timeout for `get_positions()` call: wrap in `tokio::time::timeout(Duration::from_secs(60), ...)` — PnL enrichment for 16 positions with 3s per-position timeout could take up to 48s sequential (concurrent: ~5s). 60s is a safe upper bound.

---

## Comparison: Old Plan vs New Plan

| Aspect | Old Plan (Option A) | New Plan (Option B) |
|--------|---------------------|---------------------|
| Primary API | `account_updates()` | `pnl_single()` per position |
| `daily_pnl` | Left at 0.0 | Populated |
| `market_price` | Direct | Computed from `value/position` |
| `market_value` | Direct | From `value` |
| `unrealized_pnl` | Direct | Direct |
| Subscriptions | 1 broad | N per position (~16) |
| Rate limit risk | Low | **Proven safe (Go project)** |
| Proven? | No | **Yes -- production Go project** |
| Complexity | Medium (broad subscription filtering) | Medium (N subscriptions, but simple per-position logic) |
| Pitfalls documented | 1 (cancel no-op) | 5 (sentinels, multiplier, closed, wait, ConID) |

---

## Open Questions for Jiri

1. **Sequential vs concurrent `pnl_single`**: Sequential is simpler but takes Nx3s worst case. Concurrent via `JoinSet` is faster (~3s total) but more complex. The Go project uses sequential callbacks. Recommendation: start sequential, optimize to concurrent if latency is an issue.

2. **Options multiplier**: Hardcode `/100` (like Go) or use the `multiplier` field from the `Position` struct? The Go project hardcodes 100. If we have non-standard multipliers (futures options with different multipliers), we'd need the field value. For now, hardcode 100 and note the TODO.

3. **Long-lived vs snapshot**: Should we keep `pnl_single` subscriptions alive (streaming updates like the account_summary pump) or do snapshot (open -> read -> cancel)? Snapshot is simpler. The margin monitor cron calls every ~15 min, so snapshot is fine. If we see subscription leaks, convert to long-lived.

4. **`contract_id` in JSON response**: Should we expose `contractId` in the `get_positions` JSON response? It's useful for the consumer (margin monitor) to call `pnl_single` or other contract-specific APIs. Currently it's internal only.