# Plan: Add Option Contract Fields to `get_positions`

**Date:** 2026-05-17
**Status:** Planning
**Goal:** Return strike, expiry, put/call, multiplier for option positions so the MCP client never needs OCC probing, CSV lookups, or session search to identify option contracts.

---

## Current State

`get_positions` returns this for every position (stocks AND options):

```json
{
  "symbol": "IBIT",
  "quantity": -1,
  "averageCost": 254.29,
  "marketPrice": 0.0,
  "marketValue": 0.0,
  "unrealizedPnL": 0.0,
  "dailyPnL": 0.0
}
```

No way to tell if `-1` is a short put, short call, or a fractional share. No strike, no expiry, no right, no multiplier.

### Pre-existing bugs

| Bug | Detail |
|-----|--------|
| `market_price` = 0.0 always | Hardcoded in `account.rs:176`. Never populated from stream. |
| `market_value` = 0.0 always | Same. |
| `unrealized_pnl` = 0.0 always | Same. |
| `daily_pnl` = 0.0 always | Same. |

These are misleading — the JSON claims the field exists but always returns zero. Either populate them or remove them.

---

## Target State

`get_positions` returns (for an option):

```json
{
  "symbol": "IBIT",
  "quantity": -1,
  "averageCost": 254.29,
  "marketPrice": 0.0,
  "marketValue": 0.0,
  "unrealizedPnL": 0.0,
  "dailyPnL": 0.0,
  "securityType": "OPT",
  "strike": 45.0,
  "right": "P",
  "expiration": "20260522",
  "multiplier": "100"
}
```

For stock positions, option fields are `null`:

```json
{
  "symbol": "SOFI",
  "quantity": 700,
  "averageCost": 25.21,
  "marketPrice": 0.0,
  "marketValue": 0.0,
  "unrealizedPnL": 0.0,
  "dailyPnL": 0.0,
  "securityType": "STK",
  "strike": null,
  "right": null,
  "expiration": null,
  "multiplier": null
}
```

---

## Files to Change

| File | Change |
|------|--------|
| `src/ibkr/account.rs` | Position struct: add 5 fields. `get_positions()`: populate from `pos.contract.*` |
| `src/mcp/tools.rs` | JSON output: add 5 fields to serde_json mapping |
| `tests/live_ibkr_test.rs` | Add test: verify option fields populated for live option positions |
| `tests/mcp_server_test.rs` | Add test: verify JSON output includes new fields |

---

## Phase 1 — Verify ibapi Field Names ✅ COMPLETE

Cloned `rust-ibapi` from `https://github.com/wboayue/rust-ibapi` (branch `main`).

### Confirmed ibapi types and field names

**`Contract` struct** (`src/contracts/mod.rs:177`):

| Field | Type | Notes |
|-------|------|-------|
| `security_type` | `SecurityType` (enum) | Has `Display` impl → `"STK"`, `"OPT"`, etc. |
| `strike` | `f64` | Always present, `0.0` for non-options |
| `right` | `Option<OptionRight>` | `None` for stocks, `Some(Call)` or `Some(Put)` for options |
| `last_trade_date_or_contract_month` | `String` | `"YYYYMMDD"` or `"YYYYMM"` format |
| `multiplier` | `String` | `"100"` for equity options, `""` for stocks |

**`SecurityType` enum** (`src/contracts/mod.rs:47`):
- `SecurityType::Stock` → Display = `"STK"`
- `SecurityType::Option` → Display = `"OPT"`
- Implements `Display` via `to_string()` ✅
- Supports `PartialEq` for comparison ✅

**`OptionRight` enum** (`src/contracts/types.rs:188`):
- `OptionRight::Call` → `as_str()` = `"C"`
- `OptionRight::Put` → `as_str()` = `"P"`
- No `Default` — carried as `Option<OptionRight>` on Contract

**`Position` struct from ibapi** (`src/accounts/mod.rs:187`):
```rust
pub struct Position {
    pub account: String,
    pub contract: Contract,   // ← FULL contract with all fields above
    pub position: f64,
    pub average_cost: f64,
}
```

**Key finding:** `pos.contract` in the positions stream contains the **full** `Contract` struct — strike, right, expiry, multiplier, security_type are all available. The current `ibkr-mcp-rs` code just extracts `pos.contract.symbol` and discards everything else.

**Bonus finding:** `AccountPortfolioValue` (`src/accounts/mod.rs`) has `market_price`, `market_value`, `unrealized_pnl`, `realized_pnl` — but this is a different stream (`account_updates`), not `positions()`. The positions stream only returns `account`, `contract`, `position`, `average_cost`.

### Remaining unknown: does TWS populate strike/expiry in positions stream?

The ibapi `Position` struct embeds a full `Contract`, so the data path exists. TWS historically populates contract details in position messages (verified by IBKR API docs). **Step 1.4 (live smoke test) recommended before Phase 2.**

---

## Phase 2 — Implementation

### Step 2.1: Edit `src/ibkr/account.rs` — Position struct

Current (line 24-32):

```rust
pub struct Position {
    pub account_id: String,
    pub symbol: String,
    pub quantity: f64,
    pub average_cost: f64,
    pub market_price: f64,
    pub market_value: f64,
    pub unrealized_pnl: f64,
    pub daily_pnl: f64,
}
```

New:

```rust
pub struct Position {
    pub account_id: String,
    pub symbol: String,
    pub quantity: f64,
    pub average_cost: f64,
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

### Step 2.2: Edit `src/ibkr/account.rs` — get_positions() population

Current (line 166-177):

```rust
positions.push(Position {
    account_id: pos.account.clone(),
    symbol: pos.contract.symbol.to_string(),
    quantity: pos.position,
    average_cost: pos.average_cost,
    market_price: 0.0,
    market_value: 0.0,
    unrealized_pnl: 0.0,
    daily_pnl: 0.0,
});
```

New (exact field names from Phase 1):

```rust
let is_option = pos.contract.security_type == SecurityType::Option;

positions.push(Position {
    account_id: pos.account.clone(),
    symbol: pos.contract.symbol.to_string(),
    quantity: pos.position,
    average_cost: pos.average_cost,
    market_price: 0.0,
    market_value: 0.0,
    unrealized_pnl: 0.0,
    daily_pnl: 0.0,
    security_type: pos.contract.security_type.to_string(),
    strike: if is_option { Some(pos.contract.strike) } else { None },
    right: pos.contract.right.as_ref().map(|r| r.as_str().to_string()),
    expiration: if is_option && !pos.contract.last_trade_date_or_contract_month.is_empty() {
        Some(pos.contract.last_trade_date_or_contract_month.clone())
    } else {
        None
    },
    multiplier: if is_option && !pos.contract.multiplier.is_empty() {
        Some(pos.contract.multiplier.clone())
    } else {
        None
    },
});
```

**Notes:**
- `security_type.to_string()` works because `SecurityType` implements `Display` → `"STK"`, `"OPT"`, etc.
- `strike` is `f64` on Contract, always populated (0.0 for stocks). We gate on `is_option` to return `None` for stocks.
- `right` is `Option<OptionRight>` — `None` for stocks, `Some(Call)` or `Some(Put)` for options. We convert via `.as_ref().map(|r| r.as_str().to_string())` → `None`, `Some("C")`, or `Some("P")`.
- `last_trade_date_or_contract_month` is `String` — can be empty. We check `is_empty()` as well.
- `multiplier` is `String` — empty for stocks, `"100"` for equity options. We check `is_empty()`.
- Requires `use ibapi::prelude::*;` already in scope (confirmed in contract.rs).

### Step 2.3: Edit `src/mcp/tools.rs` — JSON serialization

Current (line 267-274):

```rust
"symbol": p.symbol,
"quantity": p.quantity,
"averageCost": p.average_cost,
"marketPrice": p.market_price,
"marketValue": p.market_value,
"unrealizedPnL": p.unrealized_pnl,
"dailyPnL": p.daily_pnl,
```

New:

```rust
"symbol": p.symbol,
"quantity": p.quantity,
"averageCost": p.average_cost,
"marketPrice": p.market_price,
"marketValue": p.market_value,
"unrealizedPnL": p.unrealized_pnl,
"dailyPnL": p.daily_pnl,
"securityType": p.security_type,
"strike": p.strike,
"right": p.right,
"expiration": p.expiration,
"multiplier": p.multiplier,
```

---

## Phase 3 — Tests

### Step 3.1: Unit test — Position struct construction

File: `src/ibkr/account.rs` (add `#[cfg(test)] mod tests` if not present)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_stock_has_null_option_fields() {
        let pos = Position {
            account_id: "U18197748".into(),
            symbol: "SOFI".into(),
            quantity: 700.0,
            average_cost: 25.21,
            market_price: 15.61,
            market_value: 10927.0,
            unrealized_pnl: -6720.0,
            daily_pnl: 0.0,
            security_type: "STK".into(),
            strike: None,
            right: None,
            expiration: None,
            multiplier: None,
        };
        assert_eq!(pos.security_type, "STK");
        assert!(pos.strike.is_none());
        assert!(pos.right.is_none());
        assert!(pos.expiration.is_none());
        assert!(pos.multiplier.is_none());
    }

    #[test]
    fn position_option_has_populated_fields() {
        let pos = Position {
            account_id: "U18197748".into(),
            symbol: "IBIT".into(),
            quantity: -1.0,
            average_cost: 254.29,
            market_price: 0.0,
            market_value: 0.0,
            unrealized_pnl: 0.0,
            daily_pnl: 0.0,
            security_type: "OPT".into(),
            strike: Some(45.0),
            right: Some("P".into()),
            expiration: Some("20260522".into()),
            multiplier: Some("100".into()),
        };
        assert_eq!(pos.security_type, "OPT");
        assert_eq!(pos.strike, Some(45.0));
        assert_eq!(pos.right.as_deref(), Some("P"));
        assert_eq!(pos.expiration.as_deref(), Some("20260522"));
        assert_eq!(pos.multiplier.as_deref(), Some("100"));
    }
}
```

### Step 3.2: Integration test — JSON output includes option fields

File: `tests/mcp_server_test.rs`

```rust
#[tokio::test]
async fn get_positions_json_includes_option_fields() {
    // Build a Position with option fields, serialize to JSON,
    // verify all new fields appear in output
    let position = Position {
        account_id: "U18197748".into(),
        symbol: "BTDR".into(),
        quantity: -2.0,
        average_cost: 476.30,
        market_price: 0.0,
        market_value: 0.0,
        unrealized_pnl: 0.0,
        daily_pnl: 0.0,
        security_type: "OPT".into(),
        strike: Some(40.0),
        right: Some("C".into()),
        expiration: Some("20260618".into()),
        multiplier: Some("100".into()),
    };

    let json = serde_json::to_string(&position).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["securityType"], "OPT");
    assert_eq!(parsed["strike"], 40.0);
    assert_eq!(parsed["right"], "C");
    assert_eq!(parsed["expiration"], "20260618");
    assert_eq!(parsed["multiplier"], "100");
}

#[tokio::test]
async fn get_positions_json_stock_has_null_option_fields() {
    let position = Position {
        account_id: "U18197748".into(),
        symbol: "SOFI".into(),
        quantity: 700.0,
        average_cost: 25.21,
        market_price: 15.61,
        market_value: 10927.0,
        unrealized_pnl: -6720.0,
        daily_pnl: 0.0,
        security_type: "STK".into(),
        strike: None,
        right: None,
        expiration: None,
        multiplier: None,
    };

    let json = serde_json::to_string(&position).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["securityType"], "STK");
    assert!(parsed["strike"].is_null());
    assert!(parsed["right"].is_null());
    assert!(parsed["expiration"].is_null());
    assert!(parsed["multiplier"].is_null());
}
```

### Step 3.3: Live integration test

File: `tests/live_ibkr_test.rs`

```rust
#[tokio::test]
#[ignore] // Run with `cargo test -- --ignored` when TWS is connected
async fn live_get_positions_returns_option_fields() {
    // Connect to TWS, call get_positions, verify:
    // 1. At least one position has securityType == "OPT"
    // 2. That position has non-null strike, right, expiration, multiplier
    // 3. Stock positions have null option fields
    // 4. strike is a positive number
    // 5. right is "P" or "C"
    // 6. expiration matches YYYYMMDD format
    // 7. multiplier is "100" (standard equity option)
}
```

**Note:** Position struct doesn't derive `Serialize`, so the JSON test in 3.2 needs to go through the same serde_json::json! path used in `tools.rs`, not direct struct serialization. Adjust accordingly — the test should construct the JSON object the same way the tool handler does, or we add `Serialize` derive to Position.

---

## Phase 4 — Build & Deploy

### Step 4.1: Build

```bash
docker exec -it hermes-agent bash
cd /data/dev/trading/ibkr-mcp-rs
cargo build --release
```

If Rust toolchain is missing from the container, build on host and copy binary:

```bash
# On host (needs Rust + same target)
cd /home/jiri/dev/trading/ibkr-mcp-rs
cargo build --release
docker cp target/release/ibkr-mcp-rs hermes-agent:/usr/local/bin/ibkr-mcp-rs
```

### Step 4.2: Deploy

```bash
# Kill old bridge process
docker exec hermes-agent pkill -f ibkr-mcp-rs

# Hermes agent will auto-respawn ibkr-mcp-rs with new binary
# OR manually: restart hermes-agent container
docker restart hermes-agent
```

### Step 4.3: Verify

```bash
# Call get_positions via MCP and check option fields
docker exec -e IBKR_MCP_IBKR__CLIENT_ID=999 hermes-agent sh -c '
printf "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"verify\",\"version\":\"1.0\"}}}\n"
printf "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"get_positions\",\"arguments\":{}}}\n"
' | /usr/local/bin/ibkr-mcp-rs --stdio 2>/dev/null | python3 -c "
import sys, json
for line in sys.stdin:
    resp = json.loads(line)
    if resp.get('id') == 2:
        data = json.loads(resp['result']['content'][0]['text'])
        for p in data['positions']:
            if p.get('securityType') == 'OPT':
                print(f'OPTION: {p[\"symbol\"]} {p[\"right\"]}{p[\"strike\"]} exp={p[\"expiration\"]} x{p[\"multiplier\"]}')
            else:
                print(f'STOCK:  {p[\"symbol\"]} {p[\"quantity\"]} shares')
"
```

Expected output:

```
STOCK:  SOFI 700 shares
STOCK:  IBIT 391 shares
OPTION: IBIT P45.0 exp=20260522 x100
OPTION: XXI P17.5 exp=20260717 x100
...
```

---

## Future Improvements (out of scope for this PR)

| Item | Priority | Notes |
|------|----------|-------|
| Populate `market_price`, `market_value`, `unrealized_pnl`, `daily_pnl` | High | Currently hardcoded 0.0. May need separate TWS subscription or account update stream |
| `get_option_chain` tool | Medium | MCP returns "Unsupported data_type". Would enable browsing strikes/premiums without Yahoo |
| Exercise/assignment history | Low | Needed for tax reporting. No TWS API endpoint known — may need CSV parsing |
| Margin per position | Low | TWS doesn't provide this per-position; only aggregate via account summary |

---

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| TWS doesn't populate strike/expiry in positions stream | Low (ibapi struct has full Contract, API spec confirms) | High — entire plan fails | Live smoke test before Phase 2 (add `tracing::info!("{:?}", pos.contract)` temporarily) |
| `strike` is 0.0 for stocks instead of unset | Confirmed | None — we gate on `is_option` | Already handled: `if is_option { Some(strike) } else { None }` |
| `right` is `None` for stocks (not `Some("?")`) | Confirmed | None | Already handled: `.as_ref().map()` |
| `multiplier` is empty string for stocks | Confirmed | None | Already handled: `!is_empty()` check |
| `last_trade_date_or_contract_month` empty for some options | Possible | Low | Handled: `!is_empty()` check returns `None` |
| Cargo build fails in container (no Rust toolchain) | Medium | Medium | Build on host, copy binary |
| Change breaks existing MCP consumers | Low | High | Additive only — no fields removed or renamed |

## Appendix: ibapi Source Verification

Cloned `rust-ibapi` from `https://github.com/wboayue/rust-ibapi` branch `main` to `/tmp/rust-ibapi/`.

Key files inspected:
- `src/contracts/mod.rs` — Contract struct (line 177), SecurityType enum (line 47), Display impl (line 94)
- `src/contracts/types.rs` — OptionRight enum (line 188)
- `src/accounts/mod.rs` — Position struct (line 187), AccountPortfolioValue (line 244 — has market_price etc.)