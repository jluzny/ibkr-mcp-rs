# Plan: Add `get_executions` MCP Tool for Historical Trade P&L

**Date:** 2026-05-17
**Status:** Planning → Implementation
**Goal:** Return historical executions (fills) with P&L so user can compute weekly/daily realized P&L

## TWS API Capabilities

`Client::executions(filter: ExecutionFilter)` → `Subscription<Executions>`

**ExecutionFilter fields:**
- `client_id: i32` — filter by client ID (0 = all)
- `account_code: String` — filter by account (e.g. "U18197748")
- `time: String` — filter by time (format: "YYYYMMDD-HH:MM:SS")
- `symbol: String` — filter by symbol
- `security_type: SecurityType` — filter by type
- `exchange: String` — filter by exchange
- `side: Option<ExecutionFilterSide>` — Buy/Sell filter

**ExecutionData fields:**
- `contract: Contract` — symbol, type, strike, expiry, etc.
- `execution: Execution` — order_id, time, side, shares, price, account_number

**CommissionReport fields:**
- `execution_id: String`
- `commission: f64`
- `realized_pnl: f64`
- `yield_redemption_date: i32`
- `yield_: f64`

## Implementation Plan

### Step 1: Add `get_executions` to `ibkr/account.rs` or new `ibkr/orders.rs`

New struct:
```rust
pub struct Execution {
    pub execution_id: String,
    pub symbol: String,
    pub security_type: String,
    pub side: String,
    pub quantity: f64,
    pub price: f64,
    pub commission: f64,
    pub realized_pnl: f64,
    pub time: String,
    pub account_id: String,
    pub strike: Option<f64>,
    pub right: Option<String>,
    pub expiration: Option<String>,
    pub multiplier: Option<String>,
}
```

New method:
```rust
pub async fn get_executions(
    &self,
    account_id: Option<&str>,
    symbol: Option<&str>,
    since: Option<&str>, // "YYYYMMDD-HH:MM:SS" or "2026-05-12"
) -> Result<Vec<Execution>, IbkrError>
```

### Step 2: Wire into MCP tools (`src/mcp/tools.rs`)

New tool: `get_executions`
- Input: `account_id`, `symbol` (optional), `since` (optional, default 7 days ago)
- Output: JSON array of executions

### Step 3: Add P&L helper

Optionally compute realized P&L per symbol from the execution list.

### Step 4: Tests

- Unit test: mock ExecutionData → verify JSON output
- Integration test: live query (marked `#[ignore]`)

## Risks

| Risk | Mitigation |
|------|-----------|
| TWS execution history limited | Document limitation; TWS keeps ~7 days via API |
| CommissionReport arrives separately | Collect both ExecutionData and CommissionReport, match by execution_id |
| Time filter format | Use "YYYYMMDD-HH:MM:SS" format per TWS spec |

## Notes

- The `executions()` stream yields `Executions` enum with `ExecutionData` and `CommissionReport` variants
- Need to match commission reports to executions by `execution_id`
- For weekly P&L: filter executions where `time >= last Monday`
