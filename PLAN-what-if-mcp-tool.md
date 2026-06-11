# What-If Order Preview — Implementation Spec

## Status
Not yet implemented. Spec for developer agent.

## Overview

Add an MCP tool `what_if` to `ibkr-mcp-rs` that previews the margin/equity/commission impact of a hypothetical order **without executing it**. This answers: "if I sell 200 BTDR, will maintenance margin actually drop, or is this position margin-neutral?"

## Why

During margin distress, the agent needs to rank positions by **actual margin relief per dollar sold**, not just P&L. The BTDR lesson: a green position (+$1,474 gain) can be margin-neutral — selling it frees cash but doesn't release maintenance margin. Without what-if, the agent guesses. With it, the agent gives confident, verified recommendations.

## Dependency: Already Done

The `ibapi` crate (git: `wboayue/rust-ibapi`, branch `main`) already has full what-if support:

**Builder API** (file: `ibapi/src/orders/builder/order_builder.rs`):
```rust
// Lines 589-595
pub fn what_if(mut self) -> Self {
    self.what_if = true;
    self
}
```

**Async analyze method** (file: `ibapi/src/orders/builder/async_impl.rs`):
```rust
// Lines 26-47
pub async fn analyze(mut self) -> Result<crate::orders::OrderState, Error> {
    self.what_if = true;
    let client = self.client;
    let contract = self.contract;
    let order_id = client.next_order_id();
    let order = self.build()?;
    let mut subscription = client.place_order(order_id, contract, &order).await?;
    while let Some(Ok(response)) = subscription.next_data().await {
        if let crate::orders::PlaceOrder::OpenOrder(order_data) = response {
            if order_data.order_id == order_id {
                return Ok(order_data.order_state);
            }
        }
    }
    Err(Error::Simple("What-if analysis did not return order state".to_string()))
}
```

**`OrderState` struct** (file: `ibapi/src/orders/mod.rs`, lines 1278-1340) — this is the response we want:

| Field | Type | Meaning |
|---|---|---|
| `status` | `OrderStatusKind` | Order status |
| `maintenance_margin_before` | `Option<f64>` | Current maintenance margin |
| `maintenance_margin_change` | `Option<f64>` | **Change in maintenance margin (KEY FIELD)** |
| `maintenance_margin_after` | `Option<f64>` | Maintenance margin after order |
| `initial_margin_before` | `Option<f64>` | Current initial margin |
| `initial_margin_change` | `Option<f64>` | Change in initial margin |
| `initial_margin_after` | `Option<f64>` | Initial margin after order |
| `equity_with_loan_before` | `Option<f64>` | Current equity with loan |
| `equity_with_loan_change` | `Option<f64>` | Change in equity with loan |
| `equity_with_loan_after` | `Option<f64>` | Equity with loan after order |
| `commission` | `Option<f64>` | Estimated commission |
| `minimum_commission` | `Option<f64>` | Minimum commission |
| `maximum_commission` | `Option<f64>` | Maximum commission |
| `commission_currency` | `String` | Currency |
| `suggested_size` | `Option<f64>` | Suggested order size |
| `warning_text` | `String` | Warning text if any |
| `reject_reason` | `String` | Reject reason if rejected |

## MCP Tool Spec

### Tool Name
`what_if`

### Description
"Preview margin, equity, and commission impact of a hypothetical order without executing it. Use this to check whether a sale will actually release maintenance margin before committing."

### Parameters

```rust
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct WhatIfParams {
    /// Stock symbol, e.g. "BTDR", "IBIT"
    pub symbol: String,

    /// "SELL" or "BUY"
    pub action: String,

    /// Number of shares/contracts
    pub quantity: f64,

    /// "MKT" or "LMT". Defaults to "MKT".
    #[serde(default = "default_mkt")]
    pub order_type: String,

    /// Limit price. Required only if order_type is "LMT".
    #[serde(default)]
    pub price: Option<f64>,

    /// Account ID. Optional — uses default if omitted.
    #[serde(default)]
    pub account_id: Option<String>,
}
```

Note: First version handles **stock sales only**. Option contracts can be added later.

### Response JSON

```json
{
  "success": true,
  "symbol": "BTDR",
  "action": "SELL",
  "quantity": 200.0,
  "orderType": "MKT",
  "status": "PreSubmitted",
  "initialMarginBefore": 12000.00,
  "initialMarginChange": -800.00,
  "initialMarginAfter": 11200.00,
  "maintenanceMarginBefore": 8500.00,
  "maintenanceMarginChange": -1200.00,
  "maintenanceMarginAfter": 7300.00,
  "equityWithLoanBefore": 15500.00,
  "equityWithLoanChange": -3500.00,
  "equityWithLoanAfter": 12000.00,
  "commission": 1.00,
  "minimumCommission": 0.50,
  "maximumCommission": 1.50,
  "commissionCurrency": "USD",
  "suggestedSize": null,
  "warningText": "",
  "rejectReason": ""
}
```

Error response:
```json
{
  "success": false,
  "symbol": "BTDR",
  "error": "What-if analysis did not return order state"
}
```

### Key Field for Jiri

**`maintenanceMarginChange`** — this is the single field that answers "does this sale help?"

- Negative value = margin relief (good — sell this)
- Zero = margin-neutral (skip this position)
- Positive = margin increase (bad — don't sell)

Jiri verifies this same field in TWS what-if on his phone under: Order Preview → Balances → Maintenance → Change column.

## Implementation Plan

### Files to Modify

| File | What |
|---|---|
| `src/ibkr/orders.rs` | Add `WhatIfRequest` struct + `what_if_order()` method on `OrderManager` |
| `src/mcp/tools.rs` | Add `WhatIfParams` struct + `#[tool]` handler on `IbkrMcpServer` |

### Step 1: Add `what_if_order()` to `OrderManager` (`src/ibkr/orders.rs`)

```rust
use ibapi::contracts::Contract;

/// What-if order request
#[derive(Debug, Clone)]
pub struct WhatIfRequest {
    pub symbol: String,
    pub action: String,     // "SELL" or "BUY"
    pub quantity: f64,
    pub order_type: String, // "MKT" or "LMT"
    pub price: Option<f64>,
}

/// What-if result — margin/equity/commission impact
#[derive(Debug, Clone, Serialize)]
pub struct WhatIfResult {
    pub symbol: String,
    pub action: String,
    pub quantity: f64,
    pub order_type: String,
    pub status: String,
    pub initial_margin_before: Option<f64>,
    pub initial_margin_change: Option<f64>,
    pub initial_margin_after: Option<f64>,
    pub maintenance_margin_before: Option<f64>,
    pub maintenance_margin_change: Option<f64>,
    pub maintenance_margin_after: Option<f64>,
    pub equity_with_loan_before: Option<f64>,
    pub equity_with_loan_change: Option<f64>,
    pub equity_with_loan_after: Option<f64>,
    pub commission: Option<f64>,
    pub minimum_commission: Option<f64>,
    pub maximum_commission: Option<f64>,
    pub commission_currency: String,
    pub suggested_size: Option<f64>,
    pub warning_text: String,
    pub reject_reason: String,
}
```

Implementation of `OrderManager::what_if_order()`:

```rust
pub async fn what_if_order(
    &self,
    req: WhatIfRequest,
) -> Result<WhatIfResult, IbkrError> {
    if !self.client.is_connected().await {
        return Err(IbkrError::NotConnected);
    }

    let client = self.client.get_client().await?;

    // Build stock contract (Contract::stock returns a builder, .build() finalizes)
    let contract = Contract::stock(&req.symbol).build();

    // Build order via ibapi OrderBuilder
    // .buy(qty) and .sell(qty) combine action + quantity — no separate .action() needed
    let mut builder = match req.action.to_uppercase().as_str() {
        "SELL" => client.order(&contract).sell(req.quantity),
        "BUY"  => client.order(&contract).buy(req.quantity),
        other => return Err(IbkrError::Unknown(
            format!("Invalid action '{}'. Use SELL or BUY.", other)
        )),
    };

    // Set order type
    match req.order_type.to_uppercase().as_str() {
        "MKT" => {
            builder = builder.market();  // NOT .market_order()
        }
        "LMT" => {
            let limit_price = req.price
                .ok_or_else(|| IbkrError::Unknown(
                    "Limit order requires a price".to_string()
                ))?;
            builder = builder.limit(limit_price);
        }
        other => return Err(IbkrError::Unknown(
            format!("Invalid order_type '{}'. Use MKT or LMT.", other)
        )),
    };

    // Mark as what-if and analyze
    builder = builder.what_if();

    let order_state = builder.analyze().await
        .map_err(|e| IbkrError::Unknown(format!("what-if failed: {e}")))?;

    Ok(WhatIfResult {
        symbol: req.symbol,
        action: req.action,
        quantity: req.quantity,
        order_type: req.order_type,
        status: order_state.status.to_string(),
        initial_margin_before: order_state.initial_margin_before,
        initial_margin_change: order_state.initial_margin_change,
        initial_margin_after: order_state.initial_margin_after,
        maintenance_margin_before: order_state.maintenance_margin_before,
        maintenance_margin_change: order_state.maintenance_margin_change,
        maintenance_margin_after: order_state.maintenance_margin_after,
        equity_with_loan_before: order_state.equity_with_loan_before,
        equity_with_loan_change: order_state.equity_with_loan_change,
        equity_with_loan_after: order_state.equity_with_loan_after,
        commission: order_state.commission,
        minimum_commission: order_state.minimum_commission,
        maximum_commission: order_state.maximum_commission,
        commission_currency: order_state.commission_currency,
        suggested_size: order_state.suggested_size,
        warning_text: order_state.warning_text,
        reject_reason: order_state.reject_reason,
    })
}
```

**Important notes:**
- `.buy(qty)` and `.sell(qty)` are convenience methods that set both action and quantity — there is NO separate `.action()` or `.total_quantity()` on `OrderBuilder`. Source: `ibapi/src/orders/builder/order_builder.rs` lines 154-165.
- `.market()` not `.market_order()` — the method is simply named `market()`. Source: same file line 170.
- `client.order(&contract)` borrows `&Client` — the `Arc<Client>` from `get_client()` must be dereffed. This works because `Arc<T>` implements `Deref`. Source: `ibapi/src/client/async.rs` lines 296-298.
- `order_state.status.to_string()` — `OrderStatusKind` implements `Display`.
- Add `Serialize` derive to `WhatIfResult` (needed for JSON output in tools.rs).
- Add `use serde::Serialize;` if not already imported.
- Verified against `ibapi` v3 crate source at `rust-ibapi/9fe6144`.

### Step 2: Add `#[tool]` handler in `tools.rs`

After the existing `get_option_quote` handler, add:

```rust
/// Preview margin/equity/commission impact without executing
#[tool(description = "Preview margin, equity, and commission impact of a hypothetical order without executing it. Use to check if a sale will release maintenance margin before committing.")]
async fn what_if(
    &self,
    Parameters(params): Parameters<WhatIfParams>,
) -> String {
    let req = WhatIfRequest {
        symbol: params.symbol.to_uppercase(),
        action: params.action.to_uppercase(),
        quantity: params.quantity,
        order_type: params.order_type.to_uppercase(),
        price: params.price,
    };

    match self.orders.what_if_order(req).await {
        Ok(result) => {
            serde_json::to_string_pretty(
                &serde_json::json!({
                    "success": true,
                    "symbol": result.symbol,
                    "action": result.action,
                    "quantity": result.quantity,
                    "orderType": result.order_type,
                    "status": result.status,
                    "initialMarginBefore": result.initial_margin_before,
                    "initialMarginChange": result.initial_margin_change,
                    "initialMarginAfter": result.initial_margin_after,
                    "maintenanceMarginBefore": result.maintenance_margin_before,
                    "maintenanceMarginChange": result.maintenance_margin_change,
                    "maintenanceMarginAfter": result.maintenance_margin_after,
                    "equityWithLoanBefore": result.equity_with_loan_before,
                    "equityWithLoanChange": result.equity_with_loan_change,
                    "equityWithLoanAfter": result.equity_with_loan_after,
                    "commission": result.commission,
                    "minimumCommission": result.minimum_commission,
                    "maximumCommission": result.maximum_commission,
                    "commissionCurrency": result.commission_currency,
                    "suggestedSize": result.suggested_size,
                    "warningText": result.warning_text,
                    "rejectReason": result.reject_reason,
                })
            )
            .unwrap_or_default()
        }
        Err(e) => {
            serde_json::to_string_pretty(
                &serde_json::json!({
                    "success": false,
                    "symbol": params.symbol,
                    "error": e.to_string(),
                })
            )
            .unwrap_or_default()
        }
    }
}
```

Also add the params struct in the params section:

```rust
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct WhatIfParams {
    #[schemars(description = "Stock symbol, e.g. BTDR, IBIT")]
    pub symbol: String,

    #[schemars(description = "Order action: 'SELL' or 'BUY'")]
    pub action: String,

    #[schemars(description = "Number of shares")]
    pub quantity: f64,

    #[schemars(description = "Order type: 'MKT' or 'LMT'. Defaults to 'MKT'.")]
    #[serde(default = "default_mkt")]
    pub order_type: String,

    #[schemars(description = "Limit price. Required only if order_type is 'LMT'.")]
    #[serde(default)]
    pub price: Option<f64>,

    #[schemars(description = "Account ID. Optional — uses default if omitted.")]
    #[serde(default)]
    pub account_id: Option<String>,
}
```

### Step 3: Wire up in `tools.rs`

Add `WhatIfRequest` and `WhatIfResult` to the imports from `crate::ibkr::orders`. They need to be `pub` in `orders.rs`.

No changes needed to `IbkrMcpServer::new()` — `OrderManager` is already `Arc<OrderManager>` on the struct, and `what_if_order` takes `&self`.

## Key API Decisions

| Decision | Why |
|---|---|
| **Market orders only** in v1 | Limit orders require `price` — keep it simple. The margin impact for a MKT vs LMT at market price is identical for what-if purposes. |
| **Stocks only** in v1 | Option contracts require strike/right/expiration — add later. The immediate use case is portfolio stocks during margin distress. |
| **No actual order placed** | `what_if = true` on the `Order` struct means IBKR treats it as a preview only. Zero risk of accidental execution. |
| **No `outbox_pattern` config needed** | The `analyze()` method handles the full flow internally — no manual `next_order_id` + `place_order` + stream polling needed in our code. |
| **Read-only compatible** | What-if orders are inherently read-only. The MCP bridge's `READ_ONLY: true` config stays unchanged. |

## Expected Behavior

### Happy path
```
mcp_ibkr_what_if(symbol="BTDR", action="SELL", quantity=200)
→ maintenanceMarginChange: -1200.00  ← margin relief confirmed
→ equityWithLoanChange: -3500.00     ← cash freed
→ commission: 1.00
```

### Margin-neutral position
```
mcp_ibkr_what_if(symbol="BTDR", action="SELL", quantity=100)
→ maintenanceMarginChange: 0.00       ← MARGIN NEUTRAL — skip this
→ equityWithLoanChange: -1750.00
```

### Error: invalid action
```
mcp_ibkr_what_if(symbol="BTDR", action="HODL", quantity=200)
→ success: false, error: "Invalid action 'HODL'. Use SELL or BUY."
```

## Pitfalls / Notes for Developer

1. **`IbkrClient::get_client()` returns `Arc<Client>`** — The `OrderBuilder` needs `&Client`, and `OrderBuilder::analyze()` takes ownership of `self`. So the flow is: get the `Arc<Client>`, deref to `&Client` (automatic via `Arc: Deref`), pass to `client.order(&contract)`, chain `.what_if().analyze().await`. Source: `ibapi/src/client/async.rs` lines 296-298.

2. **`next_order_id()` is called inside `analyze()`** — The `ibapi` crate handles order ID allocation internally. We don't need to manage IDs.

3. **`OrderStatusKind` doesn't implement `Serialize`** — We call `.to_string()` on it to produce `"PreSubmitted"`, `"Submitted"`, etc.

4. **`OrderManager` currently stubs `place_order` with `"not yet implemented"`** — The `what_if_order` method is completely new and uses the `ibapi` crate's `OrderBuilder` path, not the existing stub. No conflict.

5. **Need `use ibapi::contracts::Contract;`** — The `Contract::stock(&symbol).build()` call requires the import if not already present.

6. **`.buy(qty)` / `.sell(qty)` combine action+quantity** — There is NO separate `.action()` or `.total_quantity()` method on `OrderBuilder`. Use `.buy(req.quantity)` or `.sell(req.quantity)` directly. Verified against `ibapi/src/orders/builder/order_builder.rs` lines 154-165.

7. **`.market()` not `.market_order()`** — The method name is simply `market()`. Verified against `ibapi/src/orders/builder/order_builder.rs` line 170. The default `Order` already has `order_type: "MKT"`, so `.market()` is technically redundant for market orders — but being explicit is safer.

8. **No `Action` import needed in orders.rs** — Since we use `.buy(qty)`/`.sell(qty)` which internally set `Action::Buy`/`Action::Sell`, we don't need to import `Action` directly. We do need `use ibapi::contracts::Contract;` though.

9. **IBKR official spec confirms behavior** — The `Order.WhatIf` flag set to `true` + `placeOrder` = credit check only, no actual order transmission. The `OrderState` object returns `initialMarginBefore/Change/After`, `maintenanceMarginBefore/Change/After`, `equityWithLoanBefore/Change/After`, and `commission`. Source: [IBKR TWS API margin docs](https://interactivebrokers.github.io/tws-api/margin.html).

9. **Build command**:
   ```bash
   cd /data/dev/trading/ibkr-mcp-rs
   cargo build --release
   ```

10. **Test approach**: After building and deploying, test from Hermes:
    ```
    mcp_ibkr_what_if(symbol="BTDR", action="SELL", quantity=200)
    ```
    Check `maintenanceMarginChange` — if negative, the position releases margin. If zero, it's margin-neutral.

## Agent Usage Pattern

After this tool ships, the margin-distress workflow becomes:

```
1. excess_liquidity drops below $2K → alert
2. Agent calls mcp_ibkr_what_if() on all 11 stock positions (MKT SELL full qty)
3. Agent ranks by maintenanceMarginChange (most negative = best sell)
4. Agent tells Jiri: "Sell BTDR (−$1,200 margin relief, +$1,474 gain) then NVTS (−$800 margin relief, +$36 flat)"
5. Jiri verifies in TWS → executes
```

No more guessing about which positions are margin-neutral.
