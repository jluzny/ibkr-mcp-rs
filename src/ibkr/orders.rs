
use std::sync::Arc;
use tracing::info;
use serde::Serialize;
use futures::StreamExt;

use crate::ibkr::client::IbkrClient;
use crate::ibkr::error::IbkrError;

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

/// Order information
#[derive(Debug, Clone)]
pub struct Order {
    pub id: String,
    pub symbol: String,
    pub side: String,   // BUY or SELL
    pub order_type: String, // MKT, LMT, STP
    pub quantity: f64,
    pub price: Option<f64>,
    pub status: String,
    pub filled_qty: f64,
    pub avg_fill_price: f64,
}

/// Order placement request
#[derive(Debug, Clone)]
pub struct PlaceOrderRequest {
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub quantity: f64,
    pub price: Option<f64>,
}

/// Order manager
#[derive(Debug)]
pub struct OrderManager {
    client: Arc<IbkrClient>,
}

impl OrderManager {
    pub fn new(client: Arc<IbkrClient>) -> Self {
        Self { client }
    }

    /// Place an order
    pub async fn place_order(
        &self,
        req: PlaceOrderRequest,
    ) -> Result<Order, IbkrError> {
        if !self.client.is_connected().await {
            return Err(IbkrError::NotConnected);
        }

        info!(
            symbol = %req.symbol,
            side = %req.side,
            order_type = %req.order_type,
            quantity = req.quantity,
            "Placing order"
        );

        Err(IbkrError::Unknown(
            "Place order not yet implemented".to_string(),
        ))
    }

    /// Cancel an order
    pub async fn cancel_order(
        &self,
        order_id: &str,
    ) -> Result<(), IbkrError> {
        if !self.client.is_connected().await {
            return Err(IbkrError::NotConnected);
        }

        info!(order_id = %order_id, "Cancelling order");

        Err(IbkrError::Unknown(
            "Cancel order not yet implemented".to_string(),
        ))
    }

    /// List the orders the broker currently reports open — the D4 read.
    ///
    /// `reqAllOpenOrders` (via `all_open_orders()`) and **not** `open_orders()`:
    /// this read exists for orders submitted by a client id that may no longer
    /// exist, and the per-client request would miss them. Strictly read-only —
    /// no order path is reachable from here.
    ///
    /// A failed read is an error, never an empty list: `orders: []` is what an
    /// empty book looks like, and collapsing the two is the defect D4 closes.
    pub async fn open_orders(
        &self,
        account_id: &str,
        symbol: &str,
    ) -> Result<OpenOrdersRead, IbkrError> {
        if !self.client.is_connected().await {
            return Err(IbkrError::NotConnected);
        }

        let client = self.client.get_client().await?;
        let client_ref: &ibapi::Client = &*client;

        let mut subscription = client_ref
            .all_open_orders()
            .await
            .map_err(|e| IbkrError::Unknown(format!("open orders read failed: {e}")))?;

        // One request, one round trip: the gateway answers `openOrder`s and then
        // `openOrderEnd`, which closes the subscription (the library maps the
        // marker to end-of-stream). A snapshot read is cheap; 10 s is generous
        // without stalling a dispatch-time precondition on a dead link.
        let rows = tokio::time::timeout(
            tokio::time::Duration::from_secs(OPEN_ORDERS_TIMEOUT_SECS),
            async {
                let mut rows: Vec<OpenOrderRow> = Vec::new();
                while let Some(item) = subscription.next().await {
                    match item {
                        Ok(ibapi::subscriptions::SubscriptionItem::Data(message)) => {
                            apply_orders_message(&mut rows, message);
                        }
                        Ok(ibapi::subscriptions::SubscriptionItem::Notice(n)) => {
                            info!(notice = %n, "open orders subscription notice");
                        }
                        Err(e) => {
                            return Err(IbkrError::Unknown(format!("open orders read failed: {e}")));
                        }
                    }
                }
                Ok(rows)
            },
        )
        .await
        .map_err(|_| {
            IbkrError::Unknown("open orders: timed out waiting for the gateway".to_string())
        })??;

        Ok(OpenOrdersRead {
            account_id: account_id.to_string(),
            symbol: symbol.to_string(),
            orders: filter_orders(rows, account_id, symbol),
        })
    }

    /// Get order status
    pub async fn get_order_status(
        &self,
        order_id: &str,
    ) -> Result<Order, IbkrError> {
        let _client = self.client.get_client().await?;

        info!(order_id = %order_id, "Getting order status");

        Err(IbkrError::Unknown(
            "Get order status not yet implemented".to_string(),
        ))
    }

    /// Preview margin/equity/commission impact of a hypothetical order without executing it.
    ///
    /// Manually places the what-if order and polls the order-specific subscription for the
    /// OpenOrder response.  We avoid the upstream `OrderBuilder::analyze()` because it returns
    /// `UnexpectedEndOfStream` when the subscription closes before the what-if data arrives.
    pub async fn what_if_order(
        &self,
        req: WhatIfRequest,
    ) -> Result<WhatIfResult, IbkrError> {
        if !self.client.is_connected().await {
            return Err(IbkrError::NotConnected);
        }

        let client = self.client.get_client().await?;
        let client_ref: &ibapi::Client = &*client;

        // Build stock contract (defaults to SMART/USD)
        let contract = ibapi::contracts::Contract::stock(&req.symbol).build();

        // Build order via ibapi OrderBuilder
        let mut builder = match req.action.to_uppercase().as_str() {
            "SELL" => client_ref.order(&contract).sell(req.quantity),
            "BUY"  => client_ref.order(&contract).buy(req.quantity),
            other => return Err(IbkrError::Unknown(
                format!("Invalid action '{}'. Use SELL or BUY.", other)
            )),
        };

        // Set order type
        match req.order_type.to_uppercase().as_str() {
            "MKT" => { builder = builder.market(); }
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

        let order = builder.what_if().build_order()
            .map_err(|e| IbkrError::Unknown(format!("what-if build failed: {e}")))?;

        let order_id = client_ref.next_order_id();

        info!(
            order_id = order_id,
            symbol = %req.symbol,
            action = %order.action,
            quantity = order.total_quantity,
            order_type = %order.order_type,
            what_if = order.what_if,
            "Submitting what-if order"
        );

        // Place the order and get the order-specific subscription.
        let mut subscription = client_ref.place_order(order_id, &contract, &order).await
            .map_err(|e| IbkrError::Unknown(format!("what-if place_order failed: {e}")))?;

        // Poll the subscription for the OpenOrder response containing margin data.
        // IBKR usually responds within 1–3 s for what-if; we time out at 15 s.
        let deadline = tokio::time::Duration::from_secs(15);

        let order_state = tokio::time::timeout(deadline, async {
            while let Some(item) = subscription.next().await {
                match item {
                    Ok(ibapi::subscriptions::SubscriptionItem::Data(
                        ibapi::orders::PlaceOrder::OpenOrder(data)
                    )) => {
                        if data.order_id == order_id {
                            info!(order_id = order_id, status = %data.order_state.status, "what-if OpenOrder received");
                            return Some(data.order_state);
                        }
                    }
                    Ok(ibapi::subscriptions::SubscriptionItem::Data(
                        ibapi::orders::PlaceOrder::OrderStatus(st)
                    )) => {
                        info!(order_id = st.order_id, status = %st.status, "what-if OrderStatus received");
                    }
                    Ok(ibapi::subscriptions::SubscriptionItem::Data(
                        ibapi::orders::PlaceOrder::ExecutionData(ed)
                    )) => {
                        info!(exec_id = %ed.execution.execution_id, "what-if ExecutionData received");
                    }
                    Ok(ibapi::subscriptions::SubscriptionItem::Data(
                        ibapi::orders::PlaceOrder::CommissionReport(cr)
                    )) => {
                        info!(exec_id = %cr.execution_id, commission = ?cr.commission, "what-if CommissionReport received");
                    }
                    Ok(ibapi::subscriptions::SubscriptionItem::Notice(n)) => {
                        info!(notice = %n, "what-if subscription notice");
                    }
                    Err(e) => {
                        info!(error = %e, "what-if subscription error");
                    }
                }
            }
            info!(order_id = order_id, "what-if subscription closed without OpenOrder");
            None
        }).await
        .map_err(|_| IbkrError::Unknown("what-if: timed out waiting for OpenOrder response".to_string()))?
        .ok_or_else(|| IbkrError::Unknown("what-if: no OpenOrder received".to_string()))?;

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
}

/// One request, one round trip: the gateway answers `openOrder`s and then
/// `openOrderEnd`, which is what resolves the request (D4 contract).
pub const OPEN_ORDERS_TIMEOUT_SECS: u64 = 10;

/// One order the gateway reported open — the D4 read, as plain data.
///
/// Projected from IBKR's *all open orders* response, which covers orders
/// created through the TWS API **regardless of which client id submitted
/// them** — the residual case behind `t_e85f4f68` run 141, where the submitting
/// client no longer exists. Orders placed *manually* in TWS are not covered;
/// that needs the master-client mechanism and is deliberately out of scope.
///
/// `filled` / `remaining` are the values the gateway reported **for this
/// snapshot**. The authoritative *"is it still working"* discriminator is
/// `working` (and `status`), not `remaining`: IBKR does not keep an ownerless
/// order in sync, so `filled` / `remaining` can read `0` on a partially filled
/// resting order — use `get_executions` for fills.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenOrderRow {
    pub order_id: i32,
    pub perm_id: i64,
    pub client_id: i32,
    pub account: String,
    pub symbol: String,
    pub con_id: i32,
    pub sec_type: String,
    pub side: String,
    pub quantity: f64,
    pub order_type: String,
    pub lmt_price: Option<f64>,
    pub tif: String,
    pub status: String,
    /// The D4 discriminator: `status` not in a terminal state. An unknown
    /// status reports `working: true` — the fail-closed direction.
    pub working: bool,
    pub filled: f64,
    pub remaining: f64,
    pub outside_rth: bool,
    pub order_ref: String,
}

impl OpenOrderRow {
    /// The row the Python contract constructs for a status-only report: every
    /// key present, `sec_type` defaulted to `STK`, everything else empty.
    fn empty() -> Self {
        Self {
            order_id: 0,
            perm_id: 0,
            client_id: 0,
            account: String::new(),
            symbol: String::new(),
            con_id: 0,
            sec_type: "STK".to_string(),
            side: String::new(),
            quantity: 0.0,
            order_type: String::new(),
            lmt_price: None,
            tif: String::new(),
            status: String::new(),
            working: false,
            filled: 0.0,
            remaining: 0.0,
            outside_rth: false,
            order_ref: String::new(),
        }
    }

    /// Project the `OpenOrder` snapshot message (`Orders::OrderData`).
    fn from_order_data(data: &ibapi::orders::OrderData) -> Self {
        let status = &data.order_state.status;
        Self {
            order_id: data.order.order_id,
            perm_id: data.order.perm_id,
            client_id: data.order.client_id,
            account: data.order.account.clone(),
            symbol: data.contract.symbol.to_string(),
            con_id: data.contract.contract_id,
            sec_type: data.contract.security_type.to_string(),
            side: data.order.action.to_string(),
            quantity: data.order.total_quantity,
            order_type: data.order.order_type.clone(),
            lmt_price: data.order.limit_price,
            tif: data.order.tif.to_string(),
            status: status.to_string(),
            working: !status.is_terminal(),
            filled: 0.0,
            remaining: 0.0,
            outside_rth: data.order.outside_rth,
            order_ref: data.order.order_ref.clone(),
        }
    }

    /// Overlay the live `OrderStatus` message: status fields only — the
    /// statics belong to the `OpenOrder` report.
    fn apply_status(&mut self, status: &ibapi::orders::OrderStatus) {
        self.status = status.status.to_string();
        self.working = !status.status.is_terminal();
        self.filled = status.filled;
        self.remaining = status.remaining;
        if self.perm_id == 0 {
            self.perm_id = status.perm_id;
        }
    }
}

/// The orders the gateway reported open, plus the filter that was asked for.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenOrdersRead {
    /// The account filter that was asked for (echoed, never discovered).
    pub account_id: String,
    /// The symbol filter that was asked for (echoed, never discovered).
    pub symbol: String,
    pub orders: Vec<OpenOrderRow>,
}

impl OpenOrdersRead {
    /// The rows that are not in a terminal state — the D4 answer, as data.
    pub fn working(&self) -> impl Iterator<Item = &OpenOrderRow> {
        self.orders.iter().filter(|order| order.working)
    }

    /// The tool's JSON object: `success` + camelCase, like every other tool.
    pub fn as_payload(&self) -> serde_json::Value {
        serde_json::json!({
            "success": true,
            "accountId": self.account_id,
            "symbol": self.symbol,
            "count": self.orders.len(),
            "workingCount": self.working().count(),
            "orders": self.orders.iter().map(order_payload).collect::<Vec<_>>(),
        })
    }
}

/// One order, every key present, `None` rendered as `null` (the bridge's rule).
fn order_payload(order: &OpenOrderRow) -> serde_json::Value {
    serde_json::json!({
        "orderId": order.order_id,
        "permId": order.perm_id,
        "clientId": order.client_id,
        "accountId": order.account,
        "symbol": order.symbol,
        "conId": order.con_id,
        "secType": order.sec_type,
        "side": order.side,
        "quantity": order.quantity,
        "orderType": order.order_type,
        "lmtPrice": order.lmt_price,
        "tif": order.tif,
        "status": order.status,
        "working": order.working,
        "filled": order.filled,
        "remaining": order.remaining,
        "outsideRth": order.outside_rth,
        "orderRef": order.order_ref,
    })
}

/// Fold one stream message into the rows: `OpenOrder` projects the row,
/// `OrderStatus` overlays the live status fields on it.
///
/// Keyed by `(client_id, order_id)` — order ids are scoped to a client
/// namespace, so the pair is the identity across clients. A status message for
/// an identity with no `OpenOrder` report still becomes a row: this read must
/// never hide a working order because a field or a message is absent.
fn apply_orders_message(rows: &mut Vec<OpenOrderRow>, message: ibapi::orders::Orders) {
    match message {
        ibapi::orders::Orders::OrderData(data) => {
            let key = (data.order.client_id, data.order.order_id);
            match rows.iter_mut().find(|row| (row.client_id, row.order_id) == key) {
                Some(existing) => {
                    // A repeated report refreshes the statics and status; the
                    // live filled/remaining belong to the latest OrderStatus.
                    let (filled, remaining) = (existing.filled, existing.remaining);
                    *existing = OpenOrderRow::from_order_data(&data);
                    existing.filled = filled;
                    existing.remaining = remaining;
                }
                None => rows.push(OpenOrderRow::from_order_data(&data)),
            }
        }
        ibapi::orders::Orders::OrderStatus(status) => {
            let key = (status.client_id, status.order_id);
            match rows.iter_mut().find(|row| (row.client_id, row.order_id) == key) {
                Some(row) => row.apply_status(&status),
                None => {
                    let mut row = OpenOrderRow::empty();
                    row.order_id = status.order_id;
                    row.client_id = status.client_id;
                    row.apply_status(&status);
                    rows.push(row);
                }
            }
        }
    }
}

/// The D4 filters, exactly as the seam documents them: the `account` filter may
/// only *widen* (a row whose gateway report omits `account` is kept rather than
/// dropped, because dropping it could hide a working order), and `symbol` is
/// exact and case-insensitive.
fn filter_orders(
    rows: Vec<OpenOrderRow>,
    account_id: &str,
    symbol: &str,
) -> Vec<OpenOrderRow> {
    rows.into_iter()
        .filter(|row| {
            if !account_id.is_empty() && !row.account.is_empty() && row.account != account_id {
                return false;
            }
            if !symbol.is_empty() && row.symbol.to_uppercase() != symbol.to_uppercase() {
                return false;
            }
            true
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ibapi::contracts::Contract;
    use ibapi::orders::{
        Action, Order, OrderData, OrderState, OrderStatus, OrderStatusKind, Orders, TimeInForce,
    };

    fn resting_sell_6_aapl() -> OrderData {
        let mut contract = Contract::stock("AAPL").build();
        contract.contract_id = 265598;

        let mut order = Order::default();
        order.order_id = 7001;
        order.client_id = 320;
        order.perm_id = 99887766;
        order.account = "DUK271345".to_string();
        order.action = Action::Sell;
        order.total_quantity = 6.0;
        order.order_type = "LMT".to_string();
        order.limit_price = Some(331.74);
        order.tif = TimeInForce::Day;
        order.outside_rth = false;
        order.order_ref = "ladder".to_string();

        let mut order_state = OrderState::default();
        order_state.status = OrderStatusKind::Submitted;

        OrderData {
            order_id: 7001,
            contract,
            order,
            order_state,
        }
    }

    fn status_update(status: OrderStatusKind, filled: f64, remaining: f64) -> OrderStatus {
        let mut st = OrderStatus::default();
        st.order_id = 7001;
        st.client_id = 320;
        st.perm_id = 99887766;
        st.status = status;
        st.filled = filled;
        st.remaining = remaining;
        st
    }

    #[test]
    fn open_order_data_projects_the_d4_row() {
        let row = OpenOrderRow::from_order_data(&resting_sell_6_aapl());
        assert_eq!(row.order_id, 7001);
        assert_eq!(row.perm_id, 99887766);
        assert_eq!(row.client_id, 320);
        assert_eq!(row.account, "DUK271345");
        assert_eq!(row.symbol, "AAPL");
        assert_eq!(row.con_id, 265598);
        assert_eq!(row.sec_type, "STK");
        assert_eq!(row.side, "SELL");
        assert_eq!(row.quantity, 6.0);
        assert_eq!(row.order_type, "LMT");
        assert_eq!(row.lmt_price, Some(331.74));
        assert_eq!(row.tif, "DAY");
        assert_eq!(row.status, "Submitted");
        assert!(row.working);
        assert_eq!(row.filled, 0.0);
        assert_eq!(row.remaining, 0.0);
        assert!(!row.outside_rth);
        assert_eq!(row.order_ref, "ladder");
    }

    #[test]
    fn a_market_order_reports_a_null_limit_price() {
        let mut data = resting_sell_6_aapl();
        data.order.order_type = "MKT".to_string();
        data.order.limit_price = None;
        let row = OpenOrderRow::from_order_data(&data);
        assert_eq!(row.lmt_price, None);
        assert_eq!(order_payload(&row)["lmtPrice"], serde_json::Value::Null);
    }

    #[test]
    fn the_order_status_overlay_updates_the_live_fields() {
        let mut rows = Vec::new();
        apply_orders_message(&mut rows, Orders::OrderData(resting_sell_6_aapl()));
        apply_orders_message(
            &mut rows,
            Orders::OrderStatus(status_update(OrderStatusKind::Submitted, 2.0, 4.0)),
        );

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.status, "Submitted");
        assert_eq!(row.filled, 2.0);
        assert_eq!(row.remaining, 4.0);
        // The statics survive the overlay.
        assert_eq!(row.symbol, "AAPL");
        assert_eq!(row.order_ref, "ladder");
    }

    #[test]
    fn a_status_only_report_is_still_shown() {
        let mut rows = Vec::new();
        apply_orders_message(
            &mut rows,
            Orders::OrderStatus(status_update(OrderStatusKind::Submitted, 0.0, 6.0)),
        );

        assert_eq!(rows.len(), 1, "a status-only order must not be hidden");
        let row = &rows[0];
        assert_eq!(row.order_id, 7001);
        assert_eq!(row.client_id, 320);
        assert_eq!(row.sec_type, "STK");
        assert_eq!(row.status, "Submitted");
        assert!(row.working);
    }

    #[test]
    fn working_fails_closed_on_an_unknown_status() {
        let mut rows = Vec::new();
        apply_orders_message(
            &mut rows,
            Orders::OrderStatus(status_update(OrderStatusKind::Unknown("WEIRD".to_string()), 0.0, 1.0)),
        );
        assert!(rows[0].working, "an unknown status must read as working");
        assert_eq!(rows[0].status, "WEIRD");

        for terminal in [
            OrderStatusKind::Filled,
            OrderStatusKind::Cancelled,
            OrderStatusKind::ApiCancelled,
            OrderStatusKind::Inactive,
        ] {
            let mut rows = Vec::new();
            apply_orders_message(&mut rows, Orders::OrderStatus(status_update(terminal, 6.0, 0.0)));
            assert!(!rows[0].working, "{:?} is terminal", rows[0].status);
        }
    }

    #[test]
    fn the_account_filter_only_widens() {
        let mut rows = vec![OpenOrderRow {
            account: "DUK271345".to_string(),
            ..OpenOrderRow::empty()
        }];
        rows.push(OpenOrderRow {
            account: String::new(),
            ..OpenOrderRow::empty()
        });
        rows.push(OpenOrderRow {
            account: "U18197748".to_string(),
            ..OpenOrderRow::empty()
        });

        let kept = filter_orders(rows, "DUK271345", "");
        assert_eq!(kept.len(), 2, "the unreported-account row is kept, the live one dropped");
        assert_eq!(kept[0].account, "DUK271345");
        assert_eq!(kept[1].account, "");
    }

    #[test]
    fn the_symbol_filter_is_exact_and_case_insensitive() {
        let rows = vec![
            OpenOrderRow {
                symbol: "AAPL".to_string(),
                ..OpenOrderRow::empty()
            },
            OpenOrderRow {
                symbol: "AAP".to_string(),
                ..OpenOrderRow::empty()
            },
        ];

        assert_eq!(filter_orders(rows.clone(), "", "aapl").len(), 1);
        assert_eq!(filter_orders(rows.clone(), "", "AAPL")[0].symbol, "AAPL");
        assert_eq!(filter_orders(rows, "", "MSFT").len(), 0);
    }

    #[test]
    fn the_payload_carries_every_key_and_counts() {
        let mut rows = Vec::new();
        apply_orders_message(&mut rows, Orders::OrderData(resting_sell_6_aapl()));
        apply_orders_message(
            &mut rows,
            Orders::OrderStatus(status_update(OrderStatusKind::Filled, 6.0, 0.0)),
        );
        let mut working_rows = rows.clone();
        working_rows.push(OpenOrderRow {
            order_id: 7002,
            status: "Submitted".to_string(),
            working: true,
            ..OpenOrderRow::empty()
        });

        let read = OpenOrdersRead {
            account_id: "DUK271345".to_string(),
            symbol: String::new(),
            orders: working_rows,
        };
        let payload = read.as_payload();

        assert_eq!(payload["success"], serde_json::json!(true));
        assert_eq!(payload["accountId"], serde_json::json!("DUK271345"));
        assert_eq!(payload["symbol"], serde_json::json!(""));
        assert_eq!(payload["count"], serde_json::json!(2));
        assert_eq!(payload["workingCount"], serde_json::json!(1));

        let row = &payload["orders"][0];
        let mut keys: Vec<&str> = row.as_object().unwrap().keys().map(|k| k.as_str()).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "accountId", "clientId", "conId", "filled", "lmtPrice", "orderId", "orderRef",
                "orderType", "outsideRth", "permId", "quantity", "remaining", "secType", "side",
                "status", "symbol", "tif", "working"
            ],
            "the D4 row carries exactly its eighteen keys"
        );
    }

    /// The defect D4 closes: a failed read is an error envelope, never
    /// `orders: []`. A disconnected bridge fails before any request.
    #[tokio::test]
    async fn a_failed_read_is_an_error_not_an_empty_list() {
        let client = IbkrClient::new(crate::config::Config::default().ibkr);
        let manager = OrderManager::new(client);
        let result = manager.open_orders("", "").await;
        assert!(
            matches!(result, Err(IbkrError::NotConnected)),
            "a dead link must read as an error, got {result:?}"
        );
    }
}
