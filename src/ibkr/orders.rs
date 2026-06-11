
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

    /// List open orders
    pub async fn list_orders(&self,
    ) -> Result<Vec<Order>, IbkrError> {
        let _client = self.client.get_client().await?;

        info!("Listing open orders");

        Err(IbkrError::Unknown(
            "List orders not yet implemented".to_string(),
        ))
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
