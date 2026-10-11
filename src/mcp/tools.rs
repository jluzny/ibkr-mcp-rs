//! MCP tool definitions for the IBKR MCP server.
//!
//! Tools are registered via the `rmcp` macro-driven router. Read-only tools
//! cover quotes, historical/IV bars, scanners and Wall Street Horizon events;
//!
//! | Tool | Handler | Description |
//! |------|---------|-------------|
//! | `get_market_data` | `IbkrMcpServer::get_market_data` | Stock quotes with delayed-data fallback |
//! | `get_bulk_quotes` | `IbkrMcpServer::get_bulk_quotes` | Concurrent quotes for multiple symbols |
//! | `get_account_info` | `IbkrMcpServer::get_account_info` | Account summary (net liquidation, funds, PnL) |
//! | `get_positions` | `IbkrMcpServer::get_positions` | Current holdings per account |
//! | `get_connection_status` | `IbkrMcpServer::get_connection_status` | IBKR gateway connectivity |
//! | `get_open_orders` | `IbkrMcpServer::get_open_orders` | Orders the broker reports open (D4, read-only) |
//!
//! All tools return JSON strings that the MCP client receives as text content.
//! Error responses include `"success": false` and an `"error"` field.

use rmcp::{
    ServerHandler,
    handler::server::{
        router::tool::ToolRouter,
        wrapper::Parameters,
    },
    model::{ServerCapabilities, ServerInfo},
    tool, tool_router, tool_handler,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::warn;

use std::collections::HashMap;
use std::str::FromStr;

use ibapi::market_data::historical::{Duration as IbkrDuration, WhatToShow};

use crate::ibkr::client::IbkrClient;
use crate::ibkr::error::IbkrError;
use crate::ibkr::events::EventManager;
use crate::ibkr::history::{
    parse_bar_size, parse_date, parse_datetime, BarRequest, HistoricalDataManager, TickKind,
    TicksRequest,
};
use crate::ibkr::market_data::{MarketDataManager, QuoteSource};
use crate::ibkr::scanner::ScannerManager;
use crate::ibkr::account::AccountManager;
use crate::ibkr::orders::OrderManager;
use crate::ibkr::orders::WhatIfRequest;

/// MCP server state — holds all IBKR managers and routes tool calls.
///
/// Built via [`IbkrMcpServer::new`] with a shared [`IbkrClient`].
/// The [`Default`] implementation wires up a client from default config
/// and is mainly useful for testing.
#[derive(Debug, Clone)]
pub struct IbkrMcpServer {
    #[allow(dead_code)]
    tool_router: ToolRouter<Self>,
    market_data: Arc<MarketDataManager>,
    account: Arc<AccountManager>,
    #[allow(dead_code)]
    orders: Arc<OrderManager>,
    history: Arc<HistoricalDataManager>,
    scanner: Arc<ScannerManager>,
    events: Arc<EventManager>,
    client: Arc<IbkrClient>,
    /// The D5 flag gate — refuses `what_if` under an explicit `read_only` and a
    /// non-paper `account_id` on the account-scoped tools under an explicit
    /// `paper_trading` (see [`crate::policy`]).
    gate: Arc<crate::policy::OrderGate>,
}

impl IbkrMcpServer {
    pub fn new(client: Arc<IbkrClient>, config: Arc<crate::config::Config>) -> Self {
        let market_data = Arc::new(MarketDataManager::new(
            Arc::clone(&client),
            config.market_data.clone(),
        ));
        let account = Arc::new(AccountManager::new(Arc::clone(&client)));
        let orders = Arc::new(OrderManager::new(Arc::clone(&client)));
        let history = Arc::new(HistoricalDataManager::new(Arc::clone(&client)));
        let scanner = Arc::new(ScannerManager::new(Arc::clone(&client)));
        let events = Arc::new(EventManager::new(Arc::clone(&client)));
        let gate = Arc::new(crate::policy::OrderGate::from_config(&config));

        Self {
            tool_router: Self::tool_router(),
            market_data,
            account,
            orders,
            history,
            scanner,
            events,
            client,
            gate,
        }
    }

    /// Create with shared state managers so caches persist across sessions.
    pub fn new_shared(
        client: Arc<IbkrClient>,
        market_data: Arc<MarketDataManager>,
        account: Arc<AccountManager>,
        orders: Arc<OrderManager>,
        history: Arc<HistoricalDataManager>,
        scanner: Arc<ScannerManager>,
        events: Arc<EventManager>,
        gate: Arc<crate::policy::OrderGate>,
    ) -> Self {
        Self {
            tool_router: Self::tool_router(),
            market_data,
            account,
            orders,
            history,
            scanner,
            events,
            client,
            gate,
        }
    }
}

impl Default for IbkrMcpServer {
    fn default() -> Self {
        let config = crate::config::Config::default();
        Self::new(
            IbkrClient::new(config.ibkr.clone()),
            Arc::new(config),
        )
    }
}

#[tool_handler(router = Self::tool_router())]
impl ServerHandler for IbkrMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .build()
        )
        .with_server_info(rmcp::model::Implementation::new("ibkr-mcp-rs", "0.1.0"))
        .with_instructions(
            "Interactive Brokers MCP Server. Provides market data, account info, positions, and orders."
        )
    }
}

#[tool_router]
impl IbkrMcpServer {
    /// Get market data for a symbol (quote, historical, or option chain)
    #[tool(description = "Get market data for a symbol. Supports quotes, historical bars, and option chains.")]
    async fn get_market_data(
        &self,
        Parameters(params): Parameters<GetMarketDataParams>,
    ) -> String {
        let symbol = params.symbol.to_uppercase();

        match params.data_type.as_str() {
            "quote" => {
                match self.market_data.get_quote(&symbol).await {
                    Ok(quote) => {
                        let result = QuoteResult {
                            success: true,
                            symbol: quote.symbol,
                            data_type: "quote".to_string(),
                            bid: quote.bid,
                            ask: quote.ask,
                            last: quote.last,
                            volume: quote.volume,
                            high: quote.high,
                            low: quote.low,
                            close: quote.close,
                            source: match quote.source {
                                QuoteSource::RealTime => "realtime".to_string(),
                                QuoteSource::Delayed => "delayed".to_string(),
                                QuoteSource::Frozen => "frozen".to_string(),
                                QuoteSource::DelayedFrozen => "delayed_frozen".to_string(),
                                QuoteSource::Cache => "cache".to_string(),
                            },
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            delta: quote.delta,
                            gamma: quote.gamma,
                            theta: quote.theta,
                            vega: quote.vega,
                            rho: quote.rho,
                            implied_volatility: quote.implied_volatility,
                            error: None,
                        };
                        serde_json::to_string_pretty(&result)
                            .unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e))
                    }
                    Err(IbkrError::MarketDataSubscriptionRequired { code, message }) => {
                        warn!(symbol = %symbol, code = code, "Entitlement error");
                        serde_json::to_string_pretty(
                            &serde_json::json!({
                                "success": false,
                                "symbol": symbol,
                                "error": format!(
                                    "Market data subscription required (code {}): {}. Retry to use delayed data automatically.",
                                    code, message
                                ),
                            })
                        )
                        .unwrap_or_default()
                    }
                    Err(e) => {
                        serde_json::to_string_pretty(
                            &serde_json::json!({
                                "success": false,
                                "symbol": symbol,
                                "error": e.to_string(),
                            })
                        )
                        .unwrap_or_default()
                    }
                }
            }
            "historical" => {
                let period = params.period.clone().unwrap_or_else(|| "1 Y".to_string());

                match self.market_data.get_historical(&symbol, &period).await {
                    Ok(bars) => serde_json::to_string_pretty(
                        &serde_json::json!({
                            "success": true,
                            "symbol": symbol,
                            "data_type": "historical",
                            "period": period,
                            "count": bars.len(),
                            "bars": bars.iter().map(|b| serde_json::json!({
                                "timestamp": b.timestamp,
                                "open": b.open,
                                "high": b.high,
                                "low": b.low,
                                "close": b.close,
                                "volume": b.volume,
                            })).collect::<Vec<_>>(),
                            "note": "daily TRADES bars; use get_historical_data for IV history, other bar sizes or what_to_show",
                        })
                    )
                    .unwrap_or_default(),
                    Err(e) => {
                        warn!(symbol = %symbol, error = %e, "Historical data request failed");
                        serde_json::to_string_pretty(
                            &serde_json::json!({
                                "success": false,
                                "symbol": symbol,
                                "error": e.to_string(),
                            })
                        )
                        .unwrap_or_default()
                    }
                }
            }
            _ => {
                serde_json::to_string_pretty(
                    &serde_json::json!({
                        "success": false,
                        "error": format!("Unsupported data_type: {}", params.data_type),
                    })
                )
                .unwrap_or_default()
            }
        }
    }

    /// Get quotes for multiple symbols concurrently
    #[tool(description = "Get market quotes for multiple symbols concurrently. Much faster than individual calls.")]
    async fn get_bulk_quotes(
        &self,
        Parameters(params): Parameters<GetBulkQuotesParams>,
    ) -> String {
        use crate::ibkr::market_data::MarketDataManager;

        let symbols: Vec<String> = params
            .symbols
            .iter()
            .map(|s| s.to_uppercase())
            .collect();

        let results = MarketDataManager::get_bulk_quotes(
            Arc::clone(&self.market_data),
            &symbols,
        )
        .await;

        let quotes: Vec<serde_json::Value> = results
            .into_iter()
            .map(|(sym, result)| match result {
                Ok(quote) => serde_json::json!({
                    "success": true,
                    "symbol": sym,
                    "data_type": "quote",
                    "bid": quote.bid,
                    "ask": quote.ask,
                    "last": quote.last,
                    "volume": quote.volume,
                    "high": quote.high,
                    "low": quote.low,
                    "close": quote.close,
                    "source": match quote.source {
                        QuoteSource::RealTime => "realtime",
                        QuoteSource::Delayed => "delayed",
                        QuoteSource::Frozen => "frozen",
                        QuoteSource::DelayedFrozen => "delayed_frozen",
                        QuoteSource::Cache => "cache",
                    },
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "delta": quote.delta,
                    "gamma": quote.gamma,
                    "theta": quote.theta,
                    "vega": quote.vega,
                    "rho": quote.rho,
                    "implied_volatility": quote.implied_volatility,
                }),
                Err(e) => serde_json::json!({
                    "success": false,
                    "symbol": sym,
                    "error": e.to_string(),
                }),
            })
            .collect();

        serde_json::to_string_pretty(&quotes).unwrap_or_default()
    }

    /// Get account information
    #[tool(description = "Get account information including net liquidation, available funds, buying power, and daily PnL.")]
    async fn get_account_info(
        &self,
        Parameters(params): Parameters<GetAccountInfoParams>,
    ) -> String {
        if let Some(refusal) = self.gate.refusal_for("get_account_info", params.account_id.as_deref()) {
            return error_json(refusal);
        }
        let account_id = params.account_id.as_deref();

        match self.account.get_account_info(account_id).await {
            Ok(info) => {
                serde_json::to_string_pretty(
                    &serde_json::json!({
                        "success": true,
                        "accountId": info.account_id,
                        "netLiquidation": info.net_liquidation,
                        "availableFunds": info.available_funds,
                        "excessLiquidity": info.excess_liquidity,
                        "buyingPower": info.buying_power,
                        "currency": info.currency,
                        "dailyPnL": info.daily_pnl,
                    })
                )
                .unwrap_or_default()
            }
            Err(e) => {
                serde_json::to_string_pretty(
                    &serde_json::json!({
                        "success": false,
                        "error": e.to_string(),
                    })
                )
                .unwrap_or_default()
            }
        }
    }

    /// Get current positions
    #[tool(description = "Get current positions for an account or all accounts.")]
    async fn get_positions(
        &self,
        Parameters(params): Parameters<GetPositionsParams>,
    ) -> String {
        if let Some(refusal) = self.gate.refusal_for("get_positions", params.account_id.as_deref()) {
            return error_json(refusal);
        }
        let account_id = params.account_id.as_deref();

        match self.account.get_positions(account_id).await {
            Ok(positions) => {
                serde_json::to_string_pretty(
                    &serde_json::json!({
                        "success": true,
                        "positions": positions.iter().map(|p| serde_json::json!({
                            "symbol": p.symbol,
                            "quantity": p.quantity,
                            "averageCost": p.average_cost,
                            "contractId": p.contract_id,
                            "marketPrice": p.market_price,
                            "marketValue": p.market_value,
                            "unrealizedPnL": p.unrealized_pnl,
                            "dailyPnL": p.daily_pnl,
                            "securityType": p.security_type,
                            "strike": p.strike,
                            "right": p.right,
                            "expiration": p.expiration,
                            "multiplier": p.multiplier,
                        })).collect::<Vec<_>>(),
                    })
                )
                .unwrap_or_default()
            }
            Err(e) => {
                serde_json::to_string_pretty(
                    &serde_json::json!({"success": false, "error": e.to_string()})
                )
                .unwrap_or_default()
            }
        }
    }

    /// Get historical executions (fills) with realized P&L
    #[tool(description = "Get historical trade executions (fills) with realized P&L and commissions. Filter by account, symbol, or time range.")]
    async fn get_executions(
        &self,
        Parameters(params): Parameters<GetExecutionsParams>,
    ) -> String {
        if let Some(refusal) = self.gate.refusal_for("get_executions", params.account_id.as_deref()) {
            return error_json(refusal);
        }
        let account_id = params.account_id.as_deref();
        let symbol = params.symbol.as_deref();
        let since = params.since.as_deref();

        match self.account.get_executions(account_id, symbol, since).await {
            Ok(executions) => {
                let total_pnl: f64 = executions.iter().map(|e| e.realized_pnl).sum();
                let total_commission: f64 = executions.iter().map(|e| e.commission).sum();
                serde_json::to_string_pretty(
                    &serde_json::json!({
                        "success": true,
                        "totalRealizedPnL": total_pnl,
                        "totalCommission": total_commission,
                        "netPnL": total_pnl - total_commission,
                        "count": executions.len(),
                        "executions": executions.iter().map(|e| serde_json::json!({
                            "executionId": e.execution_id,
                            "symbol": e.symbol,
                            "securityType": e.security_type,
                            "side": e.side,
                            "quantity": e.quantity,
                            "price": e.price,
                            "commission": e.commission,
                            "realizedPnL": e.realized_pnl,
                            "time": e.time,
                            "accountId": e.account_id,
                            "strike": e.strike,
                            "right": e.right,
                            "expiration": e.expiration,
                            "multiplier": e.multiplier,
                        })).collect::<Vec<_>>(),
                    })
                )
                .unwrap_or_default()
            }
            Err(e) => {
                serde_json::to_string_pretty(
                    &serde_json::json!({"success": false, "error": e.to_string()})
                )
                .unwrap_or_default()
            }
        }
    }

    /// Get IBKR connection status
    ///
    /// Always answers promptly, including while the broker is unreachable: this
    /// is the cheap "is the broker up?" probe for cron jobs. `brokerConnected:
    /// false` is a normal answer, not an error — the bridge stays up and
    /// reconnects on its own (see docs/broker-outage-policy.md).
    #[tool(description = "Check if the IBKR broker connection is active.")]
    async fn get_connection_status(
        &self,
        _params: Parameters<GetConnectionStatusParams>,
    ) -> String {
        let connected = self.client.is_connected().await;
        let consecutive_failures = self.client.broker_failures();
        let outage_secs = self.client.broker_outage_secs();
        serde_json::to_string_pretty(
            &serde_json::json!({
                "success": true,
                "brokerConnected": connected,
                "consecutiveFailures": consecutive_failures,
                "outageSeconds": outage_secs,
                "message": if connected {
                    "connected to IBKR"
                } else {
                    "not connected to IBKR — the bridge is up and reconnecting in the background; broker-backed tools will fail until the gateway is logged in"
                },
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })
        )
        .unwrap_or_default()
    }

    /// Get option chain for a symbol
    #[tool(description = "Get the option chain for a stock symbol — returns all available expirations and strikes via IBKR's sec_def_opt_params.")]
    async fn get_option_chain(
        &self,
        Parameters(params): Parameters<GetOptionChainParams>,
    ) -> String {
        match self.market_data.get_option_chain(&params.symbol).await {
            Ok(chain) => {
                let result = serde_json::json!({
                    "success": true,
                    "symbol": params.symbol,
                    "exchange": chain.exchange,
                    "underlying_contract_id": chain.underlying_contract_id,
                    "trading_class": chain.trading_class,
                    "multiplier": chain.multiplier,
                    "expirations": chain.expirations,
                    "strikes": chain.strikes,
                });
                serde_json::to_string_pretty(&result).unwrap_or_default()
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

    /// Get quote for an option contract
    #[tool(description = "Get a market data quote for a specific option contract. Specify underlying symbol, strike, expiration (YYYYMMDD), and right (C or P). Returns bid/ask/last price plus greeks (delta, gamma, theta, vega, rho) and implied volatility where available.")]
    async fn get_option_quote(
        &self,
        Parameters(params): Parameters<GetOptionQuoteParams>,
    ) -> String {
        let symbol = params.symbol.to_uppercase();
        let right = params.right.to_uppercase();

        // Parse expiration date YYYYMMDD
        let (year, month, day) = match params.expiration.len() {
            8 => {
                let y: i32 = params.expiration[0..4].parse().unwrap_or(0);
                let m: u32 = params.expiration[4..6].parse().unwrap_or(0);
                let d: u32 = params.expiration[6..8].parse().unwrap_or(0);
                (y, m, d)
            }
            _ => {
                return serde_json::to_string_pretty(
                    &serde_json::json!({
                        "success": false,
                        "error": format!("Invalid expiration format '{}'. Use YYYYMMDD.", params.expiration),
                    })
                )
                .unwrap_or_default();
            }
        };

        let contract = match right.as_str() {
            "C" => crate::ibkr::contract::option_call(&symbol, params.strike, year, month, day),
            "P" => crate::ibkr::contract::option_put(&symbol, params.strike, year, month, day),
            _ => {
                return serde_json::to_string_pretty(
                    &serde_json::json!({
                        "success": false,
                        "error": format!("Invalid right '{}'. Use 'C' for Call or 'P' for Put.", params.right),
                    })
                )
                .unwrap_or_default();
            }
        };

        let label = format!("{} {} {}{}", symbol, params.expiration, right, params.strike);

        match self.market_data.get_option_quote(&contract, &label).await {
            Ok(quote) => {
                let result = serde_json::json!({
                    "success": true,
                    "symbol": symbol,
                    "expiration": params.expiration,
                    "right": right,
                    "strike": params.strike,
                    "bid": quote.bid,
                    "ask": quote.ask,
                    "last": quote.last,
                    "volume": quote.volume,
                    "high": quote.high,
                    "low": quote.low,
                    "close": quote.close,
                    "source": match quote.source {
                        QuoteSource::RealTime => "realtime",
                        QuoteSource::Delayed => "delayed",
                        QuoteSource::Frozen => "frozen",
                        QuoteSource::DelayedFrozen => "delayed_frozen",
                        QuoteSource::Cache => "cache",
                    },
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "delta": quote.delta,
                    "gamma": quote.gamma,
                    "theta": quote.theta,
                    "vega": quote.vega,
                    "rho": quote.rho,
                    "implied_volatility": quote.implied_volatility,
                    "underlying_price": quote.underlying_price,
                });
                serde_json::to_string_pretty(&result).unwrap_or_default()
            }
            Err(e) => {
                serde_json::to_string_pretty(
                    &serde_json::json!({
                        "success": false,
                        "symbol": symbol,
                        "expiration": params.expiration,
                        "right": right,
                        "strike": params.strike,
                        "error": e.to_string(),
                    })
                )
                .unwrap_or_default()
            }
        }
    }

    /// Preview margin/equity/commission impact without executing
    #[tool(description = "Preview margin, equity, and commission impact of a hypothetical order without executing it. Use to check if a sale will release maintenance margin before committing.")]
    async fn what_if(
        &self,
        Parameters(params): Parameters<WhatIfParams>,
    ) -> String {
        if let Some(refusal) = self.gate.refusal_for("what_if", params.account_id.as_deref()) {
            return error_json(refusal);
        }
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
    /// Get historical bars (OHLCV) for a symbol
    #[tool(description = "Get historical bar data (OHLCV) for a stock symbol. what_to_show: TRADES, MIDPOINT, BID, ASK, BID_ASK, AGGTRADES, HISTORICAL_VOLATILITY, OPTION_IMPLIED_VOLATILITY, ADJUSTED_LAST. For IV history use bar_size='1 day' with what_to_show='OPTION_IMPLIED_VOLATILITY'. Option price history is NOT available from IBKR.")]
    async fn get_historical_data(
        &self,
        Parameters(params): Parameters<GetHistoricalDataParams>,
    ) -> String {
        let symbol = params.symbol.to_uppercase();

        let duration = match IbkrDuration::from_str(params.duration.trim()) {
            Ok(d) => d,
            Err(_) => {
                return error_json(format!(
                    "invalid duration '{}' — expected an IBKR duration like '1 Y', '6 M', '30 D'",
                    params.duration
                ))
            }
        };

        let bar_size = match parse_bar_size(&params.bar_size) {
            Ok(b) => b,
            Err(e) => return error_json(e),
        };

        let what_to_show = match WhatToShow::from_str(params.what_to_show.trim()) {
            Ok(w) => w,
            Err(_) => {
                return error_json(format!(
                    "invalid what_to_show '{}' — expected TRADES, MIDPOINT, BID, ASK, BID_ASK, \
                     AGGTRADES, HISTORICAL_VOLATILITY, OPTION_IMPLIED_VOLATILITY or ADJUSTED_LAST",
                    params.what_to_show
                ))
            }
        };

        let end = match parse_datetime(&params.end_datetime) {
            Ok(e) => e,
            Err(e) => return error_json(e),
        };

        let request = BarRequest {
            duration,
            bar_size,
            what_to_show,
            use_rth: params.use_rth,
            end,
        };

        match self.history.get_historical_data(&symbol, &request).await {
            Ok(series) => serde_json::to_string_pretty(&serde_json::json!({
                "success": true,
                "symbol": symbol,
                "whatToShow": what_to_show.to_string(),
                "barSize": bar_size.to_string(),
                "duration": duration.to_string(),
                "useRth": params.use_rth,
                "count": series.bars.len(),
                "firstDate": series.first_date,
                "lastDate": series.last_date,
                "bars": series.bars,
            }))
            .unwrap_or_default(),
            Err(e) => {
                warn!(symbol = %symbol, error = %e, "Historical data request failed");
                error_json(format!("historical data for {symbol} failed: {e}"))
            }
        }
    }

    /// Get IV rank / IV percentile for a symbol
    #[tool(description = "Get IV rank and IV percentile for a symbol, computed from 1 year of daily OPTION_IMPLIED_VOLATILITY bars (stocks/ETFs/indices only — IBKR does not serve IV history for option contracts). window (default 252) is the maximum number of daily observations used.")]
    async fn get_iv_rank(
        &self,
        Parameters(params): Parameters<GetIvRankParams>,
    ) -> String {
        let symbol = params.symbol.to_uppercase();

        let window = match params.window.trim().parse::<usize>() {
            Ok(w) if w > 0 => w,
            _ => {
                return error_json(format!(
                    "invalid window '{}' — expected a positive integer, e.g. 252",
                    params.window
                ))
            }
        };

        match self.history.get_iv_rank(&symbol, window).await {
            Ok((stats, first_date, last_date)) => {
                serde_json::to_string_pretty(&serde_json::json!({
                    "success": true,
                    "symbol": symbol,
                    "iv": stats.iv,
                    "iv_rank_pct": stats.iv_rank_pct,
                    "iv_percentile": stats.iv_percentile,
                    "iv_52w_low": stats.iv_52w_low,
                    "iv_52w_high": stats.iv_52w_high,
                    "bars": stats.bars,
                    "windowRequested": stats.window_requested,
                    "firstDate": first_date,
                    "lastDate": last_date,
                }))
                .unwrap_or_default()
            }
            Err(e) => {
                warn!(symbol = %symbol, error = %e, "IV rank request failed");
                error_json(format!("IV rank for {symbol} failed: {e}"))
            }
        }
    }

    /// Run an IBKR market scanner
    #[tool(description = "Run an IBKR market scanner and return ranked symbols. Max 50 rows per scan, max 10 concurrent scans. Scanner rows carry NO price data — fetch quotes separately (get_bulk_quotes / get_option_quote). Example scan codes: HOT_BY_OPT_VOLUME, HIGH_OPT_IMP_VOLAT_OVER_HIST, MOST_ACTIVE, TOP_PERC_GAIN. For multi-leg trades on one underlying: instrument='NATCOMB', location='NATCOMB.OPT.US', scan_code='COMBO_LATEST_TRADE', tag_filters={'underConID': '<conid>'}.")]
    async fn run_scanner(
        &self,
        Parameters(params): Parameters<RunScannerParams>,
    ) -> String {
        match self
            .scanner
            .run_scan(
                &params.scan_code,
                &params.instrument,
                &params.location,
                params.rows,
                &params.tag_filters,
            )
            .await
        {
            Ok(result) => serde_json::to_string_pretty(&serde_json::json!({
                "success": true,
                "scanCode": params.scan_code,
                "instrument": params.instrument,
                "location": params.location,
                "requestedRows": params.rows,
                "rowsReturned": result.rows.len(),
                "note": "scanner rows carry no market-data fields — fetch prices separately (e.g. get_bulk_quotes)",
                "notices": result.notices,
                "rows": result.rows,
            }))
            .unwrap_or_default(),
            Err(e) => {
                warn!(scan_code = %params.scan_code, error = %e, "Scanner request failed");
                error_json(format!("scanner '{}' failed: {e}", params.scan_code))
            }
        }
    }

    /// Get Wall Street Horizon event data
    #[tool(description = "Get Wall Street Horizon event data (earnings / corporate events) for a symbol or contract id. Always reports entitlement explicitly ('entitlement' + 'status' + 'message') instead of returning a silent empty list. Requires a Wall Street Horizon subscription on the IBKR account.")]
    async fn get_events(
        &self,
        Parameters(params): Parameters<GetEventsParams>,
    ) -> String {
        let start = if params.start_date.trim().is_empty() {
            None
        } else {
            match parse_date(&params.start_date) {
                Ok(d) => Some(d),
                Err(e) => return error_json(e),
            }
        };
        let end = if params.end_date.trim().is_empty() {
            None
        } else {
            match parse_date(&params.end_date) {
                Ok(d) => Some(d),
                Err(e) => return error_json(e),
            }
        };

        match self
            .events
            .get_events(params.symbol.as_deref(), params.conid, start, end)
            .await
        {
            Ok(result) => {
                let mut value = serde_json::to_value(&result).unwrap_or_default();
                if let Some(obj) = value.as_object_mut() {
                    obj.insert("success".to_string(), serde_json::json!(true));
                    obj.insert(
                        "symbol".to_string(),
                        serde_json::json!(params.symbol.clone().unwrap_or_default().to_uppercase()),
                    );
                }
                serde_json::to_string_pretty(&value).unwrap_or_default()
            }
            Err(e) => {
                warn!(error = %e, "WSH event request failed");
                error_json(format!("WSH event data request failed: {e}"))
            }
        }
    }

    /// Get historical trade prints (ticks)
    #[tool(description = "Get historical tick data (trade prints) for a symbol. tick_type: TRADES, MIDPOINT or BID_ASK. Max 1000 ticks; at least one of start_datetime/end_datetime must be set (IBKR requires a time anchor). Entitlement/permission gaps are reported explicitly. IBKR does not serve option price history — use stock/ETF symbols.")]
    async fn get_historical_ticks(
        &self,
        Parameters(params): Parameters<GetHistoricalTicksParams>,
    ) -> String {
        let symbol = params.symbol.to_uppercase();

        let kind = match TickKind::parse(&params.tick_type) {
            Some(k) => k,
            None => {
                return error_json(format!(
                    "invalid tick_type '{}' — expected TRADES, MIDPOINT or BID_ASK",
                    params.tick_type
                ))
            }
        };

        let start = match parse_datetime(&params.start_datetime) {
            Ok(v) => v,
            Err(e) => return error_json(e),
        };
        let end = match parse_datetime(&params.end_datetime) {
            Ok(v) => v,
            Err(e) => return error_json(e),
        };

        if start.is_none() && end.is_none() {
            return error_json(
                "provide start_datetime and/or end_datetime — IBKR requires a time anchor for historical ticks",
            );
        }

        let request = TicksRequest {
            symbol: symbol.clone(),
            number_of_ticks: params.number_of_ticks,
            kind,
            start,
            end,
            use_rth: params.use_rth,
        };

        match self.history.get_historical_ticks(&request).await {
            Ok(ticks) => {
                let count = ticks.len();
                serde_json::to_string_pretty(&serde_json::json!({
                    "success": count > 0,
                    "symbol": symbol,
                    "tickType": kind.as_str(),
                    "count": count,
                    "status": if count > 0 { "ok" } else { "no_data" },
                    "note": if count > 0 {
                        ""
                    } else {
                        "no ticks returned — historical ticks need a market-data subscription, and IBKR does not serve option price history at all"
                    },
                    "ticks": ticks,
                }))
                .unwrap_or_default()
            }
            Err(e) => {
                warn!(symbol = %symbol, error = %e, "Historical ticks request failed");
                error_json(format!("historical ticks for {symbol} failed: {e}"))
            }
        }
    }

    /// Get the orders the broker currently reports as open (the D4 read)
    ///
    /// Strictly read-only: it can never place, modify or cancel an order. A
    /// failed read is an error envelope, never an empty list — `orders: []` is
    /// what an empty book looks like.
    #[tool(description = "Get the orders the broker currently reports as open, with order id, symbol/conId, side, quantity, limit price, time-in-force, status, filled/remaining, outside-RTH flag and order ref. STRICTLY READ-ONLY — it can never place, modify or cancel an order. Covers orders placed through the TWS API by any client id, including a client that has since disconnected; orders placed manually in TWS are not covered. A failed read is an error, never an empty list.")]
    async fn get_open_orders(
        &self,
        Parameters(params): Parameters<GetOpenOrdersParams>,
    ) -> String {
        if let Some(refusal) = self.gate.refusal_for("get_open_orders", params.account_id.as_deref()) {
            return error_json(refusal);
        }
        let account_id = params.account_id.as_deref().unwrap_or("");
        let symbol = params.symbol.as_deref().unwrap_or("");

        match self.orders.open_orders(account_id, symbol).await {
            Ok(read) => serde_json::to_string_pretty(&read.as_payload()).unwrap_or_default(),
            Err(e) => error_json(e.to_string()),
        }
    }
}

fn default_mkt() -> String {
    "MKT".to_string()
}

fn default_quote() -> String {
    "quote".to_string()
}

fn default_duration() -> String {
    "1 Y".to_string()
}

fn default_bar_size() -> String {
    "1 day".to_string()
}

fn default_what_to_show() -> String {
    "TRADES".to_string()
}

fn default_window() -> String {
    "252".to_string()
}

fn default_instrument() -> String {
    "STK".to_string()
}

fn default_location() -> String {
    "STK.US.MAJOR".to_string()
}

fn default_rows() -> i32 {
    25
}

fn default_ticks() -> i32 {
    100
}

fn default_tick_type() -> String {
    "TRADES".to_string()
}

fn default_true() -> bool {
    true
}

/// Shared error envelope for the historical / scanner / event tools.
fn error_json(message: impl std::fmt::Display) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "success": false,
        "error": message.to_string(),
    }))
    .unwrap_or_default()
}

// ============================================================================
// Parameter types
// ============================================================================

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetMarketDataParams {
    #[schemars(description = "Stock symbol, e.g. AAPL, AMD")]
    pub symbol: String,

    #[schemars(description = "Type of data: 'quote', 'historical', or 'option_chain'. Defaults to 'quote'.")]
    #[serde(default = "default_quote")]
    pub data_type: String,

    #[schemars(description = "For historical data: time period like '1 D', '1 W'. Optional.")]
    #[serde(default)]
    pub period: Option<String>,

    #[schemars(description = "For option chains: expiration date YYYYMMDD. Optional.")]
    #[serde(default)]
    pub expiration: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetAccountInfoParams {
    #[schemars(description = "Account ID. Optional — uses default if omitted.")]
    #[serde(default)]
    pub account_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetPositionsParams {
    #[schemars(description = "Account ID. Optional — returns all positions if omitted.")]
    #[serde(default)]
    pub account_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetConnectionStatusParams {}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetBulkQuotesParams {
    #[schemars(description = "List of stock symbols, e.g. ['AAPL', 'AMD', 'TSLA']")]
    pub symbols: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetExecutionsParams {
    #[schemars(description = "Account ID. Optional — returns all executions if omitted.")]
    #[serde(default)]
    pub account_id: Option<String>,

    #[schemars(description = "Symbol filter. Optional — returns all symbols if omitted.")]
    #[serde(default)]
    pub symbol: Option<String>,

    #[schemars(description = "Time filter — returns executions since this time. Format: YYYYMMDD (e.g. \"20260521\") or YYYYMMDD-HH:MM:SS in UTC (e.g. \"20260521-15:30:00\"). Optional.")]
    #[serde(default)]
    pub since: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetOpenOrdersParams {
    #[schemars(description = "Account ID. Optional — returns open orders for all accounts if omitted.")]
    #[serde(default)]
    pub account_id: Option<String>,

    #[schemars(description = "Symbol filter, e.g. AAPL. Optional — returns all symbols if omitted.")]
    #[serde(default)]
    pub symbol: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetOptionChainParams {
    #[schemars(description = "Stock symbol, e.g. SPY, AAPL, AMD")]
    pub symbol: String,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetOptionQuoteParams {
    #[schemars(description = "Underlying stock symbol, e.g. SPY, AAPL")]
    pub symbol: String,

    #[schemars(description = "Expiration date in YYYYMMDD format, e.g. '20250618'")]
    pub expiration: String,

    #[schemars(description = "Strike price, e.g. 200.0")]
    pub strike: f64,

    #[schemars(description = "Option right: 'C' for Call or 'P' for Put")]
    pub right: String,
}

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

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetHistoricalDataParams {
    #[schemars(description = "Stock symbol, e.g. WOLF, AAPL")]
    pub symbol: String,

    #[schemars(description = "IBKR duration string: '<n> S|D|W|M|Y', e.g. '1 Y', '6 M', '30 D'. Defaults to '1 Y'.")]
    #[serde(default = "default_duration")]
    pub duration: String,

    #[schemars(description = "Bar size: '1 day', '1 hour', '15 mins', '5 secs', '1 week', '1 month'. Defaults to '1 day'.")]
    #[serde(default = "default_bar_size")]
    pub bar_size: String,

    #[schemars(description = "Data type: TRADES (default), MIDPOINT, BID, ASK, BID_ASK, AGGTRADES, HISTORICAL_VOLATILITY, OPTION_IMPLIED_VOLATILITY, ADJUSTED_LAST.")]
    #[serde(default = "default_what_to_show")]
    pub what_to_show: String,

    #[schemars(description = "Regular trading hours only. Defaults to true.")]
    #[serde(default = "default_true")]
    pub use_rth: bool,

    #[schemars(description = "End of the requested window: '' (now), 'YYYYMMDD', 'YYYY-MM-DD HH:MM:SS' or RFC3339 ('2026-09-16T20:00:00Z'). Optional.")]
    #[serde(default)]
    pub end_datetime: String,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetIvRankParams {
    #[schemars(description = "Stock symbol, e.g. WOLF, AAPL")]
    pub symbol: String,

    #[schemars(description = "Maximum number of trailing daily IV observations to use. Defaults to '252'.")]
    #[serde(default = "default_window")]
    pub window: String,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RunScannerParams {
    #[schemars(description = "Scan code, e.g. HOT_BY_OPT_VOLUME, HIGH_OPT_IMP_VOLAT_OVER_HIST, MOST_ACTIVE, TOP_PERC_GAIN, COMBO_LATEST_TRADE.")]
    pub scan_code: String,

    #[schemars(description = "Instrument type: 'STK' (default), 'NATCOMB' for multi-leg combos.")]
    #[serde(default = "default_instrument")]
    pub instrument: String,

    #[schemars(description = "Location code: 'STK.US.MAJOR' (default), 'NATCOMB.OPT.US' for combo scans.")]
    #[serde(default = "default_location")]
    pub location: String,

    #[schemars(description = "Rows to return, 1-50. IBKR rejects more than 50. Defaults to 25.")]
    #[serde(default = "default_rows")]
    pub rows: i32,

    #[schemars(description = "Scanner filters as a string map, e.g. {'underConID': '12345'} for NATCOMB combo scans. Optional.")]
    #[serde(default)]
    pub tag_filters: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetEventsParams {
    #[schemars(description = "Stock symbol, e.g. WOLF. Provide this or conid.")]
    #[serde(default)]
    pub symbol: Option<String>,

    #[schemars(description = "IBKR contract id (conid). Takes precedence over symbol. Optional.")]
    #[serde(default)]
    pub conid: Option<i32>,

    #[schemars(description = "Start date 'YYYYMMDD' or 'YYYY-MM-DD'. Optional.")]
    #[serde(default)]
    pub start_date: String,

    #[schemars(description = "End date 'YYYYMMDD' or 'YYYY-MM-DD'. Optional.")]
    #[serde(default)]
    pub end_date: String,
}

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetHistoricalTicksParams {
    #[schemars(description = "Stock symbol, e.g. AAPL")]
    pub symbol: String,

    #[schemars(description = "Number of ticks to return, 1-1000. Defaults to 100.")]
    #[serde(default = "default_ticks")]
    pub number_of_ticks: i32,

    #[schemars(description = "Tick type: TRADES (default), MIDPOINT or BID_ASK.")]
    #[serde(default = "default_tick_type")]
    pub tick_type: String,

    #[schemars(description = "Start of the window: 'YYYYMMDD HH:MM:SS' or RFC3339. Optional (but see end_datetime).")]
    #[serde(default)]
    pub start_datetime: String,

    #[schemars(description = "End of the window: 'YYYYMMDD HH:MM:SS' or RFC3339. Optional (but see start_datetime).")]
    #[serde(default)]
    pub end_datetime: String,

    #[schemars(description = "Regular trading hours only. Defaults to true.")]
    #[serde(default = "default_true")]
    pub use_rth: bool,
}

// ============================================================================
// Result types
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct QuoteResult {
    pub success: bool,
    pub symbol: String,
    pub data_type: String,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    pub volume: i64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub source: String,
    pub timestamp: String,
    pub delta: Option<f64>,
    pub gamma: Option<f64>,
    pub theta: Option<f64>,
    pub vega: Option<f64>,
    pub rho: Option<f64>,
    pub implied_volatility: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The parity pin for the tool surface (`ibkr-mcp-py/docs/DEVIATIONS.md`
    /// §D4): the captured fourteen plus `get_open_orders`, in the order the
    /// server actually emits (rmcp lists alphabetically — the order recorded in
    /// the `rust_paper/tools_list.json` capture, the PARITY §1 schema source).
    /// The list a client sees must not drift silently.
    #[test]
    fn the_tool_surface_is_the_captured_fourteen_plus_get_open_orders() {
        let names: Vec<String> = IbkrMcpServer::default()
            .tool_router
            .list_all()
            .into_iter()
            .map(|tool| tool.name.into_owned())
            .collect();
        assert_eq!(
            names,
            vec![
                "get_account_info",
                "get_bulk_quotes",
                "get_connection_status",
                "get_events",
                "get_executions",
                "get_historical_data",
                "get_historical_ticks",
                "get_iv_rank",
                "get_market_data",
                "get_open_orders",
                "get_option_chain",
                "get_option_quote",
                "get_positions",
                "run_scanner",
                "what_if",
            ],
            "the tool list is the captured fourteen plus get_open_orders"
        );
    }
}