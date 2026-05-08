use rmcp::{
    ServerHandler,
    handler::server::{
        router::tool::ToolRouter,
        wrapper::Parameters,
    },
    model::{ServerCapabilities, ServerInfo},
    tool, tool_router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::warn;

use crate::ibkr::client::IbkrClient;
use crate::ibkr::error::IbkrError;
use crate::ibkr::market_data::{MarketDataManager, QuoteSource};
use crate::ibkr::account::AccountManager;
use crate::ibkr::orders::OrderManager;

/// MCP server state — holds all IBKR managers
#[derive(Debug, Clone)]
pub struct IbkrMcpServer {
    #[allow(dead_code)]
    tool_router: ToolRouter<Self>,
    market_data: Arc<MarketDataManager>,
    account: Arc<AccountManager>,
    #[allow(dead_code)]
    orders: Arc<OrderManager>,
    client: Arc<IbkrClient>,
}

impl IbkrMcpServer {
    pub fn new(client: Arc<IbkrClient>) -> Self {
        let market_data = Arc::new(MarketDataManager::new(
            Arc::clone(&client),
            crate::config::Config::default().market_data,
        ));
        let account = Arc::new(AccountManager::new(Arc::clone(&client)));
        let orders = Arc::new(OrderManager::new(Arc::clone(&client)));

        Self {
            tool_router: Self::tool_router(),
            market_data,
            account,
            orders,
            client,
        }
    }
}

impl Default for IbkrMcpServer {
    fn default() -> Self {
        Self::new(IbkrClient::new(
            crate::config::Config::default().ibkr
        ))
    }
}

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
                                QuoteSource::Cache => "cache".to_string(),
                            },
                            timestamp: chrono::Utc::now().to_rfc3339(),
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

    /// Get account information
    #[tool(description = "Get account information including net liquidation, available funds, buying power, and daily PnL.")]
    async fn get_account_info(
        &self,
        Parameters(params): Parameters<GetAccountInfoParams>,
    ) -> String {
        let account_id = params.account_id.as_deref();

        match self.account.get_account_info(account_id).await {
            Ok(info) => {
                serde_json::to_string_pretty(
                    &serde_json::json!({
                        "success": true,
                        "accountId": info.account_id,
                        "netLiquidation": info.net_liquidation,
                        "availableFunds": info.available_funds,
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
                            "marketPrice": p.market_price,
                            "marketValue": p.market_value,
                            "unrealizedPnL": p.unrealized_pnl,
                            "dailyPnL": p.daily_pnl,
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
    #[tool(description = "Check if the IBKR broker connection is active.")]
    async fn get_connection_status(
        &self,
        _params: Parameters<GetConnectionStatusParams>,
    ) -> String {
        let connected = self.client.is_connected().await;
        serde_json::to_string_pretty(
            &serde_json::json!({
                "success": true,
                "brokerConnected": connected,
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })
        )
        .unwrap_or_default()
    }
}

// ============================================================================
// Parameter types
// ============================================================================

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct GetMarketDataParams {
    #[schemars(description = "Stock symbol, e.g. AAPL, AMD")]
    pub symbol: String,

    #[schemars(description = "Type of data: 'quote', 'historical', or 'option_chain'")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
