
use std::sync::Arc;
use tracing::info;

use crate::ibkr::client::IbkrClient;
use crate::ibkr::error::IbkrError;

/// Account information
#[derive(Debug, Clone)]
pub struct AccountInfo {
    pub account_id: String,
    pub net_liquidation: f64,
    pub available_funds: f64,
    pub buying_power: f64,
    pub currency: String,
    pub daily_pnl: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
}

/// Position information
#[derive(Debug, Clone)]
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

/// Account manager
#[derive(Debug)]
pub struct AccountManager {
    client: Arc<IbkrClient>,
}

impl AccountManager {
    pub fn new(client: Arc<IbkrClient>) -> Self {
        Self { client }
    }

    /// Get list of managed accounts
    pub async fn list_accounts(&self) -> Result<Vec<String>, IbkrError> {
        let _client = self.client.get_client().await?;

        info!("Listing managed accounts");

        // TODO: Implement using ibapi v3 API
        // In ibapi v3, managed accounts come from the connection handshake
        // or can be requested explicitly

        Err(IbkrError::Unknown(
            "List accounts not yet implemented".to_string(),
        ))
    }

    /// Get account information
    pub async fn get_account_info(
        &self,
        account_id: Option<&str>,
    ) -> Result<AccountInfo, IbkrError> {
        let _client = self.client.get_client().await?;

        info!(
            account_id = account_id.unwrap_or("default"),
            "Fetching account info"
        );

        // TODO: Implement using ibapi v3's account_summary or account_updates
        // This requires subscribing to account updates and collecting values

        Err(IbkrError::Unknown(
            "Get account info not yet implemented".to_string(),
        ))
    }

    /// Get current positions
    pub async fn get_positions(
        &self,
        account_id: Option<&str>,
    ) -> Result<Vec<Position>, IbkrError> {
        let _client = self.client.get_client().await?;

        info!(
            account_id = account_id.unwrap_or("all"),
            "Fetching positions"
        );

        // TODO: Implement using ibapi v3's req_positions
        // This returns a subscription of Position updates

        Err(IbkrError::Unknown(
            "Get positions not yet implemented".to_string(),
        ))
    }
}
