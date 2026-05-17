use ibapi::accounts::{AccountSummaryResult, AccountSummaryTags, PositionUpdate};
use ibapi::accounts::types::AccountGroup;
use ibapi::contracts::SecurityType;
use ibapi::subscriptions::SubscriptionItemStreamExt;
use futures::StreamExt;
use tokio::time::{timeout, Duration};
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
    pub security_type: String,
    pub strike: Option<f64>,
    pub right: Option<String>,
    pub expiration: Option<String>,
    pub multiplier: Option<String>,
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
        Err(IbkrError::Unknown(
            "List accounts not yet implemented".to_string(),
        ))
    }

    /// Get account information
    pub async fn get_account_info(
        &self,
        account_id: Option<&str>,
    ) -> Result<AccountInfo, IbkrError> {
        let client = self.client.get_client().await?;
        let target_account = account_id.map(|s| s.to_string());

        info!(
            account_id = account_id.unwrap_or("default"),
            "Fetching account info"
        );

        let tags = &[
            AccountSummaryTags::NET_LIQUIDATION,
            AccountSummaryTags::AVAILABLE_FUNDS,
            AccountSummaryTags::BUYING_POWER,
            AccountSummaryTags::TOTAL_CASH_VALUE,
            AccountSummaryTags::GROSS_POSITION_VALUE,
            AccountSummaryTags::EQUITY_WITH_LOAN_VALUE,
        ];

        let mut subscription = client
            .account_summary(&AccountGroup("All".to_string()), tags)
            .await
            .map_err(|e| IbkrError::Unknown(format!("account_summary failed: {e}")))?;

        let mut values: std::collections::HashMap<String, (String, String)> =
            std::collections::HashMap::new();

        let mut data_stream = subscription.filter_data();
        let collect_timeout = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < collect_timeout {
            match timeout(Duration::from_millis(500), data_stream.next()).await {
                Ok(Some(Ok(AccountSummaryResult::Summary(summary)))) => {
                    if target_account.is_none()
                        || target_account.as_ref() == Some(&summary.account)
                    {
                        values.insert(
                            summary.tag.clone(),
                            (summary.value.clone(), summary.currency.clone()),
                        );
                    }
                }
                Ok(Some(Ok(AccountSummaryResult::End))) => break,
                Ok(Some(Err(e))) => {
                    return Err(IbkrError::Unknown(format!(
                        "account_summary stream error: {e}"
                    )));
                }
                Ok(None) => break,
                Err(_) => {
                    if !values.is_empty() {
                        break;
                    }
                }
            }
        }

        if values.is_empty() {
            return Err(IbkrError::Unknown(
                "No account summary data received".to_string(),
            ));
        }

        let get = |tag: &str| values.get(tag).map(|(v, _)| v.clone()).unwrap_or_default();

        let account_id = target_account
            .unwrap_or_else(|| values.values().next().map(|(v, _)| v.clone()).unwrap_or_default());

        Ok(AccountInfo {
            account_id,
            net_liquidation: parse_f64(&get(AccountSummaryTags::NET_LIQUIDATION)),
            available_funds: parse_f64(&get(AccountSummaryTags::AVAILABLE_FUNDS)),
            buying_power: parse_f64(&get(AccountSummaryTags::BUYING_POWER)),
            currency: "USD".to_string(),
            daily_pnl: 0.0,
            unrealized_pnl: 0.0,
            realized_pnl: 0.0,
        })
    }

    /// Get current positions
    pub async fn get_positions(
        &self,
        account_id: Option<&str>,
    ) -> Result<Vec<Position>, IbkrError> {
        let client = self.client.get_client().await?;
        let target_account = account_id.map(|s| s.to_string());

        info!(
            account_id = account_id.unwrap_or("all"),
            "Fetching positions"
        );

        let mut subscription = client
            .positions()
            .await
            .map_err(|e| IbkrError::Unknown(format!("positions failed: {e}")))?;

        let mut data_stream = subscription.filter_data();
        let mut positions = Vec::new();
        let collect_timeout = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < collect_timeout {
            match timeout(Duration::from_millis(500), data_stream.next()).await {
                Ok(Some(Ok(PositionUpdate::Position(pos)))) => {
                    if target_account.is_none()
                        || target_account.as_ref() == Some(&pos.account)
                    {
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
                            strike: if is_option && pos.contract.strike > 0.0 { Some(pos.contract.strike) } else { None },
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
                    }
                }
                Ok(Some(Ok(PositionUpdate::PositionEnd))) => break,
                Ok(Some(Err(e))) => {
                    return Err(IbkrError::Unknown(format!(
                        "positions stream error: {e}"
                    )));
                }
                Ok(None) => break,
                Err(_) => {
                    if !positions.is_empty() {
                        break;
                    }
                }
            }
        }

        Ok(positions)
    }
}

fn parse_f64(s: &str) -> f64 {
    s.parse().unwrap_or(0.0)
}

#[cfg(test)]
#[path = "account_tests.rs"]
mod tests;
