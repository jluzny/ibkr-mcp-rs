use ibapi::accounts::{AccountSummaryResult, AccountSummaryTags, PositionUpdate};
use ibapi::accounts::types::AccountGroup;
use ibapi::contracts::SecurityType;
use ibapi::orders::{CommissionReport, ExecutionData, ExecutionFilter, Executions};
use ibapi::subscriptions::SubscriptionItemStreamExt;
use futures::StreamExt;
use tokio::time::{timeout, Duration};
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

use crate::ibkr::client::IbkrClient;
use crate::ibkr::error::IbkrError;

/// Account information
#[derive(Debug, Clone)]
pub struct AccountInfo {
    pub account_id: String,
    pub net_liquidation: f64,
    pub available_funds: f64,
    pub excess_liquidity: f64,
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

/// Execution / fill information
#[derive(Debug, Clone)]
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

/// Account manager
#[derive(Debug)]
pub struct AccountManager {
    client: Arc<IbkrClient>,
    /// Cached account summary to avoid rate-limiting IBKR.
    /// account_summary() is a streaming subscription; opening one per
    /// cron tick (every 10 min) exhausts the ~3/hour request cap.
    cache: std::sync::Mutex<Option<(AccountInfo, Instant)>>,
    /// Cache TTL — stale after this duration triggers a fresh fetch.
    cache_ttl: Duration,
}

impl AccountManager {
    pub fn new(client: Arc<IbkrClient>) -> Self {
        Self {
            client,
            cache: std::sync::Mutex::new(None),
            // account_summary() is a streaming subscription with a hard cap of
            // ~3 concurrent requests per client ID. A short TTL (60s) causes
            // rate-limit [322] under any moderate load. 5 min is safe for
            // account snapshots; positions/quotes have separate endpoints.
            cache_ttl: Duration::from_secs(300),
        }
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

    /// Get account information — cached to avoid IBKR rate-limit [322].
    /// account_summary() is a streaming subscription with a ~3/hour request
    /// cap per client ID. The cache serves data ≤60s old; stale cache
    /// triggers a background refresh but returns the stale value so the
    /// caller never waits on the rate-limit window.
    pub async fn get_account_info(
        &self,
        account_id: Option<&str>,
    ) -> Result<AccountInfo, IbkrError> {
        let target_account = account_id.map(|s| s.to_string());

        // 1. Check cache (synchronous — avoids async lock contention)
        {
            let guard = self.cache.lock().unwrap();
            if let Some((cached, ts)) = guard.as_ref() {
                if ts.elapsed() < self.cache_ttl {
                    info!(
                        account_id = account_id.unwrap_or("default"),
                        "Serving cached account info (age={}s)",
                        ts.elapsed().as_secs()
                    );
                    return Ok(cached.clone());
                }
            }
        } // lock dropped here

        info!(
            account_id = account_id.unwrap_or("default"),
            "Fetching fresh account info"
        );

        // 2. Fetch fresh data
        let client = self.client.get_client().await?;

        let tags = &[
            AccountSummaryTags::NET_LIQUIDATION,
            AccountSummaryTags::AVAILABLE_FUNDS,
            AccountSummaryTags::EXCESS_LIQUIDITY,
            AccountSummaryTags::BUYING_POWER,
            AccountSummaryTags::TOTAL_CASH_VALUE,
            AccountSummaryTags::GROSS_POSITION_VALUE,
            AccountSummaryTags::EQUITY_WITH_LOAN_VALUE,
        ];

        let subscription = timeout(
            Duration::from_secs(10),
            client.account_summary(&AccountGroup("All".to_string()), tags)
        )
        .await
        .map_err(|e| {
            // On timeout, try to serve stale cache
            let guard = self.cache.lock().unwrap();
            if let Some((cached, _)) = guard.as_ref() {
                return IbkrError::Unknown(
                    format!("Timeout; serving stale cache: {e}")
                );
            }
            IbkrError::Unknown(format!("account_summary timeout: {e}"))
        })?
        .map_err(|e| {
            let err_msg = format!("account_summary failed: {e}");
            if err_msg.contains("Maximum number of account summary requests exceeded") {
                let guard = self.cache.lock().unwrap();
                if let Some((cached, _)) = guard.as_ref() {
                    return IbkrError::Unknown(
                        format!("Rate-limited; serving stale cache: {err_msg}")
                    );
                }
            }
            IbkrError::Unknown(err_msg)
        })?;

        let mut values: std::collections::HashMap<String, (String, String)> =
            std::collections::HashMap::new();

        let mut account_id: Option<String> = None;

        let mut data_stream = subscription.clone().filter_data();
        let collect_timeout = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < collect_timeout {
            match timeout(Duration::from_millis(500), data_stream.next()).await {
                Ok(Some(Ok(AccountSummaryResult::Summary(summary)))) => {
                    if account_id.is_none() {
                        account_id = Some(summary.account.clone());
                    }
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
                    subscription.cancel().await;
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

        subscription.cancel().await;

        if values.is_empty() {
            // Fall back to stale cache on empty result
            let guard = self.cache.lock().unwrap();
            if let Some((cached, _)) = guard.as_ref() {
                return Ok(cached.clone());
            }
            return Err(IbkrError::Unknown(
                "No account summary data received".to_string(),
            ));
        }

        let get = |tag: &str| values.get(tag).map(|(v, _)| v.clone()).unwrap_or_default();

        let account_id = account_id.unwrap_or_default();

        let info = AccountInfo {
            account_id,
            net_liquidation: parse_f64(&get(AccountSummaryTags::NET_LIQUIDATION)),
            available_funds: parse_f64(&get(AccountSummaryTags::AVAILABLE_FUNDS)),
            excess_liquidity: parse_f64(&get(AccountSummaryTags::EXCESS_LIQUIDITY)),
            buying_power: parse_f64(&get(AccountSummaryTags::BUYING_POWER)),
            currency: "USD".to_string(),
            daily_pnl: 0.0,
            unrealized_pnl: 0.0,
            realized_pnl: 0.0,
        };

        // 3. Update cache
        {
            let mut guard = self.cache.lock().unwrap();
            *guard = Some((info.clone(), Instant::now()));
        }

        Ok(info)
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

        let subscription = client
            .positions()
            .await
            .map_err(|e| IbkrError::Unknown(format!("positions failed: {e}")))?;

        let mut data_stream = subscription.clone().filter_data();
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
                    subscription.cancel().await;
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

        // Explicitly cancel the positions subscription so TWS releases it.
        // Same race condition as account_summary — Drop's fire-and-forget
        // tokio::spawn may not complete before the next request.
        subscription.cancel().await;

        Ok(positions)
    }

    /// Get historical executions (fills) with P&L
    pub async fn get_executions(
        &self,
        account_id: Option<&str>,
        symbol: Option<&str>,
        since: Option<&str>,
    ) -> Result<Vec<Execution>, IbkrError> {
        let client = self.client.get_client().await?;

        info!(
            account_id = account_id.unwrap_or("all"),
            symbol = symbol.unwrap_or("all"),
            "Fetching executions"
        );

        // Convert since format: MCP accepts "YYYYMMDD-HH:MM:SS" or "YYYYMMDD",
        // IBKR API time format: "yyyymmdd hh:mm:ss xx/xxxx" (with timezone)
        // or "yyyymmdd-hh:mm:ss" (UTC, with dash between date and time).
        // If no time part given, use start-of-day US/Eastern.
        let (time_filter, last_n_days) = match since {
            Some(s) => {
                let formatted = if s.contains('-') && s.len() >= 9 {
                    // "20260521-15:30:00" is already in IBKR UTC format (dash = UTC)
                    // Just pass it through as-is.
                    s.to_string()
                } else {
                    // "20260521" -> "20260521-00:00:00" (UTC dash format for start of day)
                    format!("{}-00:00:00", s)
                };
                (formatted, 7) // keep last_n_days=7 alongside time filter
            }
            None => (String::new(), 7), // default: last 7 days
        };

        let filter = ExecutionFilter {
            client_id: None,
            account_code: account_id.unwrap_or("").to_string(),
            time: time_filter,
            symbol: symbol.unwrap_or("").to_string(),
            security_type: "".to_string(),
            exchange: "".to_string(),
            side: None,
            last_n_days,
            specific_dates: vec![],
        };

        let subscription = client
            .executions(filter)
            .await
            .map_err(|e| IbkrError::Unknown(format!("executions failed: {e}")))?;

        let mut data_stream = subscription.filter_data();
        let mut executions: std::collections::HashMap<String, Execution> = std::collections::HashMap::new();
        let collect_timeout = Duration::from_secs(10);
        let start = std::time::Instant::now();

        while start.elapsed() < collect_timeout {
            match timeout(Duration::from_millis(500), data_stream.next()).await {
                Ok(Some(Ok(Executions::ExecutionData(data)))) => {
                    let is_option = data.contract.security_type == SecurityType::Option;
                    let exec = Execution {
                        execution_id: data.execution.execution_id.clone(),
                        symbol: data.contract.symbol.to_string(),
                        security_type: data.contract.security_type.to_string(),
                        side: data.execution.side.as_str().to_string(),
                        quantity: data.execution.shares,
                        price: data.execution.price,
                        commission: 0.0,
                        realized_pnl: 0.0,
                        time: data.execution.time.clone(),
                        account_id: data.execution.account_number.clone(),
                        strike: if is_option && data.contract.strike > 0.0 { Some(data.contract.strike) } else { None },
                        right: data.contract.right.as_ref().map(|r| r.as_str().to_string()),
                        expiration: if is_option && !data.contract.last_trade_date_or_contract_month.is_empty() {
                            Some(data.contract.last_trade_date_or_contract_month.clone())
                        } else {
                            None
                        },
                        multiplier: if is_option && !data.contract.multiplier.is_empty() {
                            Some(data.contract.multiplier.clone())
                        } else {
                            None
                        },
                    };
                    executions.insert(exec.execution_id.clone(), exec);
                }
                Ok(Some(Ok(Executions::CommissionReport(report)))) => {
                    if let Some(exec) = executions.get_mut(&report.execution_id) {
                        exec.commission = report.commission;
                        exec.realized_pnl = report.realized_pnl.unwrap_or(0.0);
                    }
                }
                Ok(Some(Err(e))) => {
                    return Err(IbkrError::Unknown(format!(
                        "executions stream error: {e}"
                    )));
                }
                Ok(None) => break,
                Err(_) => {
                    if !executions.is_empty() {
                        break;
                    }
                }
            }
        }

        let mut result: Vec<Execution> = executions.into_values().collect();
        result.sort_by(|a, b| b.time.cmp(&a.time));

        // Client-side since filter — IBKR API may not reliably filter server-side.
        // Time formats from IBKR vary: "20260521 15:28:44 US/Eastern" or "20260521 19:28:44 Africa/Abidjan"
        // We normalize to YYYYMMDD for comparison.
        if let Some(since) = since {
            // Normalize since to YYYYMMDD (8 chars)
            let since_date: String = since.chars().take(8).collect();
            result.retain(|e| {
                // IBKR time format starts with YYYYMMDD, possibly followed by space/timezone
                let exec_date: String = e.time.chars().take(8).collect();
                exec_date >= since_date
            });
        }

        Ok(result)
    }
}

fn parse_f64(s: &str) -> f64 {
    s.parse().unwrap_or(0.0)
}

#[cfg(test)]
#[path = "account_tests.rs"]
mod tests;
