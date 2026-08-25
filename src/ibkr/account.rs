use ibapi::accounts::{AccountSummaryResult, AccountSummaryTags, PositionUpdate};
use ibapi::accounts::types::{AccountGroup, AccountId, ContractId};
use ibapi::contracts::SecurityType;
use ibapi::orders::{ExecutionFilter, Executions};
use ibapi::subscriptions::{SubscriptionItem, SubscriptionItemStreamExt};
use futures::StreamExt;
use tokio::time::{timeout, Duration};
use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{info, warn};

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
    pub contract_id: i32,
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

/// Shared cache state — held via `Arc` so the background subscription pump
/// task (which must be `'static`) can update it.
#[derive(Debug)]
struct SummaryState {
    cache: std::sync::Mutex<Option<(AccountInfo, Instant)>>,
    /// True while the background pump task is running. CAS-guarded so only
    /// one pump (and therefore ONE account-summary subscription) exists per
    /// process — this is what makes the 3-concurrent-request cap unreachable.
    pump_running: AtomicBool,
}

/// Account manager
#[derive(Debug)]
pub struct AccountManager {
    client: Arc<IbkrClient>,
    state: Arc<SummaryState>,
    /// Cache TTL — data younger than this is served without question.
    cache_ttl: Duration,
    /// Data older than `stale_max` is NOT served (margin monitor must not
    /// act on ancient numbers); between TTL and stale_max it is served while
    /// the pump refreshes in the background.
    stale_max: Duration,
    /// Serializes cache-miss fetches so N concurrent callers spawn at most
    /// one pump.
    fetch_lock: tokio::sync::Mutex<()>,
}

impl AccountManager {
    pub fn new(client: Arc<IbkrClient>) -> Self {
        Self {
            client,
            state: Arc::new(SummaryState {
                cache: std::sync::Mutex::new(None),
                pump_running: AtomicBool::new(false),
            }),
            cache_ttl: Duration::from_secs(300),
            stale_max: Duration::from_secs(900),
            fetch_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Snapshot of cached data with its age, if any.
    fn cached(&self) -> Option<(AccountInfo, std::time::Duration)> {
        let guard = self.state.cache.lock().unwrap();
        guard.as_ref().map(|(info, ts)| (info.clone(), ts.elapsed()))
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

    /// Get account information — one streaming subscription per process.
    ///
    /// `account_summary()` is a STREAMING subscription: IBKR pushes updates as
    /// account values change, and caps CONCURRENT subscriptions at 3 per client.
    /// The old pattern (open → collect until `End` → cancel) leaked a gateway
    /// slot per call because ibapi's `cancel()` is a no-op once the `End`
    /// sentinel has been consumed (`snapshot_ended` flag) — after 3 fetches per
    /// gateway connection every subsequent call hit
    /// `[322] Maximum number of account summary requests exceeded`.
    ///
    /// This implementation opens ONE subscription per process lifetime and
    /// pumps it in the background (`spawn_summary_pump`), folding streamed
    /// updates into the cache. Callers read the cache; on first call they wait
    /// (bounded by 10s) for the initial snapshot. Data older than `stale_max`
    /// (15 min) is refused — a margin monitor must not act on ancient numbers.
    pub async fn get_account_info(
        &self,
        account_id: Option<&str>,
    ) -> Result<AccountInfo, IbkrError> {
        let _ = account_id; // single-account deployment; subscription covers "All"

        // 1. Fresh cache → serve immediately
        if let Some((cached, age)) = self.cached() {
            if age < self.cache_ttl {
                info!(
                    account_id = %cached.account_id,
                    "Serving cached account info (age={}s)", age.as_secs()
                );
                return Ok(cached);
            }
        }

        // 2. Stale or empty → make sure the pump is running, wait for refresh.
        //    fetch_lock collapses N concurrent callers into one pump spawn.
        let _permit = self.fetch_lock.lock().await;

        // Re-check after acquiring the lock — the lock holder may have refreshed
        if let Some((cached, age)) = self.cached() {
            if age < self.cache_ttl {
                info!(
                    account_id = %cached.account_id,
                    "Serving cached account info (age={}s)", age.as_secs()
                );
                return Ok(cached);
            }
        }

        self.spawn_summary_pump();

        // 3. Wait for the pump to land a FRESH snapshot (bounded 10s).
        //    Only return when age < cache_ttl — stale cache must NOT be
        //    returned from this step (that was BUG 2).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if let Some((cached, age)) = self.cached() {
                if age < self.cache_ttl {
                    info!(
                        account_id = %cached.account_id,
                        "Fresh snapshot received after wait (age={}s)", age.as_secs()
                    );
                    return Ok(cached);
                }
                // Cache exists but is stale — keep waiting for the pump to refresh.
            }
        }

        // 4. Timed out waiting for fresh data. Serve stale if within stale_max,
        //    else refuse so the margin monitor never acts on ancient numbers.
        if let Some((cached, age)) = self.cached() {
            if age < self.stale_max {
                warn!(
                    age_secs = age.as_secs(),
                    "Wait timed out; serving stale account info (within stale_max)"
                );
                return Ok(cached);
            } else {
                warn!(
                    age_secs = age.as_secs(),
                    stale_max_secs = self.stale_max.as_secs(),
                    "Refusing stale account info (exceeds stale_max)"
                );
                return Err(IbkrError::Unknown(format!(
                    "account info stale (age={}s, max={}s) — pump may be disconnected or gateway down",
                    age.as_secs(), self.stale_max.as_secs()
                )));
            }
        }
        warn!("No cache available and pump didn't deliver within 10s");
        Err(IbkrError::Unknown(
            "account summary snapshot not received within 10s — gateway may be starting up".to_string(),
        ))
    }

    /// Ensure exactly one background subscription pump is running.
    ///
    /// The pump opens the account-summary subscription ONCE and keeps it open
    /// for the process lifetime, folding streamed updates into the cache. If
    /// the stream errors/ends, the pump clears `pump_running` and exits; the
    /// next `get_account_info` call observes stale/empty cache and re-spawns
    /// it (a new open is then legitimate — the old subscription is gone).
    fn spawn_summary_pump(&self) {
        // CAS: only one caller transitions false → true and spawns the pump
        if self
            .state
            .pump_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return; // pump already running
        }

        let client = Arc::clone(&self.client);
        let state = Arc::clone(&self.state);

        tokio::spawn(async move {
            let mut backoff_secs: u64 = 2;
            loop {
                let arc_client = match client.get_client().await {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("Pump: not connected ({e}), retrying");
                        sleep_with_backoff(&mut backoff_secs).await;
                        continue;
                    }
                };

                let tags: Vec<&str> = vec![
                    AccountSummaryTags::NET_LIQUIDATION,
                    AccountSummaryTags::AVAILABLE_FUNDS,
                    AccountSummaryTags::EXCESS_LIQUIDITY,
                    AccountSummaryTags::BUYING_POWER,
                    AccountSummaryTags::TOTAL_CASH_VALUE,
                    AccountSummaryTags::GROSS_POSITION_VALUE,
                    AccountSummaryTags::EQUITY_WITH_LOAN_VALUE,
                ];

                info!("Pump: opening account summary subscription");
                let subscription = match arc_client
                    .account_summary(&AccountGroup("All".to_string()), &tags)
                    .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("Maximum number of account summary requests exceeded") {
                            // Another consumer holds the slots — do NOT hammer.
                            warn!("Pump: [322] opening summary — backing off 5 min");
                            tokio::time::sleep(Duration::from_secs(300)).await;
                        } else {
                            warn!("Pump: account_summary failed: {msg}");
                            sleep_with_backoff(&mut backoff_secs).await;
                        }
                        continue;
                    }
                };

                info!("Pump: subscription open, streaming updates into cache");
                // Use the raw subscription stream (not filter_data) so Notices
                // — including IBKR error notices like [322] — are observable.
                let mut stream = subscription;
                // Accumulator: tag → (value, currency). Persisted to cache on
                // every End / update batch.
                let mut values: std::collections::HashMap<String, (String, String)> =
                    std::collections::HashMap::new();
                let mut acct: Option<String> = None;

                loop {
                    match stream.next().await {
                        Some(Ok(SubscriptionItem::Data(AccountSummaryResult::Summary(s)))) => {
                            if acct.is_none() {
                                acct = Some(s.account.clone());
                            }
                            values.insert(s.tag.clone(), (s.value.clone(), s.currency.clone()));
                            // BUG 1 FIX: Also update the cache on every Summary event,
                            // not just on End. IBKR sends End once (initial snapshot),
                            // then only streams individual Summary updates. Without
                            // this, the cache timestamp freezes at the initial End and
                            // the cache appears stale forever.
                            // Guard: only write once NLV is present to avoid partial
                            // snapshots during the initial tag-by-tag stream.
                            if values.contains_key(AccountSummaryTags::NET_LIQUIDATION) {
                                let info = build_account_info(&values, acct.as_deref());
                                *state.cache.lock().unwrap() = Some((info, Instant::now()));
                            }
                        }
                        // End marks the end of the *initial snapshot*; the
                        // subscription stays open and continues streaming.
                        Some(Ok(SubscriptionItem::Data(AccountSummaryResult::End))) => {
                            if !values.is_empty() {
                                let info = build_account_info(&values, acct.as_deref());
                                *state.cache.lock().unwrap() = Some((info, Instant::now()));
                                info!(account_id = acct.as_deref().unwrap_or("?"),
                                      "Pump: snapshot complete, cache updated");
                            }
                            backoff_secs = 2; // reset on success
                        }
                        Some(Ok(SubscriptionItem::Notice(n))) => {
                            warn!("Pump: notice from IBKR: {n}");
                        }
                        Some(Err(e)) => {
                            warn!("Pump: stream error ({e}) — will reopen");
                            break;
                        }
                        None => {
                            warn!("Pump: stream ended — will reopen");
                            break;
                        }
                    }
                }

                // Stream died: brief pause, then reopen (IBKR sends updates
                // periodically; the cap applies to CONCURRENT opens, and the
                // old one is gone).
                sleep_with_backoff(&mut backoff_secs).await;
            }
        });
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
                            contract_id: pos.contract.contract_id,
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

        // --- PnL Enrichment (Option B: pnl_single per position) ---
        // Proven approach from Go project: options-trading-agent-go.
        // After collecting positions, call pnl_single for each to populate
        // market_price, market_value, unrealized_pnl, daily_pnl.
        if !positions.is_empty() {
            let account_id = positions[0].account_id.clone();
            let ibkr_account = AccountId(account_id.clone());

            for pos in &mut positions {
                let contract_id = ContractId(pos.contract_id);

                match client.pnl_single(&ibkr_account, contract_id, None).await {
                    Ok(pnl_sub) => {
                        // filter_data() takes ownership, so clone the subscription
                        // to retain a handle for cancellation afterwards.
                        let pnl_clone = pnl_sub.clone();
                        let pnl_result = timeout(
                            Duration::from_secs(3),
                            pnl_sub.filter_data().next(),
                        ).await;

                        match pnl_result {
                            Ok(Some(Ok(pnl))) => {
                                let value = filter_sentinel(pnl.value);
                                let daily_pnl = filter_sentinel(pnl.daily_pnl);
                                let unrealized_pnl = filter_sentinel(pnl.unrealized_pnl);

                                // Compute mark price: value / position.
                                // Options: divide by 100 (standard multiplier).
                                let mark_price = if pnl.position != 0.0 {
                                    let raw = value / pnl.position;
                                    if pos.security_type == "OPT" || pos.security_type == "FOP" {
                                        raw / 100.0
                                    } else {
                                        raw
                                    }
                                } else {
                                    0.0
                                };

                                pos.market_price = mark_price;
                                pos.market_value = value;
                                pos.unrealized_pnl = unrealized_pnl;
                                pos.daily_pnl = daily_pnl;
                            }
                            _ => {
                                warn!("pnl_single timeout for {}", pos.symbol);
                            }
                        }
                        pnl_clone.cancel().await;
                    }
                    Err(e) => {
                        warn!("pnl_single failed for {}: {e}", pos.symbol);
                    }
                }
            }
        }

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

/// Filter IBKR sentinel/garbage values (MaxFloat64 ~1.79e11).
/// Cap at +/-1e11 — anything beyond is garbage data from IBKR.
/// Also catches NaN and Infinity.
fn filter_sentinel(v: f64) -> f64 {
    const MAX_REASONABLE: f64 = 1e11;
    if v >= MAX_REASONABLE || v <= -MAX_REASONABLE || v.is_nan() || v.is_infinite() {
        0.0
    } else {
        v
    }
}

/// Build an `AccountInfo` from accumulated summary values.
fn build_account_info(
    values: &std::collections::HashMap<String, (String, String)>,
    account_id: Option<&str>,
) -> AccountInfo {
    let get = |tag: &str| {
        values
            .get(tag)
            .map(|(v, _)| v.clone())
            .unwrap_or_default()
    };
    AccountInfo {
        account_id: account_id.unwrap_or_default().to_string(),
        net_liquidation: parse_f64(&get(AccountSummaryTags::NET_LIQUIDATION)),
        available_funds: parse_f64(&get(AccountSummaryTags::AVAILABLE_FUNDS)),
        excess_liquidity: parse_f64(&get(AccountSummaryTags::EXCESS_LIQUIDITY)),
        buying_power: parse_f64(&get(AccountSummaryTags::BUYING_POWER)),
        currency: "USD".to_string(),
        daily_pnl: 0.0,
        unrealized_pnl: 0.0,
        realized_pnl: 0.0,
    }
}

/// Exponential backoff sleep for the pump loop, capped at 60s.
async fn sleep_with_backoff(backoff_secs: &mut u64) {
    tokio::time::sleep(Duration::from_secs(*backoff_secs)).await;
    *backoff_secs = (*backoff_secs * 2).min(60);
}

#[cfg(test)]
#[path = "account_tests.rs"]
mod tests;