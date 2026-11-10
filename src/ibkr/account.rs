use ibapi::accounts::{AccountSummaryResult, AccountSummaryTags, PositionUpdate};
use ibapi::accounts::types::{AccountGroup, AccountId, ContractId};
use ibapi::contracts::SecurityType;
use ibapi::orders::{ExecutionFilter, Executions};
use ibapi::subscriptions::{Subscription, SubscriptionItem, SubscriptionItemStreamExt};
use futures::StreamExt;
use tokio::time::{timeout, Duration};
use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use chrono::{SecondsFormat, Utc};
use tracing::{info, warn};

use crate::ibkr::client::IbkrClient;
use crate::ibkr::error::IbkrError;
use crate::persistence;

/// Account information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

/// Provenance for one account-summary tag, captured from the frame that
/// carried it. `value` is `None` when the tag was absent, unparseable or a
/// sentinel — it is never defaulted to `0.0`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TagProvenance {
    pub value: Option<f64>,
    pub currency: Option<String>,
    /// RFC 3339 UTC instant this tag was last received.
    pub received_at: String,
}

/// What the stream proved about one read.
///
/// `terminal_sentinel` is `true` only when a real IBKR end marker was observed.
/// A deadline, a channel close, or a stream error leaves it `false`, so a
/// consumer can never mistake "we stopped waiting" for "IBKR finished".
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ReadProvenance {
    /// `AccountSummaryEnd`, `PositionEnd` or `ExecutionDataEnd`; `None` when the
    /// read ended without one.
    pub end_marker: Option<String>,
    pub terminal_sentinel: bool,
    /// Elements emitted in this payload (positive count marker), never the
    /// number of frames seen before filtering.
    pub count: usize,
    /// Monotonic read id. Payloads with different `generation` values must not
    /// be merged field-by-field.
    pub generation: u64,
    /// RFC 3339 UTC instant this payload was finalized.
    pub received_at: String,
}

/// Account snapshot plus the per-tag provenance that proves it.
#[derive(Debug, Clone)]
pub struct AccountRead {
    pub info: AccountInfo,
    /// Keyed by IBKR tag id (`NetLiquidation`, `MaintMarginReq`, ...).
    pub tags: std::collections::HashMap<String, TagProvenance>,
    pub provenance: ReadProvenance,
}

/// Positions plus the provenance of the read that produced them.
#[derive(Debug, Clone)]
pub struct PositionsRead {
    pub positions: Vec<Position>,
    /// `Position` frames seen before the account filter; `count` is the number
    /// of rows actually emitted.
    pub received_count: usize,
    /// True only when every emitted row's mark came from a `pnl_single` reply.
    pub marks_attested: bool,
    pub provenance: ReadProvenance,
}

/// Executions plus the provenance of the read that produced them.
#[derive(Debug, Clone)]
pub struct ExecutionsRead {
    pub executions: Vec<Execution>,
    pub provenance: ReadProvenance,
}

/// IBKR account-summary tag id → payload field name. Payload keys and the
/// `presence` / `field_receipts` / `tag_currency` tables use the field name.
pub const ACCOUNT_TAG_FIELDS: [(&str, &str); 11] = [
    (AccountSummaryTags::NET_LIQUIDATION, "netLiquidation"),
    (AccountSummaryTags::AVAILABLE_FUNDS, "availableFunds"),
    (AccountSummaryTags::EXCESS_LIQUIDITY, "excessLiquidity"),
    (AccountSummaryTags::BUYING_POWER, "buyingPower"),
    (AccountSummaryTags::TOTAL_CASH_VALUE, "totalCashValue"),
    (AccountSummaryTags::EQUITY_WITH_LOAN_VALUE, "equityWithLoanValue"),
    (AccountSummaryTags::GROSS_POSITION_VALUE, "grossPositionValue"),
    (AccountSummaryTags::MAINT_MARGIN_REQ, "maintMarginReq"),
    (AccountSummaryTags::INIT_MARGIN_REQ, "initMarginReq"),
    (AccountSummaryTags::LOOK_AHEAD_MAINT_MARGIN_REQ, "lookAheadMaintMarginReq"),
    (AccountSummaryTags::LOOK_AHEAD_INIT_MARGIN_REQ, "lookAheadInitMarginReq"),
];

/// Health of the single account-summary slot, shared by the pump and the reads.
///
/// The bridge can only prove what it observed. A `CancelAccountSummary` is
/// written to the socket by `Subscription::cancel()`, but IBKR never
/// acknowledges it, so "the previous request is gone" is established by the
/// replacement being accepted, not by the cancel itself. A `[322]` on that
/// replacement is the proof that the previous request was still registered:
/// from then on the pump must not open another subscription at all.
#[derive(Debug, Default)]
struct SummaryHealth {
    /// Set when IBKR answered a replacement request with `[322]`: the previous
    /// request is still registered, so every further open would be rejected
    /// and only extend the leak. Never cleared while the process runs.
    slot_leaked: bool,
    /// Set when the pump stopped for a reason it does not retry (an
    /// unconfirmed retirement, or the failure cap). Reads then fail closed on
    /// the real error instead of waiting for a snapshot that cannot arrive;
    /// only a process restart re-arms the pump.
    parked: bool,
    /// The real error text (e.g. the full `[322] ...` notice), surfaced in the
    /// read payload instead of the old generic "pump may be down" message.
    last_error: Option<String>,
}

/// Outcome of retiring the current summary request before a replacement is
/// opened.
///
/// IBKR never acknowledges a `CancelAccountSummary`, so "confirmed" here means
/// the SDK reported the cancelled subscription terminal for every clone — the
/// strongest local proof, and the gate that must pass before another
/// subscription may exist. Server-side retirement is only proved by the
/// replacement being accepted; a `[322]` on it latches
/// [`SummaryHealth::slot_leaked`] and the pump parks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retirement {
    /// The cancel took effect and the stream is terminal: the replacement may
    /// be opened.
    Confirmed,
    /// The subscription carries no request id, so no `CancelAccountSummary`
    /// can name it. Its slot must be assumed still registered.
    NoRequestId,
    /// The cancel was requested but nothing proved the subscription is gone
    /// (no `Cancelled` report within [`RETIRE_CONFIRM_WAIT`]).
    Unconfirmed,
}

/// Why the pump left its stream loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamExit {
    /// The current request was retired; one replacement may be opened.
    Refresh,
    /// The stream errored or ended without a replacement being due.
    Failure,
}

/// Consecutive open/stream failures the pump tolerates before it parks. Bounds
/// a broken gateway; combined with [`Retirement`] this is what keeps the pump
/// from ever becoming an unbounded reopen loop.
const MAX_SUMMARY_FAILURES: u32 = 5;

/// How long the pump waits for the SDK to report the cancelled subscription
/// terminal (`Error::Cancelled`) before treating the retirement as
/// unconfirmed.
const RETIRE_CONFIRM_WAIT: Duration = Duration::from_secs(1);

/// Shared cache state — held via `Arc` so the background subscription pump
/// task (which must be `'static`) can update it.
struct SummaryState {
    cache: std::sync::Mutex<Option<(AccountRead, Instant)>>,
    /// True while the background pump task is running. CAS-guarded so only
    /// one pump (and therefore ONE account-summary subscription) exists per
    /// process — this is what makes the 3-concurrent-request cap unreachable.
    pump_running: AtomicBool,
    /// Monotonic read id: one value per account-summary subscription and per
    /// finite read, so payloads from different reads cannot be merged.
    generation: AtomicU64,
    /// Set by a read that observed per-tag receipts older than the refresh
    /// interval; consumed by the pump, which cancels the one subscription and
    /// re-requests the summary. At most one request is ever pending.
    refresh_requested: AtomicBool,
    /// Wakes the pump out of `stream.next()` when a refresh is requested.
    refresh_notify: tokio::sync::Notify,
    /// Slot health and the last real IBKR error, for the pump and the reads.
    health: std::sync::Mutex<SummaryHealth>,
}

impl std::fmt::Debug for SummaryState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `Notify` is not `Debug`; report the observable bits only.
        let has_cache = self.cache.lock().map(|g| g.is_some()).unwrap_or(false);
        let (leaked, parked) = self
            .health
            .lock()
            .map(|h| (h.slot_leaked, h.parked))
            .unwrap_or((false, false));
        f.debug_struct("SummaryState")
            .field("has_cache", &has_cache)
            .field("pump_running", &self.pump_running.load(Ordering::SeqCst))
            .field("generation", &self.generation.load(Ordering::SeqCst))
            .field(
                "refresh_requested",
                &self.refresh_requested.load(Ordering::SeqCst),
            )
            .field("slot_leaked", &leaked)
            .field("parked", &parked)
            .finish()
    }
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
    /// Per-tag receipts older than this make a read ask the pump to cancel and
    /// re-request the account summary (IBKR does not resend unchanged values on
    /// a long-lived subscription, so receipts would otherwise freeze). `ZERO`
    /// disables the periodic refresh.
    refresh_interval: Duration,
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
                generation: AtomicU64::new(0),
                refresh_requested: AtomicBool::new(false),
                refresh_notify: tokio::sync::Notify::new(),
                health: std::sync::Mutex::new(SummaryHealth::default()),
            }),
            cache_ttl: Duration::from_secs(300),
            stale_max: Duration::from_secs(900),
            refresh_interval: Duration::from_secs(60),
            fetch_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Set the cadence for re-requesting the account summary. `ZERO` disables
    /// periodic refreshes and keeps the one-subscription behaviour.
    pub fn with_refresh_interval(mut self, refresh_interval: Duration) -> Self {
        self.refresh_interval = refresh_interval;
        self
    }

    /// Set how long a cached snapshot is served without question.
    pub fn with_cache_ttl(mut self, cache_ttl: Duration) -> Self {
        self.cache_ttl = cache_ttl;
        self
    }

    /// Set the hard staleness limit: older data is refused, never served.
    pub fn with_stale_max(mut self, stale_max: Duration) -> Self {
        self.stale_max = stale_max;
        self
    }

    /// Snapshot of cached data with its age, if any.
    fn cached(&self) -> Option<(AccountRead, std::time::Duration)> {
        let guard = self.state.cache.lock().unwrap();
        guard.as_ref().map(|(read, ts)| (read.clone(), ts.elapsed()))
    }

    /// Next monotonic read id.
    fn next_generation(&self) -> u64 {
        self.state.generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Last real error the account-summary pump observed (IBKR text included,
    /// e.g. `[322] ... Maximum number of account summary requests exceeded`).
    pub fn summary_error(&self) -> Option<String> {
        self.state.health.lock().unwrap().last_error.clone()
    }

    /// True when IBKR refused a replacement request with `[322]`: the previous
    /// request is still registered, so the pump will not open another
    /// subscription (nor will a read ask it to).
    pub fn summary_slot_leaked(&self) -> bool {
        self.state.health.lock().unwrap().slot_leaked
    }

    /// True when the pump is parked (a `[322]`, an unconfirmed retirement, or
    /// the failure cap): no fresh snapshot can land without a process restart,
    /// so a read must not ask for a re-request nor wait for one.
    fn summary_unavailable(&self) -> bool {
        let health = self.state.health.lock().unwrap();
        health.slot_leaked || health.parked
    }

    /// `; last pump error: ...` for the stale refusal, empty when clean.
    fn pump_error_suffix(&self) -> String {
        match self.summary_error() {
            Some(error) => format!("; last pump error: {error}"),
            None => String::new(),
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

    /// Get account information — one streaming subscription per process.
    ///
    /// `account_summary()` is a STREAMING subscription: IBKR pushes updates as
    /// account values change, and caps CONCURRENT subscriptions at 3 per client.
    /// A read-driven refresh must therefore retire the current request and
    /// confirm it before one replacement exists (see [`Self::pump_loop`]); a
    /// replacement refused with `[322]` parks the pump for the process lifetime
    /// and surfaces the IBKR text in the read payload.
    ///
    /// This implementation opens ONE subscription per process lifetime and
    /// pumps it in the background (`spawn_summary_pump`), folding streamed
    /// updates into the cache. Callers read the cache; on first call they wait
    /// (bounded by 10s) for the initial snapshot. Data older than `stale_max`
    /// (15 min) is refused — a margin monitor must not act on ancient numbers.
    pub async fn get_account_info_read(
        &self,
        account_id: Option<&str>,
    ) -> Result<AccountRead, IbkrError> {
        let _ = account_id; // single-account deployment; subscription covers "All"

        // 1. Fresh cache → serve it; if the per-tag receipts are older than
        //    the refresh interval, ask the pump to re-request first (bounded).
        if let Some((cached, age)) = self.cached() {
            if age < self.cache_ttl {
                info!(
                    account_id = %cached.info.account_id,
                    "Serving cached account info (age={}s)", age.as_secs()
                );
                return self.serve_fresh(cached, age).await;
            }
        }

        // 2. Stale or empty → make sure the pump is running, wait for refresh.
        //    fetch_lock collapses N concurrent callers into one pump spawn.
        let _permit = self.fetch_lock.lock().await;

        // Re-check after acquiring the lock — the lock holder may have refreshed.
        if let Some((cached, age)) = self.cached() {
            if age < self.cache_ttl {
                info!(
                    account_id = %cached.info.account_id,
                    "Serving cached account info (age={}s)", age.as_secs()
                );
                return self.serve_fresh(cached, age).await;
            }
        }

        self.spawn_summary_pump();

        // A [322] proved the previous summary request is still registered at
        // the gateway, or the pump parked on an unconfirmed retirement: no
        // fresh snapshot can land without a process restart. Fail closed now
        // with the real error instead of waiting out the 10s window for
        // nothing.
        let latched = self.summary_unavailable();

        // A stale cache means the long-lived subscription is delivering nothing
        // (IBKR does not resend unchanged values). Ask the pump for one
        // re-request; the pump drops it if the cadence has not elapsed.
        if !latched {
            self.request_summary_refresh();
        }

        // 3. Wait for the pump to land a FRESH snapshot (bounded 10s).
        //    Only return when age < cache_ttl — stale cache must NOT be
        //    returned from this step (that was BUG 2).
        let deadline = if latched {
            tokio::time::Instant::now()
        } else {
            tokio::time::Instant::now() + Duration::from_secs(10)
        };
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if let Some((cached, age)) = self.cached() {
                if age < self.cache_ttl {
                    info!(
                        account_id = %cached.info.account_id,
                        "Fresh snapshot received after wait (age={}s)",
                        age.as_secs()
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
                    "account info stale (age={}s, max={}s) — pump may be disconnected or gateway down{}",
                    age.as_secs(),
                    self.stale_max.as_secs(),
                    self.pump_error_suffix()
                )));
            }
        }
        warn!("No cache available and pump didn't deliver within 10s");
        Err(IbkrError::Unknown(
            "account summary snapshot not received within 10s — gateway may be starting up".to_string(),
        ))
    }

    /// Serve a cache that is still within `cache_ttl`.
    ///
    /// When the per-tag receipts are older than `refresh_interval`, this asks
    /// the pump for one re-request and waits (bounded by [`REFRESH_WAIT`]) for
    /// the refreshed snapshot, so a monitor read sees receipts no older than
    /// the cadence on a healthy gateway. If the re-request does not land — the
    /// pump is down, the gateway closes, or the deadline passes — the
    /// pre-existing cache is served unchanged: receipts are NEVER restamped
    /// locally, only by a frame that actually arrived.
    async fn serve_fresh(
        &self,
        cached: AccountRead,
        age: Duration,
    ) -> Result<AccountRead, IbkrError> {
        let oldest = oldest_receipt_age(&cached).unwrap_or(age);
        if !refresh_is_due(oldest, self.refresh_interval) {
            return Ok(cached);
        }

        if self.summary_unavailable() {
            // A [322] proved the previous request was never retired, or the
            // pump parked because a retirement could not be confirmed. Asking
            // for another re-request cannot succeed and would only extend the
            // leak, so serve the last snapshot with its true age.
            return Ok(cached);
        }

        let before = cached.provenance.generation;
        self.spawn_summary_pump();
        self.request_summary_refresh();

        if let Some(refreshed) = self.await_generation_after(before).await {
            info!(
                account_id = %refreshed.info.account_id,
                generation = refreshed.provenance.generation,
                "Serving refreshed account info (receipts re-requested)"
            );
            return Ok(refreshed);
        }

        warn!(
            age_secs = age.as_secs(),
            "Account summary refresh did not land; serving the last received snapshot"
        );
        Ok(cached)
    }

    /// Signal the pump that the cached receipts are due for a re-request.
    fn request_summary_refresh(&self) {
        self.state.refresh_requested.store(true, Ordering::SeqCst);
        self.state.refresh_notify.notify_one();
    }

    /// Wait for the pump to publish a snapshot from a newer subscription.
    async fn await_generation_after(&self, before: u64) -> Option<AccountRead> {
        let deadline = tokio::time::Instant::now() + REFRESH_WAIT;
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if let Some((cached, _)) = self.cached() {
                if cached.provenance.generation > before {
                    return Some(cached);
                }
            }
        }
        None
    }

    /// Reduced account info, without provenance. Prefer
    /// [`get_account_info_read`](Self::get_account_info_read): the tool payload
    /// must carry the per-tag proof, not just the numbers.
    pub async fn get_account_info(
        &self,
        account_id: Option<&str>,
    ) -> Result<AccountInfo, IbkrError> {
        Ok(self.get_account_info_read(account_id).await?.info)
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

        let pump_handle = tokio::spawn(Self::pump_loop(
            client,
            state,
            self.refresh_interval,
        ));

        // Panic supervisor: if the pump task panics (a panic inside a spawned
        // task does NOT kill the process — tokio swallows it), exit so systemd
        // Restart=always revives a clean process instead of a silent zombie.
        // Same pattern as the reconnect-loop supervisor in client.rs.
        tokio::spawn(async move {
            match pump_handle.await {
                Ok(()) => {
                    // Pump exited cleanly (e.g. stream ended and loop broke out).
                    // Next get_account_info call will re-spawn it if needed.
                    warn!("Account summary pump exited cleanly");
                }
                Err(e) => {
                    tracing::error!(
                        is_panic = e.is_panic(),
                        error = %e,
                        "Account summary pump task panicked — exiting for systemd restart"
                    );
                    std::process::exit(1);
                }
            }
        });
    }

    /// The streaming pump loop. Extracted to a free-standing method so the
    /// spawn site stays small and the supervisor stays obvious.
    ///
    /// Exactly ONE subscription exists per process. A read-driven refresh
    /// retires the current request — writes `CancelAccountSummary` against its
    /// request id and waits for the SDK to report that stream finished — and
    /// only then opens exactly one replacement. If the retirement cannot be
    /// confirmed, no replacement is opened: the slot may still be registered
    /// at IBKR, and a second `ReqAccountSummary` against a full cap is exactly
    /// the `[322]` loop this pump must never re-enter. When IBKR does answer a
    /// replacement with `[322]`, [`SummaryHealth::slot_leaked`] latches and the
    /// pump parks: the last snapshot keeps its true age and reads fail closed
    /// at `stale_max`.
    async fn pump_loop(
        client: Arc<IbkrClient>,
        state: Arc<SummaryState>,
        refresh_interval: Duration,
    ) {
        let mut backoff_secs: u64 = 2;
        // Consecutive opens/streams that produced no `End`. Bounded so a
        // broken gateway cannot turn the pump into an unbounded reopen loop.
        let mut failures: u32 = 0;
        loop {
            if state.health.lock().unwrap().slot_leaked {
                // Hard cap: a [322] proved the previous summary request is
                // still registered. Another open can only be rejected and
                // would extend the leak, so stop for this process lifetime.
                // `pump_running` stays true, so no read can respawn the pump.
                warn!("Pump: summary slot leaked ([322]) — not opening another subscription");
                return;
            }
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
                // Delivered but previously never requested: margin coverage.
                AccountSummaryTags::MAINT_MARGIN_REQ,
                AccountSummaryTags::INIT_MARGIN_REQ,
                AccountSummaryTags::LOOK_AHEAD_MAINT_MARGIN_REQ,
                AccountSummaryTags::LOOK_AHEAD_INIT_MARGIN_REQ,
            ];

            info!(attempt = failures + 1, "Pump: opening account summary subscription");
            let subscription = match arc_client
                .account_summary(&AccountGroup("All".to_string()), &tags)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    let msg = e.to_string();
                    record_pump_error(&state, &msg);
                    if is_summary_cap_error(&msg) {
                        // Another consumer (or our own leaked request) holds
                        // the slots: opening again cannot succeed.
                        let mut health = state.health.lock().unwrap();
                        health.slot_leaked = true;
                        health.parked = true;
                        drop(health);
                        warn!(error = %msg, "Pump: IBKR refused the summary request ([322]); not reopening");
                        return;
                    }
                    failures += 1;
                    if failures >= MAX_SUMMARY_FAILURES {
                        state.health.lock().unwrap().parked = true;
                        warn!(failures, "Pump: giving up on the summary after repeated open failures");
                        return;
                    }
                    warn!("Pump: account_summary failed: {msg}");
                    sleep_with_backoff(&mut backoff_secs).await;
                    continue;
                }
            };

            info!("Pump: subscription open, streaming updates into cache");
            // Use the raw subscription stream (not filter_data) so Notices
            // — including IBKR error notices like [322] — are observable.
            // Consumers poll one clone; the original stays available as the
            // handle that sends `CancelAccountSummary` on a refresh.
            let mut stream = subscription.clone();
            let handle = subscription;
            // A new subscription is a new generation: the `End` marker of a
            // previous initial dump must never be carried over into this one.
            let generation = state.generation.fetch_add(1, Ordering::SeqCst) + 1;
            // Accumulator: tag → per-tag provenance. Seeded from the last
            // published snapshot so a re-request never drops a tag that the new
            // dump has not delivered yet; a seeded tag keeps the instant it was
            // LAST actually received at until a real frame carries it again.
            let (mut values, mut acct, per_frame_publish) = {
                let guard = state.cache.lock().unwrap();
                seed_summary_values(guard.as_ref().map(|(read, _)| read))
            };
            let mut end_marker: Option<String> = None;
            let opened = Instant::now();

            let exit = loop {
                tokio::select! {
                    item = stream.next() => match item {
                        Some(Ok(SubscriptionItem::Data(AccountSummaryResult::Summary(s)))) => {
                            if acct.is_none() {
                                acct = Some(s.account.clone());
                            }
                            // Missing/unparseable/sentinel tags stay `None`;
                            // they are never coerced to `0.0`.
                            apply_summary_frame(
                                &mut values,
                                &s.tag,
                                parse_tag_value(&s.value),
                                Some(s.currency.clone()).filter(|c| !c.is_empty()),
                                now_rfc3339(),
                            );
                            // BUG 1 FIX: also update the cache on every Summary
                            // event after the initial dump, not just on End —
                            // IBKR sends End once, then only streams individual
                            // Summary updates. The INITIAL dump is published per
                            // frame so a dump without `End` still yields partial
                            // evidence; a re-request is published atomically at
                            // `End`, so a refresh never exposes a half-filled
                            // table. `end_marker` stays `None` until the End
                            // frame arrives, so a partial dump can never claim
                            // completeness.
                            if per_frame_publish || end_marker.is_some() {
                                if values.contains_key(AccountSummaryTags::NET_LIQUIDATION) {
                                    publish_summary(
                                        &state,
                                        &values,
                                        acct.as_deref(),
                                        end_marker.clone(),
                                        generation,
                                    );
                                }
                            }
                        }
                        // End marks the end of the *initial snapshot*; the
                        // subscription stays open and continues streaming.
                        Some(Ok(SubscriptionItem::Data(AccountSummaryResult::End))) => {
                            end_marker = Some("AccountSummaryEnd".to_string());
                            if !values.is_empty() {
                                let read = publish_summary(
                                    &state,
                                    &values,
                                    acct.as_deref(),
                                    end_marker.clone(),
                                    generation,
                                );
                                info!(account_id = acct.as_deref().unwrap_or("?"),
                                      tags = read.provenance.count,
                                      generation,
                                      "Pump: snapshot complete (end=AccountSummaryEnd)");
                            }
                            backoff_secs = 2; // reset on success
                            failures = 0;
                            // A real `End` proves the request is live and the
                            // gateway is answering: the previous error is no
                            // longer the current state.
                            if let Ok(mut health) = state.health.lock() {
                                health.last_error = None;
                            }
                        }
                        Some(Ok(SubscriptionItem::Notice(n))) => {
                            warn!("Pump: notice from IBKR: {n}");
                        }
                        Some(Err(e)) => {
                            let msg = e.to_string();
                            record_pump_error(&state, &msg);
                            if is_summary_cap_error(&msg) {
                                // IBKR answered OUR request with [322]: the
                                // previous one was never retired, so another
                                // open is guaranteed to be rejected too. Latch
                                // and park instead of looping.
                                let mut health = state.health.lock().unwrap();
                                health.slot_leaked = true;
                                health.parked = true;
                                drop(health);
                                warn!(
                                    error = %msg,
                                    "Pump: IBKR refused the summary request ([322]) — the previous request is still registered; not reopening"
                                );
                                return;
                            }
                            warn!("Pump: stream error ({msg}) — will reopen after backoff");
                            break StreamExit::Failure;
                        }
                        None => {
                            warn!("Pump: stream ended — will reopen after backoff");
                            break StreamExit::Failure;
                        }
                    },
                    _ = state.refresh_notify.notified() => {
                        let requested = state.refresh_requested.swap(false, Ordering::SeqCst);
                        if !refresh_may_reissue(requested, opened.elapsed(), refresh_interval) {
                            // Either no request is pending (a stale wakeup) or
                            // the current subscription is younger than the
                            // cadence: at most one re-request per interval.
                            continue;
                        }
                        info!(
                            generation,
                            subscribed_secs = opened.elapsed().as_secs(),
                            "Pump: retiring the summary request before re-requesting"
                        );
                        // Retire the CURRENT request and confirm it before any
                        // replacement exists. `Subscription::cancel()` only
                        // writes `CancelAccountSummary`; waiting for the SDK to
                        // report the stream finished is the strongest proof a
                        // request-id subscription offers, because IBKR does not
                        // acknowledge a summary cancel.
                        match retire_summary(&handle, &mut stream).await {
                            Retirement::Confirmed => {
                                info!(
                                    generation,
                                    "Pump: summary cancel confirmed — opening one replacement after the settle"
                                );
                                // The cancel is on the wire; give the gateway a
                                // moment to retire the request before the
                                // replacement is counted against the cap.
                                tokio::time::sleep(REFRESH_RELEASE_DELAY).await;
                                break StreamExit::Refresh;
                            }
                            Retirement::NoRequestId => {
                                // No `CancelAccountSummary` can name this
                                // subscription, so its slot must be assumed
                                // still registered. Keep the live stream and
                                // never open a second one.
                                let msg = "account summary cancel unconfirmed: the subscription carries no request id; not opening another";
                                record_pump_error(&state, msg);
                                warn!("Pump: {msg}");
                                continue;
                            }
                            Retirement::Unconfirmed => {
                                // The cancel was written but nothing proved the
                                // request is gone. Opening a replacement risks
                                // the [322] cap, and this subscription can no
                                // longer stream (cancel stops every clone), so
                                // park: reads serve the last snapshot with its
                                // true age and fail closed at stale_max.
                                let msg = "account summary cancel unconfirmed: the previous request may still be registered; not opening another subscription";
                                record_pump_error(&state, msg);
                                state.health.lock().unwrap().parked = true;
                                warn!("Pump: {msg}");
                                return;
                            }
                        }
                    }
                }
            };

            match exit {
                // The refresh branch already waited out the settle and
                // confirmed the retirement.
                StreamExit::Refresh => {}
                StreamExit::Failure => {
                    failures += 1;
                    if failures >= MAX_SUMMARY_FAILURES {
                        // Bounded: no unbounded reopen loop.
                        record_pump_error(
                            &state,
                            &format!("{failures} consecutive summary stream failures; pump parked"),
                        );
                        state.health.lock().unwrap().parked = true;
                        warn!(failures, "Pump: giving up on the summary after repeated stream failures");
                        return;
                    }
                    sleep_with_backoff(&mut backoff_secs).await;
                }
            }
        }
    }

    /// Get current positions (reduced form, without provenance).
    pub async fn get_positions(
        &self,
        account_id: Option<&str>,
    ) -> Result<Vec<Position>, IbkrError> {
        Ok(self.get_positions_read(account_id).await?.positions)
    }

    /// Positions plus the proof that the initial dump ended.
    pub async fn get_positions_read(
        &self,
        account_id: Option<&str>,
    ) -> Result<PositionsRead, IbkrError> {
        let client = self.client.get_client().await?;
        let target_account = account_id.map(|s| s.to_string());
        let generation = self.next_generation();

        info!(
            account_id = account_id.unwrap_or("all"),
            "Fetching positions"
        );

        let mut subscription = client
            .positions()
            .await
            .map_err(|e| IbkrError::Unknown(format!("positions failed: {e}")))?;

        // `collect_until` stops the moment the batch-end marker arrives and
        // includes it as the last row; on the deadline it returns the rows read
        // without a marker. That last-row check is the only place the two exits
        // are distinguished — the previous hand-rolled loop treated both alike.
        // A stream error also leaves no marker (the SDK logs it and returns the
        // rows), so it degrades to `terminal_sentinel: false`, never to a false
        // "complete".
        let mut items = subscription
            .collect_until(Duration::from_secs(5), |rows| {
                matches!(rows.last(), Some(PositionUpdate::PositionEnd))
            })
            .await;
        let end_seen = matches!(items.last(), Some(PositionUpdate::PositionEnd));
        if end_seen {
            items.pop();
        }

        let mut received_count = 0usize;
        let mut positions = Vec::new();
        for item in items {
            if let PositionUpdate::Position(pos) = item {
                received_count += 1;
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
        }

        // Explicitly cancel the positions subscription so TWS releases it.
        // Same race condition as account_summary — Drop's fire-and-forget
        // tokio::spawn may not complete before the next request.
        subscription.cancel().await;

        // Marks are attested only when every emitted row got a real
        // `pnl_single` reply. An empty read proves nothing either.
        let mut marks_attested = !positions.is_empty();

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
                                    // No position → no mark. Never 0.0-as-observed.
                                    marks_attested = false;
                                    0.0
                                };

                                pos.market_price = mark_price;
                                pos.market_value = value;
                                pos.unrealized_pnl = unrealized_pnl;
                                pos.daily_pnl = daily_pnl;
                            }
                            _ => {
                                marks_attested = false;
                                warn!("pnl_single timeout for {}", pos.symbol);
                            }
                        }
                        pnl_clone.cancel().await;
                    }
                    Err(e) => {
                        marks_attested = false;
                        warn!("pnl_single failed for {}: {e}", pos.symbol);
                    }
                }
            }
        }

        let provenance = ReadProvenance {
            end_marker: if end_seen { Some("PositionEnd".to_string()) } else { None },
            terminal_sentinel: end_seen,
            count: positions.len(),
            generation,
            received_at: now_rfc3339(),
        };
        Ok(PositionsRead {
            positions,
            received_count,
            marks_attested,
            provenance,
        })
    }

    /// Get historical executions (fills) with P&L (reduced form).
    pub async fn get_executions(
        &self,
        account_id: Option<&str>,
        symbol: Option<&str>,
        since: Option<&str>,
    ) -> Result<Vec<Execution>, IbkrError> {
        Ok(self
            .get_executions_read(account_id, symbol, since)
            .await?
            .executions)
    }

    /// Executions plus the proof that the ledger ended.
    pub async fn get_executions_read(
        &self,
        account_id: Option<&str>,
        symbol: Option<&str>,
        since: Option<&str>,
    ) -> Result<ExecutionsRead, IbkrError> {
        let client = self.client.get_client().await?;
        let generation = self.next_generation();

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

        let mut subscription = client
            .executions(filter)
            .await
            .map_err(|e| IbkrError::Unknown(format!("executions failed: {e}")))?;

        // `collect_to_end_within` is the SDK's fail-closed collector: the
        // `ExecutionDataEnd` frame closes the stream natively, so `Ok` means the
        // marker arrived, `Err(UnexpectedEndOfStream)` means the channel closed
        // early, and `Err(Timeout)` means the deadline passed. The old loop
        // returned `{"success": true, "count": 0}` for all three.
        let items = subscription
            .collect_to_end_within(Duration::from_secs(10))
            .await
            .map_err(|e| {
                IbkrError::Unknown(format!(
                    "executions read failed (no ExecutionDataEnd marker): {e}"
                ))
            })?;

        let mut executions: std::collections::HashMap<String, Execution> =
            std::collections::HashMap::new();
        for item in items {
            match item {
                Executions::ExecutionData(data) => {
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
                Executions::CommissionReport(report) => {
                    if let Some(exec) = executions.get_mut(&report.execution_id) {
                        exec.commission = report.commission;
                        exec.realized_pnl = report.realized_pnl.unwrap_or(0.0);
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

        let provenance = ReadProvenance {
            end_marker: Some("ExecutionDataEnd".to_string()),
            terminal_sentinel: true,
            count: result.len(),
            generation,
            received_at: now_rfc3339(),
        };
        Ok(ExecutionsRead {
            executions: result,
            provenance,
        })
    }
}

/// How long a read waits for a re-requested snapshot before falling back to
/// the last received one unchanged.
const REFRESH_WAIT: Duration = Duration::from_secs(2);

/// Pause between `CancelAccountSummary` and the replacement request, so the
/// gateway releases the slot before the new request is counted.
const REFRESH_RELEASE_DELAY: Duration = Duration::from_millis(250);

/// Whether a cache whose OLDEST receipt is `receipt_age` old should trigger a
/// re-request. `interval == ZERO` disables the periodic refresh.
fn refresh_is_due(receipt_age: Duration, interval: Duration) -> bool {
    !interval.is_zero() && receipt_age >= interval
}

/// Age of the OLDEST per-tag receipt, if any receipt parses.
///
/// A long-lived subscription may restamp the cache continuously while some tags
/// never change; the cadence must be judged against the receipts themselves,
/// otherwise those tags' `received_at` would freeze forever.
fn oldest_receipt_age(read: &AccountRead) -> Option<Duration> {
    let now = Utc::now();
    read.tags
        .values()
        .filter_map(|tag| chrono::DateTime::parse_from_rfc3339(&tag.received_at).ok())
        .map(|at| now.signed_duration_since(at.with_timezone(&Utc)))
        .max()
        .map(|age| age.to_std().unwrap_or(Duration::ZERO))
}

/// Whether the pump may cancel + re-request now: a request must be pending and
/// the current subscription must have been open for at least `interval`, so at
/// most one re-request happens per interval.
fn refresh_may_reissue(requested: bool, subscribed_for: Duration, interval: Duration) -> bool {
    requested && !interval.is_zero() && subscribed_for >= interval
}

/// Seed a new subscription's accumulator from the last published snapshot.
///
/// Returns `(values, account, publish_per_frame)`. A seeded tag keeps the
/// instant it was last actually received at — only a frame that carries the tag
/// again restamps it. `publish_per_frame` is true only for the first
/// subscription (empty cache), so a dump that never reaches `End` still yields
/// partial evidence; every later dump is published atomically at `End`.
fn seed_summary_values(
    cache: Option<&AccountRead>,
) -> (
    std::collections::HashMap<String, TagProvenance>,
    Option<String>,
    bool,
) {
    match cache {
        Some(read) => (
            read.tags.clone(),
            Some(read.info.account_id.clone()).filter(|a| !a.is_empty()),
            false,
        ),
        None => (std::collections::HashMap::new(), None, true),
    }
}

/// Fold one `AccountSummary` frame into the per-tag accumulator.
///
/// Only the tag carried by the frame gets a new receipt; every other tag keeps
/// the instant it was last received at. Nothing here invents a receipt for a
/// value IBKR did not send again.
fn apply_summary_frame(
    values: &mut std::collections::HashMap<String, TagProvenance>,
    tag: &str,
    value: Option<f64>,
    currency: Option<String>,
    received_at: String,
) {
    values.insert(
        tag.to_string(),
        TagProvenance {
            value,
            currency,
            received_at,
        },
    );
}

/// Publish an accumulator as the shared cache and record it, returning the read
/// so the caller can log its provenance.
fn publish_summary(
    state: &SummaryState,
    values: &std::collections::HashMap<String, TagProvenance>,
    account_id: Option<&str>,
    end_marker: Option<String>,
    generation: u64,
) -> AccountRead {
    let read = build_account_read(values, account_id, end_marker, generation);
    *state.cache.lock().unwrap() = Some((read.clone(), Instant::now()));
    persistence::record(read.info.clone());
    read
}

/// Filter IBKR sentinel/garbage values for PnL marks (legacy behaviour:
/// coerces to `0.0`). Account-summary tags use [`parse_tag_value`] instead,
/// which reports absence as `None`.
fn filter_sentinel(v: f64) -> f64 {
    if v >= SENTINEL_MAX || v <= -SENTINEL_MAX || v.is_nan() || v.is_infinite() {
        0.0
    } else {
        v
    }
}

/// Strict tag parse. Absent, empty, unparseable, non-finite and sentinel
/// values all become `None` — they are NEVER coerced to `0.0`.
fn parse_tag_value(s: &str) -> Option<f64> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    let value: f64 = trimmed.parse().ok()?;
    let is_sentinel = value >= SENTINEL_MAX
        || value <= -SENTINEL_MAX
        || value.is_nan()
        || value.is_infinite();
    if is_sentinel {
        None
    } else {
        Some(value)
    }
}

/// IBKR uses MaxFloat64 (~1.79e308) and similar as "no value" markers; anything
/// beyond +/-1e11 is not a plausible account figure.
const SENTINEL_MAX: f64 = 1e11;

/// RFC 3339 UTC instant with millisecond precision, e.g.
/// `2026-10-22T12:34:56.789Z`.
fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Build an [`AccountRead`] from accumulated per-tag provenance.
///
/// The flat `AccountInfo` fields are kept only so existing consumers of the
/// struct (and its snake_case JSON keys) keep working; the authoritative,
/// absence-aware values live in `tags` / `provenance`. A missing tag leaves the
/// legacy field at `0.0` but is reported as `None` in `tags` and as `ABSENT` in
/// the payload's `presence` table, so no decision should be based on the legacy
/// field alone.
fn build_account_read(
    values: &std::collections::HashMap<String, TagProvenance>,
    account_id: Option<&str>,
    end_marker: Option<String>,
    generation: u64,
) -> AccountRead {
    let value = |tag: &str| values.get(tag).and_then(|p| p.value);
    let count = values.values().filter(|p| p.value.is_some()).count();

    // The summary frame's currency is per tag. `currency` on the legacy struct
    // is the one carried by NetLiquidation when present, otherwise "USD".
    let currency = values
        .get(AccountSummaryTags::NET_LIQUIDATION)
        .and_then(|p| p.currency.clone())
        .unwrap_or_else(|| "USD".to_string());

    let info = AccountInfo {
        account_id: account_id.unwrap_or_default().to_string(),
        net_liquidation: value(AccountSummaryTags::NET_LIQUIDATION).unwrap_or(0.0),
        available_funds: value(AccountSummaryTags::AVAILABLE_FUNDS).unwrap_or(0.0),
        excess_liquidity: value(AccountSummaryTags::EXCESS_LIQUIDITY).unwrap_or(0.0),
        buying_power: value(AccountSummaryTags::BUYING_POWER).unwrap_or(0.0),
        currency,
        // Not part of the AccountSummary subscription; reported as unproven.
        daily_pnl: 0.0,
        unrealized_pnl: 0.0,
        realized_pnl: 0.0,
    };

    AccountRead {
        info,
        tags: values.clone(),
        provenance: ReadProvenance {
            terminal_sentinel: end_marker.is_some(),
            end_marker,
            count,
            generation,
            received_at: now_rfc3339(),
        },
    }
}

/// Record the real IBKR error so a stale refusal reports what actually
/// happened instead of the old generic "pump may be down" text.
fn record_pump_error(state: &SummaryState, message: &str) {
    if let Ok(mut health) = state.health.lock() {
        health.last_error = Some(message.to_string());
    }
}

/// Whether an error text is IBKR's account-summary subscription cap (`[322]`).
///
/// The cap applies to requests still registered at the gateway, so this is the
/// proof that the previous `ReqAccountSummary` was never retired: after it the
/// pump must not open another subscription.
fn is_summary_cap_error(message: &str) -> bool {
    message.contains("Maximum number of account summary requests exceeded")
        || message.contains("[322]")
}

/// Retire the CURRENT summary request and report whether the replacement may be
/// opened.
///
/// `Subscription::cancel()` writes `CancelAccountSummary` (IBKR never answers
/// it), stops every clone at its next poll and releases the local route on
/// drop. The strongest local confirmation is therefore the SDK reporting the
/// cancelled stream terminal (`Error::Cancelled`), which this waits for before
/// the caller drops the handles: without it (no request id, a stream that had
/// already ended, or a timeout) the request's slot must be assumed still
/// registered, and opening a replacement would risk the `[322]` cap.
///
/// Server-side retirement is not provable here; the caller proves it by the
/// replacement being accepted, and parks the pump on a `[322]`.
async fn retire_summary(
    handle: &Subscription<AccountSummaryResult>,
    stream: &mut Subscription<AccountSummaryResult>,
) -> Retirement {
    if handle.request_id().is_none() {
        return Retirement::NoRequestId;
    }

    handle.cancel().await;

    match timeout(RETIRE_CONFIRM_WAIT, stream.next()).await {
        Ok(Some(Err(ibapi::Error::Cancelled))) => Retirement::Confirmed,
        Ok(Some(other)) => {
            warn!("Pump: cancel not confirmed — stream yielded {other:?} instead of Cancelled");
            Retirement::Unconfirmed
        }
        Ok(None) => {
            warn!("Pump: cancel not confirmed — the stream had already ended");
            Retirement::Unconfirmed
        }
        Err(_) => {
            warn!(
                wait_secs = RETIRE_CONFIRM_WAIT.as_secs(),
                "Pump: cancel not confirmed — no Cancelled report from the SDK in time"
            );
            Retirement::Unconfirmed
        }
    }
}

/// Exponential backoff sleep for the pump loop, capped at 60s.
async fn sleep_with_backoff(backoff_secs: &mut u64) {
    tokio::time::sleep(Duration::from_secs(*backoff_secs)).await;
    *backoff_secs = (*backoff_secs * 2).min(60);
}

#[cfg(test)]
#[path = "account_gateway.rs"]
pub(crate) mod gateway;

#[cfg(test)]
#[path = "account_tests.rs"]
mod tests;