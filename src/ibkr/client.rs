//! IBKR connection manager with automatic reconnection.
//!
//! [`IbkrClient`] wraps the `ibapi::Client` in an `Arc<RwLock<Option<...>>>`
//! so it can be shared across async tasks and safely replaced on reconnect.
//!
//! ## Connection Lifecycle
//!
//! 1. Create with [`IbkrClient::new`]
//! 2. Call [`connect`](IbkrClient::connect) to start a background reconnection loop
//! 3. Use [`get_client`](IbkrClient::get_client) to borrow the inner `ibapi::Client`
//! 4. Call [`disconnect`](IbkrClient::disconnect) on shutdown
//!
//! ## Outage policy — never exit because the broker is unreachable
//!
//! An unreachable or unauthenticated broker is **not** a process fault. The
//! gateway can sit waiting for a human 2FA approval for hours, and exiting on
//! that condition produces a restart loop that (a) cannot fix anything and
//! (b) floods the journal. The policy is therefore:
//!
//! * The reconnect loop retries forever with a bounded backoff
//!   (500 ms · 1.6ⁿ capped at 60 s, flat 300 s once the outage is clearly not
//!   transient — see [`backoff`](IbkrClient::backoff)).
//! * While the broker is down the process stays up and the HTTP server keeps
//!   answering; broker-side tools return a clean "not connected" instead of
//!   hanging, and `/health/live` stays 200 so an orchestrator cannot mistake a
//!   broker outage for a dead process.
//! * `std::process::exit(1)` is reserved for a genuinely wedged *process*:
//!   the canary's HTTP self-probe of our own `/health/live`, the reconnect
//!   task panicking, and the HTTP server task ending abnormally.
//!
//! See `docs/broker-outage-policy.md` for the operator-facing version.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{error, info, warn};

use ibapi::prelude::*;

use crate::config::IbkrConfig;
use crate::ibkr::error::IbkrError;

/// Exponential backoff ceiling while an outage is still short-lived.
const RECONNECT_FAST_CAP: Duration = Duration::from_secs(60);
/// Flat retry interval once an outage is clearly not transient. Deliberately
/// calm: an outage that outlives 2FA or runs into a weekend must not turn into
/// a handshake storm against a gateway that is waiting for a human.
const RECONNECT_CALM_INTERVAL: Duration = Duration::from_secs(300);
/// Consecutive failed attempts (TCP pre-flight included) before switching from
/// the exponential ramp to the flat calm interval (≈10 min of failures).
const RECONNECT_CALM_AFTER_ATTEMPTS: u32 = 15;
/// How often the canary ticks.
const CANARY_TICK: Duration = Duration::from_secs(60);
/// Consecutive canary ticks on which our **own** HTTP `/health/live` did not
/// answer before we accept the process is wedged (~3 min).
const SELF_HEALTH_FAILURES_BEFORE_EXIT: u32 = 3;
/// Log every Nth canary tick while the broker is down, so a multi-hour outage
/// costs ~6 log lines/hour instead of 60.
const OUTAGE_LOG_EVERY: u32 = 10;

/// The canary's view of the bridge's own HTTP server. `None` in stdio mode
/// (there is no HTTP server to probe, and no systemd restart to lean on).
#[derive(Debug, Clone)]
pub struct SelfHealthTarget {
    pub host: String,
    pub port: u16,
}

impl SelfHealthTarget {
    /// Build a target from the configured bind address, mapping wildcard binds
    /// (`0.0.0.0`, `::`) onto loopback — you cannot usefully `connect()` to a
    /// wildcard on every stack.
    pub fn loopback(bind_host: &str, port: u16) -> Self {
        let host = match bind_host {
            "" | "0.0.0.0" => "127.0.0.1".to_string(),
            "::" | "[::]" => "::1".to_string(),
            other => other.to_string(),
        };
        Self { host, port }
    }
}

/// Shared IBKR client state.
/// Stores `Arc<Client>` in `RwLock` for safe concurrent access and reconnection.
pub struct IbkrClient {
    pub config: IbkrConfig,
    inner: Arc<RwLock<Option<Arc<Client>>>>,
    /// Consecutive broker-probe failures recorded by the canary. Diagnostic
    /// only — never drives a process exit (see [`IbkrClient::start_canary`]).
    broker_failures: Arc<AtomicU32>,
    /// Unix seconds at which the current broker outage started (0 = this process
    /// has not observed an outage yet).
    outage_started_unix: Arc<AtomicU64>,
}

impl IbkrClient {
    pub fn new(config: IbkrConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            inner: Arc::new(RwLock::new(None)),
            broker_failures: Arc::new(AtomicU32::new(0)),
            outage_started_unix: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Start background reconnection loop.
    ///
    /// Retries forever. Being unable to reach the broker is never a reason to
    /// exit — see the module docs for the outage policy.
    pub fn connect(self: Arc<Self>) {
        let supervised = Arc::clone(&self);
        let reconnect_handle = tokio::spawn(async move {
            let mut attempts: u32 = 0;
            // True while the TCP endpoint itself is refusing connections; used
            // to retry the handshake immediately when the gateway comes back
            // instead of waiting out the long-outage interval.
            let mut tcp_down = false;

            loop {
                if attempts > 0 {
                    let delay = Self::backoff(attempts);
                    info!(
                        attempt = attempts,
                        delay_ms = delay.as_millis() as u64,
                        "Waiting before IBKR reconnect"
                    );
                    sleep(delay).await;
                }

                // Pre-flight: verify the TCP endpoint is actually accepting
                // connections. Using Client::connect on a dead socket may
                // return early eof from socat when the IBKR API isn't yet
                // listening (startup / 2FA / relogin).
                let target = format!("{}:{}", self.config.host, self.config.port);
                match tokio::time::timeout(
                    Duration::from_secs(5),
                    tokio::net::TcpStream::connect(&target),
                )
                .await
                {
                    Ok(Ok(_socket)) => {
                        if tcp_down {
                            info!(target = %target, "IBKR TCP endpoint is back — handshaking immediately");
                            tcp_down = false;
                            attempts = 0;
                        }
                    }
                    _ => {
                        let attempt = attempts.saturating_add(1);
                        warn!(attempt, target = %target, "IBKR TCP endpoint not ready, skipping handshake attempt");
                        attempts = attempt;
                        tcp_down = true;
                        continue;
                    }
                }

                attempts = attempts.saturating_add(1);

                match self.try_connect_once().await {
                    Ok(client) => {
                        let mut guard = self.inner.write().await;
                        // Gracefully shut down old client if any
                        if let Some(old) = guard.take() {
                            old.disconnect().await;
                        }
                        *guard = Some(Arc::new(client));
                        drop(guard);

                        info!("IBKR connected successfully");
                        attempts = 0;
                        tcp_down = false;

                        // Wait until connection drops, then reconnect
                        self.maintain_connection().await;
                    }
                    Err(e) => {
                        warn!(attempt = attempts, error = %e, "IBKR connection failed");
                    }
                }
            }
        });

        // Supervise the reconnect task: if it panics (a panic inside a spawned
        // task does not kill the process), exit so systemd Restart=always brings
        // up a fresh process. Without this, a dead reconnect loop leaves the MCP
        // HTTP server serving stale/erroring responses indefinitely.
        tokio::spawn(async move {
            if reconnect_handle.await.is_err() {
                error!("IBKR reconnect loop panicked or ended — exiting for systemd restart");
                std::process::exit(1);
            }
            let _ = supervised; // keep Arc alive for the lifetime of the supervisor
        });
    }

    async fn try_connect_once(&self) -> Result<Client, IbkrError> {
        let url = format!("{}:{}", self.config.host, self.config.port);
        let timeout = Duration::from_secs(self.config.connection_timeout_secs);

        // Use process PID as unique client id when config says 0 (default)
        let client_id = if self.config.client_id == 0 {
            std::process::id() as i32
        } else {
            self.config.client_id
        };

        info!(url = %url, client_id, "Connecting to IBKR");

        match tokio::time::timeout(timeout, Client::connect(&url, client_id)).await {
            Ok(Ok(client)) => Ok(client),
            Ok(Err(e)) => Err(IbkrError::ConnectionFailed(e.to_string())),
            Err(_) => Err(IbkrError::ConnectionFailed("timeout".into())),
        }
    }

    /// Polls connection health. Returns — it does **not** exit the process —
    /// when the session is gone, handing control back to the outer reconnect
    /// loop. An unreachable broker must never restart the bridge; systemd
    /// restarts cannot restore a gateway that is waiting for a human 2FA
    /// approval, and the restart itself is what floods the journal.
    ///
    /// Two checks are needed because ibapi's `is_connected()` only tracks the
    /// TCP socket state (an atomic bool set at handshake time). When socat is
    /// in front of the IB Gateway, the TCP socket stays open even when the
    /// IBKR API session goes stale (daily re-authentication, 2FA expiry, etc.),
    /// so `is_connected()` returns `true` while the actual IBKR session is dead.
    ///
    /// On either failure the stale client is dropped first, so `is_connected()`
    /// (and therefore `get_connection_status`) truthfully reports `false` while
    /// the reconnect loop retries, instead of claiming a dead session is up.
    async fn maintain_connection(&self) {
        let mut consecutive_failures: u32 = 0;
        loop {
            sleep(Duration::from_secs(30)).await;

            // Fast path: ibapi says disconnected (TCP dead)
            if !self.is_connected().await {
                warn!("Connection lost (TCP) — dropping session, reconnecting in background");
                self.drop_connection().await;
                return;
            }

            // Deep path: TCP is alive but the IBKR session may be stale.
            // Try a lightweight request to probe the actual API health.
            match self.probe_connection().await {
                Ok(()) => consecutive_failures = 0,
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    warn!(
                        error = %e,
                        failures = consecutive_failures,
                        "Connection probe failed (stale session suspected)"
                    );
                    // After 3 consecutive probe failures (~90s), drop the
                    // session and let the reconnect loop rebuild it.
                    if consecutive_failures >= 3 {
                        warn!(
                            failures = consecutive_failures,
                            "Sustained connection probe failures — dropping session, reconnecting in background"
                        );
                        self.drop_connection().await;
                        return;
                    }
                }
            }
        }
    }

    /// Forget the current client and close its socket. Used when the session is
    /// judged dead: the reconnect loop then retries with backoff, and
    /// [`is_connected`](IbkrClient::is_connected) reports `false` in the
    /// meantime rather than describing a corpse as connected.
    async fn drop_connection(&self) {
        let old = { self.inner.write().await.take() };
        if let Some(old) = old {
            // ibapi's disconnect can block on a half-dead socket; don't let a
            // wedged socket stall the reconnect loop.
            let _ = tokio::time::timeout(Duration::from_secs(5), old.disconnect()).await;
        }
    }

    /// Lightweight IBKR API probe. Requests managed accounts — a cheap,
    /// read-only call that doesn't consume subscription slots.
    async fn probe_connection(&self) -> Result<(), IbkrError> {
        let client = self.get_client().await?;
        // Request managed accounts: a minimal call that exercises the
        // full IBKR message bus without opening a subscription.
        match tokio::time::timeout(
            Duration::from_secs(10),
            client.managed_accounts(),
        ).await {
            Ok(Ok(accounts)) => {
                if accounts.is_empty() {
                    return Err(IbkrError::Unknown(
                        "probe returned empty account list".to_string(),
                    ));
                }
                Ok(())
            }
            Ok(Err(e)) => Err(IbkrError::Unknown(format!("probe error: {e}"))),
            Err(_) => Err(IbkrError::Unknown("probe timeout".to_string())),
        }
    }

    /// Start a constant "canary" that probes the broker every 60s regardless of
    /// connection state, plus our own HTTP server.
    ///
    /// Two independent jobs, deliberately with different consequences:
    ///
    /// * **Broker probe** (every 60 s): keeps the outage visible in the journal
    ///   and in [`broker_failures`](IbkrClient::broker_failures) /
    ///   [`broker_outage_secs`](IbkrClient::broker_outage_secs), and writes the
    ///   heartbeat file on success. **Never exits the process** — a broker that
    ///   is down or awaiting 2FA is a human's problem, and restarting the bridge
    ///   cannot fix it. This is a deliberate reversal of the previous policy,
    ///   which exited after 5 consecutive failures (~5 min) and produced 59+
    ///   systemd restarts on 2026-09-18.
    /// * **Self-health probe** (every 60 s, HTTP mode only): `GET /health/live`
    ///   on our own listener over a raw socket. Three consecutive failures mean
    ///   the process is genuinely wedged and serving nothing, which *is* worth a
    ///   systemd restart.
    ///
    /// Note the failure accounting is per-tick and not shared with the
    /// reconnect loop: a tick counts one failure whether the client slot is
    /// empty or the probe request failed. Startup handshake attempts made by
    /// [`connect`](IbkrClient::connect) are never counted here, so a slow
    /// initial login cannot inflate the counter.
    ///
    /// The heartbeat file (`IBKR_MCP_HEARTBEAT_PATH`, default
    /// `/tmp/ibkr-mcp-heartbeat`) is only written on a successful probe — it
    /// means "the broker link is healthy", not "the process is alive".
    pub fn start_canary(self: &Arc<Self>, self_health: Option<SelfHealthTarget>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            const HEARTBEAT_PATH_ENV: &str = "IBKR_MCP_HEARTBEAT_PATH";

            let mut consecutive_failures: u32 = 0;
            let mut self_health_failures: u32 = 0;

            loop {
                sleep(CANARY_TICK).await;

                // ---- broker side: diagnostic only, never fatal -------------
                let broker_ok = this.is_connected().await
                    && matches!(
                        tokio::time::timeout(Duration::from_secs(10), this.probe_connection())
                            .await,
                        Ok(Ok(()))
                    );

                if broker_ok {
                    if consecutive_failures > 0 {
                        info!(
                            outage_ticks = consecutive_failures,
                            "Canary: broker link restored"
                        );
                    }
                    consecutive_failures = 0;
                    this.broker_failures.store(0, Ordering::Relaxed);
                    this.outage_started_unix.store(0, Ordering::Relaxed);

                    let path = std::env::var(HEARTBEAT_PATH_ENV)
                        .unwrap_or_else(|_| "/tmp/ibkr-mcp-heartbeat".to_string());
                    // Heartbeat for external watchdogs/systemd timers that
                    // want a cheap "broker link is healthy" signal without
                    // touching the API.
                    if let Ok(mut f) = std::fs::File::create(&path) {
                        use std::io::Write;
                        let _ = writeln!(f, "{}", chrono::Utc::now().to_rfc3339());
                    }
                } else {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    this.broker_failures
                        .store(consecutive_failures, Ordering::Relaxed);

                    if consecutive_failures == 1 {
                        this.outage_started_unix.store(now_unix(), Ordering::Relaxed);
                        warn!(
                            failures = consecutive_failures,
                            "Canary: broker not connected — bridge stays up and keeps serving MCP, reconnecting with backoff"
                        );
                    } else if consecutive_failures % OUTAGE_LOG_EVERY == 0 {
                        info!(
                            failures = consecutive_failures,
                            outage_secs = this.broker_outage_secs(),
                            "Canary: broker still not connected — bridge still up and serving MCP"
                        );
                    }
                }

                // ---- self side: the only canary failure that may exit ------
                let Some(target) = self_health.as_ref() else {
                    continue;
                };
                if self_http_alive(target).await {
                    if self_health_failures > 0 {
                        info!(
                            previous_failures = self_health_failures,
                            "Canary: HTTP self-health recovered"
                        );
                    }
                    self_health_failures = 0;
                } else {
                    self_health_failures = self_health_failures.saturating_add(1);
                    error!(
                        failures = self_health_failures,
                        addr = %format!("{}:{}/health/live", target.host, target.port),
                        "Canary: own HTTP server did not answer"
                    );
                    if self_health_failures >= SELF_HEALTH_FAILURES_BEFORE_EXIT {
                        error!(
                            failures = self_health_failures,
                            "Canary: process wedged (own HTTP server unresponsive) — exiting for systemd restart"
                        );
                        std::process::exit(1);
                    }
                }
            }
        });
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        let guard = self.inner.read().await;
        guard.as_ref().map(|c| c.is_connected()).unwrap_or(false)
    }

    /// Consecutive failed broker probes observed by the canary (0 = healthy).
    pub fn broker_failures(&self) -> u32 {
        self.broker_failures.load(Ordering::Relaxed)
    }

    /// Seconds the current broker outage has lasted, counted from the first
    /// failed canary tick. `None` while the link is healthy (or before the first
    /// tick), so consumers can tell "up" from "down for N seconds" — the case
    /// that matters is a process that has never connected at all.
    pub fn broker_outage_secs(&self) -> Option<u64> {
        if self.broker_failures.load(Ordering::Relaxed) == 0 {
            return None;
        }
        let started = self.outage_started_unix.load(Ordering::Relaxed);
        if started == 0 {
            None
        } else {
            Some(now_unix().saturating_sub(started))
        }
    }

    /// Get a clone of the `Arc<Client>` for use in async operations.
    /// Returns error if not connected.
    pub async fn get_client(&self) -> Result<Arc<Client>, IbkrError> {
        let guard = self.inner.read().await;
        match guard.as_ref() {
            Some(client) => Ok(Arc::clone(client)),
            None => Err(IbkrError::NotConnected),
        }
    }

    pub async fn disconnect(&self) {
        let mut guard = self.inner.write().await;
        if let Some(client) = guard.take() {
            client.disconnect().await;
            info!("Disconnected from IBKR");
        }
    }

    /// Bounded reconnect backoff.
    ///
    /// Exponential (500 ms · 1.6ⁿ) up to [`RECONNECT_FAST_CAP`], then flat at
    /// [`RECONNECT_CALM_INTERVAL`] once the outage has survived
    /// [`RECONNECT_CALM_AFTER_ATTEMPTS`] attempts. There is no attempt at which
    /// the loop gives up: an unreachable broker means "wait", not "exit".
    fn backoff(attempts: u32) -> Duration {
        if attempts == 0 {
            return Duration::ZERO;
        }
        if attempts >= RECONNECT_CALM_AFTER_ATTEMPTS {
            return RECONNECT_CALM_INTERVAL;
        }
        // Overflow-safe exponentiation: cap the exponent so 1.6^e can never
        // exceed u64 nanoseconds (panics in mul_f64 past ~e=96, which killed
        // the reconnect loop for 20h on 2026-08-26).
        const MAX_EXPONENT: u32 = 40; // 1.6^40 ≈ 2.4e8 s, far above any sane ceiling
        let exp = attempts.min(MAX_EXPONENT) - 1;
        let base = Duration::from_millis(500);
        let delay = base.mul_f64(1.6f64.powi(exp as i32));
        delay.min(RECONNECT_FAST_CAP)
    }
}

/// Seconds since the Unix epoch (0 if the clock is before it, which cannot
/// happen in practice but keeps the arithmetic total).
fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Bytes the status-line read may accumulate before it is judged dead: a
/// complete status line is ~17 bytes, so anything past this is not one.
const SELF_PROBE_MAX_STATUS_BYTES: usize = 1024;

/// Timeout for each phase of the self-probe: connect, write, and the whole
/// accumulation of the status line.
const SELF_PROBE_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Minimal `GET /health/live` against our own listener over a raw socket.
///
/// Deliberately dependency-free (the binary has no HTTP *client*): we only need
/// to prove that the axum server still accepts a connection and answers, which
/// the status line alone establishes.
///
/// The read **accumulates across TCP segments** until the status line is
/// complete - the first `\n`, EOF, or [`SELF_PROBE_MAX_STATUS_BYTES`] - inside
/// ONE [`SELF_PROBE_IO_TIMEOUT`] budget. Requiring the whole status line inside
/// a single read judged a *fragmented* 200 dead (defect F4, fault-matrix M5a),
/// and three such ticks in a row call `exit(1)`: a load-dependent restart of a
/// bridge that was serving perfectly well.
async fn self_http_alive(target: &SelfHealthTarget) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let connect = tokio::time::timeout(
        SELF_PROBE_IO_TIMEOUT,
        tokio::net::TcpStream::connect((target.host.as_str(), target.port)),
    )
    .await;
    let mut stream = match connect {
        Ok(Ok(stream)) => stream,
        _ => return false,
    };

    let request = format!(
        "GET /health/live HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\n\r\n",
        target.host, target.port
    );
    if !matches!(
        tokio::time::timeout(SELF_PROBE_IO_TIMEOUT, stream.write_all(request.as_bytes())).await,
        Ok(Ok(()))
    ) {
        return false;
    }

    // Read until the status line is complete. The whole accumulation shares one
    // deadline, so a server that drips bytes forever cannot keep this probe
    // (and therefore the canary tick) alive indefinitely.
    let read_status_line = async {
        let mut buf: Vec<u8> = Vec::with_capacity(128);
        let mut chunk = [0u8; 128];
        loop {
            match stream.read(&mut chunk).await {
                // EOF: judge what actually arrived.
                Ok(0) => return Some(buf),
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.contains(&b'\n') || buf.len() >= SELF_PROBE_MAX_STATUS_BYTES {
                        return Some(buf);
                    }
                }
                // Read error: the socket is not answering.
                Err(_) => return None,
            }
        }
    };

    match tokio::time::timeout(SELF_PROBE_IO_TIMEOUT, read_status_line).await {
        Ok(Some(buf)) => status_line_is_200(&buf),
        _ => false,
    }
}

/// Is the first line of `buf` an HTTP 200 status line?
///
/// Only the version and the 3-digit code are required: the reason phrase is
/// optional in HTTP, so `HTTP/1.1 200` followed by EOF is a complete status line
/// while `HTTP/1.1 20` (truncated) is not.
fn status_line_is_200(buf: &[u8]) -> bool {
    let head = String::from_utf8_lossy(buf);
    let line = head.split(['\r', '\n']).next().unwrap_or("");
    let mut parts = line.splitn(3, ' ');
    let version = parts.next().unwrap_or("");
    let code = parts.next().unwrap_or("");
    version.starts_with("HTTP/1.") && code == "200"
}

impl std::fmt::Debug for IbkrClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IbkrClient")
            .field("config", &self.config)
            .field("connected", &self.inner.try_read().map(|g| g.is_some()).unwrap_or(false))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_ramps_then_goes_calm() {
        assert_eq!(IbkrClient::backoff(0), Duration::ZERO);
        // 500ms · 1.6^0, 1.6^1, 1.6^2 ...
        assert_eq!(IbkrClient::backoff(1), Duration::from_millis(500));
        assert_eq!(IbkrClient::backoff(2), Duration::from_millis(800));
        assert_eq!(IbkrClient::backoff(3), Duration::from_millis(1280));
        // 500ms · 1.6^10 = 54.975581389 s (the journal prints it truncated to ms)
        assert_eq!(IbkrClient::backoff(11).as_millis(), 54975);
        // Exponential ramp is capped at 60s while the outage is young.
        assert_eq!(IbkrClient::backoff(12), RECONNECT_FAST_CAP);
        assert_eq!(IbkrClient::backoff(14), RECONNECT_FAST_CAP);
        // ...and flat once it is clearly not transient.
        assert_eq!(IbkrClient::backoff(15), RECONNECT_CALM_INTERVAL);
        assert_eq!(IbkrClient::backoff(10_000), RECONNECT_CALM_INTERVAL);
        // Monotonic, never overflows, never panics (2026-08-26 regression).
        for n in 0..40u32 {
            assert!(IbkrClient::backoff(n) <= IbkrClient::backoff(n + 1));
        }
        assert_eq!(IbkrClient::backoff(u32::MAX), RECONNECT_CALM_INTERVAL);
    }

    #[test]
    fn self_health_target_maps_wildcards_to_loopback() {
        assert_eq!(SelfHealthTarget::loopback("0.0.0.0", 8881).host, "127.0.0.1");
        assert_eq!(SelfHealthTarget::loopback("::", 8881).host, "::1");
        assert_eq!(SelfHealthTarget::loopback("127.0.0.1", 8881).host, "127.0.0.1");
        assert_eq!(SelfHealthTarget::loopback("", 8881).host, "127.0.0.1");
    }

    #[tokio::test]
    async fn self_http_alive_reports_truthfully() {
        use tokio::io::AsyncWriteExt;

        // Dead port: must return false and must not hang.
        let dead = SelfHealthTarget {
            host: "127.0.0.1".to_string(),
            port: 4999,
        };
        assert!(!self_http_alive(&dead).await);

        // A listener that answers 200: must return true.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 12\r\n\r\n{\"alive\":1}\n")
                    .await;
            }
        });
        let alive = SelfHealthTarget {
            host: "127.0.0.1".to_string(),
            port: addr.port(),
        };
        assert!(self_http_alive(&alive).await);
    }

    #[tokio::test]
    async fn self_http_alive_is_false_for_non_200() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let _ = socket
                    .write_all(b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n")
                    .await;
            }
        });
        let target = SelfHealthTarget {
            host: "127.0.0.1".to_string(),
            port: addr.port(),
        };
        assert!(!self_http_alive(&target).await);
    }
}

/// Phase 0 fault-matrix coverage (PLAN-gateway-supervisor-refactor.md §6.2).
///
/// Deliberately in a module of its own: the qualification work must not touch
/// the upstream `tests` module. Both cases exercise the canary self-probe, the
/// only place where the MCP decides it is too broken to keep running.
#[cfg(test)]
mod fault_matrix_phase0 {
    use super::*;
    use std::io::Write as _;
    use std::net::TcpListener;

    /// Spawn a blocking HTTP server on an ephemeral port that writes `chunks`
    /// in order, sleeping `gap` between them, then holds the connection open -
    /// so that a *partial* response can be observed by the probe.
    fn spawn_chunked_server(chunks: Vec<Vec<u8>>, gap: Duration) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        std::thread::spawn(move || {
            if let Ok((mut socket, _)) = listener.accept() {
                for chunk in chunks {
                    if socket.write_all(&chunk).is_err() {
                        return;
                    }
                    let _ = socket.flush();
                    std::thread::sleep(gap);
                }
                std::thread::sleep(Duration::from_secs(5));
            }
        });
        port
    }

    /// M5a: a 200 whose status line is split across two TCP segments.
    ///
    /// FIXED 2026-09-22 (defect F4). MEASURED on the frozen snapshot: the probe
    /// performed ONE 64-byte read and required the status line inside that first
    /// chunk, so a fragmented 200 was judged DEAD and three such ticks in a row
    /// called `exit(1)` - a load-dependent restart with no broker involvement.
    /// The probe now accumulates reads until the status line is complete within
    /// its 5 s budget, so the same fragmented 200 is judged ALIVE.
    ///
    /// The assertion pins the FIXED behaviour: if it ever fails, the probe has
    /// regressed to the false positive that produced the pre-Phase-1 restart
    /// loop.
    #[tokio::test]
    async fn m5a_fragmented_status_line_is_judged_alive() {
        let port = spawn_chunked_server(
            vec![
                b"HTTP/1.1 ".to_vec(),
                b"200 OK\r\ncontent-length: 12\r\n\r\n{\"a\":1}\n".to_vec(),
            ],
            Duration::from_millis(300),
        );
        let target = SelfHealthTarget { host: "127.0.0.1".to_string(), port };
        assert!(
            self_http_alive(&target).await,
            "a 200 fragmented across TCP segments must be judged alive"
        );
    }

    /// M5c: a status line that never completes, then EOF, must still be judged
    /// DEAD. Accumulating reads must not degrade into "some bytes arrived",
    /// which would make the probe unable to detect a wedged server at all.
    #[tokio::test]
    async fn m5c_truncated_status_line_is_judged_dead() {
        let port = spawn_closing_server(
            vec![b"HTTP/1.1 20".to_vec()],
            Duration::from_millis(10),
        );
        let target = SelfHealthTarget { host: "127.0.0.1".to_string(), port };
        assert!(
            !self_http_alive(&target).await,
            "a truncated status line must be judged dead"
        );
    }

    /// Like [`spawn_chunked_server`], but CLOSES the connection after writing
    /// the chunks, so a truncated response can be observed as EOF.
    fn spawn_closing_server(chunks: Vec<Vec<u8>>, gap: Duration) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        std::thread::spawn(move || {
            if let Ok((mut socket, _)) = listener.accept() {
                for chunk in chunks {
                    if socket.write_all(&chunk).is_err() {
                        return;
                    }
                    let _ = socket.flush();
                    std::thread::sleep(gap);
                }
                drop(socket);
            }
        });
        port
    }

    /// M5b: the status line arrives complete but the body never does. The probe
    /// only needs the status line, so a partial response must still be judged
    /// ALIVE - a wedged process is one that says nothing at all.
    #[tokio::test]
    async fn m5b_headers_only_response_is_alive() {
        let port = spawn_chunked_server(
            vec![b"HTTP/1.1 200 OK\r\ncontent-length: 4096\r\n\r\n".to_vec()],
            Duration::from_secs(1),
        );
        let target = SelfHealthTarget { host: "127.0.0.1".to_string(), port };
        assert!(
            self_http_alive(&target).await,
            "a complete status line with an incomplete body must be alive"
        );
    }
}
