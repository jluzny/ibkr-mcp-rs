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
//! The loop uses exponential backoff capped at 15 s and retries indefinitely.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{info, warn};

use ibapi::prelude::*;

use crate::config::IbkrConfig;
use crate::ibkr::error::IbkrError;

/// Shared IBKR client state.
/// Stores `Arc<Client>` in `RwLock` for safe concurrent access and reconnection.
pub struct IbkrClient {
    pub config: IbkrConfig,
    inner: Arc<RwLock<Option<Arc<Client>>>>,
}

impl IbkrClient {
    pub fn new(config: IbkrConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            inner: Arc::new(RwLock::new(None)),
        })
    }

    /// Start background reconnection loop
    pub fn connect(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut attempts: u32 = 0;
            let max_delay = Duration::from_secs(60);
            let max_attempts_before_stall = 10;

            loop {
                let delay = Self::backoff(attempts, max_delay);
                if attempts > 0 {
                    info!(
                        attempt = attempts,
                        delay_ms = delay.as_millis(),
                        "Waiting before IBKR reconnect"
                    );
                    sleep(delay).await;
                }
                attempts += 1;

                // Stall the retry loop after many consecutive failures to avoid
                // hammering the gateway (e.g. during 2FA or IBKR maintenance).
                if attempts > max_attempts_before_stall {
                    let stall = Duration::from_secs(30);
                    warn!(stall_secs = stall.as_secs(), "Connection flapping detected, backing off");
                    sleep(stall).await;
                }

                // Pre-flight: verify the TCP endpoint is actually accepting connections.
                // Using Client::connect on a dead socket may return early eof from socat
                // when the IBKR API isn't yet listening (startup / 2FA / relogin).
                let target = format!("{}:{}", self.config.host, self.config.port);
                match tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(&target)).await {
                    Ok(Ok(_socket)) => {
                        // TCP is open — now attempt the IBKR handshake
                        drop(_socket);
                    }
                    _ => {
                        warn!(attempt = attempts, target = %target, "IBKR TCP endpoint not ready, skipping handshake attempt");
                        continue;
                    }
                }

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

                        // Wait until connection drops, then reconnect
                        self.maintain_connection().await;
                    }
                    Err(e) => {
                        warn!(attempt = attempts, error = %e, "IBKR connection failed");
                    }
                }
            }
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

    /// Polls connection health. Returns when connection is lost.
    ///
    /// To prevent IBKR rate-limiting (error 322), we exit the process
    /// instead of auto-reconnecting with the same client_id. Docker
    /// autoheal restarts the container with a clean subscription slate.
    async fn maintain_connection(&self) {
        loop {
            sleep(Duration::from_secs(5)).await;

            let guard = self.inner.read().await;
            let connected = guard
                .as_ref()
                .map(|c| c.is_connected())
                .unwrap_or(false);
            drop(guard);

            if !connected {
                warn!("Connection lost — exiting to let Docker restart with fresh subscriptions");
                std::process::exit(1);
            }
        }
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        let guard = self.inner.read().await;
        guard.as_ref().map(|c| c.is_connected()).unwrap_or(false)
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

    fn backoff(attempts: u32, max: Duration) -> Duration {
        if attempts == 0 {
            return Duration::ZERO;
        }
        let base = Duration::from_millis(500);
        let delay = base.mul_f64(1.6f64.powi(attempts as i32 - 1));
        delay.min(max)
    }
}

impl std::fmt::Debug for IbkrClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IbkrClient")
            .field("config", &self.config)
            .field("connected", &self.inner.try_read().map(|g| g.is_some()).unwrap_or(false))
            .finish()
    }
}
