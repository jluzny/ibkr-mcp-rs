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
            let max_delay = Duration::from_secs(15);

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

        info!(url = %url, client_id = self.config.client_id, "Connecting to IBKR");

        match tokio::time::timeout(timeout, Client::connect(&url, self.config.client_id)).await {
            Ok(Ok(client)) => Ok(client),
            Ok(Err(e)) => Err(IbkrError::ConnectionFailed(e.to_string())),
            Err(_) => Err(IbkrError::ConnectionFailed("timeout".into())),
        }
    }

    /// Polls connection health. Returns when connection is lost.
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
                info!("Connection lost, will reconnect");
                return;
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
