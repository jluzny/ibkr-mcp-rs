use std::sync::{Arc, OnceLock};
use axum::{
    routing::get,
    Router,
    Json,
    http::StatusCode,
};
use serde_json::json;

use crate::ibkr::client::IbkrClient;
use crate::persistence;

/// The contract version reported when nothing was configured. It exists so that
/// dev runs, the fault-matrix scratch instances and library users still start;
/// a deployed process records the tuple's explicit value via
/// [`set_contract_version`] (PLAN §6.1 — the deployed config and unit both
/// declare it, and a mismatch fails the pre-install gate).
pub const DEFAULT_CONTRACT_VERSION: &str = "legacy-v0";

/// Health / tool-authorization contract version of this process, fixed for its
/// lifetime. Written once from `health.contract_version` at startup.
static CONTRACT_VERSION: OnceLock<Arc<str>> = OnceLock::new();

/// Record the configured contract version. The first call wins: the version is a
/// property of the deployment, not of a request.
pub fn set_contract_version(version: impl Into<Arc<str>>) {
    let _ = CONTRACT_VERSION.set(version.into());
}

/// The contract version this process reports on `/version`, `/health/live` and
/// `/health/ready`.
pub fn contract_version() -> Arc<str> {
    CONTRACT_VERSION
        .get()
        .cloned()
        .unwrap_or_else(|| Arc::from(DEFAULT_CONTRACT_VERSION))
}

/// Attach the contract version to a health/version payload. Consumers that must
/// fail closed on an unknown contract (PLAN §6.1) read it from here instead of
/// inferring it from the process or the HTTP status.
fn with_contract_version(mut payload: serde_json::Value) -> serde_json::Value {
    payload["contractVersion"] = json!(contract_version().as_ref());
    payload
}

/// Build health check router
pub fn health_router(client: Arc<IbkrClient>) -> Router {
    Router::new()
        .route("/health/ready", get(health_ready))
        .route("/health/live", get(health_live))
        .route("/health/broker", get(health_broker))
        .route("/health/last-good", get(health_last_good))
        .route("/version", get(version))
        .with_state(client)
}

async fn health_ready(
    axum::extract::State(client): axum::extract::State<Arc<IbkrClient>>,
) -> (StatusCode, Json<serde_json::Value>) {
    let broker_connected = client.is_connected().await;
    let status = if broker_connected {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        Json(with_contract_version(json!({
            "ready": broker_connected,
            "brokerConnected": broker_connected,
            "mcpServer": true,
        }))),
    )
}

async fn health_live() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(with_contract_version(json!({"alive": true}))),
    )
}

/// Exposes the last-known-good account snapshot regardless of current
/// connection state. Callers can check `ageSecs` and decide for themselves
/// whether to act on it — turns "broker unreachable" from a hard 500 into
/// a data-quality signal (fresh / stale / very-stale).
async fn health_last_good() -> (StatusCode, Json<serde_json::Value>) {
    match persistence::get() {
        Some(state) => (
            StatusCode::OK,
            Json(json!({
                "available": true,
                "ageSecs": persistence::age_secs(),
                "fetchedAtUnix": state.fetched_at_unix,
                "loadedFromDisk": state.loaded_from_disk,
                "account": state.account,
            })),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "available": false,
                "reason": "no snapshot recorded yet this process and none on disk",
            })),
        ),
    }
}

/// Broker link status. Always `200`: a broker outage is a data-quality
/// signal, not an HTTP failure — `/health/ready` is the endpoint that carries
/// the 503 for consumers that must not act without a live broker.
async fn health_broker(
    axum::extract::State(client): axum::extract::State<Arc<IbkrClient>>,
) -> (StatusCode, Json<serde_json::Value>) {
    let connected = client.is_connected().await;
    (
        StatusCode::OK,
        Json(json!({
            "brokerConnected": connected,
            "consecutiveFailures": client.broker_failures(),
            "outageSeconds": client.broker_outage_secs(),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        })),
    )
}

async fn version() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(with_contract_version(json!({
            "service": "ibkr-mcp-rs",
            "version": env!("CARGO_PKG_VERSION"),
            "transport": "streamable-http",
        }))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PLAN §6.1: the contract version is explicit in the health output, not
    /// inferred from the process or the HTTP status.
    #[test]
    fn health_payloads_carry_the_contract_version() {
        set_contract_version(DEFAULT_CONTRACT_VERSION);
        assert_eq!(contract_version().as_ref(), DEFAULT_CONTRACT_VERSION);

        let live = with_contract_version(json!({"alive": true}));
        assert_eq!(live["alive"], true);
        assert_eq!(live["contractVersion"], DEFAULT_CONTRACT_VERSION);

        let version = with_contract_version(json!({"service": "ibkr-mcp-rs"}));
        assert_eq!(version["contractVersion"], DEFAULT_CONTRACT_VERSION);
    }
}
