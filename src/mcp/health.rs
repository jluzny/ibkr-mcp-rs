use std::sync::Arc;
use axum::{
    routing::get,
    Router,
    Json,
    http::StatusCode,
};
use serde_json::json;

use crate::ibkr::client::IbkrClient;
use crate::persistence;

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
        Json(json!({
            "ready": broker_connected,
            "brokerConnected": broker_connected,
            "mcpServer": true,
        })),
    )
}

async fn health_live() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(json!({"alive": true})),
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

async fn health_broker(
    axum::extract::State(client): axum::extract::State<Arc<IbkrClient>>,
) -> (StatusCode, Json<serde_json::Value>) {
    let connected = client.is_connected().await;
    (
        StatusCode::OK,
        Json(json!({
            "brokerConnected": connected,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        })),
    )
}

async fn version() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(json!({
            "service": "ibkr-mcp-rs",
            "version": env!("CARGO_PKG_VERSION"),
            "transport": "streamable-http",
        })),
    )
}
