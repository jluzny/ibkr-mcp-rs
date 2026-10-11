use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use rmcp::{
    transport::{
        StreamableHttpServerConfig,
        streamable_http_server::{
            session::local::{LocalSessionManager, SessionConfig},
            tower::StreamableHttpService,
        },
    },
};

use crate::config::{Config, McpConfig};
use crate::ibkr::account::AccountManager;
use crate::ibkr::events::EventManager;
use crate::ibkr::history::HistoricalDataManager;
use crate::ibkr::market_data::MarketDataManager;
use crate::ibkr::orders::OrderManager;
use crate::ibkr::scanner::ScannerManager;
use crate::mcp::tools::IbkrMcpServer;
use crate::policy::OrderGate;

use rmcp::ServiceExt;

/// Start the MCP stdio server
pub async fn start_stdio(
    client: Arc<crate::ibkr::client::IbkrClient>,
    config: Arc<Config>,
) -> anyhow::Result<()> {
    info!("Starting MCP stdio server");
    let server = IbkrMcpServer::new(client, config);
    let running = server.serve(rmcp::transport::stdio()).await?;
    running.waiting().await?;
    Ok(())
}

/// Bind the MCP HTTP listener for `config`.
///
/// Split out of [`start_http`] so the process entry point can bind *before*
/// spawning the server task: a listener that cannot bind must reach a non-zero
/// exit, not a log line followed by a clean `exit 0` (defect F3, fault-matrix
/// case M4).
pub async fn bind_http(config: &McpConfig) -> anyhow::Result<tokio::net::TcpListener> {
    let addr = format!("{}:{}", config.host, config.port);
    info!(addr = %addr, "Starting MCP HTTP server");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    Ok(listener)
}

/// Start the MCP HTTP server on the configured address
pub async fn start_http(
    client: Arc<crate::ibkr::client::IbkrClient>,
    config: Arc<Config>,
    ct: CancellationToken,
) -> anyhow::Result<()> {
    let listener = bind_http(&config.mcp).await?;
    start_http_on(listener, client, config, ct).await
}

/// Start the MCP HTTP server on a pre-bound listener.
/// Useful for tests that need a random ephemeral port.
pub async fn start_http_on(
    listener: tokio::net::TcpListener,
    client: Arc<crate::ibkr::client::IbkrClient>,
    config: Arc<Config>,
    ct: CancellationToken,
) -> anyhow::Result<()> {
    let http_config = StreamableHttpServerConfig::default()
        .with_stateful_mode(true)
        .with_json_response(false)
        .with_cancellation_token(ct.child_token())
        .disable_allowed_hosts();
    start_http_on_with_config(listener, client, config, http_config, ct).await
}

/// Start the MCP HTTP server with a custom HTTP config.
pub async fn start_http_on_with_config(
    listener: tokio::net::TcpListener,
    client: Arc<crate::ibkr::client::IbkrClient>,
    config: Arc<Config>,
    http_config: StreamableHttpServerConfig,
    ct: CancellationToken,
) -> anyhow::Result<()> {
    info!(addr = ?listener.local_addr()?, "Starting MCP HTTP server");

    // Create shared state managers once — they hold caches that must
    // survive across session boundaries (IBKR rate-limit [322] protection).
    let client_for_factory = Arc::clone(&client);
    let market_data = Arc::new(MarketDataManager::new(
        Arc::clone(&client),
        config.market_data.clone(),
    ));
    let account = Arc::new(AccountManager::new(Arc::clone(&client)));
    let orders = Arc::new(OrderManager::new(Arc::clone(&client)));
    // Scanner holds a process-wide concurrency semaphore (IBKR allows 10
    // concurrent scans) — it must be shared across sessions, not per-session.
    let history = Arc::new(HistoricalDataManager::new(Arc::clone(&client)));
    let scanner = Arc::new(ScannerManager::new(Arc::clone(&client)));
    let events = Arc::new(EventManager::new(Arc::clone(&client)));
    let gate = Arc::new(OrderGate::from_config(&config));

    // The D5 session timeout: `0`/unset keeps rmcp's 300 s idle reaper; a
    // non-zero value is the idle lifetime
    // (`ibkr-mcp-py/docs/DEVIATIONS.md` §D5 row 5).
    let mut session_config = SessionConfig::default();
    if config.mcp.session_timeout_secs > 0 {
        session_config.keep_alive =
            Some(std::time::Duration::from_secs(config.mcp.session_timeout_secs));
    }
    let mut session_manager = LocalSessionManager::default();
    session_manager.session_config = session_config;

    let service: StreamableHttpService<IbkrMcpServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || {
                Ok(IbkrMcpServer::new_shared(
                    Arc::clone(&client_for_factory),
                    Arc::clone(&market_data),
                    Arc::clone(&account),
                    Arc::clone(&orders),
                    Arc::clone(&history),
                    Arc::clone(&scanner),
                    Arc::clone(&events),
                    Arc::clone(&gate),
                ))
            },
            Arc::new(session_manager),
            http_config,
        );

    let router = axum::Router::new()
        .nest_service("/mcp", service)
        .merge(crate::mcp::health::health_router(Arc::clone(&client)));

    axum::serve(listener, router)
        .with_graceful_shutdown(async move { ct.cancelled_owned().await })
        .await?;

    Ok(())
}
