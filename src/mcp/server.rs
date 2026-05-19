use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use rmcp::{
    transport::{
        StreamableHttpServerConfig,
        streamable_http_server::{
            session::local::LocalSessionManager,
            tower::StreamableHttpService,
        },
    },
};

use crate::config::McpConfig;
use crate::mcp::tools::IbkrMcpServer;

use rmcp::ServiceExt;

/// Start the MCP stdio server
pub async fn start_stdio(
    client: Arc<crate::ibkr::client::IbkrClient>,
) -> anyhow::Result<()> {
    info!("Starting MCP stdio server");
    let server = IbkrMcpServer::new(client);
    let running = server.serve(rmcp::transport::stdio()).await?;
    running.waiting().await?;
    Ok(())
}

/// Start the MCP HTTP server on the configured address
pub async fn start_http(
    client: Arc<crate::ibkr::client::IbkrClient>,
    config: McpConfig,
    ct: CancellationToken,
) -> anyhow::Result<()> {
    let addr = format!("{}:{}", config.host, config.port);
    info!(addr = %addr, "Starting MCP HTTP server");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    start_http_on(listener, client, ct).await
}

/// Start the MCP HTTP server on a pre-bound listener.
/// Useful for tests that need a random ephemeral port.
pub async fn start_http_on(
    listener: tokio::net::TcpListener,
    client: Arc<crate::ibkr::client::IbkrClient>,
    ct: CancellationToken,
) -> anyhow::Result<()> {
    let http_config = StreamableHttpServerConfig::default()
        .with_stateful_mode(false)
        .with_json_response(true)
        .with_cancellation_token(ct.child_token())
        .disable_allowed_hosts();
    start_http_on_with_config(listener, client, http_config, ct).await
}

/// Start the MCP HTTP server with a custom HTTP config.
pub async fn start_http_on_with_config(
    listener: tokio::net::TcpListener,
    client: Arc<crate::ibkr::client::IbkrClient>,
    http_config: StreamableHttpServerConfig,
    ct: CancellationToken,
) -> anyhow::Result<()> {
    info!(addr = ?listener.local_addr()?, "Starting MCP HTTP server");

    let client_for_factory = Arc::clone(&client);
    let service: StreamableHttpService<IbkrMcpServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || {
                Ok(IbkrMcpServer::new(Arc::clone(&client_for_factory)))
            },
            Default::default(),
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
