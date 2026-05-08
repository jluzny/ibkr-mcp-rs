use std::sync::Arc;
use tokio;
use tokio_util::sync::CancellationToken;
use tracing::info;

use ibkr_mcp_rs::config::Config;
use ibkr_mcp_rs::ibkr::client::IbkrClient;
use ibkr_mcp_rs::logging;
use ibkr_mcp_rs::mcp::server::start_http;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load()?;
    logging::init_tracing(&config.logging.level, &config.logging.format);

    info!("Starting IBKR MCP Server");

    let client = IbkrClient::new(config.ibkr.clone());
    client.clone().connect();

    // Wait for initial connection
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    if client.is_connected().await {
        info!("IBKR connected successfully");
    } else {
        info!("IBKR connection pending, will retry in background...");
    }

    // Start MCP HTTP server
    let ct = CancellationToken::new();
    let server_handle = {
        let ct = ct.clone();
        let client = Arc::clone(&client);
        let mcp_config = config.mcp.clone();
        tokio::spawn(async move {
            if let Err(e) = start_http(client, mcp_config, ct).await {
                tracing::error!("MCP server error: {}", e);
            }
        })
    };

    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
        _ = server_handle => {
            info!("MCP server exited");
        }
    }

    ct.cancel();
    client.disconnect().await;
    info!("Shutdown complete");

    Ok(())
}
