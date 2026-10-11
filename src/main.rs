use std::sync::Arc;
use tokio;
use tokio_util::sync::CancellationToken;
use tracing::info;

use ibkr_mcp_rs::config::Config;
use ibkr_mcp_rs::ibkr::client::{IbkrClient, SelfHealthTarget};
use ibkr_mcp_rs::logging;
use ibkr_mcp_rs::mcp::server::{bind_http, start_http_on, start_stdio};

/// Exit code for "the bridge is not serving MCP": the HTTP listener could not be
/// bound, the server task failed, or it stopped without a shutdown request.
///
/// Deliberately **non-zero**. Defect F3 (fault-matrix case M4) was that a bind
/// failure was logged and then followed by a clean `exit 0`, which is exactly
/// what a deliberate stop looks like to systemd - so `Restart=on-failure` would
/// have been defeated and a bridge that never came up would have reported
/// success.
const EXIT_SERVER_FAILED: i32 = 1;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let use_stdio = std::env::args().any(|a| a == "--stdio");

    let config = Config::load()?;
    logging::init_tracing(&config.logging.level, &config.logging.format);

    // The health/tool-authorization contract version is a member of the
    // deployment tuple (PLAN 6.1): record it before anything can be served, so
    // /version, /health/live and /health/ready report the value that the
    // artifact manifest, config and unit all declare.
    ibkr_mcp_rs::mcp::health::set_contract_version(config.health.contract_version.clone());

    info!(
        stdio = use_stdio,
        contract_version = %config.health.contract_version,
        client_id = config.ibkr.client_id,
        "Starting IBKR MCP Server"
    );

    // Load any persisted last-good snapshot before taking traffic
    ibkr_mcp_rs::persistence::load_from_disk();

    let client = IbkrClient::new(config.ibkr.clone());
    client.clone().connect();

    // The canary's self-health probe targets our own HTTP listener. There is no
    // HTTP server in stdio mode, so the canary has nothing to exit on there.
    let self_health = if use_stdio {
        None
    } else {
        Some(SelfHealthTarget::loopback(&config.mcp.host, config.mcp.port))
    };
    client.start_canary(self_health);

    // Wait for initial connection
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    if client.is_connected().await {
        info!("IBKR connected successfully");
    } else {
        info!("IBKR connection pending, will retry in background...");
    }

    if use_stdio {
        // Stdio mode: serve directly over stdin/stdout
        if let Err(e) = start_stdio(Arc::clone(&client)).await {
            // Same rule as HTTP mode: a server that FAILED must never look
            // like a deliberate stop (defect F3).
            tracing::error!("MCP stdio server error: {}", e);
            std::process::exit(EXIT_SERVER_FAILED);
        }
    } else {
        // HTTP mode. Bind the listener BEFORE spawning the server task, so a
        // listener that cannot bind is a non-zero exit instead of a logged
        // error followed by a clean exit 0 (defect F3 / fault-matrix M4: the
        // bind error used to be swallowed by the spawned task, which then
        // completed normally and let the process fall through to `Ok(())`).
        let listener = match bind_http(&config.mcp).await {
            Ok(listener) => listener,
            Err(e) => {
                tracing::error!("MCP HTTP server error: {}", e);
                std::process::exit(EXIT_SERVER_FAILED);
            }
        };

        let ct = CancellationToken::new();
        let server_handle = {
            let ct = ct.clone();
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                let res = start_http_on(listener, client, ct).await;
                if let Err(e) = &res {
                    tracing::error!("MCP HTTP server error: {}", e);
                }
                res
            })
        };

        // Wait for shutdown signal. Every way the HTTP task can end without a
        // shutdown request means the bridge stopped serving MCP: `JoinError` is
        // a panic inside the spawned server, `Err` is a server failure, and a
        // plain `Ok` is a serve loop that returned without the cancellation
        // token being cancelled. All of them exit NON-ZERO, so systemd restarts
        // us instead of recording a clean stop.
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal");
            }
            res = server_handle => {
                match res {
                    Ok(Ok(())) => {
                        tracing::error!("MCP HTTP server exited without a shutdown request — exiting");
                        std::process::exit(EXIT_SERVER_FAILED);
                    }
                    Ok(Err(e)) => {
                        tracing::error!(error = %e, "MCP HTTP server failed — exiting");
                        std::process::exit(EXIT_SERVER_FAILED);
                    }
                    Err(e) if e.is_panic() => {
                        tracing::error!(error = %e, "MCP HTTP server panicked — exiting");
                        std::process::exit(EXIT_SERVER_FAILED);
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "MCP HTTP server task ended abnormally — exiting");
                        std::process::exit(EXIT_SERVER_FAILED);
                    }
                }
            }
        }

        ct.cancel();
    }

    client.disconnect().await;
    info!("Shutdown complete");

    Ok(())
}
