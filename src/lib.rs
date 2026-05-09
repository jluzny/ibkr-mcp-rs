//! # ibkr-mcp-rs
//!
//! Interactive Brokers MCP Server — exposes live brokerage data
//! (market data, account info, positions, orders) via the
//! [Model Context Protocol (MCP)](https://modelcontextprotocol.io/).
//!
//! ## Modules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`config`] | Layered YAML + env configuration |
//! | [`ibkr`] | IBKR gateway client, market data, account, orders |
//! | [`logging`] | Tracing subscriber setup |
//! | [`mcp`] | MCP server, tools, and health endpoints |
//!
//! ## Quick Example — HTTP Server
//!
//! ```no_run
//! use std::sync::Arc;
//! use ibkr_mcp_rs::config::Config;
//! use ibkr_mcp_rs::ibkr::client::IbkrClient;
//! use ibkr_mcp_rs::mcp::server::start_http;
//! use tokio_util::sync::CancellationToken;
//!
//! # async fn run() -> anyhow::Result<()> {
//! let config = Config::load()?;
//! let client = IbkrClient::new(config.ibkr.clone());
//! client.clone().connect();
//! let ct = CancellationToken::new();
//! start_http(Arc::new(client), config.mcp, ct).await?;
//! # Ok(()) }
//! ```

pub mod config;
pub mod ibkr;
pub mod logging;
pub mod mcp;
