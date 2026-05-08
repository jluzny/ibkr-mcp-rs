use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use ibkr_mcp_rs::config::{Config, McpConfig};
use ibkr_mcp_rs::ibkr::client::IbkrClient;
use ibkr_mcp_rs::mcp::server::start_http_on_with_config;
use rmcp::transport::StreamableHttpServerConfig;

/// Start the MCP server on a random ephemeral port for testing.
async fn start_test_server() -> (String, CancellationToken) {
    let ct = CancellationToken::new();
    let config = Config::default();
    let client = IbkrClient::new(config.ibkr.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    let http_config = StreamableHttpServerConfig::default()
        .with_stateful_mode(false)
        .with_json_response(true)
        .with_cancellation_token(ct.child_token());

    let ct_clone = ct.clone();
    tokio::spawn(async move {
        let _ = start_http_on_with_config(listener, client, http_config, ct_clone).await;
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    (url, ct)
}

#[tokio::test]
async fn test_health_live() {
    let (base_url, _ct) = start_test_server().await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/health/live", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["alive"], true);
}

#[tokio::test]
async fn test_health_ready_broker_not_connected() {
    let (base_url, _ct) = start_test_server().await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/health/ready", base_url))
        .send()
        .await
        .unwrap();

    // Broker is not connected, so ready should be false
    assert_eq!(resp.status(), 503);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["brokerConnected"], false);
    assert_eq!(body["mcpServer"], true);
}

#[tokio::test]
async fn test_version_endpoint() {
    let (base_url, _ct) = start_test_server().await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/version", base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["service"], "ibkr-mcp-rs");
    assert!(body["version"].as_str().unwrap().len() > 0);
    assert_eq!(body["transport"], "streamable-http");
}

#[tokio::test]
async fn test_mcp_initialize_handshake() {
    let (base_url, _ct) = start_test_server().await;

    let client = reqwest::Client::new();

    // Step 1: Send initialize request
    let init_request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "test-client",
                "version": "1.0.0"
            }
        }
    });

    let resp = client
        .post(format!("{}/mcp", base_url))
        .json(&init_request)
        .header("Accept", "application/json, text/event-stream")
        .send()
        .await
        .unwrap();

    // The response may be SSE or JSON depending on config.
    // For stateful mode with SSE, we need to read the stream.
    // For our default config, it returns JSON for simple requests
    // or SSE for session-based. Let's read as text and check it contains
    // the expected response.
    let status = resp.status();
    let body = resp.text().await.unwrap();
    println!("MCP initialize response status: {}", status);
    println!("MCP initialize response body: {}", body);
    assert_eq!(status, 200);

    // Should contain a JSON-RPC response with result
    assert!(
        body.contains("jsonrpc"),
        "Response should be JSON-RPC, got: {}",
        body
    );
    assert!(
        body.contains("result") || body.contains("capabilities"),
        "Response should contain result or capabilities, got: {}",
        body
    );
}

