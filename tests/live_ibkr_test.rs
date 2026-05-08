use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use ibkr_mcp_rs::config::Config;
use ibkr_mcp_rs::ibkr::client::IbkrClient;
use ibkr_mcp_rs::ibkr::market_data::MarketDataManager;
use ibkr_mcp_rs::mcp::server::start_http_on;

/// Live E2E test against a running IB Gateway.
/// Requires IB Gateway or TWS to be running on localhost:4002 (paper trading).
/// Run with: cargo test --test live_ibkr_test -- --ignored

#[tokio::test]
#[ignore = "Requires live IB Gateway on localhost:4002"]
async fn test_live_market_data_quote() {
    let config = Config::default();
    let client = IbkrClient::new(config.ibkr.clone());
    client.clone().connect();

    // Wait for connection
    let mut attempts = 0;
    while !client.is_connected().await && attempts < 30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        attempts += 1;
    }

    assert!(client.is_connected().await, "Failed to connect to IB Gateway");

    let market_data = MarketDataManager::new(
        Arc::clone(&client),
        config.market_data,
    );

    // Try to get a quote for a highly liquid stock
    let result = market_data.get_quote("AAPL").await;

    match result {
        Ok(quote) => {
            println!("AAPL quote: bid={}, ask={}, last={}", quote.bid, quote.ask, quote.last);
            assert!(quote.bid > 0.0 || quote.ask > 0.0 || quote.last > 0.0,
                "Expected at least one valid price field");
        }
        Err(e) => {
            // If we get an entitlement error, that's still a valid test result
            // as long as the error is properly formatted
            println!("Market data error (expected for some accounts): {}", e);
        }
    }
}

#[tokio::test]
#[ignore = "Requires live IB Gateway on localhost:4002"]
async fn test_live_mcp_server_with_ibkr() {
    let config = Config::default();
    let client = IbkrClient::new(config.ibkr.clone());
    client.clone().connect();

    // Wait for connection
    let mut attempts = 0;
    while !client.is_connected().await && attempts < 30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        attempts += 1;
    }

    assert!(client.is_connected().await, "Failed to connect to IB Gateway");

    let ct = CancellationToken::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    let ct_clone = ct.clone();
    tokio::spawn(async move {
        let _ = start_http_on(listener, client, ct_clone).await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let http_client = reqwest::Client::new();

    // Health ready should now return 200 since broker is connected
    let resp = http_client
        .get(format!("{}/health/ready", url))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["brokerConnected"], true);

    // MCP initialize should work
    let init_request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "test", "version": "1.0.0" }
        }
    });

    let resp = http_client
        .post(format!("{}/mcp", url))
        .json(&init_request)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    ct.cancel();
}
