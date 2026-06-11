use std::sync::Arc;
use std::time::Duration;

use ibkr_mcp_rs::config::Config;
use ibkr_mcp_rs::ibkr::client::IbkrClient;
use ibkr_mcp_rs::ibkr::orders::{OrderManager, WhatIfRequest};

/// Live integration test for the what_if_order functionality.
///
/// Requires IB Gateway or TWS to be running (paper or live).
/// Run with: cargo test --test what_if_test -- --ignored
///
/// Tests:
/// 1. SELL market order — should return margin/equity impact
/// 2. BUY market order — same (different direction)
/// 3. Invalid action — should return error
/// 4. Limit order without price — should return error

async fn connect_to_ibkr() -> Arc<IbkrClient> {
    let mut config = Config::default();
    // Use paper trading port by default (4002), override with env var for live
    config.ibkr.port = std::env::var("IBKR_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(4002);
    config.ibkr.paper_trading = std::env::var("IBKR_LIVE")
        .map(|v| v != "1")
        .unwrap_or(true);

    let client = IbkrClient::new(config.ibkr.clone());
    client.clone().connect();

    // Wait for connection
    let mut attempts = 0;
    while !client.is_connected().await && attempts < 30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        attempts += 1;
    }

    assert!(client.is_connected().await, "Failed to connect to IB Gateway after 30s");
    client
}

#[tokio::test]
#[ignore = "Requires live IB Gateway"]
async fn test_what_if_sell_market_order() {
    let client = connect_to_ibkr().await;
    let orders = Arc::new(OrderManager::new(Arc::clone(&client)));

    let result = orders.what_if_order(WhatIfRequest {
        symbol: "AAPL".to_string(),
        action: "SELL".to_string(),
        quantity: 100.0,
        order_type: "MKT".to_string(),
        price: None,
    }).await;

    match result {
        Ok(r) => {
            println!("=== SELL 100 AAPL MKT what-if result ===");
            println!("  status: {}", r.status);
            println!("  initialMarginBefore: {:?}", r.initial_margin_before);
            println!("  initialMarginChange: {:?}", r.initial_margin_change);
            println!("  initialMarginAfter: {:?}", r.initial_margin_after);
            println!("  maintenanceMarginBefore: {:?}", r.maintenance_margin_before);
            println!("  maintenanceMarginChange: {:?}", r.maintenance_margin_change);
            println!("  maintenanceMarginAfter: {:?}", r.maintenance_margin_after);
            println!("  equityWithLoanBefore: {:?}", r.equity_with_loan_before);
            println!("  equityWithLoanChange: {:?}", r.equity_with_loan_change);
            println!("  equityWithLoanAfter: {:?}", r.equity_with_loan_after);
            println!("  commission: {:?}", r.commission);
            println!("  commissionCurrency: {}", r.commission_currency);
            println!("  warningText: '{}'", r.warning_text);
            println!("  rejectReason: '{}'", r.reject_reason);

            // A what-if order should return some status
            assert!(!r.status.is_empty(), "Status should not be empty");
            // Should have margin data (even if some are 0)
            assert!(
                r.maintenance_margin_before.is_some() || r.maintenance_margin_change.is_some(),
                "Expected at least one maintenance margin field to be set"
            );
            assert_eq!(r.action, "SELL");
            assert_eq!(r.order_type, "MKT");
            assert_eq!(r.quantity, 100.0);
        }
        Err(e) => {
            panic!("what_if_order failed: {:?}", e);
        }
    }
}

#[tokio::test]
#[ignore = "Requires live IB Gateway"]
async fn test_what_if_buy_market_order() {
    let client = connect_to_ibkr().await;
    let orders = Arc::new(OrderManager::new(Arc::clone(&client)));

    let result = orders.what_if_order(WhatIfRequest {
        symbol: "AAPL".to_string(),
        action: "BUY".to_string(),
        quantity: 10.0,
        order_type: "MKT".to_string(),
        price: None,
    }).await;

    match result {
        Ok(r) => {
            println!("=== BUY 10 AAPL MKT what-if result ===");
            println!("  maintenanceMarginChange: {:?}", r.maintenance_margin_change);
            println!("  equityWithLoanChange: {:?}", r.equity_with_loan_change);
            println!("  commission: {:?}", r.commission);
            assert!(!r.status.is_empty());
            assert_eq!(r.action, "BUY");
        }
        Err(e) => {
            panic!("what_if_order BUY failed: {:?}", e);
        }
    }
}

#[tokio::test]
#[ignore = "Requires live IB Gateway"]
async fn test_what_if_sell_limit_order() {
    let client = connect_to_ibkr().await;
    let orders = Arc::new(OrderManager::new(Arc::clone(&client)));

    let result = orders.what_if_order(WhatIfRequest {
        symbol: "AAPL".to_string(),
        action: "SELL".to_string(),
        quantity: 100.0,
        order_type: "LMT".to_string(),
        price: Some(200.0),
    }).await;

    match result {
        Ok(r) => {
            println!("=== SELL 100 AAPL LMT @200 what-if result ===");
            println!("  maintenanceMarginChange: {:?}", r.maintenance_margin_change);
            println!("  commission: {:?}", r.commission);
            assert!(!r.status.is_empty());
            assert_eq!(r.order_type, "LMT");
        }
        Err(e) => {
            panic!("what_if_order LMT failed: {:?}", e);
        }
    }
}

#[tokio::test]
#[ignore = "Requires live IB Gateway"]
async fn test_what_if_invalid_action() {
    let client = connect_to_ibkr().await;
    let orders = Arc::new(OrderManager::new(Arc::clone(&client)));

    let result = orders.what_if_order(WhatIfRequest {
        symbol: "AAPL".to_string(),
        action: "HODL".to_string(),
        quantity: 100.0,
        order_type: "MKT".to_string(),
        price: None,
    }).await;

    assert!(result.is_err(), "Expected error for invalid action");
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Invalid action") || msg.contains("HODL"),
        "Expected 'Invalid action' error, got: {}",
        msg
    );
}

#[tokio::test]
#[ignore = "Requires live IB Gateway"]
async fn test_what_if_limit_without_price() {
    let client = connect_to_ibkr().await;
    let orders = Arc::new(OrderManager::new(Arc::clone(&client)));

    let result = orders.what_if_order(WhatIfRequest {
        symbol: "AAPL".to_string(),
        action: "SELL".to_string(),
        quantity: 100.0,
        order_type: "LMT".to_string(),
        price: None,  // Missing limit price
    }).await;

    assert!(result.is_err(), "Expected error for LMT without price");
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Limit order requires a price") || msg.contains("price"),
        "Expected 'Limit order requires a price' error, got: {}",
        msg
    );
}

#[tokio::test]
#[ignore = "Requires live IB Gateway"]
async fn test_what_if_margin_relief_ranking() {
    // Test the core use case: comparing margin relief across positions
    let client = connect_to_ibkr().await;
    let orders = Arc::new(OrderManager::new(Arc::clone(&client)));

    let symbols = vec!["AAPL", "MSFT", "GOOGL"];
    let mut results = Vec::new();

    for symbol in &symbols {
        let result = orders.what_if_order(WhatIfRequest {
            symbol: symbol.to_string(),
            action: "SELL".to_string(),
            quantity: 100.0,
            order_type: "MKT".to_string(),
            price: None,
        }).await;

        match result {
            Ok(r) => {
                println!(
                    "  {}: marginChange={:?}, equityChange={:?}",
                    symbol, r.maintenance_margin_change, r.equity_with_loan_change
                );
                results.push((symbol, r.maintenance_margin_change, r.equity_with_loan_change));
            }
            Err(e) => {
                println!("  {}: ERROR — {}", symbol, e);
            }
        }
    }

    // We should have gotten at least one successful result
    assert!(!results.is_empty(), "Expected at least one successful what-if result");
}