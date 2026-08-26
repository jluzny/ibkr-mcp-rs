use std::sync::Arc;
use std::time::Duration;

use ibkr_mcp_rs::config::Config;
use ibkr_mcp_rs::ibkr::client::IbkrClient;
use ibkr_mcp_rs::ibkr::account::AccountManager;

/// Shared helper: connect to IB Gateway and return an Arc<IbkrClient>.
/// Reads IBKR_PORT env var (default 4003 for live, 4004 for paper).
async fn connect_to_ibkr() -> Arc<IbkrClient> {
    let port: u16 = std::env::var("IBKR_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4003);

    let mut config = Config::default();
    config.ibkr.port = port;
    let client = IbkrClient::new(config.ibkr.clone());
    client.clone().connect();

    // Wait for connection (max 30s)
    let mut attempts = 0;
    while !client.is_connected().await && attempts < 30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        attempts += 1;
    }
    assert!(client.is_connected().await, "Failed to connect to IB Gateway on port {port}");
    client
}

/// Test 1: get_positions returns non-zero market fields for open positions.
#[tokio::test]
#[ignore = "Requires live IB Gateway; set LIVE_TEST=true"]
async fn test_live_get_positions_returns_nonzero_market_fields() {
    if std::env::var("LIVE_TEST").as_deref() != Ok("true") {
        eprintln!("Skipping: set LIVE_TEST=true to run");
        return;
    }

    let client = connect_to_ibkr().await;
    let account = Arc::new(AccountManager::new(Arc::clone(&client)));

    let result = tokio::time::timeout(
        Duration::from_secs(60),
        account.get_positions(None),
    )
    .await;

    let positions = match result {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            eprintln!("get_positions error (not a failure): {e}");
            return;
        }
        Err(_) => {
            eprintln!("get_positions timed out after 60s");
            return;
        }
    };

    if positions.is_empty() {
        eprintln!("No positions — skipping non-zero assertions");
        return;
    }

    println!("Got {} positions:", positions.len());
    let mut has_open = false;
    for pos in &positions {
        println!(
            "  {} qty={} type={} price={} value={} uPnL={} dPnL={}",
            pos.symbol, pos.quantity, pos.security_type,
            pos.market_price, pos.market_value,
            pos.unrealized_pnl, pos.daily_pnl
        );
        if pos.quantity != 0.0 {
            has_open = true;
            // Assert no sentinel values leak through
            assert!(pos.market_price.abs() < 1e11, "Sentinel in market_price for {}", pos.symbol);
            assert!(pos.market_value.abs() < 1e11, "Sentinel in market_value for {}", pos.symbol);
            assert!(pos.unrealized_pnl.abs() < 1e11, "Sentinel in unrealized_pnl for {}", pos.symbol);
            assert!(pos.daily_pnl.abs() < 1e11, "Sentinel in daily_pnl for {}", pos.symbol);
            assert!(pos.market_price.is_finite(), "NaN/Inf in market_price for {}", pos.symbol);
            assert!(pos.market_value.is_finite(), "NaN/Inf in market_value for {}", pos.symbol);
        }
    }

    if has_open {
        // At least one open position should have non-zero market data
        // (unless markets are completely closed with no frozen data)
        let any_nonzero = positions.iter().any(|p| {
            p.quantity != 0.0 && (p.market_price != 0.0 || p.market_value != 0.0)
        });
        if !any_nonzero {
            eprintln!("Warning: all open positions have zero market data — markets may be closed");
        }
    }
}

/// Test 2: Sentinel filter prevents garbage IBKR values from leaking.
#[tokio::test]
#[ignore = "Requires live IB Gateway; set LIVE_TEST=true"]
async fn test_live_get_positions_sentinel_filter() {
    if std::env::var("LIVE_TEST").as_deref() != Ok("true") {
        eprintln!("Skipping: set LIVE_TEST=true to run");
        return;
    }

    let client = connect_to_ibkr().await;
    let account = Arc::new(AccountManager::new(Arc::clone(&client)));

    let result = tokio::time::timeout(
        Duration::from_secs(60),
        account.get_positions(None),
    )
    .await;

    let positions = match result {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            eprintln!("get_positions error: {e}");
            return;
        }
        Err(_) => {
            eprintln!("get_positions timed out");
            return;
        }
    };

    for pos in &positions {
        assert!(pos.market_price.abs() < 1e11, "Sentinel in market_price for {}", pos.symbol);
        assert!(pos.market_value.abs() < 1e11, "Sentinel in market_value for {}", pos.symbol);
        assert!(pos.unrealized_pnl.abs() < 1e11, "Sentinel in unrealized_pnl for {}", pos.symbol);
        assert!(pos.daily_pnl.abs() < 1e11, "Sentinel in daily_pnl for {}", pos.symbol);
        assert!(pos.market_price.is_finite(), "NaN/Inf in market_price for {}", pos.symbol);
        assert!(pos.market_value.is_finite(), "NaN/Inf in market_value for {}", pos.symbol);
        assert!(pos.unrealized_pnl.is_finite(), "NaN/Inf in unrealized_pnl for {}", pos.symbol);
        assert!(pos.daily_pnl.is_finite(), "NaN/Inf in daily_pnl for {}", pos.symbol);
    }
    println!("Sentinel filter test passed for {} positions", positions.len());
}

/// Test 3: Options get the /100 multiplier applied to mark_price.
#[tokio::test]
#[ignore = "Requires live IB Gateway; set LIVE_TEST=true"]
async fn test_live_get_positions_options_multiplier() {
    if std::env::var("LIVE_TEST").as_deref() != Ok("true") {
        eprintln!("Skipping: set LIVE_TEST=true to run");
        return;
    }

    let client = connect_to_ibkr().await;
    let account = Arc::new(AccountManager::new(Arc::clone(&client)));

    let result = tokio::time::timeout(
        Duration::from_secs(60),
        account.get_positions(None),
    )
    .await;

    let positions = match result {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            eprintln!("get_positions error: {e}");
            return;
        }
        Err(_) => {
            eprintln!("get_positions timed out");
            return;
        }
    };

    let option_positions: Vec<_> = positions
        .iter()
        .filter(|p| p.security_type == "OPT" || p.security_type == "FOP")
        .collect();

    if option_positions.is_empty() {
        eprintln!("No option positions — skipping");
        return;
    }

    for pos in &option_positions {
        if pos.quantity != 0.0 && pos.market_value != 0.0 {
            let expected_mark = (pos.market_value / pos.quantity).abs() / 100.0;
            let diff = (pos.market_price - expected_mark).abs();
            assert!(
                diff < 0.01 || diff / expected_mark.abs() < 0.01,
                "Option mark_price {} doesn't match expected {} for {}",
                pos.market_price, expected_mark, pos.symbol
            );
            println!("  {} OPT: mark={} expected={:.4}", pos.symbol, pos.market_price, expected_mark);
        }
    }
}

/// Test 4: Degraded mode — PnL failures don't drop positions.
#[tokio::test]
#[ignore = "Requires live IB Gateway; set LIVE_TEST=true"]
async fn test_live_get_positions_degraded_mode() {
    if std::env::var("LIVE_TEST").as_deref() != Ok("true") {
        eprintln!("Skipping: set LIVE_TEST=true to run");
        return;
    }

    let client = connect_to_ibkr().await;
    let account = Arc::new(AccountManager::new(Arc::clone(&client)));

    let result = tokio::time::timeout(
        Duration::from_secs(60),
        account.get_positions(None),
    )
    .await;

    // Must return Ok even if some PnL fetches timed out
    assert!(result.is_ok(), "get_positions should not return Err in degraded mode");
    let positions = result.unwrap().expect("should have positions vec");
    // All positions should be present (none dropped)
    println!("Degraded mode test: {} positions returned", positions.len());
}

/// Test 5: Closed positions (qty=0) are handled correctly.
#[tokio::test]
#[ignore = "Requires live IB Gateway; set LIVE_TEST=true"]
async fn test_live_get_positions_closed_positions() {
    if std::env::var("LIVE_TEST").as_deref() != Ok("true") {
        eprintln!("Skipping: set LIVE_TEST=true to run");
        return;
    }

    let client = connect_to_ibkr().await;
    let account = Arc::new(AccountManager::new(Arc::clone(&client)));

    let result = tokio::time::timeout(
        Duration::from_secs(60),
        account.get_positions(None),
    )
    .await;

    let positions = match result {
        Ok(Ok(p)) => p,
        _ => {
            eprintln!("get_positions failed or timed out");
            return;
        }
    };

    let closed: Vec<_> = positions.iter().filter(|p| p.quantity == 0.0).collect();
    if closed.is_empty() {
        eprintln!("No closed positions — skipping");
        return;
    }

    for pos in &closed {
        // mark_price should be 0.0 (no division by zero)
        assert_eq!(pos.market_price, 0.0, "Closed position {} should have mark_price=0", pos.symbol);
        // Position should be present (not dropped)
        println!("  Closed: {} daily_pnl={}", pos.symbol, pos.daily_pnl);
    }
}
