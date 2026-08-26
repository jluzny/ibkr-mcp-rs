use std::sync::Arc;
use std::time::Duration;

use ibkr_mcp_rs::config::Config;
use ibkr_mcp_rs::ibkr::client::IbkrClient;
use ibkr_mcp_rs::ibkr::market_data::MarketDataManager;

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

    let mut attempts = 0;
    while !client.is_connected().await && attempts < 30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        attempts += 1;
    }
    assert!(client.is_connected().await, "Failed to connect to IB Gateway on port {port}");
    client
}

/// Test 1: AAPL option chain succeeds (liquid name, should already work).
#[tokio::test]
#[ignore = "Requires live IB Gateway; set LIVE_TEST=true"]
async fn test_live_option_chain_aapl() {
    if std::env::var("LIVE_TEST").as_deref() != Ok("true") {
        eprintln!("Skipping: set LIVE_TEST=true to run");
        return;
    }

    let client = connect_to_ibkr().await;
    let market_data = Arc::new(MarketDataManager::new(
        Arc::clone(&client),
        Config::default().market_data,
    ));

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        market_data.get_option_chain("AAPL"),
    )
    .await;

    match result {
        Ok(Ok(chain)) => {
            println!("AAPL: {} expirations, {} strikes, conID={}",
                chain.expirations.len(), chain.strikes.len(), chain.underlying_contract_id);
            assert!(!chain.expirations.is_empty(), "AAPL should have expirations");
            assert!(!chain.strikes.is_empty(), "AAPL should have strikes");
            // AAPL's real conID is 265598
            assert_eq!(chain.underlying_contract_id, 265598,
                "AAPL underlying_contract_id should be 265598");
        }
        Ok(Err(e)) => panic!("AAPL option chain failed: {e}"),
        Err(_) => panic!("AAPL option chain timed out"),
    }
}

/// Test 2: SPY option chain succeeds (liquid ETF).
#[tokio::test]
#[ignore = "Requires live IB Gateway; set LIVE_TEST=true"]
async fn test_live_option_chain_spy() {
    if std::env::var("LIVE_TEST").as_deref() != Ok("true") {
        eprintln!("Skipping: set LIVE_TEST=true to run");
        return;
    }

    let client = connect_to_ibkr().await;
    let market_data = Arc::new(MarketDataManager::new(
        Arc::clone(&client),
        Config::default().market_data,
    ));

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        market_data.get_option_chain("SPY"),
    )
    .await;

    match result {
        Ok(Ok(chain)) => {
            println!("SPY: {} expirations, {} strikes, conID={}",
                chain.expirations.len(), chain.strikes.len(), chain.underlying_contract_id);
            assert!(!chain.expirations.is_empty(), "SPY should have expirations");
        }
        Ok(Err(e)) => panic!("SPY option chain failed: {e}"),
        Err(_) => panic!("SPY option chain timed out"),
    }
}

/// Test 3: INIO option chain succeeds (previously failing with 321).
#[tokio::test]
#[ignore = "Requires live IB Gateway; set LIVE_TEST=true"]
async fn test_live_option_chain_inio() {
    if std::env::var("LIVE_TEST").as_deref() != Ok("true") {
        eprintln!("Skipping: set LIVE_TEST=true to run");
        return;
    }

    let client = connect_to_ibkr().await;
    let market_data = Arc::new(MarketDataManager::new(
        Arc::clone(&client),
        Config::default().market_data,
    ));

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        market_data.get_option_chain("INIO"),
    )
    .await;

    match result {
        Ok(Ok(chain)) => {
            println!("INIO: {} expirations, {} strikes, conID={}",
                chain.expirations.len(), chain.strikes.len(), chain.underlying_contract_id);
            // INIO may or may not have options — but it should NOT 321
            if chain.expirations.is_empty() {
                eprintln!("INIO has no option expirations (may not have options listed)");
            }
        }
        Ok(Err(e)) => {
            let msg = e.to_string();
            if msg.contains("321") {
                panic!("INIO still getting 321 error — fix not working: {msg}");
            }
            eprintln!("INIO option chain error (not 321): {msg}");
        }
        Err(_) => panic!("INIO option chain timed out"),
    }
}

/// Test 4: XYZ option chain succeeds (previously failing with 321).
#[tokio::test]
#[ignore = "Requires live IB Gateway; set LIVE_TEST=true"]
async fn test_live_option_chain_xyz() {
    if std::env::var("LIVE_TEST").as_deref() != Ok("true") {
        eprintln!("Skipping: set LIVE_TEST=true to run");
        return;
    }

    let client = connect_to_ibkr().await;
    let market_data = Arc::new(MarketDataManager::new(
        Arc::clone(&client),
        Config::default().market_data,
    ));

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        market_data.get_option_chain("XYZ"),
    )
    .await;

    match result {
        Ok(Ok(chain)) => {
            println!("XYZ: {} expirations, {} strikes, conID={}",
                chain.expirations.len(), chain.strikes.len(), chain.underlying_contract_id);
        }
        Ok(Err(e)) => {
            let msg = e.to_string();
            if msg.contains("321") {
                panic!("XYZ still getting 321 error — fix not working: {msg}");
            }
            eprintln!("XYZ option chain error (not 321): {msg}");
        }
        Err(_) => panic!("XYZ option chain timed out"),
    }
}

/// Test 5: Rapid repeat — no rate-limit on contract_details + option_chain.
#[tokio::test]
#[ignore = "Requires live IB Gateway; set LIVE_TEST=true"]
async fn test_live_option_chain_rapid_repeat() {
    if std::env::var("LIVE_TEST").as_deref() != Ok("true") {
        eprintln!("Skipping: set LIVE_TEST=true to run");
        return;
    }

    let client = connect_to_ibkr().await;
    let market_data = Arc::new(MarketDataManager::new(
        Arc::clone(&client),
        Config::default().market_data,
    ));

    for i in 0..3 {
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            market_data.get_option_chain("AAPL"),
        )
        .await;

        match result {
            Ok(Ok(chain)) => {
                println!("Call {}: {} expirations", i + 1, chain.expirations.len());
            }
            Ok(Err(e)) => {
                let msg = e.to_string();
                if msg.contains("322") {
                    panic!("Rate-limited on call {}: {msg}", i + 1);
                }
                panic!("Error on call {}: {msg}", i + 1);
            }
            Err(_) => panic!("Timed out on call {}", i + 1),
        }
    }
    println!("Rapid repeat test passed — no rate-limiting");
}
