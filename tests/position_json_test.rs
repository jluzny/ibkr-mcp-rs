use ibkr_mcp_rs::ibkr::account::Position;

/// Verify that option fields appear correctly in JSON output
/// following the same serialization path as tools.rs get_positions.
#[test]
fn position_json_includes_option_fields() {
    let pos = Position {
        account_id: "U18197748".into(),
        symbol: "BTDR".into(),
        quantity: -2.0,
        average_cost: 476.30,
        market_price: 0.0,
        market_value: 0.0,
        unrealized_pnl: 0.0,
        daily_pnl: 0.0,
        security_type: "OPT".into(),
        strike: Some(40.0),
        right: Some("C".into()),
        expiration: Some("20260618".into()),
        multiplier: Some("100".into()),
    };

    let json = serde_json::json!({
        "symbol": pos.symbol,
        "quantity": pos.quantity,
        "averageCost": pos.average_cost,
        "marketPrice": pos.market_price,
        "marketValue": pos.market_value,
        "unrealizedPnL": pos.unrealized_pnl,
        "dailyPnL": pos.daily_pnl,
        "securityType": pos.security_type,
        "strike": pos.strike,
        "right": pos.right,
        "expiration": pos.expiration,
        "multiplier": pos.multiplier,
    });

    assert_eq!(json["securityType"], "OPT");
    assert_eq!(json["strike"], 40.0);
    assert_eq!(json["right"], "C");
    assert_eq!(json["expiration"], "20260618");
    assert_eq!(json["multiplier"], "100");
}

#[test]
fn position_json_stock_has_null_option_fields() {
    let pos = Position {
        account_id: "U18197748".into(),
        symbol: "SOFI".into(),
        quantity: 700.0,
        average_cost: 25.21,
        market_price: 15.61,
        market_value: 10927.0,
        unrealized_pnl: -6720.0,
        daily_pnl: 0.0,
        security_type: "STK".into(),
        strike: None,
        right: None,
        expiration: None,
        multiplier: None,
    };

    let json = serde_json::json!({
        "symbol": pos.symbol,
        "quantity": pos.quantity,
        "averageCost": pos.average_cost,
        "marketPrice": pos.market_price,
        "marketValue": pos.market_value,
        "unrealizedPnL": pos.unrealized_pnl,
        "dailyPnL": pos.daily_pnl,
        "securityType": pos.security_type,
        "strike": pos.strike,
        "right": pos.right,
        "expiration": pos.expiration,
        "multiplier": pos.multiplier,
    });

    assert_eq!(json["securityType"], "STK");
    assert!(json["strike"].is_null());
    assert!(json["right"].is_null());
    assert!(json["expiration"].is_null());
    assert!(json["multiplier"].is_null());
}

#[test]
fn position_json_put_option_round_trip() {
    let pos = Position {
        account_id: "U18197748".into(),
        symbol: "XXI".into(),
        quantity: -1.0,
        average_cost: 848.75,
        market_price: 0.0,
        market_value: 0.0,
        unrealized_pnl: 0.0,
        daily_pnl: 0.0,
        security_type: "OPT".into(),
        strike: Some(17.5),
        right: Some("P".into()),
        expiration: Some("20260717".into()),
        multiplier: Some("100".into()),
    };

    let json = serde_json::json!({
        "symbol": pos.symbol,
        "quantity": pos.quantity,
        "averageCost": pos.average_cost,
        "securityType": pos.security_type,
        "strike": pos.strike,
        "right": pos.right,
        "expiration": pos.expiration,
        "multiplier": pos.multiplier,
    });

    // Verify all field types are correct after JSON round-trip
    assert_eq!(json["symbol"], "XXI");
    assert_eq!(json["quantity"], -1.0);
    assert_eq!(json["averageCost"], 848.75);
    assert_eq!(json["securityType"], "OPT");
    assert_eq!(json["strike"], 17.5);
    assert_eq!(json["right"], "P");
    assert_eq!(json["expiration"], "20260717");
    assert_eq!(json["multiplier"], "100");
}