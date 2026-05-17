use super::*;

#[test]
fn position_stock_has_null_option_fields() {
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
    assert_eq!(pos.security_type, "STK");
    assert!(pos.strike.is_none());
    assert!(pos.right.is_none());
    assert!(pos.expiration.is_none());
    assert!(pos.multiplier.is_none());
}

#[test]
fn position_option_has_populated_fields() {
    let pos = Position {
        account_id: "U18197748".into(),
        symbol: "IBIT".into(),
        quantity: -1.0,
        average_cost: 254.29,
        market_price: 0.0,
        market_value: 0.0,
        unrealized_pnl: 0.0,
        daily_pnl: 0.0,
        security_type: "OPT".into(),
        strike: Some(45.0),
        right: Some("P".into()),
        expiration: Some("20260522".into()),
        multiplier: Some("100".into()),
    };
    assert_eq!(pos.security_type, "OPT");
    assert_eq!(pos.strike, Some(45.0));
    assert_eq!(pos.right.as_deref(), Some("P"));
    assert_eq!(pos.expiration.as_deref(), Some("20260522"));
    assert_eq!(pos.multiplier.as_deref(), Some("100"));
}

#[test]
fn position_call_option_has_right_c() {
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
    assert_eq!(pos.right.as_deref(), Some("C"));
    assert_eq!(pos.strike, Some(40.0));
}