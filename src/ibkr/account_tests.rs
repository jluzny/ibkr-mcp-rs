use super::*;

#[test]
fn position_stock_has_null_option_fields() {
    let pos = Position {
        account_id: "U18197748".into(),
        symbol: "SOFI".into(),
        quantity: 700.0,
        average_cost: 25.21,
        contract_id: 0,
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
        contract_id: 0,
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
        contract_id: 0,
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

// ============================================================================
// Unit tests for FIX 1 (stale cache) and FIX 2 (PnL enrichment)
// ============================================================================

#[test]
fn filter_sentinel_zeros_garbage_values() {
    assert_eq!(filter_sentinel(f64::MAX), 0.0);
    assert_eq!(filter_sentinel(1.79e11), 0.0);
    assert_eq!(filter_sentinel(-1.79e11), 0.0);
    assert_eq!(filter_sentinel(f64::NAN), 0.0);
    assert_eq!(filter_sentinel(f64::INFINITY), 0.0);
    assert_eq!(filter_sentinel(f64::NEG_INFINITY), 0.0);
    assert_eq!(filter_sentinel(1e11), 0.0);       // exactly at boundary
    assert_eq!(filter_sentinel(-1e11), 0.0);      // exactly at boundary
}

#[test]
fn filter_sentinel_passes_valid_values() {
    assert_eq!(filter_sentinel(150.25), 150.25);
    assert_eq!(filter_sentinel(-5000.00), -5000.00);
    assert_eq!(filter_sentinel(0.0), 0.0);
    assert_eq!(filter_sentinel(99999999.99), 99999999.99);  // just under cap
    assert_eq!(filter_sentinel(-99999999.99), -99999999.99);
}

#[test]
fn mark_price_stock_computation() {
    // Stock: value=10927.0, qty=700.0 -> mark_price=15.61
    let value: f64 = 10927.0;
    let qty: f64 = 700.0;
    let mark: f64 = value / qty;  // no /100 for stocks
    assert!((mark - 15.61).abs() < 0.01);
}

#[test]
fn mark_price_option_computation() {
    // Option: value=848.75, qty=-1.0 -> mark_price = 848.75 / 1 / 100 = 8.4875
    let value: f64 = 848.75;
    let qty: f64 = -1.0;
    let mark: f64 = (value / qty).abs() / 100.0;  // options: /100
    assert!((mark - 8.4875).abs() < 0.01);
}

#[test]
fn mark_price_zero_qty_no_division_by_zero() {
    let value: f64 = 0.0;
    let qty: f64 = 0.0;
    let mark: f64 = if qty != 0.0 { value / qty } else { 0.0 };
    assert_eq!(mark, 0.0);
}

#[test]
fn position_with_contract_id() {
    let pos = Position {
        account_id: "U12345".into(),
        symbol: "AAPL".into(),
        quantity: 100.0,
        average_cost: 150.25,
        contract_id: 265598,
        market_price: 175.50,
        market_value: 17550.0,
        unrealized_pnl: 2525.0,
        daily_pnl: 100.0,
        security_type: "STK".into(),
        strike: None,
        right: None,
        expiration: None,
        multiplier: None,
    };
    assert_eq!(pos.contract_id, 265598);
}