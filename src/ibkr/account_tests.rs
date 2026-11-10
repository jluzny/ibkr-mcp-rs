use super::*;

#[test]
fn position_stock_has_null_option_fields() {
    let pos = Position {
        account_id: "U00000000".into(),
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
        account_id: "U00000000".into(),
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
        account_id: "U00000000".into(),
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
// ============================================================================
// Offline fake-gateway scenarios (design §7.1-7.3)
// ============================================================================

use super::gateway::{
    connected_manager, connected_manager_with_refresh, spawn_gateway, ExecutionsScript,
    PositionsScript, Script, SummaryScript, ACCOUNT, OTHER_ACCOUNT,
};

/// Poll the manager until the predicate holds, or fail after 8s.
///
/// The summary pump caches every NLV-bearing frame, so a read can legitimately
/// observe the dump mid-flight. Waiting for the property under test (not for a
/// fixed sleep) keeps the assertions about semantics rather than timing.
async fn wait_for_summary(
    manager: &AccountManager,
    what: &str,
    mut done: impl FnMut(&AccountRead) -> bool,
) -> AccountRead {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let read = manager
            .get_account_info_read(None)
            .await
            .unwrap_or_else(|e| panic!("{what}: account read failed: {e}"));
        if done(&read) {
            return read;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: timed out waiting; last read = {read:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// --- 7.1 account summary ---------------------------------------------------

#[tokio::test]
async fn account_summary_complete() {
    let (port, _capture) = spawn_gateway(Script::summary(SummaryScript::Complete)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = wait_for_summary(&manager, "complete dump", |r| {
        r.provenance.terminal_sentinel && r.provenance.count == 9
    })
    .await;

    assert_eq!(read.provenance.end_marker.as_deref(), Some("AccountSummaryEnd"));
    assert_eq!(read.provenance.count, 9);
    assert_eq!(read.info.net_liquidation, 100_000.50);
    assert_eq!(read.info.available_funds, 50_000.25);
    assert_eq!(read.info.excess_liquidity, 60_000.10);
    assert_eq!(read.info.buying_power, 200_000.00);
    assert_eq!(read.info.currency, "USD");
    for tag in [
        "TotalCashValue",
        "GrossPositionValue",
        "MaintMarginReq",
        "InitMarginReq",
    ] {
        assert!(
            read.tags.get(tag).and_then(|t| t.value).is_some(),
            "{tag} must be carried by the complete dump"
        );
    }
    assert_eq!(read.tags["TotalCashValue"].value, Some(75_000.00));
    assert_eq!(read.tags["GrossPositionValue"].value, Some(25_000.00));
    assert_eq!(read.tags["EquityWithLoanValue"].value, Some(100_000.50));
    assert_eq!(read.tags["MaintMarginReq"].value, Some(15_000.00));
    assert_eq!(read.tags["InitMarginReq"].value, Some(18_000.00));
    // LookAhead tags are payload-only: subscribed, but no fixture carries them.
    assert!(read.tags.get("LookAheadMaintMarginReq").is_none());
    // `received_at` is a bridge-local RFC3339 UTC instant, not broker time.
    assert!(read.provenance.received_at.ends_with('Z'));
    assert!(read.tags["MaintMarginReq"].received_at.ends_with('Z'));
}

#[tokio::test]
async fn account_summary_no_end_is_partial() {
    let (port, _capture) = spawn_gateway(Script::summary(SummaryScript::NoEnd)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = wait_for_summary(&manager, "partial dump", |r| {
        r.provenance.count == 9 && r.provenance.end_marker.is_none()
    })
    .await;

    // Rows are still reported...
    assert_eq!(read.tags["NetLiquidation"].value, Some(100_000.50));
    assert_eq!(read.provenance.count, 9);
    // ...but the read never claims completeness without the end marker.
    assert!(!read.provenance.terminal_sentinel);
    assert_eq!(read.provenance.end_marker, None);
}

#[tokio::test]
async fn account_summary_absent_tag_is_null() {
    let (port, _capture) = spawn_gateway(Script::summary(SummaryScript::AbsentMaintMargin)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = wait_for_summary(&manager, "dump without MaintMarginReq", |r| {
        r.provenance.terminal_sentinel
    })
    .await;

    // Not requested/absent: absent from the map entirely, never 0.0.
    assert!(read.tags.get("MaintMarginReq").is_none());
    assert_eq!(read.provenance.count, 8);
    assert!(read.tags["NetLiquidation"].value.is_some());
}

#[tokio::test]
async fn account_summary_sentinel_is_null() {
    let (port, _capture) = spawn_gateway(Script::summary(SummaryScript::SentinelMaintMargin)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = wait_for_summary(&manager, "dump with sentinel MaintMarginReq", |r| {
        r.provenance.terminal_sentinel
    })
    .await;

    let maint = read.tags.get("MaintMarginReq").expect("tag arrives");
    assert_eq!(maint.value, None, "a sentinel is absence, not a number");
    assert_eq!(read.provenance.count, 8);
}

#[tokio::test]
async fn account_summary_per_tag_receipt() {
    let (port, _capture) = spawn_gateway(Script::summary(SummaryScript::PerTagReceipt)).await;
    let (_client, manager) = connected_manager(port).await;

    let first = wait_for_summary(&manager, "first tag", |r| {
        r.tags.contains_key("NetLiquidation")
    })
    .await;
    let nlv_receipt = first.tags["NetLiquidation"].received_at.clone();
    let generation = first.provenance.generation;

    let later = wait_for_summary(&manager, "second tag", |r| {
        r.provenance.terminal_sentinel && r.tags.contains_key("BuyingPower")
    })
    .await;

    // A later frame refreshes only the tag it carries: NLV's receipt is not
    // restamped by the arrival of BuyingPower.
    assert_eq!(later.tags["NetLiquidation"].received_at, nlv_receipt);
    assert!(
        later.tags["BuyingPower"].received_at >= nlv_receipt,
        "the new tag's receipt must not predate the old one"
    );
    assert_eq!(later.provenance.generation, generation);
}

#[tokio::test]
async fn account_summary_margin_tags_requested() {
    let (port, capture) = spawn_gateway(Script::summary(SummaryScript::Complete)).await;
    let (_client, manager) = connected_manager(port).await;

    let _ = wait_for_summary(&manager, "complete dump", |r| r.provenance.terminal_sentinel).await;
    let tags = capture.summary_tags().await;

    for tag in [
        "NetLiquidation",
        "AvailableFunds",
        "ExcessLiquidity",
        "BuyingPower",
        "TotalCashValue",
        "GrossPositionValue",
        "EquityWithLoanValue",
        "MaintMarginReq",
        "InitMarginReq",
        "LookAheadMaintMarginReq",
        "LookAheadInitMarginReq",
    ] {
        assert!(tags.split(',').any(|t| t == tag), "{tag} missing from {tags}");
    }
}

// --- 7.1b refresh cadence (card A/B) ---------------------------------------

/// Read until the gateway has seen `n` summary requests, or fail after 8s.
///
/// Refreshes are read-driven: a read that observes receipts older than the
/// cadence is what asks the pump to cancel and re-request.
async fn wait_for_summary_requests_with_reads(
    manager: &AccountManager,
    capture: &super::gateway::Capture,
    n: usize,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while capture.summary_requests() < n {
        assert!(
            tokio::time::Instant::now() < deadline,
            "gateway never saw {n} summary requests; events = {:?}",
            capture.summary_events()
        );
        manager
            .get_account_info_read(None)
            .await
            .unwrap_or_else(|e| panic!("read while waiting for request {n}: {e}"));
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[test]
fn refresh_cadence_gates_requests() {
    let interval = Duration::from_secs(60);
    assert!(!refresh_is_due(Duration::from_secs(59), interval));
    assert!(refresh_is_due(interval, interval));
    assert!(
        !refresh_is_due(Duration::from_secs(3600), Duration::ZERO),
        "a ZERO interval disables the periodic refresh"
    );
    assert!(refresh_may_reissue(true, interval, interval));
    assert!(!refresh_may_reissue(false, interval, interval));
    assert!(
        !refresh_may_reissue(true, Duration::from_secs(1), interval),
        "at most one re-request per interval"
    );
}

#[test]
fn a_refresh_restamps_only_the_tags_a_frame_carries() {
    let mut dump = std::collections::HashMap::new();
    apply_summary_frame(
        &mut dump,
        "NetLiquidation",
        Some(100_000.50),
        Some("USD".into()),
        "2020-01-01T00:00:00.000Z".into(),
    );
    apply_summary_frame(
        &mut dump,
        "BuyingPower",
        Some(200_000.00),
        Some("USD".into()),
        "2026-10-26T10:00:00.000Z".into(),
    );
    let snapshot = build_account_read(&dump, Some(ACCOUNT), None, 7);

    // The cadence is judged by the OLDEST receipt, not by the cache timestamp:
    // a streaming subscription can restamp the cache while a tag never changes.
    let oldest = oldest_receipt_age(&snapshot).expect("both receipts parse");
    assert!(
        oldest > Duration::from_secs(3600),
        "the 2020 receipt is the oldest, got {oldest:?}"
    );

    let (mut seeded, account, per_frame) = seed_summary_values(Some(&snapshot));
    assert!(!per_frame, "a re-request publishes atomically at End");
    assert_eq!(account.as_deref(), Some(ACCOUNT));

    apply_summary_frame(
        &mut seeded,
        "BuyingPower",
        Some(250_000.00),
        Some("USD".into()),
        "2026-10-26T10:01:00.000Z".into(),
    );

    // Only the tag the new frame carried is restamped; nothing invents a
    // receipt for a value IBKR did not send again.
    assert_eq!(seeded["NetLiquidation"].received_at, "2020-01-01T00:00:00.000Z");
    assert_eq!(seeded["BuyingPower"].received_at, "2026-10-26T10:01:00.000Z");
    assert_eq!(seeded["BuyingPower"].value, Some(250_000.00));

    // The first subscription (empty cache) is published per frame so a dump
    // without an end marker still yields partial evidence.
    let (fresh, no_account, per_frame) = seed_summary_values(None);
    assert!(fresh.is_empty());
    assert_eq!(no_account, None);
    assert!(per_frame);
}

#[tokio::test]
async fn account_summary_refresh_reissues_after_the_cadence() {
    let (port, capture) = spawn_gateway(Script::summary(SummaryScript::Complete)).await;
    let (_client, manager) = connected_manager_with_refresh(port, Duration::from_millis(300)).await;

    let first = wait_for_summary(&manager, "initial dump", |r| r.provenance.terminal_sentinel).await;
    assert_eq!(capture.summary_requests(), 1, "the initial dump is one request");

    wait_for_summary_requests_with_reads(&manager, &capture, 2).await;

    // The refresh goes through the ONE pump: cancel first, then exactly one
    // replacement request — the old slot is released, never leaked.
    let events = capture.summary_events();
    assert_eq!(events.first().copied(), Some("request"));
    assert_eq!(events.last().copied(), Some("request"));
    assert!(
        events.contains(&"cancel"),
        "a refresh must cancel before re-requesting: {events:?}"
    );
    assert!(capture.summary_cancels() >= 1);
    assert!(
        capture.summary_requests() <= capture.summary_cancels() + 1,
        "at most one summary request is ever in flight: {events:?}"
    );

    let refreshed = wait_for_summary(&manager, "refreshed dump", |r| {
        r.provenance.generation > first.provenance.generation && r.provenance.terminal_sentinel
    })
    .await;
    assert_eq!(refreshed.provenance.count, 9);
    assert!(
        refreshed.tags["NetLiquidation"].received_at > first.tags["NetLiquidation"].received_at,
        "a re-request restamps the receipts the new dump actually carried"
    );
}

/// A refresh whose subscription never delivers a frame must fall back to the
/// last received snapshot — receipts are never restamped locally.
async fn assert_unanswered_refresh_fails_closed(script: SummaryScript) {
    let (port, capture) = spawn_gateway(Script::summary(script)).await;
    let (_client, manager) = connected_manager_with_refresh(port, Duration::from_millis(200)).await;

    let first = wait_for_summary(&manager, "initial dump", |r| r.provenance.terminal_sentinel).await;
    let nlv_receipt = first.tags["NetLiquidation"].received_at.clone();
    let generation = first.provenance.generation;

    // Let the receipts age past the cadence, then read once: the read asks the
    // pump to re-request, the gateway accepts and hangs up / stays silent, and
    // the read must return the old snapshot unchanged within REFRESH_WAIT.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = manager
        .get_account_info_read(None)
        .await
        .expect("a failed refresh still serves the last received snapshot");

    assert_eq!(
        after.provenance.generation, generation,
        "no replacement subscription ever published"
    );
    assert_eq!(
        after.tags["NetLiquidation"].received_at, nlv_receipt,
        "receipts are restamped only by a real frame"
    );
    assert_eq!(after.provenance.count, 9);
    assert!(after.tags["NetLiquidation"].value.is_some());
    assert!(capture.summary_requests() >= 2, "the refresh was attempted");
    assert!(capture.summary_cancels() >= 1, "the old slot was released");
}

#[tokio::test]
async fn account_summary_refresh_transport_close_fails_closed() {
    assert_unanswered_refresh_fails_closed(SummaryScript::RefreshCloses).await;
}

#[tokio::test]
async fn account_summary_refresh_silent_fails_closed() {
    assert_unanswered_refresh_fails_closed(SummaryScript::RefreshSilent).await;
}

#[tokio::test]
async fn account_summary_refresh_disabled_by_zero_interval() {
    let (port, capture) = spawn_gateway(Script::summary(SummaryScript::Complete)).await;
    let (_client, manager) = connected_manager_with_refresh(port, Duration::ZERO).await;

    let first = wait_for_summary(&manager, "initial dump", |r| r.provenance.terminal_sentinel).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let again = manager.get_account_info_read(None).await.expect("read");

    assert_eq!(capture.summary_requests(), 1, "ZERO disables re-requests");
    assert_eq!(capture.summary_cancels(), 0);
    assert_eq!(again.provenance.generation, first.provenance.generation);
    assert_eq!(
        again.tags["NetLiquidation"].received_at,
        first.tags["NetLiquidation"].received_at
    );
}

// --- 7.1c slot safety: desubscribe, cap, park ------------------------------

/// `Subscription::cancel()` after `AccountSummaryEnd` DOES write
/// `CancelAccountSummary` on this pin (rust-ibapi v5.1.0, checkout `726407c`):
/// the account-summary `End` is a *batch* end (`is_batch_end`), not a snapshot
/// end, so it neither suppresses the cancel nor marks the request finished.
///
/// This test pins that fact because the r39 note claimed the opposite. The
/// frame alone still releases nothing observable: IBKR never answers it, so the
/// bridge must gate the replacement itself and treat `[322]` as the proof that
/// the desubscribe did not take.
#[tokio::test]
async fn cancel_after_end_is_written_to_the_wire() {
    let (port, capture) = spawn_gateway(Script::summary(SummaryScript::CapEnforced)).await;
    let client = super::gateway::connected_client(port).await;
    let arc = client.get_client().await.expect("connected");
    let mut subscription = arc
        .account_summary(
            &AccountGroup("All".to_string()),
            &[AccountSummaryTags::NET_LIQUIDATION],
        )
        .await
        .expect("summary subscription");
    let request_id = subscription.request_id().expect("request-id subscription");

    // Consume the initial dump up to and including the `End` marker.
    loop {
        match subscription.next().await {
            Some(Ok(SubscriptionItem::Data(AccountSummaryResult::End))) => break,
            Some(Ok(_)) => continue,
            other => panic!("unexpected item before the end marker: {other:?}"),
        }
    }

    subscription.cancel().await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while capture.summary_cancels() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "cancel after AccountSummaryEnd never reached the wire"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        capture.summary_cancel_ids(),
        vec![request_id],
        "the cancel must name the request it retires"
    );
    assert!(
        matches!(subscription.next().await, Some(Err(ibapi::Error::Cancelled))),
        "cancel must make every clone terminal"
    );
}

/// The production soak: twenty read-driven refreshes against a gateway that
/// enforces IBKR's three-request cap. Every replacement must be preceded by a
/// cancel that the gateway processed, so no request is ever refused.
#[tokio::test]
async fn summary_refresh_soak_never_exceeds_the_request_cap() {
    let (port, capture) = spawn_gateway(Script::summary(SummaryScript::CapEnforced)).await;
    let (_client, manager) =
        connected_manager_with_refresh(port, Duration::from_millis(50)).await;

    let mut previous =
        wait_for_summary(&manager, "initial dump", |r| r.provenance.terminal_sentinel).await;

    let cycles = 20usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut landed = 0usize;
    while landed < cycles {
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {landed}/{cycles} refresh cycles landed; events = {:?}",
            capture.summary_events()
        );
        let read = manager.get_account_info_read(None).await.expect("read");
        if read.provenance.generation > previous.provenance.generation {
            assert!(
                read.provenance.terminal_sentinel,
                "a replacement is published atomically at End"
            );
            assert_eq!(read.provenance.count, 9);
            assert!(
                read.tags["NetLiquidation"].received_at
                    >= previous.tags["NetLiquidation"].received_at,
                "a receipt is only ever restamped by a frame that arrived"
            );
            previous = read;
            landed += 1;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(
        capture.summary_peak_open() <= 3,
        "the gateway's cap is three concurrent requests, saw {}",
        capture.summary_peak_open()
    );
    assert_eq!(
        capture.summary_cap_errors(),
        0,
        "a desubscribe-before-resubscribe refresh is never refused: {:?}",
        capture.summary_events()
    );
    assert!(
        capture.summary_requests() >= cycles + 1,
        "each cycle re-requested: {} requests",
        capture.summary_requests()
    );

    // Wire order: every request after the first is preceded by a cancel, so the
    // replacement never coexists with the request it retires.
    let events = capture.summary_events();
    let mut open = false;
    for (index, event) in events.iter().enumerate() {
        match *event {
            "request" => {
                assert!(!open, "two requests without a cancel between them: {events:?}");
                open = true;
            }
            "cancel" => {
                assert!(open, "a cancel must retire an open request: {events:?}");
                open = false;
            }
            other => panic!("unexpected event {other} at {index}"),
        }
    }

    // No fabricated receipts: a published snapshot is always backed by an
    // accepted request.
    assert!(
        previous.provenance.generation as usize <= capture.summary_requests(),
        "generation {} exceeds {} requests",
        previous.provenance.generation,
        capture.summary_requests()
    );
    assert!(!manager.summary_slot_leaked());
    assert!(manager.summary_error().is_none());
}

/// The r39 failure in a test: the gateway holds two slots from a previous
/// client and ignores this connection's cancel, so the replacement after a
/// refresh is refused with `[322]`. The pump must park after that single
/// refusal — never reopen, never loop — and the read payload must carry IBKR's
/// own text.
#[tokio::test]
async fn summary_refresh_parks_when_the_cancel_does_not_release_the_slot() {
    let (port, capture) = spawn_gateway(Script::summary(SummaryScript::CapSaturated)).await;
    let (_client, manager) =
        connected_manager_with_refresh(port, Duration::from_millis(50)).await;

    let first =
        wait_for_summary(&manager, "initial dump", |r| r.provenance.terminal_sentinel).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while capture.summary_cap_errors() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the refresh was never refused; events = {:?}",
            capture.summary_events()
        );
        let _ = manager.get_account_info_read(None).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let requests_after_refusal = capture.summary_requests();
    assert!(manager.summary_slot_leaked(), "the [322] must latch");

    // Reads keep arriving for over a second of wall clock: none may reopen.
    let until = tokio::time::Instant::now() + Duration::from_secs(1);
    while tokio::time::Instant::now() < until {
        let _ = manager.get_account_info_read(None).await;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert_eq!(
        capture.summary_requests(),
        requests_after_refusal,
        "the pump must not open another subscription after [322]; events = {:?}",
        capture.summary_events()
    );
    assert_eq!(
        capture.summary_cap_errors(),
        1,
        "exactly one refusal, not a loop: {:?}",
        capture.summary_events()
    );

    let error = manager.summary_error().expect("the real IBKR error is kept");
    assert!(
        error.contains("Maximum number of account summary requests exceeded"),
        "read payload must carry IBKR's text, got: {error}"
    );

    // The cancel named the request it retired, so the leak is the gateway's
    // released-nothing behaviour, not a cancel aimed at a stale id.
    assert_eq!(
        capture.summary_cancel_ids().first().copied(),
        capture.summary_request_ids().first().copied()
    );

    // The last snapshot is still served unchanged: no fabricated receipt.
    let read = manager.get_account_info_read(None).await.expect("served");
    assert_eq!(read.provenance.generation, first.provenance.generation);
    assert_eq!(
        read.tags["NetLiquidation"].received_at,
        first.tags["NetLiquidation"].received_at
    );
}

/// A parked pump serves the last snapshot with its TRUE age and fails closed at
/// `stale_max`, quoting the real error instead of "pump may be down".
#[tokio::test]
async fn parked_pump_serves_true_age_and_fails_closed_at_stale_max() {
    let (port, capture) = spawn_gateway(Script::summary(SummaryScript::CapSaturated)).await;
    let (_client, manager) =
        super::gateway::connected_manager_with_refresh(port, Duration::from_millis(50)).await;
    let manager = manager
        .with_cache_ttl(Duration::from_secs(1))
        .with_stale_max(Duration::from_secs(4));

    let first =
        wait_for_summary(&manager, "initial dump", |r| r.provenance.terminal_sentinel).await;
    let published = tokio::time::Instant::now();
    let nlv_receipt = first.tags["NetLiquidation"].received_at.clone();

    // Refreshes are read-driven: the read that sees aged receipts asks the
    // pump for one, which is what the saturated gateway then refuses.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while capture.summary_cap_errors() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the refresh was never refused; events = {:?}",
            capture.summary_events()
        );
        let _ = manager.get_account_info_read(None).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(manager.summary_slot_leaked(), "the [322] must latch the pump");

    // Past the cache TTL, still inside stale_max: the stale snapshot is served
    // with its true receipts.
    tokio::time::sleep_until(published + Duration::from_millis(1500)).await;
    let stale = manager
        .get_account_info_read(None)
        .await
        .expect("a stale snapshot inside stale_max is still served");
    assert_eq!(stale.provenance.generation, first.provenance.generation);
    assert_eq!(stale.tags["NetLiquidation"].received_at, nlv_receipt);

    // Past stale_max: refuse, and say why.
    tokio::time::sleep_until(published + Duration::from_millis(4500)).await;
    let error = manager
        .get_account_info_read(None)
        .await
        .expect_err("data older than stale_max must never be served");
    let text = error.to_string();
    assert!(text.contains("account info stale"), "{text}");
    assert!(
        text.contains("Maximum number of account summary requests exceeded"),
        "the refusal carries IBKR's text, got: {text}"
    );
}

// --- 7.2 positions ---------------------------------------------------------

#[tokio::test]
async fn positions_complete_empty() {
    let (port, _capture) = spawn_gateway(Script::positions(PositionsScript::EmptyEnd)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = manager.get_positions_read(None).await.expect("empty book");
    assert!(read.positions.is_empty());
    assert_eq!(read.provenance.count, 0);
    assert_eq!(read.received_count, 0);
    assert!(read.provenance.terminal_sentinel);
    assert_eq!(read.provenance.end_marker.as_deref(), Some("PositionEnd"));
    // An empty read proves nothing about marks.
    assert!(!read.marks_attested);
}

#[tokio::test]
async fn positions_complete_rows() {
    let (port, _capture) =
        spawn_gateway(Script::positions_with_marks(PositionsScript::TwoRowsEnd)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = manager.get_positions_read(None).await.expect("two rows");
    assert_eq!(read.provenance.count, 2);
    assert_eq!(read.received_count, 2);
    assert!(read.provenance.terminal_sentinel);
    assert_eq!(read.provenance.end_marker.as_deref(), Some("PositionEnd"));
    assert!(read.marks_attested);
    let aapl = read.positions.iter().find(|p| p.symbol == "AAPL").expect("AAPL");
    assert_eq!(aapl.quantity, 100.0);
    assert_eq!(aapl.contract_id, 265_598);
    assert_eq!(aapl.market_price, 2.0); // 200.0 value / 100 shares
    assert_eq!(aapl.market_value, 200.0);
    let ibit = read.positions.iter().find(|p| p.symbol == "IBIT").expect("IBIT");
    assert_eq!(ibit.security_type, "OPT");
    assert_eq!(ibit.strike, Some(45.0));
    assert_eq!(ibit.right.as_deref(), Some("P"));
    assert_eq!(ibit.expiration.as_deref(), Some("20260522"));
    assert_eq!(ibit.multiplier.as_deref(), Some("100"));
}

#[tokio::test]
async fn positions_timeout_partial() {
    let (port, _capture) = spawn_gateway(Script::positions(PositionsScript::OneRowSilence)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = manager.get_positions_read(None).await.expect("partial rows");
    assert_eq!(read.positions.len(), 1);
    assert_eq!(read.provenance.count, 1);
    // Silence is not an end marker.
    assert!(!read.provenance.terminal_sentinel);
    assert_eq!(read.provenance.end_marker, None);
}

/// A stream failure yields the rows collected so far plus an explicit partial
/// provenance (design §4). Positions ride a shared channel, so an id-bearing
/// `ErrorMessage` cannot reach this stream (`routing.rs::classify_error`); the
/// in-band failure it can observe is the transport close.
#[tokio::test]
async fn positions_stream_error_partial() {
    let (port, _capture) = spawn_gateway(Script::positions(PositionsScript::OneRowClose)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = manager.get_positions_read(None).await.expect("partial rows");
    assert_eq!(read.provenance.count, 1);
    assert_eq!(read.positions[0].symbol, "AAPL");
    assert!(!read.provenance.terminal_sentinel);
    assert_eq!(read.provenance.end_marker, None);
    assert!(!read.marks_attested);
}

#[tokio::test]
async fn positions_account_filter() {
    let (port, _capture) =
        spawn_gateway(Script::positions_with_marks(PositionsScript::ThreeRowsFilter)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = manager
        .get_positions_read(Some(ACCOUNT))
        .await
        .expect("filtered rows");
    assert_eq!(read.provenance.count, 2, "only this account's rows are emitted");
    assert_eq!(read.received_count, 3, "the filtered frame is still counted");
    assert!(read.positions.iter().all(|p| p.account_id == ACCOUNT));
    assert!(!read.positions.iter().any(|p| p.account_id == OTHER_ACCOUNT));
    assert!(read.provenance.terminal_sentinel);
}

#[tokio::test]
async fn positions_marks_unattested() {
    let (port, _capture) = spawn_gateway(Script::positions(PositionsScript::OneRowEnd)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = manager.get_positions_read(None).await.expect("one row");
    assert_eq!(read.provenance.count, 1);
    // Without a pnl_single reply the mark must not be inferred.
    assert!(!read.marks_attested);
    assert_eq!(read.positions[0].market_price, 0.0);
    assert_eq!(read.positions[0].market_value, 0.0);
}

// --- 7.3 executions --------------------------------------------------------

#[tokio::test]
async fn executions_complete_empty() {
    let (port, _capture) = spawn_gateway(Script::executions(ExecutionsScript::EmptyEnd)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = manager
        .get_executions_read(None, None, None)
        .await
        .expect("a marked empty ledger is a success");
    assert!(read.executions.is_empty());
    assert_eq!(read.provenance.count, 0);
    assert!(read.provenance.terminal_sentinel);
    assert_eq!(read.provenance.end_marker.as_deref(), Some("ExecutionDataEnd"));
}

#[tokio::test]
async fn executions_complete_rows() {
    let (port, _capture) =
        spawn_gateway(Script::executions(ExecutionsScript::TwoRowsWithCommission)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = manager
        .get_executions_read(None, None, None)
        .await
        .expect("marked ledger");
    assert_eq!(read.provenance.count, 2);
    assert!(read.provenance.terminal_sentinel);
    assert_eq!(read.provenance.end_marker.as_deref(), Some("ExecutionDataEnd"));

    let with_commission = read
        .executions
        .iter()
        .find(|e| e.execution_id == "0001.aaa")
        .expect("first fill");
    assert_eq!(with_commission.commission, 1.25);
    assert_eq!(with_commission.realized_pnl, 10.0);
    assert_eq!(with_commission.side, "BOT");
    assert_eq!(with_commission.quantity, 10.0);
    assert_eq!(with_commission.symbol, "AAPL");

    let without = read
        .executions
        .iter()
        .find(|e| e.execution_id == "0002.bbb")
        .expect("second fill");
    assert_eq!(without.commission, 0.0);
}

#[tokio::test]
async fn executions_timeout_is_error() {
    let (port, _capture) = spawn_gateway(Script::executions(ExecutionsScript::OneRowSilence)).await;
    let (_client, manager) = connected_manager(port).await;

    let result = manager.get_executions_read(None, None, None).await;
    assert!(
        result.is_err(),
        "a deadline without ExecutionDataEnd must be an error, never an empty success: {result:?}"
    );
}

#[tokio::test]
async fn executions_early_close_is_error() {
    let (port, _capture) = spawn_gateway(Script::executions(ExecutionsScript::OneRowClose)).await;
    let (_client, manager) = connected_manager(port).await;

    let result = manager.get_executions_read(None, None, None).await;
    assert!(
        result.is_err(),
        "a close without ExecutionDataEnd must fail the read: {result:?}"
    );
}

#[tokio::test]
async fn executions_notice_is_error() {
    let (port, _capture) =
        spawn_gateway(Script::executions(ExecutionsScript::OneRowThenError)).await;
    let (_client, manager) = connected_manager(port).await;

    let result = manager.get_executions_read(None, None, None).await;
    assert!(
        result.is_err(),
        "a hard error before the end marker must fail the read: {result:?}"
    );
}

/// Documented limitation: `ExecutionDataEnd` closes the stream, so a
/// `CommissionReport` that follows it is not collected. The test pins the gap
/// so a future fix is visible.
#[tokio::test]
async fn executions_commission_after_end() {
    let (port, _capture) =
        spawn_gateway(Script::executions(ExecutionsScript::CommissionAfterEnd)).await;
    let (_client, manager) = connected_manager(port).await;

    let read = manager
        .get_executions_read(None, None, None)
        .await
        .expect("marker arrived");
    assert_eq!(read.provenance.count, 1);
    assert_eq!(read.executions[0].execution_id, "0001.aaa");
    assert_eq!(
        read.executions[0].commission, 0.0,
        "a report after the end marker is outside the ledger the marker closes"
    );
}
