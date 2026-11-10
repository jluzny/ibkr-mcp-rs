//! Test-only fake gateway for the account-summary / positions / executions
//! reads (see `docs/account-summary-streaming.md`).
//!
//! This file is included once, under `#[cfg(test)]` only, from
//! `src/ibkr/account.rs` (`crate::ibkr::account::gateway`). It is shared by the
//! manager-level tests (`src/ibkr/account_tests.rs`) and the payload tests in
//! the inline `mod tests` at the tail of `src/mcp/tools.rs`, so nothing here
//! reaches a release build.
//!
//! The harness mirrors `src/ibkr/orders_fault_injection.rs`: a real
//! `ibapi::Client` connects over `127.0.0.1` to a scripted gateway that speaks
//! the v213 handshake and answers the request frames the bridge itself sends.
//! Frames are hand-encoded protobuf (the upstream builders are `pub(crate)` to
//! `ibapi`). Every fixture is proved by the scenario that decodes it: a frame
//! that stops decoding fails loudly rather than turning into a passing
//! "it errored" assertion.
//!
//! Stream facts this harness relies on (verified against rust-ibapi v5.1.0,
//! checkout `726407c`):
//! * account summary and executions are **request-id** subscriptions; every
//!   response frame must echo the id from the request (`req_id`, proto tag 1),
//!   including `AccountSummaryEnd` and `ExecutionDataEnd`, or upstream cannot
//!   route it.
//! * positions are a **shared** (message-type) subscription: `Position` /
//!   `PositionEnd` carry no request id. An `ErrorMessage` therefore cannot be
//!   delivered into this stream (`routing.rs::classify_error` routes id-bearing
//!   errors to request subscriptions and id-less ones to the notice stream); a
//!   transport close is the in-band failure a positions stream can observe.
//! * `ExecutionDataEnd`'s id doubles as the order-id routing key, so it must
//!   carry the executions request id.
//! * `CommissionReport` is routed by `execution_id`, which upstream aliases on
//!   the preceding `ExecutionData`; a report after the end marker is unrouted.

#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::config::Config;
use crate::ibkr::account::AccountManager;
use crate::ibkr::client::IbkrClient;

// === wire constants (v5.1.0 / server version 213) =========================

const SERVER_VERSION: i32 = 213;
const PROTOBUF_MSG_ID: i32 = 200;

const IN_ERROR: i32 = 4;
const IN_NEXT_VALID_ID: i32 = 9;
const IN_EXECUTION_DATA: i32 = 11;
const IN_MANAGED_ACCOUNTS: i32 = 15;
const IN_EXECUTION_DATA_END: i32 = 55;
const IN_COMMISSIONS_REPORT: i32 = 59;
const IN_POSITION: i32 = 61;
const IN_POSITION_END: i32 = 62;
const IN_ACCOUNT_SUMMARY: i32 = 63;
const IN_ACCOUNT_SUMMARY_END: i32 = 64;
const IN_PNL_SINGLE: i32 = 95;

const OUT_REQUEST_EXECUTIONS: i32 = 7;
const OUT_REQUEST_POSITIONS: i32 = 61;
const OUT_REQUEST_ACCOUNT_SUMMARY: i32 = 62;
const OUT_CANCEL_ACCOUNT_SUMMARY: i32 = 63;
const OUT_REQUEST_PNL_SINGLE: i32 = 94;

/// The account the fake gateway reports.
pub(crate) const ACCOUNT: &str = "DUK000000";
/// A second account, used by the positions account-filter scenario.
pub(crate) const OTHER_ACCOUNT: &str = "DU9000001";

const ERROR_CODE: i32 = 201;
const ERROR_MESSAGE: &str = "query rejected";

// === minimal protobuf writer ==============================================

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn put_tag(out: &mut Vec<u8>, field: u32, wire_type: u8) {
    put_varint(out, u64::from((field << 3) | u32::from(wire_type)));
}

fn put_int(out: &mut Vec<u8>, field: u32, value: i64) {
    put_tag(out, field, 0);
    put_varint(out, value as u64);
}

fn put_double(out: &mut Vec<u8>, field: u32, value: f64) {
    put_tag(out, field, 1);
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, field: u32, bytes: &[u8]) {
    put_tag(out, field, 2);
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn put_str(out: &mut Vec<u8>, field: u32, value: &str) {
    put_bytes(out, field, value.as_bytes());
}

// === minimal protobuf reader (just enough for requests) ===================

fn read_varint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*pos)?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

/// Walk a proto payload and return the first varint field with `want`.
fn varint_field(bytes: &[u8], want: u32) -> Option<u64> {
    let mut i = 0usize;
    while i < bytes.len() {
        let tag = read_varint(bytes, &mut i)?;
        let field = (tag >> 3) as u32;
        match (tag & 7) as u8 {
            0 => {
                let value = read_varint(bytes, &mut i)?;
                if field == want {
                    return Some(value);
                }
            }
            1 => i = i.checked_add(8)?,
            2 => {
                let len = read_varint(bytes, &mut i)? as usize;
                if field == want {
                    return None; // a string, not a varint
                }
                i = i.checked_add(len)?;
            }
            5 => i = i.checked_add(4)?,
            _ => return None,
        }
    }
    None
}

/// Walk a proto payload and return the first length-delimited field with `want`.
fn string_field(bytes: &[u8], want: u32) -> Option<String> {
    let mut i = 0usize;
    while i < bytes.len() {
        let tag = read_varint(bytes, &mut i)?;
        let field = (tag >> 3) as u32;
        match (tag & 7) as u8 {
            0 => {
                read_varint(bytes, &mut i)?;
            }
            1 => i = i.checked_add(8)?,
            2 => {
                let len = read_varint(bytes, &mut i)? as usize;
                let end = i.checked_add(len)?;
                let chunk = bytes.get(i..end)?;
                if field == want {
                    return String::from_utf8(chunk.to_vec()).ok();
                }
                i = end;
            }
            5 => i = i.checked_add(4)?,
            _ => return None,
        }
    }
    None
}

// === frames ===============================================================

fn framed(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 4);
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn proto_frame(msg_id: i32, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(payload.len() + 4);
    body.extend_from_slice(&(msg_id + PROTOBUF_MSG_ID).to_be_bytes());
    body.extend_from_slice(payload);
    framed(&body)
}

fn handshake_ack() -> Vec<u8> {
    framed(format!("{SERVER_VERSION}\020240120 12:00:00 EST\0").as_bytes())
}

fn next_valid_id(order_id: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(order_id));
    proto_frame(IN_NEXT_VALID_ID, &payload)
}

fn managed_accounts(accounts: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    put_str(&mut payload, 1, accounts);
    proto_frame(IN_MANAGED_ACCOUNTS, &payload)
}

/// `AccountSummary{req_id=1, account=2, tag=3, value=4, currency=5}`.
fn account_summary(req_id: i32, account: &str, tag: &str, value: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(req_id));
    put_str(&mut payload, 2, account);
    put_str(&mut payload, 3, tag);
    put_str(&mut payload, 4, value);
    put_str(&mut payload, 5, "USD");
    proto_frame(IN_ACCOUNT_SUMMARY, &payload)
}

fn account_summary_end(req_id: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(req_id));
    proto_frame(IN_ACCOUNT_SUMMARY_END, &payload)
}

/// A minimal `Contract`: con_id=1, symbol=2, sec_type=3,
/// last_trade_date_or_contract_month=4, strike=5, right=6, multiplier=7.
fn contract(
    con_id: i32,
    symbol: &str,
    sec_type: &str,
    last_trade_date: &str,
    strike: f64,
    right: &str,
    multiplier: f64,
) -> Vec<u8> {
    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(con_id));
    put_str(&mut payload, 2, symbol);
    put_str(&mut payload, 3, sec_type);
    if !last_trade_date.is_empty() {
        put_str(&mut payload, 4, last_trade_date);
    }
    if strike != 0.0 {
        put_double(&mut payload, 5, strike);
    }
    if !right.is_empty() {
        put_str(&mut payload, 6, right);
    }
    if multiplier != 0.0 {
        put_double(&mut payload, 7, multiplier);
    }
    payload
}

/// `Position{account=1, contract=2, position=3 (decimal string), avg_cost=4}`.
fn position(account: &str, contract: &[u8], quantity: &str, avg_cost: f64) -> Vec<u8> {
    let mut payload = Vec::new();
    put_str(&mut payload, 1, account);
    put_bytes(&mut payload, 2, contract);
    put_str(&mut payload, 3, quantity);
    put_double(&mut payload, 4, avg_cost);
    proto_frame(IN_POSITION, &payload)
}

/// `PositionEnd` — empty body, no request id (shared channel).
fn position_end() -> Vec<u8> {
    proto_frame(IN_POSITION_END, &[])
}

/// `Execution{order_id=1, exec_id=2, time=3, acct_number=4, exchange=5,
/// side=6, shares=7, price=8}`.
fn execution(
    order_id: i32,
    exec_id: &str,
    time: &str,
    account: &str,
    side: &str,
    shares: &str,
    price: f64,
) -> Vec<u8> {
    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(order_id));
    put_str(&mut payload, 2, exec_id);
    put_str(&mut payload, 3, time);
    put_str(&mut payload, 4, account);
    put_str(&mut payload, 5, "NASDAQ");
    put_str(&mut payload, 6, side);
    put_str(&mut payload, 7, shares);
    put_double(&mut payload, 8, price);
    payload
}

/// `ExecutionDetails{req_id=1, contract=2, execution=3}`.
fn execution_data(req_id: i32, contract: &[u8], execution: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(req_id));
    put_bytes(&mut payload, 2, contract);
    put_bytes(&mut payload, 3, execution);
    proto_frame(IN_EXECUTION_DATA, &payload)
}

/// `ExecutionDataEnd{req_id=1}` — the id also serves as the order routing key.
fn execution_data_end(req_id: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(req_id));
    proto_frame(IN_EXECUTION_DATA_END, &payload)
}

/// `CommissionAndFeesReport{exec_id=1, commission_and_fees=2, currency=3,
/// realized_pnl=4}`.
fn commission_report(exec_id: &str, commission: f64, realized_pnl: f64) -> Vec<u8> {
    let mut payload = Vec::new();
    put_str(&mut payload, 1, exec_id);
    put_double(&mut payload, 2, commission);
    put_str(&mut payload, 3, "USD");
    put_double(&mut payload, 4, realized_pnl);
    proto_frame(IN_COMMISSIONS_REPORT, &payload)
}

/// `PnLSingle{req_id=1, position=2 (decimal string), daily_pn_l=3,
/// unrealized_pn_l=4, realized_pn_l=5, value=6}`.
fn pnl_single(req_id: i32, position_qty: &str, value: f64) -> Vec<u8> {
    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(req_id));
    put_str(&mut payload, 2, position_qty);
    put_double(&mut payload, 3, 5.0);
    put_double(&mut payload, 4, 7.5);
    put_double(&mut payload, 5, 0.0);
    put_double(&mut payload, 6, value);
    proto_frame(IN_PNL_SINGLE, &payload)
}

/// `ErrorMessage{id=1, error_code=3, error_msg=4}`.
fn error_frame(req_id: i32, code: i32, message: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(req_id));
    put_int(&mut payload, 3, i64::from(code));
    put_str(&mut payload, 4, message);
    proto_frame(IN_ERROR, &payload)
}

// === scripts ==============================================================

/// What the gateway answers when the bridge asks for the account summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SummaryScript {
    /// Nine tags, then `AccountSummaryEnd` (the complete initial dump).
    Complete,
    /// Nine tags, then hang up without the end marker.
    NoEnd,
    /// Nine tags minus `MaintMarginReq` (an absent tag).
    AbsentMaintMargin,
    /// Nine tags with `MaintMarginReq = MaxFloat64` (a sentinel).
    SentinelMaintMargin,
    /// `NetLiquidation`, a pause, then `BuyingPower` + end (per-tag receipt).
    PerTagReceipt,
    /// Complete dump on the first request; the second request (a refresh) hangs
    /// up without a single frame.
    RefreshCloses,
    /// Complete dump on the first request; the second request (a refresh) is
    /// accepted and left silent, so the refresh never delivers a frame.
    RefreshSilent,
    /// IBKR's three-request cap, with cancels honoured: at most three
    /// `RequestAccountSummary` frames may be registered at once, and a fourth
    /// is answered with `[322]` for its request id. `AccountSummaryEnd` does
    /// NOT release a slot — only `CancelAccountSummary` does. Every accepted
    /// request gets the complete dump, so a refresh that follows a confirmed
    /// desubscribe always restamps the receipts.
    CapEnforced,
    /// The production failure mode: two summary requests are already
    /// registered by a previous client, and a `CancelAccountSummary` from this
    /// connection is observed but never releases its slot. The replacement
    /// opened after a refresh is therefore answered with `[322]`, exactly as
    /// the r39 build saw in the journal.
    CapSaturated,
}

/// Two request ids a previous client left registered, for `CapSaturated`.
const FOREIGN_SUMMARY_ID_1: i32 = 7001;
const FOREIGN_SUMMARY_ID_2: i32 = 7002;

/// IBKR's documented cap on concurrent `RequestAccountSummary` requests.
const SUMMARY_CAP: usize = 3;

/// The error text IBKR sends when the cap is exceeded (`error code 322`).
const SUMMARY_CAP_ERROR: &str =
    "Maximum number of account summary requests exceeded; desubscribe to previous request first";

/// Whether this script enforces the three-request cap (and therefore tracks
/// registered request ids).
fn enforces_summary_cap(script: SummaryScript) -> bool {
    matches!(script, SummaryScript::CapEnforced | SummaryScript::CapSaturated)
}

/// What the gateway answers when the bridge asks for positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PositionsScript {
    /// `PositionEnd` and nothing else.
    EmptyEnd,
    /// One stock position, then `PositionEnd`.
    OneRowEnd,
    /// A stock and an option, then `PositionEnd`.
    TwoRowsEnd,
    /// Three positions (one for [`OTHER_ACCOUNT`]), then `PositionEnd`.
    ThreeRowsFilter,
    /// One position, then silence (the deadline must fire).
    OneRowSilence,
    /// One position, then hang up with no end marker.
    OneRowClose,
}

/// What the gateway answers when the bridge asks for executions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionsScript {
    /// `ExecutionDataEnd` and nothing else.
    EmptyEnd,
    /// Two fills, one commission report, then `ExecutionDataEnd`.
    TwoRowsWithCommission,
    /// One fill, then silence (the deadline must fire).
    OneRowSilence,
    /// One fill, then hang up with no end marker.
    OneRowClose,
    /// One fill, then a hard error bound to the executions request id.
    OneRowThenError,
    /// One fill, `ExecutionDataEnd`, a pause, then a commission report.
    CommissionAfterEnd,
}

/// The per-stream scripts for one gateway instance.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Script {
    pub summary: Option<SummaryScript>,
    pub positions: Option<PositionsScript>,
    pub executions: Option<ExecutionsScript>,
    /// Answer `RequestPnLSingle` with a mark (`value / position`).
    pub respond_to_pnl: bool,
}

impl Script {
    pub(crate) fn summary(script: SummaryScript) -> Self {
        Self { summary: Some(script), ..Self::default() }
    }

    pub(crate) fn positions(script: PositionsScript) -> Self {
        Self { positions: Some(script), ..Self::default() }
    }

    pub(crate) fn positions_with_marks(script: PositionsScript) -> Self {
        Self { positions: Some(script), respond_to_pnl: true, ..Self::default() }
    }

    pub(crate) fn executions(script: ExecutionsScript) -> Self {
        Self { executions: Some(script), ..Self::default() }
    }
}

/// What the gateway observed on the wire, for assertions about the request.
#[derive(Debug, Clone, Default)]
pub(crate) struct Capture {
    /// The comma-joined tag list of the `RequestAccountSummary` frame.
    pub summary_tags: Arc<Mutex<Option<String>>>,
    /// Number of `RequestAccountSummary` frames seen (shared across every
    /// connection, so a reconnect cannot reset the count).
    summary_requests: Arc<AtomicUsize>,
    /// Number of `CancelAccountSummary` frames seen.
    summary_cancels: Arc<AtomicUsize>,
    /// `request` / `cancel` in wire order, so a test can prove the cancel that
    /// releases the single slot always precedes a re-request.
    summary_events: Arc<Mutex<Vec<&'static str>>>,
    /// Highest number of summary requests registered at the same time (only
    /// tracked by the cap-enforcing scripts).
    summary_peak_open: Arc<AtomicUsize>,
    /// Number of `[322]` refusals written.
    summary_cap_errors: Arc<AtomicUsize>,
    /// Request ids of the `RequestAccountSummary` frames, in wire order.
    summary_request_ids: Arc<Mutex<Vec<i32>>>,
    /// Request ids named by the `CancelAccountSummary` frames, in wire order.
    summary_cancel_ids: Arc<Mutex<Vec<i32>>>,
}

impl Capture {
    /// Wait for the summary request and return its tag list.
    pub(crate) async fn summary_tags(&self) -> String {
        for _ in 0..100 {
            if let Some(tags) = self.summary_tags.lock().unwrap().clone() {
                return tags;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the bridge never sent a RequestAccountSummary frame");
    }

    pub(crate) fn summary_requests(&self) -> usize {
        self.summary_requests.load(Ordering::SeqCst)
    }

    pub(crate) fn summary_cancels(&self) -> usize {
        self.summary_cancels.load(Ordering::SeqCst)
    }

    pub(crate) fn summary_events(&self) -> Vec<&'static str> {
        self.summary_events.lock().unwrap().clone()
    }

    /// Peak number of concurrently registered summary requests.
    pub(crate) fn summary_peak_open(&self) -> usize {
        self.summary_peak_open.load(Ordering::SeqCst)
    }

    /// Number of `[322]` refusals the gateway wrote.
    pub(crate) fn summary_cap_errors(&self) -> usize {
        self.summary_cap_errors.load(Ordering::SeqCst)
    }

    pub(crate) fn summary_request_ids(&self) -> Vec<i32> {
        self.summary_request_ids.lock().unwrap().clone()
    }

    pub(crate) fn summary_cancel_ids(&self) -> Vec<i32> {
        self.summary_cancel_ids.lock().unwrap().clone()
    }
}

// === the fake gateway =====================================================

/// Bind a fake gateway on an ephemeral loopback port.
pub(crate) async fn spawn_gateway(script: Script) -> (u16, Capture) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind 127.0.0.1:0");
    let port = listener.local_addr().expect("local addr").port();
    let served = Arc::new(AtomicBool::new(false));
    let capture = Capture::default();
    let capture_for_task = capture.clone();

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let served = Arc::clone(&served);
            let capture = capture_for_task.clone();
            tokio::spawn(async move {
                let _ = serve(stream, script, served, capture).await;
            });
        }
    });

    (port, capture)
}

async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut length = [0u8; 4];
    if stream.read_exact(&mut length).await.is_err() {
        return Ok(None);
    }
    let mut body = vec![0u8; u32::from_be_bytes(length) as usize];
    stream.read_exact(&mut body).await?;
    Ok(Some(body))
}

/// The message id of a frame body, with the protobuf offset removed.
fn request_id_of(body: &[u8]) -> Option<i32> {
    let raw = i32::from_be_bytes(body.get(..4)?.try_into().ok()?);
    Some(if raw > PROTOBUF_MSG_ID { raw - PROTOBUF_MSG_ID } else { raw })
}

/// Whether the scenario must hang up (the truncated-read scripts).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flow {
    Keep,
    Close,
}

async fn serve(
    mut stream: TcpStream,
    script: Script,
    served: Arc<AtomicBool>,
    capture: Capture,
) -> std::io::Result<()> {
    let mut magic = [0u8; 4];
    stream.read_exact(&mut magic).await?;
    let mut range_length = [0u8; 4];
    stream.read_exact(&mut range_length).await?;
    let mut version_range = vec![0u8; u32::from_be_bytes(range_length) as usize];
    stream.read_exact(&mut version_range).await?;

    stream.write_all(&handshake_ack()).await?;
    stream.write_all(&next_valid_id(9000)).await?;
    stream.write_all(&managed_accounts(ACCOUNT)).await?;
    stream.flush().await?;

    if served.swap(true, Ordering::SeqCst) {
        drain(&mut stream).await;
        return Ok(());
    }

    // Request ids currently registered against the summary cap on THIS
    // connection. Only the cap-enforcing scripts fill it; `CapSaturated`
    // starts with two slots held by a "previous client" and never releases
    // them, which is the production failure this harness reproduces.
    let mut open_summary: Vec<i32> = Vec::new();
    if script.summary == Some(SummaryScript::CapSaturated) {
        open_summary.push(FOREIGN_SUMMARY_ID_1);
        open_summary.push(FOREIGN_SUMMARY_ID_2);
        capture.summary_peak_open.store(open_summary.len(), Ordering::SeqCst);
    }

    loop {
        let Some(body) = read_frame(&mut stream).await? else {
            return Ok(());
        };
        let Some(msg) = request_id_of(&body) else {
            continue;
        };
        let payload = &body[4..];
        let flow = match msg {
            OUT_REQUEST_ACCOUNT_SUMMARY => {
                let req_id = varint_field(payload, 1).unwrap_or(0) as i32;
                if let Some(tags) = string_field(payload, 3) {
                    *capture.summary_tags.lock().unwrap() = Some(tags);
                }
                let nth = capture.summary_requests.fetch_add(1, Ordering::SeqCst) + 1;
                capture.summary_events.lock().unwrap().push("request");
                capture.summary_request_ids.lock().unwrap().push(req_id);

                // The cap counts requests still registered at the gateway: an
                // `AccountSummaryEnd` does not release the slot, only a
                // `CancelAccountSummary` does.
                if script.summary.is_some_and(enforces_summary_cap) {
                    if open_summary.len() >= SUMMARY_CAP {
                        capture.summary_cap_errors.fetch_add(1, Ordering::SeqCst);
                        stream
                            .write_all(&error_frame(req_id, 322, SUMMARY_CAP_ERROR))
                            .await?;
                        stream.flush().await?;
                        Flow::Keep
                    } else {
                        open_summary.push(req_id);
                        let open = open_summary.len();
                        capture.summary_peak_open.fetch_max(open, Ordering::SeqCst);
                        run_summary(&mut stream, script.summary.unwrap(), req_id, ACCOUNT).await?
                    }
                } else {
                    match script.summary {
                        // A refresh that never answers: one script closes the
                        // transport, the other leaves the request accepted and
                        // silent (the read-side deadline is the only bound).
                        Some(SummaryScript::RefreshCloses) if nth > 1 => Flow::Close,
                        Some(SummaryScript::RefreshSilent) if nth > 1 => Flow::Keep,
                        Some(s) => run_summary(&mut stream, s, req_id, ACCOUNT).await?,
                        None => Flow::Keep,
                    }
                }
            }
            OUT_CANCEL_ACCOUNT_SUMMARY => {
                // Observed, not answered: IBKR releases the slot silently, if
                // it releases it at all — `CapSaturated` models a cancel that
                // names the right request id and is still not honoured.
                let req_id = varint_field(payload, 1).unwrap_or(0) as i32;
                capture.summary_cancels.fetch_add(1, Ordering::SeqCst);
                capture.summary_events.lock().unwrap().push("cancel");
                capture.summary_cancel_ids.lock().unwrap().push(req_id);
                // `CapSaturated` is the one script whose cancels are ignored.
                if script.summary == Some(SummaryScript::CapEnforced) {
                    open_summary.retain(|id| *id != req_id);
                }
                Flow::Keep
            }
            OUT_REQUEST_POSITIONS => match script.positions {
                Some(s) => run_positions(&mut stream, s).await?,
                None => Flow::Keep,
            },
            OUT_REQUEST_EXECUTIONS => {
                let req_id = varint_field(payload, 1).unwrap_or(0) as i32;
                match script.executions {
                    Some(s) => run_executions(&mut stream, s, req_id).await?,
                    None => Flow::Keep,
                }
            }
            OUT_REQUEST_PNL_SINGLE if script.respond_to_pnl => {
                let req_id = varint_field(payload, 1).unwrap_or(0) as i32;
                stream.write_all(&pnl_single(req_id, "100", 200.0)).await?;
                stream.flush().await?;
                Flow::Keep
            }
            _ => Flow::Keep,
        };
        if flow == Flow::Close {
            return Ok(());
        }
    }
}

/// The nine-tag dump. `MaintMarginReq` is overridden per scenario.
fn summary_rows(script: SummaryScript) -> Vec<(&'static str, &'static str)> {
    let maint = match script {
        SummaryScript::SentinelMaintMargin => "1.7976931348623157e308",
        _ => "15000.00",
    };
    let mut rows = vec![
        ("NetLiquidation", "100000.50"),
        ("AvailableFunds", "50000.25"),
        ("ExcessLiquidity", "60000.10"),
        ("BuyingPower", "200000.00"),
        ("TotalCashValue", "75000.00"),
        ("GrossPositionValue", "25000.00"),
        ("EquityWithLoanValue", "100000.50"),
        ("MaintMarginReq", maint),
        ("InitMarginReq", "18000.00"),
    ];
    if script == SummaryScript::AbsentMaintMargin {
        rows.retain(|(tag, _)| *tag != "MaintMarginReq");
    }
    rows
}

async fn run_summary(
    stream: &mut TcpStream,
    script: SummaryScript,
    req_id: i32,
    account: &str,
) -> std::io::Result<Flow> {
    match script {
        SummaryScript::PerTagReceipt => {
            stream
                .write_all(&account_summary(req_id, account, "NetLiquidation", "100000.50"))
                .await?;
            stream.flush().await?;
            tokio::time::sleep(Duration::from_millis(500)).await;
            stream
                .write_all(&account_summary(req_id, account, "BuyingPower", "200000.00"))
                .await?;
            stream.flush().await?;
            stream.write_all(&account_summary_end(req_id)).await?;
            stream.flush().await?;
            Ok(Flow::Keep)
        }
        _ => {
            for (tag, value) in summary_rows(script) {
                stream.write_all(&account_summary(req_id, account, tag, value)).await?;
            }
            stream.flush().await?;
            if script == SummaryScript::NoEnd {
                return Ok(Flow::Close);
            }
            stream.write_all(&account_summary_end(req_id)).await?;
            stream.flush().await?;
            Ok(Flow::Keep)
        }
    }
}

fn stock_position(account: &str) -> Vec<u8> {
    let contract = contract(265_598, "AAPL", "STK", "", 0.0, "", 0.0);
    position(account, &contract, "100", 150.25)
}

fn option_position(account: &str) -> Vec<u8> {
    let contract = contract(728_391, "IBIT", "OPT", "20260522", 45.0, "P", 100.0);
    position(account, &contract, "-1", 254.29)
}

async fn run_positions(stream: &mut TcpStream, script: PositionsScript) -> std::io::Result<Flow> {
    match script {
        PositionsScript::EmptyEnd => {}
        PositionsScript::OneRowEnd => {
            stream.write_all(&stock_position(ACCOUNT)).await?;
        }
        PositionsScript::TwoRowsEnd => {
            stream.write_all(&stock_position(ACCOUNT)).await?;
            stream.write_all(&option_position(ACCOUNT)).await?;
        }
        PositionsScript::ThreeRowsFilter => {
            stream.write_all(&stock_position(ACCOUNT)).await?;
            stream.write_all(&option_position(ACCOUNT)).await?;
            let contract = contract(934_221, "TSLA", "STK", "", 0.0, "", 0.0);
            stream
                .write_all(&position(OTHER_ACCOUNT, &contract, "42", 250.0))
                .await?;
        }
        PositionsScript::OneRowSilence => {
            stream.write_all(&stock_position(ACCOUNT)).await?;
            stream.flush().await?;
            return Ok(Flow::Keep);
        }
        PositionsScript::OneRowClose => {
            stream.write_all(&stock_position(ACCOUNT)).await?;
            stream.flush().await?;
            return Ok(Flow::Close);
        }
    }
    stream.write_all(&position_end()).await?;
    stream.flush().await?;
    Ok(Flow::Keep)
}

fn fill_one(req_id: i32) -> Vec<u8> {
    let contract = contract(265_598, "AAPL", "STK", "", 0.0, "", 0.0);
    let exec = execution(
        7001,
        "0001.aaa",
        "20261020 10:00:00 US/Eastern",
        ACCOUNT,
        "BOT",
        "10",
        150.5,
    );
    execution_data(req_id, &contract, &exec)
}

fn fill_two(req_id: i32) -> Vec<u8> {
    let contract = contract(265_598, "AAPL", "STK", "", 0.0, "", 0.0);
    let exec = execution(
        7002,
        "0002.bbb",
        "20261021 11:30:00 US/Eastern",
        ACCOUNT,
        "SLD",
        "5",
        155.0,
    );
    execution_data(req_id, &contract, &exec)
}

async fn run_executions(
    stream: &mut TcpStream,
    script: ExecutionsScript,
    req_id: i32,
) -> std::io::Result<Flow> {
    match script {
        ExecutionsScript::EmptyEnd => {}
        ExecutionsScript::TwoRowsWithCommission => {
            stream.write_all(&fill_one(req_id)).await?;
            stream.write_all(&fill_two(req_id)).await?;
            stream
                .write_all(&commission_report("0001.aaa", 1.25, 10.0))
                .await?;
        }
        ExecutionsScript::OneRowSilence => {
            stream.write_all(&fill_one(req_id)).await?;
            stream.flush().await?;
            return Ok(Flow::Keep);
        }
        ExecutionsScript::OneRowClose => {
            stream.write_all(&fill_one(req_id)).await?;
            stream.flush().await?;
            return Ok(Flow::Close);
        }
        ExecutionsScript::OneRowThenError => {
            stream.write_all(&fill_one(req_id)).await?;
            stream
                .write_all(&error_frame(req_id, ERROR_CODE, ERROR_MESSAGE))
                .await?;
            stream.flush().await?;
            return Ok(Flow::Keep);
        }
        ExecutionsScript::CommissionAfterEnd => {
            stream.write_all(&fill_one(req_id)).await?;
            stream.flush().await?;
            stream.write_all(&execution_data_end(req_id)).await?;
            stream.flush().await?;
            tokio::time::sleep(Duration::from_millis(400)).await;
            stream
                .write_all(&commission_report("0001.aaa", 1.25, 10.0))
                .await?;
            stream.flush().await?;
            return Ok(Flow::Keep);
        }
    }
    stream.write_all(&execution_data_end(req_id)).await?;
    stream.flush().await?;
    Ok(Flow::Keep)
}

async fn drain(stream: &mut TcpStream) {
    loop {
        match read_frame(stream).await {
            Ok(Some(_)) => continue,
            Ok(None) | Err(_) => return,
        }
    }
}

// === driving the production reads =========================================

/// Connect a real `ibapi::Client` to the fake gateway.
pub(crate) async fn connected_client(port: u16) -> Arc<IbkrClient> {
    connected_client_config(port).await.0
}

/// Connect and also hand back the config the client was built from.
pub(crate) async fn connected_client_config(port: u16) -> (Arc<IbkrClient>, Arc<Config>) {
    let mut config = Config::default();
    config.ibkr.host = "127.0.0.1".to_string();
    config.ibkr.port = port;
    config.ibkr.client_id = 320;
    config.ibkr.connection_timeout_secs = 5;

    let client = IbkrClient::new(config.ibkr.clone());
    Arc::clone(&client).connect();

    for _ in 0..100 {
        if client.is_connected().await {
            return (client, Arc::new(config));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the bridge never connected to the fake gateway on port {port}");
}

/// Connect and hand back the account manager under test.
pub(crate) async fn connected_manager(port: u16) -> (Arc<IbkrClient>, AccountManager) {
    let client = connected_client(port).await;
    (Arc::clone(&client), AccountManager::new(Arc::clone(&client)))
}

/// Connect and hand back a manager whose refresh cadence is test-sized.
pub(crate) async fn connected_manager_with_refresh(
    port: u16,
    refresh_interval: Duration,
) -> (Arc<IbkrClient>, AccountManager) {
    let client = connected_client(port).await;
    (
        Arc::clone(&client),
        AccountManager::new(Arc::clone(&client)).with_refresh_interval(refresh_interval),
    )
}
