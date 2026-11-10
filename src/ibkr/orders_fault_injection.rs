//! Offline fault injection for the D4 open-orders read (F1).
//!
//! The production read ([`OrderManager::open_orders`]) is exercised end to end:
//! a real `ibapi::Client` connects over a loopback socket to a deterministic
//! **fake gateway** that speaks the handshake, answers `reqAllOpenOrders` and
//! then fails the stream in a scripted way. Nothing here reaches a broker: the
//! listener is bound to `127.0.0.1:0` and only ever answers the frames the
//! bridge itself sends.
//!
//! The contract under test is **fail closed**: only the gateway's
//! `openOrderEnd` marker may produce `Ok(orders)`, and a truncated or silent
//! read must never collapse into a successful `orders: []` — that is exactly
//! the collapse the D4 read exists to prevent (see `docs/broker-outage-policy.md`).
//!
//! The wire fixtures are hand-encoded (protobuf, field by field) because the
//! upstream `testdata`/`MessageBusStub` helpers are `pub(crate)` to `ibapi` and
//! cannot be reached from this crate. Every frame this module builds is
//! asserted against the decoded result of the scenario that uses it, so a
//! fixture that stops decoding fails the test loudly instead of turning into a
//! passing "it errored" assertion. The `openOrder`/`orderStatus` frames are
//! proved by the populated snapshot assertion; the `ErrorMessage` frame is
//! proved by subscribing to `Client::notice_stream()` in every error scenario
//! and asserting the decoded hard notice (code, text, request-less id,
//! `!is_informational()`).
//!
//! **Accepted contract (review round 2, decision A).** Gateway `ErrorMessage`
//! frames are *not* delivered to this subscription at all: upstream routes a
//! request-less hard error to `notice_stream()` (and to in-flight one-shot
//! requests), never into the `RequestAllOpenOrders` stream. Only transport
//! failures (`ConnectionReset`, `Shutdown`) arrive in band. The gateway's end
//! marker therefore remains authoritative: an explicit hard error followed by
//! `openOrderEnd` yields the ordinary successful snapshot (`count: 0` if no
//! rows arrived) and the error is visible only on the notice stream, which the
//! bridge does not consume. That is upstream-faithful and is the contract these
//! tests pin; the scenarios below are split into close / silence / marker so
//! each outcome is measured rather than implied.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::config::Config;
use crate::ibkr::client::IbkrClient;
use crate::ibkr::error::IbkrError;
use crate::ibkr::orders::OrderManager;

// === wire constants (v5.1.0 / server version 213) =========================

/// The protocol version the fake gateway negotiates
/// (`server_versions::PROTOBUF_REST_MESSAGES_3`): the version the ibapi test
/// suite itself uses for a real-socket handshake.
const SERVER_VERSION: i32 = 213;
/// Added to every protobuf-framed message id on the wire.
const PROTOBUF_MSG_ID: i32 = 200;

/// `IncomingMessages::OrderStatus`.
const IN_ORDER_STATUS: i32 = 3;
/// `IncomingMessages::Error`.
const IN_ERROR: i32 = 4;
/// `IncomingMessages::OpenOrder`.
const IN_OPEN_ORDER: i32 = 5;
/// `IncomingMessages::NextValidId`.
const IN_NEXT_VALID_ID: i32 = 9;
/// `IncomingMessages::ManagedAccounts`.
const IN_MANAGED_ACCOUNTS: i32 = 15;
/// `IncomingMessages::OpenOrderEnd` — the D4 end marker.
const IN_OPEN_ORDER_END: i32 = 53;
/// `OutgoingMessages::RequestAllOpenOrders`.
const OUT_REQUEST_ALL_OPEN_ORDERS: i32 = 16;

/// The account the fake gateway reports.
const ACCOUNT: &str = "DUK000000";
/// Order id the fake gateway reports as open.
const ORDER_ID: i32 = 7001;
/// Client id the fake gateway attributes the open order to.
const ORDER_CLIENT_ID: i32 = 320;
/// TWS's unspecified id: a request-less error (route: global notice stream).
const UNSPECIFIED_ID: i32 = -1;
/// The injected hard error code / text. 201 is a hard error, not a warning.
const ERROR_CODE: i32 = 201;
const ERROR_MESSAGE: &str = "Order rejected";

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

/// `int32` / `int64` / `bool` field (wire type 0).
fn put_int(out: &mut Vec<u8>, field: u32, value: i64) {
    put_tag(out, field, 0);
    put_varint(out, value as u64);
}

/// `double` field (wire type 1).
fn put_double(out: &mut Vec<u8>, field: u32, value: f64) {
    put_tag(out, field, 1);
    out.extend_from_slice(&value.to_le_bytes());
}

/// `string` / nested-message field (wire type 2).
fn put_bytes(out: &mut Vec<u8>, field: u32, bytes: &[u8]) {
    put_tag(out, field, 2);
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn put_str(out: &mut Vec<u8>, field: u32, value: &str) {
    put_bytes(out, field, value.as_bytes());
}

// === frames ===============================================================

/// Length-prefix a frame body: `[4-byte BE length][body]` (the IB wire format).
fn framed(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 4);
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A protobuf body: `[4-byte BE msg_id + 200][proto payload]`.
fn proto_body(msg_id: i32, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(payload.len() + 4);
    body.extend_from_slice(&(msg_id + PROTOBUF_MSG_ID).to_be_bytes());
    body.extend_from_slice(payload);
    body
}

/// A protobuf-framed wire frame.
fn proto_frame(msg_id: i32, payload: &[u8]) -> Vec<u8> {
    framed(&proto_body(msg_id, payload))
}

/// A text body: `[4-byte BE msg_id][NUL-delimited payload]`. The end marker
/// (`OpenOrderEnd`) is a unit marker with no payload to decode, so it ships as
/// a text frame — exactly as the upstream real-socket fixtures do.
fn text_frame(msg_id: i32, payload: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(payload.len() + 4);
    body.extend_from_slice(&msg_id.to_be_bytes());
    body.extend_from_slice(payload.as_bytes());
    framed(&body)
}

/// The handshake ack: `"<server_version>\0<connection time>\0"`.
fn handshake_ack() -> Vec<u8> {
    framed(format!("{SERVER_VERSION}\020240120 12:00:00 EST\0").as_bytes())
}

/// `NextValidId{order_id}` — proto tag 1.
fn next_valid_id(order_id: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(order_id));
    proto_frame(IN_NEXT_VALID_ID, &payload)
}

/// `ManagedAccounts{accounts_list}` — proto tag 1.
fn managed_accounts(accounts: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    put_str(&mut payload, 1, accounts);
    proto_frame(IN_MANAGED_ACCOUNTS, &payload)
}

/// A complete `OpenOrder` report for the resting AAPL limit order.
///
/// `OpenOrder{order_id=1, contract=2, order=3, order_state=4}` with the fields
/// `proto::decoders::{decode_contract, decode_order, decode_order_state}`
/// require: `Order.action` and `OrderState.status` are mandatory there, and a
/// missing `contract` / `order` / `order_state` is a decode error.
fn open_order() -> Vec<u8> {
    let mut contract = Vec::new();
    put_int(&mut contract, 1, 265_598); // con_id
    put_str(&mut contract, 2, "AAPL"); // symbol
    put_str(&mut contract, 3, "STK"); // sec_type

    let mut order = Vec::new();
    put_int(&mut order, 1, i64::from(ORDER_CLIENT_ID)); // client_id
    put_int(&mut order, 2, i64::from(ORDER_ID)); // order_id
    put_int(&mut order, 3, 99_887_766); // perm_id
    put_str(&mut order, 5, "SELL"); // action (required)
    put_str(&mut order, 6, "6"); // total_quantity (decimal string)
    put_str(&mut order, 8, "LMT"); // order_type
    put_double(&mut order, 9, 331.74); // lmt_price
    put_str(&mut order, 11, "DAY"); // tif
    put_str(&mut order, 12, ACCOUNT); // account
    put_int(&mut order, 19, 0); // outside_rth
    put_str(&mut order, 28, "ladder"); // order_ref

    let mut order_state = Vec::new();
    put_str(&mut order_state, 1, "Submitted"); // status (required)

    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(ORDER_ID)); // order_id
    put_bytes(&mut payload, 2, &contract);
    put_bytes(&mut payload, 3, &order);
    put_bytes(&mut payload, 4, &order_state);
    proto_frame(IN_OPEN_ORDER, &payload)
}

/// A live `OrderStatus` overlay for [`open_order`]`s order: 2 filled, 4 left.
fn order_status() -> Vec<u8> {
    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(ORDER_ID)); // order_id
    put_str(&mut payload, 2, "Submitted"); // status
    put_str(&mut payload, 3, "2"); // filled
    put_str(&mut payload, 4, "4"); // remaining
    put_int(&mut payload, 6, 99_887_766); // perm_id
    put_int(&mut payload, 9, i64::from(ORDER_CLIENT_ID)); // client_id
    proto_frame(IN_ORDER_STATUS, &payload)
}

/// The D4 end marker: `openOrderEnd`, which closes the subscription cleanly.
fn open_order_end() -> Vec<u8> {
    text_frame(IN_OPEN_ORDER_END, "1\0")
}

/// A hard, request-less error frame: `ErrorMessage{id, error_code, error_msg}`,
/// proto schema tags 1 / 3 / 4 (`error_time` is tag 2 and is left off). `id =
/// -1` is TWS's unspecified id, so the error is delivered on the global notice
/// stream instead of into the open-orders subscription.
fn order_error_frame(order_id: i32, code: i32, message: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    put_int(&mut payload, 1, i64::from(order_id)); // id
    put_int(&mut payload, 3, i64::from(code)); // error_code
    put_str(&mut payload, 4, message); // error_msg
    proto_frame(IN_ERROR, &payload)
}

// === the fake gateway =====================================================

/// What the fake gateway does once the bridge asks for the open orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Script {
    /// Hang up before any `openOrder`: the truncated read.
    CloseBeforeRows,
    /// One `openOrder`, then hang up. The partial list that must not become a
    /// successful read.
    CloseAfterPartialRows,
    /// One `openOrder`, then a hard request-less error, then hang up. The
    /// partial list that must not become a successful read.
    ErrorThenClose,
    /// A hard request-less error, then silence with the socket held open. The
    /// error is on the notice stream, not on the read; only the deadline ends it.
    ErrorThenSilence,
    /// A hard request-less error, then `openOrderEnd` with zero rows. The
    /// marker is authoritative upstream: a successful empty snapshot (accepted).
    ErrorThenMarker,
    /// The legitimate empty book: the end marker and nothing else.
    EmptyEndMarker,
    /// One `openOrder`, one live `orderStatus`, then the end marker.
    PopulatedEndMarker,
    /// Say nothing and keep the socket open: the deadline must fire.
    Silence,
}

/// Bind a fake gateway on an ephemeral loopback port and return it.
///
/// The first connection that completes a handshake and sends the open-orders
/// request runs `script`; later connections (the client's own reconnect
/// attempts) are handshaken and then left silent, so a scenario runs exactly
/// once per test.
pub(crate) async fn spawn_gateway(script: Script) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind 127.0.0.1:0");
    let port = listener.local_addr().expect("local addr").port();
    let served = Arc::new(AtomicBool::new(false));

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let served = Arc::clone(&served);
            tokio::spawn(async move {
                let _ = serve(stream, script, served).await;
            });
        }
    });

    port
}

/// Read one length-prefixed frame body, or `None` at end of stream.
async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut length = [0u8; 4];
    if stream.read_exact(&mut length).await.is_err() {
        return Ok(None);
    }
    let mut body = vec![0u8; u32::from_be_bytes(length) as usize];
    stream.read_exact(&mut body).await?;
    Ok(Some(body))
}

/// The message id of a request frame, with the protobuf offset removed.
fn request_id_of(body: &[u8]) -> Option<i32> {
    let raw = i32::from_be_bytes(body.get(..4)?.try_into().ok()?);
    Some(if raw > PROTOBUF_MSG_ID { raw - PROTOBUF_MSG_ID } else { raw })
}

/// Serve one connection: handshake, account info, then — on the first
/// connection that actually handshakes — the script.
///
/// The bridge's connect path opens a throwaway TCP pre-flight connection
/// before the handshake, so "the first socket accepted" is not the session.
/// The script is claimed by the first *completed handshake* instead, and later
/// connections (the client's own reconnect attempts) are handshaken and left
/// silent so a scenario runs exactly once per test.
async fn serve(mut stream: TcpStream, script: Script, served: Arc<AtomicBool>) -> std::io::Result<()> {
    // The bridge's handshake: "API\0" + length-prefixed version range.
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

    // The bridge sends start_api and then the request. Everything before the
    // request is drained: a scripted frame sent earlier would be consumed by
    // the handshake's account-info reader instead of the subscription.
    loop {
        match read_frame(&mut stream).await? {
            None => return Ok(()),
            Some(body) => {
                if request_id_of(&body) == Some(OUT_REQUEST_ALL_OPEN_ORDERS) {
                    break;
                }
            }
        }
    }

    run_script(&mut stream, script).await
}

async fn run_script(stream: &mut TcpStream, script: Script) -> std::io::Result<()> {
    match script {
        Script::CloseBeforeRows => {}
        Script::CloseAfterPartialRows => {
            stream.write_all(&open_order()).await?;
            stream.flush().await?;
        }
        Script::ErrorThenClose => {
            stream.write_all(&open_order()).await?;
            stream
                .write_all(&order_error_frame(UNSPECIFIED_ID, ERROR_CODE, ERROR_MESSAGE))
                .await?;
            stream.flush().await?;
        }
        Script::ErrorThenSilence => {
            stream
                .write_all(&order_error_frame(UNSPECIFIED_ID, ERROR_CODE, ERROR_MESSAGE))
                .await?;
            stream.flush().await?;
            drain(stream).await;
            return Ok(());
        }
        Script::ErrorThenMarker => {
            stream
                .write_all(&order_error_frame(UNSPECIFIED_ID, ERROR_CODE, ERROR_MESSAGE))
                .await?;
            stream.write_all(&open_order_end()).await?;
            stream.flush().await?;
        }
        Script::EmptyEndMarker => {
            stream.write_all(&open_order_end()).await?;
            stream.flush().await?;
        }
        Script::PopulatedEndMarker => {
            stream.write_all(&open_order()).await?;
            stream.write_all(&order_status()).await?;
            stream.write_all(&open_order_end()).await?;
            stream.flush().await?;
        }
        Script::Silence => {
            drain(stream).await;
            return Ok(());
        }
    }
    // Dropping the stream hangs up (FIN) with no end marker — the truncated
    // read the fail-closed contract is about.
    Ok(())
}

/// Read and discard inbound frames until the peer hangs up (or forever, for
/// the scenarios that hold the socket open).
async fn drain(stream: &mut TcpStream) {
    loop {
        match read_frame(stream).await {
            Ok(Some(_)) => continue,
            Ok(None) | Err(_) => return,
        }
    }
}

// === driving the production read =========================================

/// Connect the bridge to a fake gateway and hand back the manager.
pub(crate) async fn connected_manager(port: u16) -> (Arc<IbkrClient>, OrderManager) {
    let mut config = Config::default();
    config.ibkr.host = "127.0.0.1".to_string();
    config.ibkr.port = port;
    config.ibkr.client_id = 320;
    config.ibkr.connection_timeout_secs = 5;

    let client = IbkrClient::new(config.ibkr.clone());
    Arc::clone(&client).connect();

    for _ in 0..100 {
        if client.is_connected().await {
            let manager = OrderManager::new(Arc::clone(&client));
            return (client, manager);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the bridge never connected to the fake gateway on port {port}");
}

// === tests ===============================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// The fail-closed contract, in one place: whatever the gateway does, a
    /// read that did not end in `openOrderEnd` is an error — never a
    /// successful (possibly empty) order list.
    fn assert_failed_closed(
        scenario: &str,
        result: Result<crate::ibkr::orders::OpenOrdersRead, IbkrError>,
    ) {
        match result {
            Err(e) => println!("{scenario}: failed closed with {e}"),
            Ok(read) => panic!(
                "{scenario}: a truncated read must not succeed, got orders={:?}",
                read.orders
            ),
        }
    }

    /// A hang-up before any `openOrder`: no end marker ever arrived, so the
    /// read must be an error, and never `orders: []`.
    #[tokio::test]
    async fn a_close_before_any_row_fails_the_read() {
        let port = spawn_gateway(Script::CloseBeforeRows).await;
        let (_client, manager) = connected_manager(port).await;
        assert_failed_closed("close before rows", manager.open_orders("", "").await);
    }

    /// A hang-up after a partial row: the rows that did arrive are not a
    /// result. This is the collapse D4 closes — "one open order came back" and
    /// "the snapshot is one order" must not read the same.
    #[tokio::test]
    async fn a_close_after_a_partial_row_fails_the_read() {
        let port = spawn_gateway(Script::CloseAfterPartialRows).await;
        let (_client, manager) = connected_manager(port).await;
        assert_failed_closed("close after partial rows", manager.open_orders("", "").await);
    }

    /// Subscribe to the client's global notice stream. Must be called before
    /// the read so the error broadcast is not missed. (The concrete
    /// `NoticeStream` type lives in an upstream `pub(crate)` module, so it is
    /// kept as an inferred local rather than named.)
    macro_rules! subscribe_notices {
        ($client:expr) => {
            $client
                .get_client()
                .await
                .expect("the connected client")
                .notice_stream()
                .expect("a notice stream")
        };
    }

    /// The `ErrorMessage` fixture must decode as a **hard** notice: this is the
    /// self-check the module header promises for the error frame. It would pass
    /// silently on the pre-F4 encoding only as a default/undecodable notice
    /// (code 0, empty text, informational); the assertions below reject that.
    macro_rules! assert_hard_notice {
        ($notices:expr, $scenario:expr) => {{
            let notice = tokio::time::timeout(
                Duration::from_secs(2),
                $notices.next(),
            )
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{}: the hard error frame never decoded into a notice",
                    $scenario
                )
            })
            .unwrap_or_else(|| panic!("{}: the notice stream closed", $scenario));
            assert_eq!(notice.code, ERROR_CODE, "{}: decoded error code", $scenario);
            assert_eq!(
                notice.message, ERROR_MESSAGE,
                "{}: decoded error text",
                $scenario
            );
            assert_eq!(
                notice.request_id, None,
                "{}: id -1 is request-less",
                $scenario
            );
            assert!(
                !notice.is_informational(),
                "{}: code {ERROR_CODE} must classify as a hard error, not a warning",
                $scenario
            );
        }};
    }

    /// A hard request-less error, then a hang-up: the read still fails closed on
    /// the transport (`ConnectionReset`) and the error is separately proved to
    /// have decoded into a hard notice.
    #[tokio::test]
    async fn an_error_then_a_close_fails_the_read() {
        let port = spawn_gateway(Script::ErrorThenClose).await;
        let (client, manager) = connected_manager(port).await;
        let mut notices = subscribe_notices!(client);

        assert_failed_closed("error then close", manager.open_orders("", "").await);
        assert_hard_notice!(notices, "error then close");
    }

    /// A hard request-less error with the socket then held open: upstream does
    /// not put gateway error frames on this subscription, so the only end is
    /// the production deadline. The error is still decoded on the notice stream.
    ///
    /// This is the full [`OPEN_ORDERS_TIMEOUT_SECS`] wait on purpose.
    #[tokio::test]
    async fn an_error_then_silence_fails_the_read_on_the_deadline() {
        let port = spawn_gateway(Script::ErrorThenSilence).await;
        let (client, manager) = connected_manager(port).await;
        let mut notices = subscribe_notices!(client);
        let started = std::time::Instant::now();

        assert_failed_closed("error then silence", manager.open_orders("", "").await);
        assert!(
            started.elapsed() >= Duration::from_secs(crate::ibkr::orders::OPEN_ORDERS_TIMEOUT_SECS),
            "the read must wait out its deadline, took {:?}",
            started.elapsed()
        );
        assert_hard_notice!(notices, "error then silence");
    }

    /// A hard request-less error followed by `openOrderEnd` and zero rows: the
    /// marker is authoritative upstream and the read succeeds with an empty
    /// snapshot. This pins the accepted behaviour (review round 2, decision A) —
    /// the error is observable on `Client::notice_stream()`, which the bridge
    /// does not consume, and is *not* an error envelope for this read.
    #[tokio::test]
    async fn an_error_then_the_marker_is_an_accepted_empty_snapshot() {
        let port = spawn_gateway(Script::ErrorThenMarker).await;
        let (client, manager) = connected_manager(port).await;
        let mut notices = subscribe_notices!(client);

        let read = manager
            .open_orders(ACCOUNT, "")
            .await
            .expect("upstream treats the end marker as authoritative");

        let payload = read.as_payload();
        assert_eq!(payload["success"], serde_json::json!(true));
        assert_eq!(payload["count"], serde_json::json!(0));
        assert_eq!(payload["orders"], serde_json::json!([]));
        assert_hard_notice!(notices, "error then marker");
    }

    /// A gateway that says nothing at all: the read must fail on its deadline
    /// rather than return an empty book.
    ///
    /// This is the full [`OPEN_ORDERS_TIMEOUT_SECS`] wait on purpose — the
    /// production deadline is the thing under test.
    #[tokio::test]
    async fn a_silent_gateway_fails_the_read_on_the_deadline() {
        let port = spawn_gateway(Script::Silence).await;
        let (_client, manager) = connected_manager(port).await;
        let started = std::time::Instant::now();
        assert_failed_closed("silent gateway", manager.open_orders("", "").await);
        assert!(
            started.elapsed() >= Duration::from_secs(crate::ibkr::orders::OPEN_ORDERS_TIMEOUT_SECS),
            "the read must wait out its deadline, took {:?}",
            started.elapsed()
        );
    }

    /// The session is torn down **while the snapshot read is in flight** — the
    /// window upstream #966 widened, where `Client::disconnect()` now closes
    /// the socket under a read that already holds it (the bridge's
    /// `drop_connection()` on a stale session, `src/ibkr/client.rs`).
    ///
    /// The read must end in an error: a torn-down session is not an empty book.
    #[tokio::test]
    async fn a_session_torn_down_during_the_read_fails_it() {
        let port = spawn_gateway(Script::Silence).await;
        let (client, manager) = connected_manager(port).await;

        let mut read = Box::pin(manager.open_orders(ACCOUNT, ""));
        let result = tokio::time::timeout(Duration::from_secs(20), async move {
            tokio::select! {
                result = &mut read => result,
                _ = tokio::time::sleep(Duration::from_millis(300)) => {
                    client.disconnect().await;
                    read.await
                }
            }
        })
        .await
        .expect("the in-flight read must settle once the session is gone");

        assert_failed_closed("session torn down during the read", result);
    }

    /// The legitimate empty book: `openOrderEnd` with no rows before it is a
    /// success, and it is the *only* way an empty `orders` list is produced.
    #[tokio::test]
    async fn the_end_marker_alone_is_a_successful_empty_snapshot() {
        let port = spawn_gateway(Script::EmptyEndMarker).await;
        let (_client, manager) = connected_manager(port).await;
        let read = manager
            .open_orders(ACCOUNT, "")
            .await
            .expect("an end marker is a successful read");

        let payload = read.as_payload();
        assert_eq!(payload["success"], serde_json::json!(true));
        assert_eq!(payload["accountId"], serde_json::json!(ACCOUNT));
        assert_eq!(payload["count"], serde_json::json!(0));
        assert_eq!(payload["workingCount"], serde_json::json!(0));
        assert_eq!(payload["orders"], serde_json::json!([]));
    }

    /// The end marker after a complete snapshot: the rows are projected and
    /// the live status overlay lands on them, so the fixture frames are proved
    /// to decode rather than merely to fail.
    #[tokio::test]
    async fn the_end_marker_returns_the_snapshot_the_gateway_reported() {
        let port = spawn_gateway(Script::PopulatedEndMarker).await;
        let (_client, manager) = connected_manager(port).await;
        let read = manager
            .open_orders(ACCOUNT, "aapl")
            .await
            .expect("a complete snapshot with its end marker is a successful read");

        assert_eq!(read.orders.len(), 1, "got {:?}", read.orders);
        let order = &read.orders[0];
        assert_eq!(order.order_id, ORDER_ID);
        assert_eq!(order.client_id, ORDER_CLIENT_ID);
        assert_eq!(order.perm_id, 99_887_766);
        assert_eq!(order.account, ACCOUNT);
        assert_eq!(order.symbol, "AAPL");
        assert_eq!(order.con_id, 265_598);
        assert_eq!(order.sec_type, "STK");
        assert_eq!(order.side, "SELL");
        assert_eq!(order.quantity, 6.0);
        assert_eq!(order.order_type, "LMT");
        assert_eq!(order.lmt_price, Some(331.74));
        assert_eq!(order.tif, "DAY");
        assert_eq!(order.status, "Submitted");
        assert!(order.working);
        // The `orderStatus` frame overlays the live fields...
        assert_eq!(order.filled, 2.0);
        assert_eq!(order.remaining, 4.0);
        // ...without losing the statics the `openOrder` report carried.
        assert_eq!(order.order_ref, "ladder");
        assert!(!order.outside_rth);
    }

    /// The filters still apply to a snapshot that ended properly: a symbol the
    /// gateway did not report is an empty — but successful — result.
    #[tokio::test]
    async fn a_complete_snapshot_that_filters_to_nothing_is_still_successful() {
        let port = spawn_gateway(Script::PopulatedEndMarker).await;
        let (_client, manager) = connected_manager(port).await;
        let read = manager
            .open_orders(ACCOUNT, "MSFT")
            .await
            .expect("the snapshot ended; the filter is not a failure");

        assert!(read.orders.is_empty());
        assert_eq!(read.as_payload()["success"], serde_json::json!(true));
    }

    // === regression discriminator (F5) =======================================
    //
    // The black-box scenarios above cannot tell the round-2 fix from the
    // pre-fix `while let Some(item) = subscription.next() { … } Ok(rows)` fold:
    // on this checkout every injectable truncation arrives as an in-band error
    // item, so both implementations fail (or succeed) identically — the
    // reviewer's sabotage control passed 10/10 on the reverted read. This
    // tripwire is the part of the suite that fails on the old code path.

    #[test]
    fn the_open_orders_read_uses_the_fail_closed_collector() {
        let source = include_str!("orders.rs");
        let start = source
            .find("pub async fn open_orders(")
            .expect("the open_orders read must exist in orders.rs");
        let tail = &source[start..];
        let end = tail
            .find("pub async fn get_order_status(")
            .expect("open_orders is followed by get_order_status");
        let body = &tail[..end];

        assert!(
            body.contains("collect_to_end_within"),
            "open_orders must end through collect_to_end_within (v5.1.0 fail-closed collector)"
        );
        assert!(
            !body.contains("subscription.next()"),
            "open_orders must not drive the subscription with a hand-written `next()` fold"
        );
        assert!(
            body.contains("OPEN_ORDERS_TIMEOUT_SECS"),
            "the manual fold was reverted with its deadline too; the 10 s bound must stay"
        );
    }

    // === rejection controls ==================================================
    //
    // The scenarios above are only evidence if the assertions they use reject
    // the outcome they exist to catch. These two controls run the *same*
    // production read against the *same* fake gateway and show the two
    // directions disagreeing on the truncated/complete split — so neither an
    // `open_orders` that collapsed a failed read into `Ok(orders)` nor one that
    // failed every read could pass this module.

    /// Catch a panic from `body` without its message reaching the test output.
    fn catching<F: FnOnce()>(body: F) -> bool {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).is_err();
        std::panic::set_hook(previous);
        panicked
    }

    /// A real, successful (empty) snapshot must be *rejected* by the fail-closed
    /// assertion — i.e. that assertion cannot pass a fail-open read.
    #[tokio::test]
    async fn control_the_fail_closed_assertion_rejects_a_real_successful_read() {
        let port = spawn_gateway(Script::EmptyEndMarker).await;
        let (_client, manager) = connected_manager(port).await;
        let read = manager
            .open_orders(ACCOUNT, "")
            .await
            .expect("the end marker alone is a successful read");

        assert!(
            catching(move || assert_failed_closed("control", Ok(read))),
            "assert_failed_closed accepted a successful read"
        );
    }

    /// A real, truncated read must be *rejected* by the success path the other
    /// scenarios assert through.
    #[tokio::test]
    async fn control_the_success_path_rejects_a_truncated_read() {
        let port = spawn_gateway(Script::CloseBeforeRows).await;
        let (_client, manager) = connected_manager(port).await;
        let result = manager.open_orders("", "").await;

        assert!(
            catching(move || {
                let _ = result.expect("a truncated read is not a result");
            }),
            "the success path accepted a truncated read"
        );
    }
}
